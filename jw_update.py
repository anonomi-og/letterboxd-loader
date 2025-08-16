# jw_update.py — with DB logging
import os, time, datetime as dt
import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv
from simplejustwatchapi.justwatch import search, offers_for_countries
from datetime import datetime, timezone
from logger import log_to_db  # <-- your logger

# ---------------- Config ----------------
PROJECT_NAME = "lbx-justwatch"

COUNTRY    = os.getenv("JW_COUNTRY", "GB")
LANG       = os.getenv("JW_LANGUAGE", "en")
SLEEP_S    = float(os.getenv("JW_SLEEP_S", "0.8"))
BEST_ONLY  = True

RECHECK_DAYS = int(os.getenv("JW_RECHECK_DAYS", "7"))    # re-scan mapped titles every N days
BATCH_SIZE   = int(os.getenv("JW_BATCH_SIZE", "500"))    # cap per run (safety)

load_dotenv()

DB_HOST = os.getenv("MARIADB_HOST", "localhost")
DB_PORT = int(os.getenv("MARIADB_PORT", "3306"))
DB_NAME = os.getenv("MARIADB_DB", "letterboxd")
DB_USER = os.getenv("MARIADB_USER", "root")
DB_PASS = os.getenv("MARIADB_PASS", "")

WATCHLIST_TABLE = "watchlist"
WL_ID, WL_TTL, WL_YEAR = "id", "film_name", "film_year"

def db():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS,
        database=DB_NAME, charset="utf8mb4", cursorclass=DictCursor, autocommit=True
    )

# ---------------- SQL ----------------
SQL_SELECT_STALE = f"""
SELECT w.{WL_ID}   AS watchlist_id,
       w.{WL_TTL}  AS title,
       w.{WL_YEAR} AS year
FROM {WATCHLIST_TABLE} w
LEFT JOIN jw_title_map m ON m.watchlist_id = w.{WL_ID}
WHERE
    m.watchlist_id IS NULL
    OR m.last_checked_at < (NOW() - INTERVAL {RECHECK_DAYS} DAY)
ORDER BY w.{WL_ID}
LIMIT %s;
"""

def fetch_targets(conn):
    with conn.cursor() as c:
        c.execute(SQL_SELECT_STALE, (BATCH_SIZE,))
        return c.fetchall()

SQL_UPSERT_MAP = """
INSERT INTO jw_title_map
(watchlist_id, entry_id, matched_via, confidence, matched_title, matched_year, matched_type, last_checked_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
 entry_id=VALUES(entry_id),
 matched_via=VALUES(matched_via),
 confidence=VALUES(confidence),
 matched_title=VALUES(matched_title),
 matched_year=VALUES(matched_year),
 matched_type=VALUES(matched_type),
 last_checked_at=VALUES(last_checked_at);
"""

SQL_SELECT_CURRENT_PROVIDERS = "SELECT provider_id FROM jw_offers_current WHERE watchlist_id=%s;"

SQL_UPSERT_CURRENT = """
INSERT INTO jw_offers_current
(watchlist_id, entry_id, provider_id, provider_name, presentation_type, url, last_seen_at)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  entry_id=VALUES(entry_id),
  provider_name=VALUES(provider_name),
  presentation_type=VALUES(presentation_type),
  url=VALUES(url),
  last_seen_at=VALUES(last_seen_at);
"""

SQL_DELETE_CURRENT_PROVIDER = "DELETE FROM jw_offers_current WHERE watchlist_id=%s AND provider_id=%s;"

SQL_SELECT_OPEN_HISTORY = """
SELECT 1 FROM jw_offers_history
WHERE watchlist_id=%s AND provider_id=%s AND valid_to IS NULL
LIMIT 1;
"""

SQL_OPEN_HISTORY = """
INSERT INTO jw_offers_history
(watchlist_id, entry_id, provider_id, provider_name, presentation_type, url, valid_from, valid_to)
VALUES (%s,%s,%s,%s,%s,%s,%s,NULL);
"""

SQL_CLOSE_HISTORY = """
UPDATE jw_offers_history
SET valid_to=%s
WHERE watchlist_id=%s AND provider_id=%s AND valid_to IS NULL;
"""

# ---------------- Helpers ----------------
def rank_pres(val: str | None) -> int:
    return {"_4K": 3, "HD": 2, "SD": 1}.get((val or "").upper(), 0)

def best_match_entry_id(title: str, year: int | None):
    """Return (entry_id, matched_via, confidence, info_dict)"""
    query = f"{title} {year}" if year else title
    results = search(query, COUNTRY, LANG, 5, BEST_ONLY) or []
    if not results:
        return None, "name_only", 0, {}
    match = None
    if year:
        for r in results:
            if getattr(r, "release_year", None) == year:
                match = r; break
    match = match or results[0]
    entry_id = getattr(match, "entry_id", None)
    matched_type = (getattr(match, "object_type", "MOVIE") or "MOVIE").upper()
    matched_title = getattr(match, "title", None)
    matched_year = getattr(match, "release_year", None)
    via = "name_year" if year else "name_only"
    conf = 85 if year else 70
    return entry_id, via, conf, {"title": matched_title, "year": matched_year, "type": matched_type}

def update_one(conn, row):
    wl_id  = row["watchlist_id"]
    title  = row["title"]
    year   = row.get("year")

    # 1) Map to JustWatch entry_id
    try:
        entry_id, via, conf, info = best_match_entry_id(title, year)
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"[{wl_id}] Search failed for '{title}' ({year}): {e}")
        return

    if not entry_id:
        log_to_db(PROJECT_NAME, "WARNING", f"[{wl_id}] No JW match for: {title} ({year})")
        return

    # timezone-aware → naive UTC for MariaDB DATETIME
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

    try:
        with conn.cursor() as c:
            c.execute(SQL_UPSERT_MAP, (
                wl_id, entry_id, via, conf,
                info.get("title") or title,
                info.get("year"),
                info.get("type") or "MOVIE",
                now_utc
            ))
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"[{wl_id}] Failed to upsert title_map: {e}")
        return

    # 2) Snapshot existing providers BEFORE update (to detect removals)
    with conn.cursor() as c:
        c.execute(SQL_SELECT_CURRENT_PROVIDERS, (wl_id,))
        pre_existing = {r["provider_id"] for r in c.fetchall()}

    # 3) Fetch offers → FLATRATE only; keep best presentation per provider
    try:
        offers_by_country = offers_for_countries(entry_id, {COUNTRY}, LANG, BEST_ONLY) or {}
        offers = offers_by_country.get(COUNTRY, []) or []
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"[{wl_id}] Offer fetch failed for entry_id={entry_id}: {e}")
        return

    flatrate = [o for o in offers if getattr(o, "monetization_type", None) == "FLATRATE"]

    seen = {}  # provider_id -> (provider_name, pres, url)
    for o in flatrate:
        pkg = getattr(o, "package", None)
        prov_id = getattr(pkg, "package_id", None) if pkg else None
        prov_name = getattr(pkg, "name", "Unknown") if pkg else "Unknown"
        pres = getattr(o, "presentation_type", None)
        url = getattr(o, "url", None)
        if prov_id is None:
            continue
        prev = seen.get(prov_id)
        if not prev or rank_pres(pres) > rank_pres(prev[1]):
            seen[prov_id] = (prov_name, pres, url)

    # 4) Upsert current snapshot for providers we see now
    try:
        with conn.cursor() as c:
            for prov_id, (prov_name, pres, url) in seen.items():
                c.execute(SQL_UPSERT_CURRENT, (
                    wl_id, entry_id, prov_id, prov_name, pres, url, now_utc
                ))
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"[{wl_id}] Failed to upsert current offers: {e}")
        return

    # 5) Close & remove providers that disappeared
    current_ids = set(seen.keys())
    to_close = pre_existing - current_ids
    try:
        with conn.cursor() as c:
            for prov_id in to_close:
                c.execute(SQL_CLOSE_HISTORY, (now_utc, wl_id, prov_id))
                c.execute(SQL_DELETE_CURRENT_PROVIDER, (wl_id, prov_id))
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"[{wl_id}] Failed to close/delete old providers: {e}")

    # 6) Open history for providers newly active (if not already open)
    try:
        with conn.cursor() as c:
            for prov_id in current_ids:
                c.execute(SQL_SELECT_OPEN_HISTORY, (wl_id, prov_id))
                exists = c.fetchone()
                if not exists:
                    prov_name, pres, url = seen[prov_id]
                    c.execute(SQL_OPEN_HISTORY, (
                        wl_id, entry_id, prov_id, prov_name, pres, url, now_utc
                    ))
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"[{wl_id}] Failed to open history rows: {e}")

    log_to_db(PROJECT_NAME, "INFO", f"[{wl_id}] {title} ({year}) → {len(seen)} flatrate providers")

def main():
    conn = db()
    try:
        log_to_db(PROJECT_NAME, "INFO",
                  f"Starting availability update (country={COUNTRY}, lang={LANG}, batch_size={BATCH_SIZE}, recheck_days={RECHECK_DAYS})")

        rows = fetch_targets(conn)
        total = len(rows)
        log_to_db(PROJECT_NAME, "INFO", f"Targets to process: {total}")

        for i, row in enumerate(rows, 1):
            log_to_db(PROJECT_NAME, "INFO",
                      f"[{i}/{total}] {row['watchlist_id']} — {row['title']} ({row.get('year')})")
            update_one(conn, row)
            time.sleep(SLEEP_S)

        log_to_db(PROJECT_NAME, "INFO", "✔️ Availability update complete.")
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"❌ Fatal error in jw_update: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
