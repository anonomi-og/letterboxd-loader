# jw_update.py — unified mapper for WATCHLIST and DIARY → jw_title_map (MariaDB)
# - Uses PyMySQL (no SQLite)
# - Composite key (source, source_row_id)
# - WATCHLIST gets offers history in jw_offers_history
# - Tolerates JustWatch library returning dicts OR objects

import os
import time
from dotenv import load_dotenv
import pymysql
from pymysql.cursors import DictCursor

from logger import log_to_db
from simplejustwatchapi.justwatch import search, offers_for_countries

PROJECT_NAME = "lbx-justwatch"

load_dotenv()

# ---------- Config ----------
COUNTRY       = os.getenv("JW_COUNTRY", "GB")
LANG          = os.getenv("JW_LANGUAGE", "en")
SLEEP_S       = float(os.getenv("JW_SLEEP_S", "0.8"))
BATCH_SIZE    = int(os.getenv("JW_BATCH_SIZE", "500"))
STALE_DAYS    = int(os.getenv("JW_STALE_DAYS", "7"))
BEST_ONLY     = True

# Which source to map this run
JW_SOURCE       = os.getenv("JW_SOURCE", "WATCHLIST").upper()           # WATCHLIST | DIARY
JW_SOURCE_TABLE = os.getenv("JW_SOURCE_TABLE") or ("watchlist" if JW_SOURCE == "WATCHLIST" else "diary")
JW_ID_COL       = os.getenv("JW_ID_COL", "id")
JW_TITLE_COL    = os.getenv("JW_TITLE_COL", "film_name")
JW_YEAR_COL     = os.getenv("JW_YEAR_COL", "film_year")

# Offers are tracked only for WATCHLIST history table per your schema
UPDATE_OFFERS = os.getenv("JW_UPDATE_OFFERS", "true").lower() in ("1", "true", "yes")

# MariaDB connection
DB = dict(
    host=os.getenv("MARIADB_HOST", "localhost"),
    port=int(os.getenv("MARIADB_PORT", "3306")),
    user=os.getenv("MARIADB_USER", "root"),
    password=os.getenv("MARIADB_PASS", ""),
    database=os.getenv("MARIADB_DB", "letterboxd"),
    charset="utf8mb4",
    cursorclass=DictCursor,
    autocommit=True,
)

# ---------- Small helpers ----------

def g(obj, *names):
    """Return first non-empty value by attribute or dict key from obj."""
    for n in names:
        if isinstance(obj, dict):
            if n in obj and obj[n] not in (None, ""):
                return obj[n]
        else:
            v = getattr(obj, n, None)
            if v not in (None, ""):
                return v
    return None

def pick_best_match(results, title, year):
    """Heuristic to pick best JustWatch result; returns (obj, via, confidence, matched_type)."""
    def norm(s): return (s or "").strip().lower()
    tnorm = norm(title)
    try:
        y = int(year) if year else None
    except Exception:
        y = None

    scored = []
    for r in results or []:
        r_title = g(r, "title", "original_title", "name") or ""
        r_year  = g(r, "year", "original_release_year")
        if not r_year:
            od = g(r, "original_release_date")
            r_year = (od or "")[:4] if od else None
        try:
            r_year = int(r_year) if r_year else None
        except Exception:
            r_year = None

        score = 0
        via   = "name_only"
        if norm(r_title) == tnorm:
            score += 10
            if y and r_year == y:
                score += 10
                via = "name_year"
        elif tnorm and tnorm in norm(r_title):
            score += 3
            if y and r_year and abs(r_year - y) <= 1:
                score += 2
                via = "name_year"

        scored.append((score, via, r_year, r_title, r))

    if not scored:
        return None, None, None, None

    scored.sort(key=lambda x: x[0], reverse=True)
    score, via, r_year, r_title, r = scored[0]
    obj_type = (g(r, "object_type", "type") or "").upper()
    matched_type = "MOVIE" if obj_type.startswith("MOVIE") else "SHOW"
    confidence = max(0, min(100, score * 5))
    return r, via, confidence, matched_type

def fetch_offers(entry_id: str):
    """Normalize offers for COUNTRY; returns list of dicts with provider_id/name/presentation_type/url."""
    try:
        raw = offers_for_countries(entry_id, countries=[COUNTRY])
    except Exception as e:
        log_to_db(PROJECT_NAME, "WARNING", f"offers_for_countries failed for {entry_id}: {e}")
        return []

    offers = raw.get(COUNTRY, []) if isinstance(raw, dict) else (raw or [])
    out = []
    for off in offers:
        provider_id       = g(off, "provider_id", "providerId")
        provider_name     = g(off, "provider_name", "providerName")
        presentation_type = g(off, "presentation_type", "presentationType")
        urls              = g(off, "urls")
        url = None
        if isinstance(urls, dict):
            url = urls.get("standard_web") or urls.get("deeplink_web") or urls.get("url")
        elif urls:
            url = getattr(urls, "standard_web", None) or getattr(urls, "deeplink_web", None) or getattr(urls, "url", None)
        if not url:
            url = g(off, "url")
        out.append({
            "provider_id": provider_id,
            "provider_name": provider_name,
            "presentation_type": presentation_type,
            "url": url
        })
    return out

# ---------- SQL ----------

SQL_SELECT_CANDIDATES = f"""
SELECT s.`{JW_ID_COL}`    AS source_row_id,
       s.`{JW_TITLE_COL}` AS title,
       s.`{JW_YEAR_COL}`  AS year
FROM `{JW_SOURCE_TABLE}` s
LEFT JOIN jw_title_map m
  ON m.source = %s
 AND m.source_row_id = s.`{JW_ID_COL}`
WHERE m.source_row_id IS NULL
   OR m.last_checked_at < (NOW() - INTERVAL {STALE_DAYS} DAY)
ORDER BY s.`{JW_ID_COL}`
LIMIT {BATCH_SIZE};
"""

SQL_UPSERT_MAP = """
INSERT INTO jw_title_map
(source, source_row_id, entry_id, matched_via, confidence, matched_title, matched_year, matched_type, last_checked_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
ON DUPLICATE KEY UPDATE
  entry_id        = VALUES(entry_id),
  matched_via     = VALUES(matched_via),
  confidence      = VALUES(confidence),
  matched_title   = VALUES(matched_title),
  matched_year    = VALUES(matched_year),
  matched_type    = VALUES(matched_type),
  last_checked_at = VALUES(last_checked_at);
"""

# jw_offers_history (WATCHLIST only)
SQL_SELECT_LAST_OFFER = """
SELECT provider_id, provider_name, presentation_type, url, valid_from, valid_to
FROM jw_offers_history
WHERE watchlist_id = %s AND provider_id = %s
ORDER BY valid_from DESC
LIMIT 1;
"""
SQL_CLOSE_OFFER = """
UPDATE jw_offers_history
SET valid_to = NOW()
WHERE watchlist_id = %s AND provider_id = %s AND valid_to IS NULL;
"""
SQL_INSERT_OFFER = """
INSERT INTO jw_offers_history
(watchlist_id, entry_id, provider_id, provider_name, presentation_type, url, valid_from, valid_to)
VALUES (%s, %s, %s, %s, %s, %s, NOW(), NULL);
"""

# ---------- Core ----------

def upsert_offer_history_watchlist(conn, watchlist_id, entry_id, provider_id, provider_name, presentation_type, url):
    with conn.cursor() as c:
        c.execute(SQL_SELECT_LAST_OFFER, (watchlist_id, provider_id))
        last = c.fetchone()

        def changed(a, b): return (a or "") != (b or "")

        if not last:
            c.execute(SQL_INSERT_OFFER, (
                watchlist_id, entry_id, provider_id,
                provider_name or str(provider_id or ""),
                presentation_type, url
            ))
            return

        if changed(last.get("presentation_type"), presentation_type) or changed(last.get("url"), url) or changed(last.get("provider_name"), provider_name):
            if last["valid_to"] is None:
                c.execute(SQL_CLOSE_OFFER, (watchlist_id, provider_id))
            c.execute(SQL_INSERT_OFFER, (
                watchlist_id, entry_id, provider_id,
                provider_name or str(provider_id or ""),
                presentation_type, url
            ))

def update_one(conn, row, cur_source):
    """Map a single row from source → jw_title_map, and (if WATCHLIST) update offers history."""
    src_id = row["source_row_id"]
    title  = (row["title"] or "").strip()
    year   = row.get("year")

    if not title:
        log_to_db(PROJECT_NAME, "WARNING", f"Empty title for {cur_source}:{src_id}, skipping")
        return

    # 1) JustWatch search
    try:
        results = search(title, country=COUNTRY, language=LANG, best_only=BEST_ONLY)
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"search() failed for {title}: {e}")
        return

    if not results:
        log_to_db(PROJECT_NAME, "WARNING", f"No JW results for {title} ({year})")
        return

    # 2) pick best
    best, matched_via, confidence, matched_type = pick_best_match(results, title, year)
    if not best:
        log_to_db(PROJECT_NAME, "WARNING", f"No match selected for {title} ({year})")
        return

    # 3) extract JustWatch entry_id (tm... for movies, ts... for shows)
    entry_id = g(best, "id", "jw_entity_id", "jwId", "jw_id")
    if not entry_id:
        # Don’t spam logs per your feedback; keep a single warn per title
        log_to_db(PROJECT_NAME, "WARNING", f"Matched but no JustWatch entry_id for {title} ({year})")
        return

    matched_title = g(best, "title", "original_title", "name") or title
    matched_year  = g(best, "year", "original_release_year")
    if not matched_year:
        od = g(best, "original_release_date")
        matched_year = (od or "")[:4] if od else None
    try:
        matched_year = int(matched_year) if matched_year else None
    except Exception:
        matched_year = None

    # 4) upsert mapping
    with conn.cursor() as c:
        c.execute(SQL_UPSERT_MAP, (
            cur_source, src_id, entry_id, matched_via, confidence, matched_title, matched_year, matched_type
        ))

    log_to_db(PROJECT_NAME, "INFO",
              f"Mapped {cur_source}:{src_id} → {entry_id} ({matched_title}, {matched_year}) via {matched_via} [{confidence}]")

    # 5) offers history (WATCHLIST only)
    if UPDATE_OFFERS and cur_source == "WATCHLIST":
        offers_list = fetch_offers(entry_id)
        if offers_list:
            for off in offers_list:
                provider_id       = off.get("provider_id")
                if provider_id is None:
                    continue
                provider_name     = off.get("provider_name") or str(provider_id)
                presentation_type = off.get("presentation_type")
                url               = off.get("url")
                upsert_offer_history_watchlist(
                    conn,
                    watchlist_id=src_id,
                    entry_id=entry_id,
                    provider_id=provider_id,
                    provider_name=provider_name,
                    presentation_type=presentation_type,
                    url=url
                )

def main():
    conn = pymysql.connect(**DB)
    try:
        # select candidates for this source
        with conn.cursor() as c:
            c.execute(SQL_SELECT_CANDIDATES, (JW_SOURCE,))
            rows = c.fetchall()

        total = len(rows)
        log_to_db(PROJECT_NAME, "INFO", f"Source={JW_SOURCE}, COUNTRY={COUNTRY}, rows={total}")

        for i, row in enumerate(rows, 1):
            log_to_db(PROJECT_NAME, "INFO",
                      f"[{i}/{total}] {JW_SOURCE}:{row['source_row_id']} — {row['title']} ({row.get('year')})")
            update_one(conn, row, JW_SOURCE)
            time.sleep(SLEEP_S)

        log_to_db(PROJECT_NAME, "INFO", "✔️ JustWatch mapping complete.")
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"❌ Fatal error in jw_update: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
