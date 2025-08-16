import os, io, zipfile, glob, csv, sys
from typing import List, Dict, Optional

import pymysql
from dotenv import load_dotenv
from logger import log_to_db

load_dotenv()

PROJECT_NAME = "letterboxd_loader"

EXPORT_DIR = os.getenv("DOWNLOAD_DIR", "./exports")

DB = dict(
    host=os.getenv("MARIADB_HOST", "localhost"),
    port=int(os.getenv("MARIADB_PORT", "3306")),
    user=os.getenv("MARIADB_USER", "root"),
    password=os.getenv("MARIADB_PASS", ""),
    database=os.getenv("MARIADB_DB", "letterboxd"),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True,
)

def latest_zip(path: str) -> str:
    zips = sorted(glob.glob(os.path.join(path, "*.zip")))
    if not zips:
        msg = f"No export ZIPs found in {path}"
        log_to_db(PROJECT_NAME, "ERROR", msg)
        raise SystemExit(msg)
    return zips[-1]

def open_csv(z: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
    with z.open(name) as f:
        return list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")))

def to_int(val: Optional[str]) -> Optional[int]:
    if val is None: return None
    val = val.strip()
    if val == "": return None
    try: return int(val)
    except: return None

def to_float(val: Optional[str]) -> Optional[float]:
    if val is None: return None
    val = val.strip()
    if val == "": return None
    try: return float(val)
    except: return None

def to_bool(val: Optional[str]) -> Optional[int]:
    if val is None: return None
    v = val.strip().lower()
    if v in ("yes", "y", "true", "1"): return 1
    if v in ("no", "n", "false", "0", ""): return 0
    return None

def ensure_unique(cur, table: str, index_name: str, cols: str):
    """Create UNIQUE index if it's missing."""
    # whitelist to avoid SQL injection in identifiers
    if table not in {"watchlist", "watched", "diary"}:
        raise ValueError("unexpected table")
    if index_name not in {"uq_watchlist", "uq_watched", "uq_diary"}:
        raise ValueError("unexpected index name")

    cur.execute(f"SHOW INDEX FROM {table} WHERE Key_name=%s", (index_name,))
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute(f"ALTER TABLE {table} ADD UNIQUE KEY {index_name} ({cols})")

def ensure_schema(cur):
    # Create tables if missing
    cur.execute("""
    CREATE TABLE IF NOT EXISTS watchlist (
      id INT AUTO_INCREMENT PRIMARY KEY,
      added_date DATE,
      film_name  VARCHAR(255),
      film_year  INT NULL,
      film_uri   TEXT
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS watched (
      id INT AUTO_INCREMENT PRIMARY KEY,
      watched_date DATE,
      film_name    VARCHAR(255),
      film_year    INT NULL,
      film_uri     TEXT
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS diary (
      id INT AUTO_INCREMENT PRIMARY KEY,
      logged_date  DATE,
      film_name    VARCHAR(255),
      film_year    INT NULL,
      film_uri     TEXT,
      rating       FLOAT NULL,
      rewatch      TINYINT NULL,
      tags         TEXT NULL,
      watched_date DATE NULL
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;""")

    # Ensure UNIQUE keys even when tables already exist
    ensure_unique(cur, "watchlist", "uq_watchlist", "film_name, film_year, added_date")
    ensure_unique(cur, "watched",   "uq_watched",   "film_name, film_year, watched_date")
    ensure_unique(cur, "diary",     "uq_diary",     "logged_date, film_name, film_year")

def main():
    zip_path = latest_zip(EXPORT_DIR)
    log_to_db(PROJECT_NAME, "INFO", f"📦 Using export: {zip_path}")

    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as cur:
            ensure_schema(cur)

            with zipfile.ZipFile(zip_path) as z:
                names = set(z.namelist())

                ins_watchlist = ins_watched = ins_diary = 0

                # watchlist.csv
                cand = [n for n in names if n.endswith("/watchlist.csv") or n == "watchlist.csv"]
                if cand:
                    rows = open_csv(z, cand[0])
                    for r in rows:
                        added_date = r.get("Date") or None
                        film_name  = r.get("Name") or None
                        film_year  = to_int(r.get("Year"))
                        film_uri   = r.get("Letterboxd URI") or None
                        if not film_name: continue
                        cur.execute(
                          """INSERT INTO watchlist (added_date, film_name, film_year, film_uri)
                             VALUES (%s, %s, %s, %s)
                             ON DUPLICATE KEY UPDATE film_uri = VALUES(film_uri)""",
                          (added_date, film_name, film_year, film_uri)
                        )
                        ins_watchlist += 1
                else:
                    log_to_db(PROJECT_NAME, "WARNING", "⚠️  watchlist.csv not found in ZIP")

                # watched.csv
                cand = [n for n in names if n.endswith("/watched.csv") or n == "watched.csv"]
                if cand:
                    rows = open_csv(z, cand[0])
                    for r in rows:
                        watched_date = r.get("Date") or None
                        film_name    = r.get("Name") or None
                        film_year    = to_int(r.get("Year"))
                        film_uri     = r.get("Letterboxd URI") or None
                        if not film_name: continue
                        cur.execute(
                          """INSERT INTO watched (watched_date, film_name, film_year, film_uri)
                             VALUES (%s, %s, %s, %s)
                             ON DUPLICATE KEY UPDATE film_uri = VALUES(film_uri)""",
                          (watched_date, film_name, film_year, film_uri)
                        )
                        ins_watched += 1
                else:
                    log_to_db(PROJECT_NAME, "WARNING", "⚠️  watched.csv not found in ZIP")

                # diary.csv
                cand = [n for n in names if n.endswith("/diary.csv") or n == "diary.csv"]
                if cand:
                    rows = open_csv(z, cand[0])
                    for r in rows:
                        logged_date  = r.get("Date") or None
                        film_name    = r.get("Name") or None
                        film_year    = to_int(r.get("Year"))
                        film_uri     = r.get("Letterboxd URI") or None
                        rating       = to_float(r.get("Rating"))
                        rewatch      = to_bool(r.get("Rewatch"))
                        tags         = (r.get("Tags") or None)
                        watched_date = r.get("Watched Date") or None
                        if not film_name: continue
                        cur.execute(
                          """INSERT INTO diary
                             (logged_date, film_name, film_year, film_uri, rating, rewatch, tags, watched_date)
                             VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                             ON DUPLICATE KEY UPDATE
                               film_uri     = VALUES(film_uri),
                               rating       = VALUES(rating),
                               rewatch      = VALUES(rewatch),
                               tags         = VALUES(tags),
                               watched_date = VALUES(watched_date)""",
                          (logged_date, film_name, film_year, film_uri, rating, rewatch, tags, watched_date)
                        )
                        ins_diary += 1
                else:
                    log_to_db(PROJECT_NAME, "WARNING", "⚠️  diary.csv not found in ZIP")

                log_to_db(PROJECT_NAME, "INFO", f"✅ Upserted rows → watchlist={ins_watchlist}, watched={ins_watched}, diary={ins_diary}")

        log_to_db(PROJECT_NAME, "INFO", "✔️  Load complete.")
    except Exception as e:
        log_to_db(PROJECT_NAME, "ERROR", f"❌ Loader failed: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
