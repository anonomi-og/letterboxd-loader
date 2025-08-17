import os
import sys
import logging
import sqlite3
from datetime import datetime

import requests

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S%z",
)
log = logging.getLogger("lbx-justwatch")

DB_PATH = os.getenv("DB_PATH", "/data/letterboxd.db")
COUNTRY = os.getenv("JW_COUNTRY", "GB")
SOURCE = os.getenv("JW_SOURCE", "WATCHLIST").upper()  # WATCHLIST or DIARY

TMDB_API_KEY = os.getenv("TMDB_API_KEY")


def tmdb_search(title, year=None):
    """Search TMDB for a title and optional year."""
    if not TMDB_API_KEY:
        return None

    url = "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": TMDB_API_KEY, "query": title}
    if year:
        params["year"] = year

    r = requests.get(url, params=params)
    if r.status_code != 200:
        return None

    data = r.json().get("results", [])
    return data[0] if data else None


def fetch_rows(conn, source):
    """Fetch rows from Letterboxd tables depending on source."""
    cur = conn.cursor()
    if source == "WATCHLIST":
        cur.execute("SELECT entry_id, name, year FROM watchlist ORDER BY rowid")
    else:  # DIARY
        cur.execute("SELECT diary_id, name, year FROM diary ORDER BY diary_id")
    return cur.fetchall()


def main():
    log.info(f"Source={SOURCE}, COUNTRY={COUNTRY}")

    conn = sqlite3.connect(DB_PATH)
    rows = fetch_rows(conn, SOURCE)
    log.info(f"rows={len(rows)}")

    results = []
    for idx, row in enumerate(rows, start=1):
        if SOURCE == "WATCHLIST":
            row_id, title, year = row
        else:  # DIARY
            row_id, title, year = row

        log.info(f"[{idx}/{len(rows)}] {SOURCE}:{row_id} — {title} ({year})")

        match = tmdb_search(title, year)
        if not match or not match.get("id"):
            log.warning(f"No TMDB id for {title}")
            tmdb_id = None
        else:
            tmdb_id = match["id"]

        results.append((row_id, SOURCE, title, tmdb_id, year))

    # Write results back into a simple table
    conn.execute(
        """CREATE TABLE IF NOT EXISTS jw_map (
            row_id TEXT,
            source TEXT,
            title TEXT,
            tmdb_id INTEGER,
            year INTEGER,
            updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    conn.executemany(
        "INSERT INTO jw_map (row_id, source, title, tmdb_id, year) VALUES (?,?,?,?,?)",
        results,
    )
    conn.commit()
    conn.close()

    log.info("✔️ JustWatch mapping complete.")


if __name__ == "__main__":
    sys.exit(main())
