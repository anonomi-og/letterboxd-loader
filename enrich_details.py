import os, time, json
import requests
import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv
from logger import log_to_db

PROJECT = "lbx-enrich"
load_dotenv()

# ---- Config ----
TMDB_API_KEY = os.getenv("TMDB_API_KEY")  # REQUIRED
OMDB_API_KEY = os.getenv("OMDB_API_KEY")  # optional

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

BATCH_LIMIT   = int(os.getenv("ENRICH_BATCH_LIMIT", "300"))  # how many jw_title_map rows per run
SLEEP_SECONDS = float(os.getenv("ENRICH_SLEEP_SECONDS", "0.35"))  # be nice to TMDb

# ---- SQL ----
SQL_SELECT_TARGETS = f"""
SELECT source, source_row_id, entry_id, matched_title, matched_year, matched_type
FROM jw_title_map
WHERE film_id IS NULL
ORDER BY source, source_row_id
LIMIT {BATCH_LIMIT};
"""

SQL_UPSERT_DETAILS = """
INSERT INTO film_details
(type,title,original_title,year,release_date,imdb_id,tmdb_id,jw_entry_id,
 genres_json,runtime_min,countries_json,languages_json,directors_json,cast_json,
 poster_url,backdrop_url,tmdb_vote_avg,tmdb_vote_count,box_office_usd)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  type=VALUES(type),
  title=VALUES(title),
  original_title=VALUES(original_title),
  year=VALUES(year),
  release_date=VALUES(release_date),
  imdb_id=VALUES(imdb_id),
  tmdb_id=VALUES(tmdb_id),
  jw_entry_id=VALUES(jw_entry_id),
  genres_json=VALUES(genres_json),
  runtime_min=VALUES(runtime_min),
  countries_json=VALUES(countries_json),
  languages_json=VALUES(languages_json),
  directors_json=VALUES(directors_json),
  cast_json=VALUES(cast_json),
  poster_url=VALUES(poster_url),
  backdrop_url=VALUES(backdrop_url),
  tmdb_vote_avg=VALUES(tmdb_vote_avg),
  tmdb_vote_count=VALUES(tmdb_vote_count),
  box_office_usd=VALUES(box_office_usd);
"""

SQL_RESOLVE_FILM_ID = """
SELECT id FROM film_details
WHERE (imdb_id IS NOT NULL AND imdb_id=%s)
   OR (tmdb_id IS NOT NULL AND tmdb_id=%s)
   OR (jw_entry_id IS NOT NULL AND jw_entry_id=%s)
LIMIT 1;
"""

SQL_SET_MAP_FILM_ID = """
UPDATE jw_title_map
SET film_id=%s
WHERE source=%s AND source_row_id=%s;
"""

# ---- TMDb / OMDb helpers ----
TMDB_BASE = "https://api.themoviedb.org/3"

def tmdb_get(path, params=None):
    if not TMDB_API_KEY:
        raise RuntimeError("Set TMDB_API_KEY")
    p = {"api_key": TMDB_API_KEY}
    if params: p.update(params)
    r = requests.get(f"{TMDB_BASE}/{path.lstrip('/')}", params=p, timeout=20)
    r.raise_for_status()
    return r.json()

def tmdb_search(title, year, obj_type):
    media = "movie" if (obj_type or "").upper() == "MOVIE" else "tv"
    params = {"query": title}
    if year and media == "movie":
        params["year"] = year
    if year and media == "tv":
        params["first_air_date_year"] = year
    js = tmdb_get(f"/search/{media}", params)
    res = js.get("results") or []
    return res[0]["id"] if res else None, media

def tmdb_bundle(tmdb_id, media):
    core = tmdb_get(f"/{media}/{tmdb_id}", {"append_to_response": "external_ids,credits,images"})
    ext  = (core.get("external_ids") or {})
    imdb_id = ext.get("imdb_id")

    # basics
    title  = core.get("title") or core.get("name")
    otitle = core.get("original_title") or core.get("original_name")
    rdate  = core.get("release_date") or core.get("first_air_date")
    year   = int((rdate or "")[:4]) if (rdate or "")[:4].isdigit() else None

    # people
    crew = (core.get("credits") or {}).get("crew") or []
    if media == "movie":
        directors = [ {"id":p.get("id"), "name":p.get("name")} for p in crew if p.get("job") == "Director" ]
    else:
        directors = [ {"id":p.get("id"), "name":p.get("name")} for p in crew if "Director" in (p.get("job") or "") ]

    cast = (core.get("credits") or {}).get("cast") or []
    cast = [ {"id":c.get("id"), "name":c.get("name"), "character":c.get("character")} for c in cast[:10] ]

    # misc
    genres    = [g.get("name") for g in (core.get("genres") or [])]
    countries = [c.get("iso_3166_1") for c in (core.get("production_countries") or [])]
    langs     = [l.get("iso_639_1") for l in (core.get("spoken_languages") or [])]
    poster    = f"https://image.tmdb.org/t/p/w500{core['poster_path']}" if core.get("poster_path") else None
    backdrop  = f"https://image.tmdb.org/t/p/w780{core['backdrop_path']}" if core.get("backdrop_path") else None
    runtime   = core.get("runtime") or (core.get("episode_run_time") or [None])[0]
    vote_avg  = core.get("vote_average")
    vote_cnt  = core.get("vote_count")

    return dict(
        imdb_id=imdb_id, title=title, original_title=otitle, year=year, release_date=rdate,
        genres=genres, runtime_min=runtime, countries=countries, languages=langs,
        directors=directors, cast=cast, poster=poster, backdrop=backdrop,
        vote_avg=vote_avg, vote_count=vote_cnt
    )

def omdb_box_office(imdb_id):
    if not OMDB_API_KEY or not imdb_id:
        return None
    try:
        r = requests.get("https://www.omdbapi.com/", params={"apikey": OMDB_API_KEY, "i": imdb_id}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("Response") != "True":
            return None
        raw = (data.get("BoxOffice") or "").replace("$","").replace(",","").strip()
        return int(raw) if raw.isdigit() else None
    except Exception:
        return None

# ---- Core ----
def enrich_one(conn, row):
    src, src_id = row["source"], row["source_row_id"]
    title       = (row["matched_title"] or "").strip()
    year        = row.get("matched_year")
    obj_type    = (row.get("matched_type") or "MOVIE").upper()
    entry_id    = row.get("entry_id")

    if not title:
        log_to_db(PROJECT, "WARNING", f"Empty matched_title for {src}:{src_id}")
        return

    # 1) Find TMDb id
    tmdb_id, media = tmdb_search(title, year, obj_type)
    if not tmdb_id:
        log_to_db(PROJECT, "WARNING", f"No TMDb match: {title} ({year}) [{obj_type}]")
        return

    # 2) Fetch full bundle (and imdb id)
    b = tmdb_bundle(tmdb_id, media)

    # 3) Optional OMDb box office
    box_office = omdb_box_office(b.get("imdb_id"))

    # 4) Upsert into film_details
    with conn.cursor() as c:
        c.execute(SQL_UPSERT_DETAILS, (
            "MOVIE" if media == "movie" else "SHOW",
            b["title"], b["original_title"], b["year"], b["release_date"],
            b["imdb_id"], tmdb_id, entry_id,
            json.dumps(b["genres"], ensure_ascii=False),
            b["runtime_min"],
            json.dumps(b["countries"], ensure_ascii=False),
            json.dumps(b["languages"], ensure_ascii=False),
            json.dumps(b["directors"], ensure_ascii=False),
            json.dumps(b["cast"], ensure_ascii=False),
            b["poster"], b["backdrop"],
            b["vote_avg"], b["vote_count"],
            box_office
        ))

        # resolve id
        c.execute(SQL_RESOLVE_FILM_ID, (b["imdb_id"], tmdb_id, entry_id))
        film = c.fetchone()
        if not film:
            log_to_db(PROJECT, "ERROR", f"Upsert ok but SELECT id failed for {title}")
            return

        # 5) Backfill jw_title_map.film_id
        c.execute(SQL_SET_MAP_FILM_ID, (film["id"], src, src_id))

    log_to_db(PROJECT, "INFO", f"Enriched {src}:{src_id} → film_id {film['id']} ({b['title']})")

def main():
    if not TMDB_API_KEY:
        raise SystemExit("Set TMDB_API_KEY")

    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as c:
            c.execute(SQL_SELECT_TARGETS)
            rows = c.fetchall()

        log_to_db(PROJECT, "INFO", f"Targets: {len(rows)}")
        for i, r in enumerate(rows, 1):
            log_to_db(PROJECT, "INFO", f"[{i}/{len(rows)}] {r['source']}:{r['source_row_id']} – {r['matched_title']} ({r.get('matched_year')})")
            enrich_one(conn, r)
            time.sleep(SLEEP_SECONDS)

        log_to_db(PROJECT, "INFO", "✓ Enrichment complete")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
