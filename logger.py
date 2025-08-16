import os
import pymysql
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

def get_log_conn():
    return pymysql.connect(
        host=os.getenv("LOG_DB_HOST"),
        port=int(os.getenv("LOG_DB_PORT", 3306)),
        user=os.getenv("LOG_DB_USER"),
        password=os.getenv("LOG_DB_PASS"),
        database=os.getenv("LOG_DB_NAME"),
        autocommit=True
    )

def log_to_db(project, level, message):
    """Log to DB and console"""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")
    print(f"[{ts}] {project} {level}: {message}")  # Console

    try:
        conn = get_log_conn()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO logs (project_name, log_level, message) VALUES (%s, %s, %s)",
                (project, level, message)
            )
    except Exception as e:
        print(f"[{ts}] {project} ERROR: Failed to log to DB: {e}")
