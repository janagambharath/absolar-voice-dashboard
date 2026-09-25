"""Database layer: sqlite locally, Postgres (Supabase) in production.

Set DATABASE_URL to use Postgres; otherwise falls back to sqlite at DB_PATH.
All timestamps are stored as ISO-861 TEXT in both dialects, and every query
goes through the Q placeholder constant, so the rest of the app is identical
on either backend.
"""
import os
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "dashboard.db"))
DATABASE_URL = os.environ.get("DATABASE_URL", "")

USE_PG = bool(DATABASE_URL)
Q = "%s" if USE_PG else "?"


def db():
    """Fresh connection per call (safe for FastAPI's threadpool)."""
    if USE_PG:
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(DATABASE_URL, row_factory=dict_row)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


_LEADS_DDL = """CREATE TABLE IF NOT EXISTS leads (
    id {pk},
    name TEXT, phone TEXT NOT NULL, source TEXT DEFAULT 'manual',
    campaign TEXT DEFAULT '', created_at TEXT NOT NULL,
    status TEXT DEFAULT 'new', notes TEXT DEFAULT ''
)"""
_CALLS_DDL = """CREATE TABLE IF NOT EXISTS calls (
    id TEXT PRIMARY KEY, lead_id INTEGER, to_number TEXT,
    status TEXT, started_at TEXT, duration_seconds INTEGER DEFAULT 0,
    outcome TEXT DEFAULT '', summary TEXT DEFAULT '',
    transcript TEXT DEFAULT '[]', cost_usd REAL DEFAULT 0,
    has_recording INTEGER DEFAULT 0, demo INTEGER DEFAULT 0
)"""


def init_db():
    pk = "SERIAL PRIMARY KEY" if USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
    con = db()
    # one statement per execute: psycopg does not take multi-statement scripts
    con.execute(_LEADS_DDL.format(pk=pk))
    con.execute(_CALLS_DDL)
    con.commit()
    con.close()


def insert_lead(con, *, name="", phone="", source="manual", campaign="",
               created_at, status="new", notes=""):
    """Insert a lead, return its id (RETURNING on pg, lastrowid on sqlite)."""
    if USE_PG:
        cur = con.execute(
            f"INSERT INTO leads (name, phone, source, campaign, created_at,"
            f" status, notes) VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q}) RETURNING id",
            (name, phone, source, campaign, created_at, status, notes))
        return cur.fetchone()["id"]
    cur = con.execute(
        "INSERT INTO leads (name, phone, source, campaign, created_at,"
        " status, notes) VALUES (?,?,?,?,?,?,?)",
        (name, phone, source, campaign, created_at, status, notes))
    return cur.lastrowid


def insert_call_ignore(con, *, id, lead_id=None, to_number="", status="",
                       started_at="", duration_seconds=0, demo=0):
    """Insert a call row, silently skipping duplicates."""
    if USE_PG:
        con.execute(
            f"INSERT INTO calls (id, lead_id, to_number, status, started_at,"
            f" duration_seconds, demo)"
            f" VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q})"
            f" ON CONFLICT (id) DO NOTHING",
            (id, lead_id, to_number, status, started_at,
             duration_seconds, demo))
    else:
        con.execute(
            "INSERT OR IGNORE INTO calls (id, lead_id, to_number, status,"
            " started_at, duration_seconds, demo) VALUES (?,?,?,?,?,?,?)",
            (id, lead_id, to_number, status, started_at,
             duration_seconds, demo))
