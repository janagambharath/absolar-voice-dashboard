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
    has_recording INTEGER DEFAULT 0, demo INTEGER DEFAULT 0,
    usage_json TEXT DEFAULT '[]'
)"""
_KV_DDL = """CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY, value TEXT DEFAULT ''
)"""
_COMPANIES_DDL = """CREATE TABLE IF NOT EXISTS companies (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, slug TEXT DEFAULT '',
    logo_url TEXT DEFAULT '', primary_color TEXT DEFAULT '#15803D',
    speko_agent_id TEXT DEFAULT '', caller_id TEXT DEFAULT '',
    meta_page_token TEXT DEFAULT '', meta_autodial INTEGER DEFAULT 0,
    meta_daily_cap INTEGER DEFAULT 50,
    created_at TEXT NOT NULL
)"""
_TASKS_DDL = """CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY, company_id TEXT DEFAULT '', lead_id INTEGER,
    kind TEXT DEFAULT 'followup', title TEXT DEFAULT '', due_at TEXT DEFAULT '',
    done INTEGER DEFAULT 0, created_at TEXT NOT NULL
)"""
_NOTES_DDL = """CREATE TABLE IF NOT EXISTS notes (
    id TEXT PRIMARY KEY, company_id TEXT DEFAULT '', lead_id INTEGER,
    body TEXT DEFAULT '', created_at TEXT NOT NULL
)"""
_ACTIVITIES_DDL = """CREATE TABLE IF NOT EXISTS activities (
    id TEXT PRIMARY KEY, company_id TEXT DEFAULT '', lead_id INTEGER,
    kind TEXT DEFAULT 'note', title TEXT DEFAULT '', detail TEXT DEFAULT '',
    created_at TEXT NOT NULL
)"""

# v3 column migrations: (table, column, ddl)
_V3_COLS = [
    ("leads", "company_id", "TEXT DEFAULT ''"),
    ("leads", "stage", "TEXT DEFAULT 'new'"),
    ("leads", "score", "INTEGER DEFAULT 0"),
    ("leads", "language", "TEXT DEFAULT ''"),
    ("leads", "discom", "TEXT DEFAULT ''"),
    ("leads", "consumer_no", "TEXT DEFAULT ''"),
    ("leads", "property_type", "TEXT DEFAULT ''"),
    ("leads", "roof_ownership", "TEXT DEFAULT ''"),
    ("leads", "roof_type", "TEXT DEFAULT ''"),
    ("leads", "roof_area_sqft", "REAL DEFAULT 0"),
    ("leads", "monthly_bill_inr", "REAL DEFAULT 0"),
    ("leads", "monthly_units", "REAL DEFAULT 0"),
    ("leads", "system_size_kw", "REAL DEFAULT 0"),
    ("leads", "financing", "TEXT DEFAULT ''"),
    ("leads", "decision_maker", "TEXT DEFAULT ''"),
    ("leads", "timeline", "TEXT DEFAULT ''"),
    ("leads", "subsidy_aware", "TEXT DEFAULT ''"),
    ("leads", "dnc", "INTEGER DEFAULT 0"),
    ("leads", "lost_reason", "TEXT DEFAULT ''"),
    ("leads", "next_followup_at", "TEXT DEFAULT ''"),
    ("leads", "last_activity_at", "TEXT DEFAULT ''"),
    ("calls", "company_id", "TEXT DEFAULT ''"),
    ("calls", "structured_json", "TEXT DEFAULT '{}'"),
    ("calls", "talk_ratio", "REAL DEFAULT 0"),
    ("calls", "objections_json", "TEXT DEFAULT '[]'"),
    ("calls", "next_action", "TEXT DEFAULT ''"),
    ("calls", "disposition", "TEXT DEFAULT ''"),
    ("companies", "meta_page_token", "TEXT DEFAULT ''"),
    ("companies", "meta_autodial", "INTEGER DEFAULT 0"),
    ("companies", "meta_daily_cap", "INTEGER DEFAULT 50"),
]


def _add_col(con, table, col, ddl):
    try:
        if USE_PG:
            con.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {ddl}")
        else:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
    except Exception:
        pass


def init_db():
    pk = "SERIAL PRIMARY KEY" if USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
    con = db()
    # one statement per execute: psycopg does not take multi-statement scripts
    con.execute(_LEADS_DDL.format(pk=pk))
    con.execute(_CALLS_DDL)
    con.execute(_KV_DDL)
    con.execute(_COMPANIES_DDL)
    con.execute(_TASKS_DDL)
    con.execute(_NOTES_DDL)
    con.execute(_ACTIVITIES_DDL)
    # migrate: older DBs lack usage_json on calls
    try:
        con.execute("ALTER TABLE calls ADD COLUMN usage_json TEXT DEFAULT '[]'")
    except Exception:
        pass
    # v3 migrations
    for table, col, ddl in _V3_COLS:
        _add_col(con, table, col, ddl)
    con.commit()
    con.close()


def seed_company(con, *, id, name, speko_agent_id="", caller_id="",
                 primary_color="#15803D"):
    """Insert the company if missing; return its id."""
    now = __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc).isoformat()
    if USE_PG:
        con.execute(
            f"INSERT INTO companies (id, name, slug, primary_color,"
            f" speko_agent_id, caller_id, created_at)"
            f" VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q})"
            f" ON CONFLICT (id) DO NOTHING",
            (id, name, id, primary_color, speko_agent_id, caller_id, now))
    else:
        con.execute(
            "INSERT OR IGNORE INTO companies (id, name, slug, primary_color,"
            " speko_agent_id, caller_id, created_at) VALUES (?,?,?,?,?,?,?)",
            (id, name, id, primary_color, speko_agent_id, caller_id, now))
    return id


def backfill_company(con, company_id):
    """Point legacy rows (empty company_id) at the default company."""
    con.execute(f"UPDATE leads SET company_id={Q} WHERE company_id='' OR company_id IS NULL",
                (company_id,))
    con.execute(f"UPDATE calls SET company_id={Q} WHERE company_id='' OR company_id IS NULL",
                (company_id,))


def kv_get(key, default=""):
    con = db()
    r = con.execute(f"SELECT value FROM kv WHERE key={Q}",
                    (key,)).fetchone()
    con.close()
    return r["value"] if r and r["value"] is not None else default


def kv_set(key, value):
    con = db()
    if USE_PG:
        con.execute(
            f"INSERT INTO kv (key, value) VALUES ({Q},{Q})"
            f" ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
            (key, value))
    else:
        con.execute("INSERT OR REPLACE INTO kv (key, value) VALUES (?,?)",
                    (key, value))
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


# ------------------------------------------------------- migrations ------
_LEAD_COLS = [
    # (name, ddl)
    ("company_id", "TEXT DEFAULT ''"),
    ("stage", "TEXT DEFAULT 'new'"),
    ("lost_reason", "TEXT DEFAULT ''"),
    ("follow_up_at", "TEXT DEFAULT ''"),
    ("dnc", "INTEGER DEFAULT 0"),
    ("last_activity_at", "TEXT DEFAULT ''"),
    ("property_type", "TEXT DEFAULT ''"),
    ("roof_ownership", "TEXT DEFAULT ''"),
    ("roof_type", "TEXT DEFAULT ''"),
    ("roof_area_sqft", "REAL"),
    ("monthly_bill_inr", "REAL"),
    ("timeline", "TEXT DEFAULT ''"),
    ("decision_maker", "TEXT DEFAULT ''"),
    ("financing_interest", "TEXT DEFAULT ''"),
    ("subsidy_awareness", "TEXT DEFAULT ''"),
    ("discom", "TEXT DEFAULT ''"),
    ("notes_count", "INTEGER DEFAULT 0"),
    ("email", "TEXT DEFAULT ''"),
]

_CALL_COLS = [
    ("company_id", "TEXT DEFAULT ''"),
    ("outcome", "TEXT DEFAULT ''"),
    ("summary", "TEXT DEFAULT ''"),
    ("structured_json", "TEXT DEFAULT '{}'"),
    ("intent_verdict", "TEXT DEFAULT ''"),
    ("talk_ratio", "REAL"),
    ("objections_json", "TEXT DEFAULT '[]'"),
    ("next_action", "TEXT DEFAULT ''"),
    ("disposition", "TEXT DEFAULT ''"),
    ("has_recording", "INTEGER DEFAULT 0"),
]


def _add_cols(con, table, cols):
    # _add_col is already dialect-aware (PG: ADD COLUMN IF NOT EXISTS).
    for name, ddl in cols:
        _add_col(con, table, name, ddl)


def migrate():
    """Create v3 tables and add new columns; safe to run repeatedly."""
    con = db()
    pk = "SERIAL PRIMARY KEY" if USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
    try:
        # base tables (v1 schema) — created if missing
        con.execute(
            """CREATE TABLE IF NOT EXISTS leads (
                id {pk}, phone TEXT NOT NULL,
                name TEXT DEFAULT '', source TEXT DEFAULT '',
                status TEXT DEFAULT '', notes TEXT DEFAULT '',
                last_call_at TEXT DEFAULT '', created_at TEXT DEFAULT '',
                demo INTEGER DEFAULT 0, score INTEGER DEFAULT 0,
                campaign TEXT DEFAULT '')""".format(pk=pk))
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_phone"
                    " ON leads(phone)")
        con.execute(
            """CREATE TABLE IF NOT EXISTS calls (
                id TEXT PRIMARY KEY, lead_id INTEGER,
                to_number TEXT DEFAULT '', started_at TEXT DEFAULT '',
                duration_seconds INTEGER DEFAULT 0, cost_usd REAL DEFAULT 0,
                cost_breakdown_usd TEXT DEFAULT '', status TEXT DEFAULT '',
                outcome TEXT DEFAULT '', transcript TEXT DEFAULT '[]',
                usage_json TEXT DEFAULT '[]', demo INTEGER DEFAULT 0,
                has_recording INTEGER DEFAULT 0)""")
        con.execute(
            """CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY, value TEXT DEFAULT '')""")
        con.execute(
            """CREATE TABLE IF NOT EXISTS companies (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, slug TEXT DEFAULT '',
                logo_url TEXT DEFAULT '', primary_color TEXT DEFAULT '#15803D',
                speko_agent_id TEXT DEFAULT '', caller_id TEXT DEFAULT '',
                created_at TEXT DEFAULT '')""")
        con.execute(
            """CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY, company_id TEXT DEFAULT '',
                lead_id INTEGER, kind TEXT DEFAULT 'followup',
                title TEXT NOT NULL, due_at TEXT DEFAULT '',
                done INTEGER DEFAULT 0, created_at TEXT DEFAULT '',
                completed_at TEXT DEFAULT '')""")
        con.execute("CREATE INDEX IF NOT EXISTS ix_tasks_co"
                    " ON tasks(company_id, done, due_at)")
        con.execute(
            """CREATE TABLE IF NOT EXISTS notes (
                id TEXT PRIMARY KEY, company_id TEXT DEFAULT '',
                lead_id INTEGER, body TEXT NOT NULL,
                created_at TEXT DEFAULT '')""")
        con.execute(
            """CREATE TABLE IF NOT EXISTS activities (
                id TEXT PRIMARY KEY, company_id TEXT DEFAULT '',
                lead_id INTEGER, kind TEXT DEFAULT 'note',
                title TEXT NOT NULL, detail TEXT DEFAULT '',
                created_at TEXT DEFAULT '')""")
        con.execute("CREATE INDEX IF NOT EXISTS ix_act_lead"
                    " ON activities(lead_id, created_at)")
        _add_cols(con, "leads", _LEAD_COLS)
        _add_cols(con, "calls", _CALL_COLS)
        con.execute("CREATE INDEX IF NOT EXISTS ix_leads_co"
                    " ON leads(company_id, stage)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_calls_co"
                    " ON calls(company_id, started_at)")
        con.commit()
    finally:
        con.close()


migrate()
