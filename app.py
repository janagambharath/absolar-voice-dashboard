"""AB Solar — client dashboard for the Priya voice agent (v2).

What it does:
  - Shows ad-campaign leads (Facebook leadgen webhook + manual add)
  - Shows every outbound call: status, duration, outcome, cost (+ per-call
    provider cost breakdown)
  - Call detail: full transcript, recording playback
  - Stats row: leads, calls, connected, qualified, spend
  - Billing: Speko balance/credits + where the money goes (provider split)
  - Agent card: Priya's config + caller-ID number

Speed design (this is the v2 fix):
  - Page loads serve instantly from the local DB / cache — NOTHING on the
    critical path waits for Speko.
  - Speko sync runs in the BACKGROUND: one sessions-list call, then only
    NEW sessions are enriched, in parallel (10-way). Old code did up to
    3 sequential calls per session on every page load — that was the
    minutes-long "loading" state.
  - Recording presence comes from the session detail's recordingStatus —
    no extra per-session recording probe.

Run locally:
  .venv/bin/uvicorn app:app --port 8000
Env:
  SPEKO_API_KEY    Speko platform key (server-side only, never in frontend)
  SPEKO_AGENT_ID   Priya solar agent id (agent_c0b517ff8530400f) for auto-dial
  DEMO_MODE        1 = serve clearly-labeled demo data (default when no key)
  AUTO_DIAL        1 = dial new Facebook leads automatically (default 0)
  FB_VERIFY_TOKEN  token for the Facebook webhook handshake
  DB_PATH          sqlite path (default ./dashboard.db) — ignored when
                   DATABASE_URL is set
  DATABASE_URL     Postgres connection string (e.g. Supabase). When set, the
                   app uses Postgres instead of sqlite — this is the
                   production path on Render's free tier (no disks there).
"""
import asyncio
import base64
import hmac
import io
import json
import math
import os
import struct
import time
import wave
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import (
    HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse,
    RedirectResponse,
)

from db import db, init_db, insert_lead, insert_call_ignore, kv_get, kv_set, Q

BASE_DIR = Path(__file__).parent
SPEKO_API_KEY = os.environ.get("SPEKO_API_KEY", "")
SPEKO_AGENT_ID = os.environ.get("SPEKO_AGENT_ID", "")
SPEKO_BASE = "https://api.speko.dev"
DEMO_MODE = os.environ.get("DEMO_MODE", "1" if not SPEKO_API_KEY else "0") == "1"
AUTO_DIAL = os.environ.get("AUTO_DIAL", "0") == "1"
FB_VERIFY_TOKEN = os.environ.get("FB_VERIFY_TOKEN", "absolar-dev")
# Production login: set both env vars and every route (except /api/health)
# requires the username/password. Leave unset for open demo mode.
DASH_USER = os.environ.get("DASH_USER", "")
DASH_PASS = os.environ.get("DASH_PASS", "")

USD_INR = 88  # approximate; display only
SYNC_CONCURRENCY = 10
BILLING_CACHE_S = 900      # 15 min
AGENT_CACHE_S = 3600       # 1 h

app = FastAPI(title="AB Solar — Voice Agent Dashboard")


@asynccontextmanager
async def lifespan(app):
    if not DEMO_MODE and SPEKO_API_KEY:
        # warm the cache in the background; page loads never wait for this
        asyncio.create_task(refresh_calls_from_speko())
        asyncio.create_task(refresh_billing_cache())
    yield


app.router.lifespan_context = lifespan


@app.middleware("http")
async def basic_auth(request: Request, call_next):
    if not DASH_USER or request.url.path == "/api/health":
        return await call_next(request)
    ok = False
    auth = request.headers.get("authorization", "")
    if auth.startswith("Basic "):
        try:
            u, p = base64.b64decode(auth[6:]).decode().split(":", 1)
            ok = hmac.compare_digest(u, DASH_USER) and hmac.compare_digest(p, DASH_PASS)
        except Exception:
            pass
    if not ok:
        return PlainTextResponse(
            "Login required", 401,
            {"WWW-Authenticate": 'Basic realm="AB Solar dashboard"'})
    return await call_next(request)


# ---------------------------------------------------------------- db ----
init_db()


def digits(phone: str) -> str:
    return "".join(c for c in (phone or "") if c.isdigit())


def lead_by_phone(phone: str):
    d = digits(phone)
    if len(d) < 10:
        return None
    tail = d[-10:]
    con = db()
    row = con.execute(
        "SELECT * FROM leads ORDER BY id DESC"
    ).fetchall()
    con.close()
    for r in row:
        if digits(r["phone"])[-10:] == tail:
            return dict(r)
    return None


# ------------------------------------------------------------ demo ------
DEMO_LEADS = [
    ("Bharath", "+917995854994", "facebook", "Rooftop Solar – Miyapur"),
    ("Srinivas Rao", "+919848012345", "facebook", "Rooftop Solar – Miyapur"),
    ("Lakshmi Garu", "+919849876543", "facebook", "Rooftop Solar – Kukatpally"),
    ("Venkatesh", "+917702345678", "facebook", "Rooftop Solar – Miyapur"),
    ("Anitha", "+919391234567", "facebook", "Rooftop Solar – Nizampet"),
    ("Ramesh Kumar", "+918008765432", "facebook", "Rooftop Solar – Bachupally"),
    ("Padma Sri", "+917893456789", "google", "Solar Subsidy – Hyderabad"),
    ("Kiran Reddy", "+919848112233", "facebook", "Rooftop Solar – Miyapur"),
]

DEMO_TALKS = {
    "qualified": [
        ("agent", "హలో... ఆ, {name} మాట్లాడుతున్నారా?"),
        ("lead", "అవునండి, నేనే."),
        ("agent", "నేను Sri Surya Solar నుండి Priya ని మాట్లాడుతున్నాను. మీరు Facebook లో enquiry చేసారు కదా. మీది independent house ఆ?"),
        ("lead", "అవునండి, independent house ఏ."),
        ("agent", "మీ monthly current bill ఎంత వస్తుంది అండి?"),
        ("lead", "నాలుగున్నర వేలు వస్తుంది."),
        ("agent", "మీ enquiry qualified అయింది. మా team site survey కోసం contact చేస్తారు. సరే {name}."),
    ],
    "no_answer": [],
    "not_interested": [
        ("agent", "హలో... ఆ, {name} మాట్లాడుతున్నారా?"),
        ("lead", "అవునండి."),
        ("agent", "నేను Sri Surya Solar నుండి Priya ని మాట్లాడుతున్నాను. మీరు Facebook లో enquiry చేసారు కదా."),
        ("lead", "నాకు interest లేదండి, వద్దు."),
        ("agent", "సరే అండి, thank you."),
    ],
    "callback": [
        ("agent", "హలో... ఆ, {name} మాట్లాడుతున్నారా?"),
        ("lead", "అవునండి."),
        ("agent", "నేను Sri Surya Solar నుండి Priya ని మాట్లాడుతున్నాను. మీరు enquiry చేసారు కదా."),
        ("lead", "అవునండి, కానీ ఇప్పుడు busy గా ఉన్నాను. సాయంత్రం చేయండి."),
        ("agent", "సరే అండి, సాయంత్రం మళ్ళీ call చేస్తాను."),
    ],
}


def demo_wav(path: Path, seconds: int = 4):
    """Tiny placeholder tone so the demo audio player has something to play."""
    if path.exists():
        return
    rate = 8000
    n = rate * seconds
    with wave.open(str(path), "w") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        for i in range(n):
            v = int(9000 * math.sin(2 * math.pi * 440 * i / rate)
                    * (1 - i / n))
            w.writeframes(struct.pack("<h", v))


def seed_demo():
    con = db()
    if con.execute("SELECT COUNT(*) c FROM leads").fetchone()["c"]:
        con.close()
        return
    now = datetime.now(timezone.utc).isoformat()
    outcomes = ["qualified", "qualified", "no_answer", "qualified",
                "callback", "not_interested", "no_answer", "qualified"]
    rec_dir = BASE_DIR / "static" / "demo_audio"
    rec_dir.mkdir(parents=True, exist_ok=True)
    for i, (name, phone, src, camp) in enumerate(DEMO_LEADS):
        lid = insert_lead(con, name=name, phone=phone, source=src,
                          campaign=camp, created_at=now, status="new")
        oc = outcomes[i]
        turns = [
            {"speaker": s, "text": t.format(name=name.split()[0])}
            for s, t in DEMO_TALKS[oc]
        ]
        dur = 45 + (i * 13) % 50 if oc != "no_answer" else 0
        status = "completed" if oc != "no_answer" else "no-answer"
        summary = {
            "qualified": "Lead qualified — independent house, bill ~₹4,500. Handed to team for site survey.",
            "no_answer": "No answer.",
            "not_interested": "Lead declined — not interested.",
            "callback": "Lead asked for an evening callback.",
        }[oc]
        rec = f"demo-{lid}.wav"
        if oc != "no_answer":
            demo_wav(rec_dir / rec)
        usage = json.dumps([
            {"provider": "speko", "metric": "session_seconds",
             "quantity": dur, "cost": round(dur * 0.00077, 4)},
        ])
        con.execute(
            f"INSERT INTO calls (id, lead_id, to_number, status, started_at,"
            f" duration_seconds, outcome, summary, transcript, cost_usd,"
            f" has_recording, demo, usage_json)"
            f" VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},1,{Q})",
            (f"demo-call-{lid}", lid, phone, status, now, dur, oc, summary,
             json.dumps(turns, ensure_ascii=False), round(dur * 0.00077, 4),
             1 if oc != "no_answer" else 0, usage),
        )
        con.execute(
            f"UPDATE leads SET status={Q} WHERE id={Q}",
            ({"qualified": "qualified", "callback": "callback",
              "not_interested": "not_interested"}.get(oc, "called"), lid),
        )
    con.commit()
    con.close()


if DEMO_MODE:
    seed_demo()


# ----------------------------------------------------------- speko ------
# Platform API: https://api.speko.dev  (sessions, transcripts, recordings)
# NOTE: https://router.speko.dev/v1 is Speko's *component* API
# (TTS/STT/LLM gateway) — it has no sessions routes. Do not point the
# dashboard at it; /v1/sessions only exists on api.speko.dev.
def speko():
    if not SPEKO_API_KEY:
        raise HTTPException(503, "Speko API key not configured")
    return httpx.Client(
        base_url=SPEKO_BASE,
        headers={"Authorization": f"Bearer {SPEKO_API_KEY}"},
        timeout=20,
    )


def speko_async():
    if not SPEKO_API_KEY:
        raise HTTPException(503, "Speko API key not configured")
    return httpx.AsyncClient(
        base_url=SPEKO_BASE,
        headers={"Authorization": f"Bearer {SPEKO_API_KEY}"},
        timeout=20.0,
    )


def norm_entries(entries):
    """Normalize transcript entries from GET /v1/sessions/{id}/transcript.

    Returns (lines, outcome): lines = [{"speaker", "text"}] and outcome is
    the agent's OUTCOME: line when it wrote one, else "".
    """
    lines, outcome = [], ""
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        text = str(e.get("text") or e.get("content") or e.get("message")
                   or e.get("transcript") or "").strip()
        if not text:
            continue
        if text.upper().startswith("OUTCOME:"):
            outcome = text.split(":", 1)[1].strip()
            continue
        src = str(e.get("source") or e.get("role") or e.get("speaker") or "")
        speaker = ("agent" if src.lower() in
                   ("agent", "assistant", "ai", "system") else "lead")
        lines.append({"speaker": speaker, "text": text})
    return lines, outcome


sync_running = False


def _skip_ids():
    try:
        return set(json.loads(kv_get("skip_ids") or "[]"))
    except Exception:
        return set()


def _add_skip_ids(ids):
    if not ids:
        return
    cur = _skip_ids() | set(ids)
    # cap the list so it never grows unbounded
    cur = set(list(cur)[-500:])
    kv_set("skip_ids", json.dumps(sorted(cur)))


async def _enrich_one(client, sem, s):
    """Fetch detail + transcript for one session, in parallel with others.

    Returns (sid, kind, payload) where kind is "ok" | "skip" | "error".
    "skip" = eval/reliability run or non-phone session — remembered so we
    never fetch it again.
    """
    sid = str(s.get("id") or "")
    async with sem:
        try:
            dr, tr = await asyncio.gather(
                client.get(f"/v1/sessions/{sid}"),
                client.get(f"/v1/sessions/{sid}/transcript"),
                return_exceptions=True)
        except Exception:
            return sid, "error", None
    if isinstance(dr, Exception) or getattr(dr, "status_code", 0) != 200:
        return sid, "error", None
    det = dr.json()
    md = det.get("metadata") or {}
    # skip eval/reliability runs: no phone leg at all
    if md.get("runKind") == "reliability" or md.get("gate") is True:
        return sid, "skip", None
    to = md.get("to") or md.get("dialedNumber") or ""
    is_phone = bool(s.get("phoneCall") or det.get("phoneCall"))
    if not is_phone and not to:
        return sid, "skip", None
    lines, outcome = [], ""
    if not isinstance(tr, Exception) and tr.status_code == 200:
        lines, outcome = norm_entries(tr.json().get("entries"))
    dur = det.get("durationSeconds") or s.get("durationSeconds") or 0
    if not outcome:
        outcome = ("connected" if (dur or 0) > 0
                   else (det.get("status") or s.get("status") or "unknown"))
    return sid, "ok", {
        "to": to,
        "status": det.get("status") or s.get("status") or "",
        "started_at": s.get("createdAt") or det.get("createdAt") or "",
        "duration": dur,
        "outcome": outcome,
        "transcript": json.dumps(lines, ensure_ascii=False),
        "cost": det.get("totalCostUsd") or 0,
        "has_recording": 1 if (det.get("recordingStatus") or "") == "ready" else 0,
        "usage": json.dumps(det.get("usage") or []),
    }


async def refresh_calls_from_speko():
    """Background sync: pull new sessions from Speko into the local db.

    Only the sessions list is fetched up-front (1 call); sessions already
    in the DB — or previously identified as evals/non-phone — are skipped.
    New sessions are enriched in parallel. Never blocks page loads.
    """
    global sync_running
    if DEMO_MODE or not SPEKO_API_KEY or sync_running:
        return {"skipped": True}
    sync_running = True
    try:
        async with speko_async() as c:
            r = await c.get("/v1/sessions", params={"limit": 100})
            if r.status_code != 200:
                return {"error": f"speko {r.status_code}"}
            sessions = r.json().get("entries") or []
        con = db()
        known = {row["id"] for row in
                 con.execute("SELECT id FROM calls").fetchall()}
        con.close()
        skipped = _skip_ids()
        todo = [s for s in sessions
                if isinstance(s, dict) and str(s.get("id") or "")
                and str(s["id"]) not in known and str(s["id"]) not in skipped]
        sem = asyncio.Semaphore(SYNC_CONCURRENCY)
        async with speko_async() as c:
            results = await asyncio.gather(
                *[_enrich_one(c, sem, s) for s in todo])
        new, skip = 0, []
        con = db()
        try:
            for sid, kind, p in results:
                if kind == "skip":
                    skip.append(sid)
                    continue
                if kind != "ok" or not p:
                    continue
                lead = lead_by_phone(p["to"])
                insert_call_ignore(
                    con, id=sid, lead_id=lead["id"] if lead else None,
                    to_number=p["to"], status=p["status"],
                    started_at=p["started_at"],
                    duration_seconds=p["duration"], demo=0)
                con.execute(
                    f"UPDATE calls SET outcome={Q}, transcript={Q},"
                    f" cost_usd={Q}, has_recording={Q}, to_number={Q},"
                    f" status={Q}, duration_seconds={Q}, usage_json={Q}"
                    f" WHERE id={Q}",
                    (p["outcome"], p["transcript"], p["cost"],
                     p["has_recording"], p["to"], p["status"],
                     p["duration"], p["usage"], sid),
                )
                new += 1
            con.commit()
        finally:
            con.close()
        _add_skip_ids(skip)
        now = datetime.now(timezone.utc).isoformat()
        kv_set("last_sync", now)
        kv_set("last_sync_new", str(new))
        return {"new": new, "skipped_evals": len(skip)}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:200]}"}
    finally:
        sync_running = False


METRIC_LABELS = {
    "session_seconds": "Phone line",
    "tts_characters": "Voice (TTS)",
    "stt_seconds": "Speech-to-text",
    "llm_tokens": "AI brain (LLM)",
    "post_call_analysis_tokens": "Call analysis",
}


def _shape_breakdown(entries):
    out = []
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        cost = e.get("cost") or 0
        out.append({
            "provider": e.get("provider") or "",
            "metric": e.get("metric") or "",
            "label": METRIC_LABELS.get(e.get("metric") or "",
                                      e.get("metric") or ""),
            "quantity": e.get("quantity") or 0,
            "cost_usd": cost,
            "cost_inr": round(cost * USD_INR, 2),
        })
    out.sort(key=lambda x: -x["cost_usd"])
    return out


async def refresh_billing_cache(force=False):
    """Fetch /v1/usage (balance + provider cost split) into the kv cache."""
    if DEMO_MODE or not SPEKO_API_KEY:
        return None
    try:
        ts = float(kv_get("billing_cache_ts") or 0)
    except Exception:
        ts = 0
    if not force and time.time() - ts < BILLING_CACHE_S:
        try:
            return json.loads(kv_get("billing_cache") or "null")
        except Exception:
            pass
    try:
        async with speko_async() as c:
            r = await c.get("/v1/usage")
            if r.status_code != 200:
                return None
            data = r.json()
        out = {
            "balance_usd": data.get("balanceUsd") or 0,
            "balance_inr": round((data.get("balanceUsd") or 0) * USD_INR, 2),
            "total_cost_usd": data.get("totalCost") or 0,
            "total_cost_inr": round((data.get("totalCost") or 0) * USD_INR, 2),
            "total_sessions": data.get("totalSessions") or 0,
            "total_minutes": data.get("totalMinutes") or 0,
            "breakdown": _shape_breakdown(data.get("breakdown")),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        kv_set("billing_cache", json.dumps(out))
        kv_set("billing_cache_ts", str(time.time()))
        return out
    except Exception:
        return None


async def get_agent_info():
    if DEMO_MODE or not SPEKO_AGENT_ID:
        return {"demo": True, "name": "Priya (demo)",
                "language": "Telugu", "voice": "demo",
                "updated_at": ""}
    try:
        ts = float(kv_get("agent_cache_ts") or 0)
    except Exception:
        ts = 0
    if time.time() - ts < AGENT_CACHE_S:
        try:
            return json.loads(kv_get("agent_cache") or "null")
        except Exception:
            pass
    try:
        async with speko_async() as c:
            r = await c.get(f"/v1/agents/{SPEKO_AGENT_ID}")
            if r.status_code != 200:
                return None
            a = r.json()
        intent = a.get("intent") or {}
        stack = (a.get("stackPreferences") or {}).get("allowedProviders") or {}
        out = {
            "name": a.get("name") or "",
            "language": {"te": "Telugu"}.get(intent.get("language"),
                                             intent.get("language") or ""),
            "voice": a.get("voice") or "",
            "llm": ", ".join(stack.get("llm") or []),
            "tts": ", ".join(stack.get("tts") or []),
            "endpointing_ms": ((a.get("turnHandling") or {})
                               .get("endpointing") or {}).get("maxDelay"),
            "updated_at": a.get("updatedAt") or "",
        }
        kv_set("agent_cache", json.dumps(out))
        kv_set("agent_cache_ts", str(time.time()))
        return out
    except Exception:
        return None


async def get_phone_numbers():
    if DEMO_MODE:
        return [{"e164": "+91 98765 43210 (demo)", "source": "demo"}]
    try:
        ts = float(kv_get("numbers_cache_ts") or 0)
    except Exception:
        ts = 0
    if time.time() - ts < AGENT_CACHE_S:
        try:
            return json.loads(kv_get("numbers_cache") or "null")
        except Exception:
            pass
    try:
        async with speko_async() as c:
            r = await c.get("/v1/phone-numbers")
            if r.status_code != 200:
                return []
            out = [{"e164": n.get("e164") or "",
                    "source": n.get("source") or ""}
                   for n in (r.json() or []) if isinstance(n, dict)]
        kv_set("numbers_cache", json.dumps(out))
        kv_set("numbers_cache_ts", str(time.time()))
        return out
    except Exception:
        return []


# -------------------------------------------------------------- api ------
@app.get("/", response_class=HTMLResponse)
def index():
    return (BASE_DIR / "static" / "index.html").read_text()


@app.get("/api/health")
def health():
    return {"ok": True, "demo_mode": DEMO_MODE,
            "speko_configured": bool(SPEKO_API_KEY),
            "auto_dial": AUTO_DIAL,
            "sync_running": sync_running,
            "last_sync": kv_get("last_sync")}


def bucket(outcome: str, summary: str) -> str:
    s = f"{outcome} {summary}".lower()
    if "qualif" in s:
        return "qualified"
    if "not interested" in s or "declined" in s or "not_interested" in s:
        return "not_interested"
    if "callback" in s or "busy" in s:
        return "callback"
    if "no answer" in s or "no-answer" in s:
        return "no_answer"
    return "other"


@app.get("/api/stats")
def stats():
    # NOTE: no Speko sync here — page loads must stay instant. The client
    # triggers POST /api/sync in the background after first paint.
    con = db()
    leads = con.execute("SELECT COUNT(*) c FROM leads").fetchone()["c"]
    calls = con.execute("SELECT * FROM calls").fetchall()
    con.close()
    n = len(calls)
    connected = sum(1 for c in calls if (c["duration_seconds"] or 0) > 0)
    qualified = sum(1 for c in calls
                    if bucket(c["outcome"] or "", c["summary"] or "") == "qualified")
    dur = [c["duration_seconds"] or 0 for c in calls if (c["duration_seconds"] or 0) > 0]
    spend_usd = sum(c["cost_usd"] or 0 for c in calls)
    return {
        "leads": leads, "calls": n, "connected": connected,
        "qualified": qualified,
        "avg_duration_s": round(sum(dur) / len(dur)) if dur else 0,
        "spend_inr": round(spend_usd * USD_INR, 2),
        "demo_mode": DEMO_MODE,
    }


@app.get("/api/billing")
async def billing():
    """Speko balance/credits + provider cost split. Served from a 15-min
    cache; the underlying /v1/usage call is a single fast request."""
    if DEMO_MODE or not SPEKO_API_KEY:
        return {
            "demo": True,
            "balance_usd": 142.0, "balance_inr": round(142.0 * USD_INR, 2),
            "total_cost_usd": 8.5, "total_cost_inr": round(8.5 * USD_INR, 2),
            "total_sessions": 8, "total_minutes": 6,
            "breakdown": [
                {"provider": "speko", "metric": "session_seconds",
                 "label": "Phone line", "quantity": 360,
                 "cost_usd": 7.9, "cost_inr": round(7.9 * USD_INR, 2)},
                {"provider": "cartesia", "metric": "tts_characters",
                 "label": "Voice (TTS)", "quantity": 9000,
                 "cost_usd": 0.4, "cost_inr": round(0.4 * USD_INR, 2)},
                {"provider": "openai", "metric": "llm_tokens",
                 "label": "AI brain (LLM)", "quantity": 20000,
                 "cost_usd": 0.2, "cost_inr": round(0.2 * USD_INR, 2)},
            ],
        }
    cached = await refresh_billing_cache()
    if cached:
        return cached
    raise HTTPException(502, "billing unavailable")


@app.get("/api/agent")
async def agent():
    info = await get_agent_info()
    if info:
        return info
    raise HTTPException(502, "agent info unavailable")


@app.get("/api/numbers")
async def numbers():
    return await get_phone_numbers()


@app.get("/api/usage/daily")
def usage_daily():
    """Spend + call counts per day for the last 14 days (from local DB)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    con = db()
    rows = con.execute(
        f"SELECT substr(started_at,1,10) AS d, COUNT(*) AS n,"
        f" SUM(cost_usd) AS c FROM calls"
        f" WHERE demo=0 AND started_at >= {Q} GROUP BY d",
        (cutoff,)).fetchall()
    con.close()
    by_day = {r["d"]: {"calls": r["n"], "cost_usd": r["c"] or 0}
              for r in rows}
    days = []
    today = datetime.now(timezone.utc).date()
    for i in range(13, -1, -1):
        d = (today - timedelta(days=i)).isoformat()
        e = by_day.get(d, {"calls": 0, "cost_usd": 0})
        days.append({"day": d, "calls": e["calls"],
                     "cost_inr": round(e["cost_usd"] * USD_INR, 2)})
    return days


@app.post("/api/sync")
async def trigger_sync():
    """Start a background Speko sync; returns immediately."""
    if DEMO_MODE or not SPEKO_API_KEY:
        return {"started": False, "demo": True}
    if sync_running:
        return {"started": False, "running": True}
    asyncio.create_task(refresh_calls_from_speko())
    asyncio.create_task(refresh_billing_cache(force=True))
    return {"started": True}


@app.get("/api/sync/status")
def sync_status():
    return {"running": sync_running,
            "last_sync": kv_get("last_sync"),
            "last_sync_new": kv_get("last_sync_new")}


@app.get("/api/leads")
def leads():
    con = db()
    rows = con.execute(
        "SELECT * FROM leads ORDER BY id DESC").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        last = con.execute(
            f"SELECT outcome, summary, started_at, status FROM calls"
            f" WHERE lead_id={Q} ORDER BY started_at DESC LIMIT 1",
            (r["id"],)).fetchone()
        d["last_outcome"] = (last["outcome"] if last else "") or ""
        d["last_call_at"] = (last["started_at"] if last else "") or ""
        out.append(d)
    con.close()
    return out


@app.get("/api/calls")
def call_list():
    # NOTE: no blocking Speko sync — instant from DB.
    con = db()
    rows = con.execute(
        "SELECT c.*, l.name AS lead_name FROM calls c"
        " LEFT JOIN leads l ON l.id = c.lead_id"
        " ORDER BY c.started_at DESC, c.id DESC").fetchall()
    con.close()
    out = []
    for r in rows:
        d = dict(r)
        d.pop("transcript", None)  # list view doesn't need full transcripts
        out.append(d)
    return out


@app.get("/api/calls/{call_id}")
def call_detail(call_id: str):
    con = db()
    r = con.execute(
        f"SELECT c.*, l.name AS lead_name, l.phone AS lead_phone FROM calls c"
        f" LEFT JOIN leads l ON l.id = c.lead_id WHERE c.id={Q}",
        (call_id,)).fetchone()
    con.close()
    if not r:
        raise HTTPException(404, "call not found")
    d = dict(r)
    try:
        d["transcript"] = json.loads(d["transcript"] or "[]")
    except Exception:
        d["transcript"] = []
    try:
        d["usage"] = json.loads(d.get("usage_json") or "[]")
    except Exception:
        d["usage"] = []
    d.pop("usage_json", None)
    if not d["transcript"] and not d["demo"] and SPEKO_API_KEY \
            and not DEMO_MODE:
        # live fallback: pull the transcript straight from Speko
        try:
            with speko() as c:
                t = c.get(f"/v1/sessions/{call_id}/transcript")
                if t.status_code == 200:
                    lines, outcome = norm_entries(t.json().get("entries"))
                    d["transcript"] = lines
                    if outcome:
                        d["outcome"] = outcome
        except Exception:
            pass
    return d


@app.get("/api/calls/{call_id}/recording")
def recording(call_id: str):
    con = db()
    r = con.execute(f"SELECT demo, lead_id FROM calls WHERE id={Q}",
                    (call_id,)).fetchone()
    con.close()
    if not r:
        raise HTTPException(404, "call not found")
    if r["demo"]:
        path = BASE_DIR / "static" / "demo_audio" / f"demo-{r['lead_id']}.wav"
        if not path.exists():
            raise HTTPException(404, "no recording")
        return StreamingResponse(open(path, "rb"), media_type="audio/wav")
    with speko() as c:
        upstream = c.get(f"/v1/sessions/{call_id}/recording")
    if upstream.status_code != 200:
        raise HTTPException(502, "recording unavailable")
    url = (upstream.json() or {}).get("url", "")
    if not url:
        raise HTTPException(502, "recording unavailable")
    # Speko returns a signed storage URL — redirect instead of proxying.
    return RedirectResponse(url, status_code=302)


@app.post("/api/leads")
async def add_lead(req: Request):
    body = await req.json()
    phone = body.get("phone", "")
    if len(digits(phone)) < 10:
        raise HTTPException(400, "valid phone required")
    now = datetime.now(timezone.utc).isoformat()
    con = db()
    lid = insert_lead(con, name=body.get("name", ""), phone=phone,
                      source=body.get("source", "manual"),
                      campaign=body.get("campaign", ""), created_at=now)
    con.commit()
    con.close()
    dial = None
    if AUTO_DIAL and not DEMO_MODE:
        ok, msg = dial_lead(lid, phone, body.get("name", ""))
        dial = {"ok": ok, "message": msg}
    return {"id": lid, "dial": dial}


def dial_lead(lead_id: int, phone: str, name: str):
    """Place the Priya outbound call for a lead. Returns (ok, message)."""
    if not SPEKO_AGENT_ID:
        return False, "SPEKO_AGENT_ID not configured"
    to = phone if phone.startswith("+") else f"+91{digits(phone)[-10:]}"
    try:
        with speko() as c:
            r = c.post("/v1/sessions/phone", json={
                "to": to,
                "agentId": SPEKO_AGENT_ID,
                "metadata": {"lead_id": lead_id, "name": name,
                             "source": "dashboard"},
            })
        if r.status_code in (200, 201, 202):
            return True, f"call placed to {to}"
        return False, f"speko {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"dial failed: {e}"


# --------------------------------------------- facebook lead webhook -----
@app.get("/api/webhooks/facebook")
def fb_verify(hub_mode: str = "", hub_challenge: str = "",
              hub_verify_token: str = ""):
    if hub_mode == "subscribe" and hub_verify_token == FB_VERIFY_TOKEN:
        return PlainTextResponse(hub_challenge)
    raise HTTPException(403, "verification failed")


@app.post("/api/webhooks/facebook")
async def fb_lead(req: Request):
    """Receives Meta leadgen webhooks. Stores the raw payload as a lead
    needing enrichment (a production setup exchanges leadgen_id for the
    lead's name/phone via the Graph API with a Page token)."""
    body = await req.json()
    now = datetime.now(timezone.utc).isoformat()
    con = db()
    ids = []
    try:
        for entry in body.get("entry", []):
            for ch in entry.get("changes", []):
                v = ch.get("value", {})
                lgid = v.get("leadgen_id", "")
                ids.append(insert_lead(
                    con, name="", phone="", source="facebook",
                    campaign=v.get("form_id", ""), created_at=now,
                    status="needs_enrichment",
                    notes=f"leadgen_id={lgid} page_id={v.get('page_id','')}"))
        con.commit()
    finally:
        con.close()
    return {"ok": True, "leads": ids}


app.mount("/static", __import__("fastapi.staticfiles", fromlist=["StaticFiles"]).StaticFiles(
    directory=str(BASE_DIR / "static")), name="static")
