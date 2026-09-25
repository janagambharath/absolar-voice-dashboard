"""AB Solar — client dashboard for the Priya voice agent.

What it does:
  - Shows ad-campaign leads (Facebook leadgen webhook + manual add)
  - Shows every outbound call: status, duration, outcome, cost
  - Call detail: AI summary, full transcript, recording playback
  - Stats row: leads, calls, connected, qualified, spend

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
import io
import base64
import hmac
import json
import math
import os
import struct
import wave
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import (
    HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse,
    RedirectResponse,
)

from db import db, init_db, insert_lead, insert_call_ignore, Q

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

app = FastAPI(title="AB Solar — Voice Agent Dashboard")


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
        ("lead", "ఎవరు?"),
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
        con.execute(
            f"INSERT INTO calls (id, lead_id, to_number, status, started_at,"
            f" duration_seconds, outcome, summary, transcript, cost_usd,"
            f" has_recording, demo)"
            f" VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},1)",
            (f"demo-call-{lid}", lid, phone, status, now, dur, oc, summary,
             json.dumps(turns, ensure_ascii=False), round(dur * 0.00077, 4),
             1 if oc != "no_answer" else 0),
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


def refresh_calls_from_speko():
    """Pull recent sessions from Speko into the local db (best-effort).

    The list is one API call; each session not already enriched costs one
    detail fetch (dialed number, cost) + one transcript fetch (OUTCOME).
    """
    if DEMO_MODE or not SPEKO_API_KEY:
        return
    try:
        with speko() as c:
            r = c.get("/v1/sessions", params={"limit": 100})
            if r.status_code != 200:
                return
            sessions = r.json().get("entries") or []
            con = db()
            try:
                for s in sessions:
                    if not isinstance(s, dict):
                        continue
                    cid = str(s.get("id") or "")
                    if not cid:
                        continue
                    exists = con.execute(
                        f"SELECT to_number, transcript FROM calls WHERE id={Q}",
                        (cid,)).fetchone()
                    if (exists and exists["to_number"]
                            and exists["transcript"] not in ("", "[]")):
                        continue  # already enriched
                    to, cost = "", 0.0
                    dur = s.get("durationSeconds") or 0
                    status = s.get("status") or ""
                    is_phone = bool(s.get("phoneCall"))
                    try:
                        d = c.get(f"/v1/sessions/{cid}")
                        if d.status_code == 200:
                            det = d.json()
                            md = det.get("metadata") or {}
                            # skip eval/reliability runs: no phone leg at all
                            if (md.get("runKind") == "reliability"
                                    or md.get("gate") is True):
                                continue
                            to = (md.get("to") or md.get("dialedNumber") or "")
                            cost = det.get("totalCostUsd") or 0
                            dur = det.get("durationSeconds") or dur
                            status = det.get("status") or status
                            is_phone = is_phone or bool(det.get("phoneCall"))
                    except Exception:
                        pass
                    if not is_phone and not to:
                        continue  # not a real phone call; don't store
                    lead = lead_by_phone(to)
                    insert_call_ignore(
                        con, id=cid, lead_id=lead["id"] if lead else None,
                        to_number=to, status=status,
                        started_at=s.get("createdAt") or "",
                        duration_seconds=dur, demo=0)
                    lines, outcome = [], ""
                    try:
                        t = c.get(f"/v1/sessions/{cid}/transcript")
                        if t.status_code == 200:
                            lines, outcome = norm_entries(
                                t.json().get("entries"))
                    except Exception:
                        pass
                    if not outcome:
                        outcome = ("connected" if (dur or 0) > 0
                                   else (status or "unknown"))
                    has_rec = 0
                    try:
                        rec = c.get(f"/v1/sessions/{cid}/recording")
                        has_rec = 1 if rec.status_code == 200 else 0
                    except Exception:
                        pass
                    con.execute(
                        f"UPDATE calls SET outcome={Q}, transcript={Q},"
                        f" cost_usd={Q}, has_recording={Q}, to_number={Q},"
                        f" status={Q}, duration_seconds={Q} WHERE id={Q}",
                        (outcome, json.dumps(lines, ensure_ascii=False),
                         cost, has_rec, to, status, dur, cid),
                    )
                con.commit()
            finally:
                con.close()
    except Exception:
        pass


# -------------------------------------------------------------- api ------
@app.get("/", response_class=HTMLResponse)
def index():
    return (BASE_DIR / "static" / "index.html").read_text()


@app.get("/api/health")
def health():
    return {"ok": True, "demo_mode": DEMO_MODE,
            "speko_configured": bool(SPEKO_API_KEY),
            "auto_dial": AUTO_DIAL}


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
    refresh_calls_from_speko()
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
        "spend_inr": round(spend_usd * 88, 2),
        "demo_mode": DEMO_MODE,
    }


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
    refresh_calls_from_speko()
    con = db()
    rows = con.execute(
        "SELECT c.*, l.name AS lead_name FROM calls c"
        " LEFT JOIN leads l ON l.id = c.lead_id"
        " ORDER BY c.started_at DESC, c.id DESC").fetchall()
    con.close()
    return [dict(r) for r in rows]


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
