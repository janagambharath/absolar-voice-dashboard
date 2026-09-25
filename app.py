"""REvorax Voice-AI CRM (v3) — multi-company client dashboard.

AB Solar is the first company; the schema and UI are built for many.

What it does:
  - Companies: every lead/call/task scoped to a company, switcher in UI.
  - Pipeline CRM: stages (New -> Contacting -> Qualified -> Survey
    scheduled -> Survey done -> Proposal sent -> Negotiation -> Won,
    plus Nurture / Lost), dispositions, follow-up tasks, notes,
    per-lead activity timeline, lead score.
  - Call analysis: Speko's post-call report (summary / outcome /
    structured facts / cost) pulled during sync, plus derived intent,
    objection signals, talk ratio and a suggested next action with
    one-tap apply.
  - Manual dial: POST /api/dial triggers POST /v1/sessions/phone on the
    company's agent + caller ID, with DNC guard and activity logging.
  - Billing: Speko balance/credits + provider cost split (cached).
  - Instant page loads: everything served from the local DB; Speko syncs
    in the background (new sessions only, 10-way parallel).

Env: SPEKO_API_KEY, SPEKO_AGENT_ID, DEMO_MODE, AUTO_DIAL,
     FB_VERIFY_TOKEN, DASH_USER / DASH_PASS, DB_PATH / DATABASE_URL.
"""
import asyncio
import base64
import hashlib
import hmac
import json
import math
import os
import struct
import time
import uuid
import wave
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import (
    HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse,
    RedirectResponse,
)

from db import (db, init_db, insert_lead, insert_call_ignore, kv_get, kv_set,
                Q, seed_company, backfill_company)

BASE_DIR = Path(__file__).parent
SPEKO_API_KEY = os.environ.get("SPEKO_API_KEY", "")
SPEKO_AGENT_ID = os.environ.get("SPEKO_AGENT_ID", "")
SPEKO_BASE = "https://api.speko.dev"
DEMO_MODE = os.environ.get("DEMO_MODE", "1" if not SPEKO_API_KEY else "0") == "1"
AUTO_DIAL = os.environ.get("AUTO_DIAL", "0") == "1"
FB_VERIFY_TOKEN = os.environ.get("FB_VERIFY_TOKEN", "absolar-dev")
DASH_USER = os.environ.get("DASH_USER", "")
DASH_PASS = os.environ.get("DASH_PASS", "")

USD_INR = 88  # approximate; display only
SYNC_CONCURRENCY = 10
BILLING_CACHE_S = 900
AGENT_CACHE_S = 3600
DEFAULT_COMPANY = "ab-solar"

app = FastAPI(title="REvorax Voice-AI CRM")


@asynccontextmanager
async def lifespan(app):
    if not DEMO_MODE and SPEKO_API_KEY:
        asyncio.create_task(refresh_calls_from_speko())
        asyncio.create_task(refresh_billing_cache())
    yield


app.router.lifespan_context = lifespan


@app.middleware("http")
async def basic_auth(request: Request, call_next):
    # Meta's servers can't send Basic credentials, so the Lead Ads webhook
    # must stay public (it has its own verify-token + signature checks).
    if (not DASH_USER or request.url.path == "/api/health"
            or request.url.path == "/api/webhooks/facebook"):
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
            {"WWW-Authenticate": 'Basic realm="Voice-AI CRM"'})
    return await call_next(request)


# ---------------------------------------------------------------- db ----
init_db()

con = db()
seed_company(con, id=DEFAULT_COMPANY, name="AB Solar Power Systems",
             speko_agent_id=SPEKO_AGENT_ID, caller_id="+18059067310",
             primary_color="#15803D")
backfill_company(con, DEFAULT_COMPANY)
# --- demo/live split: `ab-solar` is Bharath's demo playground; the real
# client gets a fresh, clean company id. Runs once, idempotent. ---
_DEMO_ID, _LIVE_ID = "ab-solar", "ab-solar-live"
_live = con.execute(f"SELECT * FROM companies WHERE id={Q}", (_LIVE_ID,)).fetchone()
if not _live:
    _src = con.execute(f"SELECT * FROM companies WHERE id={Q}", (_DEMO_ID,)).fetchone()
    seed_company(con, id=_LIVE_ID, name="AB Solar Power Systems",
                 speko_agent_id=(_src["speko_agent_id"] if _src else SPEKO_AGENT_ID) or "",
                 caller_id=(_src["caller_id"] if _src else "+18059067310") or "",
                 primary_color=(_src["primary_color"] if _src else "#15803D") or "#15803D")
    con.execute(f"UPDATE companies SET name='AB Solar (Demo)' WHERE id={Q}"
                f" AND name='AB Solar Power Systems'", (_DEMO_ID,))
con.commit()
con.close()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def digits(phone: str) -> str:
    return "".join(c for c in (phone or "") if c.isdigit())


def get_company(cid: str = ""):
    con = db()
    row = None
    if cid:
        row = con.execute(f"SELECT * FROM companies WHERE id={Q}",
                          (cid,)).fetchone()
    if not row:
        row = con.execute(
            "SELECT * FROM companies ORDER BY created_at LIMIT 1").fetchone()
    con.close()
    return dict(row) if row else None


def lead_by_phone(phone: str, company_id: str):
    d = digits(phone)
    if len(d) < 10:
        return None
    tail = d[-10:]
    con = db()
    rows = con.execute(
        f"SELECT * FROM leads WHERE company_id={Q} ORDER BY id DESC",
        (company_id,)).fetchall()
    con.close()
    for r in rows:
        if digits(r["phone"])[-10:] == tail:
            return dict(r)
    return None


def lead_by_phone_any(phone: str):
    """Match a dialed number against leads in ANY company.

    Returns (lead_dict, company_id) or (None, ''). The Speko sync uses this
    so Karthik's live-company calls are never misattributed to the demo co."""
    d = digits(phone)
    if len(d) < 10:
        return None, ""
    tail = d[-10:]
    con = db()
    rows = con.execute(
        "SELECT * FROM leads ORDER BY id DESC").fetchall()
    con.close()
    for r in rows:
        if digits(r["phone"])[-10:] == tail:
            return dict(r), (r["company_id"] or "")
    return None, ""


def log_activity_inline(con, company_id, lead_id, kind, title, detail=""):
    """Same as log_activity but reuses an open connection (for seeding)."""
    aid = f"act-{uuid.uuid4().hex[:12]}"
    con.execute(
        f"INSERT INTO activities (id, company_id, lead_id, kind, title,"
        f" detail, created_at) VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q})",
        (aid, company_id, lead_id, kind, title, detail, now_iso()))
    if lead_id:
        con.execute(f"UPDATE leads SET last_activity_at={Q} WHERE id={Q}",
                    (now_iso(), lead_id))
    return aid


def log_activity(company_id, lead_id, kind, title, detail=""):
    con = db()
    aid = f"act-{uuid.uuid4().hex[:12]}"
    con.execute(
        f"INSERT INTO activities (id, company_id, lead_id, kind, title,"
        f" detail, created_at) VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q})",
        (aid, company_id, lead_id, kind, title, detail, now_iso()))
    if lead_id:
        con.execute(f"UPDATE leads SET last_activity_at={Q} WHERE id={Q}",
                    (now_iso(), lead_id))
    con.commit()
    con.close()
    return aid


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
        ("lead", "అవునండి, independent house ఏ. నాలుగున్నర వేలు bill వస్తుంది."),
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

DEMO_ANALYSIS = {
    "qualified": {
        "outcome": "qualified_survey_booked",
        "summary": "Lead owns an independent house; monthly bill ~₹4,500. Qualified and handed to the team for a site survey.",
        "structured": {"property_type": "independent_house",
                       "roof_ownership": "own", "monthly_bill": "4500",
                       "timeline": "this_month"},
        "objections": [], "talk_ratio": 0.42,
        "next_action": "Book site survey",
        "disposition": "qualified_survey",
    },
    "callback": {
        "outcome": "callback_requested",
        "summary": "Lead was busy and asked to be called back in the evening.",
        "structured": {"callback_requested": True,
                       "callback_window": "evening"},
        "objections": ["timing"], "talk_ratio": 0.35,
        "next_action": "Schedule callback",
        "disposition": "qualified_callback",
    },
    "not_interested": {
        "outcome": "not_interested",
        "summary": "Lead declined — not interested in solar right now.",
        "structured": {"interest": "none"},
        "objections": [], "talk_ratio": 0.3,
        "next_action": "Move to Nurture",
        "disposition": "not_interested_timing",
    },
    "no_answer": {
        "outcome": "no_answer", "summary": "No answer.",
        "structured": {}, "objections": [], "talk_ratio": 0,
        "next_action": "Retry tomorrow", "disposition": "no_answer",
    },
}


def demo_wav(path: Path, seconds: int = 4):
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
    now = now_iso()
    seed_company(con, id=DEFAULT_COMPANY, name="AB Solar Power Systems",
                 speko_agent_id="agent_demo", caller_id="+18059067310",
                 primary_color="#15803D")
    seed_company(con, id="demo-dental", name="Demo Dental Clinic",
                 speko_agent_id="agent_demo", caller_id="+14085551234",
                 primary_color="#1D4ED8")
    outcomes = ["qualified", "qualified", "no_answer", "qualified",
                "callback", "not_interested", "no_answer", "qualified"]
    stage_for = {"qualified": "qualified", "callback": "contacting",
                 "not_interested": "nurture", "no_answer": "contacting"}
    rec_dir = BASE_DIR / "static" / "demo_audio"
    rec_dir.mkdir(parents=True, exist_ok=True)
    for i, (name, phone, src, camp) in enumerate(DEMO_LEADS):
        oc = outcomes[i]
        an = DEMO_ANALYSIS[oc]
        lid = insert_lead(con, name=name, phone=phone, source=src,
                          campaign=camp, created_at=now, status="new")
        con.execute(
            f"UPDATE leads SET company_id={Q}, stage={Q}, score={Q},"
            f" monthly_bill_inr={Q}, property_type={Q}, roof_ownership={Q},"
            f" timeline={Q}, last_activity_at={Q} WHERE id={Q}",
            (DEFAULT_COMPANY, stage_for[oc],
             82 if oc == "qualified" else (45 if oc == "callback" else 20),
             4500 if oc == "qualified" else 0,
             "independent_house" if oc == "qualified" else "",
             "own" if oc == "qualified" else "",
             "this_month" if oc == "qualified" else "", now, lid))
        turns = [{"speaker": s, "text": t.format(name=name.split()[0])}
                 for s, t in DEMO_TALKS[oc]]
        dur = 45 + (i * 13) % 50 if oc != "no_answer" else 0
        status = "completed" if oc != "no_answer" else "no-answer"
        rec = f"demo-{lid}.wav"
        if oc != "no_answer":
            demo_wav(rec_dir / rec)
        usage = json.dumps([
            {"provider": "speko", "metric": "session_seconds",
             "quantity": dur, "cost": round(dur * 0.00077, 4)},
        ])
        con.execute(
            f"INSERT INTO calls (id, company_id, lead_id, to_number, status,"
            f" started_at, duration_seconds, outcome, summary, transcript,"
            f" cost_usd, has_recording, demo, usage_json, structured_json,"
            f" talk_ratio, objections_json, next_action, disposition,"
            f" intent_verdict)"
            f" VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},{Q},1,"
            f"{Q},{Q},{Q},{Q},{Q},{Q},{Q})",
            (f"demo-call-{lid}", DEFAULT_COMPANY, lid, phone, status, now,
             dur, an["outcome"], an["summary"],
             json.dumps(turns, ensure_ascii=False),
             round(dur * 0.00077, 4), 1 if oc != "no_answer" else 0, usage,
             json.dumps(an["structured"]), an["talk_ratio"],
             json.dumps(an["objections"]), an["next_action"],
             an["disposition"],
             analyze_call(oc, an["summary"], [], an["structured"])["intent"]),
        )
        log_activity_inline(con, DEFAULT_COMPANY, lid, "call",
                            f"AI call — {an['outcome'].replace('_', ' ')}",
                            an["summary"])
        if oc == "callback":
            tid = f"task-{uuid.uuid4().hex[:12]}"
            due = (datetime.now(timezone.utc)
                   + timedelta(hours=5)).isoformat()
            con.execute(
                f"INSERT INTO tasks (id, company_id, lead_id, kind, title,"
                f" due_at, created_at) VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q})",
                (tid, DEFAULT_COMPANY, lid, "callback",
                 "Callback requested (evening)", due, now))
    # a couple of dental leads to show the company switcher
    for j, (nm, ph) in enumerate([("Ravi Teja", "+919876543210"),
                                  ("Sneha", "+918765432109")]):
        lid = insert_lead(con, name=nm, phone=ph, source="walkin",
                          campaign="Clinic board", created_at=now,
                          status="new")
        con.execute(f"UPDATE leads SET company_id={Q}, stage='new'"
                    f" WHERE id={Q}", ("demo-dental", lid))
    con.commit()
    con.close()


# demo seeding is invoked at the very bottom of this file, after all
# helpers (including the call-analysis functions) are defined.


# ----------------------------------------------------------- speko ------
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
    """Normalize transcript entries. Returns (lines, outcome)."""
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
    cur = set(list(cur)[-500:])
    kv_set("skip_ids", json.dumps(sorted(cur)))


async def _enrich_one(client, sem, s, company_id):
    """Fetch detail + transcript + post-call report for one session."""
    sid = str(s.get("id") or "")
    async with sem:
        try:
            dr, tr, rp = await asyncio.gather(
                client.get(f"/v1/sessions/{sid}"),
                client.get(f"/v1/sessions/{sid}/transcript"),
                client.get(f"/v1/calls/{sid}/report"),
                return_exceptions=True)
        except Exception:
            return sid, "error", None
    if isinstance(dr, Exception) or getattr(dr, "status_code", 0) != 200:
        return sid, "error", None
    det = dr.json()
    md = det.get("metadata") or {}
    if md.get("runKind") == "reliability" or md.get("gate") is True:
        return sid, "skip", None
    to = md.get("to") or md.get("dialedNumber") or ""
    is_phone = bool(s.get("phoneCall") or det.get("phoneCall"))
    if not is_phone and not to:
        return sid, "skip", None
    lines, outcome = [], ""
    if not isinstance(tr, Exception) and tr.status_code == 200:
        lines, outcome = norm_entries(tr.json().get("entries"))
    report = {}
    if not isinstance(rp, Exception) and rp.status_code == 200:
        try:
            report = rp.json() or {}
        except Exception:
            report = {}
    rep_outcome = str(report.get("outcome") or "").strip()
    rep_summary = str(report.get("summary") or "").strip()
    structured = report.get("structured_data") or {}
    if rep_outcome:
        outcome = rep_outcome
    dur = det.get("durationSeconds") or s.get("durationSeconds") or 0
    if not outcome:
        outcome = ("connected" if (dur or 0) > 0
                   else (det.get("status") or s.get("status") or "unknown"))
    analysis = analyze_call(outcome, rep_summary, lines, structured)
    return sid, "ok", {
        "to": to,
        "company_id": company_id,
        "agent_id": det.get("agentId") or md.get("agentId") or "",
        "meta_lead_id": md.get("lead_id"),
        "status": det.get("status") or s.get("status") or "",
        "started_at": s.get("createdAt") or det.get("createdAt") or "",
        "duration": dur,
        "outcome": outcome,
        "summary": rep_summary,
        "structured": json.dumps(structured if isinstance(structured, dict)
                                 else {}),
        "transcript": json.dumps(lines, ensure_ascii=False),
        "cost": det.get("totalCostUsd") or 0,
        "has_recording": 1 if (det.get("recordingStatus") or "") == "ready"
        else 0,
        "usage": json.dumps(det.get("usage") or []),
        "analysis": analysis,
    }


async def refresh_calls_from_speko():
    """Background sync: new sessions only, enriched in parallel."""
    global sync_running
    if DEMO_MODE or not SPEKO_API_KEY or sync_running:
        return {"skipped": True}
    sync_running = True
    try:
        comp = get_company(DEFAULT_COMPANY) or {}
        fallback_company = comp.get("id") or DEFAULT_COMPANY
        async with speko_async() as c:
            r = await c.get("/v1/sessions", params={"limit": 100})
            if r.status_code != 200:
                return {"error": f"speko {r.status_code}"}
            sessions = r.json().get("entries") or []
        con = db()
        known = {row["id"] for row in
                 con.execute("SELECT id FROM calls").fetchall()}
        # agent id -> company: a call belongs to whichever company owns
        # the Speko agent that placed it (never the default company)
        agent_company = {}
        for c in con.execute(
                "SELECT id, speko_agent_id FROM companies").fetchall():
            if c["speko_agent_id"]:
                agent_company[c["speko_agent_id"]] = c["id"]
        con.close()
        skipped = _skip_ids()
        todo = [s for s in sessions
                if isinstance(s, dict) and str(s.get("id") or "")
                and str(s["id"]) not in known and str(s["id"]) not in skipped]
        sem = asyncio.Semaphore(SYNC_CONCURRENCY)
        async with speko_async() as c:
            results = await asyncio.gather(
                *[_enrich_one(c, sem, s, fallback_company) for s in todo])
        new, skip = 0, []
        con = db()
        try:
            for sid, kind, p in results:
                if kind == "skip":
                    skip.append(sid)
                    continue
                if kind != "ok" or not p:
                    continue
                # resolve the true company for this call:
                # 1) lead_id stamped in dial metadata, 2) agent->company map,
                # 3) phone match across all companies, 4) default fallback
                lead, cid = None, ""
                if p.get("meta_lead_id"):
                    r = con.execute(
                        f"SELECT * FROM leads WHERE id={Q}",
                        (p["meta_lead_id"],)).fetchone()
                    if r:
                        lead, cid = dict(r), r["company_id"] or ""
                if not lead and p.get("agent_id") in agent_company:
                    cid = agent_company[p["agent_id"]]
                    lead = lead_by_phone(p["to"], cid)
                if not lead:
                    lead, cid = lead_by_phone_any(p["to"])
                if not cid:
                    cid = fallback_company
                an = p["analysis"]
                insert_call_ignore(
                    con, id=sid, lead_id=lead["id"] if lead else None,
                    to_number=p["to"], status=p["status"],
                    started_at=p["started_at"],
                    duration_seconds=p["duration"], demo=0)
                con.execute(
                    f"UPDATE calls SET company_id={Q}, outcome={Q},"
                    f" summary={Q}, structured_json={Q}, transcript={Q},"
                    f" cost_usd={Q}, has_recording={Q}, to_number={Q},"
                    f" status={Q}, duration_seconds={Q}, usage_json={Q},"
                    f" talk_ratio={Q}, objections_json={Q}, next_action={Q},"
                    f" disposition={Q}, intent_verdict={Q},"
                    f" intent_confidence={Q} WHERE id={Q}",
                    (cid, p["outcome"], p["summary"],
                     p["structured"], p["transcript"], p["cost"],
                     p["has_recording"], p["to"], p["status"],
                     p["duration"], p["usage"], an["talk_ratio"],
                     json.dumps(an["objections"]), an["next_action"],
                     an["disposition"], an["intent"],
                     an["intent_confidence"], sid),
                )
                lid = lead["id"] if lead else None
                if lid:
                    # keep the stored score fresh so lists/kanban match the
                    # drawer (previously lists always showed 0)
                    lrow = con.execute(
                        f"SELECT * FROM leads WHERE id={Q}",
                        (lid,)).fetchone()
                    lcalls = [dict(x) for x in con.execute(
                        f"SELECT duration_seconds FROM calls"
                        f" WHERE lead_id={Q}", (lid,)).fetchall()]
                    score = lead_score(dict(lrow), lcalls)
                    con.execute(
                        f"UPDATE leads SET last_activity_at={Q}, score={Q}"
                        f" WHERE id={Q}", (now_iso(), score, lid))
                    if an["disposition"] == "dnc":
                        # prospect said do-not-call: suppress immediately,
                        # don't wait for a human to tap the disposition
                        con.execute(f"UPDATE leads SET dnc=1 WHERE id={Q}",
                                    (lid,))
                        log_activity_inline(
                            con, cid, lid, "note",
                            "Auto-suppressed (DNC)",
                            "Prospect asked not to be called again.")
                    elif an["intent"] == "callback":
                        # prospect asked for a callback: make sure there's
                        # an open task so it can't slip through
                        ex = con.execute(
                            f"SELECT id FROM tasks WHERE lead_id={Q}"
                            f" AND done=0 AND kind='callback' LIMIT 1",
                            (lid,)).fetchone()
                        if not ex:
                            tid = f"task-{uuid.uuid4().hex[:12]}"
                            due = (datetime.now(timezone.utc)
                                   + timedelta(days=1)).isoformat()
                            nm = (lead.get("name") or p["to"]).strip()
                            con.execute(
                                f"INSERT INTO tasks (id, company_id, lead_id,"
                                f" kind, title, due_at, created_at)"
                                f" VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q})",
                                (tid, cid, lid, "callback",
                                 f"Call back {nm}", due, now_iso()))
                            log_activity_inline(
                                con, cid, lid, "task",
                                "Callback task created",
                                "Prospect asked for a callback on the call.")
                new += 1
            con.commit()
        finally:
            con.close()
        _add_skip_ids(skip)
        kv_set("last_sync", now_iso())
        kv_set("last_sync_new", str(new))
        return {"new": new, "skipped_evals": len(skip)}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:200]}"}
    finally:
        sync_running = False

# ------------------------------------------------- call analysis ------
# Speko's report gives us summary / outcome / structured_data. On top of
# that we derive intent, objection signals, talk ratio and a suggested
# next action — all from the transcript, no extra LLM calls.

OBJECTION_KEYWORDS = {
    "price": ["price", "cost", "expensive", "costly", "rate", "charge",
              "ధర", "రేటు", "డబ్బు", "ఖర్చు"],
    "trust": ["scam", "fraud", "fake", "cheat", "నమ్మకం", "మోసం"],
    "timing": ["later", "busy", "not now", "తర్వాత", "తరువాత"],
    "roof": ["rent", "rented", "apartment", "flat", "అద్దె"],
    "subsidy": ["subsidy", "government", "సబ్సిడీ"],
}

DISPOSITIONS = [
    ("qualified_survey", "Qualified – survey booked", "survey_scheduled"),
    ("qualified_callback", "Qualified – callback requested", "qualified"),
    ("interested_followup", "Interested – follow-up needed", "contacting"),
    ("not_interested_price", "Not interested – price", "lost"),
    ("not_interested_roof", "Not interested – no roof/tenant", "lost"),
    ("not_interested_timing", "Not interested – timing", "nurture"),
    ("wrong_number", "Wrong number / language barrier", "lost"),
    ("dnc", "Do not call", "lost"),
    ("no_answer", "No answer", "contacting"),
    ("busy", "Busy", "contacting"),
    ("failed", "Call failed", "contacting"),
]
DISP_STAGE = {d[0]: d[2] for d in DISPOSITIONS}
DISP_LABEL = {d[0]: d[1] for d in DISPOSITIONS}

STAGES = [
    ("new", "New"), ("contacting", "Contacting"),
    ("qualified", "Qualified"), ("survey_scheduled", "Survey scheduled"),
    ("survey_done", "Survey done"), ("proposal_sent", "Proposal sent"),
    ("negotiation", "Negotiation"), ("won", "Won"),
    ("nurture", "Nurture"), ("lost", "Lost"),
]
STAGE_LABEL = dict(STAGES)


def detect_objections(lines):
    text = " ".join(l.get("text", "") for l in lines
                    if l.get("speaker") == "lead").lower()
    found = []
    for name, kws in OBJECTION_KEYWORDS.items():
        if any(k in text for k in kws):
            found.append(name)
    return found


def talk_ratio(lines):
    lead_chars = sum(len(l.get("text", "")) for l in lines
                     if l.get("speaker") == "lead")
    total = sum(len(l.get("text", "")) for l in lines)
    return round(lead_chars / total, 2) if total else 0


def analyze_call(outcome, summary, lines, structured):
    """Derive intent verdict, objections, next action, suggested
    disposition from the outcome + transcript. Speko's own summary and
    outcome are kept verbatim; everything derived is labeled as such."""
    s = f"{outcome} {summary}".lower()
    if any(k in s for k in ("not interested", "declined", "not_interested",
                            "dnc", "do not call", "not qualified",
                            "not_qualified", "unqualified")):
        intent, conf = "not_interested", 0.9
        disp = ("dnc" if "dnc" in s or "do not call" in s
                else "not_interested_timing")
        nxt = "Move to Nurture" if disp != "dnc" else "Suppress (DNC)"
    elif any(k in s for k in ("callback", "call back", "call me",
                              "busy", "later")):
        # before the survey/book branch: "please book a callback"
        # contains "book" but is a callback, not a buying signal
        intent, conf = "callback", 0.85
        disp, nxt = "qualified_callback", "Schedule callback"
    elif any(k in s for k in ("survey", "book", "site visit", "qualified")):
        intent, conf = "buying", 0.85
        disp, nxt = "qualified_survey", "Book site survey"
    elif any(k in s for k in ("no answer", "no-answer", "voicemail",
                              "not reachable")):
        intent, conf = "unknown", 0.9
        disp, nxt = "no_answer", "Retry tomorrow"
    elif any(k in s for k in ("wrong number", "wrong_number")):
        intent, conf = "not_interested", 0.9
        disp, nxt = "wrong_number", "Mark lost"
    elif any(k in s for k in ("interested", "curious", "details", "price",
                              "cost", "subsidy")):
        intent, conf = "curious", 0.7
        disp, nxt = "interested_followup", "Send details on WhatsApp"
    elif any(k in s for k in ("connected", "answered")):
        intent, conf = "curious", 0.55
        disp, nxt = "interested_followup", "Review transcript"
    else:
        intent, conf = "unknown", 0.5
        disp, nxt = "failed", "Review transcript"
    return {
        "intent": intent, "intent_confidence": conf,
        "objections": detect_objections(lines),
        "talk_ratio": talk_ratio(lines),
        "next_action": nxt, "disposition": disp,
        "derived": True,  # UI labels these "detected signals"
    }


def lead_score(lead, calls):
    s = 0
    bill = lead.get("monthly_bill_inr") or 0
    if bill >= 4000:
        s += 25
    elif bill >= 2000:
        s += 15
    elif bill > 0:
        s += 8
    if (lead.get("roof_ownership") or "") == "own":
        s += 20
    tl = lead.get("timeline") or ""
    if tl == "this_month":
        s += 15
    elif tl == "1_3_months":
        s += 8
    if (lead.get("decision_maker") or "") == "self":
        s += 10
    if (lead.get("property_type") or "") in ("independent_house", "villa"):
        s += 10
    best = max([c.get("duration_seconds") or 0 for c in calls] or [0])
    if best >= 60:
        s += 15
    elif best > 0:
        s += 8
    return min(s, 100)


# ------------------------------------------------------------ billing ---
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
            "fetched_at": now_iso(),
        }
        kv_set("billing_cache", json.dumps(out))
        kv_set("billing_cache_ts", str(time.time()))
        return out
    except Exception:
        return None


async def get_agent_info(company):
    agent_id = (company or {}).get("speko_agent_id") or ""
    if DEMO_MODE or not agent_id:
        return {"demo": True, "name": "Priya (demo)",
                "language": "Telugu", "voice": "demo", "updated_at": ""}
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
            r = await c.get(f"/v1/agents/{agent_id}")
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
    # schema self-check: every v3 column the API reads must exist, otherwise
    # endpoints 500 (this caught a silently-skipped PG migration on 2026-09-25)
    missing = []
    try:
        from db import _V3_COLS, USE_PG
        con = db()
        for table, col, _ddl in _V3_COLS:
            if USE_PG:
                r = con.execute(
                    "SELECT 1 FROM information_schema.columns"
                    " WHERE table_name=%s AND column_name=%s",
                    (table, col)).fetchone()
            else:
                r = [x for x in con.execute(f"PRAGMA table_info({table})")
                     if x[1] == col]
            if not r:
                missing.append(f"{table}.{col}")
        con.close()
    except Exception as e:
        missing = [f"check failed: {e}"]
    return {"ok": True, "demo_mode": DEMO_MODE,
            "speko_configured": bool(SPEKO_API_KEY),
            "auto_dial": AUTO_DIAL,
            "sync_running": sync_running,
            "last_sync": kv_get("last_sync"),
            "version": 3,
            "usd_inr": USD_INR,
            "schema_ok": not missing,
            "missing_columns": missing}


@app.get("/api/companies")
def companies():
    con = db()
    rows = con.execute("SELECT * FROM companies ORDER BY created_at").fetchall()
    con.close()
    return [dict(r) for r in rows]


def _cid(req: Request, body_company=""):
    return (req.query_params.get("company") or body_company
            or DEFAULT_COMPANY)


@app.get("/api/kpis")
def kpis(req: Request):
    """Owner's daily glance: funnel, rates, money, follow-ups, stale."""
    cid = _cid(req)
    con = db()
    leads = [dict(r) for r in con.execute(
        f"SELECT * FROM leads WHERE company_id={Q}", (cid,)).fetchall()]
    calls = [dict(r) for r in con.execute(
        f"SELECT * FROM calls WHERE company_id={Q}", (cid,)).fetchall()]
    today = datetime.now(timezone.utc).date().isoformat()
    tasks_due = con.execute(
        f"SELECT COUNT(*) c FROM tasks WHERE company_id={Q} AND done=0"
        f" AND substr(due_at,1,10) <= {Q}", (cid, today)).fetchone()["c"]
    con.close()
    dials = len(calls)
    connected = sum(1 for c in calls if (c["duration_seconds"] or 0) > 0)
    qualified = sum(1 for c in calls if (c["disposition"] or "")
                    .startswith("qualified"))
    spend = sum(c["cost_usd"] or 0 for c in calls) * USD_INR
    funnel = {s: 0 for s, _ in STAGES}
    for l in leads:
        funnel[l.get("stage") or "new"] = funnel.get(
            l.get("stage") or "new", 0) + 1
    stale = sum(1 for l in leads
                if (l.get("stage") or "") in
                ("qualified", "proposal_sent", "negotiation")
                and (l.get("last_activity_at") or "") <
                (datetime.now(timezone.utc) - timedelta(days=7)).isoformat())
    return {
        "leads": len(leads), "dials": dials, "connected": connected,
        "connect_rate": round(connected / dials * 100) if dials else 0,
        "qualified": qualified,
        "qualify_rate": round(qualified / connected * 100) if connected else 0,
        "spend_inr": round(spend, 2),
        "cost_per_qualified": round(spend / qualified, 2) if qualified else 0,
        "funnel": funnel, "tasks_due": tasks_due, "stale_deals": stale,
        "demo_mode": DEMO_MODE,
    }


@app.get("/api/pipeline")
def pipeline(req: Request):
    cid = _cid(req)
    con = db()
    leads = [dict(r) for r in con.execute(
        f"SELECT * FROM leads WHERE company_id={Q}"
        f" ORDER BY last_activity_at DESC", (cid,)).fetchall()]
    con.close()
    cols = []
    for sid, label in STAGES:
        items = [l for l in leads if (l.get("stage") or "new") == sid]
        cols.append({"id": sid, "label": label, "count": len(items),
                     "leads": [{"id": l["id"], "name": l["name"],
                                "phone": l["phone"], "score": l["score"],
                                "last_activity_at":
                                l.get("last_activity_at") or "",
                                "next_followup_at":
                                l.get("next_followup_at") or ""}
                               for l in items[:50]]})
    return cols


@app.get("/api/leads")
def lead_list(req: Request):
    cid = _cid(req)
    stage = req.query_params.get("stage", "")
    q = (req.query_params.get("q") or "").lower()
    con = db()
    rows = [dict(r) for r in con.execute(
        f"SELECT * FROM leads WHERE company_id={Q} ORDER BY id DESC",
        (cid,)).fetchall()]
    out = []
    for r in rows:
        if stage and (r.get("stage") or "new") != stage:
            continue
        if q and q not in f"{r['name']} {r['phone']}".lower():
            continue
        last = con.execute(
            f"SELECT outcome, started_at, status FROM calls"
            f" WHERE lead_id={Q} ORDER BY started_at DESC LIMIT 1",
            (r["id"],)).fetchone()
        r["last_outcome"] = (last["outcome"] if last else "") or ""
        r["last_call_at"] = (last["started_at"] if last else "") or ""
        out.append(r)
    con.close()
    return out


@app.get("/api/leads/{lead_id}")
def lead_detail(lead_id: int, req: Request):
    con = db()
    r = con.execute(f"SELECT * FROM leads WHERE id={Q}",
                    (lead_id,)).fetchone()
    if not r:
        con.close()
        raise HTTPException(404, "lead not found")
    lead = dict(r)
    calls = [dict(x) for x in con.execute(
        f"SELECT id, status, started_at, duration_seconds, outcome,"
        f" summary, cost_usd, disposition, next_action"
        f" FROM calls WHERE lead_id={Q} ORDER BY started_at DESC",
        (lead_id,)).fetchall()]
    tasks = [dict(x) for x in con.execute(
        f"SELECT * FROM tasks WHERE lead_id={Q} ORDER BY done, due_at",
        (lead_id,)).fetchall()]
    notes = [dict(x) for x in con.execute(
        f"SELECT * FROM notes WHERE lead_id={Q} ORDER BY created_at DESC",
        (lead_id,)).fetchall()]
    acts = [dict(x) for x in con.execute(
        f"SELECT * FROM activities WHERE lead_id={Q}"
        f" ORDER BY created_at DESC LIMIT 50", (lead_id,)).fetchall()]
    con.close()
    lead["score"] = lead_score(lead, calls)
    # the drawer reads these names; the columns predate the UI labels
    lead["financing_interest"] = lead.get("financing") or ""
    lead["subsidy_awareness"] = lead.get("subsidy_aware") or ""
    lead["calls"], lead["tasks"], lead["notes"], lead["timeline"] = \
        calls, tasks, notes, acts
    return lead


@app.post("/api/leads")
async def add_lead(req: Request):
    body = await req.json()
    cid = _cid(req, body.get("company", ""))
    phone = body.get("phone", "")
    if len(digits(phone)) < 10:
        raise HTTPException(400, "valid phone required")
    now = now_iso()
    con = db()
    lid = insert_lead(con, name=body.get("name", ""), phone=phone,
                      source=body.get("source", "manual"),
                      campaign=body.get("campaign", ""), created_at=now)
    # qualification facts collected by the add-lead form (previously dropped)
    qual = {k: body.get(k) for k in
            ("monthly_bill_inr", "property_type", "roof_ownership",
             "timeline", "decision_maker", "language")
            if body.get(k) not in (None, "")}
    cols = list(qual) + ["company_id", "stage", "last_activity_at"]
    vals = list(qual.values()) + [cid, "new", now, lid]
    con.execute(f"UPDATE leads SET {', '.join(f'{k}={Q}' for k in cols)}"
                f" WHERE id={Q}", vals)
    con.commit()
    con.close()
    log_activity(cid, lid, "lead", "Lead added",
                 f"Source: {body.get('source', 'manual')}")
    dial = None
    if AUTO_DIAL and not DEMO_MODE:
        ok, msg = dial_now(cid, phone, lid, body.get("name", ""))
        dial = {"ok": ok, "message": msg}
    return {"id": lid, "dial": dial}


@app.patch("/api/leads/{lead_id}")
async def update_lead(lead_id: int, req: Request):
    """Update qualification facts / fields on a lead."""
    body = await req.json()
    # the lead drawer reads financing_interest / subsidy_awareness while the
    # schema historically used financing / subsidy_aware — accept both
    aliases = {"financing_interest": "financing",
               "subsidy_awareness": "subsidy_aware"}
    for src, dst in aliases.items():
        if src in body and dst not in body:
            body[dst] = body[src]
    allowed = ["name", "phone", "language", "discom", "consumer_no",
               "property_type", "roof_ownership", "roof_type",
               "roof_area_sqft", "monthly_bill_inr", "monthly_units",
               "system_size_kw", "financing", "decision_maker", "timeline",
               "subsidy_aware", "notes", "dnc"]
    sets, vals = [], []
    for k in allowed:
        if k in body:
            sets.append(f"{k}={Q}")
            vals.append(body[k])
    if not sets:
        raise HTTPException(400, "nothing to update")
    sets.append(f"last_activity_at={Q}")
    vals.append(now_iso())
    vals.append(lead_id)
    con = db()
    con.execute(f"UPDATE leads SET {', '.join(sets)} WHERE id={Q}", vals)
    row = con.execute(f"SELECT company_id FROM leads WHERE id={Q}",
                      (lead_id,)).fetchone()
    con.commit()
    con.close()
    if row:
        log_activity(row["company_id"], lead_id, "note",
                     "Lead details updated", "")
    return {"ok": True}


@app.post("/api/leads/{lead_id}/stage")
async def move_stage(lead_id: int, req: Request):
    body = await req.json()
    stage = body.get("stage", "")
    if stage not in STAGE_LABEL:
        raise HTTPException(400, "unknown stage")
    con = db()
    row = con.execute(f"SELECT company_id, stage FROM leads WHERE id={Q}",
                      (lead_id,)).fetchone()
    if not row:
        con.close()
        raise HTTPException(404, "lead not found")
    vals = [stage, now_iso()]
    extra = ""
    if stage == "lost" and body.get("lost_reason"):
        extra = f", lost_reason={Q}"
        vals.append(body["lost_reason"])
    vals.append(lead_id)
    con.execute(f"UPDATE leads SET stage={Q}, last_activity_at={Q}{extra}"
                f" WHERE id={Q}", vals)
    con.commit()
    con.close()
    log_activity(row["company_id"], lead_id, "stage",
                 f"Stage → {STAGE_LABEL[stage]}",
                 f"from {STAGE_LABEL.get(row['stage'], row['stage'])}")
    return {"ok": True, "stage": stage}


@app.post("/api/leads/{lead_id}/notes")
async def add_note(lead_id: int, req: Request):
    body = await req.json()
    text = (body.get("body") or "").strip()
    if not text:
        raise HTTPException(400, "empty note")
    con = db()
    row = con.execute(f"SELECT company_id FROM leads WHERE id={Q}",
                      (lead_id,)).fetchone()
    if not row:
        con.close()
        raise HTTPException(404, "lead not found")
    nid = f"note-{uuid.uuid4().hex[:12]}"
    con.execute(
        f"INSERT INTO notes (id, company_id, lead_id, body, created_at)"
        f" VALUES ({Q},{Q},{Q},{Q},{Q})",
        (nid, row["company_id"], lead_id, text, now_iso()))
    con.commit()
    con.close()
    log_activity(row["company_id"], lead_id, "note", "Note added", text[:120])
    return {"ok": True, "id": nid}


@app.get("/api/tasks")
def task_list(req: Request):
    cid = _cid(req)
    filt = req.query_params.get("filter", "open")  # open|done|all|today
    con = db()
    rows = [dict(r) for r in con.execute(
        f"SELECT t.*, l.name AS lead_name, l.phone AS lead_phone"
        f" FROM tasks t LEFT JOIN leads l ON l.id=t.lead_id"
        f" WHERE t.company_id={Q} ORDER BY t.done, t.due_at", (cid,)).fetchall()]
    con.close()
    today = datetime.now(timezone.utc).date().isoformat()
    if filt == "today":
        rows = [r for r in rows
                if not r["done"] and (r["due_at"] or "")[:10] <= today]
    elif filt == "open":
        rows = [r for r in rows if not r["done"]]
    elif filt == "done":
        rows = [r for r in rows if r["done"]]
    return rows


@app.post("/api/tasks")
async def add_task(req: Request):
    body = await req.json()
    cid = _cid(req, body.get("company", ""))
    lead_id = body.get("lead_id")
    title = (body.get("title") or "").strip()
    if not title:
        raise HTTPException(400, "title required")
    tid = f"task-{uuid.uuid4().hex[:12]}"
    con = db()
    con.execute(
        f"INSERT INTO tasks (id, company_id, lead_id, kind, title, due_at,"
        f" created_at) VALUES ({Q},{Q},{Q},{Q},{Q},{Q},{Q})",
        (tid, cid, lead_id, body.get("kind", "followup"), title,
         body.get("due_at", ""), now_iso()))
    con.commit()
    con.close()
    if lead_id:
        log_activity(cid, lead_id, "task", f"Task: {title}",
                     f"Due {body.get('due_at', '—')}")
    return {"ok": True, "id": tid}


@app.post("/api/tasks/{task_id}/done")
async def task_done(task_id: str, req: Request):
    body = await req.json() if req.headers.get("content-type") else {}
    done = 0 if body.get("done") is False else 1
    con = db()
    con.execute(f"UPDATE tasks SET done={Q} WHERE id={Q}", (done, task_id))
    con.commit()
    con.close()
    return {"ok": True, "done": bool(done)}

# -------------------------------------------------- dial & calls ------
def dial_now(company_id, phone, lead_id=None, name=""):
    """Place a manual outbound call via Speko. Returns (ok, message)."""
    comp = get_company(company_id) or {}
    agent_id = comp.get("speko_agent_id") or ""
    if DEMO_MODE:
        return True, f"(demo) call queued to {phone}"
    if not agent_id:
        return False, "no Speko agent configured for this company"
    if not SPEKO_API_KEY:
        return False, "Speko API key not configured"
    to = phone if phone.startswith("+") else f"+91{digits(phone)[-10:]}"
    payload = {"to": to, "agentId": agent_id,
               "metadata": {"lead_id": lead_id, "name": name,
                            "source": "dashboard_manual"}}
    if comp.get("caller_id"):
        payload["from"] = comp["caller_id"]
    try:
        with speko() as c:
            r = c.post("/v1/sessions/phone", json=payload)
        if r.status_code not in (200, 201, 202):
            return False, f"speko {r.status_code}: {r.text[:200]}"
        sid = (r.json() or {}).get("sessionId") or f"manual-{uuid.uuid4().hex[:8]}"
        con = db()
        insert_call_ignore(con, id=sid, lead_id=lead_id, to_number=to,
                           status="dialing", started_at=now_iso(),
                           duration_seconds=0, demo=0)
        con.execute(f"UPDATE calls SET company_id={Q} WHERE id={Q}",
                    (company_id, sid))
        con.commit()
        con.close()
        if lead_id:
            log_activity(company_id, lead_id, "call",
                         f"Manual call placed to {to}", "")
        return True, f"calling {to}…"
    except Exception as e:
        return False, f"dial failed: {e}"


@app.post("/api/dial")
async def manual_dial(req: Request):
    """Manual call trigger from the dashboard (per-lead Call button or
    the dialer). Guards DNC; every dial is logged."""
    body = await req.json()
    cid = _cid(req, body.get("company", ""))
    lead_id = body.get("lead_id")
    phone = (body.get("to") or "").strip()
    name = ""
    if lead_id:
        con = db()
        row = con.execute(f"SELECT phone, name, dnc, company_id FROM leads"
                          f" WHERE id={Q}", (lead_id,)).fetchone()
        con.close()
        if not row:
            raise HTTPException(404, "lead not found")
        if row["dnc"]:
            raise HTTPException(403, "lead is on Do-Not-Call")
        phone, name = row["phone"], row["name"]
        cid = row["company_id"] or cid
    if len(digits(phone)) < 10:
        raise HTTPException(400, "valid phone required")
    ok, msg = dial_now(cid, phone, lead_id, name)
    if not ok:
        raise HTTPException(502, msg)
    return {"ok": True, "message": msg}


@app.post("/api/calls/{call_id}/disposition")
async def set_disposition(call_id: str, req: Request):
    """Owner confirms/corrects the AI-suggested disposition. Moves the
    lead's pipeline stage accordingly and logs it."""
    body = await req.json()
    disp = body.get("disposition", "")
    if disp not in DISP_LABEL:
        raise HTTPException(400, "unknown disposition")
    con = db()
    call = con.execute(
        f"SELECT lead_id, company_id FROM calls WHERE id={Q}",
        (call_id,)).fetchone()
    if not call:
        con.close()
        raise HTTPException(404, "call not found")
    con.execute(f"UPDATE calls SET disposition={Q} WHERE id={Q}",
                (disp, call_id))
    stage = DISP_STAGE[disp]
    if call["lead_id"]:
        vals = [stage, now_iso()]
        extra = ""
        if stage == "lost":
            extra, vals = f", lost_reason={Q}", vals + [disp]
        elif stage == "nurture":
            extra = ""
        vals.append(call["lead_id"])
        con.execute(f"UPDATE leads SET stage={Q}, last_activity_at={Q}"
                    f"{extra} WHERE id={Q}", vals)
        if disp == "dnc":
            con.execute(f"UPDATE leads SET dnc=1 WHERE id={Q}",
                        (call["lead_id"],))
    con.commit()
    con.close()
    if call["lead_id"]:
        log_activity(call["company_id"], call["lead_id"], "call",
                     f"Disposition: {DISP_LABEL[disp]}",
                     f"Stage → {STAGE_LABEL[stage]}")
    return {"ok": True, "disposition": disp, "stage": stage,
            "dispositions": [{"id": d[0], "label": d[1]}
                             for d in DISPOSITIONS]}


@app.get("/api/dispositions")
def dispositions():
    return [{"id": d[0], "label": d[1], "stage": d[2],
             "stage_label": STAGE_LABEL[d[2]]} for d in DISPOSITIONS]


@app.get("/api/calls")
def call_list(req: Request):
    cid = _cid(req)
    con = db()
    rows = con.execute(
        f"SELECT c.*, l.name AS lead_name FROM calls c"
        f" LEFT JOIN leads l ON l.id = c.lead_id"
        f" WHERE c.company_id={Q}"
        f" ORDER BY c.started_at DESC, c.id DESC", (cid,)).fetchall()
    con.close()
    out = []
    for r in rows:
        d = dict(r)
        d.pop("transcript", None)
        d.pop("structured_json", None)
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
    for jk, df in (("transcript", []), ("usage_json", []),
                   ("structured_json", {}), ("objections_json", [])):
        try:
            d[jk] = json.loads(d.get(jk) or json.dumps(df))
        except Exception:
            d[jk] = df
    d.pop("usage_json", None)
    if not d["transcript"] and not d["demo"] and SPEKO_API_KEY \
            and not DEMO_MODE:
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
    return RedirectResponse(url, status_code=302)


# ------------------------------------------------------ misc api ------
@app.get("/api/billing")
async def billing():
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
async def agent(req: Request):
    comp = get_company(_cid(req))
    info = await get_agent_info(comp)
    if info:
        # the dialer's "From" line needs the real phone number, not just
        # the agent's display name
        info["caller_id"] = (comp or {}).get("caller_id") or ""
        return info
    raise HTTPException(502, "agent info unavailable")


# ------------------------------------------------- agent control ------
# Read and update the live Speko agent configuration (system prompt,
# voice instructions, voice, first message, endpointing). PATCH goes
# live on Speko immediately — the UI warns before saving.

DEMO_AGENT_CONFIG = {
    "name": "Priya - Demo Solar Outbound",
    "agent_id": "agent_demo",
    "systemPrompt": ("DEMO MODE — connect the Speko API key in production "
                     "to read and edit the live agent prompt here."),
    "voiceInstructions": "",
    "voice": "",
    "firstMessage": "",
    "endpointing_min_ms": 300,
    "endpointing_max_ms": 500,
    "demo": True,
}

AGENT_WRITABLE = ("systemPrompt", "voiceInstructions", "voice",
                  "firstMessage")


def _agent_config_from_speko(a):
    th = a.get("turnHandling") or {}
    ep = th.get("endpointing") or {}
    return {
        "name": a.get("name") or "",
        "agent_id": a.get("id") or "",
        "systemPrompt": a.get("systemPrompt") or "",
        "voiceInstructions": a.get("voiceInstructions") or "",
        "voice": a.get("voice") or "",
        "firstMessage": a.get("firstMessage") or "",
        "endpointing_min_ms": ep.get("minDelay"),
        "endpointing_max_ms": ep.get("maxDelay"),
    }


@app.get("/api/agent/config")
async def agent_config(req: Request):
    cid = _cid(req)
    co = get_company(cid)
    if DEMO_MODE or not co.get("speko_agent_id"):
        return DEMO_AGENT_CONFIG
    async with speko_async() as c:
        r = await c.get(f"/v1/agents/{co['speko_agent_id']}")
    if r.status_code != 200:
        raise HTTPException(502, "could not read agent config from Speko")
    return _agent_config_from_speko(r.json())


@app.patch("/api/agent/config")
async def agent_config_update(req: Request):
    cid = _cid(req)
    co = get_company(cid)
    if DEMO_MODE:
        raise HTTPException(400, "agent control is disabled in demo mode")
    if not co.get("speko_agent_id"):
        raise HTTPException(400, "no Speko agent linked to this company")
    body = await req.json()
    patch = {}
    for f in AGENT_WRITABLE:
        if f in body and isinstance(body[f], str):
            patch[f] = body[f]
    async with speko_async() as c:
        # merge endpointing into the existing turnHandling so we never
        # wipe sibling settings
        ep_min, ep_max = body.get("endpointing_min_ms"), \
            body.get("endpointing_max_ms")
        if ep_min is not None or ep_max is not None:
            cur = await c.get(f"/v1/agents/{co['speko_agent_id']}")
            th = (cur.json().get("turnHandling") or {}) if \
                cur.status_code == 200 else {}
            ep = dict(th.get("endpointing") or {})
            if ep_min is not None:
                ep["minDelay"] = max(0, int(ep_min))
            if ep_max is not None:
                ep["maxDelay"] = max(0, int(ep_max))
            th["endpointing"] = ep
            patch["turnHandling"] = th
        if not patch:
            raise HTTPException(400, "nothing to update")
        r = await c.patch(f"/v1/agents/{co['speko_agent_id']}", json=patch)
    if r.status_code not in (200, 201):
        raise HTTPException(502,
                            f"Speko rejected the update ({r.status_code})")
    # read-back verify for the prompt (standing rule: saved != live)
    if "systemPrompt" in patch:
        async with speko_async() as c:
            r2 = await c.get(f"/v1/agents/{co['speko_agent_id']}")
            if r2.status_code == 200 and \
                    r2.json().get("systemPrompt") != patch["systemPrompt"]:
                raise HTTPException(502,
                                    "Speko did not confirm the prompt update")
    kv_set("agent_cache_ts", "0")  # invalidate the agent card cache
    log_activity(cid, None, "agent_update", "Agent config updated",
                 "Updated: " + ", ".join(sorted(patch.keys())))
    return {"ok": True, "updated": sorted(patch.keys())}


@app.get("/api/numbers")
async def numbers():
    return await get_phone_numbers()


@app.get("/api/usage/daily")
def usage_daily(req: Request):
    cid = _cid(req)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    con = db()
    rows = con.execute(
        f"SELECT substr(started_at,1,10) AS d, COUNT(*) AS n,"
        f" SUM(cost_usd) AS c FROM calls"
        f" WHERE demo=0 AND company_id={Q} AND started_at >= {Q} GROUP BY d",
        (cid, cutoff)).fetchall()
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


# --------------------------------------------- facebook lead webhook -----
@app.get("/api/webhooks/facebook")
def fb_verify(hub_mode: str = Query(default="", alias="hub.mode"),
              hub_challenge: str = Query(default="", alias="hub.challenge"),
              hub_verify_token: str = Query(default="",
                                            alias="hub.verify_token")):
    # Meta sends hub.mode / hub.challenge / hub.verify_token (dotted);
    # FastAPI needs explicit aliases to bind them.
    if hub_mode == "subscribe" and hub_verify_token == FB_VERIFY_TOKEN:
        return PlainTextResponse(hub_challenge)
    raise HTTPException(403, "verification failed")


def verify_meta_signature(req: Request, body: bytes, app_secret: str) -> bool:
    """Check X-Hub-Signature-256 against the Meta app secret.

    When no secret is configured we can't verify — allow but flag it, so a
    missing secret never silently blocks leads, yet the Integrations view
    can warn that the webhook is unverified."""
    if not app_secret:
        return True
    sig = req.headers.get("x-hub-signature-256", "")
    if not sig.startswith("sha256="):
        return False
    mac = hmac.new(app_secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig[7:], mac)


def enrich_meta_lead(leadgen_id: str, page_token: str):
    """Fetch a Meta lead ad's field_data via Graph API.

    Returns (name, phone, email). Empty strings on any failure —
    the lead is still created, just flagged needs_enrichment."""
    try:
        with httpx.Client(timeout=15) as c:
            r = c.get(f"https://graph.facebook.com/v21.0/{leadgen_id}",
                      params={"access_token": page_token})
        if r.status_code != 200:
            return "", "", ""
        fields = {f.get("name"): (f.get("values") or [""])[0]
                  for f in (r.json().get("field_data") or [])}
        name = (fields.get("full_name") or "").strip()
        if not name:
            name = " ".join(x for x in
                            (fields.get("first_name", ""),
                             fields.get("last_name", "")) if x).strip()
        return name, (fields.get("phone_number") or "").strip(), \
            (fields.get("email") or "").strip()
    except Exception:
        return "", "", ""


def auto_dials_today(company_id: str) -> int:
    """Count real auto-dial attempts today (for the Meta daily cap).

    Skipped dials (DNC / cap reached) are logged with kind='auto_dial' too
    but must NOT eat the cap — only placed/failed attempts count."""
    con = db()
    day = now_iso()[:10]
    row = con.execute(
        f"SELECT COUNT(*) AS n FROM activities WHERE company_id={Q}"
        f" AND kind='auto_dial' AND substr(created_at,1,10)={Q}"
        f" AND (title LIKE {Q} OR title LIKE {Q})",
        (company_id, day, "Auto-dial placed%",
         "Auto-dial failed%")).fetchone()
    con.close()
    return (row["n"] if row else 0) or 0


@app.post("/api/webhooks/facebook")
async def fb_lead(req: Request):
    cid = req.query_params.get("company") or DEFAULT_COMPANY
    comp = get_company(cid) or {}
    raw = await req.body()
    if not verify_meta_signature(req, raw,
                                 comp.get("meta_app_secret") or ""):
        raise HTTPException(403, "bad signature")
    try:
        body = json.loads(raw.decode() or "{}")
    except Exception:
        raise HTTPException(400, "bad json")
    now = now_iso()
    con = db()
    ids = []
    try:
        for entry in body.get("entry", []):
            for ch in entry.get("changes", []):
                v = ch.get("value", {})
                lgid = v.get("leadgen_id", "")
                # Meta retries a webhook when we 500 — never double-create
                if lgid:
                    dup = con.execute(
                        f"SELECT id FROM leads WHERE company_id={Q}"
                        f" AND meta_leadgen_id={Q} LIMIT 1",
                        (cid, lgid)).fetchone()
                    if dup:
                        ids.append(dup["id"])
                        continue
                name, phone, email = "", "", ""
                if lgid and comp.get("meta_page_token"):
                    name, phone, email = enrich_meta_lead(
                        lgid, comp["meta_page_token"])
                try:
                    lid = insert_lead(
                        con, name=name, phone=phone, source="facebook",
                        campaign=v.get("form_id", ""), created_at=now,
                        status="new" if phone else "needs_enrichment",
                        notes=f"leadgen_id={lgid} page_id={v.get('page_id','')}")
                except Exception:
                    # duplicate phone (or any insert conflict) — roll back
                    # the poisoned transaction, treat the webhook as
                    # handled, never 500 back to Meta
                    con.rollback()
                    continue
                con.execute(f"UPDATE leads SET company_id={Q}, stage='new',"
                            f" email={Q}, last_activity_at={Q},"
                            f" meta_leadgen_id={Q} WHERE id={Q}",
                            (cid, email, now, lgid, lid))
                ids.append(lid)
                log_activity_inline(con, cid, lid, "lead",
                                    "Lead from Facebook",
                                    (f"form {v.get('form_id', '')}"
                                     + (f" · {name}" if name else "")))
                con.commit()
                # instant outbound: call the lead the moment it lands
                if phone and comp.get("meta_autodial") and not DEMO_MODE:
                    row = con.execute(
                        f"SELECT dnc FROM leads WHERE id={Q}",
                        (lid,)).fetchone()
                    cap = comp.get("meta_daily_cap") or 50
                    if row and row["dnc"]:
                        log_activity(cid, lid, "auto_dial",
                                     "Auto-dial skipped", "Do-Not-Call")
                    elif auto_dials_today(cid) >= cap:
                        log_activity(cid, lid, "auto_dial",
                                     "Auto-dial skipped",
                                     f"daily cap ({cap}) reached")
                    else:
                        ok, msg = dial_now(cid, phone, lid, name)
                        log_activity(cid, lid, "auto_dial",
                                     f"Auto-dial {'placed' if ok else 'failed'}",
                                     msg)
    finally:
        con.close()
    return {"ok": True, "leads": ids}


@app.get("/api/integrations/meta")
def meta_settings(req: Request):
    """Meta Lead Ads wiring: webhook URL, token status, auto-dial + cap."""
    cid = _cid(req)
    comp = get_company(cid) or {}
    host = str(req.base_url).rstrip("/")
    return {
        "connected": bool(comp.get("meta_page_token")),
        "webhook_url": f"{host}/api/webhooks/facebook?company={cid}",
        "verify_token_set": bool(FB_VERIFY_TOKEN),
        "app_secret_set": bool(comp.get("meta_app_secret")),
        "autodial": bool(comp.get("meta_autodial")),
        "daily_cap": comp.get("meta_daily_cap") or 50,
        "auto_dials_today": auto_dials_today(cid),
    }


@app.patch("/api/integrations/meta")
async def meta_settings_update(req: Request):
    """Save Page token (enables lead enrichment), auto-dial toggle, daily cap."""
    cid = _cid(req)
    body = await req.json()
    sets, vals = [], []
    if "page_token" in body and isinstance(body["page_token"], str):
        sets.append(f"meta_page_token={Q}")
        vals.append(body["page_token"].strip())
    if "app_secret" in body and isinstance(body["app_secret"], str):
        # Meta app secret → enables X-Hub-Signature-256 verification so
        # nobody can inject fake leads and burn call credits
        sets.append(f"meta_app_secret={Q}")
        vals.append(body["app_secret"].strip())
    if "autodial" in body:
        sets.append(f"meta_autodial={Q}")
        vals.append(1 if body["autodial"] else 0)
    if "daily_cap" in body:
        try:
            cap = max(1, min(500, int(body["daily_cap"])))
        except (TypeError, ValueError):
            cap = 50
        sets.append(f"meta_daily_cap={Q}")
        vals.append(cap)
    if not sets:
        raise HTTPException(400, "nothing to update")
    con = db()
    con.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id={Q}",
                (*vals, cid))
    con.commit()
    con.close()
    log_activity(cid, None, "settings", "Meta integration updated", "")
    return {"ok": True}


@app.get("/api/integrations/meta/activity")
def meta_activity(req: Request):
    """Recent auto-dial events for the Meta integration view."""
    cid = _cid(req)
    con = db()
    rows = con.execute(
        f"SELECT a.created_at, a.title, a.detail, l.name"
        f" FROM activities a LEFT JOIN leads l ON l.id=a.lead_id"
        f" WHERE a.company_id={Q} AND a.kind='auto_dial'"
        f" ORDER BY a.created_at DESC LIMIT 15", (cid,)).fetchall()
    con.close()
    return {"items": [dict(r) for r in rows]}


if DEMO_MODE:
    seed_demo()


app.mount("/static", __import__("fastapi.staticfiles", fromlist=["StaticFiles"]).StaticFiles(
    directory=str(BASE_DIR / "static")), name="static")
