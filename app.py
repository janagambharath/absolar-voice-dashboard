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
import csv
import hashlib
import hmac
import json
import math
import os
import re
import secrets
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
                Q, seed_company, backfill_company, USE_PG)

import smallest_atoms
from smallest_atoms import SmallestError

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
AGENT_CACHE_S = 300
DEFAULT_COMPANY = "ab-solar"

app = FastAPI(title="REvorax Voice-AI CRM")


@asynccontextmanager
async def lifespan(app):
    if not DEMO_MODE and SPEKO_API_KEY:
        asyncio.create_task(refresh_calls_from_speko())
        asyncio.create_task(refresh_calls_from_smallest())
        asyncio.create_task(refresh_billing_cache())
    # a restart must never leave a campaign stuck in "running"
    try:
        con = db()
        con.execute("UPDATE campaigns SET status='paused'"
                    " WHERE status='running'")
        con.commit()
        con.close()
    except Exception:
        pass
    yield


app.router.lifespan_context = lifespan


@app.middleware("http")
async def basic_auth(request: Request, call_next):
    # Meta's servers can't send Basic credentials, so the Lead Ads webhook
    # must stay public (it has its own verify-token + signature checks).
    if (not DASH_USER or request.url.path == "/api/health"
            or request.url.path == "/api/webhooks/facebook"):
        request.state.role = "operator"
        request.state.company_id = ""
        return await call_next(request)
    role, company_id, ok = "operator", "", False
    auth = request.headers.get("authorization", "")
    if auth.startswith("Basic "):
        try:
            u, p = base64.b64decode(auth[6:]).decode().split(":", 1)
            if hmac.compare_digest(u, DASH_USER) and hmac.compare_digest(p, DASH_PASS):
                ok = True
            else:
                # company-scoped client login: locked to one company,
                # client view always forced on
                con = db()
                row = con.execute(
                    f"SELECT id, password_hash FROM companies WHERE login_id={Q} AND login_id<>''",
                    (u,)).fetchone()
                con.close()
                if row and _verify_password(p, row["password_hash"] or ""):
                    ok, role, company_id = True, "company", row["id"]
        except Exception:
            pass
    if not ok:
        return PlainTextResponse(
            "Login required", 401,
            {"WWW-Authenticate": 'Basic realm="Voice-AI CRM"'})
    request.state.role = role
    request.state.company_id = company_id
    return await call_next(request)


def _hash_password(pw: str, salt: str = "") -> str:
    """PBKDF2-SHA256 password hash. Only the hash is ever stored."""
    salt = salt or secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", pw.encode(),
                             bytes.fromhex(salt), 200_000)
    return f"pbkdf2$200000${salt}${dk.hex()}"


def _verify_password(pw: str, stored: str) -> bool:
    try:
        _, iters, salt, h = (stored or "").split("$")
        dk = hashlib.pbkdf2_hmac("sha256", pw.encode(),
                                 bytes.fromhex(salt), int(iters))
        return hmac.compare_digest(dk.hex(), h)
    except Exception:
        return False


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
# --- client login for ab-solar-live: login_id 'absolar', PBKDF2-SHA256 hash
# of the generated password (plaintext handed to Bharath once, never stored
# anywhere). A company login is locked to its own company with client view
# forced on — set here once, idempotent. ---
con.execute(
    f"UPDATE companies SET login_id='absolar',"
    f" password_hash='pbkdf2$200000$1d34394b5078f5db58b036aacfe9a42e$83f52e8100d94733ece317df5bc9638d565e0678949061eced507d359201716d'"
    f" WHERE id={Q} AND (login_id IS NULL OR login_id='')", (_LIVE_ID,))
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


# Columns whose values are operator secrets — the browser must never see
# them. The integrations GET routes already answer "is it set?" with
# *_set booleans; this does the same for the company list.
_SECRET_SUFFIXES = ("_key", "_token", "_secret")


def _public_company(d: dict) -> dict:
    """Company JSON safe for the UI: drop every *_key/*_token/*_secret
    column, replacing each with a has_<col> boolean so the UI can show
    'Set' pills without ever receiving the value."""
    if not d:
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(k, str) and (k.endswith(_SECRET_SUFFIXES)
                                   or k == "password_hash"):
            out[f"has_{k}"] = bool(v)
        else:
            out[k] = v
    return out


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
        ("agent", "సరే, thank you."),
    ],
    "callback": [
        ("agent", "హలో... ఆ, {name} మాట్లాడుతున్నారా?"),
        ("lead", "అవునండి."),
        ("agent", "నేను Sri Surya Solar నుండి Priya ని మాట్లాడుతున్నాను. మీరు enquiry చేసారు కదా."),
        ("lead", "అవునండి, కానీ ఇప్పుడు busy గా ఉన్నాను. సాయంత్రం చేయండి."),
        ("agent", "సరే, సాయంత్రం మళ్ళీ call చేస్తాను."),
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
        "quality_flags": scan_quality(lines, analysis, dur or 0),
        "cost": det.get("totalCostUsd") or 0,
        "has_recording": 1 if (det.get("recordingStatus") or "") == "ready"
        else 0,
        "usage": json.dumps(det.get("usage") or []),
        "analysis": analysis,
        # stamped by dial_now(): only dashboard-placed outbound calls
        # (manual + Meta auto-dial) qualify for WhatsApp follow-up
        "dial_source": md.get("source") or "",
        "campaign_id": md.get("campaign_id") or "",
        "campaign_kind": md.get("campaign_kind") or "",
    }


async def _persist_synced_call(con, sid, p, fallback_company, agent_company,
                               provider="speko"):
    """Store one enriched synced call + run post-call bookkeeping.

    Shared by the Speko and Smallest background syncs. ``p`` is the
    enriched-call dict both syncs build (same keys). ``agent_company``
    maps voice-agent ids -> company ids. Returns True when stored.
    """
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
        f"UPDATE calls SET company_id={Q}, provider={Q}, outcome={Q},"
        f" summary={Q}, structured_json={Q}, transcript={Q},"
        f" cost_usd={Q}, has_recording={Q}, to_number={Q},"
        f" status={Q}, duration_seconds={Q}, usage_json={Q},"
        f" talk_ratio={Q}, objections_json={Q}, next_action={Q},"
        f" disposition={Q}, intent_verdict={Q},"
        f" intent_confidence={Q}, campaign_id={Q},"
        f" quality_flags={Q} WHERE id={Q}",
        (cid, provider, p["outcome"], p["summary"],
         p["structured"], p["transcript"], p["cost"],
         p["has_recording"], p["to"], p["status"],
         p["duration"], p["usage"], an["talk_ratio"],
         json.dumps(an["objections"]), an["next_action"],
         an["disposition"], an["intent"],
         an["intent_confidence"], p["campaign_id"],
         json.dumps(p["quality_flags"]), sid),
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
        # post-call WhatsApp follow-up, once per call, only
        # for dashboard-placed outbound (manual + auto-dial)
        # campaign bookkeeping first so the WhatsApp variant
        # knows the kind (review link / payment amount)
        camp = None
        if p.get("campaign_id"):
            camp = _campaign_ctx(con, p["campaign_id"], lid)
            if camp:
                con.execute(
                    f"UPDATE campaign_leads SET status='done',"
                    f" called_at={Q} WHERE campaign_id={Q}"
                    f" AND lead_id={Q}",
                    (now_iso(), p["campaign_id"], lid))
                con.execute(
                    f"UPDATE campaigns SET"
                    f" done_count=done_count+1 WHERE id={Q}",
                    (p["campaign_id"],))
                if an["intent"] in ("buying", "callback",
                                    "curious"):
                    con.execute(
                        f"UPDATE campaigns SET interested_count="
                        f"interested_count+1 WHERE id={Q}",
                        (p["campaign_id"],))
        if p.get("dial_source") == "dashboard_manual":
            await maybe_whatsapp_followup(
                con, cid, sid, lead, p, camp)
    return True


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
        # the voice agent that placed it (never the default company).
        # Covers both providers: Speko agent ids and Smallest agent ids.
        agent_company = {}
        for c in con.execute(
                "SELECT id, speko_agent_id, smallest_agent_id"
                " FROM companies").fetchall():
            if c["speko_agent_id"]:
                agent_company[c["speko_agent_id"]] = c["id"]
            if c["smallest_agent_id"]:
                agent_company[c["smallest_agent_id"]] = c["id"]
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
                if await _persist_synced_call(con, sid, p, fallback_company,
                                              agent_company, "speko"):
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


async def refresh_calls_from_smallest():
    """Background sync: new Smallest conversations only.

    Mirrors refresh_calls_from_speko(): per-company agent, new ids only,
    same enrichment + analysis + bookkeeping via _persist_synced_call,
    stored with provider='smallest'. Read-only against the Smallest API.
    """
    if DEMO_MODE:
        return {"skipped": True}
    con = db()
    comps = [dict(r) for r in con.execute(
        "SELECT id, smallest_api_key, smallest_agent_id FROM companies"
        " WHERE smallest_api_key <> '' AND smallest_agent_id <> ''"
    ).fetchall()]
    known = {row["id"] for row in
             con.execute("SELECT id FROM calls").fetchall()}
    agent_company = {}
    for c in con.execute(
            "SELECT id, speko_agent_id, smallest_agent_id"
            " FROM companies").fetchall():
        if c["speko_agent_id"]:
            agent_company[c["speko_agent_id"]] = c["id"]
        if c["smallest_agent_id"]:
            agent_company[c["smallest_agent_id"]] = c["id"]
    con.close()
    if not comps:
        return {"skipped": True}
    comp0 = get_company(DEFAULT_COMPANY) or {}
    fallback_company = comp0.get("id") or DEFAULT_COMPANY
    total_new = 0
    for comp in comps:
        cid, key = comp["id"], comp["smallest_api_key"]
        agent_id = comp["smallest_agent_id"]
        try:
            logs = await asyncio.to_thread(
                smallest_atoms.list_calls, key, agent_id, 100)
        except SmallestError:
            continue
        except Exception:
            continue
        todo = [x for x in logs
                if x.get("id") and x["id"] not in known]
        if not todo:
            continue
        con = db()
        try:
            for log in todo:
                sid = log["id"]
                try:
                    det = await asyncio.to_thread(
                        smallest_atoms.get_call, key, sid)
                except Exception:
                    continue
                lines = det.get("transcript") or []
                variables = det.get("variables") or log.get("variables") or {}
                status = det.get("status") or log.get("status") or ""
                reason = (det.get("disconnection_reason")
                          or log.get("disconnection_reason") or "")
                dur = det.get("duration") or log.get("duration") or 0
                outcome = (reason or status
                           or ("connected" if (dur or 0) > 0 else "unknown"))
                analysis = analyze_call(outcome, "", lines, {})
                p = {
                    "to": det.get("to") or log.get("to") or "",
                    "company_id": cid,
                    "agent_id": (det.get("agent_id") or log.get("agent_id")
                                 or agent_id),
                    "meta_lead_id": variables.get("lead_id") or "",
                    "status": status,
                    "started_at": (det.get("started_at")
                                   or log.get("started_at") or ""),
                    "duration": dur,
                    "outcome": outcome,
                    "summary": "",
                    "structured": json.dumps({}),
                    "transcript": json.dumps(lines, ensure_ascii=False),
                    # Smallest bills in credits; per-call USD cost is not
                    # exposed, so keep cost_usd at 0 rather than store a
                    # number in an unknown unit.
                    "cost": 0,
                    "has_recording": 1 if det.get("recording_url") else 0,
                    "usage": "[]",
                    "analysis": analysis,
                    "dial_source": variables.get("source") or "",
                    "campaign_id": variables.get("campaign_id") or "",
                    "campaign_kind": variables.get("campaign_kind") or "",
                    "quality_flags": scan_quality(lines, analysis, dur or 0),
                }
                if await _persist_synced_call(con, sid, p, cid,
                                              agent_company, "smallest"):
                    total_new += 1
                    known.add(sid)
            con.commit()
        finally:
            con.close()
    kv_set("last_sync_smallest", now_iso())
    kv_set("last_sync_smallest_new", str(total_new))
    return {"new": total_new}


# ------------------------------------------------- call analysis ------
# Speko's report gives us summary / outcome / structured_data. On top of
# that we derive intent, objection signals, talk ratio and a suggested
# next action — all from the transcript, no extra LLM calls.

OBJECTION_KEYWORDS = {
    "price": ["price", "cost", "expensive", "costly", "rate", "charge",
              "ధర", "రేటు", "డబ్బు", "ఖర్చు",
              "dhara", "dabbu", "kharchu", "karchu"],
    "trust": ["scam", "fraud", "fake", "cheat", "నమ్మకం", "మోసం",
              "mosam"],
    "timing": ["later", "busy", "not now", "తర్వాత", "తరువాత",
               "రేపు", "tarvata", "taruvata", "repu"],
    "roof": ["rent", "rented", "apartment", "flat", "అద్దె", "adde"],
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


# Bilingual intent keywords: English + Telugu script + Roman Telugish.
# Speko's outcome/summary for a Telugu call frequently comes back in
# Telugu, so English-only matching silently classified every Telugu
# call as "unknown" — callbacks never scheduled, DNC never honored.
_KW_NOT_INTERESTED = (
    "not interested", "declined", "not_interested",
    "not qualified", "not_qualified", "unqualified",
    "వద్దు", "వద్దండి", "అవసరం లేదు", "ఆసక్తి లేదు",
    "ఇష్టం లేదు", "interest లేదు",
    "vaddu", "vaddandi", "avasaram ledu", "interest ledu",
)
_KW_DNC = (
    "dnc", "do not call", "not to call", "don't call", "dont call",
    "never call",
    "కాల్ చేయవద్దు", "కాల్ చేయొద్దు",
    "మళ్లీ చేయవద్దు", "మళ్ళీ చేయవద్దు", "మళ్లీ కాల్ వద్దు",
    "call cheyavaddu", "call cheyoddu", "malli cheyavaddu",
)
_KW_CALLBACK = (
    "callback", "call back", "call me", "busy", "later",
    "తర్వాత", "తరువాత", "సాయంత్రం", "రేపు",
    "మళ్లీ చేయండి", "మళ్ళీ చేయండి", "మళ్లీ కాల్ చేయండి",
    "tarvata", "taruvata", "sayantram", "sayankalam", "repu",
    "malli cheyandi",
)
_KW_BUYING = (
    "survey", "book", "site visit", "qualified",
    "సర్వే", "బుక్", "సైట్ విజిట్", "సర్వేకి రండి",
    "survey ki randi",
)
_KW_NO_ANSWER = (
    "no answer", "no-answer", "no_answer", "voicemail",
    "not reachable", "ఎత్తలేదు", "లిఫ్ట్ చేయలేదు",
)
_KW_WRONG_NUMBER = (
    "wrong number", "wrong_number", "రాంగ్ నంబర్", "తప్పు నంబర్",
)
_KW_CURIOUS = (
    "interested", "curious", "details", "price", "cost", "subsidy",
    "ఎంత", "ధర", "వివరాలు", "సబ్సిడీ",
    "entha", "dhara", "vivaralu",
)


def _classify(text):
    """Single-pass intent ladder. Returns (intent, confidence,
    disposition, next_action). Order matters: terminal negatives first,
    then callback (so 'book a callback' never reads as buying)."""
    if any(k in text for k in _KW_NOT_INTERESTED + _KW_DNC):
        disp = ("dnc" if any(k in text for k in _KW_DNC)
                else "not_interested_timing")
        nxt = "Suppress (DNC)" if disp == "dnc" else "Move to Nurture"
        return "not_interested", 0.9, disp, nxt
    if any(k in text for k in _KW_CALLBACK):
        return "callback", 0.85, "qualified_callback", "Schedule callback"
    if any(k in text for k in _KW_BUYING):
        return "buying", 0.85, "qualified_survey", "Book site survey"
    if any(k in text for k in _KW_NO_ANSWER):
        return "unknown", 0.9, "no_answer", "Retry tomorrow"
    if any(k in text for k in _KW_WRONG_NUMBER):
        return "not_interested", 0.9, "wrong_number", "Mark lost"
    if any(k in text for k in _KW_CURIOUS):
        return "curious", 0.7, "interested_followup", "Send details on WhatsApp"
    if any(k in text for k in ("connected", "answered")):
        return "curious", 0.55, "interested_followup", "Review transcript"
    return "unknown", 0.5, "failed", "Review transcript"


def analyze_call(outcome, summary, lines, structured):
    """Derive intent verdict, objections, next action, suggested
    disposition from the outcome + transcript. Speko's own summary and
    outcome are kept verbatim; everything derived is labeled as such.

    Two tiers: Speko's report first; if it yields nothing, fall back to
    the lead's own words. Agent speech is excluded from the fallback —
    questions like 'మీకు interest ఉందా?' must not count as intent."""
    s = f"{outcome} {summary}".lower()
    intent, conf, disp, nxt = _classify(s)
    # The lead's own terminal words override a weak report guess: a
    # vague/weak summary must not drown out the lead actually saying
    # "వద్దు". Only 0.9 verdicts (explicit negative, no-answer, wrong
    # number) are final.
    if conf < 0.9:
        lead_text = " ".join(
            l.get("text", "") for l in lines
            if l.get("speaker") == "lead").lower()
        if lead_text.strip():
            i2, c2, d2, n2 = _classify(lead_text)
            if i2 in ("not_interested", "callback", "buying"):
                intent, conf, disp, nxt = i2, min(c2, 0.8), d2, n2
    return {
        "intent": intent, "intent_confidence": conf,
        "objections": detect_objections(lines),
        "talk_ratio": talk_ratio(lines),
        "next_action": nxt, "disposition": disp,
        "derived": True,  # UI labels these "detected signals"
    }


# Call-quality monitor: heuristic flags scanned from every transcript so
# Karthik sees quality at a glance instead of worrying blindly.
_BANNED_ENGLISH = (
    "sorry", "could you", "didn't catch", "did not catch", "pardon",
    "come again", "repeat that", "say that again", "i didn't understand",
    "couldn't understand", "can you hear me",
)


def scan_quality(lines, analysis, duration):
    """Return a list of quality flag strings; empty means clean."""
    flags = []
    agent_text = " ".join(
        l.get("text", "") for l in lines
        if l.get("speaker") == "agent").lower()
    lead_words = " ".join(
        l.get("text", "") for l in lines
        if l.get("speaker") == "lead").split()
    if any(b in agent_text for b in _BANNED_ENGLISH):
        flags.append("english_fallback")
    if analysis.get("disposition") == "no_answer":
        return flags
    if len(lead_words) < 4:
        flags.append("lead_silent")
    if duration and duration > 30 \
            and (analysis.get("talk_ratio") or 0) < 0.15:
        flags.append("agent_monologue")
    if duration and duration < 15:
        flags.append("too_short")
    return flags


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


async def refresh_smallest_billing_cache(company_id="", force=False):
    """Smallest prepaid credit balance, cached like the Speko billing
    cache. Uses the company's own Smallest key (default company first)."""
    try:
        ts = float(kv_get("smallest_billing_cache_ts") or 0)
    except Exception:
        ts = 0
    if not force and time.time() - ts < BILLING_CACHE_S:
        try:
            return json.loads(kv_get("smallest_billing_cache") or "null")
        except Exception:
            pass
    comp = get_company(company_id or DEFAULT_COMPANY) or {}
    key = comp.get("smallest_api_key") or ""
    if not key and not company_id:
        # fall back to any company that has a Smallest key configured
        con = db()
        r = con.execute(
            "SELECT smallest_api_key FROM companies"
            " WHERE smallest_api_key <> '' LIMIT 1").fetchone()
        con.close()
        key = (r["smallest_api_key"] if r else "") or ""
    if DEMO_MODE or not key:
        return None
    try:
        bal = await asyncio.to_thread(smallest_atoms.get_balance, key)
        out = {
            "credit_balance": bal["credit_balance"],
            "credit_balance_inr": round(
                bal["credit_balance"] * USD_INR, 2),
            "plan_id": bal["plan_id"],
            "fetched_at": now_iso(),
        }
        kv_set("smallest_billing_cache", json.dumps(out))
        kv_set("smallest_billing_cache_ts", str(time.time()))
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
def companies(req: Request):
    con = db()
    rows = con.execute("SELECT * FROM companies ORDER BY created_at").fetchall()
    con.close()
    out = [_public_company(dict(r)) for r in rows]
    if getattr(req.state, "role", "") == "company":
        # a client login sees only its own company, always in client view —
        # the Integrations nav, wallet and toggle can never appear for it
        out = [c for c in out if c["id"] == req.state.company_id]
        for c in out:
            c["hide_integrations"] = 1
    # secrets (provider API keys/tokens) never leave the server
    return out


@app.patch("/api/companies/{target_id}")
async def update_company(target_id: str, req: Request):
    """Operator-only: flip client view (hide_integrations) or set the
    client billing rate for a company. Gated on the CURRENT company
    BEFORE applying the update — a client company can never un-hide
    itself or change its own rate."""
    _assert_integrations_visible(req, _cid(req))
    body = await req.json()
    sets, vals = [], []
    if "hide_integrations" in body:
        sets.append(f"hide_integrations={Q}")
        vals.append(1 if body["hide_integrations"] else 0)
    if "rate_per_min" in body:
        try:
            rate = float(body["rate_per_min"])
        except (TypeError, ValueError):
            raise HTTPException(400, "rate_per_min must be a number")
        if rate < 0 or rate > 10000:
            raise HTTPException(400, "rate_per_min out of range")
        sets.append(f"rate_per_min={Q}")
        vals.append(rate)
    if not sets:
        raise HTTPException(400, "nothing to update")
    con = db()
    row = con.execute(f"SELECT id FROM companies WHERE id={Q}",
                      (target_id,)).fetchone()
    if not row:
        con.close()
        raise HTTPException(404, "company not found")
    con.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id={Q}",
                (*vals, target_id))
    con.commit()
    out = con.execute(f"SELECT * FROM companies WHERE id={Q}",
                      (target_id,)).fetchone()
    con.close()
    return _public_company(dict(out))


def _cid(req: Request, body_company=""):
    # a company-scoped login is locked to its own company — the ?company=
    # param / body field is ignored for them
    if getattr(req.state, "role", "") == "company" and getattr(req.state, "company_id", ""):
        return req.state.company_id
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
# Per-call personalization: production Meta leads get their OWN name and
# details in the greeting + LEAD block, via Speko's per-call firstMessage
# and systemPrompt overrides. The agent's stored prompt is only a fallback.
SOLAR_PROMPT_TEMPLATE = """RULE 0 — NO-INPUT / UNCLEAR AUDIO (highest priority — read this first, it overrides everything below):
If the caller's audio is empty, silent, or you cannot understand it, your ENTIRE response must be exactly this one sentence, with no other words before or after it: "సారీ, కొంచెం మళ్ళీ చెప్తారా?"
Never translate it. Never rephrase it. Never say it in English. There is no other correct response to unclear audio.

LANGUAGE (ABSOLUTE RULE — ఇది కింద ఉన్న అన్నిటికంటే ముఖ్యం):
- నువ్వు మాట్లాడే ప్రతి పదం Teluguలోనే ఉండాలి. Englishలో ఒక్క పదం కూడా పలకవద్దు — ఒక్క "sorry" కూడా వద్దు.
- అర్థం కాకపోతే, వినపడకపోతే, లేదా ఏమైనా doubt వస్తే — ఈ ఒక్క line మాత్రమే వాడు, అక్షరం పొల్లుపోకుండా: "సారీ, కొంచెం మళ్ళీ చెప్తారా?"
- BANNED — ఇవి ఎప్పుడూ, ఏ పరిస్థితిలోనూ వాడవద్దు: "Sorry, I didn't quite catch that", "could you say that again", "could you repeat that", "I didn't understand", "please repeat", లేదా ఏ English sentence అయినా.
- BANNED word: "అండి" — ఎప్పుడూ వాడవద్దు, ఏ రూపంలోనూ. Respect గారు/మీరుతో చూపించు.

నువ్వు Priya — 28 ఏళ్ల అమ్మాయి, Dilsukhnagarలో ఉంటావు, Sri Surya Solarలో 2 years నుంచి customersతో మాట్లాడుతున్నావు. Enquiry ఇచ్చిన వాళ్లకి call చేసి, details confirm చేసి, qualify అయితే "మా team contact చేస్తారు" అని చెప్పి close చేస్తావు. తక్కువ మాట్లాడు — అవసరమైనది మాత్రమే.

LEAD (నీకు ముందే తెలుసు — మళ్లీ అడగవద్దు): {lead_block}

STYLE:
- Telangana Telugu: చేస్తా, అయితది, ఎట్లా, ఏంటి.
- Max 1-2 చిన్న sentences per turn. Yapping వద్దు.
- No "ha, huuh, umm" fillers — confident, direct.
- Language: పైన LANGUAGE rule చూడు — Telugu మాత్రమే, exceptions లేవు.
- DELIVERY: మనిషిలా మాట్లాడు — sentenceల మధ్య soft breath pause తీసుకో, unhurried, warm. Clipped/robotic వద్దు.

FLOW:
1. "{greeting_line}"
   Wrong person: "సరే,{lead_name} ఎప్పుడు దొరుకుతారో చెప్తారా?" → end.
2. "నేను Priyaని, Sri Surya Solar నుంచి. మీ rooftop solar enquiry గురించి — terrace మీద panelsకి space ఉందా?"
   (LEAD blockలో property type చూసి, flat/apartment అయితే terrace access ఉందా అని అడుగు.)
   లేదు: → NOT QUALIFIED.
3. "నెలకి current bill ఎంత వస్తుంది?"
4. "మీ enquiry qualified అయింది. మా team site survey కోసం contact చేస్తారు. సరే{lead_name}." → end.

NOT QUALIFIED: "మీ requirement అర్థమైంది — కానీ rooftop solar మీకు suit అవ్వదు. Enquiry ఇక్కడితో close చేస్తున్నా." → end.

QUESTIONS:
- Price: "System size బట్టి మారతది. Site survey తర్వాత exact quotation ఇస్తాం."
- Subsidy: "Government subsidy options ఉన్నాయి — team details చెప్తారు."
- Warranty: "Panelsకి 25 years performance warranty వస్తది."
- AIనా: "అవును, నేను Sri Surya Solar AI assistantని. Humanతో మాట్లాడాలంటే arrange చేస్తా."
- తెలియనిది: "అది... అంటే ఒక్కసారి check చేసి చెప్తా."

SITUATIONS:
- Busy: "అయ్యో, పర్లేదు. మరి ఎప్పుడు చేస్తే కుదురుతది?"
- WhatsApp: "సరే, details WhatsAppలో పంపిస్తా." → ఆపు.
- "ఆలోచించి చెప్తా": "సరే. మీకు వీలైనప్పుడు చెప్పండి."
- "Interest లేదు": "సరే, పర్లేదు." → end.

GUARDRAILS: Price, subsidy, company name — verified info మాత్రమే. Savings guarantee వద్దు. "No" = full stop.
"""


# ------------------------------------------------- campaigns ------------
def _campaign_ctx(con, campaign_id, lead_id):
    """Load {kind, params} for a campaign call (WhatsApp variant needs
    the review link / payment amount). Merges campaign-level params with
    per-lead params."""
    try:
        c = con.execute(
            f"SELECT kind, params_json FROM campaigns WHERE id={Q}",
            (campaign_id,)).fetchone()
        if not c:
            return None
        params = json.loads(c["params_json"] or "{}")
        cl = con.execute(
            f"SELECT params_json FROM campaign_leads"
            f" WHERE campaign_id={Q} AND lead_id={Q}",
            (campaign_id, lead_id)).fetchone()
        if cl:
            params.update(json.loads(cl["params_json"] or "{}"))
        return {"kind": c["kind"], "params": params}
    except Exception:
        return None


# One engine, four painkillers. Each kind injects a CAMPAIGN BRIEF block
# at the top of the per-call prompt (via personalize_solar_call) so the
# same Priya agent runs revival / survey-confirm / review / payment
# calls without any agent-config change.
CAMPAIGN_KINDS = {
    "revival": {
        "label": "Dead-lead revival",
        "desc": "Cold old leads → reactivation call, zero ad spend",
        "brief": (
            "CAMPAIGN: DEAD-LEAD REVIVAL. This person enquired about solar "
            "long ago and went cold. Do NOT pretend you spoke recently. "
            "Opener angle: 'మీ area లో కొత్త subsidy benefits వచ్చాయి, "
            "అందుకే మళ్ళీ call చేస్తున్నాం'. Goal: re-qualify fast "
            "(independent house + roof + monthly bill), then close with "
            "'మా team contact చేస్తారు'. If they already installed solar "
            "or say not interested, accept gracefully and end warmly."
        ),
    },
    "survey_confirm": {
        "label": "Survey confirmation",
        "desc": "Confirm the site survey, kill technician no-shows",
        "brief": (
            "CAMPAIGN: SURVEY CONFIRMATION. This lead already booked a free "
            "site survey. Your ONLY job: confirm they will be available. "
            "Ask: 'మా technician survey కి వస్తారు — మీరు ఇంట్లో ఉంటారు "
            "కదా?'. If yes: confirm warmly and end. If no: ask which day "
            "works instead and note it. Do NOT re-sell solar, do NOT "
            "re-qualify — this call is 60 seconds max."
        ),
    },
    "review": {
        "label": "Review + referral",
        "desc": "Post-install: Google review ask + neighbour referral",
        "brief": (
            "CAMPAIGN: REVIEW + REFERRAL. This customer's solar installation "
            "is already done. Thank them warmly first. Then ask TWO things: "
            "(1) 'మీ experience ఎలా ఉంది?' — if happy, tell them we will "
            "WhatsApp the Google review link. (2) 'మీ neighbours లేదా "
            "friends లో ఎవరికైనా solar కావాలంటే మా number share చేయండి'. "
            "Keep it short and grateful — never pushy, never salesy."
        ),
    },
    "payment": {
        "label": "Payment reminder",
        "desc": "Polite pending-payment follow-up call",
        "brief": (
            "CAMPAIGN: PAYMENT REMINDER. This customer has a pending "
            "payment{amount_phrase}. Be POLITE and brief — never "
            "threatening, never embarrassing. Opener: '{name} గారు, మీ "
            "pending payment గురించి gentle reminder కోసం call "
            "చేస్తున్నాం'. Ask when they can complete it. If they promise "
            "a date, note it warmly and end. If they raise an installation "
            "issue, note it and say our team will resolve it first."
        ),
    },
}


def personalize_solar_call(lead, campaign_kind=None, campaign_params=None):
    """Build per-call (first_message, system_prompt) from a lead dict.

    lead keys used: name, city, property_type, monthly_bill_inr, source.
    Missing fields are simply omitted — never shown as blank/unknown.
    campaign_kind injects a CAMPAIGN BRIEF block overriding the default
    new-lead flow (revival / survey_confirm / review / payment).
    """
    lead = lead or {}
    name = (lead.get("name") or "").strip()
    disp_name = name or ""
    # lead_name carries its own leading space (" Deva" / "") so template
    # lines read naturally with or without a name: "సరే, Deva ఎప్పుడు…"
    lead_name = f" {name}" if name else ""
    if name:
        greeting = f"హలో... ఆ, {name} మాట్లాడుతున్నారా?"
    else:
        greeting = "హలో... నమస్తే?"
    facts = []
    if name:
        facts.append(name)
    city = (lead.get("city") or "").strip()
    if city:
        facts.append(city)
    ptype = (lead.get("property_type") or "").strip()
    if ptype:
        facts.append(ptype)
    bill = lead.get("monthly_bill_inr") or 0
    try:
        bill = float(bill)
    except (TypeError, ValueError):
        bill = 0
    if bill > 0:
        facts.append(f"నెలకి ₹{int(bill):,} current bill")
    src = (lead.get("source") or "").strip()
    if src and src not in ("manual",):
        facts.append({"facebook": "Facebook ad"}.get(src, src))
    lead_block = " | ".join(facts) if facts else "details తెలియవు — politely అడిగి తెలుసుకో"
    prompt = SOLAR_PROMPT_TEMPLATE.format(
        lead_block=lead_block, greeting_line=greeting, lead_name=lead_name)
    if campaign_kind and campaign_kind in CAMPAIGN_KINDS:
        params = dict(campaign_params or {})
        params.setdefault("name", disp_name)
        amt = params.get("amount")
        try:
            params["amount_phrase"] = (f" of ₹{int(float(amt)):,}"
                                       if amt else "")
        except (TypeError, ValueError):
            params["amount_phrase"] = ""
        brief = CAMPAIGN_KINDS[campaign_kind]["brief"].format(**params)
        prompt = (f"CAMPAIGN BRIEF (highest priority — overrides the "
                  f"default flow below):\n{brief}\n\n---\n{prompt}")
    return greeting, prompt


# ------------------------------------------------- whatsapp follow-up ---
# After a dashboard-placed call ends, the lead gets a WhatsApp template
# message: missed-call nudge when they didn't pick up, next-step nudge
# when they showed interest. Business-initiated messages REQUIRE an
# approved template, so the body is fixed as:
#   "నమస్తే {{1}}, AB Solar. {{2}}"
# {{1}} = lead name (or అండి), {{2}} = one of _WA_BODY below.
# The template (name stored per-company as wa_template) must be created
# and approved in WhatsApp Manager before enabling.
WA_API_VERSION = "v21.0"

_WA_BODY = {
    "no_answer": "మీకు కాల్ చేసాం — అందలేదు. సోలార్ గురించి మాట్లాడాలనుకుంటే ఈ నంబర్‌కే రిప్లై ఇవ్వండి, మా team కాల్ చేస్తుంది.",
    "callback": "మీరు అడిగినట్టు మా team మళ్ళీ కాల్ చేస్తుంది. ముందుగా ఏమైనా డౌట్స్ ఉంటే ఇక్కడే అడగండి.",
    "buying": "మీ interest కి ధన్యవాదాలు! Free site survey book చేసుకోవడానికి 'SURVEY' అని రిప్లై ఇవ్వండి.",
    "curious": "సోలార్ పై మరిన్ని వివరాలు కావాలంటే రిప్లై ఇవ్వండి — subsidy, ధర, EMI అన్నీ క్లియర్‌గా చెప్తాం.",
}


async def wa_send_template(pnid, token, template, to_digits, params):
    """Send one WhatsApp template message via the Cloud API."""
    url = (f"https://graph.facebook.com/{WA_API_VERSION}/{pnid}"
           f"/messages")
    payload = {
        "messaging_product": "whatsapp",
        "to": to_digits,
        "type": "template",
        "template": {
            "name": template,
            "language": {"code": "te"},
            "components": [{
                "type": "body",
                "parameters": [{"type": "text", "text": p}
                               for p in params],
            }],
        },
    }
    async with httpx.AsyncClient(timeout=25) as c:
        r = await c.post(
            url,
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"},
            json=payload)
    return r.status_code, r.text[:400]


async def maybe_whatsapp_followup(con, cid, call_id, lead, p, camp=None):
    """Fire the post-call WhatsApp follow-up exactly once per call.

    camp = {"kind": campaign_kind, "params": {...}} for campaign calls:
    review campaigns send the Google-review link, payment campaigns
    include the pending amount. Everything else uses _WA_BODY.
    """
    try:
        row = con.execute(
            f"SELECT wa_sent FROM calls WHERE id={Q}",
            (call_id,)).fetchone()
        if row and row["wa_sent"]:
            return
        wac = con.execute(
            f"SELECT wa_phone_number_id, wa_token, wa_template,"
            f" wa_enabled FROM companies WHERE id={Q}",
            (cid,)).fetchone()
        wac = dict(wac) if wac else {}
        if not (wac.get("wa_enabled") and wac.get("wa_phone_number_id")
                and wac.get("wa_token") and wac.get("wa_template")):
            return
        an = p["analysis"]
        disp, intent = an["disposition"], an["intent"]
        if disp in ("dnc", "wrong_number") or intent == "not_interested" \
                or (lead.get("dnc") if isinstance(lead, dict) else 0):
            # never WhatsApp someone who said no — mark processed
            con.execute(f"UPDATE calls SET wa_sent=1 WHERE id={Q}",
                        (call_id,))
            return
        key = None
        body2 = None
        kind = (camp or {}).get("kind") or ""
        cparams = (camp or {}).get("params") or {}
        if disp == "no_answer":
            key = "no_answer"
        elif intent == "callback":
            key = "callback"
        elif intent == "buying":
            key = "buying"
        elif intent == "curious":
            key = "curious"
        if kind == "review" and key in ("buying", "curious", "callback"):
            link = (cparams.get("review_link") or "").strip()
            body2 = ("మీ installation కి ధన్యవాదాలు! Google లో మీ review "
                     "మాకు చాలా helpful గా ఉంటుంది"
                     + (f": {link}" if link else ".")
                     + " మీ neighbours కి solar కావాలంటే మా number share "
                       "చేయండి!")
        elif kind == "payment" and key in ("callback", "curious", "buying"):
            amt = cparams.get("amount")
            try:
                amt_s = f" ₹{int(float(amt)):,}" if amt else ""
            except (TypeError, ValueError):
                amt_s = ""
            body2 = (f"మీ pending payment{amt_s} గురించి reminder. "
                     "ఎప్పుడు complete చేస్తారో రిప్లై ఇవ్వండి, మా team "
                     "help చేస్తుంది.")
        elif kind == "survey_confirm" and key in ("buying", "curious",
                                                 "callback"):
            body2 = ("మీ site survey confirm అయింది! మా technician వస్తారు "
                     "— ఏమైనా changes ఉంటే ఈ నంబర్‌కే message చేయండి.")
        if not key and not body2:
            return
        name = (lead.get("name") or "").strip()
        p1 = f"{name} గారు" if name else "అండి"
        d = digits(lead.get("phone") or p.get("to") or "")
        if len(d) == 10:
            d = "91" + d
        if len(d) < 12:
            return
        status, body = await wa_send_template(
            wac["wa_phone_number_id"], wac["wa_token"],
            wac["wa_template"], d, [p1, body2 or _WA_BODY[key]])
        con.execute(f"UPDATE calls SET wa_sent=1 WHERE id={Q}",
                    (call_id,))
        if status in (200, 201):
            log_activity_inline(
                con, cid, lead.get("id"), "whatsapp",
                "WhatsApp follow-up sent",
                (body2 or _WA_BODY[key])[:80] + "…")
        else:
            log_activity_inline(
                con, cid, lead.get("id"), "whatsapp",
                "WhatsApp follow-up failed",
                f"HTTP {status}: {body[:120]}")
    except Exception as e:
        # never break the call sync because WhatsApp hiccuped
        try:
            log_activity_inline(
                con, cid, lead.get("id") if isinstance(lead, dict)
                else None, "whatsapp", "WhatsApp follow-up error",
                f"{type(e).__name__}: {str(e)[:120]}")
        except Exception:
            pass


async def dial_now_smallest(company_id, phone, lead_id=None, name="",
                           lead=None):
    """Place a manual outbound call via Smallest Atoms.

    Returns (ok, message). Only ever invoked from an explicit user dial
    action (dashboard Call button / dialer with provider="smallest").
    The agent prompt lives on the Smallest agent itself; per-call
    personalization travels in ``variables``.
    """
    comp = get_company(company_id) or {}
    key = comp.get("smallest_api_key") or ""
    agent_id = comp.get("smallest_agent_id") or ""
    from_number = comp.get("smallest_from_number") or ""
    if DEMO_MODE:
        return True, f"(demo) call queued to {phone}"
    if not key:
        return False, "no Smallest API key configured for this company"
    if not agent_id:
        return False, "no Smallest agent selected for this company"
    if not from_number:
        return False, "no Smallest caller ID selected for this company"
    to = phone if phone.startswith("+") else f"+91{digits(phone)[-10:]}"
    lead_row = dict(lead) if lead else {}
    if not lead_row.get("name"):
        lead_row["name"] = name
    variables = {"lead_id": str(lead_id or ""),
                 "name": lead_row.get("name") or "",
                 "city": lead_row.get("city") or "",
                 "source": "dashboard_manual",
                 "company_id": company_id}
    # match the E.164 to Smallest's product id so the outbound request
    # carries both identifiers the API accepts (from_number per the
    # developer docs, fromProductId per the official SDK)
    from_product_id = ""
    try:
        numbers = await asyncio.to_thread(smallest_atoms.list_numbers, key)
        for n in numbers:
            if n.get("e164") == from_number and n.get("product_id"):
                from_product_id = n["product_id"]
                break
    except SmallestError:
        pass
    try:
        res = await asyncio.to_thread(
            smallest_atoms.start_outbound_call,
            key, agent_id, to, from_number,
            from_product_id or None, variables)
    except SmallestError as e:
        return False, f"smallest: {e}"
    except Exception as e:
        return False, f"dial failed: {e}"
    sid = (res.get("conversation_id")
           or f"manual-{uuid.uuid4().hex[:8]}")
    con = db()
    insert_call_ignore(con, id=sid, lead_id=lead_id, to_number=to,
                       status="dialing", started_at=now_iso(),
                       duration_seconds=0, demo=0)
    con.execute(f"UPDATE calls SET company_id={Q}, provider={Q}"
                f" WHERE id={Q}", (company_id, "smallest", sid))
    con.commit()
    con.close()
    if lead_id:
        log_activity(company_id, lead_id, "call",
                     f"Manual call placed to {to} (Smallest)", "")
    return True, f"calling {to}…"


def dial_now(company_id, phone, lead_id=None, name="", lead=None,
             campaign_id=None, campaign_kind=None, campaign_params=None):
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
    # per-call personalization: greet with the LEAD's name + details
    lead_row = dict(lead) if lead else {}
    if lead_id and not lead_row.get("name"):
        try:
            c2 = db()
            r = c2.execute(
                f"SELECT name, city, property_type, monthly_bill_inr, source"
                f" FROM leads WHERE id={Q}", (lead_id,)).fetchone()
            c2.close()
            if r:
                lead_row = {"name": r["name"] or name,
                            "city": r["city"] or "",
                            "property_type": r["property_type"] or "",
                            "monthly_bill_inr": r["monthly_bill_inr"] or 0,
                            "source": r["source"] or ""}
        except Exception:
            pass
    if not lead_row.get("name"):
        lead_row["name"] = name
    greeting, prompt = personalize_solar_call(
        lead_row, campaign_kind=campaign_kind,
        campaign_params=campaign_params)
    payload = {"to": to, "agentId": agent_id,
               "firstMessage": greeting, "systemPrompt": prompt,
               "metadata": {"lead_id": lead_id, "name": name,
                            "source": "dashboard_manual",
                            "campaign_id": campaign_id or "",
                            "campaign_kind": campaign_kind or ""}}
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


def dial_primary(company_id, phone, lead_id=None, name="", lead=None,
                 campaign_id=None, campaign_kind=None, campaign_params=None):
    """Smallest primary, Speko backup. Uses Smallest when the company has it
    fully configured (key + agent + caller ID), otherwise Speko.
    Returns (ok, message). Sync — safe for webhook/background paths."""
    comp = get_company(company_id) or {}
    sm_ready = bool(comp.get("smallest_api_key")
                    and comp.get("smallest_agent_id")
                    and comp.get("smallest_from_number"))
    if sm_ready:
        try:
            return asyncio.run(
                dial_now_smallest(company_id, phone, lead_id, name, lead=lead))
        except Exception as e:
            return False, f"smallest dial failed: {e}"
    return dial_now(company_id, phone, lead_id, name, lead=lead,
                    campaign_id=campaign_id, campaign_kind=campaign_kind,
                    campaign_params=campaign_params)


@app.post("/api/dial")
async def manual_dial(req: Request):
    """Manual call trigger from the dashboard (per-lead Call button or
    the dialer). Guards DNC; every dial is logged. ``provider`` is optional:
    when omitted the backend picks Smallest if the company has it fully
    configured (key + agent + caller ID), else the silent Speko backup."""
    body = await req.json()
    cid = _cid(req, body.get("company", ""))
    lead_id = body.get("lead_id")
    phone = (body.get("to") or "").strip()
    provider = str(body.get("provider") or "").strip().lower()
    name = ""
    lead_ctx = None
    if lead_id:
        con = db()
        row = con.execute(f"SELECT phone, name, dnc, company_id, city,"
                          f" property_type, monthly_bill_inr, source"
                          f" FROM leads WHERE id={Q}", (lead_id,)).fetchone()
        con.close()
        if not row:
            raise HTTPException(404, "lead not found")
        if row["dnc"]:
            raise HTTPException(403, "lead is on Do-Not-Call")
        phone, name = row["phone"], row["name"]
        cid = row["company_id"] or cid
        lead_ctx = {"name": row["name"] or "", "city": row["city"] or "",
                    "property_type": row["property_type"] or "",
                    "monthly_bill_inr": row["monthly_bill_inr"] or 0,
                    "source": row["source"] or ""}
    if len(digits(phone)) < 10:
        raise HTTPException(400, "valid phone required")
    # Provider is a backend concern: explicit ``provider`` is honored when
    # given (API/testing), otherwise Smallest when the company has it fully
    # configured, else the silent Speko backup.
    if not provider:
        comp = get_company(cid) or {}
        sm_ready = bool(comp.get("smallest_api_key")
                        and comp.get("smallest_agent_id")
                        and comp.get("smallest_from_number"))
        provider = "smallest" if sm_ready else "speko"
    if provider == "smallest":
        ok, msg = await dial_now_smallest(cid, phone, lead_id, name,
                                          lead_ctx)
    elif provider == "speko":
        ok, msg = dial_now(cid, phone, lead_id, name, lead=lead_ctx)
    else:
        raise HTTPException(400, "unknown provider")
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
    # Proxy the bytes through the dashboard so the recording plays
    # inline in the call drawer instead of opening an external site.
    try:
        head = httpx.head(url, timeout=15, follow_redirects=True)
        media = head.headers.get("content-type", "").split(";")[0].strip()
    except Exception:
        media = ""
    if not media.startswith("audio/"):
        media = "audio/mpeg"

    def gen():
        try:
            with httpx.stream("GET", url, timeout=120,
                              follow_redirects=True) as rs:
                for chunk in rs.iter_bytes(65536):
                    yield chunk
        except Exception:
            return

    return StreamingResponse(gen(), media_type=media)


# ------------------------------------------------------ misc api ------
@app.get("/api/billing")
async def billing(req: Request):
    # Operator wallet — provider balances/costs are never client-visible.
    # Client companies (hide_integrations=1) get the honest bill from
    # /api/billing/client instead.
    _assert_integrations_visible(req, _cid(req))
    if DEMO_MODE or not SPEKO_API_KEY:
        return {
            "demo": True,
            "balance_usd": 142.0, "balance_inr": round(142.0 * USD_INR, 2),
            "total_cost_usd": 8.5, "total_cost_inr": round(8.5 * USD_INR, 2),
            "total_sessions": 8, "total_minutes": 6,
            "smallest": None,
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
    if not cached:
        raise HTTPException(502, "billing unavailable")
    try:
        cached["smallest"] = await refresh_smallest_billing_cache()
    except Exception:
        cached["smallest"] = None
    return cached


@app.get("/api/billing/client")
def client_billing(req: Request):
    """Honest client bill: real synced minutes x the company's rate_per_min.

    Minutes come only from the calls table (BOTH providers, no provider
    filter) for the current calendar month. Nothing is invented — no
    minutes, no balances, no estimates. Provider wallet/cost data is
    never included here; it stays operator-only on /api/billing."""
    cid = _cid(req)
    comp = get_company(cid) or {}
    rate = comp.get("rate_per_min") or 15
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    con = db()
    row = con.execute(
        f"SELECT COALESCE(SUM(duration_seconds),0) s, COUNT(*) n FROM calls"
        f" WHERE company_id={Q} AND substr(started_at,1,7)={Q}",
        (cid, month)).fetchone()
    con.close()
    minutes = round((row["s"] or 0) / 60, 2)
    return {
        "month": month,
        "minutes_this_month": minutes,
        "calls_this_month": row["n"] or 0,
        "rate_per_min": rate,
        "amount_due_inr": round(minutes * rate, 2),
    }


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
    asyncio.create_task(refresh_calls_from_smallest())
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

    Returns (name, phone, email, fields). fields is the raw {field_name:
    value} dict so custom form fields (city, property type, monthly bill…)
    can be mapped to lead columns. Empty strings / {} on any failure —
    the lead is still created, just flagged needs_enrichment."""
    try:
        with httpx.Client(timeout=15) as c:
            r = c.get(f"https://graph.facebook.com/v21.0/{leadgen_id}",
                      params={"access_token": page_token})
        if r.status_code != 200:
            return "", "", "", {}
        fields = {f.get("name"): (f.get("values") or [""])[0]
                  for f in (r.json().get("field_data") or [])}
        name = (fields.get("full_name") or "").strip()
        if not name:
            name = " ".join(x for x in
                            (fields.get("first_name", ""),
                             fields.get("last_name", "")) if x).strip()
        return name, (fields.get("phone_number") or "").strip(), \
            (fields.get("email") or "").strip(), fields
    except Exception:
        return "", "", "", {}


def map_meta_fields(fields):
    """Map Meta lead form field_data to lead columns.

    Meta field names are advertiser-defined, so match flexibly on
    lowercased keywords (word-boundary matched — "city" must not match
    "electricity"). Returns dict of {column: value}."""
    cols = {}
    for key, val in (fields or {}).items():
        k = re.sub(r"[^a-z0-9]+", " ", str(key).lower()).strip()
        v = str(val or "").strip()
        if not v:
            continue
        has = lambda *words: any(
            re.search(rf"\b{w}\b", k) for w in words)
        if has("bill") or (has("monthly") and has("electricity")):
            try:
                cols.setdefault("monthly_bill_inr",
                                float("".join(ch for ch in v
                                              if ch.isdigit() or ch == ".")))
            except ValueError:
                pass
        elif has("property", "hometype", "housetype", "home", "house"):
            cols.setdefault("property_type", v)
        elif has("city", "location", "area", "town", "village"):
            cols.setdefault("city", v)
        elif has("roof") and has("type"):
            cols.setdefault("roof_type", v)
        elif has("roof"):
            cols.setdefault("roof_ownership", v)
    return cols


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
                name, phone, email, meta_fields = "", "", "", {}
                if lgid and comp.get("meta_page_token"):
                    name, phone, email, meta_fields = enrich_meta_lead(
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
                # store Meta custom fields (city, property type, bill…)
                # so the call can greet with the lead's own details
                mapped = map_meta_fields(meta_fields)
                lead_ctx = {"name": name, "source": "facebook"}
                if mapped:
                    sets = ", ".join(f"{c}={Q}" for c in mapped)
                    con.execute(f"UPDATE leads SET {sets} WHERE id={Q}",
                                (*mapped.values(), lid))
                    lead_ctx.update(mapped)
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
                        ok, msg = dial_primary(cid, phone, lid, name,
                                              lead=lead_ctx)
                        log_activity(cid, lid, "auto_dial",
                                     f"Auto-dial {'placed' if ok else 'failed'}",
                                     msg)
    finally:
        con.close()
    return {"ok": True, "leads": ids}


# ------------------------------------------------- integrations -----
# Client companies (hide_integrations=1) must never see provider wiring:
# API keys, balances, agent configs are operator-only. The frontend hides
# the Integrations view; these 403s are the defense-in-depth backend gate.
def _assert_integrations_visible(req: Request, cid: str):
    # company-scoped logins are always client-view: operator routes
    # (integrations, wallet billing, client-view toggle) are never theirs
    if getattr(req.state, "role", "") == "company":
        raise HTTPException(403, "operator only")
    comp = get_company(cid) or {}
    if comp.get("hide_integrations"):
        raise HTTPException(403, "integrations hidden for this company")


@app.get("/api/integrations/meta")
def meta_settings(req: Request):
    """Meta Lead Ads wiring: webhook URL, token status, auto-dial + cap."""
    cid = _cid(req)
    _assert_integrations_visible(req, cid)
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
    _assert_integrations_visible(req, cid)
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
    _assert_integrations_visible(req, cid)
    con = db()
    rows = con.execute(
        f"SELECT a.created_at, a.title, a.detail, l.name"
        f" FROM activities a LEFT JOIN leads l ON l.id=a.lead_id"
        f" WHERE a.company_id={Q} AND a.kind='auto_dial'"
        f" ORDER BY a.created_at DESC LIMIT 15", (cid,)).fetchall()
    con.close()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/integrations/smallest")
def smallest_settings(req: Request):
    """Smallest Atoms wiring: API key status, agents, rented numbers.

    Read-only probes against Smallest so the owner can pick an agent
    and caller ID without touching the Smallest console."""
    cid = _cid(req)
    _assert_integrations_visible(req, cid)
    comp = get_company(cid) or {}
    key = comp.get("smallest_api_key") or ""
    out = {
        "connected": bool(key),
        "key_set": bool(key),
        "agent_id": comp.get("smallest_agent_id") or "",
        "from_number": comp.get("smallest_from_number") or "",
        "agents": [], "numbers": [], "balance": None, "error": "",
        "last_sync": kv_get("last_sync_smallest") or "",
        "last_sync_new": kv_get("last_sync_smallest_new") or "0",
    }
    if not key or DEMO_MODE:
        return out
    try:
        out["agents"] = smallest_atoms.list_agents(key)
        out["numbers"] = smallest_atoms.list_numbers(key)
        try:
            bal = smallest_atoms.get_balance(key)
            out["balance"] = {
                "credit_balance": bal["credit_balance"],
                "credit_balance_inr": round(
                    bal["credit_balance"] * USD_INR, 2),
            }
        except SmallestError:
            pass
    except SmallestError as e:
        out["error"] = str(e)[:200]
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {str(e)[:120]}"
    return out


@app.patch("/api/integrations/smallest")
async def smallest_settings_update(req: Request):
    """Save the per-company Smallest API key, agent id and caller ID."""
    cid = _cid(req)
    _assert_integrations_visible(req, cid)
    body = await req.json()
    sets, vals = [], []
    if "api_key" in body and isinstance(body["api_key"], str):
        key = body["api_key"].strip()
        if key:
            # validate before saving: wrong keys surface here, not later
            try:
                smallest_atoms.list_agents(key)
            except SmallestError as e:
                raise HTTPException(400, f"Smallest rejected the key: {e}")
            except Exception as e:
                raise HTTPException(
                    502, f"could not reach Smallest: {type(e).__name__}")
        sets.append(f"smallest_api_key={Q}")
        vals.append(key)
    if "agent_id" in body and isinstance(body["agent_id"], str):
        sets.append(f"smallest_agent_id={Q}")
        vals.append(body["agent_id"].strip())
    if "from_number" in body and isinstance(body["from_number"], str):
        sets.append(f"smallest_from_number={Q}")
        vals.append(body["from_number"].strip())
    if not sets:
        raise HTTPException(400, "nothing to save")
    vals.append(cid)
    con = db()
    con.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id={Q}",
                tuple(vals))
    con.commit()
    con.close()
    return {"ok": True}


@app.post("/api/integrations/smallest/test")
async def smallest_test(req: Request):
    """Read-only connection test: list agents with the supplied key
    (or the saved one). Never places a call."""
    body = await req.json()
    cid = _cid(req, body.get("company", ""))
    _assert_integrations_visible(req, cid)
    key = (body.get("api_key") or "").strip()
    if not key:
        comp = get_company(cid) or {}
        key = comp.get("smallest_api_key") or ""
    if not key:
        raise HTTPException(400, "no Smallest API key configured")
    try:
        agents = smallest_atoms.list_agents(key)
        numbers = smallest_atoms.list_numbers(key)
    except SmallestError as e:
        raise HTTPException(400, str(e)[:200])
    except Exception as e:
        raise HTTPException(502, f"{type(e).__name__}: {str(e)[:120]}")
    return {"ok": True, "agents": agents, "numbers": numbers}


@app.get("/api/integrations/whatsapp")
def wa_settings(req: Request):
    """WhatsApp Cloud API wiring for post-call follow-ups."""
    cid = _cid(req)
    _assert_integrations_visible(req, cid)
    comp = get_company(cid) or {}
    return {
        "connected": bool(comp.get("wa_phone_number_id")
                          and comp.get("wa_token")),
        "enabled": bool(comp.get("wa_enabled")),
        "template": comp.get("wa_template") or "",
        "phone_number_id_set": bool(comp.get("wa_phone_number_id")),
    }


@app.patch("/api/integrations/whatsapp")
async def wa_settings_update(req: Request):
    """Save WhatsApp Cloud API credentials + template, toggle follow-ups.

    Sends a test template message to the supplied test number when one
    is given, so misconfiguration surfaces immediately instead of on
    the next real call."""
    cid = _cid(req)
    _assert_integrations_visible(req, cid)
    body = await req.json()
    sets, vals = [], []
    if "phone_number_id" in body and isinstance(body["phone_number_id"],
                                               str):
        sets.append(f"wa_phone_number_id={Q}")
        vals.append(body["phone_number_id"].strip())
    if "token" in body and isinstance(body["token"], str):
        sets.append(f"wa_token={Q}")
        vals.append(body["token"].strip())
    if "template" in body and isinstance(body["template"], str):
        sets.append(f"wa_template={Q}")
        vals.append(body["template"].strip())
    if "enabled" in body:
        sets.append(f"wa_enabled={Q}")
        vals.append(1 if body["enabled"] else 0)
    if not sets:
        raise HTTPException(400, "nothing to update")
    con = db()
    con.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id={Q}",
                (*vals, cid))
    con.commit()
    comp = get_company(cid) or {}
    test_to = (body.get("test_to") or "").strip()
    test_result = None
    if test_to and comp.get("wa_phone_number_id") and comp.get("wa_token") \
            and comp.get("wa_template"):
        d = digits(test_to)
        if len(d) == 10:
            d = "91" + d
        try:
            status, rbody = await wa_send_template(
                comp["wa_phone_number_id"], comp["wa_token"],
                comp["wa_template"], d,
                ["Test గారు", _WA_BODY["no_answer"]])
            test_result = {"ok": status in (200, 201),
                           "detail": f"HTTP {status}: {rbody[:160]}"}
        except Exception as e:
            test_result = {"ok": False,
                           "detail": f"{type(e).__name__}: {str(e)[:160]}"}
    con.close()
    log_activity(cid, None, "settings", "WhatsApp integration updated", "")
    return {"ok": True, "test": test_result}


# ------------------------------------------------- campaigns api --------
_CAMPAIGN_ACTIVE = {}  # campaign_id -> True while its runner task lives
_CAMPAIGN_DIAL_GAP = 45  # seconds between campaign dials (carrier-safe)


def _campaign_report(con, camp):
    cost = con.execute(
        f"SELECT COALESCE(SUM(cost_usd),0) FROM calls"
        f" WHERE campaign_id={Q}", (camp["id"],)).fetchone()[0] or 0
    return {
        "id": camp["id"], "name": camp["name"], "kind": camp["kind"],
        "kind_label": CAMPAIGN_KINDS.get(camp["kind"], {}).get(
            "label", camp["kind"]),
        "status": camp["status"],
        "total": camp["total"], "done": camp["done_count"],
        "interested": camp["interested_count"],
        "cost_usd": round(cost, 4),
        "cost_inr": round(cost * USD_INR, 2),
        "created_at": camp["created_at"],
    }


def _create_campaign(con, cid, name, kind, lead_rows, params):
    """lead_rows: list of (lead_id, per_lead_params dict)."""
    camp_id = f"camp-{uuid.uuid4().hex[:12]}"
    con.execute(
        f"INSERT INTO campaigns (id, company_id, name, kind, status,"
        f" params_json, total, created_at)"
        f" VALUES ({Q},{Q},{Q},{Q},'draft',{Q},{Q},{Q})",
        (camp_id, cid, name, kind, json.dumps(params or {}),
         len(lead_rows), now_iso()))
    for lid, lp in lead_rows:
        if USE_PG:
            con.execute(
                f"INSERT INTO campaign_leads"
                f" (campaign_id, lead_id, params_json)"
                f" VALUES ({Q},{Q},{Q}) ON CONFLICT DO NOTHING",
                (camp_id, lid, json.dumps(lp or {})))
        else:
            con.execute(
                f"INSERT OR IGNORE INTO campaign_leads"
                f" (campaign_id, lead_id, params_json)"
                f" VALUES ({Q},{Q},{Q})",
                (camp_id, lid, json.dumps(lp or {})))
    # INSERT OR IGNORE may skip dupes: recount actual rows
    total = con.execute(
        f"SELECT COUNT(*) FROM campaign_leads WHERE campaign_id={Q}",
        (camp_id,)).fetchone()[0]
    con.execute(f"UPDATE campaigns SET total={Q} WHERE id={Q}",
                (total, camp_id))
    return camp_id


@app.get("/api/campaigns")
def campaign_list(req: Request):
    cid = _cid(req)
    con = db()
    rows = con.execute(
        f"SELECT * FROM campaigns WHERE company_id={Q}"
        f" ORDER BY created_at DESC", (cid,)).fetchall()
    out = [_campaign_report(con, dict(r)) for r in rows]
    con.close()
    return {"items": out, "kinds": {
        k: {"label": v["label"], "desc": v["desc"]}
        for k, v in CAMPAIGN_KINDS.items()}}


@app.get("/api/campaigns/{camp_id}")
def campaign_detail(camp_id: str, req: Request):
    cid = _cid(req)
    con = db()
    camp = con.execute(
        f"SELECT * FROM campaigns WHERE id={Q} AND company_id={Q}",
        (camp_id, cid)).fetchone()
    if not camp:
        con.close()
        raise HTTPException(404, "campaign not found")
    rep = _campaign_report(con, dict(camp))
    rows = con.execute(
        f"SELECT cl.status, cl.called_at, l.id, l.name, l.phone"
        f" FROM campaign_leads cl JOIN leads l ON l.id=cl.lead_id"
        f" WHERE cl.campaign_id={Q} ORDER BY cl.lead_id",
        (camp_id,)).fetchall()
    rep["leads"] = [dict(r) for r in rows]
    rep["params"] = json.loads(camp["params_json"] or "{}")
    con.close()
    return rep


@app.post("/api/campaigns")
async def campaign_create(req: Request):
    """Create a campaign from pipeline stage or explicit lead ids."""
    cid = _cid(req)
    body = await req.json()
    kind = (body.get("kind") or "revival").strip()
    if kind not in CAMPAIGN_KINDS:
        raise HTTPException(400, "unknown campaign kind")
    name = (body.get("name")
            or f"{CAMPAIGN_KINDS[kind]['label']} {now_iso()[:10]}").strip()
    params = body.get("params") or {}
    if not isinstance(params, dict):
        raise HTTPException(400, "params must be an object")
    stage = (body.get("stage") or "").strip()
    lead_ids = body.get("lead_ids") or []
    con = db()
    if stage:
        rows = con.execute(
            f"SELECT id FROM leads WHERE company_id={Q} AND stage={Q}"
            f" AND dnc=0", (cid, stage)).fetchall()
        lead_ids = [r["id"] for r in rows]
    else:
        # keep only this company's leads
        rows = con.execute(
            f"SELECT id FROM leads WHERE company_id={Q} AND dnc=0",
            (cid,)).fetchall()
        mine = {r["id"] for r in rows}
        lead_ids = [i for i in lead_ids if i in mine]
    if not lead_ids:
        con.close()
        raise HTTPException(400, "no leads selected")
    camp_id = _create_campaign(
        con, cid, name, kind, [(i, {}) for i in lead_ids], params)
    con.commit()
    con.close()
    log_activity(cid, None, "campaign", f"Campaign created: {name}",
                 f"{CAMPAIGN_KINDS[kind]['label']} · {len(lead_ids)} leads")
    return {"ok": True, "id": camp_id, "total": len(lead_ids)}


@app.post("/api/campaigns/import")
async def campaign_import(req: Request):
    """CSV upload → new leads + campaign. Raw CSV text in the request
    body; kind/name/params as query params. Columns: name, phone, city,
    amount (optional per-kind extra)."""
    cid = _cid(req)
    kind = (req.query_params.get("kind") or "revival").strip()
    if kind not in CAMPAIGN_KINDS:
        raise HTTPException(400, "unknown campaign kind")
    name = (req.query_params.get("name")
            or f"{CAMPAIGN_KINDS[kind]['label']} {now_iso()[:10]}").strip()
    try:
        params = json.loads(req.query_params.get("params") or "{}")
    except Exception:
        raise HTTPException(400, "params must be valid JSON")
    raw = (await req.body()).decode("utf-8-sig", errors="replace")
    rows = list(csv.DictReader(raw.splitlines()))
    if not rows:
        raise HTTPException(400, "empty CSV")
    con = db()
    seen = {r["phone"] for r in con.execute(
        f"SELECT phone FROM leads WHERE company_id={Q}", (cid,)).fetchall()}
    lead_rows = []
    now = now_iso()
    for r in rows:
        phone = digits(r.get("phone") or "")
        if len(phone) < 10 or phone in seen:
            continue
        seen.add(phone)
        lid = insert_lead(
            con, name=(r.get("name") or "").strip()[:80], phone=phone,
            source=kind, created_at=now)
        con.execute(
            f"UPDATE leads SET company_id={Q}, city={Q}, stage='new',"
            f" last_activity_at={Q} WHERE id={Q}",
            (cid, (r.get("city") or "").strip()[:80], now, lid))
        lp = {}
        if (r.get("amount") or "").strip():
            try:
                lp["amount"] = float(r["amount"])
            except ValueError:
                pass
        lead_rows.append((lid, lp))
    if not lead_rows:
        con.close()
        raise HTTPException(400, "no valid new leads in CSV")
    camp_id = _create_campaign(con, cid, name, kind, lead_rows, params)
    con.commit()
    con.close()
    log_activity(cid, None, "campaign",
                 f"Campaign created: {name}",
                 f"{CAMPAIGN_KINDS[kind]['label']} · {len(lead_rows)}"
                 f" leads from CSV")
    return {"ok": True, "id": camp_id, "total": len(lead_rows)}


async def _run_campaign(campaign_id):
    """Background sequential dialer: one call at a time, gap between
    dials. Stops when the campaign is paused/finished or the queue
    empties. Never raises — a dead runner must not take the sync down."""
    if campaign_id in _CAMPAIGN_ACTIVE:
        return
    _CAMPAIGN_ACTIVE[campaign_id] = True
    try:
        while True:
            con = db()
            camp = con.execute(
                f"SELECT * FROM campaigns WHERE id={Q}",
                (campaign_id,)).fetchone()
            if not camp or camp["status"] != "running":
                con.close()
                break
            cid, kind = camp["company_id"], camp["kind"]
            cparams = json.loads(camp["params_json"] or "{}")
            row = con.execute(
                f"SELECT cl.lead_id, cl.params_json, l.name, l.phone,"
                f" l.dnc FROM campaign_leads cl"
                f" JOIN leads l ON l.id=cl.lead_id"
                f" WHERE cl.campaign_id={Q} AND cl.status='queued'"
                f" ORDER BY cl.lead_id LIMIT 1",
                (campaign_id,)).fetchone()
            if not row:
                con.execute(
                    f"UPDATE campaigns SET status='done' WHERE id={Q}",
                    (campaign_id,))
                con.commit()
                con.close()
                log_activity(cid, None, "campaign",
                             f"Campaign finished: {camp['name']}", "")
                break
            con.close()
            if row["dnc"]:
                con2 = db()
                con2.execute(
                    f"UPDATE campaign_leads SET status='skipped'"
                    f" WHERE campaign_id={Q} AND lead_id={Q}",
                    (campaign_id, row["lead_id"]))
                con2.commit()
                con2.close()
                continue
            lparams = json.loads(row["params_json"] or "{}")
            merged = dict(cparams)
            merged.update(lparams)
            try:
                ok, msg = await asyncio.to_thread(
                    dial_now, cid, row["phone"], row["lead_id"],
                    row["name"] or "", campaign_id=campaign_id,
                    campaign_kind=kind, campaign_params=merged)
            except Exception as e:
                ok, msg = False, f"{type(e).__name__}: {str(e)[:100]}"
            con2 = db()
            con2.execute(
                f"UPDATE campaign_leads SET status={Q}"
                f" WHERE campaign_id={Q} AND lead_id={Q}",
                ("calling" if ok else "failed",
                 campaign_id, row["lead_id"]))
            con2.commit()
            con2.close()
            log_activity(cid, row["lead_id"], "campaign",
                         f"Campaign dial {'placed' if ok else 'failed'}",
                         msg[:120])
            await asyncio.sleep(_CAMPAIGN_DIAL_GAP)
    except Exception:
        pass
    finally:
        _CAMPAIGN_ACTIVE.pop(campaign_id, None)


@app.post("/api/campaigns/{camp_id}/start")
async def campaign_start(camp_id: str, req: Request):
    cid = _cid(req)
    con = db()
    camp = con.execute(
        f"SELECT * FROM campaigns WHERE id={Q} AND company_id={Q}",
        (camp_id, cid)).fetchone()
    if not camp:
        con.close()
        raise HTTPException(404, "campaign not found")
    queued = con.execute(
        f"SELECT COUNT(*) FROM campaign_leads"
        f" WHERE campaign_id={Q} AND status='queued'",
        (camp_id,)).fetchone()[0]
    if not queued:
        con.close()
        raise HTTPException(400, "nothing queued")
    if DEMO_MODE:
        con.close()
        return {"ok": True, "demo": True,
                "msg": f"(demo) would dial {queued} leads"}
    comp = get_company(cid) or {}
    if not comp.get("speko_agent_id"):
        con.close()
        raise HTTPException(400, "no Speko agent configured")
    con.execute(f"UPDATE campaigns SET status='running' WHERE id={Q}",
                (camp_id,))
    con.commit()
    con.close()
    asyncio.create_task(_run_campaign(camp_id))
    log_activity(cid, None, "campaign",
                 f"Campaign started: {camp['name']}",
                 f"{queued} leads queued · ~45s between dials")
    return {"ok": True, "queued": queued}


@app.post("/api/campaigns/{camp_id}/pause")
def campaign_pause(camp_id: str, req: Request):
    cid = _cid(req)
    con = db()
    camp = con.execute(
        f"SELECT * FROM campaigns WHERE id={Q} AND company_id={Q}",
        (camp_id, cid)).fetchone()
    if not camp:
        con.close()
        raise HTTPException(404, "campaign not found")
    con.execute(
        f"UPDATE campaigns SET status='paused' WHERE id={Q}",
        (camp_id,))
    con.commit()
    con.close()
    log_activity(cid, None, "campaign",
                 f"Campaign paused: {camp['name']}", "")
    return {"ok": True}


if DEMO_MODE:
    seed_demo()


app.mount("/static", __import__("fastapi.staticfiles", fromlist=["StaticFiles"]).StaticFiles(
    directory=str(BASE_DIR / "static")), name="static")
