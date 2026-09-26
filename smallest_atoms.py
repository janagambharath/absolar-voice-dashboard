"""Smallest Atoms provider client.

Sync httpx client for the Smallest Atoms voice-agent REST API
(https://api.smallest.ai/atoms/v1). Mirrors the Speko helper style in
app.py: every function takes an explicit ``api_key`` (the per-company key
stored in the dashboard DB via the Integrations view) and raises
SmallestError with a clean message on failure.

Endpoint paths were confirmed against the official smallestai SDK
(v5.4+) and verified read-only against the live account:
  - GET  /agent                        -> data.agents[]
  - GET  /agent/{id}                   -> agent detail
  - GET  /conversation?agentIds=&limit= -> data.logs[], data.pagination
  - GET  /conversation/{callId}        -> transcript, events, status,
                                         recording_url, disconnectionReason
  - POST /conversation/outbound        -> {agentId, phoneNumber,
                                           from_number, fromProductId,
                                           variables}
  - GET  /product/phone-numbers        -> rented numbers (data[])
  - GET  /payment/v1/credits/balance   -> data.creditBalance
         (payment host https://api.smallest.ai, NOT /atoms/v1)

Read-only except start_outbound_call, which the dashboard only invokes
from an explicit user dial action. Renting numbers / creating agents is
deliberately NOT exposed here.
"""
import httpx

ATOMS_BASE = "https://api.smallest.ai/atoms/v1"
PAY_BASE = "https://api.smallest.ai"


class SmallestError(Exception):
    """Any Smallest API failure, with a human-readable message."""


class SmallestAuthError(SmallestError):
    """401/403 - the stored key is missing, wrong or revoked."""


def _client(api_key, base=ATOMS_BASE):
    if not api_key:
        raise SmallestAuthError("Smallest API key not configured")
    return httpx.Client(
        base_url=base,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=25,
    )


def _data(resp, what):
    if resp.status_code in (401, 403):
        raise SmallestAuthError(
            f"Smallest rejected the API key ({resp.status_code})")
    if resp.status_code == 404:
        raise SmallestError(f"Smallest: {what} not found")
    if resp.status_code >= 400:
        raise SmallestError(
            f"Smallest {what} failed ({resp.status_code}):"
            f" {resp.text[:200]}")
    try:
        body = resp.json()
    except Exception:
        raise SmallestError(f"Smallest {what}: unreadable response")
    if isinstance(body, dict) and body.get("status") is False:
        errs = (body.get("errors") or body.get("message")
                or "unknown error")
        raise SmallestError(f"Smallest {what}: {errs}")
    if isinstance(body, dict):
        return body.get("data", body)
    return body


# ------------------------------------------------------------------ agents
def list_agents(api_key):
    """Return [{id, name, language, voice, voice_model, workflow_type}]."""
    with _client(api_key) as c:
        data = _data(c.get("/agent"), "list agents")
    agents = data.get("agents") if isinstance(data, dict) else data
    out = []
    for a in agents or []:
        if not isinstance(a, dict):
            continue
        vc = (a.get("synthesizer") or {}).get("voiceConfig") or {}
        out.append({
            "id": a.get("_id") or a.get("id") or "",
            "name": a.get("name") or "",
            "language": (a.get("language") or {}).get("default") or "",
            "voice": vc.get("voiceId") or "",
            "voice_model": vc.get("model") or "",
            "workflow_type": a.get("workflowType") or "",
        })
    return out


def get_agent(api_key, agent_id):
    """Return the raw agent dict (detail view)."""
    with _client(api_key) as c:
        data = _data(c.get(f"/agent/{agent_id}"), "get agent")
    return data if isinstance(data, dict) else {}


# ------------------------------------------------------------------- calls
def _norm_log(item):
    return {
        "id": (item.get("callId") or item.get("id")
               or item.get("_id") or ""),
        "agent_id": item.get("agentId") or "",
        "to": (item.get("phoneNumber") or item.get("toNumber")
               or item.get("to") or ""),
        "from": item.get("fromNumber") or item.get("from") or "",
        "status": item.get("status") or "",
        "direction": item.get("direction") or item.get("callType") or "",
        "disconnection_reason": (item.get("disconnectionReason")
                                 or item.get("disconnectReason") or ""),
        "started_at": (item.get("createdAt") or item.get("startedAt")
                       or ""),
        "duration": (item.get("duration") or item.get("durationSeconds")
                     or 0),
        "cost": item.get("cost") or item.get("totalCost") or 0,
        "variables": item.get("variables") or {},
    }


def list_calls(api_key, agent_id, limit=50):
    """Newest-first conversation logs for one agent (read-only)."""
    with _client(api_key) as c:
        data = _data(c.get("/conversation", params={
            "agentIds": agent_id, "limit": limit,
            "sortBy": "createdAt", "sortOrder": "desc",
        }), "list calls")
    logs = data.get("logs") if isinstance(data, dict) else None
    return [_norm_log(x) for x in (logs or []) if isinstance(x, dict)]


def _norm_transcript(raw):
    """Accept Smallest transcript shapes -> [{speaker, text}]."""
    if isinstance(raw, str):
        raw = [raw] if raw.strip() else []
    lines = []
    for e in raw or []:
        if isinstance(e, str):
            if e.strip():
                lines.append({"speaker": "lead", "text": e.strip()})
            continue
        if not isinstance(e, dict):
            continue
        text = str(e.get("text") or e.get("content") or e.get("message")
                   or "").strip()
        if not text:
            continue
        src = str(e.get("role") or e.get("speaker") or e.get("source")
                  or "")
        speaker = ("agent" if src.lower() in
                   ("agent", "assistant", "ai", "system", "bot")
                   else "lead")
        lines.append({"speaker": speaker, "text": text})
    return lines


def get_call(api_key, call_id):
    """Full conversation detail: normalized transcript lines + status."""
    with _client(api_key) as c:
        d = _data(c.get(f"/conversation/{call_id}"), "call detail")
    d = d if isinstance(d, dict) else {}
    return {
        "id": d.get("callId") or d.get("id") or call_id,
        "agent_id": d.get("agentId") or "",
        "to": d.get("phoneNumber") or d.get("toNumber") or "",
        "from": d.get("fromNumber") or "",
        "status": d.get("status") or "",
        "disconnection_reason": (d.get("disconnectionReason")
                                 or d.get("disconnectReason") or ""),
        "started_at": d.get("createdAt") or d.get("startedAt") or "",
        "duration": d.get("duration") or d.get("durationSeconds") or 0,
        "cost": d.get("cost") or d.get("totalCost") or 0,
        "recording_url": (d.get("recordingUrl")
                          or d.get("recording_url") or ""),
        "variables": d.get("variables") or {},
        "transcript": _norm_transcript(d.get("transcript")),
    }


# ------------------------------------------------------------------ dialing
def start_outbound_call(api_key, agent_id, phone_number, from_number=None,
                        from_product_id=None, variables=None):
    """Place an outbound call. Implemented for the dashboard dial action;
    never called during sync or connection tests."""
    payload = {"agentId": agent_id, "phoneNumber": phone_number}
    if from_number:
        payload["from_number"] = from_number
    if from_product_id:
        payload["fromProductId"] = from_product_id
    if variables:
        payload["variables"] = variables
    with _client(api_key) as c:
        data = _data(c.post("/conversation/outbound", json=payload),
                     "outbound call")
    d = data if isinstance(data, dict) else {}
    return {"conversation_id": (d.get("conversationId") or d.get("callId")
                                or d.get("id") or "")}


# ------------------------------------------------------------------ numbers
def list_numbers(api_key):
    """Numbers already rented on the account -> [{e164, product_id,
    country, provider}]."""
    with _client(api_key) as c:
        data = _data(c.get("/product/phone-numbers"), "list numbers")
    items = data if isinstance(data, list) else []
    out = []
    for n in items:
        if not isinstance(n, dict):
            continue
        attrs = n.get("attributes") or {}
        num = attrs.get("phone_number") or n.get("phoneNumber") or ""
        if not num:
            continue
        out.append({
            "e164": num if str(num).startswith("+") else f"+{num}",
            "product_id": n.get("id") or "",
            "country": attrs.get("country_code")
            or n.get("countryCode") or "",
            "provider": attrs.get("provider") or n.get("provider") or "",
        })
    return out


# ------------------------------------------------------------------ billing
def get_balance(api_key):
    """Prepaid credit balance -> {credit_balance, plan_id}."""
    with _client(api_key, base=PAY_BASE) as c:
        data = _data(c.get("/payment/v1/credits/balance"), "balance")
    d = data if isinstance(data, dict) else {}
    try:
        bal = float(d.get("creditBalance") or 0)
    except (TypeError, ValueError):
        bal = 0.0
    return {"credit_balance": bal,
            "plan_id": d.get("planId") or "",
            "is_enterprise": bool(d.get("isEnterprise"))}


def get_usage_breakdown(api_key):
    """Raw usage breakdown payload (shape varies by plan)."""
    with _client(api_key, base=PAY_BASE) as c:
        data = _data(c.get("/payment/v1/credits/usage/breakdown"),
                     "usage breakdown")
    return data if isinstance(data, dict) else {}
