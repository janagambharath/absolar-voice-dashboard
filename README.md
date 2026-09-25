# AB Solar — Voice Agent Dashboard

Client-facing dashboard for the Priya (Sri Surya Solar) outbound voice agent.
AB Solar sees: ad-campaign **leads**, every **call** (status / duration /
outcome / cost), full **conversation transcripts**, **recording playback**,
**Speko balance/credits**, a 14-day **spend chart**, the provider **cost
split** (phone line vs voice vs AI), and the **agent + caller-ID card**.
Mobile-first UI with bottom navigation; instant page loads — Speko syncs in
the background.

## Quick start

```bash
cd dashboard
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
# demo mode (no keys needed — clearly-labeled sample data):
.venv/bin/uvicorn app:app --port 8000
# open http://localhost:8000
```

## Going live (production checklist)

**1. Deploy the dashboard (Render)**
1. Push the `dashboard/` folder to a GitHub repo (or use Render's file upload).
2. Render → New → Web Service → connect the repo.
   - Build command: `pip install -r requirements.txt`
   - Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
3. Add a **Disk** (Settings → Disks): mount path `/data`, then set
   `DB_PATH=/data/dashboard.db` — without this, sqlite wipes on every deploy.
4. Environment variables:

| Var | Value |
|---|---|
| `SPEKO_API_KEY` | your Speko platform key |
| `SPEKO_AGENT_ID` | `agent_c0b517ff8530400f` (Priya solar) |
| `DEMO_MODE` | `0` |
| `DASH_USER` / `DASH_PASS` | login you share with AB Solar (e.g. `absolar` / a strong password) |
| `AUTO_DIAL` | `1` once the Facebook webhook is verified working |
| `FB_VERIFY_TOKEN` | any secret string you also paste in the Meta app |

**2. Money on the accounts** — Speko balance was $6.76; top up before real
volume (~₹6/call). Collect Karthik's ₹4,000 advance first.

**3. Caller ID (+91)** — the real production blocker for calling. Options in
order: Exotel trunk once KYC clears (recommended), else keep the +1 number
and accept lower pickup. File TRAI's A2P pre-declaration with the telecom
provider for the caller IDs before client campaigns (Sept 2026 rules).

**4. Facebook webhook** — Meta app → Page → subscribe `leadgen` to
`https://<service>/api/webhooks/facebook`. Verify a test lead lands in the
dashboard, *then* flip `AUTO_DIAL=1`.

**5. Hand over** — send Karthik the dashboard URL + `DASH_USER`/`DASH_PASS`
on WhatsApp. He sees leads, calls, transcripts, recordings, spend.

## Database: Supabase (free tier path)

Render's free tier has no persistent disks, so sqlite would wipe on every
deploy. Point the app at a free Supabase Postgres instead:

1. supabase.com → New project (free) → wait for it to provision.
2. Project Settings → Database → **Connection string** → URI → copy it.
   Make sure it includes `sslmode=require` (Supabase's pooled URI does).
3. Render → Environment → add `DATABASE_URL` = that URI.
4. Done — the app detects `DATABASE_URL` and uses Postgres automatically.
   Locally (no `DATABASE_URL`) it keeps using sqlite, so demo/dev is unchanged.

Supabase free = 500MB, which holds hundreds of thousands of lead/call rows —
far more than 20–30 client dashboards will generate.

## Facebook lead webhook (detail)

1. Meta app → Webhooks → Page → subscribe to `leadgen`, callback URL
   `https://<your-service>/api/webhooks/facebook`, verify token = `FB_VERIFY_TOKEN`.
2. New leads land in the dashboard as `needs_enrichment`; exchange the
   `leadgen_id` for name/phone via the Graph API with a Page token
   (hook point is in `fb_lead()`), or enrich from your ad tool.
3. With `AUTO_DIAL=1` + `SPEKO_AGENT_ID` set, each enriched lead is dialed
   automatically — Priya calls within a minute of the form fill.

## Notes

- **Speed design (v2):** page loads serve instantly from the local DB/cache —
  nothing on the critical path waits for Speko. `POST /api/sync` runs the
  sync in the background (one sessions-list call, then only *new* sessions
  enriched in parallel, 10-way); eval/reliability sessions are remembered and
  never re-fetched. Recording presence comes from the session detail's
  `recordingStatus` — no extra per-session probe. The old design did up to 3
  *sequential* calls per session on every page load (the minutes-long spinner).
- New API routes: `GET /api/billing` (Speko balance/credits + provider cost
  split, 15-min cache), `GET /api/usage/daily` (14-day spend/calls),
  `GET /api/agent` (Priya config, 1-h cache), `GET /api/numbers` (caller IDs),
  `POST /api/sync` + `GET /api/sync/status` (background sync control).
- Call list/detail pull from Speko's platform API (`https://api.speko.dev`):
  `GET /v1/sessions` (list), `GET /v1/sessions/{id}` (dialed number, cost,
  per-provider `usage`, `recordingStatus`),
  `GET /v1/sessions/{id}/transcript`, `GET /v1/sessions/{id}/recording`
  (signed URL). Cached in the DB, matched to leads by phone. Automated
  eval/reliability sessions (no phone leg) are skipped.
- `https://router.speko.dev/v1` is Speko's *component* API (TTS/STT/LLM
  gateway) — it has no sessions routes. Never point the dashboard at it.
- The Speko key never leaves the server — the browser only talks to this app.
- Demo recordings are placeholder tones, clearly badged DEMO DATA.
- Before client production calling: file TRAI's A2P pre-declaration with the
  telecom provider for the caller IDs in use (Sept 2026 rules).
