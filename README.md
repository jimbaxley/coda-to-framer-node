# Coda to Framer Node Backend

Node backend for syncing Coda tables to Framer managed collections using the `framer-api` SDK.

Supports both full-table sync and single-row sync.

## Environment Variables

- `CODA_API_TOKEN` - Coda API token with access to the source doc
- `FRAMER_API_KEY` - Framer Server API key

Optional retry/fallback tuning:

- `CODA_API_RETRY_ATTEMPTS` - retries for transient Coda HTTP/network failures (default: `3`)
- `CODA_API_RETRY_DELAY_MS` - base delay between Coda API retries (default: `800`)
- `CODA_STATE_RETRY_ATTEMPTS` - retries when mapping warnings suggest Coda UI write lag (default: `3`)
- `CODA_STATE_RETRY_DELAY_MS` - base delay for warning-triggered Coda snapshot retries (default: `1200`)
- `FRAMER_RETRY_ATTEMPTS` - retries for transient Framer connection/session/network failures (default: `3`)
- `FRAMER_RETRY_DELAY_MS` - base delay between Framer retries (default: `1000`)
- `FRAMER_CONNECT_TIMEOUT_MS` - connection timeout cap for Framer connect calls (default: `30000`, max `30000`)
- `FRAMER_OPERATION_TIMEOUT_MS` - timeout cap for Framer operations (default: `30000`, max `30000`)
- `FRAMER_ADD_CHUNK_SIZE` - chunk size used after bulk add timeout fallback (default: `2`)
- `FRAMER_ADD_CHUNK_TIMEOUT_MS` - timeout for each chunk fallback add call (default: min(operation timeout, `12000`))
- `FRAMER_ADD_PER_ITEM_TIMEOUT_MS` - timeout for final per-item fallback adds (default: min(operation timeout, `8000`))
- `CODA_INITIAL_DELAY_MS` - initial delay before extraction to allow recent Coda UI edits to become API-visible (default: `1200`)

## Securing the API Endpoint

To prevent unauthorized access, the backend requires an API key for all requests.

1. **Generate a secure API key** (e.g., using a password manager or `openssl rand -hex 32`).
2. **Set the key in Vercel** as an environment variable named `API_SECRET_KEY`.
3. **Send the key in requests** as an HTTP header:
   - Header name: `x-api-key`
   - Header value: your secret key

If the key is missing or incorrect, the API will return a 401 Unauthorized error.

**Example (using curl):**

```
curl -H "x-api-key: YOUR_SECRET_KEY" "https://your-vercel-app.vercel.app/api/collections?projectUrl=..."
```

**Never share your API key publicly.**

## Endpoint

POST `/api/sync`

Returns immediately with HTTP `202` when request is accepted. Sync runs asynchronously in the background.

Body:
```
{
  "requestId": "req_...",
  "idempotencyKey": "optional-dedupe-key",
  "docId": "...",
  "tableIdOrName": "...",
  "framerProjectUrl": "https://framer.com/projects/...",
  "collectionName": "...",
  "slugFieldId": "...",
  "rowLimit": 100,
  "initialDelayMs": 1500,
  "deleteMissing": true,
  "rowId": "C-AB012345",
  "callback": {
    "statusDocId": "...",
    "statusTableIdOrName": "...",
    "statusRow": "i-... or slug value",
    "statusRowSelector": "i-... or slug value",
    "statusColumn": "Sync Status or c-...",
    "statusColumnNameOrId": "Sync Status or c-...",
    "statusSlugField": "slug column name or id used for selector matching",
    "statusSlugFieldId": "slug column name or id used for selector matching",
    "messageColumnId": "c-...",
    "sourceStatusColumnId": "c-..."
  },
  "action": "rowSync"
}
```

Accepted response:

```json
{
  "accepted": true,
  "jobId": "...",
  "requestId": "...",
  "status": "queued",
  "statusUrl": "/api/sync?jobId=..."
}
```

Status endpoint:

- `GET /api/sync?jobId=...` returns current job status plus stage events.
- Status lookup is **best-effort** in the current in-memory model; a `NOT_FOUND` can occur if requests hit different serverless instances.

- If `rowId` is provided (or `action` is `rowSync`), the API fetches and syncs only that row. `rowId` may be an API row ID (`i-...`) or a unique slug selector value from `slugFieldId`.
- Otherwise, it performs table sync using `rowLimit`.
- For table sync, set `deleteMissing: true` to remove managed collection items that are no longer present in the Coda table snapshot.
- When Coda returns transient empty slug values during recent UI edits, the handler retries Coda snapshot/mapping before returning warnings.
- `initialDelayMs` (or env default `CODA_INITIAL_DELAY_MS`) is applied before extract to mitigate Coda UI/API propagation lag.
- When Framer bulk add times out, the handler falls back to chunked adds first, then per-item only for failed chunks.

## Coda maker callback quick-start

For most maker setups, send friendly callback values (no ID hunting required):

```json
{
  "callback": {
    "statusDocId": "<docId>",
    "statusTableIdOrName": "MyTable",
    "statusRow": "my-slug-or-row-id",
    "statusColumn": "Sync Status",
    "statusSlugField": "Short Name"
  }
}
```

- `statusRow` can be selector value or `i-...`.
- `statusColumn` can be column name or `c-...`.
- `statusSlugField` helps row selector matching when not passing API row IDs.

## Advanced callback payload

### What is implemented now

- Backend callback writes are performed by direct Coda API calls using `CODA_API_TOKEN`.
- Current writeback implementation updates one target status cell when these fields are present:
  - `callback.statusDocId`
  - `callback.statusTableIdOrName`
  - `callback.statusRowSelector` (API row ID or selector)
  - `callback.statusColumnNameOrId` (column name or ID)

### Required Coda-side inputs

- `statusRowSelector` may be API row ID (`i-...`) or a selector value.
- `statusColumnNameOrId` may be a Coda column name or ID.
- For selector-based row resolution, provide `statusSlugFieldId` (name or ID) when needed.
- Alias keys `statusRow`, `statusColumn`, and `statusSlugField` are also supported.
- If row/column IDs are unavailable, use the async status endpoint (`GET /api/sync?jobId=...`) and poll from Coda via pack formula.

### Notes on extra callback fields

- `messageColumnId` and `sourceStatusColumnId` may be sent by the pack for forward compatibility.
- They are currently reserved metadata and are not yet written by backend callback logic.

## Webhook note

- No inbound webhook receiver endpoint is required for current callback behavior.
- If you want a webhook-based fan-out pattern later, add a dedicated endpoint and post job events to it from the async worker.

## Reliability note (current model)

- Background processing now prefers `waitUntil` when available and falls back to `setTimeout`.
- Job tracking is still in-memory, so use callback writes to Coda as the primary source of truth for maker-visible status.

### Callback retry jobs

If a callback update fails because Coda is rate‑limited, the backend
automatically enqueues a lightweight follow-up job to try the write again after
a short delay (5 s). This retry mechanism will repeat up to
`CODA_CALLBACK_RETRY_JOB_MAX` times (default 3) before giving up. Use the
variable to adjust your tolerance for throttling.

### One‑shot writes

When the callback row does not yet exist, the handler creates it with the full
set of status cells; no additional `PUT` requests are made afterward. This
prevents a burst of rapid write operations (create + multiple updates) which
was the primary cause of 429 errors in high‑load scenarios. Only subsequent
runs that resolve to an existing row will issue update calls.

### API Key Requirements

- The API key must be a secure, random string. For best security, use at least 32 characters (hex or base64 recommended).
- Example (generate with `openssl rand -hex 32`):
  
  `b7e3c2f4a1d9e8c7b6a5f4e3d2c1b0a9876543210fedcba9876543210abcdef12`
- The key is case-sensitive and must match exactly between your Vercel environment (`API_SECRET_KEY`) and the value you enter when connecting the Coda pack.
- Do not use simple or guessable values (e.g., "password", "123456").
- Store the key securely and never share it publicly.

**Parameter summary:**
- Name in Vercel: `API_SECRET_KEY`
- Header in requests: `x-api-key`
- Minimum recommended length: 32 characters
- Allowed characters: Any (hex or base64 recommended)
