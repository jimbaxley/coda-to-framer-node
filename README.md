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

## Endpoint

POST `/api/sync`

Body:
```
{
  "docId": "...",
  "tableIdOrName": "...",
  "framerProjectUrl": "https://framer.com/projects/...",
  "collectionName": "...",
  "slugFieldId": "...",
  "rowLimit": 100,
  "deleteMissing": true,
  "rowId": "C-AB012345",
  "action": "rowSync"
}
```

- If `rowId` is provided (or `action` is `rowSync`), the API fetches and syncs only that row. `rowId` may be an API row ID (`i-...`) or a unique slug selector value from `slugFieldId`.
- Otherwise, it performs table sync using `rowLimit`.
- For table sync, set `deleteMissing: true` to remove managed collection items that are no longer present in the Coda table snapshot.
- When Coda returns transient empty slug values during recent UI edits, the handler retries Coda snapshot/mapping before returning warnings.
- When Framer bulk add times out, the handler falls back to chunked adds first, then per-item only for failed chunks.
