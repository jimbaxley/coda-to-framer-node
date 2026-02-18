# Coda to Framer Node Backend

Node backend for syncing Coda tables to Framer managed collections using the `framer-api` SDK.

Supports both full-table sync and single-row sync.

## Environment Variables

- `CODA_API_TOKEN` - Coda API token with access to the source doc
- `FRAMER_API_KEY` - Framer Server API key

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
  "rowId": "C-AB012345",
  "action": "rowSync"
}
```

- If `rowId` is provided (or `action` is `rowSync`), the API fetches and syncs only that row. `rowId` may be an API row ID (`i-...`) or a unique slug selector value from `slugFieldId`.
- Otherwise, it performs table sync using `rowLimit`.
