# Coda to Framer Node Backend

Node backend for syncing Coda tables to Framer managed collections using the `framer-api` SDK.

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
  "rowLimit": 100
}
```
