# Changelog

## 2026-02-18

### Added
- Coda API requests now send `X-Coda-Doc-Version: latest` and retry when Coda reports pending mutations.
- Coda callback writes now poll mutation status when a write response includes a Coda mutation request id.
- Row-sync selector support in `/api/sync`:
  - Accepts API row IDs (`i-...`) or unique slug selector values from `slugFieldId`.
- Coda data helpers for column and single-row retrieval.
- Proactive row-sync field filtering so unknown Coda-only fields are removed before Framer `addItems`.
- Shared retry-policy module for Coda API errors, Framer transient failures, and transient empty-slug warning retries.
- New optional environment controls for retry/fallback behavior:
  - `CODA_API_RETRY_ATTEMPTS`, `CODA_API_RETRY_DELAY_MS`
  - `CODA_LATEST_VERSION_RETRY_ATTEMPTS`, `CODA_LATEST_VERSION_RETRY_DELAY_MS`
  - `CODA_MUTATION_STATUS_RETRY_ATTEMPTS`, `CODA_MUTATION_STATUS_RETRY_DELAY_MS`
  - `CODA_STATE_RETRY_ATTEMPTS`, `CODA_STATE_RETRY_DELAY_MS`
  - `FRAMER_RETRY_ATTEMPTS`, `FRAMER_RETRY_DELAY_MS`
  - `FRAMER_ADD_CHUNK_SIZE`, `FRAMER_ADD_CHUNK_TIMEOUT_MS`, `FRAMER_ADD_PER_ITEM_TIMEOUT_MS`

### Changed
- Source reads now use normal Coda snapshot reads by default; `X-Coda-Doc-Version: latest` is opt-in via `CODA_REQUIRE_LATEST_SOURCE_READS` or `requireLatestCodaSnapshot`.
- Request acceptance no longer pre-resolves callback table/column IDs by default, keeping Coda Pack calls fast for short timeouts.
- `X-Coda-Doc-Version: latest` is now limited to source row/table extraction, not callback/status-log bookkeeping.
- Latest-version retries now default to 5 attempts and clamp at 8 attempts to avoid long foreground waits.
- Missing Coda mutation-status resources no longer fail an already-accepted callback write.
- Coda callback writes now treat `Doc is not yet up to date.` as a latest-version retry condition.
- `CODA_INITIAL_DELAY_MS` and `initialDelayMs` can now be configured up to 120 seconds.
- Row sync now skips schema updates on existing collections to reduce latency.
- Framer operations now include explicit timeouts and retry handling for common transient failures.
- Selector matching tightened to slug/API-row-id only (no `Name` fallback) to avoid ambiguous row matches.
- `addItems` fallback now prefers chunked writes after bulk timeout, then narrows to per-item only for failed chunks.
- Framer outer retry logic no longer treats local operation-timeout wrappers as generic network retries, reducing long duplicate retry cycles.
- API and README docs updated for row-sync payloads and selector behavior.
