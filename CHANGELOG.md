# Changelog

## 2026-02-18

### Added
- Row-sync selector support in `/api/sync`:
  - Accepts API row IDs (`i-...`) or unique slug selector values from `slugFieldId`.
- Coda data helpers for column and single-row retrieval.
- Proactive row-sync field filtering so unknown Coda-only fields are removed before Framer `addItems`.

### Changed
- Row sync now skips schema updates on existing collections to reduce latency.
- Framer operations now include explicit timeouts and retry handling for common transient failures.
- Selector matching tightened to slug/API-row-id only (no `Name` fallback) to avoid ambiguous row matches.
- API and README docs updated for row-sync payloads and selector behavior.
