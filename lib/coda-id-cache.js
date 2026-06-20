import {
  resolveTableNameOrId,
  resolveColumnNameOrId,
  resolveBaseTableId,
} from "./coda-client.js";

// TTL in ms — 3 days. Table/column IDs are stable; they only change if
// the Coda doc structure is modified, which is rare enough that a long
// TTL is safe. Vercel will recycle instances before this matters anyway.
const TTL_MS = 3 * 24 * 60 * 60 * 1000;

const tableCache = new Map();   // `${docId}:${nameOrId}` → { id, expiresAt }
const columnCache = new Map();  // `${docId}:${tableId}:${nameOrId}` → { id, expiresAt }
const baseTableCache = new Map(); // `${docId}:${tableIdOrName}` → { id, expiresAt }

function isExpired(entry) {
  return !entry || Date.now() > entry.expiresAt;
}

function makeEntry(id) {
  return { id, expiresAt: Date.now() + TTL_MS };
}

export async function resolveTableCached(docId, nameOrId, apiToken, options = {}) {
  // Already a raw grid- ID — no resolution needed, skip cache overhead.
  if (/^grid-[A-Za-z0-9_-]+$/.test(String(nameOrId || ""))) return nameOrId;

  const key = `${docId}:${nameOrId}`;
  const cached = tableCache.get(key);
  if (!isExpired(cached)) return cached.id;

  const id = await resolveTableNameOrId(docId, nameOrId, apiToken, options);
  tableCache.set(key, makeEntry(id));
  return id;
}

export async function resolveBaseTableCached(docId, tableIdOrName, apiToken, options = {}) {
  // Base tables are already grid- IDs — skip.
  if (/^grid-[A-Za-z0-9_-]+$/.test(String(tableIdOrName || ""))) return tableIdOrName;

  const key = `${docId}:${tableIdOrName}`;
  const cached = baseTableCache.get(key);
  if (!isExpired(cached)) return cached.id;

  const id = await resolveBaseTableId(docId, tableIdOrName, apiToken, options);
  baseTableCache.set(key, makeEntry(id));
  return id;
}

export async function resolveColumnCached(docId, tableId, nameOrId, apiToken, options = {}) {
  // Already a column ID — no resolution needed.
  if (/^c-[A-Za-z0-9_-]+$/.test(String(nameOrId || ""))) return nameOrId;

  const key = `${docId}:${tableId}:${nameOrId}`;
  const cached = columnCache.get(key);
  if (!isExpired(cached)) return cached.id;

  const id = await resolveColumnNameOrId(docId, tableId, nameOrId, apiToken, options);
  columnCache.set(key, makeEntry(id));
  return id;
}

// Exposed for testing or manual cache busting (e.g. after a doc restructure).
export function clearCodaIdCache() {
  tableCache.clear();
  columnCache.clear();
  baseTableCache.clear();
}
