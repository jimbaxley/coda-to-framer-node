#!/usr/bin/env node
// cleanup-duplicate-logs.js
// Find and optionally delete pre-existing partial / duplicate Framer Sync Log rows
// Usage (dry-run):
//   CODA_API_TOKEN=... node scripts/cleanup-duplicate-logs.js --doc OySK5JOQh- --table grid-eAv1ij8OYi
// To actually delete rows:
//   CODA_API_TOKEN=... node scripts/cleanup-duplicate-logs.js --doc OySK5JOQh- --table grid-eAv1ij8OYi --confirm


import {
  resolveTableNameOrId,
  getCodaTableData,
  getCodaTableColumns,
} from "../lib/coda-client.js";
import { sleep, computeBackoffMs, isRetryableHttpStatus } from "../lib/retry-policy.js";

const CODA_API_BASE = "https://coda.io/apis/v1";

function usage(msg) {
  if (msg) console.error("Error:", msg);
  console.error("\nUsage: CODA_API_TOKEN=... node scripts/cleanup-duplicate-logs.js --doc <docId> --table <tableIdOrName> [--confirm] [--max-age-hours N]\n");
  process.exit(msg ? 1 : 0);
}

function parseArgs() {
  const argv = process.argv.slice(2);
  const out = { docId: "", table: "", confirm: false, maxAgeHours: 24 };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--doc" || a === "-d") {
      out.docId = argv[++i] || "";
      continue;
    }
    if (a === "--table" || a === "-t") {
      out.table = argv[++i] || "";
      continue;
    }
    if (a === "--confirm") {
      out.confirm = true;
      continue;
    }
    if (a === "--max-age-hours") {
      out.maxAgeHours = Number(argv[++i] || out.maxAgeHours);
      continue;
    }
    if (a === "--help" || a === "-h") {
      usage();
    }
  }
  return out;
}

function getCellText(cell) {
  if (cell === null || cell === undefined) return "";
  if (typeof cell === "string") return cell.trim();
  if (typeof cell === "number" || typeof cell === "boolean") return String(cell);
  if (typeof cell === "object") {
    return String(cell.value ?? cell.displayValue ?? cell.name ?? cell.url ?? "").trim();
  }
  return String(cell).trim();
}

async function deleteRowWithRetry(docId, tableId, rowId, apiToken) {
  const rowUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(tableId)}/rows/${encodeURIComponent(rowId)}`;
  const maxAttempts = 3;
  const baseDelayMs = 800;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const resp = await fetch(rowUrl, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${apiToken}` },
      });
      if (resp.ok || resp.status === 204) {
        return { ok: true, status: resp.status };
      }
      const body = await resp.text().catch(() => "");
      const shouldRetry = attempt < maxAttempts && isRetryableHttpStatus(resp.status);
      if (shouldRetry) {
        const waitMs = computeBackoffMs(baseDelayMs, attempt);
        await sleep(waitMs);
        continue;
      }
      return { ok: false, status: resp.status, body };
    } catch (err) {
      lastError = err;
      if (attempt === maxAttempts) break;
      const waitMs = computeBackoffMs(baseDelayMs, attempt);
      await sleep(waitMs);
    }
  }

  throw lastError || new Error("Failed to delete row");
}

(async function main() {
  const { docId, table, confirm, maxAgeHours } = parseArgs();
  const apiToken = process.env.CODA_API_TOKEN || "";
  if (!docId) usage("Missing --doc");
  if (!table) usage("Missing --table");
  if (!apiToken) usage("Missing CODA_API_TOKEN environment variable");

  console.log(`Connecting to doc=${docId} table=${table} (dry-run=${!confirm})`);

  const resolvedTableId = await resolveTableNameOrId(docId, table, apiToken).catch((err) => {
    console.error("Failed to resolve table:", err.message || err);
    process.exit(1);
  });

  const { columns, rows } = await getCodaTableData(docId, resolvedTableId, apiToken, 500).catch((err) => {
    console.error("Failed to fetch table data:", err.message || err);
    process.exit(1);
  });

  const columnNameToId = new Map(columns.map((c) => [String(c.name), String(c.id)]));
  const byLowerName = new Map(columns.map((c) => [String(c.name).toLowerCase(), String(c.id)]));

  const findColumnName = (candidates) => {
    const lowered = new Map(columns.map((c) => [c.name.toLowerCase(), c.name]));
    for (const cand of candidates) {
      const name = lowered.get(cand.toLowerCase());
      if (name) return name;
    }
    // fallback: try contains
    for (const [lower, orig] of lowered.entries()) {
      for (const cand of candidates) {
        const key = cand.toLowerCase().replace(/[^a-z0-9]+/g, "");
        if (lower.replace(/[^a-z0-9]+/g, "").includes(key)) return orig;
      }
    }
    return null;
  };

  const jobColumn = findColumnName(["Job id", "JobId", "jobId", "Job"]);
  const statusColumn = findColumnName(["Status", "status"]);
  const completedAtColumn = findColumnName(["Completed at", "completedAt", "CompletedAt"]);
  const updatedAtColumn = findColumnName(["Updated at", "updatedAt"]);
  const successColumn = findColumnName(["Success"]);

  console.log(`Table columns: ${columns.map((c) => c.name).join(", ")}`);
  console.log(`Detected columns -> job:${jobColumn} status:${statusColumn} completedAt:${completedAtColumn} updatedAt:${updatedAtColumn} success:${successColumn}`);

  // Group rows by job id (or lack thereof)
  const groups = new Map();
  for (const row of rows) {
    const jobVal = jobColumn ? getCellText(row.values?.[jobColumn]) : "";
    const key = jobVal || "(no-job-id)";
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  const now = Date.now();
  const maxAgeMs = Math.max(0, Number(maxAgeHours || 24)) * 3600 * 1000;

  const toDelete = [];
  const summary = { groupsWithDuplicates: 0, candidateOrphans: 0, candidateDuplicates: 0 };

  for (const [jobId, items] of groups.entries()) {
    if (jobId === "(no-job-id)") {
      // Orphan/partial rows: delete if older than threshold OR obviously empty
      for (const row of items) {
        const status = statusColumn ? getCellText(row.values?.[statusColumn]) : "";
        const updatedAtText = updatedAtColumn ? getCellText(row.values?.[updatedAtColumn]) : "";
        const updatedAtTs = updatedAtText ? Date.parse(updatedAtText) : NaN;
        const ageMs = Number.isFinite(updatedAtTs) ? now - updatedAtTs : Infinity;

        const hasMeaningfulFields = Object.values(row.values || {}).some((cell) => {
          const v = getCellText(cell);
          return v !== "";
        });

        const orphanCandidate = (!hasMeaningfulFields) || (ageMs > maxAgeMs) || (!status && ageMs > maxAgeMs);
        if (orphanCandidate) {
          summary.candidateOrphans += 1;
          toDelete.push({ reason: "orphan-or-partial", row });
        }
      }
      continue;
    }

    if (items.length <= 1) continue;

    summary.groupsWithDuplicates += 1;

    // Choose keeper by heuristics: prefer row with CompletedAt or Success=true or latest UpdatedAt
    const scored = items.map((row) => {
      const completedAt = completedAtColumn ? getCellText(row.values?.[completedAtColumn]) : "";
      const updatedAt = updatedAtColumn ? getCellText(row.values?.[updatedAtColumn]) : "";
      const status = statusColumn ? getCellText(row.values?.[statusColumn]) : "";
      const success = successColumn ? getCellText(row.values?.[successColumn]) : "";
      const completeness = Object.values(row.values || {}).reduce((acc, cell) => acc + (getCellText(cell) ? 1 : 0), 0);
      const completedTs = completedAt ? Date.parse(completedAt) : 0;
      const updatedTs = updatedAt ? Date.parse(updatedAt) : 0;
      const isFinal = /succeeded|failed|published/i.test(status) || /true/i.test(success) || completedAt;
      return { row, completeness, completedTs, updatedTs, isFinal };
    });

    // Prefer final rows; among finals pick most-recent completedAt/updatedAt; otherwise pick highest completeness
    const finals = scored.filter((s) => s.isFinal);
    let keeper = null;
    if (finals.length > 0) {
      finals.sort((a, b) => (b.completedTs || b.updatedTs) - (a.completedTs || a.updatedTs));
      keeper = finals[0].row;
    } else {
      scored.sort((a, b) => b.completeness - a.completeness || (b.updatedTs || 0) - (a.updatedTs || 0));
      keeper = scored[0].row;
    }

    for (const s of scored) {
      if (s.row.id === keeper.id) continue;
      summary.candidateDuplicates += 1;
      toDelete.push({ reason: `duplicate-of-${keeper.id}`, row: s.row, keepRowId: keeper.id });
    }
  }

  console.log(`\nSummary: groupsWithDuplicates=${summary.groupsWithDuplicates} candidateDuplicates=${summary.candidateDuplicates} candidateOrphans=${summary.candidateOrphans}`);
  if (toDelete.length === 0) {
    console.log("No candidate rows to delete.");
    process.exit(0);
  }

  console.log(`\nFound ${toDelete.length} candidate rows to delete (dry-run=${!confirm}):`);
  toDelete.forEach((item, i) => {
    const row = item.row;
    const jobId = jobColumn ? getCellText(row.values?.[jobColumn]) : "";
    const status = statusColumn ? getCellText(row.values?.[statusColumn]) : "";
    const updatedAt = updatedAtColumn ? getCellText(row.values?.[updatedAtColumn]) : "";
    console.log(`${i + 1}. rowId=${row.id} jobId=${jobId || '<none>'} status=${status || '<none>'} updatedAt=${updatedAt || '<none>'} reason=${item.reason}`);
  });

  if (!confirm) {
    console.log('\nRun again with --confirm to actually delete these rows.');
    process.exit(0);
  }

  // Perform deletions
  for (const item of toDelete) {
    const row = item.row;
    try {
      const result = await deleteRowWithRetry(docId, resolvedTableId, row.id, apiToken);
      if (result.ok) {
        console.log(`Deleted row ${row.id} (reason=${item.reason})`);
      } else {
        console.warn(`Failed to delete row ${row.id}: status=${result.status} body=${String(result.body).slice(0,200)}`);
      }
    } catch (err) {
      console.error(`Error deleting row ${row.id}:`, err && err.message ? err.message : String(err));
    }
  }

  console.log('\nDone.');
})();
