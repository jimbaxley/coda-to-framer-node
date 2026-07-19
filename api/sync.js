import { randomUUID } from "node:crypto";
import { waitUntil as vercelWaitUntil } from "@vercel/functions";
import {
  getCodaTableData,
  getCodaRowData,
  getCodaRowByColumnValue,
  updateTableCell,
  getCodaTableDataPaginated,
} from "../lib/coda-client.js";
import {
  normalizeColumns,
  normalizeRows,
  buildFieldsAndItems,
} from "../lib/mapping.js";
import {
  publishProject,
  listCollections,
  runSyncSession,
  withTimeout,
  formatFramerError,
  getPublishTimeoutMs,
} from "../lib/framer-client.js";
import {
  shouldPublishItemToFramer,
  upsertEventsToConvex,
} from "../lib/convex-client.js";
import {
  resolveTableCached,
  resolveBaseTableCached,
  resolveColumnCached,
} from "../lib/coda-id-cache.js";

function normalizeCollectionFieldsLocal(fields) {
  return (Array.isArray(fields) ? fields : [])
    .map((field) => {
      if (!field || typeof field !== "object") return null;
      const id = typeof field.id === "string" ? field.id : null;
      const name =
        typeof field.name === "string" ? field.name :
        typeof field.label === "string" ? field.label :
        typeof field.title === "string" ? field.title : null;
      if (!id || !name) return null;
      return { id, name };
    })
    .filter(Boolean);
}
import {
  sleep,
  parseIntEnv,
  computeBackoffMs,
  shouldRetryForTransientCodaWarnings,
  isRetryableCodaError,
  isRetryableFramerError,
} from "../lib/retry-policy.js";
import {
  createJob,
  getJobWithEvents,
  listJobsWithEvents,
  updateJob,
  appendJobEvent,
  findActiveJobByIdempotency,
  getJob,
} from "../lib/job-store.js";

export function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(body));
}

const DEFAULT_CALLBACK_COLLECTION_ID_COLUMN_ID = "c-JeQ60MRLJR";

function getCallbackCollectionIdColumnId() {
  return String(
    process.env.CODA_CALLBACK_COLLECTION_ID_COLUMN_ID
    || DEFAULT_CALLBACK_COLLECTION_ID_COLUMN_ID,
  ).trim();
}

function log(level, event, fields = {}) {
  const record = {
    ts: new Date().toISOString(),
    level,
    event,
    ...fields,
  };
  const line = JSON.stringify(record);
  if (level === "error") {
    console.error(line);
    return;
  }
  if (level === "warn") {
    console.warn(line);
    return;
  }
  console.log(line);
}

function getWaitUntil(req, res) {
  if (typeof vercelWaitUntil === "function") {
    return vercelWaitUntil;
  }
  if (typeof req?.waitUntil === "function") {
    return req.waitUntil.bind(req);
  }
  if (typeof res?.waitUntil === "function") {
    return res.waitUntil.bind(res);
  }
  if (typeof globalThis.waitUntil === "function") {
    return globalThis.waitUntil.bind(globalThis);
  }
  return null;
}

function scheduleJobProcessing({ req, res, jobId, requestId }) {
  const run = async () => {
    try {
      // pass req/res along so later code can re-schedule work via waitUntil
      await processJob(jobId, { req, res });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      appendJobEvent(jobId, {
        level: "error",
        stage: "failed",
        message: "Unhandled async processing failure",
        details: { error: errorMessage },
      });
      updateJob(jobId, {
        status: "failed",
        completedAt: new Date().toISOString(),
        error: errorMessage,
        result: makeFailureResponse(errorMessage),
      });
      log("error", "background_failed", {
        jobId,
        requestId,
        error: errorMessage,
      });
    }
  };

  const waitUntil = getWaitUntil(req, res);
  if (waitUntil) {
    log("info", "background_scheduled_waitUntil", {
      jobId,
      requestId,
      strategy: "vercel_or_runtime",
    });
    waitUntil(run());
    return;
  }

  log("warn", "background_scheduled_setTimeout", {
    jobId,
    requestId,
  });
  setTimeout(() => {
    run();
  }, 0);
}

function getReconciliationDelayMs(payload = {}) {
  // A row sync usually originates in a Coda automation immediately after a
  // UI edit. Coda's API can still return the previous snapshot at that point,
  // so run one follow-up read/write after its normal propagation window.
  if (payload.isReconciliation || payload.reconciliation === false) {
    return 0;
  }

  if (String(payload.action || "sync") !== "rowSync") {
    return 0;
  }

  return parseIntEnv(
    payload.reconciliationDelayMs ?? process.env.CODA_RECONCILIATION_DELAY_MS ?? 120000,
    120000,
    0,
    600000,
  );
}

function scheduleReconciliation(job, eventLogger, context = {}) {
  const delayMs = getReconciliationDelayMs(job.payload);
  if (delayMs <= 0) return;

  const jobId = randomUUID();
  const targetTime = new Date(job.createdAt).getTime() + delayMs;
  const waitMs = Math.max(0, targetTime - Date.now());
  const payload = {
    ...job.payload,
    // A reconciliation is deliberately a one-shot repeat. It must not spawn
    // another reconciliation when it completes.
    isReconciliation: true,
    reconciliationParentJobId: job.jobId,
    reconciliationDelayMs: 0,
    initialDelayMs: 0,
  };

  createJob({
    jobId,
    requestId: job.requestId,
    idempotencyKey: "",
    payload,
  });

  eventLogger("info", "reconciliation_scheduled", "Scheduled delayed Coda reconciliation sync", {
    reconciliationJobId: jobId,
    delayMs,
    waitMs,
  });

  const run = async () => {
    if (waitMs > 0) await sleep(waitMs);
    await processJob(jobId, context);
  };

  const waitUntil = getWaitUntil(context.req, context.res);
  if (waitUntil) {
    waitUntil(run());
    return;
  }

  setTimeout(() => {
    run();
  }, waitMs);
}

function parseJobIdFromRequest(req) {
  const requestUrl = new URL(req.url || "", "http://localhost");
  return requestUrl.searchParams.get("jobId") || "";
}

function normalizeSelectorValue(value) {
  if (value === null || value === undefined) return "";

  if (Array.isArray(value)) {
    for (const item of value) {
      const normalized = normalizeSelectorValue(item);
      if (normalized) return normalized;
    }
    return "";
  }

  if (typeof value === "object") {
    const obj = value;
    const candidates = [obj.value, obj.displayValue, obj.name, obj.url, obj.rowId];
    for (const candidate of candidates) {
      const normalized = normalizeSelectorValue(candidate);
      if (normalized) return normalized;
    }
    return "";
  }

  let text = String(value).trim();
  const triple = text.match(/^```([\s\S]*)```$/);
  if (triple && typeof triple[1] === "string") {
    text = triple[1].trim();
  }
  const single = text.match(/^`([^`]*)`$/);
  if (single && typeof single[1] === "string") {
    text = single[1].trim();
  }
  return text.trim();
}

function isApiRowId(value) {
  return typeof value === "string" && /^i-/.test(value);
}

function findRowBySelector(tableData, selector, slugFieldId) {
  const normalizedSelector = normalizeSelectorValue(selector);
  if (!normalizedSelector) return null;

  const slugColumn = tableData.columns.find((column) => String(column.id) === String(slugFieldId));
  const slugColumnName = slugColumn?.name;

  const matches = tableData.rows.filter((row) => {
    const rowValues = row?.values || {};
    const byRowId = normalizeSelectorValue(row?.id) === normalizedSelector;
    const bySlug = slugColumnName
      ? normalizeSelectorValue(rowValues[slugColumnName]) === normalizedSelector
      : false;
    return byRowId || bySlug;
  });

  if (matches.length === 1) return matches[0];
  if (matches.length > 1) {
    throw new Error(
      `Row selector "${normalizedSelector}" matched multiple rows in slug field "${slugColumnName || slugFieldId}". Use a unique slug value or API row ID.`,
    );
  }

  return null;
}

async function readJsonBody(req) {
  if (req.body && typeof req.body === "object") {
    return req.body;
  }
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString("utf8");
  if (!raw) return {};
  return JSON.parse(raw);
}

function makeFailureResponse(errorMessage) {
  return {
    success: false,
    syncSuccess: false,
    publishRequested: false,
    collectionId: "",
    collectionName: "",
    itemsAdded: 0,
    itemsRemoved: 0,
    fieldsSet: 0,
    warnings: [],
    published: false,
    publishError: "",
    deploymentId: "",
    message: `❌ Sync failed: ${errorMessage}`,
  };
}

function getDefaultCallbackTableName() {
  return String(process.env.CODA_CALLBACK_TABLE_NAME || "Framer Sync Log").trim();
}

function parseBooleanFlag(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

function normalizeFreshnessValue(value) {
  if (value === null || value === undefined) return "";

  if (Array.isArray(value)) {
    return value.map(normalizeFreshnessValue).filter(Boolean).join(", ");
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === "object") {
    const obj = value;
    const candidates = [
      obj.value,
      obj.displayValue,
      obj.name,
      obj.rawValue,
      obj.url,
      obj.rowId,
      obj.id,
    ];
    for (const candidate of candidates) {
      const normalized = normalizeFreshnessValue(candidate);
      if (normalized) return normalized;
    }
    return JSON.stringify(value);
  }

  return String(value).trim();
}

function normalizeFreshnessExpectedEntries(freshness, payload) {
  const entries = [];
  const expectedValues = freshness.expectedValues || payload.freshnessExpectedValues;

  if (expectedValues && typeof expectedValues === "object") {
    if (Array.isArray(expectedValues)) {
      for (const entry of expectedValues) {
        if (!entry || typeof entry !== "object") continue;
        const columnIdOrName = String(entry.columnIdOrName || entry.column || entry.columnId || entry.columnName || "").trim();
        if (!columnIdOrName) continue;
        entries.push({
          columnIdOrName,
          expectedValue: normalizeFreshnessValue(entry.expectedValue ?? entry.value),
        });
      }
    } else {
      for (const [columnIdOrName, expectedValue] of Object.entries(expectedValues)) {
        if (!String(columnIdOrName || "").trim()) continue;
        entries.push({
          columnIdOrName,
          expectedValue: normalizeFreshnessValue(expectedValue),
        });
      }
    }
  }

  const changedColumnIdOrName = String(
    freshness.changedColumnIdOrName
    || freshness.changedColumn
    || payload.freshnessChangedColumnIdOrName
    || payload.freshnessChangedColumn
    || "",
  ).trim();
  if (changedColumnIdOrName) {
    entries.push({
      columnIdOrName: changedColumnIdOrName,
      expectedValue: normalizeFreshnessValue(
        "expectedChangedValue" in freshness
          ? freshness.expectedChangedValue
          : payload.freshnessExpectedChangedValue,
      ),
    });
  }

  return entries;
}

function findColumnForFreshness(tableData, columnId, columnIdOrName) {
  const normalizedColumnId = String(columnId || "").trim();
  const normalizedInput = String(columnIdOrName || "").trim().toLowerCase();
  return (tableData.columns || []).find((candidate) => {
    const id = String(candidate?.id || "").trim();
    const name = String(candidate?.name || "").trim();
    return (
      (normalizedColumnId && id === normalizedColumnId)
      || id.toLowerCase() === normalizedInput
      || name.toLowerCase() === normalizedInput
    );
  });
}

function getFreshnessValue(tableData, columnId, columnIdOrName, rowInput = null) {
  const column = findColumnForFreshness(tableData, columnId, columnIdOrName);
  const columnName = column?.name || String(columnIdOrName || "");
  const row = rowInput || (tableData.rows || [])[0] || {};
  const value = row?.values?.[columnName];
  return {
    columnName,
    actual: normalizeFreshnessValue(value),
  };
}

function getExpectedFreshnessResults(tableData, entries, rowInput = null) {
  return entries.map((entry) => {
    const result = getFreshnessValue(tableData, "", entry.columnIdOrName, rowInput);
    const matches = result.actual === entry.expectedValue;
    return {
      column: result.columnName || entry.columnIdOrName,
      matches,
      expectedLength: entry.expectedValue.length,
      actualLength: result.actual.length,
      expectedPreview: entry.expectedValue.slice(0, 80),
      actualPreview: result.actual.slice(0, 80),
    };
  });
}

function normalizeFreshnessPresence(value) {
  const normalized = String(value || "present").trim().toLowerCase();
  if (["absent", "missing", "not_present", "not present", "removed", "false", "no"].includes(normalized)) {
    return "absent";
  }
  return "present";
}

function normalizeTableFreshnessMode(value) {
  const normalized = String(value || "nonblank").trim().toLowerCase().replace(/[\s_]+/g, "-");
  if (["any-nonblank", "any-filled", "any"].includes(normalized)) return "any-nonblank";
  if (["stable", "settled", "unchanged"].includes(normalized)) return "stable";
  return "nonblank";
}

function getColumnValuesForFreshness(tableData, columnId, columnIdOrName) {
  const column = findColumnForFreshness(tableData, columnId, columnIdOrName);
  const columnName = column?.name || String(columnIdOrName || "");
  const rows = tableData.rows || [];
  const values = rows.map((row) => normalizeFreshnessValue(row?.values?.[columnName]));
  return { columnName, rows, values };
}

function normalizeReferenceName(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

async function buildLinkedCollectionReferenceMap({
  columns,
  linkedCollectionName,
  framerProjectUrl,
  framerApiKey,
  eventLogger,
}) {
  const normalizedLinkedCollectionName = String(linkedCollectionName || "").trim();
  if (!normalizedLinkedCollectionName) return new Map();

  const collections = await listCollections(framerProjectUrl, framerApiKey);
  const linkedCollection = collections.find(
    (collection) => normalizeReferenceName(collection?.name) === normalizeReferenceName(normalizedLinkedCollectionName),
  );

  if (!linkedCollection?.id) {
    eventLogger("warning", "reference_link", "Linked collection not found; reference fields left as strings", {
      linkedCollectionName: normalizedLinkedCollectionName,
    });
    return new Map();
  }

  const linkedName = normalizeReferenceName(normalizedLinkedCollectionName);
  const referenceMap = new Map();

  columns.forEach((column) => {
    const columnType = normalizeReferenceName(column?.format?.type);
    if (columnType !== "lookup" && columnType !== "reference") return;

    const referencedTable = column?.format?.table || {};
    const referencedTableId = String(referencedTable.id || "").trim();
    const referencedTableName = normalizeReferenceName(referencedTable.name || referencedTable.displayName);
    if (!referencedTableId) return;

    if (referencedTableName === linkedName || normalizeReferenceName(referencedTableId) === linkedName) {
      referenceMap.set(referencedTableId, String(linkedCollection.id));
    }
  });

  if (referenceMap.size === 0) {
    eventLogger("warning", "reference_link", "No Coda reference fields matched linked collection; fields left as strings", {
      linkedCollectionName: normalizedLinkedCollectionName,
    });
  } else {
    eventLogger("info", "reference_link", "Enabled linked collection reference mapping", {
      linkedCollectionName: normalizedLinkedCollectionName,
      linkedCollectionId: linkedCollection.id,
      referencedTables: Array.from(referenceMap.keys()),
    });
  }

  return referenceMap;
}

async function provisionCallbackRowIfNeeded(payload, { requestId, jobId, vercelTrace }) {
  const callback = payload?.callback || {};
  const hasExplicitCallbackTable = Object.prototype.hasOwnProperty.call(callback, "statusTableIdOrName")
    || Object.prototype.hasOwnProperty.call(callback, "statusTableInput");
  const defaultCallbackTableName = getDefaultCallbackTableName();
  let callbackTableValue = callback.statusTableIdOrName || callback.statusTableInput || "";
  if (hasExplicitCallbackTable && !callbackTableValue && defaultCallbackTableName) {
    callbackTableValue = defaultCallbackTableName;
  }
  const hasCallbackTable = Boolean(callbackTableValue);
  if (hasExplicitCallbackTable && !hasCallbackTable) {
    log("warn", "callback_row_autocreate_skipped", {
      requestId,
      jobId,
      reason: "explicit_callback_table_unresolved",
      vercelTrace,
    });
    return payload;
  }
  if (!hasCallbackTable) {
    return payload;
  }

  // Do not auto-create an initial callback/log row at request time.
  // Instead, resolve the callback target (doc/table/column) so the
  // writeStatusCallback can create the final log row when the job completes.

  const statusDocId = callback.statusDocId || payload.docId || "";
  const statusTableIdOrName = callbackTableValue || payload.tableIdOrName || "";
  const statusColumnNameOrId = callback.statusColumn || callback.statusColumnId || callback.statusColumnNameOrId || "Status";

  if (!statusDocId || !statusTableIdOrName || !statusColumnNameOrId) {
    // Nothing for the backend to do now — leave payload unchanged.
    return payload;
  }

  const codaApiToken = process.env.CODA_API_TOKEN;
  if (!codaApiToken) {
    return payload;
  }

  try {
    const resolvedTableRef = await resolveTableCached(
      statusDocId,
      statusTableIdOrName,
      codaApiToken,
    );

    // Write status to the underlying base table. When the callback target is a
    // filtered view, the source row may no longer pass the filter (e.g. a row
    // just removed from a "ready to publish" view by deleteRow). Searching the
    // view would miss it and autocreate a duplicate; the base table always
    // contains it, and updates there are reflected in the view.
    const resolvedStatusTableId = await resolveBaseTableCached(
      statusDocId,
      resolvedTableRef,
      codaApiToken,
    );

    const resolvedStatusColumnId = await resolveColumnCached(
      statusDocId,
      resolvedStatusTableId,
      statusColumnNameOrId,
      codaApiToken,
    );

    // Return the payload with resolved table/column ids but do NOT create a row.
    const nextCallback = {
      ...callback,
      statusDocId,
      statusTableIdOrName: resolvedStatusTableId,
      statusTableInput: statusTableIdOrName,
      statusColumn: resolvedStatusColumnId,
      statusColumnNameOrId: resolvedStatusColumnId,
    };

    return {
      ...payload,
      callback: nextCallback,
    };
  } catch (error) {
    // If resolution fails, just return the original payload — writeStatusCallback
    // will skip or fail gracefully at completion.
    return payload;
  }
}

async function executeSyncWorkflow(payload, eventLogger) {
  const codaApiToken = process.env.CODA_API_TOKEN;
  if (!codaApiToken) {
    throw new Error("CODA_API_TOKEN is not configured");
  }

  const framerApiKey = process.env.FRAMER_API_KEY || payload.framerApiKey;
  if (!framerApiKey) {
    throw new Error("Missing Framer API key");
  }

  const isDeleteRow = payload.action === "deleteRow";
  if (isDeleteRow) {
    if (!payload.rowId) {
      throw new Error("Missing required field for deleteRow: rowId");
    }

    const deleteReadOptions = {
      requireLatest: parseBooleanFlag(
        payload.requireLatestCodaSnapshot ?? process.env.CODA_REQUIRE_LATEST_SOURCE_READS,
        false,
      ),
    };

    // A row being deleted has usually dropped out of any filtered/published
    // view, so resolve the underlying base table and search there. This
    // mirrors SyncRowToFramer's row-finding (getCodaTableData + findRowBySelector)
    // — which strips Coda's rich-text backtick wrappers and matches by slug or
    // API row id — but reads every page and targets the base table.
    const deleteSearchTableId = await resolveBaseTableCached(
      payload.docId,
      payload.tableIdOrName,
      codaApiToken,
      deleteReadOptions,
    );

    let resolvedSlugFieldId = payload.slugFieldId;
    if (payload.slugFieldId) {
      resolvedSlugFieldId = await resolveColumnCached(
        payload.docId,
        deleteSearchTableId,
        payload.slugFieldId,
        codaApiToken,
        deleteReadOptions,
      );
    }

    eventLogger("info", "extract", "Searching Coda table for row (deleteRow)", {
      tableIdOrName: payload.tableIdOrName,
      searchTableId: deleteSearchTableId,
      rowId: payload.rowId,
    });

    let deleteTableData;
    if (isApiRowId(payload.rowId)) {
      deleteTableData = await getCodaRowData(
        payload.docId,
        deleteSearchTableId,
        payload.rowId,
        codaApiToken,
        deleteReadOptions,
      );
    } else if (resolvedSlugFieldId) {
      deleteTableData = await getCodaRowByColumnValue(
        payload.docId,
        deleteSearchTableId,
        resolvedSlugFieldId,
        payload.rowId,
        codaApiToken,
        deleteReadOptions,
      );
    } else {
      deleteTableData = await getCodaTableDataPaginated(
        payload.docId,
        deleteSearchTableId,
        codaApiToken,
        deleteReadOptions,
      );
    }

    const matchedRow = findRowBySelector(deleteTableData, payload.rowId, resolvedSlugFieldId);

    if (!matchedRow?.id) {
      throw new Error(
        `Row not found for selector "${normalizeSelectorValue(payload.rowId)}". Cannot determine Framer item ID for deleteRow.`,
      );
    }

    const framerItemId = matchedRow.id;
    const deleteColumns = normalizeColumns(deleteTableData.columns);
    const deleteRows = normalizeRows([matchedRow]);
    const deleteMappingResult = buildFieldsAndItems({
      columns: deleteColumns,
      rows: deleteRows,
      slugFieldId: resolvedSlugFieldId,
      use12HourTime: payload.use12HourTime !== false,
    });
    const deleteConvexResult = parseBooleanFlag(payload.bypassConvex)
      ? { skipped: true, count: 0, results: [] }
      : await upsertEventsToConvex({
        payload,
        mappingResult: deleteMappingResult,
        eventLogger,
      });

    eventLogger("info", "framer_delete", "Resolved Framer item ID for deleteRow", {
      rowSelector: payload.rowId,
      framerItemId,
    });

    let deleteCollection;
    const deletePublishRequested = Boolean(payload.publish);
    let deletePublishResult = null;
    let deletePublishError = "";

    await runSyncSession(
      payload.framerProjectUrl,
      framerApiKey,
      payload.collectionName,
      async ({ collection: collectionHandle, collectionMeta, timeoutMs }) => {
        deleteCollection = collectionMeta;
        await withTimeout(
          collectionHandle.removeItems([framerItemId]),
          timeoutMs,
          "removeItem (deleteRow)",
        );
      },
    );

    if (deletePublishRequested) {
      eventLogger("info", "publishing", "Publishing project after deleteRow");
      try {
        const pubResult = await publishProject(payload.framerProjectUrl, framerApiKey);
        deletePublishResult = { published: true, deploymentId: pubResult.deploymentId };
      } catch (error) {
        deletePublishError = error instanceof Error ? error.message : String(error);
        eventLogger("warning", "publishing", "Publish failed after deleteRow", { error: deletePublishError });
      }
    }

    const deletePublishSucceeded = Boolean(deletePublishResult?.published);
    const deleteName = normalizeSelectorValue(payload.rowId);
    const deleteBaseMsg = `Removed "${deleteName}" from "${deleteCollection.collectionName}"`;
    return {
      success: !deletePublishRequested || deletePublishSucceeded,
      syncSuccess: true,
      convexSynced: Boolean(deleteConvexResult && !deleteConvexResult.skipped),
      convexSyncedCount: deleteConvexResult?.count || 0,
      convexEventId: deleteConvexResult?.results?.[0]?.response?.eventId || "",
      publishRequested: deletePublishRequested,
      collectionId: deleteCollection.collectionId,
      collectionName: deleteCollection.collectionName,
      itemsAdded: 0,
      itemsRemoved: 1,
      fieldsSet: 0,
      warnings: [],
      published: deletePublishSucceeded,
      publishError: deletePublishError,
      deploymentId: deletePublishResult?.deploymentId ?? "",
      message: (() => {
        const convexPrefix = (deleteConvexResult && !deleteConvexResult.skipped) ? `Saved ${deleteConvexResult.count || 0} to Convex. ` : "";
        const ts = new Date().toLocaleString("en-US", { month: "numeric", day: "numeric", hour: "numeric", minute: "2-digit", hour12: true, timeZone: "America/New_York" }) + " ET";
        return deletePublishSucceeded
          ? `✅ ${convexPrefix}${deleteBaseMsg} and published at ${ts}.`
          : deletePublishRequested
            ? `⚠️ ${convexPrefix}${deleteBaseMsg}, but publish failed: ${deletePublishError}`
            : `✅ ${convexPrefix}${deleteBaseMsg}.`;
      })(),
    };
  }

  const isRowSync = payload.action === "rowSync" || Boolean(payload.rowId);
  if (isRowSync && !payload.rowId) {
    throw new Error("Missing required field for rowSync: rowId");
  }

  // Parse the pre-resolved columnIds string (e.g. "Slug:c-abc,Graphic:c-xyz") into a
  // lookup map so we can skip live Coda API resolution for known column names.
  const columnIdMap = new Map();
  if (payload.columnIds) {
    for (const entry of String(payload.columnIds).split(",")) {
      const colon = entry.indexOf(":");
      if (colon > 0) {
        const name = entry.slice(0, colon).trim();
        const id = entry.slice(colon + 1).trim();
        if (name && id) columnIdMap.set(name.toLowerCase(), id);
      }
    }
  }

  function resolveColumnFromMap(nameOrId) {
    if (!nameOrId) return null;
    // Already a column ID — nothing to resolve.
    if (/^c-[A-Za-z0-9_-]+$/.test(String(nameOrId))) return nameOrId;
    return columnIdMap.get(String(nameOrId).toLowerCase()) || null;
  }

  let resolvedSlugFieldId = payload.slugFieldId;
  const requireLatestCodaSnapshotInput = payload.requireLatestCodaSnapshot
    ?? (isRowSync ? process.env.CODA_ROW_REQUIRE_LATEST_SOURCE_READS : undefined)
    ?? process.env.CODA_REQUIRE_LATEST_SOURCE_READS;
  const codaReadOptions = {
    requireLatest: parseBooleanFlag(
      requireLatestCodaSnapshotInput,
      false,
    ),
    latestRowsOnly: isRowSync,
  };
  const freshness = payload.freshness && typeof payload.freshness === "object"
    ? payload.freshness
    : {};
  const freshnessColumnIdOrName = String(freshness.columnIdOrName || payload.freshnessColumnIdOrName || "").trim();
  const freshnessExpectedValue = normalizeFreshnessValue(
    "expectedValue" in freshness ? freshness.expectedValue : payload.freshnessExpectedValue,
  );
  const freshnessRowSelector = normalizeSelectorValue(freshness.rowSelector || payload.freshnessRowSelector || "");
  const freshnessExpectedPresence = normalizeFreshnessPresence(
    freshness.expectedPresence || payload.freshnessExpectedPresence,
  );
  const freshnessMode = normalizeTableFreshnessMode(freshness.mode || payload.freshnessMode);
  const rowExpectedFreshnessEntries = normalizeFreshnessExpectedEntries(freshness, payload);
  const hasRowExpectedFreshness = isRowSync && rowExpectedFreshnessEntries.length > 0;
  const hasRowFreshnessAnchor = isRowSync && (Boolean(freshnessColumnIdOrName) || hasRowExpectedFreshness);
  const hasTableRowFreshnessAnchor = !isRowSync && Boolean(freshnessRowSelector);
  const hasTableColumnFreshnessAnchor = !isRowSync && Boolean(freshnessColumnIdOrName) && !freshnessRowSelector;
  const hasTableFreshnessAnchor = hasTableRowFreshnessAnchor || hasTableColumnFreshnessAnchor;
  const hasFreshnessAnchor = hasRowFreshnessAnchor || hasTableFreshnessAnchor;
  const freshnessMaxAttempts = parseIntEnv(
    freshness.maxAttempts ?? payload.freshnessMaxAttempts ?? process.env.CODA_FRESHNESS_RETRY_ATTEMPTS ?? 10,
    10,
    1,
    20,
  );
  const freshnessDelayMs = parseIntEnv(
    freshness.delayMs ?? payload.freshnessDelayMs ?? process.env.CODA_FRESHNESS_RETRY_DELAY_MS ?? 1500,
    1500,
    0,
    30000,
  );
  eventLogger("info", "extract", "Configured Coda snapshot read mode", {
    requireLatest: codaReadOptions.requireLatest,
    latestRowsOnly: codaReadOptions.latestRowsOnly,
    hasFreshnessAnchor,
    rowExpectedFreshnessCount: rowExpectedFreshnessEntries.length,
  });

  let resolvedFreshnessColumnId = freshnessColumnIdOrName;
  [resolvedSlugFieldId, resolvedFreshnessColumnId] = await Promise.all([
    payload.slugFieldId
      ? (resolveColumnFromMap(payload.slugFieldId) || resolveColumnCached(payload.docId, payload.tableIdOrName, payload.slugFieldId, codaApiToken, codaReadOptions))
      : Promise.resolve(resolvedSlugFieldId),
    freshnessColumnIdOrName
      ? (resolveColumnFromMap(freshnessColumnIdOrName) || resolveColumnCached(payload.docId, payload.tableIdOrName, freshnessColumnIdOrName, codaApiToken, codaReadOptions))
      : Promise.resolve(resolvedFreshnessColumnId),
  ]);

  if (resolvedSlugFieldId !== payload.slugFieldId && payload.slugFieldId) {
    eventLogger("info", "resolve_slug", "Resolved slug field ID", {
      before: payload.slugFieldId,
      after: resolvedSlugFieldId,
    });
  }

  if (resolvedFreshnessColumnId !== freshnessColumnIdOrName && freshnessColumnIdOrName) {
    eventLogger("info", "freshness", "Resolved freshness column ID", {
      before: freshnessColumnIdOrName,
      after: resolvedFreshnessColumnId,
    });
  }

  const getCodaSnapshotData = async () => {
    let tableData;
    if (isRowSync) {
      if (isApiRowId(payload.rowId)) {
        try {
          tableData = await getCodaRowData(
            payload.docId,
            payload.tableIdOrName,
            payload.rowId,
            codaApiToken,
            codaReadOptions,
          );
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          eventLogger("warning", "extract", "Direct row lookup failed; falling back to selector", {
            message,
          });
        }
      }

      if (!tableData) {
        // If we have a slug field, use Coda's server-side query to find just this row
        // instead of fetching up to 500 rows and searching locally.
        if (resolvedSlugFieldId) {
          const queried = await getCodaRowByColumnValue(
            payload.docId,
            payload.tableIdOrName,
            resolvedSlugFieldId,
            payload.rowId,
            codaApiToken,
            codaReadOptions,
          );
          const matchedRow = findRowBySelector(queried, payload.rowId, resolvedSlugFieldId);
          if (!matchedRow) {
            const normalizedSelector = normalizeSelectorValue(payload.rowId);
            throw new Error(
              `Row not found for selector "${normalizedSelector}". Pass API row ID (i-...) or unique slug value from the slug field.`,
            );
          }
          tableData = { columns: queried.columns, rows: [matchedRow] };
        } else {
          const selectorData = await getCodaTableData(
            payload.docId,
            payload.tableIdOrName,
            codaApiToken,
            payload.rowLimit || 500,
            codaReadOptions,
          );
          const matchedRow = findRowBySelector(selectorData, payload.rowId, resolvedSlugFieldId);
          if (!matchedRow) {
            const normalizedSelector = normalizeSelectorValue(payload.rowId);
            throw new Error(
              `Row not found for selector "${normalizedSelector}". Pass API row ID (i-...) or unique slug value from the slug field.`,
            );
          }
          tableData = { columns: selectorData.columns, rows: [matchedRow] };
        }
      }
    } else {
      tableData = await getCodaTableData(
        payload.docId,
        payload.tableIdOrName,
        codaApiToken,
        payload.rowLimit || 100,
        codaReadOptions,
      );
    }

    return tableData;
  };

  const buildMappingFromTableData = async (tableData) => {
    const columns = normalizeColumns(tableData.columns);
    const rows = normalizeRows(tableData.rows);
    const referenceMap = await buildLinkedCollectionReferenceMap({
      columns,
      linkedCollectionName: payload.linkedCollectionName,
      framerProjectUrl: payload.framerProjectUrl,
      framerApiKey,
      eventLogger,
    });

    const mappingResult = buildFieldsAndItems({
      columns,
      rows,
      slugFieldId: resolvedSlugFieldId,
      use12HourTime: payload.use12HourTime !== false,
      referenceMap,
    });

    return mappingResult;
  };

  const maxCodaStateRetries = parseIntEnv(
    process.env.CODA_STATE_RETRY_ATTEMPTS || 3,
    3,
    1,
    6,
  );
  const codaStateRetryDelayMs = parseIntEnv(
    process.env.CODA_STATE_RETRY_DELAY_MS || 1200,
    1200,
    0,
    10000,
  );

  eventLogger("info", "extract", "Fetching Coda snapshot", {
    isRowSync,
    tableIdOrName: payload.tableIdOrName,
  });

  let mappingResult;
  const maxSnapshotAttempts = Math.max(maxCodaStateRetries, hasFreshnessAnchor ? freshnessMaxAttempts : 1);
  let lastFreshnessActual = "";
  let lastFreshnessPresence = "";
  let lastRowFreshnessSignature = "";
  let lastTableFreshnessSignature = "";
  for (let attempt = 1; attempt <= maxSnapshotAttempts; attempt += 1) {
    const snapshotStartedAt = Date.now();
    const tableData = await getCodaSnapshotData();
    eventLogger("info", "extract", "Fetched Coda snapshot", {
      attempt,
      maxAttempts: maxSnapshotAttempts,
      rowCount: Array.isArray(tableData?.rows) ? tableData.rows.length : 0,
      columnCount: Array.isArray(tableData?.columns) ? tableData.columns.length : 0,
      elapsedMs: Date.now() - snapshotStartedAt,
    });

    if (hasRowFreshnessAnchor) {
      // Settle-based freshness: rather than require the Coda API to return the
      // exact value that triggered the sync (which fails when the row has since
      // advanced, e.g. Status moved past "Ready to Approve"), we wait until the
      // watched value(s) stop changing across consecutive reads, then proceed
      // with whatever Coda currently returns. This still guards against the
      // Coda API's eventual-consistency staleness right after an edit.
      const freshnessResult = freshnessColumnIdOrName
        ? getFreshnessValue(tableData, resolvedFreshnessColumnId, freshnessColumnIdOrName)
        : { columnName: "", actual: "" };
      lastFreshnessActual = freshnessResult.actual;
      const expectedFreshnessResults = getExpectedFreshnessResults(tableData, rowExpectedFreshnessEntries);
      const watchedValues = [
        ...(freshnessColumnIdOrName ? [lastFreshnessActual] : []),
        ...rowExpectedFreshnessEntries.map(
          (entry) => getFreshnessValue(tableData, "", entry.columnIdOrName).actual,
        ),
      ];
      const freshnessSignature = watchedValues.join("");
      const previousSignature = lastRowFreshnessSignature;
      lastRowFreshnessSignature = freshnessSignature;
      const settled = attempt > 1 && freshnessSignature === previousSignature;

      eventLogger(
        "info",
        "freshness",
        settled ? "Coda API freshness anchor settled" : "Coda API freshness anchor still settling",
        {
          attempt,
          maxAttempts: freshnessMaxAttempts,
          column: freshnessResult.columnName,
          actual: lastFreshnessActual,
          changed: attempt > 1 && freshnessSignature !== previousSignature,
          expectedValues: expectedFreshnessResults,
        },
      );

      if (!settled) {
        if (attempt >= freshnessMaxAttempts) {
          eventLogger(
            "warning",
            "freshness",
            "Coda API freshness value did not stabilize; proceeding with latest snapshot",
            {
              attempt,
              maxAttempts: freshnessMaxAttempts,
              column: freshnessResult.columnName,
              actual: lastFreshnessActual,
            },
          );
        } else {
          await sleep(freshnessDelayMs);
          continue;
        }
      }
    }

    if (hasTableRowFreshnessAnchor) {
      const matchedFreshnessRow = findRowBySelector(tableData, freshnessRowSelector, resolvedSlugFieldId);
      lastFreshnessPresence = matchedFreshnessRow ? "present" : "absent";
      const presenceMatches = lastFreshnessPresence === freshnessExpectedPresence;
      let valueMatches = true;
      let freshnessResult = null;

      if (presenceMatches && matchedFreshnessRow && freshnessColumnIdOrName) {
        freshnessResult = getFreshnessValue(
          tableData,
          resolvedFreshnessColumnId,
          freshnessColumnIdOrName,
          matchedFreshnessRow,
        );
        lastFreshnessActual = freshnessResult.actual;
        valueMatches = lastFreshnessActual === freshnessExpectedValue;
      }

      const freshnessMatches = presenceMatches && valueMatches;
      eventLogger(
        freshnessMatches ? "info" : "warning",
        "freshness",
        freshnessMatches ? "Coda table freshness anchor matched" : "Coda table freshness anchor has not settled",
        {
          attempt,
          maxAttempts: freshnessMaxAttempts,
          selector: freshnessRowSelector,
          expectedPresence: freshnessExpectedPresence,
          actualPresence: lastFreshnessPresence,
          column: freshnessResult?.columnName || freshnessColumnIdOrName || "",
          expected: freshnessColumnIdOrName ? freshnessExpectedValue : "",
          actual: freshnessColumnIdOrName ? lastFreshnessActual : "",
        },
      );

      if (!freshnessMatches) {
        if (attempt >= freshnessMaxAttempts) {
          if (!presenceMatches) {
            throw new Error(
              `Coda table freshness check failed for "${freshnessRowSelector}": expected row to be ${freshnessExpectedPresence}, got ${lastFreshnessPresence}.`,
            );
          }
          throw new Error(
            `Coda table freshness check failed for "${freshnessColumnIdOrName}" on "${freshnessRowSelector}": expected "${freshnessExpectedValue}", got "${lastFreshnessActual}".`,
          );
        }
        await sleep(freshnessDelayMs);
        continue;
      }
    }

    if (hasTableColumnFreshnessAnchor) {
      const columnValues = getColumnValuesForFreshness(tableData, resolvedFreshnessColumnId, freshnessColumnIdOrName);
      const blankCount = columnValues.values.filter((value) => !value).length;
      const nonblankCount = columnValues.values.length - blankCount;
      const signature = columnValues.values.join("\u001f");
      let freshnessMatches = false;

      if (freshnessMode === "any-nonblank") {
        freshnessMatches = nonblankCount > 0;
      } else if (freshnessMode === "stable") {
        freshnessMatches = attempt > 1 && signature === lastTableFreshnessSignature;
      } else {
        freshnessMatches = columnValues.rows.length > 0 && blankCount === 0;
      }

      eventLogger(
        freshnessMatches ? "info" : "warning",
        "freshness",
        freshnessMatches ? "Coda table column freshness matched" : "Coda table column freshness has not settled",
        {
          attempt,
          maxAttempts: freshnessMaxAttempts,
          column: columnValues.columnName,
          mode: freshnessMode,
          rowCount: columnValues.rows.length,
          nonblankCount,
          blankCount,
        },
      );

      if (!freshnessMatches) {
        if (attempt >= freshnessMaxAttempts) {
          throw new Error(
            `Coda table freshness check failed for "${freshnessColumnIdOrName}" in ${freshnessMode} mode: ${blankCount} blank value(s) across ${columnValues.rows.length} row(s).`,
          );
        }
        lastTableFreshnessSignature = signature;
        await sleep(freshnessDelayMs);
        continue;
      }
    }

    const mappingStartedAt = Date.now();
    mappingResult = await buildMappingFromTableData(tableData);
    eventLogger("info", "extract", "Mapped Coda snapshot", {
      attempt,
      itemCount: mappingResult.items.length,
      fieldCount: mappingResult.fields.length,
      warningCount: mappingResult.warnings.length,
      elapsedMs: Date.now() - mappingStartedAt,
    });

    const retryableWarning = shouldRetryForTransientCodaWarnings(mappingResult.warnings);
    if (!retryableWarning || attempt === maxSnapshotAttempts) {
      break;
    }

    const delayMs = codaStateRetryDelayMs * attempt;
    eventLogger("warning", "extract", "Transient Coda state detected; retrying snapshot", {
      attempt,
      maxCodaStateRetries: maxSnapshotAttempts,
      delayMs,
    });
    await sleep(delayMs);
  }

  eventLogger("info", "framer_sync", "Ensuring managed collection", {
    collectionName: payload.collectionName,
  });

  const convexResult = parseBooleanFlag(payload.bypassConvex)
    ? { skipped: true, count: 0, results: [] }
    : await upsertEventsToConvex({
      payload,
      mappingResult,
      eventLogger,
    });
  const framerItems = mappingResult.items.filter((item) =>
    shouldPublishItemToFramer({ payload, mappingResult, item }),
  );
  const skippedFramerItems = mappingResult.items.length - framerItems.length;
  if (skippedFramerItems > 0) {
    eventLogger("info", "framer_sync", "Skipping completed item(s) for Framer", {
      skippedFramerItems,
    });
  }
  const framerMappingResult = {
    ...mappingResult,
    items: framerItems,
  };

  let collection;
  let fieldsSet = 0;
  let itemsRemoved = 0;
  let publishResult = null;
  let publishError = "";
  const publishRequested = Boolean(payload.publish);
  const cachedCollection = isRowSync
    ? { collectionId: "", assumeNew: false }
    : await readCallbackCollectionId(payload, codaApiToken, eventLogger);

  await runSyncSession(
    payload.framerProjectUrl,
    framerApiKey,
    payload.collectionName,
    async ({ framer, collection: collectionHandle, collectionMeta, timeoutMs }) => {
      collection = collectionMeta;
      payload.resolvedCollectionId = collection.collectionId;
      let activeCollectionHandle = collectionHandle;

      async function refreshCollectionHandle(operationName, attempts = 3, delayMs = 1000) {
        let refreshedCollection = null;
        for (let attempt = 1; attempt <= attempts; attempt += 1) {
          const refreshed = await withTimeout(
            framer.getManagedCollections(),
            timeoutMs,
            `${operationName} getManagedCollections`,
          );
          refreshedCollection = refreshed.find((item) => item.id === collection.collectionId) || null;
          if (refreshedCollection) {
            activeCollectionHandle = refreshedCollection;
            return refreshedCollection;
          }
          if (attempt < attempts) await sleep(delayMs);
        }
        return null;
      }

      function shouldRefreshCollectionHandle(error) {
        const message = error instanceof Error ? error.message : String(error);
        return /expected a collection node|managed collection not found|collection not found/i.test(message);
      }

      // If collection was just created but not found, poll within the same session
      if (collection.created && collection.foundAfterCreate === false) {
        const pollAttempts = 5;
        const pollDelayMs = 1000;
        const found = await refreshCollectionHandle("getManagedCollections (poll)", pollAttempts, pollDelayMs);
        if (!found) {
          eventLogger("error", "framer_sync", "Collection not found after polling", { collectionName: collection.collectionName });
          throw new Error(`Managed collection not found after creation and polling: ${collection.collectionName}`);
        }
      }

      async function fetchItemIds(operationName) {
        if (typeof activeCollectionHandle.getItemIds !== "function") return [];

        const maxAttempts = parseIntEnv(process.env.FRAMER_RETRY_ATTEMPTS || 3, 3, 1, 6);
        const baseDelayMs = parseIntEnv(process.env.FRAMER_RETRY_DELAY_MS || 1000, 1000, 0, 10000);
        let lastError;

        for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
          try {
            const ids = await withTimeout(
              activeCollectionHandle.getItemIds(),
              timeoutMs,
              operationName,
            );
            return Array.isArray(ids) ? ids.map((id) => String(id)) : [];
          } catch (error) {
            lastError = error;
            if (shouldRefreshCollectionHandle(error)) {
              eventLogger("warning", "framer_sync", "Refreshing collection handle after item id read error", {
                operationName,
                attempt,
                message: error instanceof Error ? error.message : String(error),
              });
              await refreshCollectionHandle(`${operationName} refresh`);
              continue;
            }
            if (!isRetryableFramerError(error) || attempt === maxAttempts) {
              break;
            }

            const message = error instanceof Error ? error.message : String(error);
            const waitMs = computeBackoffMs(baseDelayMs, attempt);
            eventLogger("warning", "framer_sync", "Retrying item id read after transient Framer error", {
              operationName,
              attempt,
              maxAttempts,
              waitMs,
              message,
            });
            await sleep(waitMs);
          }
        }

        throw lastError;
      }

      // Set fields first — getItemIds shares the same WebSocket connection,
      // so running them in parallel risks a flaky connection killing both.
      let prefetchedItemIds = null;
      if (!isRowSync || collection.created) {
        const compatibleFields = framerMappingResult.fields.filter(Boolean);
        for (let attempt = 1; attempt <= 3; attempt += 1) {
          try {
            eventLogger("info", "framer_sync", "Setting managed collection fields", {
              collectionName: collection.collectionName,
              collectionId: collection.collectionId,
              fieldCount: compatibleFields.length,
              attempt,
            });
            await withTimeout(activeCollectionHandle.setFields(compatibleFields), timeoutMs, "setCollectionFields");
            break;
          } catch (error) {
            if (attempt >= 3 || !shouldRefreshCollectionHandle(error)) {
              throw error;
            }
            eventLogger("warning", "framer_sync", "Refreshing collection handle after setFields error", {
              collectionName: collection.collectionName,
              collectionId: collection.collectionId,
              attempt,
              message: error instanceof Error ? error.message : String(error),
            });
            await refreshCollectionHandle("setCollectionFields refresh");
            await sleep(1000);
          }
        }
        fieldsSet = compatibleFields.length;
      }

      if (isRowSync && mappingResult.items.length > 0 && framerMappingResult.items.length === 0) {
        const itemId = String(mappingResult.items[0]?.id || "");
        if (itemId) {
          try {
            await withTimeout(
              activeCollectionHandle.removeItems([itemId]),
              timeoutMs,
              "removeCompletedItemFromCollection (rowSync)",
            );
            itemsRemoved = 1;
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            eventLogger("warning", "framer_sync", "Completed item was not removed from Framer", {
              itemId,
              message,
            });
          }
        }
      }

      let managedItemIdsAfterAdd = null;

      if (framerMappingResult.items.length > 0) {

        // Fetch collection fields for remapping (use object fields first, fall back to getFields)
        const codaFieldIdToName = new Map(
          framerMappingResult.fields.map((field) => [field.id, field.name]),
        );

        async function fetchFields() {
          let normalized = normalizeCollectionFieldsLocal(activeCollectionHandle?.fields);
          if (normalized.length > 0) return normalized;
          if (typeof activeCollectionHandle.getFields === "function") {
            const fetched = await withTimeout(activeCollectionHandle.getFields(), timeoutMs, "getCollectionFields");
            normalized = normalizeCollectionFieldsLocal(fetched);
          }
          return normalized;
        }

        let collectionFields = await fetchFields();
        let collectionFieldNameToId = new Map(
          collectionFields.map((field) => [String(field.name).toLowerCase(), field.id]),
        );

        function remapItems(items) {
          return items.map((item) => {
            const remappedFieldData = {};
            for (const [sourceFieldId, fieldValue] of Object.entries(item?.fieldData || {})) {
              const sourceFieldName = codaFieldIdToName.get(sourceFieldId);
              if (!sourceFieldName) continue;
              const targetFieldId = collectionFieldNameToId.get(String(sourceFieldName).toLowerCase());
              if (!targetFieldId) continue;
              remappedFieldData[targetFieldId] = fieldValue;
            }
            return { ...item, fieldData: remappedFieldData };
          });
        }

        let itemsToAdd = remapItems(framerMappingResult.items);
        if (isRowSync && !collection.created) {
          const allowedFieldIds = new Set(collectionFields.map((field) => field.id));
          itemsToAdd = itemsToAdd.map((item) => {
            const filteredFieldData = {};
            for (const [fieldId, fieldValue] of Object.entries(item?.fieldData || {})) {
              if (allowedFieldIds.has(fieldId)) filteredFieldData[fieldId] = fieldValue;
            }
            return { ...item, fieldData: filteredFieldData };
          });
        }

        // Add items with one retry on field-not-found or stale collection handle.
        // Keep the optimized bulk write as the normal path.
        let addItemsAttempted = false;
        let addItemsError = null;
        for (let attempt = 0; attempt < 2; attempt++) {
          try {
            await withTimeout(
              activeCollectionHandle.addItems(itemsToAdd),
              timeoutMs,
              "addItemsToCollection (bulk)",
            );
            addItemsAttempted = true;
            break;
          } catch (err) {
            addItemsError = err;
            const msg = String(err?.message ?? err);
            if (attempt === 0 && (collection.created || shouldRefreshCollectionHandle(err) || /field not found/i.test(msg))) {
              eventLogger("warning", "framer_sync", "Retrying addItems after collection handle/schema refresh", { msg });
              await sleep(1500);
              await refreshCollectionHandle("addItemsToCollection refresh");
              collectionFields = await fetchFields();
              collectionFieldNameToId = new Map(
                collectionFields.map((field) => [String(field.name).toLowerCase(), field.id]),
              );
              itemsToAdd = remapItems(framerMappingResult.items);
              continue;
            } else {
              throw err;
            }
          }
        }
        if (!addItemsAttempted && addItemsError) throw addItemsError;

        // Verify items landed and reuse the same read for deleteMissing.
        try {
          if (!isRowSync && typeof activeCollectionHandle.getItemIds === "function") {
            managedItemIdsAfterAdd = await fetchItemIds("getManagedCollectionItemIds (after add)");
            const submittedIds = new Set(itemsToAdd.map((item) => String(item.id)));
            const afterSet = new Set(managedItemIdsAfterAdd);
            const missingNewIds = Array.from(submittedIds).filter((id) => !afterSet.has(id));
            if (missingNewIds.length > 0) {
              const warning = `Submitted ${itemsToAdd.length} items, but ${missingNewIds.length} submitted id(s) were not present after add: ${missingNewIds.join(", ")}`;
              mappingResult.warnings.push(warning);
              eventLogger("warning", "framer_sync", "Some submitted item IDs were not visible after add", { missingNewIds });
            }
          }
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          eventLogger("warning", "framer_sync", "Could not verify item ids after add", { message });
        }
      }

      // Delete missing items
      if (!isRowSync && payload.deleteMissing) {
        const codaItemIds = new Set(framerMappingResult.items.map((item) => String(item.id)));
        let managedItemIds = managedItemIdsAfterAdd ?? [];
        if (managedItemIdsAfterAdd === null && typeof activeCollectionHandle.getItemIds === "function") {
          managedItemIds = await fetchItemIds("getManagedCollectionItemIds (deleteMissing)");
        }
        const toRemove = managedItemIds.filter((id) => !codaItemIds.has(id));
        if (toRemove.length > 0) {
          await withTimeout(
            activeCollectionHandle.removeItems(toRemove),
            timeoutMs,
            "removeManagedCollectionItems",
          );
          itemsRemoved = toRemove.length;
        }
      }

    },
    {
      collectionId: cachedCollection.collectionId,
      assumeNew: cachedCollection.assumeNew,
    },
  );

  if (publishRequested) {
    eventLogger("info", "publishing", "Publishing project");
    try {
      const pubResult = await publishProject(payload.framerProjectUrl, framerApiKey);
      publishResult = { published: true, deploymentId: pubResult.deploymentId };
    } catch (error) {
      publishError = error instanceof Error ? error.message : String(error);
      eventLogger("warning", "publishing", "Publish failed after successful sync", { error: publishError });
    }
  }

  const publishSucceeded = Boolean(publishResult?.published);
  const overallSuccess = !publishRequested || publishSucceeded;
  const itemSummary = `${framerMappingResult.items.length} Framer item${framerMappingResult.items.length === 1 ? "" : "s"}${itemsRemoved > 0 ? `, removed ${itemsRemoved}` : ""}`;
  const baseSyncMessage = `Synced ${itemSummary} to "${collection.collectionName}"`;
  const convexSaved = Boolean(convexResult && !convexResult.skipped);
  const convexPrefix = convexSaved
    ? `Saved ${convexResult.count || 0} to Convex. `
    : "";

  const responseBody = {
    success: overallSuccess,
    syncSuccess: true,
    convexSynced: convexSaved,
    convexSyncedCount: convexResult?.count || 0,
    convexEventId: convexResult?.results?.[0]?.response?.eventId || "",
    publishRequested,
    collectionId: collection.collectionId,
    collectionName: collection.collectionName,
    sourceRowId: isRowSync ? String(mappingResult.items?.[0]?.id || "") : "",
    itemsAdded: framerMappingResult.items.length,
    itemsRemoved,
    fieldsSet,
    warnings: mappingResult.warnings,
    published: publishSucceeded,
    publishError,
    deploymentId: publishResult?.deploymentId ?? "",
    message: publishSucceeded
      ? `✅ ${convexPrefix}${baseSyncMessage} and published at ${new Date().toLocaleString("en-US", { month: "numeric", day: "numeric", hour: "numeric", minute: "2-digit", hour12: true, timeZone: "America/New_York" })} ET.`
      : publishRequested
        ? `⚠️ ${convexPrefix}${baseSyncMessage}, but Framer publish failed: ${publishError}`
        : `✅ ${convexPrefix}${baseSyncMessage}. Framer publish queued only when requested.`,
  };

  return responseBody;
}

// When a status-cell update fails with a transient error, we spin up a
// secondary job to retry the callback at a later time. This keeps the main
// sync job from being marked as failed while still pushing the status row
// eventually.
function scheduleCallbackRetry(originalPayload, message, retryCount, eventLogger, context) {
  const newPayload = {
    action: "callback",
    callback: originalPayload.callback || {},
    docId: originalPayload.docId,
    tableIdOrName: originalPayload.tableIdOrName,
    rowId: originalPayload.rowId,
    slugFieldId: originalPayload.slugFieldId,
    resolvedCollectionId: originalPayload.resolvedCollectionId,
    callbackColumnIds: originalPayload.callbackColumnIds,
    callbackRetryCount: retryCount,
    retryMessage: message,
  };

  const requestId = originalPayload.requestId || "";
  const jobId = randomUUID();
  createJob({ jobId, requestId, idempotencyKey: "", payload: newPayload });
  eventLogger("info", "callback_retry_scheduled", "Scheduled follow-up callback job", {
    jobId,
    retryCount,
  });

  const run = async () => {
    // give Coda a little breathing room before retrying (typically 5s)
    await sleep(5000);
    await processJob(jobId, context);
  };
  if (context && typeof context.waitUntil === "function") {
    context.waitUntil(run());
  } else {
    // best effort; the original invocation may die but this gives it a chance
    setTimeout(run, 1000 * 10);
  }
}

function getCallbackTargetInputs(payload) {
  const callback = payload.callback || {};
  const hasCallbackPayload = Boolean(payload.callback && Object.keys(callback).length > 0);
  const isSyncWithoutCallback = !hasCallbackPayload && String(payload.action || "sync") === "sync";
  const hasExplicitCallbackTable = Object.prototype.hasOwnProperty.call(callback, "statusTableIdOrName")
    || Object.prototype.hasOwnProperty.call(callback, "statusTableInput");
  const defaultCallbackTableName = getDefaultCallbackTableName();
  const rawStatusColumnNameOrId = callback.statusColumn || callback.statusColumnId || callback.statusColumnNameOrId || "Server Status";
  const rawStatusRowSelector = callback.statusRow || callback.statusRowId || callback.statusRowSelector || payload.rowId || "";
  const rawStatusTableIdOrName = callback.statusTableIdOrName
    || callback.statusTableInput
    || (isSyncWithoutCallback ? "FramerSync" : (hasExplicitCallbackTable ? defaultCallbackTableName : (payload.tableIdOrName || "")));
  const statusDocId = callback.statusDocId || payload.docId || "";

  const looksLikeUnknownObject = (value) => {
    const text = String(value || "").trim().toLowerCase();
    return text.includes("[unknown object]") || text === "[object object]";
  };

  const extractCodaId = (value) => {
    const text = String(value || "").trim();
    if (!text) return "";
    const rowMatch = text.match(/(?:\/rows\/|\b)(i-[A-Za-z0-9_-]+)\b/);
    if (rowMatch?.[1]) return rowMatch[1];
    const tableMatch = text.match(/\b(grid-[A-Za-z0-9_-]+)\b/);
    if (tableMatch?.[1]) return tableMatch[1];
    const columnMatch = text.match(/\b(c-[A-Za-z0-9_-]+)\b/);
    if (columnMatch?.[1]) return columnMatch[1];
    return "";
  };

  let statusRowSelector = String(rawStatusRowSelector || "").trim();
  let statusTableIdOrName = String(rawStatusTableIdOrName || "").trim();

  if (looksLikeUnknownObject(statusTableIdOrName)) {
    statusTableIdOrName = hasExplicitCallbackTable
      ? String(defaultCallbackTableName || "").trim()
      : String(payload.tableIdOrName || "").trim();
  }

  if (looksLikeUnknownObject(statusRowSelector)) {
    statusRowSelector = "";
  }

  const extractedRowId = extractCodaId(statusRowSelector);
  if (extractedRowId && isApiRowId(extractedRowId)) {
    statusRowSelector = extractedRowId;
  } else if (/^https?:\/\//i.test(statusRowSelector)) {
    statusRowSelector = "";
  }

  return {
    callback,
    statusDocId,
    statusTableIdOrName,
    statusColumnNameOrId: String(rawStatusColumnNameOrId || "").trim(),
    statusRowSelector,
  };
}

function parseCallbackColumnIds(payload) {
  const cbColumnIdMap = new Map();
  let cbPreresolvedTableId = "";
  if (payload.callbackColumnIds) {
    const raw = String(payload.callbackColumnIds);
    const pipeIdx = raw.indexOf("|");
    const colPart = pipeIdx > 0 ? raw.slice(pipeIdx + 1) : raw;
    if (pipeIdx > 0) cbPreresolvedTableId = raw.slice(0, pipeIdx).trim();
    for (const entry of colPart.split(",")) {
      const colon = entry.indexOf(":");
      if (colon > 0) {
        const name = entry.slice(0, colon).trim();
        const id = entry.slice(colon + 1).trim();
        if (name && id) cbColumnIdMap.set(name.toLowerCase(), id);
      }
    }
  }

  function resolveCbColumn(nameOrId) {
    if (!nameOrId) return null;
    if (/^c-[A-Za-z0-9_-]+$/.test(String(nameOrId))) return nameOrId;
    return cbColumnIdMap.get(String(nameOrId).toLowerCase()) || null;
  }

  return { cbPreresolvedTableId, resolveCbColumn };
}

async function resolveCallbackRowForCollectionId(payload, codaApiToken, eventLogger) {
  const {
    callback,
    statusDocId,
    statusTableIdOrName,
    statusRowSelector,
  } = getCallbackTargetInputs(payload);

  if (!statusDocId || !statusTableIdOrName || !statusRowSelector) {
    return null;
  }

  const { cbPreresolvedTableId, resolveCbColumn } = parseCallbackColumnIds(payload);
  const resolvedStatusTableId = cbPreresolvedTableId
    || await (async () => {
      const ref = await resolveTableCached(statusDocId, statusTableIdOrName, codaApiToken);
      return resolveBaseTableCached(statusDocId, ref, codaApiToken);
    })();

  let matchedRow = null;
  let callbackTableData = null;
  if (isApiRowId(statusRowSelector)) {
    callbackTableData = await getCodaRowData(
      statusDocId,
      resolvedStatusTableId,
      statusRowSelector,
      codaApiToken,
    );
    matchedRow = callbackTableData.rows[0] || null;
  } else {
    let callbackSlugFieldId = callback.statusSlugField || callback.statusSlugFieldId || payload.slugFieldId || "";
    if (callbackSlugFieldId) {
      callbackSlugFieldId = resolveCbColumn(callbackSlugFieldId)
        || await resolveColumnCached(statusDocId, resolvedStatusTableId, callbackSlugFieldId, codaApiToken);
    }

    if (callbackSlugFieldId) {
      callbackTableData = await getCodaRowByColumnValue(
        statusDocId,
        resolvedStatusTableId,
        callbackSlugFieldId,
        statusRowSelector,
        codaApiToken,
      );
      matchedRow = findRowBySelector(callbackTableData, statusRowSelector, callbackSlugFieldId);
    } else {
      const rowSearchLimit = parseIntEnv(callback.statusRowSearchLimit ?? 500, 500, 1, 500);
      callbackTableData = await getCodaTableData(
        statusDocId,
        resolvedStatusTableId,
        codaApiToken,
        rowSearchLimit,
      );
      matchedRow = findRowBySelector(callbackTableData, statusRowSelector, "");
    }
  }

  if (!matchedRow) {
    eventLogger("warning", "callback", "Could not find callback row for cached collection ID", {
      statusDocId,
      statusTableIdOrName: resolvedStatusTableId,
      statusRowSelector,
    });
    return null;
  }

  return { callbackTableData, matchedRow, resolvedStatusTableId };
}

function extractCallbackCollectionId(callbackTableData, row, collectionIdColumnId) {
  if (!row?.values) return "";
  const column = callbackTableData?.columns?.find((item) => String(item?.id || "") === collectionIdColumnId);
  const keys = [
    column?.name,
    "CollectionID",
    "Collection ID",
    "Collection Id",
    "collectionId",
  ].filter(Boolean);

  for (const key of keys) {
    const value = normalizeSelectorValue(row.values[key]);
    if (value) return value;
  }

  for (const [key, rawValue] of Object.entries(row.values)) {
    const normalizedKey = String(key || "").toLowerCase().replace(/[^a-z0-9]/g, "");
    if (normalizedKey === "collectionid") {
      const value = normalizeSelectorValue(rawValue);
      if (value) return value;
    }
  }

  return "";
}

async function readCallbackCollectionId(payload, codaApiToken, eventLogger) {
  const collectionIdColumnId = getCallbackCollectionIdColumnId();
  if (!collectionIdColumnId) return { collectionId: "", assumeNew: false };

  try {
    const target = await resolveCallbackRowForCollectionId(payload, codaApiToken, eventLogger);
    if (!target) {
      return { collectionId: "", assumeNew: false };
    }
    const collectionId = extractCallbackCollectionId(
      target?.callbackTableData,
      target?.matchedRow,
      collectionIdColumnId,
    );
    if (collectionId) {
      eventLogger("info", "callback", "Read cached Framer collection ID from Coda", {
        collectionId,
        collectionIdColumnId,
      });
      return { collectionId, assumeNew: false };
    } else {
      eventLogger("info", "callback", "No cached Framer collection ID on callback row; using new-collection path", {
        collectionIdColumnId,
      });
      return { collectionId: "", assumeNew: true };
    }
  } catch (error) {
    eventLogger("warning", "callback", "Could not read cached Framer collection ID; falling back to name resolution", {
      collectionIdColumnId,
      callbackError: error instanceof Error ? error.message : String(error),
    });
    return { collectionId: "", assumeNew: false };
  }
}

async function writeStatusCallback(payload, message, eventLogger, jobSnapshot = null, context = {}) {
  const callback = payload.callback || {};
  const hasCallbackPayload = Boolean(payload.callback && Object.keys(callback).length > 0);
  const isSyncWithoutCallback = !hasCallbackPayload && String(payload.action || "sync") === "sync";
  const hasExplicitCallbackTable = Object.prototype.hasOwnProperty.call(callback, "statusTableIdOrName")
    || Object.prototype.hasOwnProperty.call(callback, "statusTableInput");
  const defaultCallbackTableName = getDefaultCallbackTableName();
  const rawStatusColumnNameOrId = callback.statusColumn || callback.statusColumnId || callback.statusColumnNameOrId || "Server Status";
  const rawStatusRowSelector = callback.statusRow || callback.statusRowId || callback.statusRowSelector || payload.rowId || "";
  const rawStatusTableIdOrName = callback.statusTableIdOrName
    || callback.statusTableInput
    || (isSyncWithoutCallback ? "FramerSync" : (hasExplicitCallbackTable ? defaultCallbackTableName : (payload.tableIdOrName || "")));
  const statusDocId = callback.statusDocId || payload.docId || "";

  const statusColumnNameOrId = String(rawStatusColumnNameOrId || "").trim();
  let statusRowSelector = String(rawStatusRowSelector || "").trim();
  let statusTableIdOrName = String(rawStatusTableIdOrName || "").trim();

  const looksLikeUnknownObject = (value) => {
    const text = String(value || "").trim().toLowerCase();
    return text.includes("[unknown object]") || text === "[object object]";
  };

  const extractCodaId = (value) => {
    const text = String(value || "").trim();
    if (!text) return "";
    const rowMatch = text.match(/(?:\/rows\/|\b)(i-[A-Za-z0-9_-]+)\b/);
    if (rowMatch?.[1]) return rowMatch[1];
    const tableMatch = text.match(/\b(grid-[A-Za-z0-9_-]+)\b/);
    if (tableMatch?.[1]) return tableMatch[1];
    const columnMatch = text.match(/\b(c-[A-Za-z0-9_-]+)\b/);
    if (columnMatch?.[1]) return columnMatch[1];
    return "";
  };

  if (looksLikeUnknownObject(statusTableIdOrName)) {
    statusTableIdOrName = hasExplicitCallbackTable
      ? String(defaultCallbackTableName || "").trim()
      : String(payload.tableIdOrName || "").trim();
  }

  if (looksLikeUnknownObject(statusRowSelector)) {
    statusRowSelector = "";
  }

  const extractedRowId = extractCodaId(statusRowSelector);
  if (extractedRowId && isApiRowId(extractedRowId)) {
    statusRowSelector = extractedRowId;
  } else if (/^https?:\/\//i.test(statusRowSelector)) {
    statusRowSelector = "";
  }

  // Require doc/table/column/row. Callback writes now only update an existing
  // Server Status cell; they no longer create log rows or update extra fields.
  if (!statusColumnNameOrId || !statusTableIdOrName || !statusDocId) {
    eventLogger("warning", "callback", "Skipped Coda status callback: missing callback target", {
      hasStatusDocId: Boolean(statusDocId),
      hasStatusTableIdOrName: Boolean(statusTableIdOrName),
      hasStatusRowSelector: Boolean(statusRowSelector),
      hasStatusColumnNameOrId: Boolean(statusColumnNameOrId),
    });
    return;
  }

  if (!statusRowSelector) {
    eventLogger("warning", "callback", "Skipped Coda status callback: missing target row", {
      statusDocId,
      statusTableIdOrName,
      statusColumnNameOrId,
    });
    return;
  }

  const codaApiToken = process.env.CODA_API_TOKEN;
  if (!codaApiToken) {
    eventLogger("warning", "callback", "Skipped Coda status callback: missing CODA_API_TOKEN");
    return;
  }

  // Parse pre-resolved callback column IDs (format: "grid-xxx|Col:c-yyy,Col:c-zzz")
  // to skip table and column resolution entirely on the callback path.
  const cbColumnIdMap = new Map();
  let cbPreresolvedTableId = "";
  if (payload.callbackColumnIds) {
    const raw = String(payload.callbackColumnIds);
    const pipeIdx = raw.indexOf("|");
    const colPart = pipeIdx > 0 ? raw.slice(pipeIdx + 1) : raw;
    if (pipeIdx > 0) cbPreresolvedTableId = raw.slice(0, pipeIdx).trim();
    for (const entry of colPart.split(",")) {
      const colon = entry.indexOf(":");
      if (colon > 0) {
        const name = entry.slice(0, colon).trim();
        const id = entry.slice(colon + 1).trim();
        if (name && id) cbColumnIdMap.set(name.toLowerCase(), id);
      }
    }
  }

  function resolveCbColumn(nameOrId) {
    if (!nameOrId) return null;
    if (/^c-[A-Za-z0-9_-]+$/.test(String(nameOrId))) return nameOrId;
    return cbColumnIdMap.get(String(nameOrId).toLowerCase()) || null;
  }

  try {
    const resolvedStatusTableId = cbPreresolvedTableId
      || await (async () => {
        const ref = await resolveTableCached(statusDocId, statusTableIdOrName, codaApiToken);
        return resolveBaseTableCached(statusDocId, ref, codaApiToken);
      })();

    const resolvedStatusColumnId = resolveCbColumn(statusColumnNameOrId)
      || await resolveColumnCached(statusDocId, resolvedStatusTableId, statusColumnNameOrId, codaApiToken);

    let resolvedStatusRowId = "";
    const reusableSourceRowId = String(
      payload.sourceRowId
      || jobSnapshot?.result?.sourceRowId
      || "",
    );
    if (isApiRowId(statusRowSelector)) {
      resolvedStatusRowId = statusRowSelector;
    } else if (
      reusableSourceRowId
      && isApiRowId(reusableSourceRowId)
      && normalizeSelectorValue(statusRowSelector) === normalizeSelectorValue(payload.rowId)
    ) {
      resolvedStatusRowId = reusableSourceRowId;
      eventLogger("info", "callback", "Reused source row ID for status callback", {
        statusRowSelector,
        resolvedStatusRowId,
      });
    } else {
      let callbackSlugFieldId = callback.statusSlugField || callback.statusSlugFieldId || payload.slugFieldId || "";
      if (callbackSlugFieldId) {
        callbackSlugFieldId = resolveCbColumn(callbackSlugFieldId)
          || await resolveColumnCached(statusDocId, resolvedStatusTableId, callbackSlugFieldId, codaApiToken);
      }

      if (callbackSlugFieldId) {
        const queried = await getCodaRowByColumnValue(
          statusDocId,
          resolvedStatusTableId,
          callbackSlugFieldId,
          statusRowSelector,
          codaApiToken,
        );
        const matchedRow = findRowBySelector(queried, statusRowSelector, callbackSlugFieldId);
        resolvedStatusRowId = matchedRow?.id || "";
      } else {
        const rowSearchLimit = parseIntEnv(callback.statusRowSearchLimit ?? 500, 500, 1, 500);
        const callbackTableData = await getCodaTableData(
          statusDocId,
          resolvedStatusTableId,
          codaApiToken,
          rowSearchLimit,
        );
        const matchedRow = findRowBySelector(callbackTableData, statusRowSelector, "");
        resolvedStatusRowId = matchedRow?.id || "";
      }
    }

    if (!resolvedStatusRowId) {
      eventLogger("warning", "callback", "Skipped Coda status callback: target row not found", {
        statusDocId,
        statusTableIdOrName: resolvedStatusTableId,
        statusRowSelector,
        statusColumnNameOrId,
      });
      return;
    }

    await updateTableCell(
      statusDocId,
      resolvedStatusTableId,
      resolvedStatusRowId,
      resolvedStatusColumnId,
      message,
      codaApiToken,
      { waitForMutation: false },
    );
    eventLogger("info", "callback", "Updated Coda status cell", {
      statusDocId,
      statusTableIdOrName: resolvedStatusTableId,
      statusRowSelector,
      resolvedStatusRowId,
      statusColumnNameOrId,
      resolvedStatusColumnId,
    });

    const collectionIdColumnId = getCallbackCollectionIdColumnId();
    const resolvedCollectionId = String(
      payload.resolvedCollectionId
      || jobSnapshot?.result?.collectionId
      || payload.collectionId
      || "",
    ).trim();
    if (collectionIdColumnId && resolvedCollectionId && payload.action !== "rowSync") {
      await updateTableCell(
        statusDocId,
        resolvedStatusTableId,
        resolvedStatusRowId,
        collectionIdColumnId,
        resolvedCollectionId,
        codaApiToken,
        { waitForMutation: false },
      );
      eventLogger("info", "callback", "Updated Coda collection ID cell", {
        statusDocId,
        statusTableIdOrName: resolvedStatusTableId,
        statusRowSelector,
        resolvedStatusRowId,
        collectionIdColumnId,
        collectionId: resolvedCollectionId,
      });
    }
  } catch (error) {
    const callbackError = error instanceof Error ? error.message : String(error);
    eventLogger("warning", "callback", "Failed to update Coda status cell", {
      callbackError,
      statusDocId,
      statusTableIdOrName,
      statusRowSelector,
      statusColumnNameOrId,
    });

    const retryable = isRetryableCodaError(error);
    if (retryable) {
      eventLogger("info", "callback", "Callback failure is retryable; scheduling follow-up job", {
        callbackError,
      });
      // schedule a lightweight job to try again later, but only up to a
      // configurable limit so we don’t loop forever.
      const existingCount = payload.callbackRetryCount || 0;
      const maxRetries = parseIntEnv(process.env.CODA_CALLBACK_RETRY_JOB_MAX || 3, 3, 1, 20);
      if (existingCount < maxRetries) {
        scheduleCallbackRetry(payload, message, existingCount + 1, eventLogger, context);
      } else {
        eventLogger("warn", "callback", "Exceeded max callback retry jobs", { existingCount, maxRetries });
      }
    }
  }
}

async function processJob(jobId, context = {}) {
  const job = getJob(jobId);
  if (!job) {
    return;
  }

  log("info", "job_started", {
    jobId,
    requestId: job.requestId,
    action: job.payload?.action || "sync",
    docId: job.payload?.docId,
    tableIdOrName: job.payload?.tableIdOrName,
  });

  const eventLogger = (level, stage, message, details = {}) => {
    appendJobEvent(jobId, { level, stage, message, details });
    const normalizedLevel = level === "warning" ? "warn" : level;
    log(normalizedLevel, "job_event", {
      jobId,
      requestId: job.requestId,
      stage,
      message,
      details,
    });
  };

  updateJob(jobId, {
    status: "running",
    startedAt: new Date().toISOString(),
  });

  const initialDelayMs = parseIntEnv(
    job.payload.initialDelayMs ?? process.env.CODA_INITIAL_DELAY_MS ?? 1200,
    1200,
    0,
    120000,
  );

  if (initialDelayMs > 0) {
    updateJob(jobId, { status: "delayed" });
    eventLogger("info", "delay", "Applying initial Coda visibility delay", { initialDelayMs });
    await sleep(initialDelayMs);
    updateJob(jobId, { status: "running" });
  }

  // handle special 'callback' only jobs that may have been scheduled
  // when an earlier run failed to update the status row due to a throttling
  // spike.
  if (job.payload.action === "callback") {
    eventLogger("info", "callback_job", "Processing standalone callback job");
    const retryMsg = job.payload.retryMessage || "";
    try {
      await writeStatusCallback(job.payload, retryMsg, eventLogger, job, context);
      updateJob(jobId, {
        status: "succeeded",
        completedAt: new Date().toISOString(),
        result: { success: true, message: retryMsg },
        error: null,
      });
      eventLogger("info", "completed", "Callback job succeeded", {});
    } catch (cbErr) {
      const errMsg = cbErr instanceof Error ? cbErr.message : String(cbErr);
      updateJob(jobId, {
        status: "failed",
        completedAt: new Date().toISOString(),
        result: makeFailureResponse(errMsg),
        error: errMsg,
      });
      eventLogger("error", "failed", "Callback job failed", { error: errMsg });
    }
    return;
  }

  try {
    const result = await executeSyncWorkflow(job.payload, eventLogger);
    const isPartialFailure = Boolean(result.syncSuccess && result.publishRequested && !result.published);
    updateJob(jobId, {
      status: result.published ? "published" : (isPartialFailure ? "partial_failed" : "succeeded"),
      completedAt: new Date().toISOString(),
      result,
      error: result.publishError || null,
    });
    eventLogger("info", "completed", "Job completed", {
      success: result.success,
      syncSuccess: result.syncSuccess,
      partialFailure: isPartialFailure,
      published: result.published,
      itemsAdded: result.itemsAdded,
      itemsRemoved: result.itemsRemoved,
      publishError: result.publishError || "",
    });
    const completedJob = getJobWithEvents(jobId) || getJob(jobId);
    if (!job.payload.skipStatusCallback) {
      const callbackMessage = job.payload.isReconciliation
        ? `🔄 Reconciliation: ${result.message}`
        : result.message;
      await writeStatusCallback(job.payload, callbackMessage, eventLogger, completedJob, context);
    }
    scheduleReconciliation(getJob(jobId) || job, eventLogger, context);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    const failure = makeFailureResponse(errorMessage);
    updateJob(jobId, {
      status: "failed",
      completedAt: new Date().toISOString(),
      result: failure,
      error: errorMessage,
    });
    eventLogger("error", "failed", "Job failed", {
      error: errorMessage,
    });
    const failedJob = getJobWithEvents(jobId) || getJob(jobId);
    if (!job.payload.skipStatusCallback) {
      const callbackMessage = job.payload.isReconciliation
        ? `🔄 Reconciliation: ${failure.message}`
        : failure.message;
      await writeStatusCallback(job.payload, callbackMessage, eventLogger, failedJob, context);
    }
    scheduleReconciliation(getJob(jobId) || job, eventLogger, context);
  }
}

function validateSyncPayload(payload) {
  const required = [
    "docId",
    "tableIdOrName",
    "framerProjectUrl",
    "collectionName",
    "slugFieldId",
  ];

  for (const field of required) {
    if (!payload[field]) {
      return `Missing required field: ${field}`;
    }
  }

  if (payload.action === "deleteRow" && !payload.rowId) {
    return "Missing required field for deleteRow: rowId";
  }

  if (!process.env.CODA_API_TOKEN) {
    return "CODA_API_TOKEN is not configured";
  }

  if (!(process.env.FRAMER_API_KEY || payload.framerApiKey)) {
    return "Missing Framer API key";
  }

  return "";
}

export default async function handler(req, res) {
  const requestUrl = new URL(req.url || "/api/sync", "http://localhost");
  const vercelTrace = req?.headers?.["x-vercel-id"] || "";
  const userAgent = req?.headers?.["user-agent"] || "";
  log("info", "request_received", {
    method: req.method,
    path: requestUrl.pathname,
    vercelTrace,
    userAgent,
  });

  // Special endpoint for listing Framer collections
  if (req.method === "GET" && requestUrl.pathname.endsWith("/collections")) {
    const projectUrl = requestUrl.searchParams.get("projectUrl") || requestUrl.searchParams.get("framerProjectUrl");
    if (!projectUrl) {
      return sendJson(res, 400, {
        error: "INVALID_REQUEST",
        message: "Missing query param: projectUrl",
      });
    }
    const framerApiKey = process.env.FRAMER_API_KEY;
    if (!framerApiKey) {
      return sendJson(res, 500, {
        error: "SERVER_ERROR",
        message: "FRAMER_API_KEY not configured",
      });
    }
    try {
      const collections = await listCollections(projectUrl, framerApiKey);
      return sendJson(res, 200, {
        success: true,
        collections,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return sendJson(res, 500, {
        error: "SERVER_ERROR",
        message,
      });
    }
  }

  if (req.method === "GET") {
    const listMode = requestUrl.searchParams.get("list") === "1"
      || requestUrl.searchParams.get("mode") === "list"
      || requestUrl.searchParams.get("syncTable") === "1";

    if (listMode) {
      const limit = parseIntEnv(
        requestUrl.searchParams.get("limit") || 50,
        50,
        1,
        200,
      );
      const cursor = parseIntEnv(
        requestUrl.searchParams.get("cursor") || 0,
        0,
        0,
        1000000,
      );

      const page = listJobsWithEvents({ limit, cursor });
      log("info", "status_list", {
        count: page.jobs.length,
        limit,
        cursor,
        hasContinuation: Boolean(page.continuation?.cursor),
        vercelTrace,
      });

      return sendJson(res, 200, {
        success: true,
        jobs: page.jobs,
        continuation: page.continuation,
        total: page.total,
      });
    }

    const jobId = parseJobIdFromRequest(req);
    if (!jobId) {
      log("warn", "status_missing_jobId", {
        vercelTrace,
      });
      return sendJson(res, 400, {
        error: "INVALID_REQUEST",
        message: "Missing required query param: jobId",
      });
    }

    const job = getJobWithEvents(jobId);
    if (!job) {
      log("warn", "status_lookup_miss", {
        jobId,
        vercelTrace,
      });
      return sendJson(res, 404, {
        error: "NOT_FOUND",
        message: `No job found for jobId: ${jobId}`,
      });
    }

    log("info", "status_lookup_hit", {
      jobId,
      requestId: job.requestId,
      status: job.status,
      vercelTrace,
    });

    return sendJson(res, 200, {
      success: true,
      job,
    });
  }

  if (req.method !== "POST") {
    return sendJson(res, 405, {
      error: "METHOD_NOT_ALLOWED",
      message: "Use POST /api/sync or GET /api/sync?jobId=...",
    });
  }

  try {
    const payload = await readJsonBody(req);

    if (payload.action === "publish") {
      if (!payload.framerProjectUrl) {
        return sendJson(res, 400, {
          error: "INVALID_REQUEST",
          message: "Missing required field: framerProjectUrl",
        });
      }

      const framerApiKey = process.env.FRAMER_API_KEY || payload.framerApiKey;
      if (!framerApiKey) {
        return sendJson(res, 400, {
          error: "INVALID_REQUEST",
          message: "Missing Framer API key",
        });
      }

      const publishResult = await publishProject(
        payload.framerProjectUrl,
        framerApiKey,
      );

      return sendJson(res, 200, publishResult);
    }

    const validationError = validateSyncPayload(payload);
    if (validationError) {
      return sendJson(res, 400, {
        error: "INVALID_REQUEST",
        message: validationError,
      });
    }

    const idempotencyKey = payload.idempotencyKey || "";
    if (idempotencyKey) {
      const existing = findActiveJobByIdempotency(idempotencyKey);
      if (existing) {
        log("info", "request_deduped", {
          idempotencyKey,
          existingJobId: existing.jobId,
          requestId: existing.requestId,
          status: existing.status,
          vercelTrace,
        });
        return sendJson(res, 202, {
          accepted: true,
          deduped: true,
          jobId: existing.jobId,
          requestId: existing.requestId,
          status: existing.status,
          message: `⏳ Already running (jobId: ${existing.jobId})`,
        });
      }
    }

    const requestId = payload.requestId || randomUUID();
    const jobId = randomUUID();
    const shouldPreResolveCallback = parseBooleanFlag(process.env.CODA_PRE_RESOLVE_CALLBACK, false);
    const payloadWithCallbackRow = shouldPreResolveCallback
      ? await provisionCallbackRowIfNeeded(payload, {
        requestId,
        jobId,
        vercelTrace,
      })
      : payload;
    createJob({
      jobId,
      requestId,
      idempotencyKey,
      payload: payloadWithCallbackRow,
    });

    log("info", "request_accepted", {
      requestId,
      jobId,
      idempotencyKey,
      action: payloadWithCallbackRow.action || "sync",
      docId: payloadWithCallbackRow.docId,
      tableIdOrName: payloadWithCallbackRow.tableIdOrName,
      rowId: payloadWithCallbackRow.rowId || "",
      publish: Boolean(payloadWithCallbackRow.publish),
      vercelTrace,
    });

    scheduleJobProcessing({
      req,
      res,
      jobId,
      requestId,
    });

    const actionLabel = ({ sync: "Sync", rowSync: "Row Sync", deleteRow: "Delete" })[payloadWithCallbackRow.action] || "Sync";
    const startedAt = new Date().toLocaleString("en-US", { month: "numeric", day: "numeric", hour: "numeric", minute: "2-digit", hour12: true, timeZone: "America/New_York" }) + " ET";
    return sendJson(res, 202, {
      accepted: true,
      jobId,
      requestId,
      status: "queued",
      action: payloadWithCallbackRow.action,
      message: `⏳ ${actionLabel} started at ${startedAt}`,
      statusUrl: `/api/sync?jobId=${jobId}`,
    });
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    return sendJson(res, 500, {
      error: "SERVER_ERROR",
      message: errorMsg,
    });
  }
}
