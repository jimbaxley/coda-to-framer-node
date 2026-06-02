import { randomUUID } from "node:crypto";
import { waitUntil as vercelWaitUntil } from "@vercel/functions";
import {
  getCodaTableData,
  getCodaRowData,
  getCodaTableColumns,
  getCodaTables,
  resolveColumnNameOrId,
  resolveTableNameOrId,
  updateTableCell,
  updateTableRowCells,
  createTableRow,
} from "../lib/coda-client.js";
import {
  normalizeColumns,
  normalizeRows,
  buildFieldsAndItems,
  resolveReferenceFields,
} from "../lib/mapping.js";
import {
  publishProject,
  listCollections,
  runSyncSession,
  withTimeout,
  formatFramerError,
  getPublishTimeoutMs,
} from "../lib/framer-client.js";

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
  shouldRetryForTransientCodaWarnings,
  isRetryableCodaError,
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
    const resolvedStatusTableId = await resolveTableNameOrId(
      statusDocId,
      statusTableIdOrName,
      codaApiToken,
    );

    const resolvedStatusColumnId = await resolveColumnNameOrId(
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

  const isRowSync = payload.action === "rowSync" || Boolean(payload.rowId);
  if (isRowSync && !payload.rowId) {
    throw new Error("Missing required field for rowSync: rowId");
  }

  let resolvedSlugFieldId = payload.slugFieldId;
  const codaReadOptions = {
    requireLatest: parseBooleanFlag(
      payload.requireLatestCodaSnapshot ?? process.env.CODA_REQUIRE_LATEST_SOURCE_READS,
      false,
    ),
  };
  eventLogger("info", "extract", "Configured Coda snapshot read mode", {
    requireLatest: codaReadOptions.requireLatest,
  });

  if (payload.slugFieldId) {
    resolvedSlugFieldId = await resolveColumnNameOrId(
      payload.docId,
      payload.tableIdOrName,
      payload.slugFieldId,
      codaApiToken,
      codaReadOptions,
    );
    if (resolvedSlugFieldId !== payload.slugFieldId) {
      eventLogger("info", "resolve_slug", "Resolved slug field ID", {
        before: payload.slugFieldId,
        after: resolvedSlugFieldId,
      });
    }
  }

  const getCodaSnapshot = async () => {
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
        tableData = {
          columns: selectorData.columns,
          rows: [matchedRow],
        };
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

    const columns = normalizeColumns(tableData.columns);
    const rows = normalizeRows(tableData.rows);

    const mappingResult = buildFieldsAndItems({
      columns,
      rows,
      slugFieldId: resolvedSlugFieldId,
      use12HourTime: payload.use12HourTime !== false,
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
  for (let attempt = 1; attempt <= maxCodaStateRetries; attempt += 1) {
    mappingResult = await getCodaSnapshot();

    const retryableWarning = shouldRetryForTransientCodaWarnings(mappingResult.warnings);
    if (!retryableWarning || attempt === maxCodaStateRetries) {
      break;
    }

    const delayMs = codaStateRetryDelayMs * attempt;
    eventLogger("warning", "extract", "Transient Coda state detected; retrying snapshot", {
      attempt,
      maxCodaStateRetries,
      delayMs,
    });
    await sleep(delayMs);
  }

  eventLogger("info", "framer_sync", "Ensuring managed collection", {
    collectionName: payload.collectionName,
  });

  let collection;
  let fieldsSet = 0;
  let itemsRemoved = 0;
  let publishResult = null;
  let publishError = "";
  const publishRequested = Boolean(payload.publish);

  await runSyncSession(
    payload.framerProjectUrl,
    framerApiKey,
    payload.collectionName,
    async ({ framer, collection: collectionHandle, collectionMeta, timeoutMs }) => {
      collection = collectionMeta;

      // If collection was just created but not found, poll within the same session
      if (collection.created && collection.foundAfterCreate === false) {
        const pollAttempts = 5;
        const pollDelayMs = 1000;
        let found = false;
        for (let i = 0; i < pollAttempts; i++) {
          try {
            const refreshed = await withTimeout(
              framer.getManagedCollections(),
              timeoutMs,
              "getManagedCollections (poll)",
            );
            if (refreshed.find((item) => item.id === collection.collectionId)) {
              found = true;
              break;
            }
          } catch {
            // ignore, will retry
          }
          await sleep(pollDelayMs);
        }
        if (!found) {
          eventLogger("error", "framer_sync", "Collection not found after polling", { collectionName: collection.collectionName });
          throw new Error(`Managed collection not found after creation and polling: ${collection.collectionName}`);
        }
      }

      // Set fields
      if (!isRowSync || collection.created) {
        // Resolve lookup fields to multiCollectionReference by matching the Coda ref row IDs
        // against items already present in other Framer managed collections.
        // No extra config needed — the Coda ref objects carry the row IDs (e.g. i-BTOhjxRAt7)
        // which are the same as the Framer item IDs since we use Coda row IDs as Framer item IDs.
        const allFramerCollections = await withTimeout(
          framer.getManagedCollections(),
          timeoutMs,
          "getManagedCollections (for reference resolution)",
        );
        // Skip the current collection itself
        const otherCollections = allFramerCollections.filter(
          (c) => c.id !== collection.collectionId,
        );

        eventLogger("info", "framer_sync", "Available managed collections for reference resolution", {
          allCollections: allFramerCollections.map((c) => ({ id: c.id, name: c.name })),
        });

        // Build codaRefTableId → framerCollectionId by finding which other Framer collection
        // contains the item IDs referenced by each lookup field.
        const codaTableIdToFramerCollectionId = new Map();

        // If linkedCollectionName is provided, pre-seed all lookup fields with that collection.
        // This is the escape hatch for formula/computed lookup columns whose row values
        // can't be matched by the ID-scan loop (e.g. Sizes → Products via a formula column).
        if (payload.linkedCollectionName) {
          const linkedName = String(payload.linkedCollectionName).trim().toLowerCase();
          const linkedCol = otherCollections.find((c) => String(c.name || "").toLowerCase() === linkedName);
          if (linkedCol) {
            for (const lf of mappingResult.allMappedFields.filter((f) => f.type === "lookup")) {
              codaTableIdToFramerCollectionId.set(lf.codaRefTableId, linkedCol.id);
            }
            eventLogger("info", "framer_sync", "Pre-seeded reference fields from linkedCollectionName", {
              linkedCollectionName: payload.linkedCollectionName,
              framerCollectionId: linkedCol.id,
            });
          } else {
            eventLogger("warning", "framer_sync", "linkedCollectionName not found in Framer collections", {
              linkedCollectionName: payload.linkedCollectionName,
            });
          }
        }

        const lookupFields = mappingResult.allMappedFields.filter((f) => f.type === "lookup");

        for (const lookupField of lookupFields) {
          if (codaTableIdToFramerCollectionId.has(lookupField.codaRefTableId)) continue;

          // Collect a sample of ref IDs from the items for this field
          const sampleIds = new Set();
          for (const item of mappingResult.items) {
            const val = item.fieldData?.[lookupField.id]?.value;
            if (Array.isArray(val)) val.forEach((id) => sampleIds.add(id));
            else if (typeof val === "string") sampleIds.add(val);
            if (sampleIds.size >= 3) break;
          }
          if (sampleIds.size === 0) continue;

          eventLogger("info", "framer_sync", "Resolving reference field", {
            field: lookupField.name,
            codaRefTableId: lookupField.codaRefTableId,
            sampleIds: [...sampleIds],
            otherCollections: otherCollections.map((c) => ({ id: c.id, name: c.name })),
          });

          // Find which other collection contains at least one of those IDs
          for (const otherCol of otherCollections) {
            if (typeof otherCol.getItemIds !== "function") continue;
            try {
              const ids = await withTimeout(
                otherCol.getItemIds(),
                timeoutMs,
                `getItemIds (ref resolution: ${otherCol.name})`,
              );
              const idSet = new Set(Array.isArray(ids) ? ids.map(String) : []);
              eventLogger("info", "framer_sync", "Scanned collection for ref IDs", {
                field: lookupField.name,
                collectionName: otherCol.name,
                collectionId: otherCol.id,
                idsInCollection: [...idSet].slice(0, 5),
                matched: [...sampleIds].some((id) => idSet.has(id)),
              });
              if ([...sampleIds].some((id) => idSet.has(id))) {
                codaTableIdToFramerCollectionId.set(lookupField.codaRefTableId, otherCol.id);
                eventLogger("info", "framer_sync", "Resolved reference field to collection", {
                  field: lookupField.name,
                  codaRefTableId: lookupField.codaRefTableId,
                  framerCollectionId: otherCol.id,
                  framerCollectionName: otherCol.name,
                });
                break;
              }
            } catch (err) {
              eventLogger("warning", "framer_sync", "getItemIds failed during ref resolution", {
                field: lookupField.name,
                collectionName: otherCol.name,
                collectionId: otherCol.id,
                error: err instanceof Error ? err.message : String(err),
              });
            }
          }
        }

        const resolvedRefFields = resolveReferenceFields(
          mappingResult.allMappedFields,
          codaTableIdToFramerCollectionId,
        );
        eventLogger("info", "framer_sync", "Reference fields resolved", {
          resolvedRefFields: resolvedRefFields.map((f) => ({ id: f.id, name: f.name, type: f.type, collectionId: f.collectionId })),
        });
        // Track which lookup field IDs were successfully resolved so addItems can skip the rest
        const resolvedRefFieldIds = new Set(resolvedRefFields.map((f) => f.id));
        const compatibleFields = [
          ...mappingResult.fields.filter(Boolean),
          ...resolvedRefFields,
        ];
        await withTimeout(
          collectionHandle.setFields(compatibleFields),
          timeoutMs,
          "setCollectionFields",
        );
        fieldsSet = compatibleFields.length;

        // Store resolved ref field IDs on mappingResult so addItems remapping can use it
        mappingResult._resolvedRefFieldIds = resolvedRefFieldIds;
        const unresolvedLookups = mappingResult.allMappedFields
          .filter((f) => f.type === "lookup" && !resolvedRefFieldIds.has(f.id))
          .map((f) => f.name);
        if (unresolvedLookups.length > 0) {
          eventLogger("warning", "framer_sync", "Some lookup fields could not be resolved to a Framer collection and will be skipped", {
            unresolvedLookups,
          });
        }
      }

      if (mappingResult.items.length > 0) {
        // Fetch existing item IDs before add
        let existingItemIds = [];
        try {
          if (typeof collectionHandle.getItemIds === "function") {
            const ids = await withTimeout(
              collectionHandle.getItemIds(),
              timeoutMs,
              "getManagedCollectionItemIds (before add)",
            );
            existingItemIds = Array.isArray(ids) ? ids.map((id) => String(id)) : [];
          }
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          eventLogger("warning", "framer_sync", "Could not fetch existing item ids before add", { message });
        }

        // Fetch collection fields for remapping (use object fields first, fall back to getFields)
        // Include allMappedFields so lookup/reference fields get remapped too
        const codaFieldIdToName = new Map(
          (mappingResult.allMappedFields || mappingResult.fields).map((field) => [field.id, field.name]),
        );

        async function fetchFields() {
          let normalized = normalizeCollectionFieldsLocal(collectionHandle?.fields);
          if (normalized.length > 0) return normalized;
          if (typeof collectionHandle.getFields === "function") {
            const fetched = await withTimeout(collectionHandle.getFields(), timeoutMs, "getCollectionFields");
            normalized = normalizeCollectionFieldsLocal(fetched);
          }
          return normalized;
        }

        let collectionFields = await fetchFields();
        let collectionFieldNameToId = new Map(
          collectionFields.map((field) => [String(field.name).toLowerCase(), field.id]),
        );

        // Build a set of field IDs that are unresolved lookups (type still "lookup" / reference
        // type but no Framer collectionId found). These must be excluded from addItems or Framer
        // throws "Expected a collection node".
        const unresolvedLookupFieldIds = new Set(
          (mappingResult.allMappedFields || [])
            .filter((f) => f.type === "lookup" && !(mappingResult._resolvedRefFieldIds?.has(f.id)))
            .map((f) => f.id),
        );

        function remapItems(items) {
          return items.map((item) => {
            const remappedFieldData = {};
            for (const [sourceFieldId, fieldValue] of Object.entries(item?.fieldData || {})) {
              if (unresolvedLookupFieldIds.has(sourceFieldId)) continue;
              const sourceFieldName = codaFieldIdToName.get(sourceFieldId);
              if (!sourceFieldName) continue;
              const targetFieldId = collectionFieldNameToId.get(String(sourceFieldName).toLowerCase());
              if (!targetFieldId) continue;
              remappedFieldData[targetFieldId] = fieldValue;
            }
            return { ...item, fieldData: remappedFieldData };
          });
        }

        let itemsToAdd = remapItems(mappingResult.items);

        eventLogger("info", "framer_sync", "addItems field types snapshot", {
          unresolvedLookupFieldIds: [...unresolvedLookupFieldIds],
          sampleFieldTypes: itemsToAdd[0]
            ? Object.entries(itemsToAdd[0].fieldData || {}).map(([k, v]) => ({ k, type: v?.type }))
            : [],
        });

        // Add items with one retry on field-not-found after schema refresh
        let addItemsAttempted = false;
        let addItemsError = null;
        for (let attempt = 0; attempt < 2; attempt++) {
          try {
            await withTimeout(
              collectionHandle.addItems(itemsToAdd),
              timeoutMs,
              "addItemsToCollection (bulk)",
            );
            addItemsAttempted = true;
            break;
          } catch (err) {
            addItemsError = err;
            const msg = String(err?.message ?? err);
            if (attempt === 0 && collection.created && /field not found/i.test(msg)) {
              eventLogger("warning", "framer_sync", "Retrying addItems after collection creation and schema refresh", { msg });
              await sleep(1500);
              collectionFields = await fetchFields();
              collectionFieldNameToId = new Map(
                collectionFields.map((field) => [String(field.name).toLowerCase(), field.id]),
              );
              itemsToAdd = remapItems(mappingResult.items);
              continue;
            } else {
              throw err;
            }
          }
        }
        if (!addItemsAttempted && addItemsError) throw addItemsError;

        // rowSync path: filter to known field IDs and add again
        if (isRowSync && !collection.created) {
          const allowedFieldIds = new Set(collectionFields.map((f) => f.id));
          itemsToAdd = mappingResult.items.map((item) => {
            const filteredFieldData = {};
            for (const [fieldId, fieldValue] of Object.entries(item?.fieldData || {})) {
              if (allowedFieldIds.has(fieldId)) filteredFieldData[fieldId] = fieldValue;
            }
            return { ...item, fieldData: filteredFieldData };
          });
          await withTimeout(
            collectionHandle.addItems(itemsToAdd),
            timeoutMs,
            "addItemsToCollection (rowSync)",
          );
        }

        // Verify items landed
        try {
          if (typeof collectionHandle.getItemIds === "function") {
            const afterItemIds = await withTimeout(
              collectionHandle.getItemIds(),
              timeoutMs,
              "getManagedCollectionItemIds (after add)",
            );
            const afterIds = Array.isArray(afterItemIds) ? afterItemIds.map((id) => String(id)) : [];
            const beforeSet = new Set(existingItemIds);
            const submittedIds = new Set(itemsToAdd.map((item) => String(item.id)));
            const expectedNewIds = Array.from(submittedIds).filter((id) => !beforeSet.has(id));
            const afterSet = new Set(afterIds);
            const missingNewIds = expectedNewIds.filter((id) => !afterSet.has(id));
            if (missingNewIds.length > 0) {
              const warning = `Submitted ${itemsToAdd.length} items, but ${missingNewIds.length} expected new id(s) were not present after add: ${missingNewIds.join(", ")}`;
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
        const codaItemIds = new Set(mappingResult.items.map((item) => String(item.id)));
        let managedItemIds = [];
        if (typeof collectionHandle.getItemIds === "function") {
          const ids = await withTimeout(collectionHandle.getItemIds(), timeoutMs, "getManagedCollectionItemIds (deleteMissing)");
          managedItemIds = Array.isArray(ids) ? ids.map((id) => String(id)) : [];
        }
        const toRemove = managedItemIds.filter((id) => !codaItemIds.has(id));
        if (toRemove.length > 0) {
          await withTimeout(
            collectionHandle.removeItems(toRemove),
            timeoutMs,
            "removeManagedCollectionItems",
          );
          itemsRemoved = toRemove.length;
        }
      }

      // Publish within the same session
      if (payload.publish) {
        eventLogger("info", "publishing", "Publishing project");
        const publishTimeoutMs = getPublishTimeoutMs();
        try {
          let rawResult;
          try {
            rawResult = await withTimeout(framer.publish(), publishTimeoutMs, "publishProject");
          } catch (error) {
            const formatted = formatFramerError("publish", error);
            log("error", "publish_failed", { projectUrl: payload.framerProjectUrl, ...formatted.fields });
            throw new Error(formatted.message);
          }

          const deploymentId = rawResult?.deployment?.id || "";
          log("info", "publish_result", {
            projectUrl: payload.framerProjectUrl,
            deploymentId,
            hostnamesCount: Array.isArray(rawResult?.hostnames) ? rawResult.hostnames.length : 0,
          });

          if (!deploymentId) {
            throw new Error("publish failed | missing deployment id in publish result");
          }

          try {
            await withTimeout(framer.deploy(deploymentId), publishTimeoutMs, "deployProject");
          } catch (error) {
            const formatted = formatFramerError("deploy", error);
            log("error", "deploy_failed", { projectUrl: payload.framerProjectUrl, deploymentId, ...formatted.fields });
            throw new Error(formatted.message);
          }

          log("info", "deploy_complete", { projectUrl: payload.framerProjectUrl, deploymentId });
          publishResult = { published: true, changeCount: 1, deploymentId, message: "Successfully published and deployed" };
        } catch (error) {
          publishError = error instanceof Error ? error.message : String(error);
          eventLogger("warning", "publishing", "Publish failed after successful sync", { error: publishError });
        }
      }
    },
  );

  const publishSucceeded = Boolean(publishResult?.published);
  const overallSuccess = !publishRequested || publishSucceeded;
  const itemSummary = `${mappingResult.items.length} item${mappingResult.items.length === 1 ? "" : "s"}${itemsRemoved > 0 ? `, removed ${itemsRemoved}` : ""}`;
  const baseSyncMessage = `Synced ${itemSummary} to "${collection.collectionName}"`;

  const responseBody = {
    success: overallSuccess,
    syncSuccess: true,
    publishRequested,
    collectionId: collection.collectionId,
    collectionName: collection.collectionName,
    itemsAdded: mappingResult.items.length,
    itemsRemoved,
    fieldsSet,
    warnings: mappingResult.warnings,
    published: publishSucceeded,
    publishError,
    deploymentId: publishResult?.deploymentId ?? "",
    message: publishSucceeded
      ? `✅ ${baseSyncMessage} and published (deployment: ${publishResult.deploymentId}).`
      : publishRequested
        ? `⚠️ ${baseSyncMessage}, but publish failed: ${publishError}`
        : `✅ ${baseSyncMessage}.`,
  };

  return responseBody;
}

function buildCallbackLogValues(job, message) {
  const events = Array.isArray(job?.events) ? job.events : [];
  const latestEvent = events.length > 0 ? events[events.length - 1] : null;
  return {
    "Job id": job?.jobId || "",
    "Created at": job?.createdAt || "",
    Error: job?.error || "",
    "Items added": Number(job?.result?.itemsAdded || 0),
    Status: job?.status || "",
    Message: String(message || job?.result?.message || ""),
    Action: job?.payload?.action || "sync",
    "Collection name": String(job?.payload?.collectionName || ""),
    "Completed at": job?.completedAt || "",
    "Deployment id": String(job?.result?.deploymentId || ""),
    Id: job?.jobId || "",
    "Items removed": Number(job?.result?.itemsRemoved || 0),
    "Latest event": String(latestEvent?.message || ""),
    "Latest stage": String(latestEvent?.stage || ""),
    "Publish requested": Boolean(job?.payload?.publish),
    Published: Boolean(job?.result?.published),
    "Request id": job?.requestId || "",
    "Source table": String(job?.payload?.tableIdOrName || ""),
    "Started at": job?.startedAt || "",
    "Updated at": job?.updatedAt || "",
    Success: Boolean(job?.result?.success),
  };
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

async function writeStatusCallback(payload, message, eventLogger, jobSnapshot = null, context = {}) {
  const callback = payload.callback || {};
  const hasExplicitCallbackTable = Object.prototype.hasOwnProperty.call(callback, "statusTableIdOrName")
    || Object.prototype.hasOwnProperty.call(callback, "statusTableInput");
  const defaultCallbackTableName = getDefaultCallbackTableName();
  const rawStatusColumnNameOrId = callback.statusColumn || callback.statusColumnId || callback.statusColumnNameOrId || "Status";
  const rawStatusRowSelector = callback.statusRow || callback.statusRowId || callback.statusRowSelector || payload.rowId || "";
  const rawStatusTableIdOrName = callback.statusTableIdOrName
    || callback.statusTableInput
    || (hasExplicitCallbackTable ? defaultCallbackTableName : (payload.tableIdOrName || ""));
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

  // Require doc/table/column — a row selector is optional. If none is
  // provided (or cannot be resolved), create the final log row now.
  if (!statusColumnNameOrId || !statusTableIdOrName || !statusDocId) {
    eventLogger("warning", "callback", "Skipped Coda status callback: missing callback target", {
      hasStatusDocId: Boolean(statusDocId),
      hasStatusTableIdOrName: Boolean(statusTableIdOrName),
      hasStatusRowSelector: Boolean(statusRowSelector),
      hasStatusColumnNameOrId: Boolean(statusColumnNameOrId),
    });
    return;
  }

  const codaApiToken = process.env.CODA_API_TOKEN;
  if (!codaApiToken) {
    eventLogger("warning", "callback", "Skipped Coda status callback: missing CODA_API_TOKEN");
    return;
  }

  try {
    const resolvedStatusTableId = await resolveTableNameOrId(
      statusDocId,
      statusTableIdOrName,
      codaApiToken,
    );

    const resolvedStatusColumnId = await resolveColumnNameOrId(
      statusDocId,
      resolvedStatusTableId,
      statusColumnNameOrId,
      codaApiToken,
    );

    // Precompute table columns and the full set of log cells so we can create
    // a single, complete log row if needed (avoids partial/duplicate rows).
    const tableColumns = await getCodaTableColumns(
      statusDocId,
      resolvedStatusTableId,
      codaApiToken,
    );

    const byLowerName = new Map(
      tableColumns.map((column) => [String(column?.name || "").toLowerCase(), String(column?.id || "")]),
    );

    const messageColumnInput = callback.messageColumnId || callback.messageColumn || "";
    let resolvedMessageColumnId = "";
    if (messageColumnInput) {
      try {
        resolvedMessageColumnId = await resolveColumnNameOrId(
          statusDocId,
          resolvedStatusTableId,
          messageColumnInput,
          codaApiToken,
        );
      } catch (_) {
        resolvedMessageColumnId = byLowerName.get("message") || "";
      }
    } else {
      resolvedMessageColumnId = byLowerName.get("message") || "";
    }

    const jobData = jobSnapshot || {};
    const logValues = buildCallbackLogValues(jobData, message);
    const statusValue = String(jobData?.status || "").trim() || message;
    const hasDedicatedMessageColumn = Boolean(resolvedMessageColumnId && resolvedMessageColumnId !== resolvedStatusColumnId);

    const cellsToUpdate = [];
    cellsToUpdate.push({
      column: resolvedStatusColumnId,
      value: hasDedicatedMessageColumn ? statusValue : message,
    });
    if (hasDedicatedMessageColumn) {
      cellsToUpdate.push({ column: resolvedMessageColumnId, value: message });
    }
    for (const [columnName, value] of Object.entries(logValues)) {
      const columnId = byLowerName.get(columnName.toLowerCase()) || "";
      if (!columnId) continue;
      if (columnId === resolvedStatusColumnId || columnId === resolvedMessageColumnId) continue;
      cellsToUpdate.push({ column: columnId, value });
    }

    // Try to resolve an existing row if a selector was provided.
    let resolvedStatusRowId = "";
    if (statusRowSelector) {
      if (isApiRowId(statusRowSelector)) {
        resolvedStatusRowId = statusRowSelector;
      } else {
        let callbackSlugFieldId = callback.statusSlugField || callback.statusSlugFieldId || payload.slugFieldId || "";
        if (callbackSlugFieldId) {
          callbackSlugFieldId = await resolveColumnNameOrId(
            statusDocId,
            resolvedStatusTableId,
            callbackSlugFieldId,
            codaApiToken,
          );
        }

        const rowSearchLimit = parseIntEnv(
          callback.statusRowSearchLimit ?? 500,
          500,
          1,
          500,
        );

        const callbackTableData = await getCodaTableData(
          statusDocId,
          resolvedStatusTableId,
          codaApiToken,
          rowSearchLimit,
        );
        const matchedRow = findRowBySelector(
          callbackTableData,
          statusRowSelector,
          callbackSlugFieldId,
        );
        if (matchedRow?.id) {
          resolvedStatusRowId = matchedRow.id;
        }
      }
    }


    // If no row was resolved, create a single final log row with ALL
    // the collected cells (one-and-done). This avoids placeholder rows and
    // eliminates the need to recreate or patch later.
    let rowWasCreatedHere = false;
    if (!resolvedStatusRowId) {
      const created = await createTableRow(
        statusDocId,
        resolvedStatusTableId,
        cellsToUpdate,
        codaApiToken,
      );
      resolvedStatusRowId = created.rowId || "";
      if (resolvedStatusRowId) {
        rowWasCreatedHere = true;
        eventLogger("info", "callback", "callback_row_autocreated", {
          statusDocId,
          statusTableIdOrName: resolvedStatusTableId,
          statusColumnNameOrId,
          rowSelector: resolvedStatusRowId,
          usedRowId: true,
        });
        // allow brief eventual-consistency window before further ops
        await sleep(300);
      }
    }

    // If we just created the full row above, we can stop here. the
    // creation payload already contained every cell we care about, and
    // issuing additional updates immediately afterwards is what was blowing
    // Coda’s rate limit when a row is autocreated during a sync.
    if (rowWasCreatedHere) {
      eventLogger("info", "callback", "Callback row just created; skipping follow-on updates");
      return;
    }

    // Otherwise, attempt a single-cell update for speed, and if that
    // succeeds we’ll still run through the more robust row-update loop
    // below to set everything. we still handle missing-row gracefully.
    try {
      await updateTableCell(
        statusDocId,
        resolvedStatusTableId,
        resolvedStatusRowId,
        resolvedStatusColumnId,
        message,
        codaApiToken,
      );
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      const isNotFound = (err && err.status === 404) || /404\s+Not\s+Found/i.test(errMsg) || /row not found/i.test(errMsg.toLowerCase());
      if (isNotFound) {
        // Row missing — log and continue; the robust row-update below will
        // either populate or recreate the final log row as necessary.
        eventLogger("info", "callback", "Callback row missing before single-cell update", {
          statusDocId,
          statusTableIdOrName: resolvedStatusTableId,
          statusRowSelector,
          resolvedStatusRowId,
          statusColumnNameOrId,
          resolvedStatusColumnId,
          error: errMsg,
        });
      } else {
        throw err;
      }
    }

    // Attempt to update the row cells. Retry transient 404s that
    // can occur immediately after row creation (eventual consistency).
    const maxRowUpdateAttempts = parseIntEnv(process.env.CODA_CALLBACK_UPDATE_RETRY_ATTEMPTS || 5, 5, 1, 10);
    let lastRowUpdateError = null;
    for (let attempt = 1; attempt <= maxRowUpdateAttempts; attempt += 1) {
      try {
        await updateTableRowCells(
          statusDocId,
          resolvedStatusTableId,
          resolvedStatusRowId,
          cellsToUpdate,
          codaApiToken,
        );
        lastRowUpdateError = null;
        break;
      } catch (rowErr) {
        lastRowUpdateError = rowErr;
        const msg = rowErr instanceof Error ? rowErr.message : String(rowErr);
        const isNotFound = (rowErr && rowErr.status === 404) || /404\s+Not\s+Found/i.test(msg) || /row not found/i.test(msg.toLowerCase());
        const retryable = isNotFound || isRetryableCodaError(rowErr);

        eventLogger("warn", "callback", "Transient row-update failure", {
          statusDocId,
          resolvedStatusRowId,
          attempt,
          error: msg,
          retryable,
        });

        if (!retryable) {
          // this error is not one we think will resolve if retried
          break;
        }

        // back off before trying again
        await sleep(150 * attempt);

        // If this was the last attempt, attempt special recovery for 404
        if (attempt === maxRowUpdateAttempts && isNotFound) {
          if (!rowWasCreatedHere) {
            try {
              const created = await createTableRow(
                statusDocId,
                resolvedStatusTableId,
                // supply the same cells we intended to update
                cellsToUpdate.map((c) => ({ column: c.column, value: c.value })),
                codaApiToken,
              );
              const recreatedRowId = created.rowId || "";
              if (recreatedRowId) {
                resolvedStatusRowId = recreatedRowId;
                eventLogger("info", "callback", "Recreated callback row with full cells", { recreatedRowId });
                // Try updating again once more after recreate
                await sleep(200);
                await updateTableRowCells(
                  statusDocId,
                  resolvedStatusTableId,
                  resolvedStatusRowId,
                  cellsToUpdate,
                  codaApiToken,
                );

                eventLogger("info", "callback", "Updated Coda status cell after recreate", {
                  statusDocId,
                  statusTableIdOrName: resolvedStatusTableId,
                  statusRowSelector,
                  resolvedStatusRowId,
                  statusColumnNameOrId,
                  resolvedStatusColumnId,
                });
                lastRowUpdateError = null;
                break;
              }
            } catch (createErr) {
              // fall through to final failure
              lastRowUpdateError = createErr;
            }
          } else {
            eventLogger("warn", "callback", "Skipping recreate of callback row because rowWasCreatedHere is true");
          }
        }
      }
    }

    if (lastRowUpdateError) {
      // if the final error is retryable (e.g. rate limit) we already logged a
      // transient failure above. the caller will swallow the exception, but we
      // surface the fact so that the job event contains the detail.
      throw lastRowUpdateError instanceof Error ? lastRowUpdateError : new Error(String(lastRowUpdateError));
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
    await writeStatusCallback(job.payload, result.message, eventLogger, completedJob, context);
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
    await writeStatusCallback(job.payload, failure.message, eventLogger, failedJob, context);
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
          message: `Request already accepted (jobId: ${existing.jobId}).`,
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

    return sendJson(res, 202, {
      accepted: true,
      jobId,
      requestId,
      status: "queued",
      message: `Request accepted. Processing started (jobId: ${jobId}).`,
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
