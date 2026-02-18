import {
  getCodaTableData,
  getCodaRowData,
  resolveColumnNameOrId,
} from "../lib/coda-client.js";
import {
  normalizeColumns,
  normalizeRows,
  buildFieldsAndItems,
} from "../lib/mapping.js";
import {
  getOrCreateManagedCollection,
  getCollectionFields,
  getCollectionFieldIds,
  getManagedCollectionItemIds,
  removeItemsFromManagedCollection,
  setCollectionFields,
  addItemsToCollection,
  publishProject,
} from "../lib/framer-client.js";
import {
  sleep,
  parseIntEnv,
  shouldRetryForTransientCodaWarnings,
} from "../lib/retry-policy.js";

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(body));
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

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return sendJson(res, 405, {
      error: "METHOD_NOT_ALLOWED",
      message: "Use POST /api/sync",
    });
  }

  try {
    const payload = await readJsonBody(req);
    console.log(`[sync] Request payload:`, { 
      docId: payload.docId, 
      tableIdOrName: payload.tableIdOrName,
      rowId: payload.rowId,
      collectionName: payload.collectionName,
      slugFieldId: payload.slugFieldId,
      rowLimit: payload.rowLimit,
      publish: payload.publish,
      deleteMissing: payload.deleteMissing,
      action: payload.action,
    });

    // Handle publish action
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

      console.log(`[publish] Publishing project: ${payload.framerProjectUrl}`);
      const publishResult = await publishProject(
        payload.framerProjectUrl,
        framerApiKey,
      );
      console.log(`[publish] Result:`, publishResult);

      return sendJson(res, 200, publishResult);
    }

    // Handle sync action (default)
    const required = [
      "docId",
      "tableIdOrName",
      "framerProjectUrl",
      "collectionName",
      "slugFieldId",
    ];

    for (const field of required) {
      if (!payload[field]) {
        return sendJson(res, 400, {
          error: "INVALID_REQUEST",
          message: `Missing required field: ${field}`,
        });
      }
    }

    const codaApiToken = process.env.CODA_API_TOKEN;
    if (!codaApiToken) {
      return sendJson(res, 500, {
        error: "SERVER_ERROR",
        message: "CODA_API_TOKEN is not configured",
      });
    }

    const framerApiKey = process.env.FRAMER_API_KEY || payload.framerApiKey;
    if (!framerApiKey) {
      return sendJson(res, 400, {
        error: "INVALID_REQUEST",
        message: "Missing Framer API key",
      });
    }

    const isRowSync = payload.action === "rowSync" || Boolean(payload.rowId);
    if (isRowSync && !payload.rowId) {
      return sendJson(res, 400, {
        error: "INVALID_REQUEST",
        message: "Missing required field for rowSync: rowId",
      });
    }

    // Resolve slugFieldId (accepts both column name and ID)
    let resolvedSlugFieldId = payload.slugFieldId;
    if (payload.slugFieldId) {
      resolvedSlugFieldId = await resolveColumnNameOrId(
        payload.docId,
        payload.tableIdOrName,
        payload.slugFieldId,
        codaApiToken,
      );
      if (resolvedSlugFieldId !== payload.slugFieldId) {
        console.log(`[sync] Resolved slug field: "${payload.slugFieldId}" -> ${resolvedSlugFieldId}`);
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
            );
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            console.warn(`[sync] Direct API row lookup failed (${message}). Falling back to selector lookup.`);
          }
        }

        if (!tableData) {
          const selectorData = await getCodaTableData(
            payload.docId,
            payload.tableIdOrName,
            codaApiToken,
            payload.rowLimit || 500,
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
        );
      }

      console.log(`[sync] Coda data:`, {
        columnsCount: tableData.columns.length,
        rowsCount: tableData.rows.length,
        slugFieldId: resolvedSlugFieldId,
        columnIds: tableData.columns.map(c => c.id),
        firstRowValues: tableData.rows[0]?.values,
      });

      const columns = normalizeColumns(tableData.columns);
      const rows = normalizeRows(tableData.rows);

      console.log(`[sync] Fetched ${rows.length} rows, ${columns.length} columns from Coda (${isRowSync ? "rowSync" : "tableSync"})`);

      const mappingResult = buildFieldsAndItems({
        columns,
        rows,
        slugFieldId: resolvedSlugFieldId,
        use12HourTime: payload.use12HourTime !== false, // Default to true (12-hour format)
      });

      console.log(`[sync] Mapping result: ${mappingResult.items.length} items, ${mappingResult.skippedCount} skipped, ${mappingResult.warnings.length} warnings`);
      if (mappingResult.warnings.length > 0) {
        console.log(`[sync] Warnings:`, mappingResult.warnings);
      }

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

    let mappingResult;
    for (let attempt = 1; attempt <= maxCodaStateRetries; attempt += 1) {
      mappingResult = await getCodaSnapshot();

      const retryableWarning = shouldRetryForTransientCodaWarnings(mappingResult.warnings);
      if (!retryableWarning || attempt === maxCodaStateRetries) {
        break;
      }

      const delayMs = codaStateRetryDelayMs * attempt;
      console.warn(
        `[sync] Transient Coda warning detected (attempt ${attempt}/${maxCodaStateRetries}): retrying Coda snapshot in ${delayMs}ms`,
      );
      await sleep(delayMs);
    }

    const collection = await getOrCreateManagedCollection(
      payload.framerProjectUrl,
      framerApiKey,
      payload.collectionName,
    );

    console.log(`[sync] Collection: ${collection.collectionId} (${collection.collectionName})${collection.created ? " [NEW]" : ""}`);

    let fieldsSet = 0;
    if (!isRowSync || collection.created) {
      fieldsSet = await setCollectionFields(
        payload.framerProjectUrl,
        framerApiKey,
        collection.collectionId,
        mappingResult.fields,
      );
      console.log(`[sync] Set ${fieldsSet} fields`);
    } else {
      console.log(`[sync] Skipping field sync in rowSync for existing collection`);
    }

    if (mappingResult.items.length > 0) {
      let itemsToAdd = mappingResult.items;
      let existingItemIds = [];

      try {
        existingItemIds = await getManagedCollectionItemIds(
          payload.framerProjectUrl,
          framerApiKey,
          collection.collectionId,
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(`[sync] Could not fetch existing item ids before add: ${message}`);
      }

      const codaFieldIdToName = new Map(
        mappingResult.fields.map((field) => [field.id, field.name]),
      );
      const collectionFields = await getCollectionFields(
        payload.framerProjectUrl,
        framerApiKey,
        collection.collectionId,
      );
      const collectionFieldNameToId = new Map(
        collectionFields.map((field) => [String(field.name).toLowerCase(), field.id]),
      );

      itemsToAdd = mappingResult.items.map((item) => {
        const remappedFieldData = {};
        const originalFieldData = item?.fieldData || {};

        for (const [sourceFieldId, fieldValue] of Object.entries(originalFieldData)) {
          const sourceFieldName = codaFieldIdToName.get(sourceFieldId);
          if (!sourceFieldName) continue;
          const targetFieldId = collectionFieldNameToId.get(String(sourceFieldName).toLowerCase());
          if (!targetFieldId) continue;
          remappedFieldData[targetFieldId] = fieldValue;
        }

        return {
          ...item,
          fieldData: remappedFieldData,
        };
      });

      const originalCount = Object.keys(mappingResult.items[0]?.fieldData || {}).length;
      const remappedCount = Object.keys(itemsToAdd[0]?.fieldData || {}).length;
      if (remappedCount < originalCount) {
        console.log(
          `[sync] Remapped item fields by name: removed ${originalCount - remappedCount} unmapped key(s)`,
        );
      }

      if (isRowSync && !collection.created) {
        const allowedFieldIds = new Set(
          await getCollectionFieldIds(
            payload.framerProjectUrl,
            framerApiKey,
            collection.collectionId,
          ),
        );

        itemsToAdd = mappingResult.items.map((item) => {
          const filteredFieldData = {};
          const originalFieldData = item?.fieldData || {};
          for (const [fieldId, fieldValue] of Object.entries(originalFieldData)) {
            if (allowedFieldIds.has(fieldId)) {
              filteredFieldData[fieldId] = fieldValue;
            }
          }
          return {
            ...item,
            fieldData: filteredFieldData,
          };
        });

        const filteredCount = Object.keys(itemsToAdd[0]?.fieldData || {}).length;
        if (filteredCount < originalCount) {
          console.log(
            `[sync] RowSync pre-filtered unknown fields: removed ${originalCount - filteredCount} key(s) before addItems`,
          );
        }
      }

      console.log(`[sync] Adding ${mappingResult.items.length} items to collection...`);
      await addItemsToCollection(
        payload.framerProjectUrl,
        framerApiKey,
        collection.collectionId,
        itemsToAdd,
      );
      console.log(`[sync] Items added successfully`);

      try {
        const afterItemIds = await getManagedCollectionItemIds(
          payload.framerProjectUrl,
          framerApiKey,
          collection.collectionId,
        );
        const beforeSet = new Set(existingItemIds);
        const submittedIds = new Set(itemsToAdd.map((item) => String(item.id)));
        const expectedNewIds = Array.from(submittedIds).filter((id) => !beforeSet.has(id));
        const afterSet = new Set(afterItemIds);
        const missingNewIds = expectedNewIds.filter((id) => !afterSet.has(id));

        if (missingNewIds.length > 0) {
          const warning = `Submitted ${itemsToAdd.length} items, but ${missingNewIds.length} expected new id(s) were not present after add: ${missingNewIds.join(", ")}`;
          mappingResult.warnings.push(warning);
          console.warn(`[sync] ${warning}`);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(`[sync] Could not verify item ids after add: ${message}`);
      }
    } else {
      console.log(`[sync] No items to add`);
    }

    let itemsRemoved = 0;
    if (!isRowSync && payload.deleteMissing) {
      const codaItemIds = new Set(mappingResult.items.map((item) => String(item.id)));
      const managedItemIds = await getManagedCollectionItemIds(
        payload.framerProjectUrl,
        framerApiKey,
        collection.collectionId,
      );
      const toRemove = managedItemIds.filter((id) => !codaItemIds.has(String(id)));

      if (toRemove.length > 0) {
        console.log(`[sync] Removing ${toRemove.length} item(s) not present in Coda table snapshot...`);
        itemsRemoved = await removeItemsFromManagedCollection(
          payload.framerProjectUrl,
          framerApiKey,
          collection.collectionId,
          toRemove,
        );
        console.log(`[sync] Removed ${itemsRemoved} stale item(s)`);
      } else {
        console.log(`[sync] deleteMissing enabled: no stale items to remove`);
      }
    }

    let publishResult = null;
    if (payload.publish) {
      console.log(`[sync] Publishing project after sync...`);
      publishResult = await publishProject(
        payload.framerProjectUrl,
        framerApiKey,
      );
      console.log(`[sync] Publish result:`, publishResult);
    } else {
      console.log(`[sync] Publish parameter not set (payload.publish=${payload.publish})`);
    }

    const responseBody = {
      success: true,
      collectionId: collection.collectionId,
      collectionName: collection.collectionName,
      itemsAdded: mappingResult.items.length,
      itemsRemoved,
      fieldsSet,
      warnings: mappingResult.warnings,
      published: publishResult?.published ?? false,
      deploymentId: publishResult?.deploymentId ?? "",
      message: publishResult?.published 
        ? `✅ Synced ${mappingResult.items.length} item${mappingResult.items.length === 1 ? "" : "s"}${itemsRemoved > 0 ? `, removed ${itemsRemoved}` : ""} to "${collection.collectionName}" and published (deployment: ${publishResult.deploymentId}).`
        : `✅ Synced ${mappingResult.items.length} item${mappingResult.items.length === 1 ? "" : "s"}${itemsRemoved > 0 ? `, removed ${itemsRemoved}` : ""} to "${collection.collectionName}".`,
    };
    console.log(`[sync] Final response:`, responseBody);
    return sendJson(res, 200, responseBody);
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error(`[sync] Error:`, errorMsg, error);
    
    // Return error in response body so it shows in the Response column
    const errorResponse = {
      success: false,
      collectionId: "",
      collectionName: "",
      itemsAdded: 0,
      itemsRemoved: 0,
      fieldsSet: 0,
      warnings: [],
      published: false,
      deploymentId: "",
      message: `❌ Sync failed: ${errorMsg}`,
    };
    return sendJson(res, 200, errorResponse);
  }
}
