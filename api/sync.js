import { getCodaTableData, resolveColumnNameOrId, updateTableCell } from "../lib/coda-client.js";
import {
  normalizeColumns,
  normalizeRows,
  buildFieldsAndItems,
} from "../lib/mapping.js";
import {
  getOrCreateManagedCollection,
  setCollectionFields,
  addItemsToCollection,
  publishProject,
} from "../lib/framer-client.js";

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(body));
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
      collectionName: payload.collectionName,
      publish: payload.publish,
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

    const tableData = await getCodaTableData(
      payload.docId,
      payload.tableIdOrName,
      codaApiToken,
      payload.rowLimit || 100,
    );

    // Resolve responseColumnId if provided
    let responseColumnId = null;
    if (payload.responseColumnId) {
      responseColumnId = await resolveColumnNameOrId(
        payload.docId,
        payload.tableIdOrName,
        payload.responseColumnId,
        codaApiToken,
      );
      console.log(`[sync] Resolved response column: "${payload.responseColumnId}" -> ${responseColumnId}`);
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

    console.log(`[sync] Coda data:`, {
      columnsCount: tableData.columns.length,
      rowsCount: tableData.rows.length,
      slugFieldId: resolvedSlugFieldId,
      responseColumnId,
      columnIds: tableData.columns.map(c => c.id),
      columnNames: tableData.columns.map(c => c.name),
      firstRowValues: tableData.rows[0]?.values,
    });

    const columns = normalizeColumns(tableData.columns);
    const rows = normalizeRows(tableData.rows);

    console.log(`[sync] Fetched ${rows.length} rows, ${columns.length} columns from Coda`);

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

    const collection = await getOrCreateManagedCollection(
      payload.framerProjectUrl,
      framerApiKey,
      payload.collectionName,
    );

    console.log(`[sync] Collection: ${collection.collectionId} (${collection.collectionName})${collection.created ? " [NEW]" : ""}`);

    const fieldsSet = await setCollectionFields(
      payload.framerProjectUrl,
      framerApiKey,
      collection.collectionId,
      mappingResult.fields,
    );

    console.log(`[sync] Set ${fieldsSet} fields`);

    if (mappingResult.items.length > 0) {
      console.log(`[sync] Adding ${mappingResult.items.length} items to collection...`);
      await addItemsToCollection(
        payload.framerProjectUrl,
        framerApiKey,
        collection.collectionId,
        mappingResult.items,
      );
      console.log(`[sync] Items added successfully`);
    } else {
      console.log(`[sync] No items to add`);
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
      fieldsSet,
      warnings: mappingResult.warnings,
      published: publishResult?.published ?? false,
      deploymentId: publishResult?.deploymentId ?? "",
      responseColumnId,
      message: publishResult?.published 
        ? `✅ Synced ${mappingResult.items.length} items to "${collection.collectionName}" and published (deployment: ${publishResult.deploymentId}).`
        : `✅ Synced ${mappingResult.items.length} items to "${collection.collectionName}".`,
    };
    console.log(`[sync] Final response:`, responseBody);

    // Update the response column in the specified row if both rowId and responseColumnId are provided
    if (payload.rowId && responseColumnId) {
      try {
        console.log(`[sync] Updating row ${payload.rowId}, column ${responseColumnId} with response...`);
        await updateTableCell(
          payload.docId,
          payload.tableIdOrName,
          payload.rowId,
          responseColumnId,
          JSON.stringify(responseBody),
          codaApiToken,
        );
        console.log(`[sync] Cell updated successfully`);
      } catch (updateError) {
        const updateErrorMsg = updateError instanceof Error ? updateError.message : String(updateError);
        console.error(`[sync] Failed to update response cell: ${updateErrorMsg}`);
        // Don't fail the sync if cell update fails - just log the error
        responseBody.warnings = responseBody.warnings || [];
        responseBody.warnings.push(`Warning: Could not write response to column: ${updateErrorMsg}`);
      }
    }

    return sendJson(res, 200, responseBody);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[sync] Error:`, message, error);
    return sendJson(res, 500, {
      error: "SYNC_FAILED",
      message,
    });
  }
}
