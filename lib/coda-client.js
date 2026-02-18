const CODA_API_BASE = "https://coda.io/apis/v1";

import {
  sleep,
  parseIntEnv,
  computeBackoffMs,
  isRetryableCodaError,
  isRetryableHttpStatus,
} from "./retry-policy.js";

function createCodaHttpError(response) {
  const error = new Error(`Coda API request failed: ${response.status} ${response.statusText}`);
  error.status = response.status;
  error.statusText = response.statusText;
  error.retryable = isRetryableHttpStatus(response.status);
  return error;
}

async function fetchCodaJson(url, apiToken) {
  const maxAttempts = parseIntEnv(process.env.CODA_API_RETRY_ATTEMPTS || 3, 3, 1, 6);
  const baseDelayMs = parseIntEnv(process.env.CODA_API_RETRY_DELAY_MS || 800, 800, 0, 10000);
  let lastError;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const response = await fetch(url, {
        headers: {
          Authorization: `Bearer ${apiToken}`,
        },
      });

      if (!response.ok) {
        throw createCodaHttpError(response);
      }

      return response.json();
    } catch (error) {
      lastError = error;
      const retryable = isRetryableCodaError(error);
      if (!retryable || attempt === maxAttempts) {
        break;
      }

      const waitMs = computeBackoffMs(baseDelayMs, attempt);
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`[coda] Transient API error (attempt ${attempt}/${maxAttempts}): ${message}. Retrying in ${waitMs}ms...`);
      await sleep(waitMs);
    }
  }

  throw lastError;
}

export async function getCodaTableColumns(docId, tableIdOrName, apiToken) {
  const columnsUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/columns`;
  const columnsData = await fetchCodaJson(columnsUrl, apiToken);
  return columnsData.items || [];
}

export async function resolveColumnNameOrId(docId, tableIdOrName, columnNameOrId, apiToken) {
  const columns = await getCodaTableColumns(docId, tableIdOrName, apiToken);

  console.log(`[resolveColumnNameOrId] Looking for "${columnNameOrId}" in ${columns.length} columns:`, 
    columns.map(c => ({ id: c.id, name: c.name })));

  // If columnNameOrId matches a column ID, return it
  const byId = columns.find(c => c.id === columnNameOrId);
  if (byId) {
    console.log(`[resolveColumnNameOrId] Found by ID: ${columnNameOrId}`);
    return byId.id;
  }

  // If columnNameOrId matches a column name, return its ID
  const byName = columns.find(c => c.name === columnNameOrId);
  if (byName) {
    console.log(`[resolveColumnNameOrId] Found by name: "${columnNameOrId}" -> ${byName.id}`);
    return byName.id;
  }

  throw new Error(
    `Column not found: "${columnNameOrId}". Available columns: ${columns.map(c => `"${c.name}"`).join(", ")}`,
  );
}

export async function getCodaTableData(docId, tableIdOrName, apiToken, limit = 100) {
  const columns = await getCodaTableColumns(docId, tableIdOrName, apiToken);

  const rowsUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/rows?useColumnNames=true&valueFormat=rich&limit=${Math.min(limit, 500)}`;
  const rowsData = await fetchCodaJson(rowsUrl, apiToken);

  return {
    columns,
    rows: rowsData.items || [],
  };
}

export async function getCodaRowData(docId, tableIdOrName, rowId, apiToken) {
  const columns = await getCodaTableColumns(docId, tableIdOrName, apiToken);
  const rowUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/rows/${encodeURIComponent(rowId)}?useColumnNames=true&valueFormat=rich`;

  const rowData = await fetchCodaJson(rowUrl, apiToken);
  const row = rowData && typeof rowData === "object" && "id" in rowData ? rowData : rowData?.item;

  if (!row || typeof row !== "object") {
    throw new Error(`Row not found: ${rowId}`);
  }

  return {
    columns,
    rows: [row],
  };
}

export async function updateTableCell(docId, tableIdOrName, rowId, columnId, value, apiToken) {
  const cellUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/rows/${rowId}/cells/${columnId}`;

  const response = await fetch(cellUrl, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${apiToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ value }),
  });

  if (!response.ok) {
    throw new Error(
      `Failed to update cell: ${response.status} ${response.statusText}`,
    );
  }

  return await response.json();
}
