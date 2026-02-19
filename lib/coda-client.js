const CODA_API_BASE = "https://coda.io/apis/v1";

import {
  sleep,
  parseIntEnv,
  computeBackoffMs,
  isRetryableCodaError,
  isRetryableHttpStatus,
} from "./retry-policy.js";

function log(level, event, fields = {}) {
  const record = {
    ts: new Date().toISOString(),
    source: "coda-client",
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
      log("warn", "transient_api_retry", {
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

export async function getCodaTableColumns(docId, tableIdOrName, apiToken) {
  const columnsUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/columns`;
  const columnsData = await fetchCodaJson(columnsUrl, apiToken);
  return columnsData.items || [];
}

export async function getCodaTables(docId, apiToken) {
  const tablesUrl = `${CODA_API_BASE}/docs/${docId}/tables`;
  const tablesData = await fetchCodaJson(tablesUrl, apiToken);
  return tablesData.items || [];
}

function normalizeTableCandidate(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  const bracketed = text.match(/^\[([^\]]+)\]$/);
  if (bracketed?.[1]) {
    return bracketed[1].trim();
  }
  return text;
}

export async function resolveTableNameOrId(docId, tableNameOrId, apiToken) {
  const candidate = normalizeTableCandidate(tableNameOrId);
  if (!candidate) {
    throw new Error("Table reference is empty");
  }

  if (/^grid-[A-Za-z0-9_-]+$/.test(candidate)) {
    return candidate;
  }

  const tables = await getCodaTables(docId, apiToken);
  const byId = tables.find((table) => String(table?.id || "") === candidate);
  if (byId?.id) {
    return byId.id;
  }

  const lowered = candidate.toLowerCase();
  const byName = tables.find((table) => String(table?.name || "").toLowerCase() === lowered);
  if (byName?.id) {
    return byName.id;
  }

  throw new Error(
    `Table not found: "${candidate}". Available tables: ${tables.map((table) => `"${table?.name || ""}"`).join(", ")}`,
  );
}

export async function resolveColumnNameOrId(docId, tableIdOrName, columnNameOrId, apiToken) {
  const columns = await getCodaTableColumns(docId, tableIdOrName, apiToken);

  log("info", "resolve_column_attempt", {
    docId,
    tableIdOrName,
    columnNameOrId,
    columnsCount: columns.length,
    columns: columns.map((column) => ({ id: column.id, name: column.name })),
  });

  // If columnNameOrId matches a column ID, return it
  const byId = columns.find(c => c.id === columnNameOrId);
  if (byId) {
    log("info", "resolve_column_by_id", {
      docId,
      tableIdOrName,
      columnNameOrId,
      resolvedColumnId: byId.id,
    });
    return byId.id;
  }

  // If columnNameOrId matches a column name, return its ID
  const byName = columns.find(c => c.name === columnNameOrId);
  if (byName) {
    log("info", "resolve_column_by_name", {
      docId,
      tableIdOrName,
      columnNameOrId,
      resolvedColumnId: byName.id,
    });
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
  const tableRef = encodeURIComponent(String(tableIdOrName));
  const rowRef = encodeURIComponent(String(rowId));
  const columnRef = encodeURIComponent(String(columnId));

  const cellUrl = `${CODA_API_BASE}/docs/${docId}/tables/${tableRef}/rows/${rowRef}/cells/${columnRef}`;
  const rowUrl = `${CODA_API_BASE}/docs/${docId}/tables/${tableRef}/rows/${rowRef}`;

  const maxAttempts = parseIntEnv(process.env.CODA_CALLBACK_UPDATE_RETRY_ATTEMPTS || 3, 3, 1, 6);
  const baseDelayMs = parseIntEnv(process.env.CODA_CALLBACK_UPDATE_RETRY_DELAY_MS || 500, 500, 0, 10000);
  let lastError;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const cellResponse = await fetch(cellUrl, {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${apiToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ value }),
      });

      if (cellResponse.ok) {
        return await cellResponse.json().catch(() => ({}));
      }

      const shouldFallbackToRowUpdate =
        cellResponse.status === 400
        || cellResponse.status === 404
        || cellResponse.status === 405;

      if (shouldFallbackToRowUpdate) {
        const rowResponse = await fetch(rowUrl, {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${apiToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            row: {
              cells: [
                {
                  column: String(columnId),
                  value,
                },
              ],
            },
          }),
        });

        if (rowResponse.ok) {
          return await rowResponse.json().catch(() => ({}));
        }

        throw new Error(`Failed to update cell: ${rowResponse.status} ${rowResponse.statusText}`);
      }

      throw new Error(`Failed to update cell: ${cellResponse.status} ${cellResponse.statusText}`);
    } catch (error) {
      lastError = error;
      const retryable = isRetryableCodaError(error);
      if (!retryable || attempt === maxAttempts) {
        break;
      }

      const waitMs = computeBackoffMs(baseDelayMs, attempt);
      const message = error instanceof Error ? error.message : String(error);
      log("warn", "transient_callback_update_retry", {
        attempt,
        maxAttempts,
        waitMs,
        message,
      });
      await sleep(waitMs);
    }
  }

  throw lastError instanceof Error
    ? lastError
    : new Error(String(lastError || "Failed to update cell"));
}

export async function createTableRow(docId, tableIdOrName, cells, apiToken) {
  const rowsUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/rows`;

  const normalizedCells = (Array.isArray(cells) ? cells : [])
    .filter((cell) => cell && cell.column)
    .map((cell) => ({
      column: cell.column,
      value: cell.value,
    }));

  if (normalizedCells.length === 0) {
    throw new Error("Cannot create table row without at least one cell value");
  }

  const maxAttempts = parseIntEnv(process.env.CODA_API_RETRY_ATTEMPTS || 3, 3, 1, 6);
  const baseDelayMs = parseIntEnv(process.env.CODA_API_RETRY_DELAY_MS || 800, 800, 0, 10000);
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const response = await fetch(rowsUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          rows: [{ cells: normalizedCells }],
          disableParsing: false,
        }),
      });

      if (response.ok) {
        const body = await response.json().catch(() => ({}));
        const rowId = body?.addedRowIds?.[0]
          || body?.items?.[0]?.id
          || body?.rows?.[0]?.id
          || "";

        return {
          rowId,
          raw: body,
        };
      }

      // Handle Retry-After for rate limiting
      const status = response.status;
      const retryAfter = response.headers.get && response.headers.get("retry-after");
      if (status === 429 && retryAfter) {
        let waitMs = 0;
        if (/^\d+$/.test(retryAfter)) {
          waitMs = Number(retryAfter) * 1000;
        } else {
          const parsed = Date.parse(retryAfter);
          if (!Number.isNaN(parsed)) {
            waitMs = Math.max(0, parsed - Date.now());
          }
        }
        if (waitMs > 0) {
          await sleep(waitMs);
        }
      }

      const shouldRetry = attempt < maxAttempts && isRetryableHttpStatus(status);
      if (shouldRetry) {
        const waitMs = computeBackoffMs(baseDelayMs, attempt);
        await sleep(waitMs);
        continue;
      }

      throw new Error(`Failed to create row: ${status} ${response.statusText}`);
    } catch (error) {
      lastError = error;
      const retryable = isRetryableCodaError(error);
      if (!retryable || attempt === maxAttempts) {
        break;
      }
      const waitMs = computeBackoffMs(baseDelayMs, attempt);
      await sleep(waitMs);
      continue;
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError || "Failed to create row"));
}

export async function updateTableRowCells(docId, tableIdOrName, rowId, cells, apiToken) {
  const tableRef = encodeURIComponent(String(tableIdOrName));
  const rowRef = encodeURIComponent(String(rowId));
  const rowUrl = `${CODA_API_BASE}/docs/${docId}/tables/${tableRef}/rows/${rowRef}`;

  const normalizedCells = (Array.isArray(cells) ? cells : [])
    .filter((cell) => cell && cell.column)
    .map((cell) => ({
      column: String(cell.column),
      value: cell.value,
    }));

  if (normalizedCells.length === 0) {
    return { skipped: true };
  }

  const maxAttempts = parseIntEnv(process.env.CODA_CALLBACK_UPDATE_RETRY_ATTEMPTS || 3, 3, 1, 6);
  const baseDelayMs = parseIntEnv(process.env.CODA_CALLBACK_UPDATE_RETRY_DELAY_MS || 500, 500, 0, 10000);
  let lastError;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const response = await fetch(rowUrl, {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${apiToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          row: {
            cells: normalizedCells,
          },
        }),
      });

      if (response.ok) {
        return await response.json().catch(() => ({}));
      }

      throw new Error(`Failed to update row cells: ${response.status} ${response.statusText}`);
    } catch (error) {
      lastError = error;
      const retryable = isRetryableCodaError(error);
      if (!retryable || attempt === maxAttempts) {
        break;
      }

      const waitMs = computeBackoffMs(baseDelayMs, attempt);
      const message = error instanceof Error ? error.message : String(error);
      log("warn", "transient_callback_row_update_retry", {
        attempt,
        maxAttempts,
        waitMs,
        message,
      });
      await sleep(waitMs);
    }
  }

  throw lastError instanceof Error
    ? lastError
    : new Error(String(lastError || "Failed to update row cells"));
}
