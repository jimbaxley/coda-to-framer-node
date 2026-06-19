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

async function createCodaHttpErrorFromResponse(response, fallbackMessage = "Coda API request failed") {
  const bodyText = await response.text().catch(() => "");
  let message = `${fallbackMessage}: ${response.status} ${response.statusText}`;
  if (bodyText) {
    try {
      const parsed = JSON.parse(bodyText);
      message = parsed?.message || parsed?.error || message;
    } catch (_) {
      message = bodyText;
    }
  }

  const error = new Error(message);
  error.status = response.status;
  error.statusText = response.statusText;
  error.retryable = isRetryableHttpStatus(response.status) || isPendingMutationError(error);
  return error;
}

function getCodaHeaders(apiToken, extraHeaders = {}, options = {}) {
  const headers = {
    Authorization: `Bearer ${apiToken}`,
    ...extraHeaders,
  };

  if (options.requireLatest) {
    headers["X-Coda-Doc-Version"] = "latest";
  }

  return headers;
}

function isPendingMutationError(error) {
  const message = error instanceof Error ? error.message.toLowerCase() : String(error || "").toLowerCase();
  return (
    message.includes("pending mutation")
    || message.includes("pending mutations")
    || message.includes("not yet up to date")
    || (message.includes("latest") && message.includes("mutation"))
  );
}

function getCodaRetrySettings({ callback = false } = {}) {
  return {
    maxAttempts: parseIntEnv(
      callback
        ? process.env.CODA_CALLBACK_UPDATE_RETRY_ATTEMPTS || 5
        : process.env.CODA_API_RETRY_ATTEMPTS || 3,
      callback ? 5 : 3,
      1,
      callback ? 10 : 6,
    ),
    baseDelayMs: parseIntEnv(
      callback
        ? process.env.CODA_CALLBACK_UPDATE_RETRY_DELAY_MS || 500
        : process.env.CODA_API_RETRY_DELAY_MS || 800,
      callback ? 500 : 800,
      0,
      10000,
    ),
    maxLatestAttempts: parseIntEnv(process.env.CODA_LATEST_VERSION_RETRY_ATTEMPTS || 5, 5, 1, 8),
    latestDelayMs: parseIntEnv(process.env.CODA_LATEST_VERSION_RETRY_DELAY_MS || 1000, 1000, 100, 10000),
  };
}

function getRetryPlan(error, settings) {
  const pendingMutation = isPendingMutationError(error);
  return {
    event: pendingMutation ? "latest_version_retry" : "transient_api_retry",
    maxAttempts: pendingMutation ? settings.maxLatestAttempts : settings.maxAttempts,
    waitBaseMs: pendingMutation ? settings.latestDelayMs : settings.baseDelayMs,
  };
}

function getMutationRequestId(body) {
  if (!body || typeof body !== "object") return "";
  return String(
    body.requestId
      || body.request_id
      || body.mutationId
      || body.mutation_id
      || "",
  ).trim();
}

function isMutationApplied(statusBody) {
  if (!statusBody || typeof statusBody !== "object") return false;
  if (statusBody.completed === true || statusBody.done === true || statusBody.applied === true) {
    return true;
  }

  const status = String(statusBody.status || statusBody.state || "").toLowerCase();
  return ["complete", "completed", "done", "applied", "success", "succeeded"].includes(status);
}

async function waitForMutationStatus(docId, requestId, apiToken) {
  if (!docId || !requestId) return;

  const maxAttempts = parseIntEnv(process.env.CODA_MUTATION_STATUS_RETRY_ATTEMPTS || 20, 20, 1, 60);
  const baseDelayMs = parseIntEnv(process.env.CODA_MUTATION_STATUS_RETRY_DELAY_MS || 1000, 1000, 100, 10000);
  const statusUrl = `${CODA_API_BASE}/docs/${docId}/mutationStatus/${encodeURIComponent(requestId)}`;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const response = await fetch(statusUrl, {
        headers: {
          Authorization: `Bearer ${apiToken}`,
        },
      });

      if (!response.ok) {
        if (response.status === 404) {
          return;
        }

        throw await createCodaHttpErrorFromResponse(response, "Failed to get Coda mutation status");
      }

      const body = await response.json().catch(() => ({}));
      if (isMutationApplied(body)) {
        return;
      }
    } catch (error) {
      lastError = error;
      if (!isRetryableCodaError(error) && !isPendingMutationError(error)) {
        throw error;
      }
    }

    if (attempt < maxAttempts) {
      const waitMs = computeBackoffMs(baseDelayMs, attempt, 10000);
      log("warn", "mutation_status_retry", {
        attempt,
        maxAttempts,
        waitMs,
        requestId,
      });
      await sleep(waitMs);
    }
  }

  if (lastError) {
    throw lastError;
  }
  throw new Error(`Coda mutation did not finish before timeout: ${requestId}`);
}

async function waitForMutationFromBody(docId, body, apiToken) {
  const requestId = getMutationRequestId(body);
  if (requestId) {
    await waitForMutationStatus(docId, requestId, apiToken);
  }
}

async function fetchCodaJson(url, apiToken, options = {}) {
  const retrySettings = getCodaRetrySettings();
  let lastError;

  for (let attempt = 1; attempt <= Math.max(retrySettings.maxAttempts, retrySettings.maxLatestAttempts); attempt += 1) {
    try {
      const response = await fetch(url, {
        headers: getCodaHeaders(apiToken, {}, options),
      });

      if (!response.ok) {
        throw await createCodaHttpErrorFromResponse(response);
      }

      return response.json();
    } catch (error) {
      lastError = error;
      const retryable = isRetryableCodaError(error);
      const retryPlan = getRetryPlan(error, retrySettings);
      if (!retryable || attempt >= retryPlan.maxAttempts) {
        break;
      }

      const waitMs = computeBackoffMs(retryPlan.waitBaseMs, attempt);
      const message = error instanceof Error ? error.message : String(error);
      log("warn", retryPlan.event, {
        attempt,
        maxAttempts: retryPlan.maxAttempts,
        waitMs,
        message,
      });
      await sleep(waitMs);
    }
  }

  throw lastError;
}

export async function getCodaTableColumns(docId, tableIdOrName, apiToken, options = {}) {
  const columnsUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/columns`;
  const columnsData = await fetchCodaJson(columnsUrl, apiToken, options);
  return columnsData.items || [];
}

export async function getCodaTables(docId, apiToken, options = {}) {
  const allTables = [];
  let pageToken = "";
  do {
    const url = `${CODA_API_BASE}/docs/${docId}/tables?limit=200${pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : ""}`;
    const data = await fetchCodaJson(url, apiToken, options);
    allTables.push(...(data.items || []));
    pageToken = data.nextPageToken || "";
  } while (pageToken);
  return allTables;
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

export async function resolveTableNameOrId(docId, tableNameOrId, apiToken, options = {}) {
  const candidate = normalizeTableCandidate(tableNameOrId);
  if (!candidate) {
    throw new Error("Table reference is empty");
  }

  if (/^grid-[A-Za-z0-9_-]+$/.test(candidate)) {
    return candidate;
  }

  // Try fetching the table directly by name/id before scanning all tables.
  // The Coda API accepts both IDs and names in the table path segment.
  try {
    const url = `${CODA_API_BASE}/docs/${encodeURIComponent(docId)}/tables/${encodeURIComponent(candidate)}`;
    const data = await fetchCodaJson(url, apiToken, options);
    if (data?.id) {
      return data.id;
    }
  } catch (_) {
    // fall through to full table scan
  }

  const tables = await getCodaTables(docId, apiToken, options);
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

export async function resolveColumnNameOrId(docId, tableIdOrName, columnNameOrId, apiToken, options = {}) {
  const columns = await getCodaTableColumns(docId, tableIdOrName, apiToken, options);

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

// Resolve a table/view reference to the id of the underlying base table.
// Views only expose the rows that pass their filter, so a row that has
// dropped out of a filtered view (e.g. a "ready to publish" view) can no
// longer be found through it. Searching the parent base table instead lets
// us locate rows regardless of any view filter — essential for deleteRow,
// where the row is typically being removed precisely because it left the
// published view.
export async function resolveBaseTableId(docId, tableIdOrName, apiToken, options = {}) {
  // A grid- ID is always a base table, never a view — skip the fetch.
  if (/^grid-[A-Za-z0-9_-]+$/.test(String(tableIdOrName || ""))) {
    return tableIdOrName;
  }
  const url = `${CODA_API_BASE}/docs/${encodeURIComponent(docId)}/tables/${encodeURIComponent(tableIdOrName)}`;
  const data = await fetchCodaJson(url, apiToken, options);
  if (data?.tableType === "view" && data?.parentTable?.id) {
    return data.parentTable.id;
  }
  return data?.id || tableIdOrName;
}

// Like getCodaTableData but follows pageToken so we read every row (the
// single-page variant caps at 500). Returns the same { columns, rows } shape
// so callers can reuse findRowBySelector for matching.
export async function getCodaTableDataPaginated(docId, tableIdOrName, apiToken, options = {}, maxRows = 10000) {
  const columns = await getCodaTableColumns(docId, tableIdOrName, apiToken, options);
  const rows = [];
  let pageToken = "";

  do {
    const url = `${CODA_API_BASE}/docs/${encodeURIComponent(docId)}/tables/${encodeURIComponent(tableIdOrName)}/rows?useColumnNames=true&valueFormat=rich&limit=500${pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : ""}`;
    const data = await fetchCodaJson(url, apiToken, options);
    rows.push(...(data.items || []));
    pageToken = data.nextPageToken || "";
  } while (pageToken && rows.length < maxRows);

  return { columns, rows };
}

export async function getCodaTableData(docId, tableIdOrName, apiToken, limit = 100, options = {}) {
  const columns = await getCodaTableColumns(docId, tableIdOrName, apiToken, options);

  const rowsUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/rows?useColumnNames=true&valueFormat=rich&limit=${Math.min(limit, 500)}`;
  const rowsData = await fetchCodaJson(rowsUrl, apiToken, options);

  return {
    columns,
    rows: rowsData.items || [],
  };
}

export async function getCodaRowData(docId, tableIdOrName, rowId, apiToken, options = {}) {
  const columns = await getCodaTableColumns(docId, tableIdOrName, apiToken, options);
  const rowUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/rows/${encodeURIComponent(rowId)}?useColumnNames=true&valueFormat=rich`;

  const rowData = await fetchCodaJson(rowUrl, apiToken, options);
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

  // raise the default so a few 429s don't immediately bubble up; still
  // configurable via environment variable if an operator wants a smaller
  // ceiling.
  const retrySettings = getCodaRetrySettings({ callback: true });
  let lastError;

  for (let attempt = 1; attempt <= Math.max(retrySettings.maxAttempts, retrySettings.maxLatestAttempts); attempt += 1) {
    try {
      const cellResponse = await fetch(cellUrl, {
        method: "PUT",
        headers: getCodaHeaders(apiToken, { "Content-Type": "application/json" }),
        body: JSON.stringify({ value }),
      });

      if (cellResponse.ok) {
        const body = await cellResponse.json().catch(() => ({}));
        await waitForMutationFromBody(docId, body, apiToken);
        return body;
      }

      const shouldFallbackToRowUpdate =
        cellResponse.status === 400
        || cellResponse.status === 404
        || cellResponse.status === 405;

      if (shouldFallbackToRowUpdate) {
        const rowResponse = await fetch(rowUrl, {
          method: "PUT",
          headers: getCodaHeaders(apiToken, { "Content-Type": "application/json" }),
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
          const body = await rowResponse.json().catch(() => ({}));
          await waitForMutationFromBody(docId, body, apiToken);
          return body;
        }

        throw await createCodaHttpErrorFromResponse(rowResponse, "Failed to update cell");
      }

      throw await createCodaHttpErrorFromResponse(cellResponse, "Failed to update cell");
    } catch (error) {
      lastError = error;
      const retryable = isRetryableCodaError(error);
      const retryPlan = getRetryPlan(error, retrySettings);
      if (!retryable || attempt >= retryPlan.maxAttempts) {
        break;
      }

      const waitMs = computeBackoffMs(retryPlan.waitBaseMs, attempt);
      const message = error instanceof Error ? error.message : String(error);
      log("warn", retryPlan.event === "latest_version_retry" ? "latest_version_callback_update_retry" : "transient_callback_update_retry", {
        attempt,
        maxAttempts: retryPlan.maxAttempts,
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

  const retrySettings = getCodaRetrySettings();
  let lastError = null;

  for (let attempt = 1; attempt <= Math.max(retrySettings.maxAttempts, retrySettings.maxLatestAttempts); attempt += 1) {
    try {
      const response = await fetch(rowsUrl, {
        method: "POST",
        headers: getCodaHeaders(apiToken, { "Content-Type": "application/json" }),
        body: JSON.stringify({
          rows: [{ cells: normalizedCells }],
          disableParsing: false,
        }),
      });

      if (response.ok) {
        const body = await response.json().catch(() => ({}));
        await waitForMutationFromBody(docId, body, apiToken);
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

      const shouldRetry = attempt < retrySettings.maxAttempts && isRetryableHttpStatus(status);
      if (shouldRetry) {
        const waitMs = computeBackoffMs(retrySettings.baseDelayMs, attempt);
        await sleep(waitMs);
        continue;
      }

      throw await createCodaHttpErrorFromResponse(response, "Failed to create row");
    } catch (error) {
      lastError = error;
      const retryable = isRetryableCodaError(error);
      const retryPlan = getRetryPlan(error, retrySettings);
      if (!retryable || attempt >= retryPlan.maxAttempts) {
        break;
      }
      const waitMs = computeBackoffMs(retryPlan.waitBaseMs, attempt);
      const message = error instanceof Error ? error.message : String(error);
      log("warn", retryPlan.event === "latest_version_retry" ? "latest_version_create_row_retry" : "transient_create_row_retry", {
        attempt,
        maxAttempts: retryPlan.maxAttempts,
        waitMs,
        message,
      });
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

  // default to a slightly higher count so that throttling spikes are
  // absorbed before bubbling up to the caller. adjustable via environment.
  const retrySettings = getCodaRetrySettings({ callback: true });
  let lastError;

  for (let attempt = 1; attempt <= Math.max(retrySettings.maxAttempts, retrySettings.maxLatestAttempts); attempt += 1) {
    try {
      const response = await fetch(rowUrl, {
        method: "PUT",
        headers: getCodaHeaders(apiToken, { "Content-Type": "application/json" }),
        body: JSON.stringify({
          row: {
            cells: normalizedCells,
          },
        }),
      });

      if (response.ok) {
        const body = await response.json().catch(() => ({}));
        await waitForMutationFromBody(docId, body, apiToken);
        return body;
      }

      throw await createCodaHttpErrorFromResponse(response, "Failed to update row cells");
    } catch (error) {
      lastError = error;
      const retryable = isRetryableCodaError(error);
      const retryPlan = getRetryPlan(error, retrySettings);
      if (!retryable || attempt >= retryPlan.maxAttempts) {
        break;
      }

      const waitMs = computeBackoffMs(retryPlan.waitBaseMs, attempt);
      const message = error instanceof Error ? error.message : String(error);
      log("warn", retryPlan.event === "latest_version_retry" ? "latest_version_callback_row_update_retry" : "transient_callback_row_update_retry", {
        attempt,
        maxAttempts: retryPlan.maxAttempts,
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
