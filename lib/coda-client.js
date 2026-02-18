const CODA_API_BASE = "https://coda.io/apis/v1";

async function fetchCodaJson(url, apiToken) {
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${apiToken}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Coda API request failed: ${response.status} ${response.statusText}`);
  }

  return response.json();
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
