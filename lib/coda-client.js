const CODA_API_BASE = "https://coda.io/apis/v1";

export async function resolveColumnNameOrId(docId, tableIdOrName, columnNameOrId, apiToken) {
  const columnsUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/columns`;

  const columnsResponse = await fetch(columnsUrl, {
    headers: {
      Authorization: `Bearer ${apiToken}`,
    },
  });

  if (!columnsResponse.ok) {
    throw new Error(
      `Failed to fetch columns: ${columnsResponse.status} ${columnsResponse.statusText}`,
    );
  }

  const columnsData = await columnsResponse.json();
  const columns = columnsData.items || [];

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
  const columnsUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/columns`;

  const columnsResponse = await fetch(columnsUrl, {
    headers: {
      Authorization: `Bearer ${apiToken}`,
    },
  });

  if (!columnsResponse.ok) {
    throw new Error(
      `Failed to fetch columns: ${columnsResponse.status} ${columnsResponse.statusText}`,
    );
  }

  const columnsData = await columnsResponse.json();

  const rowsUrl = `${CODA_API_BASE}/docs/${docId}/tables/${encodeURIComponent(
    tableIdOrName,
  )}/rows?useColumnNames=true&valueFormat=rich&limit=${Math.min(limit, 500)}`;

  const rowsResponse = await fetch(rowsUrl, {
    headers: {
      Authorization: `Bearer ${apiToken}`,
    },
  });

  if (!rowsResponse.ok) {
    throw new Error(
      `Failed to fetch rows: ${rowsResponse.status} ${rowsResponse.statusText}`,
    );
  }

  const rowsData = await rowsResponse.json();

  return {
    columns: columnsData.items || [],
    rows: rowsData.items || [],
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
