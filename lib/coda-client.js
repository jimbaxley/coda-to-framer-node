const CODA_API_BASE = "https://coda.io/apis/v1";

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
