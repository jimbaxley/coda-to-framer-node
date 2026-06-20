import { getCodaTableColumns } from "../lib/coda-client.js";
import { sendJson } from "./sync.js";

export default async function handler(req, res) {
  if (req.method !== "GET") {
    return sendJson(res, 405, { error: "METHOD_NOT_ALLOWED", message: "Use GET /api/columns" });
  }

  const API_SECRET_KEY = process.env.API_SECRET_KEY;
  if (API_SECRET_KEY) {
    const authHeader = req.headers["authorization"] || "";
    const clientKey = req.headers["x-api-key"] || authHeader.replace(/^Bearer\s+/i, "");
    if (clientKey !== API_SECRET_KEY) {
      return sendJson(res, 401, { error: "UNAUTHORIZED", message: "Missing or invalid API key." });
    }
  }

  const codaApiToken = process.env.CODA_API_TOKEN;
  if (!codaApiToken) {
    return sendJson(res, 500, { error: "SERVER_ERROR", message: "CODA_API_TOKEN not configured" });
  }

  const requestUrl = new URL(req.url || "/api/columns", "http://localhost");
  const docId = requestUrl.searchParams.get("docId") || "";
  const tableIdOrName = requestUrl.searchParams.get("tableIdOrName") || "";

  if (!docId || !tableIdOrName) {
    return sendJson(res, 400, {
      error: "INVALID_REQUEST",
      message: "Missing required query params: docId, tableIdOrName",
    });
  }

  try {
    const columns = await getCodaTableColumns(docId, tableIdOrName, codaApiToken);
    // Return as a compact Name:id,Name:id string for easy storage in a Coda cell,
    // plus the raw array for any future use.
    const columnIdString = columns
      .map((col) => `${col.name}:${col.id}`)
      .join(",");

    return sendJson(res, 200, {
      success: true,
      columnIdString,
      columns: columns.map((col) => ({ name: col.name, id: col.id })),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return sendJson(res, 500, { error: "SERVER_ERROR", message });
  }
}
