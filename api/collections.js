import { listCollections } from "../lib/framer-client.js";
import { sendJson } from "./sync.js"; // reuse helper

export default async function handler(req, res) {
  // Require API key for authentication
  const API_SECRET_KEY = process.env.API_SECRET_KEY;

  const requestUrl = new URL(req.url || "/api/collections", "http://localhost");
  if (req.method !== "GET") {
    res.statusCode = 405;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "METHOD_NOT_ALLOWED", message: "Use GET /api/collections" }));
    return;
  }
  // Check API key in header
  const clientKey = req.headers["x-api-key"];
  if (!API_SECRET_KEY || clientKey !== API_SECRET_KEY) {
    return res.status(401).json({ error: "UNAUTHORIZED", message: "Missing or invalid API key." });
  }

  const projectUrl =
    requestUrl.searchParams.get("projectUrl") ||
    requestUrl.searchParams.get("framerProjectUrl");
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
    return sendJson(res, 200, { success: true, collections });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return sendJson(res, 500, { error: "SERVER_ERROR", message });
  }
}
