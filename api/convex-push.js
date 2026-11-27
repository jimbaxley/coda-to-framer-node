import { sendJson } from "./sync.js";

// Forwards a SyncRowToConvex pack call to Convex's /coda/events/push.
// The pack can only reach one domain (vercel.app), so it calls here and this
// relays the row values unchanged. Nothing reads the Coda API.

const DEFAULT_CONVEX_SITE_URL = "https://oceanic-greyhound-532.convex.site";

async function readJsonBody(req) {
  if (req.body && typeof req.body === "object") return req.body;
  if (typeof req.body === "string") return JSON.parse(req.body);
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const text = Buffer.concat(chunks).toString("utf8");
  return text ? JSON.parse(text) : {};
}

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return sendJson(res, 405, { ok: false, error: "Use POST /api/convex-push" });
  }

  const API_SECRET_KEY = process.env.API_SECRET_KEY;
  if (API_SECRET_KEY) {
    const authHeader = req.headers["authorization"] || "";
    const clientKey = req.headers["x-api-key"] || authHeader.replace(/^Bearer\s+/i, "");
    if (clientKey !== API_SECRET_KEY) {
      return sendJson(res, 401, { ok: false, error: "Missing or invalid API key." });
    }
  }

  const syncSecret = String(process.env.CODA_SYNC_SECRET || "").trim();
  if (!syncSecret) {
    return sendJson(res, 500, { ok: false, error: "CODA_SYNC_SECRET not configured" });
  }

  let body;
  try {
    body = await readJsonBody(req);
  } catch {
    return sendJson(res, 400, { ok: false, error: "Body must be JSON" });
  }

  const siteUrl = String(process.env.CONVEX_SITE_URL || DEFAULT_CONVEX_SITE_URL).trim().replace(/\/+$/, "");
  try {
    const response = await fetch(`${siteUrl}/coda/events/push`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${syncSecret}`,
      },
      body: JSON.stringify(body),
    });
    const text = await response.text();
    let result;
    try {
      result = JSON.parse(text);
    } catch {
      result = { ok: false, error: `Convex returned non-JSON (${response.status}): ${text.slice(0, 300)}` };
    }
    return sendJson(res, response.status, result);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return sendJson(res, 502, { ok: false, error: `Could not reach Convex: ${message}` });
  }
}
