const DEFAULT_EVENTS_UPSERT_PATH = "/coda/events/upsert";
const DEFAULT_CONVEX_SITE_URL = "https://oceanic-greyhound-532.convex.site";

function trimTrailingSlash(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function getConvexConfig(payload = {}) {
  const siteUrl = trimTrailingSlash(
    payload.convexSiteUrl || process.env.CONVEX_SITE_URL || DEFAULT_CONVEX_SITE_URL,
  );
  const eventsUpsertPath = String(
    payload.convexEventsUpsertPath ||
      process.env.CONVEX_EVENTS_UPSERT_PATH ||
      DEFAULT_EVENTS_UPSERT_PATH,
  ).startsWith("/")
    ? String(payload.convexEventsUpsertPath || process.env.CONVEX_EVENTS_UPSERT_PATH || DEFAULT_EVENTS_UPSERT_PATH)
    : `/${payload.convexEventsUpsertPath || process.env.CONVEX_EVENTS_UPSERT_PATH || DEFAULT_EVENTS_UPSERT_PATH}`;

  return {
    enabled: parseBooleanFlag(payload.convexSync ?? process.env.CONVEX_SYNC_ENABLED, true),
    siteUrl,
    eventsUpsertPath,
    syncSecret: String(payload.convexSyncSecret || process.env.CODA_SYNC_SECRET || "").trim(),
  };
}

function parseBooleanFlag(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

function normalizeName(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function getFieldValueByNames(item, fields, names) {
  const wanted = new Set(names.map(normalizeName));
  for (const field of fields || []) {
    if (!field?.id) continue;
    const normalizedName = normalizeName(field.name);
    if (!wanted.has(normalizedName)) continue;
    const value = item?.fieldData?.[field.id]?.value;
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return value;
    }
  }
  return undefined;
}

function getFieldValue(item, fields, envName, fallbackNames) {
  const configuredName = String(process.env[envName] || "").trim();
  const names = configuredName ? [configuredName, ...fallbackNames] : fallbackNames;
  return getFieldValueByNames(item, fields, names);
}

function asOptionalString(value) {
  if (value === undefined || value === null) return undefined;
  const text = String(value).trim();
  return text ? text : undefined;
}

function asOptionalNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function asOptionalBoolean(value) {
  if (typeof value === "boolean") return value;
  if (value === undefined || value === null || value === "") return undefined;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return undefined;
}

function asEpochMs(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string" || !value.trim()) return undefined;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function asTags(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item).trim()).filter(Boolean);
  }
  if (typeof value === "string") {
    return value.split(",").map((item) => item.trim()).filter(Boolean);
  }
  return [];
}

function normalizeStatus(value) {
  const status = String(value || "").trim().toLowerCase();
  if (status === "draft" || status === "completed" || status === "cancelled") {
    return status;
  }
  if (status === "complete") return "completed";
  return "upcoming";
}

export function shouldPublishItemToFramer({ payload = {}, mappingResult, item }) {
  if (parseBooleanFlag(payload.bypassConvex)) {
    return true;
  }
  if (parseBooleanFlag(payload.publishCompletedToFramer ?? process.env.CONVEX_PUBLISH_COMPLETED_TO_FRAMER, false)) {
    return true;
  }
  const eventPayload = buildConvexEventPayload({ payload, mappingResult, item });
  return eventPayload.publishToFramer;
}

function buildRowScopedIdempotencyKey({ payload, item }) {
  const docId = String(payload.docId || "").trim();
  const tableIdOrName = String(payload.tableIdOrName || "").trim();
  const rowId = String(item?.id || payload.rowId || "").trim();
  return `row-${docId}-${tableIdOrName}-${rowId}`;
}

export function buildConvexEventPayload({ payload, mappingResult, item }) {
  const fields = mappingResult?.fields || [];
  const title = asOptionalString(
    getFieldValue(item, fields, "CONVEX_EVENT_TITLE_FIELD", [
      "Title",
      "Event Title",
      "Name",
      "Event Name",
      "Headline",
    ]),
  );
  const startsAt = asEpochMs(
    getFieldValue(item, fields, "CONVEX_EVENT_STARTS_AT_FIELD", [
      "Starts At",
      "Start Date",
      "Start Time",
      "Date",
      "Event Date",
      "When",
    ]),
  );
  const endsAt = asEpochMs(
    getFieldValue(item, fields, "CONVEX_EVENT_ENDS_AT_FIELD", [
      "Ends At",
      "End Date",
      "End Time",
    ]),
  );
  const location = asOptionalString(
    getFieldValue(item, fields, "CONVEX_EVENT_LOCATION_FIELD", [
      "Location",
      "Venue",
      "Address",
      "Event Location",
    ]),
  );
  const description = asOptionalString(
    getFieldValue(item, fields, "CONVEX_EVENT_DESCRIPTION_FIELD", [
      "Description",
      "Details",
      "Body",
      "About",
    ]),
  );
  const capacity = asOptionalNumber(
    getFieldValue(item, fields, "CONVEX_EVENT_CAPACITY_FIELD", [
      "Capacity",
      "Max Attendees",
      "Max Capacity",
    ]),
  );
  const codaStatus = asOptionalString(
    getFieldValue(item, fields, "CONVEX_EVENT_STATUS_FIELD", [
      "Status",
      "Event Status",
    ]),
  );
  const status = normalizeStatus(codaStatus);
  const live = asOptionalBoolean(
    getFieldValue(item, fields, "CONVEX_EVENT_LIVE_FIELD", [
      "Live",
      "Is Live",
      "Published",
      "Public",
      "Publish",
      "Ready",
    ]),
  );
  const tags = asTags(
    getFieldValue(item, fields, "CONVEX_EVENT_TAGS_FIELD", [
      "Tags",
      "Categories",
      "Event Type",
    ]),
  );
  const eaEventId = asOptionalString(
    getFieldValue(item, fields, "CONVEX_EVENT_EA_ID_FIELD", [
      "EA Event ID",
      "EveryAction Event ID",
      "EveryAction ID",
    ]),
  );

  if (!title) {
    throw new Error(
      "Convex event sync requires a title. Set CONVEX_EVENT_TITLE_FIELD to the Coda column name if it is not Title/Event Title/Name.",
    );
  }

  return {
    idempotencyKey: buildRowScopedIdempotencyKey({ payload, item }),
    externalJobId: payload.jobId,
    requestId: payload.requestId,
    docId: payload.docId,
    tableIdOrName: payload.tableIdOrName,
    rowId: item.id,
    slug: item.slug,
    title,
    startsAt,
    endsAt,
    location,
    description,
    capacity,
    status,
    codaStatus,
    live,
    publishToFramer: live === true && status !== "completed",
    tags,
    eaEventId,
    framerSlug: item.slug,
    sourceUpdatedAt: Date.now(),
    rawCodaValues: item.fieldData,
  };
}

export async function upsertEventToConvex({ payload, mappingResult, eventLogger }) {
  const config = getConvexConfig(payload);
  if (!config.enabled) {
    eventLogger?.("info", "convex_sync", "Convex sync skipped by configuration");
    return { skipped: true };
  }
  if (!config.siteUrl) {
    throw new Error("CONVEX_SITE_URL is not configured");
  }
  if (!config.syncSecret) {
    throw new Error("CODA_SYNC_SECRET is not configured");
  }

  const item = mappingResult?.items?.[0];
  if (!item) {
    throw new Error("Convex event sync requires one mapped row item");
  }

  const eventPayload = buildConvexEventPayload({ payload, mappingResult, item });
  const url = `${config.siteUrl}${config.eventsUpsertPath}`;
  eventLogger?.("info", "convex_sync", "Saving event to Convex", {
    url,
    rowId: eventPayload.rowId,
    slug: eventPayload.slug,
    idempotencyKey: eventPayload.idempotencyKey,
  });

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${config.syncSecret}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(eventPayload),
  });

  let body = null;
  const text = await response.text();
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { message: text };
    }
  }

  if (!response.ok) {
    const message = body?.error || body?.message || `Convex request failed (${response.status})`;
    throw new Error(message);
  }

  eventLogger?.("info", "convex_sync", "Event saved to Convex", {
    rowId: eventPayload.rowId,
    eventId: body?.eventId || "",
    deduped: Boolean(body?.deduped),
  });

  return {
    skipped: false,
    payload: eventPayload,
    response: body || {},
  };
}

export async function upsertEventsToConvex({ payload, mappingResult, eventLogger }) {
  const config = getConvexConfig(payload);
  if (!config.enabled) {
    eventLogger?.("info", "convex_sync", "Convex sync skipped by configuration");
    return { skipped: true, count: 0, results: [] };
  }

  const items = mappingResult?.items || [];
  const results = [];
  for (const item of items) {
    const singleResult = await upsertEventToConvex({
      payload,
      mappingResult: {
        ...mappingResult,
        items: [item],
      },
      eventLogger,
    });
    results.push(singleResult);
  }

  return {
    skipped: false,
    count: results.length,
    results,
  };
}
