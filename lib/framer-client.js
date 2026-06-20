import {
  sleep,
  parseIntEnv,
  computeBackoffMs,
  isRetryableFramerError,
} from "./retry-policy.js";
import { randomUUID } from "node:crypto";

function log(level, event, fields = {}) {
  const record = {
    ts: new Date().toISOString(),
    source: "framer-client",
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

function parseFramerProjectId(projectUrl) {
  const text = String(projectUrl || "");
  const urlMatch = text.match(/--([A-Za-z0-9]+)(?:$|[/?#])/);
  if (urlMatch?.[1]) return urlMatch[1];
  const rawIdMatch = text.match(/^[A-Za-z0-9]{20}$/);
  return rawIdMatch ? text : "";
}

function makeTrace(operation, projectUrl, attempt) {
  return {
    traceId: randomUUID(),
    operation,
    projectId: parseFramerProjectId(projectUrl),
    projectUrl,
    attempt,
  };
}

function framerIds(framer) {
  if (!framer || typeof framer !== "object") return {};
  return {
    framerRequestId: framer.requestId || "",
    framerSessionId: framer.sessionId || "",
  };
}

function framerErrorFields(error) {
  const maybeError = error && typeof error === "object" ? error : {};
  return {
    errorName: error instanceof Error ? error.name : "Error",
    errorMessage: error instanceof Error ? error.message : String(error),
    errorCode: String(maybeError.code || ""),
    errorStatus: Number.isFinite(maybeError.status)
      ? Number(maybeError.status)
      : Number.isFinite(maybeError.statusCode)
        ? Number(maybeError.statusCode)
        : null,
    errorCause: maybeError.cause
      ? (maybeError.cause instanceof Error ? maybeError.cause.message : String(maybeError.cause))
      : "",
    errorDetails: safeJson(maybeError.details || maybeError.data || maybeError.response || maybeError.body),
  };
}

function isSessionExpiredError(error) {
  if (!error || typeof error !== "object") return false;
  const maybeError = error;
  const message = String(maybeError.message || "").toLowerCase();
  const code = String(maybeError.code || "").toUpperCase();
  // Exclude transient connection drops — those are not auth failures
  if (message.includes("connection closed") || message.includes("socket hang up")) return false;
  return code === "PROJECT_CLOSED" || message.includes("session expired");
}

function getConnectTimeoutMs() {
  const parsed = Number(process.env.FRAMER_CONNECT_TIMEOUT_MS || 0);
  if (Number.isFinite(parsed) && parsed >= 5000) {
    return Math.min(parsed, 30000);
  }
  return 30000;
}

async function connectWithTimeout(projectUrl, apiKey, trace = makeTrace("connect", projectUrl, 1)) {
  const { connect } = await import("framer-api");
  const timeoutMs = getConnectTimeoutMs();
  const startedAt = Date.now();

  log("info", "trace_connect_start", {
    ...trace,
    timeoutMs,
    tokenConfigured: Boolean(apiKey),
  });

  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(
        new Error(
          `Framer connection timed out after ${timeoutMs}ms (connect).`,
        ),
      );
    }, timeoutMs);
  });

  try {
    const framer = await Promise.race([connect(projectUrl, apiKey), timeoutPromise]);
    log("info", "trace_connect_success", {
      ...trace,
      ...framerIds(framer),
      elapsedMs: Date.now() - startedAt,
      timeoutMs,
    });
    return framer;
  } catch (error) {
    log("warn", "trace_connect_failed", {
      ...trace,
      ...framerErrorFields(error),
      elapsedMs: Date.now() - startedAt,
      timeoutMs,
    });
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function runWithConnection(projectUrl, apiKey, fn, trace = makeTrace("connection", projectUrl, 1)) {
  const framer = await connectWithTimeout(projectUrl, apiKey, trace);
  const startedAt = Date.now();
  try {
    const result = await fn(framer);
    log("info", "trace_session_success", {
      ...trace,
      ...framerIds(framer),
      elapsedMs: Date.now() - startedAt,
    });
    return result;
  } catch (error) {
    log("warn", "trace_session_failed", {
      ...trace,
      ...framerIds(framer),
      ...framerErrorFields(error),
      elapsedMs: Date.now() - startedAt,
    });
    throw error;
  } finally {
    const disconnectStartedAt = Date.now();
    try {
      await framer.disconnect();
      log("info", "trace_disconnect_success", {
        ...trace,
        ...framerIds(framer),
        elapsedMs: Date.now() - disconnectStartedAt,
      });
    } catch (error) {
      log("warn", "trace_disconnect_failed", {
        ...trace,
        ...framerIds(framer),
        ...framerErrorFields(error),
        elapsedMs: Date.now() - disconnectStartedAt,
      });
    }
  }
}

export function withTimeout(promise, timeoutMs, operationName) {
  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      reject(
        new Error(
          `Framer operation timed out after ${timeoutMs}ms (${operationName}).`,
        ),
      );
    }, timeoutMs);
  });

  return Promise.race([promise, timeoutPromise]).finally(() => {
    clearTimeout(timer);
  });
}

export function getOperationTimeoutMs() {
  const parsed = Number(process.env.FRAMER_OPERATION_TIMEOUT_MS || 0);
  if (Number.isFinite(parsed) && parsed >= 5000) {
    return Math.min(parsed, 120000);
  }
  return 90000;
}

export function getPublishTimeoutMs() {
  const parsed = Number(process.env.FRAMER_PUBLISH_TIMEOUT_MS || 0);
  if (Number.isFinite(parsed) && parsed >= 10000) {
    return Math.min(parsed, 300000);
  }
  return 180000;
}

function safeJson(value, maxLength = 1200) {
  if (value === null || value === undefined) return "";
  const seen = new WeakSet();
  const text = JSON.stringify(
    value,
    (_, currentValue) => {
      if (typeof currentValue === "object" && currentValue !== null) {
        if (seen.has(currentValue)) return "[Circular]";
        seen.add(currentValue);
      }
      return currentValue;
    },
  );
  if (!text) return "";
  return text.length > maxLength ? `${text.slice(0, maxLength)}…` : text;
}

export function formatFramerError(phase, error) {
  const message = error instanceof Error ? error.message : String(error);
  const maybeError = error && typeof error === "object" ? error : {};
  const code = String(maybeError.code || "");
  const name = String(maybeError.name || "");
  const status = Number.isFinite(maybeError.status)
    ? Number(maybeError.status)
    : Number.isFinite(maybeError.statusCode)
      ? Number(maybeError.statusCode)
      : null;
  const causeMessage = maybeError.cause
    ? (maybeError.cause instanceof Error ? maybeError.cause.message : String(maybeError.cause))
    : "";
  const detailsJson = safeJson(maybeError.details || maybeError.data || maybeError.response || maybeError.body);

  const segments = [
    `${phase} failed`,
    message || "Unknown Framer error",
    code ? `code=${code}` : "",
    status !== null ? `status=${status}` : "",
    causeMessage ? `cause=${causeMessage}` : "",
    detailsJson ? `details=${detailsJson}` : "",
  ].filter(Boolean);

  return {
    message: segments.join(" | "),
    fields: {
      phase,
      errorMessage: message,
      errorCode: code,
      errorName: name,
      status,
      causeMessage,
      details: detailsJson,
    },
  };
}

function extractMissingFieldKey(message) {
  const match = String(message || "").match(/Field not found for key:\s*([^\s]+)/i);
  return match?.[1] || null;
}

function stripFieldFromItems(items, fieldKey) {
  return items.map((item) => {
    const fieldData = item?.fieldData && typeof item.fieldData === "object"
      ? { ...item.fieldData }
      : {};
    if (fieldKey in fieldData) {
      delete fieldData[fieldKey];
    }
    return {
      ...item,
      fieldData,
    };
  });
}

function isOperationTimeoutError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return message.toLowerCase().includes("operation timed out");
}

function chunkArray(values, chunkSize) {
  const size = Math.max(1, chunkSize);
  const chunks = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function getAddFallbackConfig(timeoutMs) {
  return {
    chunkSize: parseIntEnv(process.env.FRAMER_ADD_CHUNK_SIZE || 2, 2, 1, 10),
    chunkTimeoutMs: parseIntEnv(
      process.env.FRAMER_ADD_CHUNK_TIMEOUT_MS || Math.min(timeoutMs, 12000),
      Math.min(timeoutMs, 12000),
      3000,
      30000,
    ),
    perItemTimeoutMs: parseIntEnv(
      process.env.FRAMER_ADD_PER_ITEM_TIMEOUT_MS || Math.min(timeoutMs, 8000),
      Math.min(timeoutMs, 8000),
      3000,
      20000,
    ),
  };
}

async function withFramer(projectUrl, apiKey, fn, operation = "framer-operation") {
  const maxAttempts = parseIntEnv(process.env.FRAMER_RETRY_ATTEMPTS || 5, 5, 1, 6);
  const baseDelayMs = parseIntEnv(process.env.FRAMER_RETRY_DELAY_MS || 2000, 2000, 0, 10000);
  let lastError;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const trace = makeTrace(operation, projectUrl, attempt);
    try {
      return await runWithConnection(projectUrl, apiKey, fn, trace);
    } catch (error) {
      lastError = error;
      const canRetry = isRetryableFramerError(error);
      if (!canRetry || attempt === maxAttempts) {
        break;
      }

      const message = error instanceof Error ? error.message : String(error);
      const waitMs = computeBackoffMs(baseDelayMs, attempt);
      log("warn", "transient_connection_retry", {
        ...trace,
        attempt,
        maxAttempts,
        waitMs,
        message,
        ...framerErrorFields(error),
      });
      await sleep(waitMs);
    }
  }

  if (isSessionExpiredError(lastError)) {
    throw new Error(
      "Framer session expired. Check that FRAMER_API_KEY is valid and regenerate it in Framer if needed.",
    );
  }

  throw lastError;
}

export async function runSyncSession(projectUrl, apiKey, collectionName, fn, options = {}) {
  const maxAttempts = parseIntEnv(process.env.FRAMER_RETRY_ATTEMPTS || 5, 5, 1, 6);
  const baseDelayMs = parseIntEnv(process.env.FRAMER_RETRY_DELAY_MS || 2000, 2000, 0, 10000);
  const timeoutMs = getOperationTimeoutMs();
  const cachedCollectionId = String(options.collectionId || "").trim();
  const assumeNew = Boolean(options.assumeNew);
  let lastError;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const trace = makeTrace(`sync:${collectionName}`, projectUrl, attempt);
    try {
      return await runWithConnection(projectUrl, apiKey, async (framer) => {
        log("info", "trace_sync_session_start", {
          ...trace,
          ...framerIds(framer),
          collectionName,
          cachedCollectionId: cachedCollectionId || null,
          assumeNew,
          timeoutMs,
        });

        let collectionHandle;
        let collectionMeta;
        let collections = null;

        async function getCollections(operationName = "getManagedCollections") {
          if (collections) return collections;
          collections = await withTimeout(
            framer.getManagedCollections(),
            timeoutMs,
            operationName,
          );
          return collections;
        }

        async function createCollection() {
          const created = await withTimeout(
            framer.createManagedCollection(collectionName),
            timeoutMs,
            "createManagedCollection",
          );

          const delayMs = 500;
          await new Promise((resolve) => setTimeout(resolve, delayMs));

          let found = null;
          for (let i = 0; i < 3; i++) {
            const refreshed = await withTimeout(
              framer.getManagedCollections(),
              timeoutMs,
              "getManagedCollections",
            );
            collections = refreshed;
            found = refreshed.find((item) => item.id === created.id);
            if (found) break;
            await new Promise((resolve) => setTimeout(resolve, delayMs));
          }

          return {
            collectionHandle: found || created,
            collectionMeta: {
              collectionId: created.id,
              collectionName: created.name,
              created: true,
              foundAfterCreate: !!found,
            },
          };
        }

        if (cachedCollectionId) {
          const managedCollections = await getCollections("getManagedCollections (cached id)");
          const existingById = managedCollections.find((item) => item.id === cachedCollectionId) ?? null;
          if (existingById) {
            collectionHandle = existingById;
            collectionMeta = {
              collectionId: existingById.id,
              collectionName: existingById.name,
              created: false,
              resolvedBy: "cachedId",
            };
          } else {
            log("warn", "trace_sync_cached_collection_missing", {
              ...trace,
              ...framerIds(framer),
              collectionName,
              cachedCollectionId,
            });
          }
        }

        if (!collectionMeta && assumeNew) {
          try {
            const createdCollection = await createCollection();
            collectionHandle = createdCollection.collectionHandle;
            collectionMeta = {
              ...createdCollection.collectionMeta,
              resolvedBy: "createdAssumeNew",
            };
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (!/already exists|duplicate|same name|name already|taken/i.test(message)) {
              throw error;
            }
            log("warn", "trace_sync_create_first_fallback", {
              ...trace,
              ...framerIds(framer),
              collectionName,
              message,
            });
            collections = null;
          }
        }

        if (!collectionMeta) {
          const managedCollections = await getCollections();
          const existing = managedCollections.find((item) => item.name === collectionName) ?? null;
          if (existing) {
            collectionHandle = existing;
            collectionMeta = {
              collectionId: existing.id,
              collectionName: existing.name,
              created: false,
              resolvedBy: cachedCollectionId ? "nameAfterMissingCachedId" : "name",
            };
          } else {
            const createdCollection = await createCollection();
            collectionHandle = createdCollection.collectionHandle;
            collectionMeta = {
              ...createdCollection.collectionMeta,
              resolvedBy: cachedCollectionId ? "createdAfterMissingCachedId" : "created",
            };
          }
        }

        if (collectionMeta.collectionId !== cachedCollectionId && cachedCollectionId) {
          log("info", "trace_sync_collection_id_changed", {
            ...trace,
            ...framerIds(framer),
            collectionName,
            cachedCollectionId,
            resolvedCollectionId: collectionMeta.collectionId,
          });
        }

        log("info", "trace_sync_collection_ready", {
          ...trace,
          ...framerIds(framer),
          collectionId: collectionMeta.collectionId,
          collectionName: collectionMeta.collectionName,
          collectionCreated: collectionMeta.created,
          collectionFoundAfterCreate: collectionMeta.foundAfterCreate ?? null,
          collectionResolvedBy: collectionMeta.resolvedBy || null,
        });

        return fn({ framer, collection: collectionHandle, collectionMeta, timeoutMs });
      }, trace);
    } catch (error) {
      lastError = error;
      const canRetry = isRetryableFramerError(error);
      if (!canRetry || attempt === maxAttempts) break;

      const message = error instanceof Error ? error.message : String(error);
      const waitMs = computeBackoffMs(baseDelayMs, attempt);
      log("warn", "transient_connection_retry", {
        ...trace,
        attempt,
        maxAttempts,
        waitMs,
        message,
        ...framerErrorFields(error),
      });
      await sleep(waitMs);
    }
  }

  if (isSessionExpiredError(lastError)) {
    throw new Error(
      "Framer session expired. Check that FRAMER_API_KEY is valid and regenerate it in Framer if needed.",
    );
  }

  throw lastError;
}

export async function getOrCreateManagedCollection(
  projectUrl,
  apiKey,
  collectionName,
) {
  const timeoutMs = getOperationTimeoutMs();
  return withFramer(projectUrl, apiKey, async (framer) => {
    const collections = await withTimeout(
      framer.getManagedCollections(),
      timeoutMs,
      "getManagedCollections",
    );
    const existing = collections.find((item) => item.name === collectionName);
    if (existing) {
      return {
        collectionId: existing.id,
        collectionName: existing.name,
        created: false,
      };
    }
    const created = await withTimeout(
      framer.createManagedCollection(collectionName),
      timeoutMs,
      "createManagedCollection",
    );

    // After creation, wait a bit and try to fetch the collection by id
    const delayMs = 500; // 0.5s delay, adjust as needed
    await new Promise((resolve) => setTimeout(resolve, delayMs));

    // Try to find the collection by id, retry a few times if not found
    let found = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      const updatedCollections = await withTimeout(
        framer.getManagedCollections(),
        timeoutMs,
        "getManagedCollections",
      );
      found = updatedCollections.find((item) => item.id === created.id);
      if (found) break;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }

    return {
      collectionId: created.id,
      collectionName: created.name,
      created: true,
      foundAfterCreate: !!found,
    };
  }, `getOrCreateManagedCollection:${collectionName}`);
}

export async function setCollectionFields(
  projectUrl,
  apiKey,
  collectionId,
  fields,
) {
  const timeoutMs = getOperationTimeoutMs();
  return withFramer(projectUrl, apiKey, async (framer) => {
    const collections = await withTimeout(
      framer.getManagedCollections(),
      timeoutMs,
      "getManagedCollections",
    );
    const collection = collections.find((item) => item.id === collectionId);
    if (!collection) {
      throw new Error("Managed collection not found.");
    }
    const compatibleFields = fields.filter(Boolean);
    await withTimeout(
      collection.setFields(compatibleFields),
      timeoutMs,
      "setCollectionFields",
    );
    return compatibleFields.length;
  }, `setCollectionFields:${collectionId}`);
}

function getFieldIdsFromCollection(collection) {
  if (!collection || typeof collection !== "object") return [];
  const rawFields = Array.isArray(collection.fields) ? collection.fields : [];
  return rawFields
    .map((field) => (field && typeof field === "object" ? field.id : null))
    .filter((id) => typeof id === "string" && id.length > 0);
}

function normalizeCollectionFields(fields) {
  return (Array.isArray(fields) ? fields : [])
    .map((field) => {
      if (!field || typeof field !== "object") return null;
      const id = typeof field.id === "string" ? field.id : null;
      const name =
        typeof field.name === "string"
          ? field.name
          : typeof field.label === "string"
            ? field.label
            : typeof field.title === "string"
              ? field.title
              : null;
      if (!id || !name) return null;
      return { id, name };
    })
    .filter(Boolean);
}

async function fetchCollectionFields(collection, timeoutMs) {
  let normalized = normalizeCollectionFields(collection?.fields);
  if (normalized.length > 0) return normalized;

  if (collection && typeof collection.getFields === "function") {
    const fetchedFields = await withTimeout(
      collection.getFields(),
      timeoutMs,
      "getCollectionFields",
    );
    normalized = normalizeCollectionFields(fetchedFields);
  }

  return normalized;
}

export async function getCollectionFields(
  projectUrl,
  apiKey,
  collectionId,
) {
  const timeoutMs = getOperationTimeoutMs();
  return withFramer(projectUrl, apiKey, async (framer) => {
    const collections = await withTimeout(
      framer.getManagedCollections(),
      timeoutMs,
      "getManagedCollections",
    );
    const collection = collections.find((item) => item.id === collectionId);
    if (!collection) {
      throw new Error("Managed collection not found.");
    }

    return fetchCollectionFields(collection, timeoutMs);
  }, `getCollectionFields:${collectionId}`);
}

// New helper: list all managed collections for a project
export async function listCollections(projectUrl, apiKey) {
  const timeoutMs = getOperationTimeoutMs();
  return withFramer(projectUrl, apiKey, async (framer) => {
    return withTimeout(
      framer.getManagedCollections(),
      timeoutMs,
      "getManagedCollections",
    );
  }, "listCollections");
}

export async function getCollectionFieldIds(
  projectUrl,
  apiKey,
  collectionId,
) {
  const fields = await getCollectionFields(projectUrl, apiKey, collectionId);
  return Array.from(new Set(fields.map((field) => field.id)));
}

export async function addItemsToCollection(
  projectUrl,
  apiKey,
  collectionId,
  items,
) {
  if (!items || items.length === 0) return;
  const timeoutMs = getOperationTimeoutMs();
  const fallbackConfig = getAddFallbackConfig(timeoutMs);

  const addBatchWithFieldRetry = async (
    collection,
    batch,
    operationName,
    operationTimeoutMs,
    removedFieldKeys,
  ) => {
    try {
      await withTimeout(
        collection.addItems(batch),
        operationTimeoutMs,
        operationName,
      );
      return batch;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const missingFieldKey = extractMissingFieldKey(message);

      if (missingFieldKey && !removedFieldKeys.has(missingFieldKey)) {
        removedFieldKeys.add(missingFieldKey);
        log("warn", "missing_field_retry", {
          operationName,
          missingFieldKey,
        });
        const sanitizedBatch = stripFieldFromItems(batch, missingFieldKey);
        await withTimeout(
          collection.addItems(sanitizedBatch),
          operationTimeoutMs,
          `${operationName} retry`,
        );
        return sanitizedBatch;
      }

      throw error;
    }
  };

  return withFramer(projectUrl, apiKey, async (framer) => {
    const collections = await withTimeout(
      framer.getManagedCollections(),
      timeoutMs,
      "getManagedCollections",
    );
    const collection = collections.find((item) => item.id === collectionId);
    if (!collection) {
      throw new Error("Managed collection not found.");
    }
    let pendingItems = items;
    const removedFieldKeys = new Set();
    let bulkFailedWithTimeout = false;

    try {
      pendingItems = await addBatchWithFieldRetry(
        collection,
        pendingItems,
        "addItemsToCollection (bulk)",
        timeoutMs,
        removedFieldKeys,
      );
      return;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      bulkFailedWithTimeout = isOperationTimeoutError(error);
      const fallbackMode = bulkFailedWithTimeout ? "chunked" : "per-item";
      log("warn", "bulk_add_fallback", {
        message,
        fallbackMode,
      });
    }

    const failedItemIds = [];

    if (bulkFailedWithTimeout && pendingItems.length > 1) {
      const chunks = chunkArray(pendingItems, fallbackConfig.chunkSize);
      const chunkFailures = [];

      for (const chunk of chunks) {
        const chunkLabel = `addItemsToCollection chunk (${chunk[0]?.id || "unknown"}..${chunk[chunk.length - 1]?.id || "unknown"})`;
        try {
          await addBatchWithFieldRetry(
            collection,
            chunk,
            chunkLabel,
            fallbackConfig.chunkTimeoutMs,
            removedFieldKeys,
          );
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          if (isOperationTimeoutError(error) && chunk.length > 1) {
            chunkFailures.push(...chunk);
          } else {
            for (const item of chunk) {
              failedItemIds.push(`${item?.id || "unknown"}: ${message}`);
            }
          }
        }
      }

      pendingItems = chunkFailures;
    }

    for (const item of pendingItems) {
      try {
        await addBatchWithFieldRetry(
          collection,
          [item],
          `addItemsToCollection item ${item?.id || "unknown"}`,
          fallbackConfig.perItemTimeoutMs,
          removedFieldKeys,
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        failedItemIds.push(`${item?.id || "unknown"}: ${message}`);
      }
    }

    if (failedItemIds.length > 0) {
      throw new Error(`Failed to add ${failedItemIds.length} item(s): ${failedItemIds.join(" | ")}`);
    }
  }, `addItemsToCollection:${collectionId}`);
}

export async function getManagedCollectionItemIds(
  projectUrl,
  apiKey,
  collectionId,
) {
  const timeoutMs = getOperationTimeoutMs();
  return withFramer(projectUrl, apiKey, async (framer) => {
    const collections = await withTimeout(
      framer.getManagedCollections(),
      timeoutMs,
      "getManagedCollections",
    );
    const collection = collections.find((item) => item.id === collectionId);
    if (!collection) {
      throw new Error("Managed collection not found.");
    }

    if (typeof collection.getItemIds !== "function") {
      return [];
    }

    const itemIds = await withTimeout(
      collection.getItemIds(),
      timeoutMs,
      "getManagedCollectionItemIds",
    );
    return Array.isArray(itemIds) ? itemIds.map((id) => String(id)) : [];
  }, `getManagedCollectionItemIds:${collectionId}`);
}

export async function removeItemsFromManagedCollection(
  projectUrl,
  apiKey,
  collectionId,
  itemIds,
) {
  if (!Array.isArray(itemIds) || itemIds.length === 0) {
    return 0;
  }

  const timeoutMs = getOperationTimeoutMs();
  return withFramer(projectUrl, apiKey, async (framer) => {
    const collections = await withTimeout(
      framer.getManagedCollections(),
      timeoutMs,
      "getManagedCollections",
    );
    const collection = collections.find((item) => item.id === collectionId);
    if (!collection) {
      throw new Error("Managed collection not found.");
    }

    await withTimeout(
      collection.removeItems(itemIds),
      timeoutMs,
      "removeManagedCollectionItems",
    );
    return itemIds.length;
  }, `removeItemsFromManagedCollection:${collectionId}`);
}
export async function publishProject(projectUrl, apiKey) {
  log("info", "publish_start", { projectUrl });
  const timeoutMs = getPublishTimeoutMs();
  const maxAttempts = 3;
  const retryDelayMs = 3000;
  let lastError;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const result = await withFramer(projectUrl, apiKey, async (framer) => {
        if (attempt > 1) {
          log("info", "publish_retry", { projectUrl, attempt, maxAttempts, ...framerIds(framer) });
        } else {
          log("info", "publish_connected", { projectUrl, ...framerIds(framer) });
        }

        log("info", "publish_running", { projectUrl, attempt, ...framerIds(framer) });
        let publishResult;
        const publishStartedAt = Date.now();
        try {
          publishResult = await withTimeout(
            framer.publish(),
            timeoutMs,
            "publishProject",
          );
        } catch (error) {
          const formatted = formatFramerError("publish", error);
          log("error", "publish_failed", {
            projectUrl,
            attempt,
            ...framerIds(framer),
            elapsedMs: Date.now() - publishStartedAt,
            ...formatted.fields,
            ...framerErrorFields(error),
          });
          throw new Error(formatted.message);
        }

        const deploymentId = publishResult?.deployment?.id || "";
        log("info", "publish_result", {
          projectUrl,
          ...framerIds(framer),
          deploymentId,
          hostnamesCount: Array.isArray(publishResult?.hostnames) ? publishResult.hostnames.length : 0,
          elapsedMs: Date.now() - publishStartedAt,
          hostnames: Array.isArray(publishResult?.hostnames)
            ? publishResult.hostnames.map((hostname) => ({
              hostname: hostname.hostname,
              type: hostname.type,
              isPrimary: hostname.isPrimary,
              isPublished: hostname.isPublished,
              deploymentId: hostname.deploymentId,
            }))
            : [],
        });

        if (!deploymentId) {
          const message = "publish failed | missing deployment id in publish result";
          log("error", "publish_failed", {
            projectUrl,
            phase: "publish",
            errorMessage: message,
            publishResult: safeJson(publishResult),
          });
          throw new Error(message);
        }

        log("info", "deploy_running", { projectUrl, ...framerIds(framer), deploymentId });
        const deployStartedAt = Date.now();
        try {
          await withTimeout(framer.deploy(deploymentId), timeoutMs, "deployProject");
        } catch (error) {
          const formatted = formatFramerError("deploy", error);
          log("error", "deploy_failed", {
            projectUrl,
            deploymentId,
            attempt,
            ...framerIds(framer),
            elapsedMs: Date.now() - deployStartedAt,
            ...formatted.fields,
            ...framerErrorFields(error),
          });
          throw new Error(formatted.message);
        }
        log("info", "deploy_complete", {
          projectUrl,
          deploymentId,
          ...framerIds(framer),
          elapsedMs: Date.now() - deployStartedAt,
        });

        return { published: true, changeCount: 1, deploymentId, message: `Successfully published and deployed` };
      }, `publishProject:outerAttempt:${attempt}`);
      return result;
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message : String(error);
      // Don't retry on auth/permission errors
      if (/not authenticated|permission|not authorized/i.test(message)) throw error;
      if (attempt < maxAttempts) {
        log("warn", "publish_transient_retry", {
          projectUrl,
          attempt,
          maxAttempts,
          retryDelayMs,
          message,
          ...framerErrorFields(error),
        });
        await sleep(retryDelayMs);
      }
    }
  }

  throw lastError;
}
