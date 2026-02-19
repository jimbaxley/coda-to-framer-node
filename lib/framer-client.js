import {
  sleep,
  parseIntEnv,
  computeBackoffMs,
  isRetryableFramerError,
} from "./retry-policy.js";

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

function isSessionExpiredError(error) {
  if (!error || typeof error !== "object") return false;
  const maybeError = error;
  const message = String(maybeError.message || "").toLowerCase();
  const code = String(maybeError.code || "").toUpperCase();
  return code === "PROJECT_CLOSED" || message.includes("session expired");
}

function getConnectTimeoutMs() {
  const parsed = Number(process.env.FRAMER_CONNECT_TIMEOUT_MS || 0);
  if (Number.isFinite(parsed) && parsed >= 5000) {
    return Math.min(parsed, 30000);
  }
  return 30000;
}

async function connectWithTimeout(projectUrl, apiKey) {
  const { connect } = await import("framer-api");
  const timeoutMs = getConnectTimeoutMs();

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
    return await Promise.race([connect(projectUrl, apiKey), timeoutPromise]);
  } finally {
    clearTimeout(timer);
  }
}

async function runWithConnection(projectUrl, apiKey, fn) {
  const framer = await connectWithTimeout(projectUrl, apiKey);
  try {
    return await fn(framer);
  } finally {
    try {
      await framer.disconnect();
    } catch {
      // ignore disconnect errors
    }
  }
}

function withTimeout(promise, timeoutMs, operationName) {
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

function getOperationTimeoutMs() {
  const parsed = Number(process.env.FRAMER_OPERATION_TIMEOUT_MS || 0);
  if (Number.isFinite(parsed) && parsed >= 5000) {
    return Math.min(parsed, 120000);
  }
  return 90000;
}

function getPublishTimeoutMs() {
  const parsed = Number(process.env.FRAMER_PUBLISH_TIMEOUT_MS || 0);
  if (Number.isFinite(parsed) && parsed >= 10000) {
    return Math.min(parsed, 300000);
  }
  return 180000;
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

async function withFramer(projectUrl, apiKey, fn) {
  const maxAttempts = parseIntEnv(process.env.FRAMER_RETRY_ATTEMPTS || 3, 3, 1, 6);
  const baseDelayMs = parseIntEnv(process.env.FRAMER_RETRY_DELAY_MS || 1000, 1000, 0, 10000);
  let lastError;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await runWithConnection(projectUrl, apiKey, fn);
    } catch (error) {
      lastError = error;
      const canRetry = isRetryableFramerError(error);
      if (!canRetry || attempt === maxAttempts) {
        break;
      }

      const message = error instanceof Error ? error.message : String(error);
      const waitMs = computeBackoffMs(baseDelayMs, attempt);
      log("warn", "transient_connection_retry", {
        attempt,
        maxAttempts,
        waitMs,
        message,
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
    return {
      collectionId: created.id,
      collectionName: created.name,
      created: true,
    };
  });
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
  });
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
  });
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
  });
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
  });
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
  });
}
export async function publishProject(projectUrl, apiKey) {
  log("info", "publish_start", { projectUrl });
  const timeoutMs = getPublishTimeoutMs();
  return withFramer(projectUrl, apiKey, async (framer) => {
    log("info", "publish_connected", { projectUrl });
    
    // Don't check for changes - if publish is requested, we should publish
    // Collection changes don't always show up in getChangedPaths()
    log("info", "publish_running", { projectUrl });
    const publishResult = await withTimeout(
      framer.publish(),
      timeoutMs,
      "publishProject",
    );
    log("info", "publish_result", {
      projectUrl,
      deploymentId: publishResult?.deployment?.id || "",
      hostnamesCount: Array.isArray(publishResult?.hostnames) ? publishResult.hostnames.length : 0,
    });
    
    log("info", "deploy_running", {
      projectUrl,
      deploymentId: publishResult.deployment.id,
    });
    await withTimeout(
      framer.deploy(publishResult.deployment.id),
      timeoutMs,
      "deployProject",
    );
    log("info", "deploy_complete", {
      projectUrl,
      deploymentId: publishResult.deployment.id,
    });

    return {
      published: true,
      changeCount: 1,
      deploymentId: publishResult.deployment.id,
      message: `Successfully published and deployed`,
    };
  });
}