function isSessionExpiredError(error) {
  if (!error || typeof error !== "object") return false;
  const maybeError = error;
  const message = String(maybeError.message || "").toLowerCase();
  const code = String(maybeError.code || "").toUpperCase();
  return code === "PROJECT_CLOSED" || message.includes("session expired");
}

async function runWithConnection(projectUrl, apiKey, fn) {
  const { connect } = await import("framer-api");
  const framer = await connect(projectUrl, apiKey);
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
    return parsed;
  }
  return 90000;
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

async function withFramer(projectUrl, apiKey, fn) {
  try {
    return await runWithConnection(projectUrl, apiKey, fn);
  } catch (error) {
    if (!isSessionExpiredError(error)) {
      throw error;
    }

    console.warn("[framer] Session expired. Retrying once with a fresh connection...");
    try {
      return await runWithConnection(projectUrl, apiKey, fn);
    } catch (retryError) {
      if (isSessionExpiredError(retryError)) {
        throw new Error(
          "Framer session expired. Check that FRAMER_API_KEY is valid and regenerate it in Framer if needed.",
        );
      }
      throw retryError;
    }
  }
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

export async function getCollectionFieldIds(
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

    let fieldIds = getFieldIdsFromCollection(collection);

    if (fieldIds.length === 0 && typeof collection.getFields === "function") {
      const fetchedFields = await withTimeout(
        collection.getFields(),
        timeoutMs,
        "getCollectionFields",
      );
      fieldIds = (Array.isArray(fetchedFields) ? fetchedFields : [])
        .map((field) => (field && typeof field === "object" ? field.id : null))
        .filter((id) => typeof id === "string" && id.length > 0);
    }

    return Array.from(new Set(fieldIds));
  });
}

export async function addItemsToCollection(
  projectUrl,
  apiKey,
  collectionId,
  items,
) {
  if (!items || items.length === 0) return;
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
    let pendingItems = items;
    const removedFieldKeys = new Set();

    for (let attempt = 1; attempt <= 5; attempt += 1) {
      try {
        await withTimeout(
          collection.addItems(pendingItems),
          timeoutMs,
          `addItemsToCollection (attempt ${attempt})`,
        );
        if (removedFieldKeys.size > 0) {
          console.warn(
            `[framer] addItems succeeded after removing unknown fields: ${Array.from(removedFieldKeys).join(", ")}`,
          );
        }
        return;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);

        if (message.includes("timed out") && attempt === 1) {
          console.warn("[framer] addItems timed out. Retrying once...");
          continue;
        }

        const missingFieldKey = extractMissingFieldKey(message);
        if (missingFieldKey && !removedFieldKeys.has(missingFieldKey)) {
          removedFieldKeys.add(missingFieldKey);
          console.warn(
            `[framer] Field ${missingFieldKey} not present in collection. Retrying without it...`,
          );
          pendingItems = stripFieldFromItems(pendingItems, missingFieldKey);
          continue;
        }

        throw error;
      }
    }

    throw new Error("Failed to add items after retries.");
  });
}
export async function publishProject(projectUrl, apiKey) {
  console.log(`[publishProject] Starting publish for: ${projectUrl}`);
  const timeoutMs = getOperationTimeoutMs();
  return withFramer(projectUrl, apiKey, async (framer) => {
    console.log(`[publishProject] Connected to Framer, publishing...`);
    
    // Don't check for changes - if publish is requested, we should publish
    // Collection changes don't always show up in getChangedPaths()
    console.log(`[publishProject] Publishing project...`);
    const publishResult = await withTimeout(
      framer.publish(),
      timeoutMs,
      "publishProject",
    );
    console.log(`[publishProject] Publish result:`, publishResult);
    
    console.log(`[publishProject] Deploying...`);
    await withTimeout(
      framer.deploy(publishResult.deployment.id),
      timeoutMs,
      "deployProject",
    );
    console.log(`[publishProject] Deployment complete: ${publishResult.deployment.id}`);

    return {
      published: true,
      changeCount: 1,
      deploymentId: publishResult.deployment.id,
      message: `Successfully published and deployed`,
    };
  });
}