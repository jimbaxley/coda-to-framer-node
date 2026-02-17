async function withFramer(projectUrl, apiKey, fn) {
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

export async function getOrCreateManagedCollection(
  projectUrl,
  apiKey,
  collectionName,
) {
  return withFramer(projectUrl, apiKey, async (framer) => {
    const collections = await framer.getManagedCollections();
    const existing = collections.find((item) => item.name === collectionName);
    if (existing) {
      return {
        collectionId: existing.id,
        collectionName: existing.name,
        created: false,
      };
    }
    const created = await framer.createManagedCollection(collectionName);
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
  return withFramer(projectUrl, apiKey, async (framer) => {
    const collections = await framer.getManagedCollections();
    const collection = collections.find((item) => item.id === collectionId);
    if (!collection) {
      throw new Error("Managed collection not found.");
    }
    const compatibleFields = fields.filter(Boolean);
    await collection.setFields(compatibleFields);
    return compatibleFields.length;
  });
}

export async function addItemsToCollection(
  projectUrl,
  apiKey,
  collectionId,
  items,
) {
  if (!items || items.length === 0) return;
  return withFramer(projectUrl, apiKey, async (framer) => {
    const collections = await framer.getManagedCollections();
    const collection = collections.find((item) => item.id === collectionId);
    if (!collection) {
      throw new Error("Managed collection not found.");
    }
    await collection.addItems(items);
  });
}
export async function publishProject(projectUrl, apiKey) {
  console.log(`[publishProject] Starting publish for: ${projectUrl}`);
  return withFramer(projectUrl, apiKey, async (framer) => {
    console.log(`[publishProject] Connected to Framer, publishing...`);
    
    // Don't check for changes - if publish is requested, we should publish
    // Collection changes don't always show up in getChangedPaths()
    console.log(`[publishProject] Publishing project...`);
    const publishResult = await framer.publish();
    console.log(`[publishProject] Publish result:`, publishResult);
    
    console.log(`[publishProject] Deploying...`);
    await framer.deploy(publishResult.deployment.id);
    console.log(`[publishProject] Deployment complete: ${publishResult.deployment.id}`);

    return {
      published: true,
      changeCount: 1,
      deploymentId: publishResult.deployment.id,
      message: `Successfully published and deployed`,
    };
  });
}