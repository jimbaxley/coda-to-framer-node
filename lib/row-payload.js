function isPlainObject(value) {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}

function slugifyFieldId(name) {
  return String(name || "")
    .trim()
    .replace(/[^A-Za-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function normalizePayloadField(name, input) {
  if (isPlainObject(input) && ("value" in input || "type" in input || "id" in input)) {
    const typed = input;
    const fieldName = String(typed.name || name || "").trim();
    const fieldId = String(typed.id || slugifyFieldId(fieldName) || fieldName).trim();
    const codaType = String(typed.type || typed.codaType || "text").trim().toLowerCase();
    const value = "value" in typed ? typed.value : undefined;
    const tableId = typed.tableId || typed.referenceTableId;

    return {
      column: {
        id: fieldId,
        name: fieldName,
        format: {
          type: codaType,
          ...(tableId ? { table: { id: String(tableId) } } : {}),
        },
      },
      value,
    };
  }

  const fieldName = String(name || "").trim();
  return {
    column: {
      id: slugifyFieldId(fieldName) || fieldName,
      name: fieldName,
      format: { type: inferCodaType(input) },
    },
    value: input,
  };
}

function inferCodaType(value) {
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "checkbox";
  return "text";
}

export function buildCodaLikeTableDataFromRowPayload(rowPayload) {
  if (!isPlainObject(rowPayload)) {
    throw new Error("rowPayload must be a JSON object.");
  }

  const values = rowPayload.values;
  if (!isPlainObject(values)) {
    throw new Error("rowPayload.values must be a JSON object.");
  }

  const rowId = String(rowPayload.rowId || rowPayload.id || "").trim();
  if (!rowId) {
    throw new Error("rowPayload.rowId or rowPayload.id is required.");
  }

  const columns = [];
  const rowValues = {};
  for (const [name, value] of Object.entries(values)) {
    const normalized = normalizePayloadField(name, value);
    if (!normalized.column.name || !normalized.column.id) continue;
    columns.push(normalized.column);
    rowValues[normalized.column.name] = normalized.value;
  }

  return {
    columns,
    rows: [
      {
        id: rowId,
        values: rowValues,
      },
    ],
  };
}

export function buildReferenceMapFromRowPayload(rowPayload) {
  const values = isPlainObject(rowPayload) && isPlainObject(rowPayload.values)
    ? rowPayload.values
    : {};
  const referenceMap = new Map();

  for (const value of Object.values(values)) {
    if (!isPlainObject(value)) continue;
    const tableId = value.tableId || value.referenceTableId;
    const collectionId = value.collectionId || value.framerCollectionId;
    if (tableId && collectionId) {
      referenceMap.set(String(tableId), String(collectionId));
    }
  }

  return referenceMap;
}
