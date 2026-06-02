import { marked } from "marked";
import sanitizeHtml from "sanitize-html";

const ALLOWED_TAGS = [
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "p",
  "a",
  "ul",
  "ol",
  "li",
  "strong",
  "em",
  "img",
  "table",
  "thead",
  "tbody",
  "tr",
  "th",
  "td",
  "blockquote",
  "code",
  "pre",
  "br",
  "hr",
  "span",
];

function markdownToSanitizedHtml(markdown) {
  // Strip outer backticks that may have been pasted in (not actual code blocks)
  let cleanedMarkdown = markdown;
  
  // Remove triple backticks that wrap the entire content
  cleanedMarkdown = cleanedMarkdown.replace(/^```\n?/, "").replace(/\n?```$/, "");
  
  // Remove single backticks that wrap the entire content
  if (cleanedMarkdown.startsWith("`") && cleanedMarkdown.endsWith("`") && !cleanedMarkdown.startsWith("```")) {
    cleanedMarkdown = cleanedMarkdown.slice(1, -1);
  }
  
  const rawHtml = marked.parse(cleanedMarkdown, { async: false });
  return sanitizeHtml(rawHtml, {
    allowedTags: ALLOWED_TAGS,
    allowedAttributes: {
      a: ["href", "name", "target", "rel"],
      img: ["src", "alt", "title", "width", "height"],
      th: ["colspan", "rowspan", "style"],
      td: ["colspan", "rowspan", "style"],
      span: ["style"],
      p: ["style"],
      table: ["style"],
      tr: ["style"],
      thead: ["style"],
      tbody: ["style"],
    },
    allowedSchemes: ["http", "https", "mailto"],
  });
}

function extractMeaningfulText(item) {
  if (typeof item === "string") return item;
  if (item && typeof item === "object" && !Array.isArray(item)) {
    const obj = item;
    // Coda lookup/relation ref objects — use the row identifier, not the display name
    if (obj.type === "ref" && typeof obj.identifier === "string") return obj.identifier;
    return (
      (typeof obj.displayValue === "string" ? obj.displayValue : "") ||
      (typeof obj.value === "string" ? obj.value : "") ||
      (typeof obj.name === "string" ? obj.name : "") ||
      JSON.stringify(item)
    );
  }
  return String(item);
}

function stripMarkdown(text) {
  let newText = text;
  newText = newText.replace(/\[([^\]]+)]\(([^)]+)\)/g, (_match, linkText) => {
    return linkText;
  });
  newText = newText.replace(/```([\s\S]*?)```/g, (_match, group1) => group1.trim());
  newText = newText.replace(/`([^`]*)`/g, (_match, group1) => group1.trim());
  return newText.trim();
}

function extractUrlFromMarkdown(text) {
  const mdImg = text.match(/!\[[^\]]*\]\(([^)]+)\)/);
  if (mdImg && mdImg[1]) return mdImg[1].trim();
  const triple = text.match(/^```([\s\S]*?)```$/);
  if (triple && typeof triple[1] === "string") return triple[1].trim();
  const single = text.match(/^`([^`]*)`$/);
  if (single && typeof single[1] === "string") return single[1].trim();
  return text.trim();
}

function isValidAssetUrl(url) {
  const trimmed = url.trim();
  return (
    trimmed.startsWith("http://") ||
    trimmed.startsWith("https://") ||
    trimmed.includes("codahosted.io/")
  );
}

function isLikelyImageUrl(url) {
  if (typeof url !== "string") return false;
  const trimmed = url.trim();
  return (
    /^https?:\/\/[\S]+\.(jpe?g|png|gif|webp|svg|bmp|tiff?|ico|apng|avif)(\?.*)?$/i.test(
      trimmed,
    ) || /^https?:\/\/codahosted\.io\//.test(trimmed)
  );
}

// Takes the full allMappedFields array and a map of { codaTableId → framerCollectionId }.
// Returns fields suitable for setFields, with lookup fields promoted to
// multiCollectionReference/collectionReference when their collectionId can be resolved.
export function resolveReferenceFields(allMappedFields, codaTableIdToFramerCollectionId) {
  return allMappedFields
    .map((field) => {
      if (field.type !== "lookup") return field;
      const framerCollectionId = field.codaRefTableId
        ? codaTableIdToFramerCollectionId.get(field.codaRefTableId)
        : null;
      if (!framerCollectionId) return null; // skip if unresolvable
      return {
        id: field.id,
        name: field.name,
        type: field.multiple ? "multiCollectionReference" : "collectionReference",
        collectionId: framerCollectionId,
      };
    })
    .filter(Boolean);
}

export function normalizeColumns(columns) {
  return columns.map((column) => ({
    ...column,
    id: String(column.id),
    name: String(column.name ?? column.id),
    format: column.format ?? { type: "text" },
  }));
}

export function normalizeRows(rows) {
  return rows.map((row) => {
    if (!row || typeof row !== "object") {
      return { values: {} };
    }
    const obj = row;
    const id = String(obj.id || "");
    const values = (typeof obj.values === "object" ? obj.values : obj) || {};
    return { id: id || undefined, values };
  });
}

export function mapCodaTypeToFramerType(column) {
  const baseType = column.format.type.toLowerCase();
  const name = column.name.toLowerCase();
  const id = column.id.toLowerCase();

  if (baseType === "button") {
    return null;
  }

  if (
    baseType === "image" ||
    name.includes("image") ||
    name.includes("graphic") ||
    id.includes("image") ||
    id.includes("graphic")
  ) {
    return {
      id: column.id,
      name: column.name,
      type: "image",
    };
  }

  if (baseType === "text" || baseType === "email" || baseType === "phone") {
    return {
      id: column.id,
      name: column.name,
      type: "string",
    };
  }

  if (baseType === "number" || baseType === "percent" || baseType === "currency") {
    return {
      id: column.id,
      name: column.name,
      type: "number",
    };
  }

  if (baseType === "checkbox" || baseType === "boolean") {
    return {
      id: column.id,
      name: column.name,
      type: "boolean",
    };
  }

  if (baseType === "date") {
    return {
      id: column.id,
      name: column.name,
      type: "date",
    };
  }

  if (baseType === "datetime") {
    return {
      id: column.id,
      name: column.name,
      type: "string",
    };
  }

  if (baseType === "duration" || baseType === "time") {
    return {
      id: column.id,
      name: column.name,
      type: "string",
    };
  }

  if (baseType === "canvas" || baseType === "richtext" || baseType === "formattedtext") {
    return {
      id: column.id,
      name: column.name,
      type: "formattedText",
    };
  }

  if (baseType === "url" || baseType === "link") {
    return {
      id: column.id,
      name: column.name,
      type: "link",
    };
  }

  if (baseType === "lookup" || baseType === "relation") {
    return {
      id: column.id,
      name: column.name,
      type: "lookup",
      multiple: column.format?.multiple === true,
      // Coda table ID of the referenced table — used to resolve Framer collectionId at sync time
      codaRefTableId: column.format?.objectId || null,
    };
  }

  if (baseType === "file") {
    return {
      id: column.id,
      name: column.name,
      type: "file",
    };
  }

  return {
    id: column.id,
    name: column.name,
    type: "string",
  };
}

function extractSlugValue(value) {
  if (typeof value === "string") return stripMarkdown(value).trim().toLowerCase();
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value).trim().toLowerCase();
  }
  if (typeof value === "object" && value !== null) {
    if ("value" in value) {
      const wrapped = value.value;
      if (typeof wrapped === "string") return stripMarkdown(wrapped).trim().toLowerCase();
      if (typeof wrapped === "number" || typeof wrapped === "boolean") {
        return String(wrapped).trim().toLowerCase();
      }
    }
    const extracted = extractMeaningfulText(value);
    if (extracted) return stripMarkdown(extracted).trim().toLowerCase();
  }
  return null;
}

function formatTimeValue(value, use12HourTime) {
  try {
    let hours;
    let minutes;
    let secondsVal;
    let parsed = false;

    if (typeof value === "string") {
      // First try to extract time from ISO string with timezone (HH:MM:SS with optional .mmm and ±HH:MM offset)
      const isoTimeMatch = value.match(/T(\d{2}):(\d{2}):(\d{2})(?:\.\d{3})?(?:[+-]\d{2}:\d{2})?/);
      if (isoTimeMatch) {
        hours = parseInt(isoTimeMatch[1], 10);
        minutes = parseInt(isoTimeMatch[2], 10);
        secondsVal = parseInt(isoTimeMatch[3], 10);
        parsed = true;
      } else {
        // Try simple HH:MM:SS format
        const timeOnlyMatch = value.match(/^([0-1]?\d|2[0-3]):([0-5]\d)(:([0-5]\d))?$/);
        if (timeOnlyMatch) {
          hours = parseInt(timeOnlyMatch[1] || "0", 10);
          minutes = parseInt(timeOnlyMatch[2] || "0", 10);
          secondsVal = timeOnlyMatch[4] ? parseInt(timeOnlyMatch[4], 10) : 0;
          parsed = true;
        }
      }
      
      // If no regex match, try Date parsing as fallback
      if (!parsed) {
        const dateObj = new Date(value);
        if (!Number.isNaN(dateObj.getTime())) {
          hours = dateObj.getHours();
          minutes = dateObj.getMinutes();
          secondsVal = dateObj.getSeconds();
          parsed = true;
        }
      }
    } else if (value instanceof Date) {
      hours = value.getHours();
      minutes = value.getMinutes();
      secondsVal = value.getSeconds();
      parsed = true;
    }

    if (!parsed || hours === undefined || minutes === undefined || secondsVal === undefined) {
      return null;
    }

    if (use12HourTime) {
      const ampm = hours >= 12 ? "PM" : "AM";
      const formattedHours = hours % 12 || 12;
      let formattedTime = `${formattedHours}:${String(minutes).padStart(2, "0")}`;
      if (secondsVal > 0) {
        formattedTime += `:${String(secondsVal).padStart(2, "0")}`;
      }
      return `${formattedTime} ${ampm}`;
    }

    let formattedTime = `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
    if (secondsVal > 0) {
      formattedTime += `:${String(secondsVal).padStart(2, "0")}`;
    }
    return formattedTime;
  } catch {
    return null;
  }
}

function transformCodaValue(value, field, codaColumnType, use12HourTime) {
  if (
    value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "")
  ) {
    return null;
  }

  switch (field.type) {
    case "number": {
      if (typeof value === "number") return value;
      if (typeof value === "string") {
        const parsed = Number(value.replace(/[$£€¥]/g, "").replace(/,/g, ""));
        return Number.isNaN(parsed) ? null : parsed;
      }
      return null;
    }
    case "boolean":
      return Boolean(value);
    case "date": {
      try {
        let dateValue = value;
        if (typeof value === "object" && value !== null && "value" in value) {
          dateValue = value.value;
        }
        
        const dateStr = String(dateValue);
        const dateObj = new Date(dateStr);
        if (Number.isNaN(dateObj.getTime())) return null;

        if (codaColumnType === "date") {
          // Date only: YYYY-MM-DD in UTC
          const year = dateObj.getUTCFullYear();
          const month = String(dateObj.getUTCMonth() + 1).padStart(2, "0");
          const day = String(dateObj.getUTCDate()).padStart(2, "0");
          return `${year}-${month}-${day}`;
        }

        if (codaColumnType === "datetime") {
          // DateTime: treat local time as UTC (add Z suffix)
          const year = dateObj.getFullYear();
          const month = String(dateObj.getMonth() + 1).padStart(2, "0");
          const day = String(dateObj.getDate()).padStart(2, "0");
          const hours = String(dateObj.getHours()).padStart(2, "0");
          const minutes = String(dateObj.getMinutes()).padStart(2, "0");
          const seconds = String(dateObj.getSeconds()).padStart(2, "0");
          const ms = String(dateObj.getMilliseconds()).padStart(3, "0");
          return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.${ms}Z`;
        }

        // Fallback to ISO string
        return dateObj.toISOString();
      } catch {
        return null;
      }
    }
    case "formattedText": {
      if (codaColumnType === "canvas" || codaColumnType === "richtext") {
        const markdown = typeof value === "string" ? value : JSON.stringify(value);
        return markdownToSanitizedHtml(markdown);
      }
      return String(value);
    }
    case "string": {
      if (codaColumnType === "time") {
        const formatted = formatTimeValue(value, use12HourTime);
        if (formatted) return formatted;
      }
      let textValue = "";
      if (Array.isArray(value) || (value && typeof value === "object" && "rawValue" in value)) {
        const rawArray = Array.isArray(value)
          ? value
          : Array.isArray((value).rawValue)
            ? (value).rawValue
            : [];
        textValue = (rawArray ?? []).map(extractMeaningfulText).filter(Boolean).join(", ");
      } else {
        textValue = extractMeaningfulText(value);
      }
      return stripMarkdown(textValue);
    }
    case "image": {
      let imageUrl = "";
      if (typeof value === "string") {
        imageUrl = extractUrlFromMarkdown(value);
      } else if (typeof value === "object" && value !== null) {
        if (Array.isArray(value)) {
          for (const item of value) {
            if (item && typeof item === "object") {
              const obj = item;
              if (obj["@type"] === "ImageObject") {
                const urls = [
                  obj.url,
                  obj.contentUrl,
                  obj.thumbnailUrl,
                ].filter((url) => typeof url === "string");
                for (const url of urls) {
                  if (isValidAssetUrl(url) || isLikelyImageUrl(url)) {
                    imageUrl = url;
                    break;
                  }
                }
              }
            }
            if (imageUrl) break;
          }
        } else {
          const obj = value;
          if (obj["@type"] === "ImageObject") {
            const possibleUrls = [obj.url, obj.contentUrl, obj.thumbnailUrl].filter(
              (url) => typeof url === "string",
            );
            for (const url of possibleUrls) {
              if (isValidAssetUrl(url) || isLikelyImageUrl(url)) {
                imageUrl = url;
                break;
              }
            }
          }
        }

        if (typeof value === "object" && value !== null) {
          const obj = value;
          if (!imageUrl && typeof obj.url === "string") {
            imageUrl = obj.url;
          } else if (!imageUrl && typeof obj.link === "string") {
            imageUrl = obj.link;
          } else if (!imageUrl && typeof obj.value === "string") {
            imageUrl = extractUrlFromMarkdown(obj.value);
          } else if (!imageUrl && typeof obj.rawValue === "string") {
            imageUrl = extractUrlFromMarkdown(obj.rawValue);
          } else if (!imageUrl && typeof obj.imageUrl === "string") {
            imageUrl = obj.imageUrl;
          } else if (!imageUrl && typeof obj.thumbnailUrl === "string") {
            imageUrl = obj.thumbnailUrl;
          }
        }

        if (!imageUrl && typeof value === "object" && value !== null) {
          const obj = value;
          if (obj.linkedRow && typeof obj.linkedRow === "object") {
            const linkedRow = obj.linkedRow;
            if (typeof linkedRow.url === "string") {
              imageUrl = linkedRow.url;
            } else if (typeof linkedRow.imageUrl === "string") {
              imageUrl = linkedRow.imageUrl;
            }
          }
        }
      }

      if (imageUrl && (isValidAssetUrl(imageUrl) || isLikelyImageUrl(imageUrl))) {
        return imageUrl.trim();
      }
      return null;
    }
    case "file": {
      let fileUrl = "";
      if (typeof value === "string") fileUrl = extractUrlFromMarkdown(value);
      if (typeof value === "object" && value !== null) {
        const obj = value;
        if (typeof obj.url === "string") fileUrl = obj.url;
        if (typeof obj.link === "string") fileUrl = obj.link;
      }
      return fileUrl ? fileUrl.trim() : null;
    }
    case "link": {
      if (typeof value === "string") return value;
      if (typeof value === "object" && value !== null && typeof value.url === "string") {
        return value.url;
      }
      return null;
    }
    case "lookup": {
      // Extract Coda row identifiers from lookup/relation ref objects.
      // These become the Framer item IDs since the sync uses Coda row IDs as Framer item IDs.
      const refs = Array.isArray(value) ? value : [value];
      const ids = refs
        .map((ref) => {
          if (ref && typeof ref === "object" && ref.type === "ref" && typeof ref.identifier === "string") {
            return ref.identifier;
          }
          if (typeof ref === "string") return ref;
          return null;
        })
        .filter(Boolean);
      return ids.length > 0 ? ids : null;
    }
    default:
      return value;
  }
}

export function buildFieldsAndItems({
  columns,
  rows,
  slugFieldId,
  use12HourTime,
}) {
  const warnings = [];

  const allMappedFields = columns
    .map((column) => mapCodaTypeToFramerType(column))
    .filter((field) => field !== null);

  // Lookup fields need their collectionId resolved at sync time (done in api/sync.js).
  // Include them in fields so setFields can create the reference field in Framer,
  // but only after collectionId has been resolved via resolveCollectionIds().
  // Until resolved they carry type "lookup" and are filtered out of setFields.
  const fields = allMappedFields.filter((field) => field.type !== "lookup");

  // Include lookup fields when building item fieldData
  const fieldMap = new Map(allMappedFields.map((field) => [field.id, field]));
  const codaColumnTypeMap = new Map(
    columns.map((column) => [column.id, column.format.type.toLowerCase()]),
  );
  
  // Map column ID to column name (for row value lookups)
  const columnIdToNameMap = new Map(
    columns.map((column) => [column.id, column.name]),
  );
  const columnNameToIdMap = new Map(
    columns.map((column) => [column.name, column.id]),
  );
  const slugColumnName = columnIdToNameMap.get(slugFieldId);
  if (!slugColumnName) {
    warnings.push(`Slug field ID ${slugFieldId} not found in columns`);
  }

  let skippedCount = 0;
  const items = [];

  rows.forEach((row, index) => {
    const rowId = row.id;
    if (!rowId) {
      skippedCount += 1;
      warnings.push(`Row at index ${index} is missing a row id and was skipped.`);
      return;
    }

    const slugFieldValue = slugColumnName ? row.values[slugColumnName] : undefined;
    let slugValue = extractSlugValue(slugFieldValue);
    if (!slugValue && rowId) {
      slugValue = String(rowId).trim().toLowerCase();
      warnings.push(`Row ${rowId} slug missing or empty, falling back to rowId as slug.`);
    }
    if (!slugValue) {
      skippedCount += 1;
      const warning = `Row ${rowId} slug extraction failed and rowId fallback missing. slugColumnName=${slugColumnName}, slugFieldValue=${JSON.stringify(slugFieldValue)}`;
      warnings.push(warning);
      console.log(`[mapping] Row skipped:`, warning);
      return;
    }

    const fieldData = {};
    for (const [columnName, value] of Object.entries(row.values)) {
      const columnId = columnNameToIdMap.get(columnName);
      if (!columnId) continue;
      
      const field = fieldMap.get(columnId);
      if (!field) continue;
      const codaType = codaColumnTypeMap.get(columnId) || "text";
      const transformed = transformCodaValue(value, field, codaType, use12HourTime);
      if (transformed !== null) {
        let framerType = field.type;
        if (field.type === "lookup") {
          framerType = field.multiple ? "multiCollectionReference" : "collectionReference";
        }
        fieldData[columnId] = { type: framerType, value: transformed };
      }
    }

    items.push({
      id: rowId,
      slug: slugValue,
      draft: false,
      fieldData,
    });
  });

  return { fields, allMappedFields, items, warnings, skippedCount };
}
