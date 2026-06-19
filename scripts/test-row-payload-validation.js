import assert from "node:assert/strict";
import { buildFieldsAndItems } from "../lib/mapping.js";
import {
  buildCodaLikeTableDataFromRowPayload,
  buildReferenceMapFromRowPayload,
} from "../lib/row-payload.js";

function assertThrowsMessage(fn, pattern) {
  assert.throws(fn, (error) => {
    assert.match(error.message, pattern);
    return true;
  });
}

assertThrowsMessage(
  () => buildCodaLikeTableDataFromRowPayload(null),
  /rowPayload must be a JSON object/,
);

assertThrowsMessage(
  () => buildCodaLikeTableDataFromRowPayload({ id: "row-1", values: [] }),
  /rowPayload\.values must be a JSON object/,
);

assertThrowsMessage(
  () => buildCodaLikeTableDataFromRowPayload({ values: { Title: "No id" } }),
  /rowPayload\.rowId or rowPayload\.id is required/,
);

const emptyValuesTableData = buildCodaLikeTableDataFromRowPayload({
  id: "row-1",
  values: {
    Slug: { id: "slug", type: "text", value: "row-1" },
    Title: { id: "title", type: "text", value: "" },
    Capacity: { id: "capacity", type: "number", value: "not a number" },
    StartDate: { id: "start_date", name: "Start Date", type: "date", value: "not a date" },
  },
});

const mappingResult = buildFieldsAndItems({
  columns: emptyValuesTableData.columns,
  rows: emptyValuesTableData.rows,
  slugFieldId: "slug",
  use12HourTime: true,
  referenceMap: buildReferenceMapFromRowPayload({}),
});

assert.equal(mappingResult.items.length, 1);
assert.equal(mappingResult.items[0].slug, "row-1");
assert.equal(mappingResult.items[0].fieldData.title, undefined);
assert.equal(mappingResult.items[0].fieldData.capacity, undefined);
assert.equal(mappingResult.items[0].fieldData.start_date, undefined);

console.log(JSON.stringify({
  ok: true,
  checked: [
    "payload object validation",
    "values object validation",
    "row id validation",
    "empty/invalid converted values omitted",
  ],
}, null, 2));
