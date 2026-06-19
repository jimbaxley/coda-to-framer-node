import assert from "node:assert/strict";
import { buildFieldsAndItems } from "../lib/mapping.js";
import {
  buildCodaLikeTableDataFromRowPayload,
  buildReferenceMapFromRowPayload,
} from "../lib/row-payload.js";

const payload = {
  id: "i-payload-test",
  rowId: "i-payload-test",
  slug: "payload-test-event",
  values: {
    Slug: { id: "slug", type: "text", value: "payload-test-event" },
    Title: { id: "title", type: "text", value: "Fresh payload title" },
    Capacity: { id: "capacity", type: "number", value: "125" },
    Live: { id: "live", type: "checkbox", value: true },
    StartDate: { id: "start_date", name: "Start Date", type: "date", value: "2026-06-19T15:30:00.000Z" },
    StartTime: { id: "start_time", name: "Start Time", type: "time", value: "2026-06-19T15:30:00.000Z" },
    Details: { id: "details", type: "canvas", value: "## Hello\n\n[TeamUP](https://teamup.example)" },
    SignupUrl: { id: "signup_url", name: "Signup URL", type: "url", value: "https://example.com/signup" },
    HeroImage: {
      id: "hero_image",
      name: "Hero Image",
      type: "image",
      value: { "@type": "ImageObject", url: "https://example.com/hero.png" },
    },
    Attachment: { id: "attachment", type: "file", value: { url: "https://example.com/file.pdf" } },
    Tags: {
      id: "tags",
      type: "selectList",
      value: [{ displayValue: "Canvass" }, { displayValue: "Training" }],
    },
    RelatedProducts: {
      id: "related_products",
      name: "Related Products",
      type: "reference",
      tableId: "grid-products",
      collectionId: "framer-products",
      value: [{ rowId: "product-a" }, { id: "product-b" }],
    },
  },
};

const tableData = buildCodaLikeTableDataFromRowPayload(payload);
const mappingResult = buildFieldsAndItems({
  columns: tableData.columns,
  rows: tableData.rows,
  slugFieldId: "slug",
  use12HourTime: true,
  referenceMap: buildReferenceMapFromRowPayload(payload),
});

assert.equal(mappingResult.items.length, 1);
assert.equal(mappingResult.skippedCount, 0);

const item = mappingResult.items[0];
assert.equal(item.id, "i-payload-test");
assert.equal(item.slug, "payload-test-event");

assert.equal(item.fieldData.title.type, "string");
assert.equal(item.fieldData.title.value, "Fresh payload title");
assert.equal(item.fieldData.capacity.type, "number");
assert.equal(item.fieldData.capacity.value, 125);
assert.equal(item.fieldData.live.type, "boolean");
assert.equal(item.fieldData.live.value, true);
assert.equal(item.fieldData.start_date.type, "date");
assert.equal(item.fieldData.start_date.value, "2026-06-19");
assert.equal(item.fieldData.start_time.type, "string");
assert.equal(item.fieldData.start_time.value, "3:30 PM");
assert.equal(item.fieldData.details.type, "formattedText");
assert.match(item.fieldData.details.value, /<h2>Hello<\/h2>/);
assert.equal(item.fieldData.signup_url.type, "link");
assert.equal(item.fieldData.signup_url.value, "https://example.com/signup");
assert.equal(item.fieldData.hero_image.type, "image");
assert.equal(item.fieldData.hero_image.value, "https://example.com/hero.png");
assert.equal(item.fieldData.attachment.type, "file");
assert.equal(item.fieldData.attachment.value, "https://example.com/file.pdf");
assert.equal(item.fieldData.tags.type, "string");
assert.equal(item.fieldData.tags.value, "Canvass, Training");
assert.equal(item.fieldData.related_products.type, "multiCollectionReference");
assert.deepEqual(item.fieldData.related_products.value, ["product-a", "product-b"]);

console.log(JSON.stringify({
  ok: true,
  fields: mappingResult.fields,
  item,
  warnings: mappingResult.warnings,
}, null, 2));
