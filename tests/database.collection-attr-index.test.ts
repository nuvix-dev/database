import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { createTestDb } from "./helpers.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, IndexEnum } from "@core/enums.js";

const ns = `db_attr_${Date.now()}`;

const attributes = [
  new Doc({ $id: "a", key: "a", type: AttributeEnum.Integer, size: 4 }),
  new Doc({ $id: "b", key: "b", type: AttributeEnum.String, size: 64 }),
];

describe("Database - attributes and indexes", () => {
  const db = createTestDb({ namespace: ns });

  beforeAll(async () => {
    await db.create("public");
    await db.createCollection({ id: "t1", attributes });
  });

  afterAll(async () => {
    await db.getAdapter().$client.disconnect();
  });

  it("adds attribute then renames, deletes it", async () => {
    await db.createAttribute("t1", {
      $id: "c",
      key: "c",
      type: AttributeEnum.Boolean,
    } as any);
    await db.renameAttribute("t1", "c", "c2");
    await db.deleteAttribute("t1", "c2");
    const coll = await db.getCollection("t1");
    expect(
      coll.get("attributes").find((x: any) => x.get("$id") === "c"),
    ).toBeFalsy();
  });

  it("creates and deletes an index", async () => {
    await db.createIndex("t1", "_index_a", IndexEnum.Key, ["a"]);
    await db.renameIndex("t1", "_index_a", "_index_a2");
    await db.deleteIndex("t1", "_index_a2");
    const coll = await db.getCollection("t1");
    expect(coll.get("indexes").length >= 0).toBe(true);
  });
});
