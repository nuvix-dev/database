import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Cache, Memory } from "@nuvix/cache";
import { describe, expect, test } from "bun:test";
import { SQLiteAdapter } from "@adapters/sqlite-adapter.js";
import { SQLiteSqlBuilder } from "@adapters/sqlite-sql-builder.js";
import { Database } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, IndexEnum } from "@core/enums.js";
import { Query } from "@core/query.js";
import { DuplicateException, LimitException } from "@errors/index.js";
import { createSQLiteTestDb } from "./sqlite-helpers.js";

const attributes = [
  new Doc({
    $id: "name",
    key: "name",
    type: AttributeEnum.String,
    size: 128,
    required: true,
  }),
  new Doc({
    $id: "email",
    key: "email",
    type: AttributeEnum.String,
    size: 255,
    required: true,
  }),
  new Doc({
    $id: "score",
    key: "score",
    type: AttributeEnum.Integer,
    required: true,
  }),
  new Doc({
    $id: "status",
    key: "status",
    type: AttributeEnum.String,
    size: 32,
    required: true,
  }),
];

async function setup(namespace: string): Promise<Database> {
  const database = createSQLiteTestDb({ namespace });
  await database.create("main");
  await database.createCollection({
    id: "items",
    attributes,
    documentSecurity: false,
  });
  return database;
}

function fileDatabase(path: string): Database {
  const adapter = new SQLiteAdapter(path);
  adapter.setMeta({
    schema: "main",
    namespace: "persistence",
    sharedTables: false,
    tenantPerDocument: false,
  });
  return new Database(adapter, new Cache(new Memory()));
}

describe("SQLite schema, CRUD, bulk operations, and indexes", () => {
  test("initializes the database and completes collection and attribute lifecycle", async () => {
    // Arrange
    const database = createSQLiteTestDb({ namespace: "schema_lifecycle" });

    try {
      // Act
      await database.create("main");
      const collection = await database.createCollection({
        id: "products",
        attributes: [attributes[0]!],
      });
      const created = await database.createAttribute("products", {
        $id: "temporary",
        key: "temporary",
        type: AttributeEnum.String,
        size: 64,
      });
      const removed = await database.createAttribute("products", {
        $id: "removed",
        key: "removed",
        type: AttributeEnum.String,
        size: 64,
      });
      const deletedAttribute = await database.deleteAttribute(
        "products",
        "removed",
      );
      const afterDelete = await database.getAdapter().getSchemaAttributes(
        "products",
      );

      // Assert
      expect(await database.exists("main")).toBe(true);
      expect(collection.getId()).toBe("products");
      expect(created).toBe(true);
      expect(removed).toBe(true);
      expect(deletedAttribute).toBe(true);
      expect(afterDelete.map((column) => column.getId())).not.toContain(
        "removed",
      );

      // Act
      const renamed = await database.renameAttribute(
        "products",
        "temporary",
        "description",
      );
      const afterRename = await database.getAdapter().getSchemaAttributes(
        "products",
      );
      const deletedRenamedAttribute = await database.deleteAttribute(
        "products",
        "description",
      );
      const afterRenamedDelete = await database.getAdapter().getSchemaAttributes(
        "products",
      );
      const deletedCollection = await database.deleteCollection("products");

      // Assert
      expect(renamed).toBe(true);
      expect(afterRename.map((column) => column.getId())).toContain(
        "description",
      );
      expect(deletedRenamedAttribute).toBe(true);
      expect(afterRenamedDelete.map((column) => column.getId())).not.toContain(
        "description",
      );
      expect(deletedCollection).toBe(true);
      expect((await database.getCollection("products")).empty()).toBe(true);
      expect(await database.exists("main", "products")).toBe(false);
    } finally {
      await database.getAdapter().$client.disconnect();
    }
  });

  test("creates physical key and unique indexes and rejects duplicate values", async () => {
    // Arrange
    const namespace = "index_behavior";
    const database = await setup(namespace);
    const session = database.system();
    const keyIndex = SQLiteSqlBuilder.getIndexName(
      { schema: "main", namespace },
      "items",
      "status_key",
    );
    const uniqueIndex = SQLiteSqlBuilder.getIndexName(
      { schema: "main", namespace },
      "items",
      "email_unique",
    );

    try {
      // Act
      const keyCreated = await database.createIndex(
        "items",
        "status_key",
        IndexEnum.Key,
        ["status"],
      );
      const uniqueCreated = await database.createIndex(
        "items",
        "email_unique",
        IndexEnum.Unique,
        ["email"],
      );
      const physicalKey = await database
        .getAdapter()
        .$client.query<{ name: string }>(
          "SELECT name FROM sqlite_schema WHERE type = 'index' AND name = ?",
          [keyIndex],
        );
      const physicalUnique = await database
        .getAdapter()
        .$client.query<{ name: string }>(
          "SELECT name FROM sqlite_schema WHERE type = 'index' AND name = ?",
          [uniqueIndex],
        );
      await session.createDocument(
        "items",
        new Doc({
          $id: "first",
          name: "First",
          email: "same@example.test",
          score: 1,
          status: "active",
        }),
      );
      const duplicate = session.createDocument(
        "items",
        new Doc({
          $id: "second",
          name: "Second",
          email: "same@example.test",
          score: 2,
          status: "active",
        }),
      );

      // Assert
      expect(keyCreated).toBe(true);
      expect(uniqueCreated).toBe(true);
      expect(physicalKey.rows).toEqual([{ name: keyIndex }]);
      expect(physicalUnique.rows).toEqual([{ name: uniqueIndex }]);
      await expect(duplicate).rejects.toBeInstanceOf(DuplicateException);

      // Act
      const keyDeleted = await database.deleteIndex("items", "status_key");
      const uniqueDeleted = await database.deleteIndex(
        "items",
        "email_unique",
      );
      const removedIndexes = await database
        .getAdapter()
        .$client.query<{ name: string }>(
          "SELECT name FROM sqlite_schema WHERE type = 'index' AND name IN (?, ?)",
          [keyIndex, uniqueIndex],
        );

      // Assert
      expect(keyDeleted).toBe(true);
      expect(uniqueDeleted).toBe(true);
      expect(removedIndexes.rows).toHaveLength(0);
    } finally {
      await database.getAdapter().$client.disconnect();
    }
  });

  test("supports single and bulk CRUD with metadata and affected results", async () => {
    // Arrange
    const database = await setup("crud_behavior");
    const session = database.system();

    try {
      // Act
      const single = await session.createDocument(
        "items",
        new Doc({
          $id: "single",
          name: "Single",
          email: "single@example.test",
          score: 10,
          status: "single",
        }),
      );
      const bulk = await session.createDocuments("items", [
        new Doc({
          $id: "bulk-a",
          name: "Bulk A",
          email: "bulk-a@example.test",
          score: 20,
          status: "new",
        }),
        new Doc({
          $id: "bulk-b",
          name: "Bulk B",
          email: "bulk-b@example.test",
          score: 30,
          status: "keep",
        }),
      ]);
      const loaded = await session.getDocument("items", "single");

      // Assert
      expect(single.getId()).toBe("single");
      expect(single.getCollection()).toBe("items");
      expect(single.getSequence()).toBeGreaterThan(0);
      expect(single.createdAt()).not.toBeNull();
      expect(single.updatedAt()).not.toBeNull();
      expect(single.createdAt()?.toISOString()).toBe(
        single.updatedAt()?.toISOString(),
      );
      expect(bulk.map((document) => document.getId())).toEqual([
        "bulk-a",
        "bulk-b",
      ]);
      expect(bulk.map((document) => document.getSequence())).toEqual([2, 3]);
      expect(loaded.get("name")).toBe("Single");

      // Act
      const bulkUpdated = await session.updateDocuments(
        "items",
        new Doc({ status: "updated" }),
        [Query.equal("status", ["new"])],
      );
      const emptyBulkUpdated = await session.updateDocuments(
        "items",
        new Doc({ status: "missing" }),
        [Query.equal("name", ["Nobody"])],
      );
      const deletedIds = await session.deleteDocuments("items", [
        Query.equal("status", ["updated"]),
      ]);

      // Assert
      expect(bulkUpdated).toBe(1);
      expect(emptyBulkUpdated).toBe(0);
      expect(deletedIds).toEqual(["bulk-a"]);

      // Act
      const updated = await session.updateDocument(
        "items",
        "single",
        new Doc({ score: 15 }),
      );
      const deletedSingle = await session.deleteDocument("items", "single");
      const deletedMissing = await session.deleteDocument("items", "missing");

      // Assert
      expect(updated.get("score")).toBe(15);
      expect(deletedSingle).toBe(true);
      expect(deletedMissing).toBe(false);
      expect((await session.getDocument("items", "single")).empty()).toBe(true);
      expect(await session.count("items")).toBe(1);
    } finally {
      await database.getAdapter().$client.disconnect();
    }
  });

  test("upserts, increments within bounds, and aggregates deterministically", async () => {
    // Arrange
    const database = await setup("upsert_aggregate");
    const session = database.system();
    await session.createDocument(
      "items",
      new Doc({
        $id: "existing",
        name: "Existing",
        email: "existing@example.test",
        score: 5,
        status: "active",
      }),
    );

    try {
      // Act
      const affected = await session.createOrUpdateDocuments("items", [
        new Doc({
          $id: "existing",
          name: "Existing updated",
          email: "existing@example.test",
          score: 8,
          status: "active",
        }),
        new Doc({
          $id: "created",
          name: "Created",
          email: "created@example.test",
          score: 12,
          status: "active",
        }),
      ]);
      const count = await session.count("items", [
        Query.equal("status", ["active"]),
      ]);
      const sum = await session.sum("items", "score", [
        Query.equal("status", ["active"]),
      ]);
      const emptyCount = await session.count("items", [
        Query.equal("status", ["missing"]),
      ]);
      const emptySum = await session.sum("items", "score", [
        Query.equal("status", ["missing"]),
      ]);

      // Assert
      expect(affected).toBe(2);
      expect((await session.getDocument("items", "existing")).get("name")).toBe(
        "Existing updated",
      );
      expect(count).toBe(2);
      expect(sum).toBe(20);
      expect(emptyCount).toBe(0);
      expect(emptySum).toBe(0);
      expect(await session.createOrUpdateDocuments("items", [])).toBe(0);

      // Act
      const incremented = await session.increaseDocumentAttribute(
        "items",
        "existing",
        "score",
        2,
        10,
      );
      const overLimit = session.increaseDocumentAttribute(
        "items",
        "existing",
        "score",
        1,
        10,
      );

      // Assert
      expect(incremented.get("score")).toBe(10);
      await expect(overLimit).rejects.toBeInstanceOf(LimitException);
    } finally {
      await database.getAdapter().$client.disconnect();
    }
  });

  test("persists schema and documents across a file-backed reopen", async () => {
    // Arrange
    const directory = await mkdtemp(join(tmpdir(), "nuvix-sqlite-crud-"));
    const path = join(directory, "database.sqlite");
    const original = fileDatabase(path);
    let reopened: Database | undefined;

    try {
      await original.create("main");
      await original.createCollection({
        id: "records",
        attributes: [
          new Doc({
            $id: "value",
            key: "value",
            type: AttributeEnum.String,
            required: true,
          }),
        ],
        documentSecurity: false,
      });
      const created = await original.system().createDocument(
        "records",
        new Doc({ $id: "persisted", value: "saved" }),
      );

      // Act
      await original.getAdapter().$client.disconnect();
      reopened = fileDatabase(path);
      const collection = await reopened.getCollection("records");
      const loaded = await reopened.system().getDocument("records", "persisted");

      // Assert
      expect(collection.empty()).toBe(false);
      expect(loaded.getId()).toBe("persisted");
      expect(loaded.get("value")).toBe("saved");
      expect(loaded.getSequence()).toBe(created.getSequence());
      expect(loaded.createdAt()?.toISOString()).toBe(
        created.createdAt()?.toISOString(),
      );
    } finally {
      await original.getAdapter().$client.disconnect();
      await reopened?.getAdapter().$client.disconnect();
      await rm(directory, { recursive: true, force: true });
    }
  });
});
