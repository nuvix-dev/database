import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "../helpers.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, IndexEnum } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";
import {
  DatabaseException,
  NotFoundException,
  DuplicateException,
} from "@errors/index.js";

describe("Index Operations", () => {
  let db: Database;
  let testCollectionId: string;

  const schema = new Date().getTime().toString();

  beforeEach(async () => {
    db = createTestDb({ namespace: `coll_op_${schema}` });
    db.setMeta({ schema });
    await db.create();

    testCollectionId = `index_test_${Date.now()}`;

    // Create test collection with various attribute types
    await db.createCollection({
      id: testCollectionId,
      attributes: [
        new Doc<Attribute>({
          $id: "name",
          key: "name",
          type: AttributeEnum.String,
          size: 255,
          required: true,
        }),
        new Doc<Attribute>({
          $id: "age",
          key: "age",
          type: AttributeEnum.Integer,
          required: false,
          default: 0,
        }),
        new Doc<Attribute>({
          $id: "email",
          key: "email",
          type: AttributeEnum.String,
          size: 255,
          required: false,
        }),
        new Doc<Attribute>({
          $id: "active",
          key: "active",
          type: AttributeEnum.Boolean,
          required: false,
          default: true,
        }),
        new Doc<Attribute>({
          $id: "score",
          key: "score",
          type: AttributeEnum.Float,
          required: false,
        }),
        new Doc<Attribute>({
          $id: "tags",
          key: "tags",
          type: AttributeEnum.String,
          size: 100,
          array: true,
          required: false,
        }),
        new Doc<Attribute>({
          $id: "description",
          key: "description",
          type: AttributeEnum.String,
          size: 1000,
          required: false,
        }),
      ],
    });
  });

  afterEach(async () => {
    await db.delete();
  });

  describe("createIndex", () => {
    test("should create key index on single attribute", async () => {
      const created = await db.createIndex(
        testCollectionId,
        "name_index",
        IndexEnum.Key,
        ["name"],
      );

      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];
      const nameIndex = indexes.find((idx) => idx.get("$id") === "name_index");

      expect(nameIndex).toBeDefined();
      expect(nameIndex?.get("type")).toBe(IndexEnum.Key);
      expect(nameIndex?.get("attributes")).toEqual(["name"]);
    });

    test("should create unique index", async () => {
      const created = await db.createIndex(
        testCollectionId,
        "email_unique",
        IndexEnum.Unique,
        ["email"],
      );

      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];
      const emailIndex = indexes.find(
        (idx) => idx.get("$id") === "email_unique",
      );

      expect(emailIndex).toBeDefined();
      expect(emailIndex?.get("type")).toBe(IndexEnum.Unique);
      expect(emailIndex?.get("attributes")).toEqual(["email"]);
    });

    test("should create fulltext index", async () => {
      const created = await db.createIndex(
        testCollectionId,
        "description_fulltext",
        IndexEnum.FullText,
        ["description"],
      );

      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];
      const descIndex = indexes.find(
        (idx) => idx.get("$id") === "description_fulltext",
      );

      expect(descIndex).toBeDefined();
      expect(descIndex?.get("type")).toBe(IndexEnum.FullText);
      expect(descIndex?.get("attributes")).toEqual(["description"]);
    });

    test("should create composite index", async () => {
      const created = await db.createIndex(
        testCollectionId,
        "name_age_composite",
        IndexEnum.Key,
        ["name", "age"],
      );

      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];
      const compositeIndex = indexes.find(
        (idx) => idx.get("$id") === "name_age_composite",
      );

      expect(compositeIndex).toBeDefined();
      expect(compositeIndex?.get("attributes")).toEqual(["name", "age"]);
    });

    test("should create index with custom orders", async () => {
      const created = await db.createIndex(
        testCollectionId,
        "name_age_ordered",
        IndexEnum.Key,
        ["name", "age"],
        ["ASC", "DESC"],
      );

      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];
      const orderedIndex = indexes.find(
        (idx) => idx.get("$id") === "name_age_ordered",
      );

      expect(orderedIndex).toBeDefined();
      expect(orderedIndex?.get("orders")).toEqual(["ASC", "DESC"]);
    });

    test("should handle array attributes in indexes", async () => {
      const created = await db.createIndex(
        testCollectionId,
        "tags_index",
        IndexEnum.Key,
        ["tags"],
        ["ASC"],
      );

      expect(created).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];
      const tagsIndex = indexes.find((idx) => idx.get("$id") === "tags_index");

      expect(tagsIndex).toBeDefined();
      // Orders should be set to null for array attributes
      expect(tagsIndex?.get("orders")).toEqual([null]);
    });

    test("should throw error for empty attributes array", async () => {
      await expect(
        db.createIndex(testCollectionId, "empty_index", IndexEnum.Key, []),
      ).rejects.toThrow(DatabaseException);
    });

    test("should throw error for non-existent attribute", async () => {
      await expect(
        db.createIndex(testCollectionId, "invalid_index", IndexEnum.Key, [
          "nonexistent_attribute",
        ]),
      ).rejects.toThrow(DatabaseException);
    });

    test("should throw error for duplicate index name", async () => {
      await db.createIndex(testCollectionId, "duplicate_index", IndexEnum.Key, [
        "name",
      ]);

      await expect(
        db.createIndex(testCollectionId, "duplicate_index", IndexEnum.Key, [
          "age",
        ]),
      ).rejects.toThrow(DuplicateException);
    });

    test("should throw error for unsupported index type", async () => {
      await expect(
        db.createIndex(
          testCollectionId,
          "invalid_type_index",
          "invalid_type" as IndexEnum,
          ["name"],
        ),
      ).rejects.toThrow(DatabaseException);
    });

    test("should create multiple indexes on same attribute", async () => {
      const keyCreated = await db.createIndex(
        testCollectionId,
        "email_key",
        IndexEnum.Key,
        ["email"],
      );

      const uniqueCreated = await db.createIndex(
        testCollectionId,
        "email_unique",
        IndexEnum.Unique,
        ["email"],
      );

      expect(keyCreated).toBe(true);
      expect(uniqueCreated).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];

      expect(
        indexes.filter((idx) => idx.get("attributes").includes("email")),
      ).toHaveLength(2);
    });
  });

  describe("deleteIndex", () => {
    test("should delete existing index", async () => {
      // First create an index
      await db.createIndex(testCollectionId, "temp_index", IndexEnum.Key, [
        "name",
      ]);

      const deleted = await db.deleteIndex(testCollectionId, "temp_index");
      expect(deleted).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];
      const tempIndex = indexes.find((idx) => idx.get("$id") === "temp_index");

      expect(tempIndex).toBeUndefined();
    });

    test("should handle deletion of non-existent index", async () => {
      const deleted = await db.deleteIndex(
        testCollectionId,
        "nonexistent_index",
      );
      // Should not throw error but return false
      expect(deleted).toBe(false);
    });

    test("should delete multiple indexes", async () => {
      // Create multiple indexes
      await db.createIndex(testCollectionId, "index1", IndexEnum.Key, ["name"]);
      await db.createIndex(testCollectionId, "index2", IndexEnum.Key, ["age"]);
      await db.createIndex(testCollectionId, "index3", IndexEnum.Key, [
        "email",
      ]);

      // Delete them
      const deleted1 = await db.deleteIndex(testCollectionId, "index1");
      const deleted2 = await db.deleteIndex(testCollectionId, "index2");
      const deleted3 = await db.deleteIndex(testCollectionId, "index3");

      expect(deleted1).toBe(true);
      expect(deleted2).toBe(true);
      expect(deleted3).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];

      expect(
        indexes.filter((idx) =>
          ["index1", "index2", "index3"].includes(idx.get("$id")),
        ),
      ).toHaveLength(0);
    });
  });

  describe("renameIndex", () => {
    test("should rename existing index", async () => {
      // First create an index
      await db.createIndex(testCollectionId, "old_index_name", IndexEnum.Key, [
        "name",
      ]);

      const renamed = await db.renameIndex(
        testCollectionId,
        "old_index_name",
        "new_index_name",
      );
      expect(renamed).toBe(true);

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];

      const oldIndex = indexes.find(
        (idx) => idx.get("$id") === "old_index_name",
      );
      const newIndex = indexes.find(
        (idx) => idx.get("$id") === "new_index_name",
      );

      expect(oldIndex).toBeUndefined();
      expect(newIndex).toBeDefined();
      expect(newIndex?.get("key")).toBe("new_index_name");
    });

    test("should throw error for non-existent collection", async () => {
      await expect(
        db.renameIndex("nonexistent", "old_name", "new_name"),
      ).rejects.toThrow(NotFoundException);
    });

    test("should throw error for non-existent index", async () => {
      await expect(
        db.renameIndex(testCollectionId, "nonexistent_index", "new_name"),
      ).rejects.toThrow(NotFoundException);
    });

    test("should throw error for duplicate new name", async () => {
      // Create two indexes
      await db.createIndex(testCollectionId, "index1", IndexEnum.Key, ["name"]);
      await db.createIndex(testCollectionId, "index2", IndexEnum.Key, ["age"]);

      // Try to rename index2 to index1 (duplicate)
      await expect(
        db.renameIndex(testCollectionId, "index2", "index1"),
      ).rejects.toThrow(DuplicateException);
    });

    test("should preserve index properties during rename", async () => {
      await db.createIndex(
        testCollectionId,
        "complex_index",
        IndexEnum.Unique,
        ["name", "email"],
        ["ASC", "DESC"],
      );

      await db.renameIndex(
        testCollectionId,
        "complex_index",
        "renamed_complex",
      );

      const collection = await db.getCollection(testCollectionId);
      const indexes = collection.get("indexes") as any[];
      const renamedIndex = indexes.find(
        (idx) => idx.get("$id") === "renamed_complex",
      );

      expect(renamedIndex).toBeDefined();
      expect(renamedIndex?.get("type")).toBe(IndexEnum.Unique);
      expect(renamedIndex?.get("attributes")).toEqual(["name", "email"]);
      expect(renamedIndex?.get("orders")).toEqual(["ASC", "DESC"]);
    });
  });

  // describe('index validation', () => {
  //     test('should validate index length limits', async () => {
  //         // Create many indexes to approach the limit
  //         const promises = Array.from({ length: 50 }, (_, i) =>
  //             db.createIndex(
  //                 testCollectionId,
  //                 `index_${i}`,
  //                 IndexEnum.Key,
  //                 ['name']
  //             ).catch(() => false) // Some may fail due to limits
  //         );

  //         const results = await Promise.all(promises);

  //         // Should have created some but hit limits at some point
  //         expect(results.some(result => result === true)).toBe(true);
  //     });

  //     test('should validate attribute types for fulltext indexes', async () => {
  //         // Fulltext should work on string attributes
  //         const stringFulltext = await db.createIndex(
  //             testCollectionId,
  //             'description_fulltext',
  //             IndexEnum.FullText,
  //             ['description']
  //         );
  //         expect(stringFulltext).toBe(true);

  //         // Fulltext should not work on non-string attributes
  //         await expect(db.createIndex(
  //             testCollectionId,
  //             'age_fulltext',
  //             IndexEnum.FullText,
  //             ['age'] // integer attribute
  //         )).rejects.toThrow();
  //     });

  //     test('should handle very long index names', async () => {
  //         const longName = 'very_long_index_name_' + 'x'.repeat(50);

  //         const created = await db.createIndex(
  //             testCollectionId,
  //             longName,
  //             IndexEnum.Key,
  //             ['name']
  //         );

  //         expect(created).toBe(true);
  //     });
  // }, 30_000);

  // describe('index performance scenarios', () => {
  //     test('should create indexes on commonly queried fields', async () => {
  //         // Create indexes for typical query patterns
  //         const emailIndex = await db.createIndex(
  //             testCollectionId,
  //             'email_lookup',
  //             IndexEnum.Unique,
  //             ['email']
  //         );

  //         const activeUsersIndex = await db.createIndex(
  //             testCollectionId,
  //             'active_users',
  //             IndexEnum.Key,
  //             ['active']
  //         );

  //         const ageRangeIndex = await db.createIndex(
  //             testCollectionId,
  //             'age_range',
  //             IndexEnum.Key,
  //             ['age']
  //         );

  //         expect(emailIndex).toBe(true);
  //         expect(activeUsersIndex).toBe(true);
  //         expect(ageRangeIndex).toBe(true);

  //         // Create some test data
  //         const testDocs = Array.from({ length: 10 }, (_, i) => new Doc({
  //             name: `User ${i}`,
  //             email: `user${i}@example.com`,
  //             age: 20 + i,
  //             active: i % 2 === 0
  //         }));

  //         await db.createDocuments(testCollectionId, testDocs);

  //         // Verify queries work efficiently with indexes
  //         const activeUsers = await db.find(testCollectionId, qb =>
  //             qb.equal('active', true)
  //         );

  //         const userByEmail = await db.findOne(testCollectionId, qb =>
  //             qb.equal('email', 'user5@example.com')
  //         );

  //         const youngUsers = await db.find(testCollectionId, qb =>
  //             qb.lessThan('age', 25)
  //         );

  //         expect(activeUsers.length).toBeGreaterThan(0);
  //         expect(userByEmail.empty()).toBe(false);
  //         expect(youngUsers.length).toBeGreaterThan(0);
  //     }, 30_000);

  //     test('should handle complex composite indexes', async () => {
  //         // Create composite index for complex queries
  //         const compositeIndex = await db.createIndex(
  //             testCollectionId,
  //             'active_age_name',
  //             IndexEnum.Key,
  //             ['active', 'age', 'name'],
  //             ['ASC', 'DESC', 'ASC']
  //         );

  //         expect(compositeIndex).toBe(true);

  //         // Create test data
  //         const testDocs = Array.from({ length: 20 }, (_, i) => new Doc({
  //             name: `User ${String.fromCharCode(65 + (i % 5))}`, // A, B, C, D, E
  //             age: 20 + (i % 10),
  //             active: i % 3 === 0
  //         }));

  //         await db.createDocuments(testCollectionId, testDocs);

  //         // Query using the composite index
  //         const complexQuery = await db.find(testCollectionId, [
  //             Query.equal('active', [true]),
  //             Query.greaterThan('age', 22),
  //             Query.orderAsc('name')
  //         ]);

  //         expect(complexQuery.length).toBeGreaterThan(0);
  //     }, 30_000);
  // }, 30_000);

  // describe('edge cases', () => {
  //     test('should handle index operations on large collections', async () => {
  //         // Create a bunch of test data first
  //         const largeBatch = Array.from({ length: 100 }, (_, i) => new Doc({
  //             name: `User ${i}`,
  //             age: 20 + (i % 50),
  //             email: `user${i}@example.com`
  //         }));

  //         await db.createDocuments(testCollectionId, largeBatch);

  //         // Now create indexes on the populated collection
  //         const nameIndex = await db.createIndex(
  //             testCollectionId,
  //             'name_on_large',
  //             IndexEnum.Key,
  //             ['name']
  //         );

  //         const emailIndex = await db.createIndex(
  //             testCollectionId,
  //             'email_unique_large',
  //             IndexEnum.Unique,
  //             ['email']
  //         );

  //         expect(nameIndex).toBe(true);
  //         expect(emailIndex).toBe(true);
  //     }, 30_000);

  //     test('should handle index names with special characters', async () => {
  //         const specialName = 'index_with-special.chars_123';

  //         const created = await db.createIndex(
  //             testCollectionId,
  //             specialName,
  //             IndexEnum.Key,
  //             ['name']
  //         );

  //         expect(created).toBe(true);

  //         const renamed = await db.renameIndex(testCollectionId, specialName, 'renamed_special');
  //         expect(renamed).toBe(true);
  //     });

  //     test('should validate index creation with null values in data', async () => {
  //         // Create some documents with null values
  //         await db.createDocuments(testCollectionId, [
  //             new Doc({ name: 'User 1', email: 'user1@example.com' }),
  //             new Doc({ name: 'User 2', email: null }),
  //             new Doc({ name: 'User 3', email: 'user3@example.com' })
  //         ]);

  //         // Create index on field that has null values
  //         const emailIndex = await db.createIndex(
  //             testCollectionId,
  //             'email_with_nulls',
  //             IndexEnum.Key,
  //             ['email']
  //         );

  //         expect(emailIndex).toBe(true);
  //     }, 30_000);
  // }, 30_000);
});
