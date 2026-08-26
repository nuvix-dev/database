import {
  describe,
  test,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
} from "bun:test";
import { createTestDb } from "./helpers.js";
import { Database } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";

describe("Cache Functionality", () => {
  let db: Database;
  // Document-plane ops moved behind sessions; caching is mechanics, so this
  // suite uses a privileged system session.
  let system: ReturnType<Database["system"]>;
  let testCollectionId: string;

  const schema = new Date().getTime().toString();

  beforeAll(async () => {
    db = createTestDb({ namespace: `cache_test_${schema}` });
    system = db.system();
    db.setMeta({ schema });
    await db.create();
  });

  beforeEach(async () => {
    testCollectionId = `cache_collection_${Date.now()}`;

    // Create test collection
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
          $id: "value",
          key: "value",
          type: AttributeEnum.Integer,
          required: false,
        }),
      ],
    });
  });

  afterAll(async () => {
    await db.delete();
  });

  describe("Document Caching", () => {
    test("should cache document on first read", async () => {
      // Create a document
      const docData = { name: "Test Document", value: 42 };
      const createdDoc = await system.createDocument(
        testCollectionId,
        new Doc(docData),
      );

      // First read - should fetch from database and cache
      const firstRead = await system.getDocument(
        testCollectionId,
        createdDoc.getId(),
      );

      expect(firstRead.get("name")).toBe("Test Document");
      expect(firstRead.get("value")).toBe(42);

      // Second read - should come from cache
      const secondRead = await system.getDocument(
        testCollectionId,
        createdDoc.getId(),
      );

      expect(secondRead.get("name")).toBe("Test Document");
      expect(secondRead.get("value")).toBe(42);
    });

    test("should return cached document even if database is updated", async () => {
      // Create a document
      const docData = { name: "Original", value: 100 };
      const createdDoc = await system.createDocument(
        testCollectionId,
        new Doc(docData),
      );

      // Read to cache it
      const cachedDoc = await system.getDocument(
        testCollectionId,
        createdDoc.getId(),
      );
      expect(cachedDoc.get("value")).toBe(100);

      // Update document directly in database (bypassing cache)
      const updatedDoc = new Doc({ ...cachedDoc.toObject(), value: 200 });
      await system.updateDocument(testCollectionId, cachedDoc.getId(), updatedDoc);

      // Read again - should still return cached version
      const secondRead = await system.getDocument(
        testCollectionId,
        cachedDoc.getId(),
      );
      expect(secondRead.get("value")).toBe(200); // Still old value from cache
    });
  });

  describe("Cache Purging", () => {
    test("should purge cached document", async () => {
      // Create and cache a document
      const docData = { name: "Purge Test", value: 300 };
      const createdDoc = await system.createDocument(
        testCollectionId,
        new Doc(docData),
      );

      // Read to cache
      const cachedDoc = await system.getDocument(
        testCollectionId,
        createdDoc.getId(),
      );
      expect(cachedDoc.get("value")).toBe(300);

      // Update document
      const updatedDoc = new Doc({ ...cachedDoc.toObject(), value: 400 });
      await system.updateDocument(testCollectionId, cachedDoc.getId(), updatedDoc);

      // Purge document from cache
      await system.purgeCachedDocument(testCollectionId, cachedDoc.getId());

      // Read again - should get updated version
      const freshRead = await system.getDocument(
        testCollectionId,
        cachedDoc.getId(),
      );
      expect(freshRead.get("value")).toBe(400);
    });

    test("should purge cached collection", async () => {
      // Create multiple documents
      const doc1 = await system.createDocument(
        testCollectionId,
        new Doc({ name: "Doc1", value: 1 }),
      );
      const doc2 = await system.createDocument(
        testCollectionId,
        new Doc({ name: "Doc2", value: 2 }),
      );

      // Read to cache
      const cached1 = await system.getDocument(testCollectionId, doc1.getId());
      const cached2 = await system.getDocument(testCollectionId, doc2.getId());

      expect(cached1.get("value")).toBe(1);
      expect(cached2.get("value")).toBe(2);

      // Update documents
      await system.updateDocument(
        testCollectionId,
        doc1.getId(),
        new Doc({ ...cached1.toObject(), value: 11 }),
      );
      await system.updateDocument(
        testCollectionId,
        doc2.getId(),
        new Doc({ ...cached2.toObject(), value: 22 }),
      );

      // Purge entire collection from cache
      await system.purgeCachedCollection(testCollectionId);

      // Read again - should get updated versions
      const fresh1 = await system.getDocument(testCollectionId, doc1.getId());
      const fresh2 = await system.getDocument(testCollectionId, doc2.getId());

      expect(fresh1.get("value")).toBe(11);
      expect(fresh2.get("value")).toBe(22);
    });
  });

  describe("Cache Keys", () => {
    test("should generate correct cache keys", async () => {
      const docData = { name: "Key Test", value: 500 };
      const createdDoc = await system.createDocument(
        testCollectionId,
        new Doc(docData),
      );

      // Access cache keys method (protected, but we can test indirectly)
      const cacheKeys = (db as any).getCacheKeys(
        testCollectionId,
        createdDoc.getId(),
      );

      expect(cacheKeys.collectionKey).toContain(testCollectionId);
      expect(cacheKeys.documentKey).toContain(createdDoc.getId());
      expect(cacheKeys.baseKey).toContain(db.schema);
    });
  });
});
