import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "../helpers.js";
import { Doc } from "@core/doc.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { DuplicateException, NotFoundException } from "@errors/index.js";
import { AttributeEnum, IndexEnum, RelationEnum } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";
import { ID } from "@utils/id.js";

describe("Collection Operations", () => {
  let db: Database;
  const schema = new Date().getTime().toString();

  beforeAll(async () => {
    db = createTestDb({ namespace: `coll_op_${schema}` });
    db.setMeta({ schema });
    await db.create();
  });

  afterAll(async () => {
    await db.delete();
  });

  describe("createCollection", () => {
    test("should create a basic collection", async () => {
      const collectionId = `test_collection_${Date.now()}`;

      const collection = await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      expect(collection.getId()).toBe(collectionId);
      expect(collection.get("name")).toBe(collectionId);
      expect(collection.get("attributes")).toEqual([]);
      expect(collection.get("indexes")).toEqual([]);
      expect(collection.get("documentSecurity")).toBe(true);
    });

    test("should create collection with attributes", async () => {
      const collectionId = `test_attrs_${Date.now()}`;
      const attributes = [
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
      ];

      const collection = await db.createCollection({
        id: collectionId,
        attributes,
      });

      expect(collection.get("attributes")).toHaveLength(2);
      const attributesInCollection = collection.get(
        "attributes",
      ) as Doc<Attribute>[];
      expect(attributesInCollection[0]?.get("$id")).toBe("name");
      expect(attributesInCollection[1]?.get("$id")).toBe("age");
    });

    test("should create collection with indexes", async () => {
      const collectionId = `test_indexes_${Date.now()}`;
      const attributes = [
        new Doc<Attribute>({
          $id: "email",
          key: "email",
          type: AttributeEnum.String,
          size: 255,
          required: true,
        }),
      ];
      const indexesInCollection = [
        new Doc({
          $id: "email_unique",
          key: "email_unique",
          type: IndexEnum.Unique,
          attributes: ["email"],
        }),
      ];

      const collection = await db.createCollection({
        id: collectionId,
        attributes,
        indexes: indexesInCollection,
      });

      expect(collection.get("indexes")).toHaveLength(1);
      const collectionIndexes = collection.get("indexes") as any[];
      expect(collectionIndexes[0]?.get("type")).toBe(IndexEnum.Unique);
    });

    test("should create collection with custom permissions", async () => {
      const collectionId = `test_perms_${Date.now()}`;
      const permissions = [
        Permission.read(Role.any()),
        Permission.create(Role.user(ID.unique())),
      ];

      const collection = await db.createCollection({
        id: collectionId,
        attributes: [],
        permissions,
      });

      expect(collection.get("$permissions")).toHaveLength(2);
    });

    test("should enable document security", async () => {
      const collectionId = `test_doc_security_${Date.now()}`;

      const collection = await db.createCollection({
        id: collectionId,
        attributes: [],
        documentSecurity: true,
      });

      expect(collection.get("documentSecurity")).toBe(true);
    });

    test("should throw error for duplicate collection", async () => {
      const collectionId = `duplicate_${Date.now()}`;

      await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      await expect(
        db.createCollection({
          id: collectionId,
          attributes: [],
        }),
      ).rejects.toThrow(DuplicateException);
    });

    test("should handle array attributes correctly in indexes", async () => {
      const collectionId = `test_array_attrs_${Date.now()}`;
      const attributes = [
        new Doc<Attribute>({
          $id: "tags",
          key: "tags",
          type: AttributeEnum.String,
          size: 100,
          array: true,
          required: false,
        }),
      ];
      const indexes = [
        new Doc({
          $id: "tags_index",
          key: "tags_index",
          type: IndexEnum.Key,
          attributes: ["tags"],
          orders: ["ASC"],
        }),
      ];

      const collection = await db.createCollection({
        id: collectionId,
        attributes,
        indexes,
      });

      // Orders should be set to null for array attributes
      const resultIndexes = collection.get("indexes") as any[];
      expect(resultIndexes[0]?.get("orders")).toEqual([null]);
    });
  });

  describe("getCollection", () => {
    test("should get existing collection", async () => {
      const collectionId = `get_test_${Date.now()}`;

      await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      const collection = await db.getCollection(collectionId);
      expect(collection.getId()).toBe(collectionId);
      expect(collection.empty()).toBe(false);
    });

    test("should return empty doc for non-existent collection", async () => {
      const collection = await db.getCollection("non_existent");
      expect(collection.empty()).toBe(true);
    });

    test("should throw error when throwOnNotFound is true", async () => {
      await expect(db.getCollection("non_existent", true)).rejects.toThrow(
        NotFoundException,
      );
    });

    test("should get metadata collection", async () => {
      const collection = await db.getCollection(Database.METADATA);
      expect(collection.getId()).toBe(Database.METADATA);
      expect(collection.empty()).toBe(false);
    });
  });

  describe("updateCollection", () => {
    test("should update collection permissions", async () => {
      const collectionId = `update_perms_${Date.now()}`;

      const collection = await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      const newPermissions = [
        Permission.read(Role.user(ID.unique())),
        Permission.create(Role.user(ID.unique())),
      ];

      const updatedCollection = await db.updateCollection({
        id: collectionId,
        permissions: newPermissions,
        documentSecurity: false,
      });

      expect(updatedCollection.get("$permissions")).toHaveLength(2);
    });

    test("should update document security", async () => {
      const collectionId = `update_security_${Date.now()}`;

      await db.createCollection({
        id: collectionId,
        attributes: [],
        documentSecurity: false,
      });

      const updatedCollection = await db.updateCollection({
        id: collectionId,
        permissions: [Permission.read(Role.any())],
        documentSecurity: true,
      });

      expect(updatedCollection.get("documentSecurity")).toBe(true);
    });

    test("should throw error for non-existent collection", async () => {
      await expect(
        db.updateCollection({
          id: "non_existent",
          permissions: [Permission.read(Role.any())],
          documentSecurity: false,
        }),
      ).rejects.toThrow(NotFoundException);
    });
  });

  describe("listCollections", () => {
    test("should list collections with default pagination", async () => {
      const collectionIds = Array.from(
        { length: 3 },
        (_, i) => `list_test_${i}_${Date.now()}`,
      );

      // Create multiple collections
      for (const id of collectionIds) {
        await db.createCollection({ id, attributes: [] });
      }
      const collections = await db.listCollections();
      expect(collections.length).toBeGreaterThanOrEqual(3);
    });

    test("should respect limit parameter", async () => {
      const collectionIds = Array.from(
        { length: 5 },
        (_, i) => `limit_test_${i}_${Date.now()}`,
      );

      for (const id of collectionIds) {
        await db.createCollection({ id, attributes: [] });
      }

      const collections = await db.listCollections(2);
      expect(collections.length).toBeLessThanOrEqual(2);
    });

    test("should respect offset parameter", async () => {
      const collectionIds = Array.from(
        { length: 5 },
        (_, i) => `offset_test_${i}_${Date.now()}`,
      );

      for (const id of collectionIds) {
        await db.createCollection({ id, attributes: [] });
      }

      const allCollections = await db.listCollections(10, 0);
      const offsetCollections = await db.listCollections(10, 2);

      expect(offsetCollections.length).toBeLessThanOrEqual(
        allCollections.length,
      );
    });
  });

  describe("deleteCollection", () => {
    test("should delete existing collection", async () => {
      const collectionId = `delete_test_${Date.now()}`;

      await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      const deleted = await db.deleteCollection(collectionId);
      expect(deleted).toBe(true);

      const collection = await db.getCollection(collectionId);
      expect(collection.empty()).toBe(true);
    });

    test("should throw error for non-existent collection", async () => {
      await expect(db.deleteCollection("non_existent")).rejects.toThrow(
        NotFoundException,
      );
    });

    test("should not delete metadata collection", async () => {
      await expect(db.deleteCollection(Database.METADATA)).rejects.toThrow(
        NotFoundException,
      );
    });

    test("should handle relationships during deletion", async () => {
      const collection1Id = `rel_parent_${Date.now()}`;
      const collection2Id = `rel_child_${Date.now()}`;

      // Create collections
      await db.createCollection({
        id: collection1Id,
        attributes: [],
      });

      await db.createCollection({
        id: collection2Id,
        attributes: [],
      });

      // Create relationship
      await db.createRelationship({
        collectionId: collection1Id,
        relatedCollectionId: collection2Id,
        type: RelationEnum.OneToMany,
        id: "children",
        twoWayKey: "parent",
      });

      // Delete parent collection - should also clean up relationships
      const deleted = await db.deleteCollection(collection1Id);
      expect(deleted).toBe(true);

      // Child collection should still exist but relationship should be cleaned up
      const childCollection = await db.getCollection(collection2Id);
      expect(childCollection.empty()).toBe(false);

      const attributes = childCollection.get("attributes", []);
      const relationshipAttr = attributes.find(
        (attr) => attr.get("$id") === "parent",
      );
      expect(relationshipAttr).toBeUndefined();
    });
  });

  describe("getSizeOfCollection", () => {
    test("should get size of collection", async () => {
      const collectionId = `size_test_${Date.now()}`;

      await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      const size = await db.getSizeOfCollection(collectionId);
      expect(typeof size).toBe("number");
      expect(size).toBeGreaterThanOrEqual(0);
    });

    test("should throw error for non-existent collection", async () => {
      await expect(db.getSizeOfCollection("non_existent")).rejects.toThrow(
        NotFoundException,
      );
    });
  });

  describe("getSizeOfCollectionOnDisk", () => {
    test("should get disk size of collection", async () => {
      const collectionId = `disk_size_test_${Date.now()}`;

      await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      const size = await db.getSizeOfCollectionOnDisk(collectionId);
      expect(typeof size).toBe("number");
      expect(size).toBeGreaterThanOrEqual(0);
    });

    test("should throw error for non-existent collection", async () => {
      await expect(
        db.getSizeOfCollectionOnDisk("non_existent"),
      ).rejects.toThrow(NotFoundException);
    });
  });

  describe("analyzeCollection", () => {
    test("should analyze collection", async () => {
      const collectionId = `analyze_test_${Date.now()}`;

      await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      const result = await db.analyzeCollection(collectionId);
      expect(typeof result).toBe("boolean");
    });
  });

  describe("edge cases", () => {
    test("should handle collections with special characters in names", async () => {
      const collectionId = `special_test-collection.name`;

      const collection = await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      expect(collection.getId()).toBe(collectionId);
    });

    test("should handle empty attribute arrays", async () => {
      const collectionId = `empty_attrs_${Date.now()}`;

      const collection = await db.createCollection({
        id: collectionId,
        attributes: [],
      });

      expect(collection.get("attributes")).toEqual([]);
    });
  });
});
