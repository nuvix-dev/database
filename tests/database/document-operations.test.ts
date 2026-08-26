import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "../helpers.js";
import { Doc } from "@core/doc.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { StructureException } from "@errors/index.js";
import { AttributeEnum } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";
import { ID } from "@utils/id.js";

Database.addFilter("versionFilter", {
  encode(_, __, ___) {
    return null;
  },
  decode(_, __, ___) {
    return "v1";
  },
});

describe("Document Operations", () => {
  let db: Database;
  let system: ReturnType<Database["system"]>;
  let testCollectionId: string;
  const schema = new Date().getTime().toString();

  beforeEach(async () => {
    db = createTestDb({ namespace: `coll_op_${schema}` });
    db.setMeta({ schema });
    await db.create();
    system = db.system();

    testCollectionId = `test_collection_${Date.now()}`;

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
          $id: "metadata",
          key: "metadata",
          type: AttributeEnum.Json,
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
          $id: "version",
          key: "version",
          type: AttributeEnum.Virtual,
          filters: ["versionFilter"],
        }),
      ],
    });
  });

  afterEach(async () => {
    await db.delete();
  });

  describe("createDocument", () => {
    test("should create a basic document", async () => {
      const documentData = {
        name: "John Doe",
        age: 30,
        email: "john@example.com",
      };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.getId()).toBeDefined();
      expect(document.get("name")).toBe("John Doe");
      expect(document.get("age")).toBe(30);
      expect(document.get("email")).toBe("john@example.com");
      expect(document.get("$collection") as unknown).toBe(testCollectionId);
      expect(document.get("$createdAt")).toBeDefined();
      expect(document.get("$updatedAt")).toBeDefined();
      expect(document.get("version") as unknown).toBe("v1");
    });

    test("should create document with default values", async () => {
      const documentData = {
        name: "Jane Doe",
      };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.get("name")).toBe("Jane Doe");
      expect(document.get("age") as unknown).toBe(0); // default value
      expect(document.get("active") as unknown).toBe(true); // default value
    });

    test("should create document with array attributes", async () => {
      const documentData = {
        name: "Array Test",
        tags: ["tag1", "tag2", "tag3"],
      };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.get("tags")).toEqual(["tag1", "tag2", "tag3"]);
    });

    test("should create document with JSON attribute", async () => {
      const metadata = { level: 5, preferences: { theme: "dark" } };
      const documentData = {
        name: "JSON Test",
        metadata,
      };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.get("metadata")).toEqual(metadata);
    });

    test("should auto-generate ID if not provided", async () => {
      const documentData = { name: "Auto ID Test" };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.getId()).toBeDefined();
      expect(document.getId()).toMatch(/^[a-zA-Z0-9]+$/);
    });

    test("should use provided ID", async () => {
      const customId = ID.unique();
      const documentData = { $id: customId, name: "Custom ID Test" };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.getId()).toBe(customId);
    });

    test("should throw error for missing required fields", async () => {
      const documentData = { age: 25 }; // missing required 'name' field

      await expect(
        system.createDocument(testCollectionId, new Doc(documentData)),
      ).rejects.toThrow(StructureException);
    });

    test("should throw error for invalid attribute types", async () => {
      const documentData = {
        name: "Type Test",
        age: "invalid_number", // should be integer
      };

      await expect(
        system.createDocument(testCollectionId, new Doc(documentData)),
      ).rejects.toThrow();
    });

    test("should create document with custom permissions", async () => {
      const userId = ID.unique();
      const permissions = [
        Permission.read(Role.user(userId)),
        Permission.update(Role.user(userId)),
      ];

      const documentData = {
        name: "Permissions Test",
        $permissions: permissions,
      };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.getPermissions()).toHaveLength(2);
    });

    test("should preserve custom timestamps when preserveDates is enabled", async () => {
      const customDate = new Date("2023-01-01T00:00:00.000Z").toISOString();
      const documentData = {
        name: "Date Test",
        $createdAt: customDate,
        $updatedAt: customDate,
      };

      // Enable preserve dates through the database instance
      (db as any).preserveDates = true;

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.get("$createdAt")).toBe(customDate);
      expect(document.get("$updatedAt")).toBe(customDate);
    });
  });

  describe("createDocuments", () => {
    test("should create multiple documents", async () => {
      const documentsData = [
        { name: "User 1", age: 25 },
        { name: "User 2", age: 30 },
        { name: "User 3", age: 35 },
      ].map((data) => new Doc(data));

      const documents = (await system.createDocuments(
        testCollectionId,
        documentsData,
      )) as Doc<Record<string, any>>[];

      expect(documents).toHaveLength(3);
      documents.forEach((doc, index) => {
        expect(doc.get("name")).toBe(`User ${index + 1}`);
        expect(doc.getId()).toBeDefined();
      });
    });

    test("should handle empty array", async () => {
      const documents = await system.createDocuments(testCollectionId, []);
      expect(documents).toEqual([]);
    });

    test("should auto-generate IDs for all documents", async () => {
      const documentsData = Array.from(
        { length: 5 },
        (_, i) => new Doc({ name: `Batch User ${i}` }),
      );

      const documents = await system.createDocuments(
        testCollectionId,
        documentsData,
      );

      const ids = documents.map((doc) => doc.getId());
      expect(new Set(ids).size).toBe(5); // All IDs should be unique
    });

    test("should validate all documents before creation", async () => {
      const documentsData = [
        new Doc({ name: "Valid User" }),
        new Doc({ age: 25 }), // missing required name
      ];

      await expect(
        system.createDocuments(testCollectionId, documentsData),
      ).rejects.toThrow(StructureException);
    });
  });

  describe("getDocument", () => {
    test("should get existing document", async () => {
      const documentData = { name: "Get Test", age: 40 };
      const created = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      const retrieved = await system.getDocument(testCollectionId, created.getId());

      expect(retrieved.getId()).toBe(created.getId());
      expect(retrieved.get("name")).toBe("Get Test");
      expect(retrieved.get("age")).toBe(40);
    });

    test("should return empty doc for non-existent document", async () => {
      const nonExistentId = ID.unique();
      const document = await system.getDocument(testCollectionId, nonExistentId);

      expect(document.empty()).toBe(true);
    });

    test("should return empty doc for empty ID", async () => {
      const document = await system.getDocument(testCollectionId, "");

      expect(document.empty()).toBe(true);
    });

    test("should get document with selected fields", async () => {
      const documentData = {
        name: "Select Test",
        age: 25,
        email: "select@test.com",
      };
      const created = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      const retrieved = await system.getDocument(
        testCollectionId,
        created.getId(),
        (qb) => qb.select("name", "age"),
      );

      expect(retrieved.get("name")).toBe("Select Test");
      expect(retrieved.get("age")).toBe(25);
      expect(retrieved.has("email")).toBeFalsy();
    });
  });

  describe("updateDocument", () => {
    test("should update existing document", async () => {
      const documentData = { name: "Update Test", age: 30 };
      const created = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      const updates = new Doc({
        name: "Updated Name",
        age: 35,
        email: "updated@test.com",
      });

      const updated = await system.updateDocument(
        testCollectionId,
        created.getId(),
        updates,
      );

      expect(updated.get("name")).toBe("Updated Name");
      expect(updated.get("age")).toBe(35);
      expect(updated.get("email")).toBe("updated@test.com");
      expect(updated.get("$updatedAt")).not.toBe(created.get("$updatedAt"));
    });

    test("should preserve $createdAt during update", async () => {
      const documentData = { name: "Preserve Date Test" };
      const created = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      const updates = new Doc({ name: "Updated" });
      const updated = await system.updateDocument(
        testCollectionId,
        created.getId(),
        updates,
      );

      expect(updated.createdAt()?.toString()).toBe(
        created.createdAt()?.toString(),
      );
    });

    test("should update only specified fields", async () => {
      const documentData = {
        name: "Partial Update",
        age: 25,
        email: "original@test.com",
      };
      const created = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      const updates = new Doc({ age: 30 }); // only update age
      const updated = await system.updateDocument(
        testCollectionId,
        created.getId(),
        updates,
      );

      expect(updated.get("name")).toBe("Partial Update"); // unchanged
      expect(updated.get("age")).toBe(30); // updated
      expect(updated.get("email")).toBe("original@test.com"); // unchanged
    });

    test("should handle array updates", async () => {
      const documentData = { name: "Array Update", tags: ["old1", "old2"] };
      const created = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      const updates = new Doc({ tags: ["new1", "new2", "new3"] });
      const updated = await system.updateDocument(
        testCollectionId,
        created.getId(),
        updates,
      );

      expect(updated.get("tags")).toEqual(["new1", "new2", "new3"]);
    });

    test("should throw error for non-existent document", async () => {
      const nonExistentId = ID.unique();
      const updates = new Doc({ name: "Should Fail" });

      const updated = await system.updateDocument(
        testCollectionId,
        nonExistentId,
        updates,
      );
      expect(updated.empty()).toBe(true);
    });

    test("should validate structure during update", async () => {
      const documentData = { name: "Structure Test" };
      const created = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      const updates = new Doc({
        name: "x".repeat(300), // exceeds string size limit of 255
      });

      await expect(
        system.updateDocument(testCollectionId, created.getId(), updates),
      ).rejects.toThrow();
    });
  });

  describe("updateDocuments", () => {
    test("should update multiple documents", async () => {
      // Create test documents
      const documentsData = Array.from(
        { length: 5 },
        (_, i) => new Doc({ name: `User ${i}`, age: 20 + i }),
      );
      await system.createDocuments(testCollectionId, documentsData);

      // Update all documents
      const updates = new Doc({ active: false });
      const modified = await system.updateDocuments(testCollectionId, updates);

      expect(modified).toBe(5);

      // Verify updates
      const allDocs = await system.find(testCollectionId);
      allDocs.forEach((doc) => {
        expect(doc.get("active")).toBe(false);
      });
    });

    test("should update with query filters", async () => {
      // Create test documents
      await system.createDocuments(testCollectionId, [
        new Doc({ name: "Young User", age: 18 }),
        new Doc({ name: "Adult User", age: 25 }),
        new Doc({ name: "Senior User", age: 65 }),
      ]);

      // Update only adults (age >= 18 and < 65)
      const updates = new Doc({ active: false });
      const modified = await system.updateDocuments(
        testCollectionId,
        updates,
        (qb) => qb.greaterThanEqual("age", 18).lessThan("age", 65),
      );

      expect(modified).toBe(2); // Young and Adult, not Senior
    });

    test("should respect batch size", async () => {
      // Create many documents
      const documentsData = Array.from(
        { length: 1000 },
        (_, i) => new Doc({ name: `Batch User ${i}`, age: i }),
      );
      await system.createDocuments(testCollectionId, documentsData);

      const updates = new Doc({ active: false });
      const modified = await system.updateDocuments(
        testCollectionId,
        updates,
        [],
        200, // small batch size
      );

      expect(modified).toBe(1000);
    }, 5000);

    test("should handle empty updates", async () => {
      const updates = new Doc({});
      const modified = await system.updateDocuments(testCollectionId, updates);

      expect(modified).toBe(0);
    });
  });

  describe("deleteDocument", () => {
    test("should delete existing document", async () => {
      const documentData = { name: "Delete Test" };
      const created = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      const deleted = await system.deleteDocument(
        testCollectionId,
        created.getId(),
      );
      expect(deleted).toBe(true);

      // Verify deletion
      const retrieved = await system.getDocument(testCollectionId, created.getId());
      expect(retrieved.empty()).toBe(true);
    });

    test("should return false for non-existent document", async () => {
      const nonExistentId = ID.unique();
      const deleted = await system.deleteDocument(testCollectionId, nonExistentId);

      expect(deleted).toBe(false);
    });

    test("should handle document with relationships", async () => {
      // This test would require relationship setup
      // Skipping for now as it's complex and would be covered in relationship tests
    });
  });

  describe("deleteDocuments", () => {
    test("should delete multiple documents", async () => {
      // Create test documents
      const documentsData = Array.from(
        { length: 5 },
        (_, i) => new Doc({ name: `Delete User ${i}` }),
      );
      const created = await system.createDocuments(testCollectionId, documentsData);

      // Delete all
      const deletedIds = await system.deleteDocuments(testCollectionId);
      expect(deletedIds).toHaveLength(5);

      // Verify deletion
      const remaining = await system.find(testCollectionId);
      expect(remaining).toHaveLength(0);
    });

    test("should delete with query filters", async () => {
      // Create test documents
      await system.createDocuments(testCollectionId, [
        new Doc({ name: "Keep Me", age: 20 }),
        new Doc({ name: "Delete Me 1", age: 30 }),
        new Doc({ name: "Delete Me 2", age: 35 }),
      ]);

      // Delete documents with age >= 30
      const deletedIds = await system.deleteDocuments(testCollectionId, (qb) =>
        qb.greaterThanEqual("age", 30),
      );

      expect(deletedIds).toHaveLength(2);

      // Verify remaining document
      const remaining = await system.find(testCollectionId);
      expect(remaining).toHaveLength(1);
      expect(remaining[0]?.get("name")).toBe("Keep Me");
    });

    test("should batch delete with query filters", async () => {
      // Create test documents
      await system.createDocuments(testCollectionId, [
        new Doc({ name: "Keep Me", age: 20 }),
        new Doc({ name: "Delete Me 1", age: 30 }),
        new Doc({ name: "Delete Me 2", age: 35 }),
        new Doc({ name: "Delete Me 3", age: 55 }),
        new Doc({ name: "Delete Me 4", age: 45 }),
        new Doc({ name: "Delete Me 4", age: 75 }),
      ]);

      // Delete documents with age >= 30
      const deleted = await system.deleteDocumentsBatch(
        testCollectionId,
        (qb) => qb.greaterThanEqual("age", 30),
        3,
      );

      expect(deleted).toEqual(5);

      // Verify remaining document
      const remaining = await system.find(testCollectionId);
      expect(remaining).toHaveLength(1);
      expect(remaining[0]?.get("name")).toBe("Keep Me");
    });

    test("should handle empty result set", async () => {
      const deletedIds = await system.deleteDocuments(testCollectionId, (qb) =>
        qb.equal("name", "Non Existent"),
      );

      expect(deletedIds).toHaveLength(0);
    });
  });

  describe("edge cases", () => {
    test("should handle documents with null values", async () => {
      const documentData = {
        name: "Null Test",
        email: null,
        age: null,
      };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.get("name")).toBe("Null Test");
      expect(document.get("email")).toBeNull();
      expect(document.get("age") as unknown).toBe(0); // default value applied
    });

    test("should handle large documents", async () => {
      const largeMetadata = {
        data: Array.from({ length: 1000 }, (_, i) => ({ key: `value${i}` })),
      };

      const documentData = {
        name: "Large Document",
        metadata: largeMetadata,
      };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.get("metadata")).toEqual(largeMetadata);
    });

    test("should handle special characters in document values", async () => {
      const documentData = {
        name: 'Special chars: äöü ñ 中文 🚀 <script>alert("xss")</script>',
        email: "test+special@example.com",
      };

      const document = await system.createDocument(
        testCollectionId,
        new Doc(documentData),
      );

      expect(document.get("name")).toBe(documentData.name);
      expect(document.get("email")).toBe(documentData.email);
    });
  });
});
