import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import { createTestDb } from "./helpers.js";
import { Database } from "../src/core/database.js";
import { Doc } from "../src/core/doc.js";
import { AttributeEnum } from "../src/core/enums.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { DuplicateException, StructureException } from "@errors/index.js";

const ns = `db_upsert_test_${Date.now()}`;

describe("Database Upsert Operations", () => {
  const db = createTestDb({ namespace: ns });
  const testCollections: string[] = [];

  beforeAll(async () => {
    await db.create("test_upsert_db");
  });

  afterAll(async () => {
    await db.delete();
    await db.getAdapter().$client.disconnect();
  });

  beforeEach(async () => {
    // Clean up test collections
    for (const collectionId of testCollections) {
      try {
        await db.deleteCollection(collectionId);
      } catch (error) {
        // Ignore if collection doesn't exist
      }
    }
    testCollections.length = 0;
  });

  describe("createOrUpdateDocuments", () => {
    let testCollectionId: string;

    beforeEach(async () => {
      testCollectionId = `test_upsert_${Date.now()}`;
      testCollections.push(testCollectionId);

      await db.createCollection({
        id: testCollectionId,
        attributes: [
          new Doc({
            $id: "name",
            key: "name",
            type: AttributeEnum.String,
            size: 100,
            required: true,
          }),
          new Doc({
            $id: "email",
            key: "email",
            type: AttributeEnum.String,
            size: 255,
          }),
          new Doc({
            $id: "score",
            key: "score",
            type: AttributeEnum.Integer,
            default: 0,
          }),
          new Doc({
            $id: "balance",
            key: "balance",
            type: AttributeEnum.Float,
            default: 0.0,
          }),
          new Doc({
            $id: "active",
            key: "active",
            type: AttributeEnum.Boolean,
            default: true,
          }),
          new Doc({
            $id: "metadata",
            key: "metadata",
            type: AttributeEnum.Json,
          }),
        ],
        permissions: [
          Permission.create(Role.any()),
          Permission.read(Role.any()),
          Permission.update(Role.any()),
          Permission.delete(Role.any()),
        ],
      });
    });

    describe("basic functionality", () => {
      it("creates new documents when they don't exist", async () => {
        const documents = [
          new Doc({
            $id: "user1",
            name: "John Doe",
            email: "john@example.com",
            score: 100,
          }),
          new Doc({
            $id: "user2",
            name: "Jane Smith",
            email: "jane@example.com",
            score: 150,
          }),
        ];

        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          documents,
        );

        expect(result).toBe(2);

        // Verify documents were created
        const doc1 = await db.getDocument(testCollectionId, "user1");
        const doc2 = await db.getDocument(testCollectionId, "user2");

        expect(doc1.get("name")).toBe("John Doe");
        expect(doc1.get("score")).toBe(100);
        expect(doc2.get("name")).toBe("Jane Smith");
        expect(doc2.get("score")).toBe(150);
      });

      it("updates existing documents", async () => {
        // First, create documents
        const initialDocuments = [
          new Doc({
            $id: "user1",
            name: "John Doe",
            email: "john@example.com",
            score: 100,
          }),
        ];

        await db.createOrUpdateDocuments(testCollectionId, initialDocuments);

        // Then update them
        const updateDocuments = [
          new Doc({
            $id: "user1",
            name: "John Updated",
            email: "john.updated@example.com",
            score: 200,
          }),
        ];

        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          updateDocuments,
        );

        expect(result).toBe(1);

        // Verify document was updated
        const updatedDoc = await db.getDocument(testCollectionId, "user1");
        expect(updatedDoc.get("name")).toBe("John Updated");
        expect(updatedDoc.get("email")).toBe("john.updated@example.com");
        expect(updatedDoc.get("score")).toBe(200);
      });

      it("handles mixed create and update operations", async () => {
        // Create initial document
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "existing",
            name: "Existing User",
            score: 50,
          }),
        ]);

        // Mix of create and update
        const documents = [
          new Doc({
            $id: "existing",
            name: "Updated Existing",
            score: 75,
          }),
          new Doc({
            $id: "new",
            name: "New User",
            score: 100,
          }),
        ];

        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          documents,
        );

        expect(result).toBe(2);

        const existingDoc = await db.getDocument(testCollectionId, "existing");
        const newDoc = await db.getDocument(testCollectionId, "new");

        expect(existingDoc.get("name")).toBe("Updated Existing");
        expect(existingDoc.get("score")).toBe(75);
        expect(newDoc.get("name")).toBe("New User");
        expect(newDoc.get("score")).toBe(100);
      });

      it("generates IDs for documents without IDs", async () => {
        const documents = [
          new Doc({
            name: "Auto ID User 1",
            email: "auto1@example.com",
          }),
          new Doc({
            name: "Auto ID User 2",
            email: "auto2@example.com",
          }),
        ];

        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          documents,
        );

        expect(result).toBe(2);

        // Documents should have auto-generated IDs
        expect(documents[0]!.getId()).toBeTruthy();
        expect(documents[1]!.getId()).toBeTruthy();
        expect(documents[0]!.getId()).not.toBe(documents[1]!.getId());
      });

      it("returns 0 for empty document array", async () => {
        const result = await db.createOrUpdateDocuments(testCollectionId, []);
        expect(result).toBe(0);
      });

      it("returns 0 for null/undefined documents", async () => {
        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          null as any,
        );
        expect(result).toBe(0);
      });
    });

    describe("batch processing", () => {
      it("processes documents in custom batch sizes", async () => {
        const documents = Array.from(
          { length: 25 },
          (_, i) =>
            new Doc({
              $id: `batch_user_${i}`,
              name: `Batch User ${i}`,
              score: i * 10,
            }),
        );

        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          documents,
          10, // Custom batch size
        );

        expect(result).toBe(25);

        // Verify all documents were created
        const firstDoc = await db.getDocument(testCollectionId, "batch_user_0");
        const lastDoc = await db.getDocument(testCollectionId, "batch_user_24");

        expect(firstDoc.get("name")).toBe("Batch User 0");
        expect(lastDoc.get("name")).toBe("Batch User 24");
      });

      it("handles large batch operations efficiently", async () => {
        const documents = Array.from(
          { length: 100 },
          (_, i) =>
            new Doc({
              $id: `large_batch_${i}`,
              name: `User ${i}`,
              score: Math.floor(Math.random() * 1000),
            }),
        );

        const startTime = Date.now();
        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          documents,
          50,
        );
        const endTime = Date.now();

        expect(result).toBe(100);
        expect(endTime - startTime).toBeLessThan(10000); // Should complete in under 10 seconds

        // Sample verify some documents
        const sampleDoc = await db.getDocument(
          testCollectionId,
          "large_batch_50",
        );
        expect(sampleDoc.get("name")).toBe("User 50");
      });
    });

    describe("onNext callback functionality", () => {
      it("calls onNext callback for each processed document", async () => {
        const processedDocs: Doc<any>[] = [];

        const documents = [
          new Doc({
            $id: "callback1",
            name: "Callback User 1",
            score: 100,
          }),
          new Doc({
            $id: "callback2",
            name: "Callback User 2",
            score: 200,
          }),
        ];

        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          documents,
          Database.DEFAULT_BATCH_SIZE,
          (doc) => {
            processedDocs.push(doc);
          },
        );

        expect(result).toBe(2);
        expect(processedDocs).toHaveLength(2);
        expect(processedDocs.some((doc) => doc.getId() === "callback1")).toBe(
          true,
        );
        expect(processedDocs.some((doc) => doc.getId() === "callback2")).toBe(
          true,
        );
      });

      it("handles async onNext callback", async () => {
        const processedIds: string[] = [];

        const documents = [
          new Doc({
            $id: "async1",
            name: "Async User 1",
          }),
          new Doc({
            $id: "async2",
            name: "Async User 2",
          }),
        ];

        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          documents,
          Database.DEFAULT_BATCH_SIZE,
          async (doc) => {
            // Simulate async processing
            await new Promise((resolve) => setTimeout(resolve, 10));
            processedIds.push(doc.getId());
          },
        );

        expect(result).toBe(2);
        expect(processedIds).toHaveLength(2);
        expect(processedIds).toContain("async1");
        expect(processedIds).toContain("async2");
      });
    });

    describe("validation and error handling", () => {
      it("throws error for duplicate document IDs in input array", async () => {
        const documents = [
          new Doc({
            $id: "duplicate",
            name: "First",
          }),
          new Doc({
            $id: "duplicate",
            name: "Second",
          }),
        ];

        await expect(
          db.createOrUpdateDocuments(testCollectionId, documents),
        ).rejects.toThrow(DuplicateException);
      });

      it("validates required fields", async () => {
        const documents = [
          new Doc({
            $id: "missing_required",
            email: "test@example.com",
            // Missing required 'name' field
          }),
        ];

        await expect(
          db.createOrUpdateDocuments(testCollectionId, documents),
        ).rejects.toThrow(StructureException);
      });

      it("handles collection not found", async () => {
        const documents = [
          new Doc({
            $id: "test",
            name: "Test User",
          }),
        ];

        await expect(
          db.createOrUpdateDocuments("non_existent_collection", documents),
        ).rejects.toThrow();
      });

      it("skips documents that haven't changed", async () => {
        // Create initial document
        const initialDoc = new Doc({
          $id: "unchanged",
          name: "Unchanged User",
          score: 100,
        });

        await db.createOrUpdateDocuments(testCollectionId, [initialDoc]);

        // Try to update with same data
        const sameDoc = await db.getDocument(testCollectionId, "unchanged");
        const result = await db.createOrUpdateDocuments(testCollectionId, [
          sameDoc,
        ]);

        // Should return 0 since no actual changes occurred
        expect(result).toBe(0);
      });
    });

    describe("attribute defaults handling", () => {
      it("applies default values for optional attributes", async () => {
        const documents = [
          new Doc({
            $id: "with_defaults",
            name: "User With Defaults",
            // Not setting optional fields with defaults
          }),
        ];

        const result = await db.createOrUpdateDocuments(
          testCollectionId,
          documents,
        );

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "with_defaults");
        expect(doc.get("active")).toBe(true); // Default value
        expect(doc.get("score")).toBe(0); // Default value
        expect(doc.get("balance")).toBe(0.0); // Default value
      });

      it("preserves existing values for optional attributes not in update", async () => {
        // Create document with specific values
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "preserve_test",
            name: "Original Name",
            score: 500,
            active: false,
          }),
        ]);

        // Update only name, should preserve other values
        const result = await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "preserve_test",
            name: "Updated Name",
          }),
        ]);

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "preserve_test");
        expect(doc.get("name")).toBe("Updated Name");
        expect(doc.get("score")).toBe(500); // Preserved
        expect(doc.get("active")).toBe(false); // Preserved
      });
    });

    describe("timestamp handling", () => {
      it("sets createdAt and updatedAt appropriately", async () => {
        const documents = [
          new Doc({
            $id: "timestamp_test",
            name: "Timestamp User",
          }),
        ];

        await db.createOrUpdateDocuments(testCollectionId, documents);

        const doc = await db.getDocument(testCollectionId, "timestamp_test");

        expect(doc.get("$createdAt")).toBeTruthy();
        expect(doc.get("$updatedAt")).toBeTruthy();
        expect(new Date(doc.get("$createdAt") as Date)).toBeInstanceOf(Date);
        expect(new Date(doc.get("$updatedAt") as Date)).toBeInstanceOf(Date);
      });

      it("preserves createdAt on update", async () => {
        // Create document
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "created_preserve",
            name: "Original",
          }),
        ]);

        const originalDoc = await db.getDocument(
          testCollectionId,
          "created_preserve",
        );
        const originalCreatedAt = originalDoc.get("$createdAt");

        // Small delay to ensure different timestamp
        await new Promise((resolve) => setTimeout(resolve, 100));

        // Update document
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "created_preserve",
            name: "Updated",
          }),
        ]);

        const updatedDoc = await db.getDocument(
          testCollectionId,
          "created_preserve",
        );

        expect(updatedDoc.get("$createdAt")).toStrictEqual(originalCreatedAt);
        expect(updatedDoc.get("$updatedAt")).not.toStrictEqual(
          originalCreatedAt,
        );
      });
    });
  });

  describe("createOrUpdateDocumentsWithIncrease", () => {
    let testCollectionId: string;

    beforeEach(async () => {
      testCollectionId = `test_increase_${Date.now()}`;
      testCollections.push(testCollectionId);

      await db.createCollection({
        id: testCollectionId,
        attributes: [
          new Doc({
            $id: "name",
            key: "name",
            type: AttributeEnum.String,
            size: 100,
            required: true,
          }),
          new Doc({
            $id: "score",
            key: "score",
            type: AttributeEnum.Integer,
            default: 0,
          }),
          new Doc({
            $id: "balance",
            key: "balance",
            type: AttributeEnum.Float,
            default: 0.0,
          }),
          new Doc({
            $id: "points",
            key: "points",
            type: AttributeEnum.Integer,
            default: 0,
          }),
        ],
        permissions: [
          Permission.create(Role.any()),
          Permission.read(Role.any()),
          Permission.update(Role.any()),
          Permission.delete(Role.any()),
        ],
      });
    });

    describe("attribute increase functionality", () => {
      it("increases integer attribute values", async () => {
        // Create initial document
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "increase_test",
            name: "Increase User",
            score: 100,
          }),
        ]);

        // Update with increase
        const documents = [
          new Doc({
            $id: "increase_test",
            name: "Increase User",
            score: 50, // This should be added to existing 100
          }),
        ];

        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "score",
          documents,
        );

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "increase_test");
        expect(doc.get("score")).toBe(150); // 100 + 50
      });

      it("increases float attribute values", async () => {
        // Create initial document
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "float_increase",
            name: "Float User",
            balance: 25.5,
          }),
        ]);

        // Update with increase
        const documents = [
          new Doc({
            $id: "float_increase",
            name: "Float User",
            balance: 10.25,
          }),
        ];

        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "balance",
          documents,
        );

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "float_increase");
        expect(doc.get("balance")).toBeCloseTo(35.75); // 25.5 + 10.25
      });

      it("creates new documents with increase attribute", async () => {
        const documents = [
          new Doc({
            $id: "new_with_increase",
            name: "New User",
            score: 75,
          }),
        ];

        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "score",
          documents,
        );

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "new_with_increase");
        expect(doc.get("score")).toBe(75); // New document, no existing value to increase
      });

      it("handles negative increase values (decrease)", async () => {
        // Create initial document
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "decrease_test",
            name: "Decrease User",
            score: 100,
          }),
        ]);

        // Update with negative increase (decrease)
        const documents = [
          new Doc({
            $id: "decrease_test",
            name: "Decrease User",
            score: -30,
          }),
        ];

        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "score",
          documents,
        );

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "decrease_test");
        expect(doc.get("score")).toBe(70); // 100 + (-30)
      });

      it("handles zero increase values", async () => {
        // Create initial document
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "zero_increase",
            name: "Zero User",
            score: 50,
          }),
        ]);

        // Update with zero increase
        const documents = [
          new Doc({
            $id: "zero_increase",
            name: "Zero User Updated",
            score: 0,
          }),
        ];

        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "score",
          documents,
        );

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "zero_increase");
        expect(doc.get("score")).toBe(50); // 50 + 0
      });
    });

    describe("fallback to regular upsert", () => {
      it("works as regular upsert when attribute is empty string", async () => {
        const documents = [
          new Doc({
            $id: "regular_upsert",
            name: "Regular User",
            score: 100,
          }),
        ];

        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "", // Empty attribute - should work as regular upsert
          documents,
        );

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "regular_upsert");
        expect(doc.get("score")).toBe(100);
      });

      it("updates normally when no increase attribute specified", async () => {
        // Create initial document
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "normal_update",
            name: "Initial Name",
            score: 50,
          }),
        ]);

        // Update normally (replace, don't increase)
        const documents = [
          new Doc({
            $id: "normal_update",
            name: "Updated Name",
            score: 75,
          }),
        ];

        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "", // No increase attribute
          documents,
        );

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "normal_update");
        expect(doc.get("score")).toBe(75); // Replaced, not increased
        expect(doc.get("name")).toBe("Updated Name");
      });
    });

    describe("batch processing with increase", () => {
      it("processes multiple documents with increases in batches", async () => {
        // Create initial documents
        const initialDocs = Array.from(
          { length: 10 },
          (_, i) =>
            new Doc({
              $id: `batch_increase_${i}`,
              name: `User ${i}`,
              score: i * 10,
            }),
        );

        await db.createOrUpdateDocuments(testCollectionId, initialDocs);

        // Increase scores for all documents
        const increaseDocs = Array.from(
          { length: 10 },
          (_, i) =>
            new Doc({
              $id: `batch_increase_${i}`,
              name: `User ${i}`,
              score: 5, // Add 5 to each score
            }),
        );

        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "score",
          increaseDocs,
          3, // Small batch size
        );

        expect(result).toBe(10);

        // Verify increases were applied
        const doc0 = await db.getDocument(testCollectionId, "batch_increase_0");
        const doc5 = await db.getDocument(testCollectionId, "batch_increase_5");

        expect(doc0.get("score")).toBe(5); // 0 + 5
        expect(doc5.get("score")).toBe(55); // 50 + 5
      });
    });

    describe("mixed create and update with increase", () => {
      it("handles mixed operations correctly", async () => {
        // Create one document initially
        await db.createOrUpdateDocuments(testCollectionId, [
          new Doc({
            $id: "existing_for_increase",
            name: "Existing User",
            score: 100,
          }),
        ]);

        // Mix of new creation and existing update with increase
        const documents = [
          new Doc({
            $id: "existing_for_increase",
            name: "Existing User",
            score: 25, // Should increase to 125
          }),
          new Doc({
            $id: "new_for_increase",
            name: "New User",
            score: 50, // New document, should be 50
          }),
        ];

        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "score",
          documents,
        );

        expect(result).toBe(2);

        const existingDoc = await db.getDocument(
          testCollectionId,
          "existing_for_increase",
        );
        const newDoc = await db.getDocument(
          testCollectionId,
          "new_for_increase",
        );

        expect(existingDoc.get("score")).toBe(125); // 100 + 25
        expect(newDoc.get("score")).toBe(50); // New document
      });
    });

    describe("error handling with increase", () => {
      it("handles edge cases gracefully", async () => {
        const documents = [
          new Doc({
            $id: "edge_case",
            name: "Edge Case User",
          }),
        ];

        // Should work even with no increase attribute value specified
        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "score",
          documents,
        );

        expect(result).toBe(1);

        const doc = await db.getDocument(testCollectionId, "edge_case");
        expect(doc.get("score")).toBe(0); // Default value
      });

      it("maintains batch size limits", async () => {
        const documents = Array.from(
          { length: 2000 },
          (_, i) =>
            new Doc({
              $id: `limit_test_${i}`,
              name: `User ${i}`,
              score: 1,
            }),
        );

        // Should cap batch size at 1000
        const result = await db.createOrUpdateDocumentsWithIncrease(
          testCollectionId,
          "score",
          documents,
          5000, // Request large batch size
        );

        expect(result).toBe(2000);
      }, 10000); // Allow longer timeout for large batch
    });
  });
});
