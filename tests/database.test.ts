import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test";
import { createTestDb } from "./helpers.js";
import { Database } from "../src/core/database.js";
import { Doc } from "../src/core/doc.js";
import {
  AttributeEnum,
  RelationEnum,
  OnDelete,
  IndexEnum,
} from "../src/core/enums.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Query } from "../src/core/query.js";
import {
  NotFoundException,
  DuplicateException,
  DatabaseException,
} from "@errors/index.js";

const ns = `db_test_${Date.now()}`;

describe("Database", () => {
  const db = createTestDb({ namespace: ns });
  // Document-plane operations moved behind ctx-first sessions; schema/admin
  // operations remain directly on the Database instance.
  const session = db.system();
  const testCollections: string[] = [];

  beforeAll(async () => {
    await db.create("yoyo");
  });

  afterAll(async () => {
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

  describe("Collection Operations", () => {
    describe("createCollection", () => {
      it("creates a basic collection", async () => {
        const collectionId = `test_collection_${Date.now()}`;
        testCollections.push(collectionId);

        const collection = await db.createCollection({
          id: collectionId,
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
            new Doc({ $id: "age", key: "age", type: AttributeEnum.Integer }),
            new Doc({
              $id: "active",
              key: "active",
              type: AttributeEnum.Boolean,
              default: true,
            }),
          ],
          permissions: [Permission.create(Role.any())],
        });
        expect(collection.getId()).toBe(collectionId);
        expect(collection.get("attributes", [])).toHaveLength(4);
      });

      it("creates collection with indexes", async () => {
        const collectionId = `test_indexed_${Date.now()}`;
        testCollections.push(collectionId);

        const collection = await db.createCollection({
          id: collectionId,
          attributes: [
            new Doc({
              $id: "email",
              key: "email",
              type: AttributeEnum.String,
              size: 255,
              required: true,
            }),
            new Doc({
              $id: "username",
              key: "username",
              type: AttributeEnum.String,
              size: 100,
              required: true,
            }),
          ],
          indexes: [
            new Doc({
              $id: "email_idx",
              key: "email_idx",
              type: IndexEnum.Unique,
              attributes: ["email"],
            }),
            new Doc({
              $id: "username_idx",
              key: "username_idx",
              type: IndexEnum.Key,
              attributes: ["username"],
            }),
          ],
          permissions: [Permission.create(Role.any())],
        });

        expect([...collection.get("indexes", [])]).toHaveLength(2);
      });

      it("throws error for duplicate collection", async () => {
        const collectionId = `test_duplicate_${Date.now()}`;
        testCollections.push(collectionId);

        await db.createCollection({
          id: collectionId,
          attributes: [
            new Doc({
              $id: "name",
              key: "name",
              type: AttributeEnum.String,
              size: 100,
            }),
          ],
          permissions: [Permission.create(Role.any())],
        });

        await expect(
          db.createCollection({
            id: collectionId,
            attributes: [
              new Doc({
                $id: "title",
                key: "title",
                type: AttributeEnum.String,
                size: 100,
              }),
            ],
            permissions: [Permission.create(Role.any())],
          }),
        ).rejects.toThrow(DuplicateException);
      });
    });

    describe("getCollection", () => {
      it("retrieves existing collection", async () => {
        const collectionId = `test_get_${Date.now()}`;
        testCollections.push(collectionId);

        await db.createCollection({
          id: collectionId,
          attributes: [
            new Doc({
              $id: "title",
              key: "title",
              type: AttributeEnum.String,
              size: 255,
            }),
          ],
          permissions: [Permission.create(Role.any())],
        });

        const collection = await db.getCollection(collectionId);
        expect(collection.getId()).toBe(collectionId);
        expect(collection.empty()).toBe(false);
      });

      it("returns empty doc for non-existent collection", async () => {
        const collection = await db.getCollection("non_existent_collection");
        expect(collection.empty()).toBe(true);
      });

      it("throws error when throwOnNotFound is true", async () => {
        await expect(
          db.getCollection("non_existent_collection", true),
        ).rejects.toThrow(NotFoundException);
      });
    });

    describe("listCollections", () => {
      it("lists collections with pagination", async () => {
        // Create multiple test collections
        const collectionIds = Array.from(
          { length: 5 },
          (_, i) => `test_list_${Date.now()}_${i}`,
        );

        for (const id of collectionIds) {
          testCollections.push(id);
          await db.createCollection({
            id,
            attributes: [
              new Doc({
                $id: "data",
                key: "data",
                type: AttributeEnum.String,
                size: 100,
              }),
            ],
            permissions: [Permission.create(Role.any())],
          });
        }

        const collections = await db.listCollections(3, 0);
        expect(collections.length).toBeGreaterThanOrEqual(3);
      });
    });

    describe("updateCollection", () => {
      it("updates collection permissions and documentSecurity", async () => {
        const collectionId = `test_update_${Date.now()}`;
        testCollections.push(collectionId);

        await db.createCollection({
          id: collectionId,
          attributes: [
            new Doc({
              $id: "content",
              key: "content",
              type: AttributeEnum.String,
              size: 500,
            }),
          ],
          permissions: [Permission.create(Role.any())],
          documentSecurity: false,
        });

        const updatedCollection = await db.updateCollection({
          id: collectionId,
          permissions: [
            Permission.read(Role.any()),
            Permission.create(Role.any()),
          ],
          documentSecurity: true,
        });

        expect(updatedCollection.get("documentSecurity")).toBe(true);
        expect(updatedCollection.get("$permissions")).toHaveLength(2);
      });
    });

    describe("deleteCollection", () => {
      it("deletes existing collection successfully", async () => {
        const collectionId = `test_delete_${Date.now()}`;

        await db.createCollection({
          id: collectionId,
          attributes: [
            new Doc({
              $id: "name",
              key: "name",
              type: AttributeEnum.String,
              size: 100,
            }),
          ],
          permissions: [
            Permission.create(Role.any()),
            Permission.delete(Role.any()),
          ],
        });

        const deleted = await db.deleteCollection(collectionId);
        expect(deleted).toBe(true);

        const collection = await db.getCollection(collectionId);
        expect(collection.empty()).toBe(true);
      });

      it("throws error when deleting non-existent collection", async () => {
        await expect(
          db.deleteCollection("non_existent_collection"),
        ).rejects.toThrow(NotFoundException);
      });

      it("deletes collection with relationships", async () => {
        const usersId = `users_${Date.now()}`;
        const postsId = `posts_${Date.now()}`;
        testCollections.push(usersId, postsId);

        // Create users collection
        await db.createCollection({
          id: usersId,
          attributes: [
            new Doc({
              $id: "name",
              key: "name",
              type: AttributeEnum.String,
              size: 100,
              required: true,
            }),
          ],
          permissions: [Permission.create(Role.any())],
        });

        // Create posts collection
        await db.createCollection({
          id: postsId,
          attributes: [
            new Doc({
              $id: "title",
              key: "title",
              type: AttributeEnum.String,
              size: 255,
              required: true,
            }),
          ],
          permissions: [Permission.create(Role.any())],
        });

        // Create relationship
        await db.createRelationship({
          collectionId: usersId,
          relatedCollectionId: postsId,
          type: RelationEnum.OneToMany,
          twoWay: true,
          id: "posts",
          twoWayKey: "author",
          onDelete: OnDelete.Cascade,
        });

        const deleted = await db.deleteCollection(usersId);
        expect(deleted).toBe(true);
      });

      it("cannot delete metadata collection", async () => {
        await expect(db.deleteCollection(Database.METADATA)).rejects.toThrow();
      });
    });
  });

  describe("Attribute Operations", () => {
    let testCollectionId: string;

    beforeEach(async () => {
      testCollectionId = `test_attributes_${Date.now()}`;
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
        ],
        permissions: [Permission.create(Role.any())],
      });
    });

    describe("createAttribute", () => {
      it("creates string attribute", async () => {
        const result = await db.createAttribute(testCollectionId, {
          $id: "description",
          key: "description",
          type: AttributeEnum.String,
          size: 500,
          required: false,
        });

        expect(result).toBe(true);

        const collection = await db.getCollection(testCollectionId);
        const attributes = collection.get("attributes", []);
        expect(
          attributes.some((attr) => attr.get("$id") === "description"),
        ).toBe(true);
      });

      it("creates integer attribute with default value", async () => {
        const result = await db.createAttribute(testCollectionId, {
          $id: "count",
          key: "count",
          type: AttributeEnum.Integer,
          required: false,
          default: 0,
        });

        expect(result).toBe(true);
      });

      it("creates boolean attribute", async () => {
        const result = await db.createAttribute(testCollectionId, {
          $id: "active",
          key: "active",
          type: AttributeEnum.Boolean,
          required: false,
          default: true,
        });

        expect(result).toBe(true);
      });

      it("throws error for relationship attribute", async () => {
        await expect(
          db.createAttribute(testCollectionId, {
            $id: "relation",
            key: "relation",
            type: AttributeEnum.Relationship,
          }),
        ).rejects.toThrow(DatabaseException);
      });
    });

    describe("createAttributes", () => {
      it("creates multiple attributes at once", async () => {
        const attributes = [
          {
            $id: "email",
            key: "email",
            type: AttributeEnum.String,
            size: 255,
            required: true,
          },
          {
            $id: "age",
            key: "age",
            type: AttributeEnum.Integer,
            required: false,
          },
          {
            $id: "metadata",
            key: "metadata",
            type: AttributeEnum.Json,
            required: false,
          },
        ];

        const result = await db.createAttributes(testCollectionId, attributes);
        expect(result).toBe(true);

        const collection = await db.getCollection(testCollectionId);
        const collectionAttributes = collection.get("attributes", []);
        expect(collectionAttributes).toHaveLength(4); // 1 initial + 3 new
      });
    });

    describe("updateAttribute", () => {
      beforeEach(async () => {
        await db.createAttribute(testCollectionId, {
          $id: "description",
          key: "description",
          type: AttributeEnum.String,
          size: 100,
          required: false,
        });
      });

      it("updates attribute size", async () => {
        const result = await db.updateAttribute(
          testCollectionId,
          "description",
          {
            size: 500,
          },
        );

        expect(result.get("size")).toBe(500);
      });

      it("updates attribute required status", async () => {
        const result = await db.updateAttributeRequired(
          testCollectionId,
          "description",
          true,
        );
        expect(result.get("required")).toBe(true);
      });

      it("updates attribute default value", async () => {
        const result = await db.updateAttributeDefault(
          testCollectionId,
          "description",
          "default text",
        );
        expect(result.get("default")).toBe("default text");
      });

      it("renames attribute", async () => {
        const result = await db.updateAttribute(
          testCollectionId,
          "description",
          {
            newKey: "content",
          },
        );

        expect(result.get("key")).toBe("content");
        expect(result.get("$id")).toBe("content");
      });
    });

    describe("deleteAttribute", () => {
      beforeEach(async () => {
        await db.createAttribute(testCollectionId, {
          $id: "temporary",
          key: "temporary",
          type: AttributeEnum.String,
          size: 100,
          required: false,
        });
      });

      it("deletes attribute successfully", async () => {
        const result = await db.deleteAttribute(testCollectionId, "temporary");
        expect(result).toBe(true);

        const collection = await db.getCollection(testCollectionId);
        const attributes = collection.get("attributes", []);
        expect(attributes.some((attr) => attr.get("$id") === "temporary")).toBe(
          false,
        );
      });

      it("throws error for non-existent attribute", async () => {
        await expect(
          db.deleteAttribute(testCollectionId, "non_existent"),
        ).rejects.toThrow(NotFoundException);
      });
    });
  });

  describe("Index Operations", () => {
    let testCollectionId: string;

    beforeEach(async () => {
      testCollectionId = `test_indexes_${Date.now()}`;
      testCollections.push(testCollectionId);

      await db.createCollection({
        id: testCollectionId,
        attributes: [
          new Doc({
            $id: "email",
            key: "email",
            type: AttributeEnum.String,
            size: 255,
            required: true,
          }),
          new Doc({
            $id: "username",
            key: "username",
            type: AttributeEnum.String,
            size: 100,
            required: true,
          }),
          new Doc({ $id: "age", key: "age", type: AttributeEnum.Integer }),
        ],
        permissions: [Permission.create(Role.any())],
      });
    });

    describe("createIndex", () => {
      it("creates unique index", async () => {
        const result = await db.createIndex(
          testCollectionId,
          "email_unique",
          IndexEnum.Unique,
          ["email"],
        );
        expect(result).toBe(true);

        const collection = await db.getCollection(testCollectionId);
        const indexes = collection.get("indexes", []);
        expect(indexes.some((idx) => idx.get("$id") === "email_unique")).toBe(
          true,
        );
      });

      it("creates key index", async () => {
        const result = await db.createIndex(
          testCollectionId,
          "username_key",
          IndexEnum.Key,
          ["username"],
        );
        expect(result).toBe(true);
      });

      it("creates composite index", async () => {
        const result = await db.createIndex(
          testCollectionId,
          "composite_idx",
          IndexEnum.Key,
          ["username", "age"],
        );
        expect(result).toBe(true);
      });
    });

    describe("deleteIndex", () => {
      beforeEach(async () => {
        await db.createIndex(testCollectionId, "test_index", IndexEnum.Key, [
          "email",
        ]);
      });

      it("deletes index successfully", async () => {
        const result = await db.deleteIndex(testCollectionId, "test_index");
        expect(result).toBe(true);

        const collection = await db.getCollection(testCollectionId);
        const indexes = collection.get("indexes", []);
        expect(indexes.some((idx) => idx.get("$id") === "test_index")).toBe(
          false,
        );
      });
    });

    describe("renameIndex", () => {
      beforeEach(async () => {
        await db.createIndex(testCollectionId, "old_index", IndexEnum.Key, [
          "email",
        ]);
      });

      it("renames index successfully", async () => {
        const result = await db.renameIndex(
          testCollectionId,
          "old_index",
          "new_index",
        );
        expect(result).toBe(true);

        const collection = await db.getCollection(testCollectionId);
        const indexes = collection.get("indexes", []);
        expect(indexes.some((idx) => idx.get("$id") === "new_index")).toBe(
          true,
        );
        expect(indexes.some((idx) => idx.get("$id") === "old_index")).toBe(
          false,
        );
      });
    });
  });

  describe("Relationship Operations", () => {
    let usersCollectionId: string;
    let postsCollectionId: string;

    beforeEach(async () => {
      usersCollectionId = `users_${Date.now()}`;
      postsCollectionId = `posts_${Date.now()}`;
      testCollections.push(usersCollectionId, postsCollectionId);

      await db.createCollection({
        id: usersCollectionId,
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
            required: true,
          }),
        ],
        permissions: [Permission.create(Role.any())],
      });

      await db.createCollection({
        id: postsCollectionId,
        attributes: [
          new Doc({
            $id: "title",
            key: "title",
            type: AttributeEnum.String,
            size: 255,
            required: true,
          }),
          new Doc({
            $id: "content",
            key: "content",
            type: AttributeEnum.String,
            size: 2000,
          }),
        ],
        permissions: [Permission.create(Role.any())],
      });
    });

    describe("createRelationship", () => {
      it("creates one-to-many relationship", async () => {
        const result = await db.createRelationship({
          collectionId: usersCollectionId,
          relatedCollectionId: postsCollectionId,
          type: RelationEnum.OneToMany,
          twoWay: true,
          id: "posts",
          twoWayKey: "author",
          onDelete: OnDelete.Cascade,
        });

        expect(result).toBe(true);

        const usersCollection = await db.getCollection(usersCollectionId);
        const postsCollection = await db.getCollection(postsCollectionId);

        const userPostsAttr = usersCollection
          .get("attributes")
          .find((attr) => attr.get("$id") === "posts");
        const postAuthorAttr = postsCollection
          .get("attributes")
          .find((attr) => attr.get("$id") === "author");

        expect(userPostsAttr).toBeTruthy();
        expect(postAuthorAttr).toBeTruthy();
        expect(userPostsAttr?.get("type")).toBe(AttributeEnum.Relationship);
        expect(postAuthorAttr?.get("type")).toBe(AttributeEnum.Relationship);
      });

      it("creates one-to-one relationship", async () => {
        const result = await db.createRelationship({
          collectionId: usersCollectionId,
          relatedCollectionId: postsCollectionId,
          type: RelationEnum.OneToOne,
          twoWay: true,
          id: "featured_post",
          twoWayKey: "featured_by",
          onDelete: OnDelete.SetNull,
        });

        expect(result).toBe(true);
      });

      it("creates many-to-many relationship", async () => {
        const tagsCollectionId = `tags_${Date.now()}`;
        testCollections.push(tagsCollectionId);

        await db.createCollection({
          id: tagsCollectionId,
          attributes: [
            new Doc({
              $id: "name",
              key: "name",
              type: AttributeEnum.String,
              size: 50,
              required: true,
            }),
          ],
          permissions: [Permission.create(Role.any())],
        });

        const result = await db.createRelationship({
          collectionId: postsCollectionId,
          relatedCollectionId: tagsCollectionId,
          type: RelationEnum.ManyToMany,
          twoWay: true,
          id: "tags",
          twoWayKey: "posts",
          onDelete: OnDelete.Cascade,
        });

        expect(result).toBe(true);
      });
    });

    describe("updateRelationship", () => {
      beforeEach(async () => {
        await db.createRelationship({
          collectionId: usersCollectionId,
          relatedCollectionId: postsCollectionId,
          type: RelationEnum.OneToMany,
          twoWay: true,
          id: "posts",
          twoWayKey: "author",
          onDelete: OnDelete.Cascade,
        });
      });

      it("updates relationship keys", async () => {
        const result = await db.updateRelationship({
          collectionId: usersCollectionId,
          id: "posts",
          newKey: "user_posts",
          newTwoWayKey: "post_author",
        });

        expect(result).toBe(true);

        const usersCollection = await db.getCollection(usersCollectionId);
        const userPostsAttr = usersCollection
          .get("attributes")
          .find((attr) => attr.get("$id") === "user_posts");
        expect(userPostsAttr).toBeTruthy();
      });
    });
  });

  describe("Document Operations", () => {
    let testCollectionId: string;

    beforeEach(async () => {
      testCollectionId = `test_documents_${Date.now()}`;
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
          new Doc({ $id: "age", key: "age", type: AttributeEnum.Integer }),
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

    describe("createDocument", () => {
      it("creates document with random data", async () => {
        const randomData = {
          name: `User_${Math.random().toString(36).substring(7)}`,
          email: `user${Math.floor(Math.random() * 1000)}@example.com`,
          age: Math.floor(Math.random() * 80) + 18,
          active: Math.random() > 0.5,
          metadata: {
            source: "test",
            timestamp: Date.now(),
            score: Math.random() * 100,
          },
        };

        const document = await session.createDocument(
          testCollectionId,
          new Doc(randomData),
        );

        expect(document.getId()).toBeTruthy();
        expect(document.get("name")).toBe(randomData.name);
        expect(document.get("email")).toBe(randomData.email);
        expect(document.get("age")).toBe(randomData.age);
        expect(document.get("$createdAt")).toBeTruthy();
        expect(document.get("$updatedAt")).toBeTruthy();
      });

      it("creates multiple documents with batch data", async () => {
        const documents = [];
        const batchSize = 50;

        for (let i = 0; i < batchSize; i++) {
          const randomData = {
            name: `BatchUser_${i}_${Math.random().toString(36).substring(7)}`,
            email: `batch${i}@example.com`,
            age: Math.floor(Math.random() * 60) + 20,
            active: i % 2 === 0,
            metadata: {
              batch: true,
              index: i,
              category: ["A", "B", "C"][i % 3],
            },
          };

          const document = await session.createDocument(
            testCollectionId,
            new Doc(randomData),
          );
          documents.push(document);
        }

        expect(documents).toHaveLength(batchSize);
        expect(documents.every((doc) => doc.getId())).toBe(true);
      });
    });

    describe("getDocument", () => {
      let testDocumentId: string;

      beforeEach(async () => {
        const randomData = {
          name: `TestUser_${Date.now()}`,
          email: "test@example.com",
          age: 25,
        };

        const document = await session.createDocument(
          testCollectionId,
          new Doc(randomData),
        );
        testDocumentId = document.getId();
      });

      it("retrieves document by ID", async () => {
        const document = await session.getDocument(testCollectionId, testDocumentId);

        expect(document.getId()).toBe(testDocumentId);
        expect(document.get("name")).toContain("TestUser_");
        expect(document.get("email")).toBe("test@example.com");
      });

      it("returns empty doc for non-existent document", async () => {
        const document = await session.getDocument(
          testCollectionId,
          "non_existent_id",
        );
        expect(document.empty()).toBe(true);
      });
    });

    describe("updateDocument", () => {
      let testDocumentId: string;

      beforeEach(async () => {
        const randomData = {
          name: "OriginalName",
          email: "original@example.com",
          age: 30,
        };

        const document = await session.createDocument(
          testCollectionId,
          new Doc(randomData),
        );
        testDocumentId = document.getId();
      });

      it("updates document fields", async () => {
        const updateData = new Doc({
          name: "UpdatedName",
          age: 35,
          metadata: { updated: true },
        });

        const updatedDocument = await session.updateDocument(
          testCollectionId,
          testDocumentId,
          updateData,
        );

        expect(updatedDocument.get("name")).toBe("UpdatedName");
        expect(updatedDocument.get("age")).toBe(35);
        expect(updatedDocument.get("email")).toBe("original@example.com"); // Should remain unchanged
        expect(updatedDocument.get("metadata")).toEqual({ updated: true });
      });
    });

    describe("find", () => {
      beforeEach(async () => {
        // Create multiple test documents with varied data
        const categories = ["electronics", "books", "clothing", "food"];
        const statuses = ["active", "inactive", "pending"];

        for (let i = 0; i < 100; i++) {
          const randomData = {
            name: `Product_${i}_${Math.random().toString(36).substring(7)}`,
            email: `product${i}@shop.com`,
            age: Math.floor(Math.random() * 100) + 1,
            active: Math.random() > 0.3,
            metadata: {
              category: categories[i % categories.length],
              status: statuses[i % statuses.length],
              price: Math.floor(Math.random() * 1000) + 10,
              rating: Math.random() * 5,
            },
          };

          await session.createDocument(testCollectionId, new Doc(randomData));
        }
      });

      it("finds all documents", async () => {
        const documents = await session.find(testCollectionId);
        expect(documents.length).toBeGreaterThan(0);
      });

      it("finds documents with limit", async () => {
        const documents = await session.find(testCollectionId, [Query.limit(10)]);
        expect(documents.length).toBeLessThanOrEqual(10);
      });

      it("finds documents with filters", async () => {
        const documents = await session.find(testCollectionId, [
          Query.equal("active", [true]),
          Query.limit(20),
        ]);

        expect(documents.length).toBeLessThanOrEqual(20);
        expect(documents.every((doc) => doc.get("active") === true)).toBe(true);
      });

      it("finds documents with complex queries", async () => {
        const documents = await session.find(testCollectionId, [
          Query.greaterThan("age", 50),
          Query.equal("active", [true]),
          Query.orderAsc("age"),
          Query.limit(15),
        ]);

        expect(documents.length).toBeLessThanOrEqual(15);
        expect(
          documents.every(
            (doc) => doc.get("age") > 50 && doc.get("active") === true,
          ),
        ).toBe(true);
      });
    });

    describe("findOne", () => {
      beforeEach(async () => {
        await session.createDocument(
          testCollectionId,
          new Doc({
            name: "UniqueUser",
            email: "unique@example.com",
            age: 42,
            active: true,
          }),
        );
      });

      it("finds single document", async () => {
        const document = await session.findOne(testCollectionId, [
          Query.equal("name", ["UniqueUser"]),
        ]);

        expect(document.empty()).toBe(false);
        expect(document.get("name")).toBe("UniqueUser");
        expect(document.get("email")).toBe("unique@example.com");
      });

      it("returns empty doc when no match found", async () => {
        const document = await session.findOne(testCollectionId, [
          Query.equal("name", ["NonExistentUser"]),
        ]);

        expect(document.empty()).toBe(true);
      });
    });
  });

  describe("Database Operations", () => {
    it("checks database exists", async () => {
      const exists = await db.exists();
      expect(exists).toBe(true);
    });

    it("gets collection size", async () => {
      const collectionId = `test_size_${Date.now()}`;
      testCollections.push(collectionId);

      await db.createCollection({
        id: collectionId,
        attributes: [
          new Doc({
            $id: "data",
            key: "data",
            type: AttributeEnum.String,
            size: 100,
          }),
        ],
        permissions: [Permission.create(Role.any())],
      });

      // Add some documents
      for (let i = 0; i < 10; i++) {
        await session.createDocument(
          collectionId,
          new Doc({
            data: `Sample data ${i}`,
          }),
        );
      }

      const size = await db.getSizeOfCollection(collectionId);
      expect(size).toBeGreaterThanOrEqual(10);
    });
  });

  describe("Error Handling", () => {
    it("handles invalid collection operations", async () => {
      await expect(db.getCollection("", true)).rejects.toThrow();
      await expect(
        db.updateCollection({
          id: "non_existent",
          permissions: [],
          documentSecurity: false,
        }),
      ).rejects.toThrow(NotFoundException);
    });

    it("handles invalid attribute operations", async () => {
      const collectionId = `test_errors_${Date.now()}`;
      testCollections.push(collectionId);

      await db.createCollection({
        id: collectionId,
        attributes: [
          new Doc({
            $id: "name",
            key: "name",
            type: AttributeEnum.String,
            size: 100,
          }),
        ],
        permissions: [Permission.create(Role.any())],
      });

      await expect(
        db.createAttribute(collectionId, {
          $id: "name", // Duplicate
          key: "name",
          type: AttributeEnum.String,
          size: 100,
        }),
      ).rejects.toThrow();

      await expect(
        db.updateAttribute(collectionId, "non_existent", { size: 200 }),
      ).rejects.toThrow(NotFoundException);
    });
  });
});
