import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "../helpers.js";
import { Doc } from "@core/doc.js";
import { Query } from "@core/query.js";
import { AttributeEnum, RelationEnum, OnDelete } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";
import {
  DatabaseException,
  NotFoundException,
  DuplicateException,
  RelationshipException,
} from "@errors/index.js";

describe("Relationship Operations", () => {
  let db: Database;
  let usersCollectionId: string;
  let postsCollectionId: string;
  let tagsCollectionId: string;

  const schema = new Date().getTime().toString();

  beforeEach(async () => {
    db = createTestDb({ namespace: `coll_op_${schema}` });
    db.setMeta({ schema });
    await db.create();

    const timestamp = Date.now();
    usersCollectionId = `users_${timestamp}`;
    postsCollectionId = `posts_${timestamp}`;
    tagsCollectionId = `tags_${timestamp}`;

    // Create Users collection
    await db.createCollection({
      id: usersCollectionId,
      attributes: [
        new Doc<Attribute>({
          $id: "name",
          key: "name",
          type: AttributeEnum.String,
          size: 255,
          required: true,
        }),
        new Doc<Attribute>({
          $id: "email",
          key: "email",
          type: AttributeEnum.String,
          size: 255,
          required: true,
        }),
      ],
    });

    // Create Posts collection
    await db.createCollection({
      id: postsCollectionId,
      attributes: [
        new Doc<Attribute>({
          $id: "title",
          key: "title",
          type: AttributeEnum.String,
          size: 255,
          required: true,
        }),
        new Doc<Attribute>({
          $id: "content",
          key: "content",
          type: AttributeEnum.String,
          size: 5000,
          required: false,
        }),
      ],
    });

    // Create Tags collection
    await db.createCollection({
      id: tagsCollectionId,
      attributes: [
        new Doc<Attribute>({
          $id: "name",
          key: "name",
          type: AttributeEnum.String,
          size: 100,
          required: true,
        }),
        new Doc<Attribute>({
          $id: "color",
          key: "color",
          type: AttributeEnum.String,
          size: 7,
          required: false,
          default: "#000000",
        }),
      ],
    });
  });

  afterEach(async () => {
    await db.delete();
  });

  describe("createRelationship", () => {
    describe("OneToOne relationships", () => {
      test("should create one-to-one relationship", async () => {
        const created = await db.createRelationship({
          collectionId: usersCollectionId,
          relatedCollectionId: postsCollectionId,
          type: RelationEnum.OneToOne,
          id: "profile",
          twoWayKey: "user",
          twoWay: true,
        });

        expect(created).toBe(true);

        // Check if relationship attribute was added to users collection
        const usersCollection = await db.getCollection(usersCollectionId);
        const userAttributes = usersCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const profileAttr = userAttributes.find(
          (attr) => attr.get("$id") === "profile",
        );

        expect(profileAttr).toBeDefined();
        expect(profileAttr?.get("type")).toBe(AttributeEnum.Relationship);

        const options = profileAttr?.get("options") as any;
        expect(options.relationType).toBe(RelationEnum.OneToOne);
        expect(options.relatedCollection).toBe(postsCollectionId);
        expect(options.twoWay).toBe(true);
        expect(options.twoWayKey).toBe("user");

        // Check if two-way relationship was added to posts collection
        const postsCollection = await db.getCollection(postsCollectionId);
        const postAttributes = postsCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const userAttr = postAttributes.find(
          (attr) => attr.get("$id") === "user",
        );

        expect(userAttr).toBeDefined();
        expect(userAttr?.get("type")).toBe(AttributeEnum.Relationship);
      });

      test("should create one-way one-to-one relationship", async () => {
        const created = await db.createRelationship({
          collectionId: usersCollectionId,
          relatedCollectionId: postsCollectionId,
          type: RelationEnum.OneToOne,
          id: "featured_post",
          twoWayKey: "featured_user",
          twoWay: false,
        });

        expect(created).toBe(true);

        // Check parent collection has the relationship
        const usersCollection = await db.getCollection(usersCollectionId);
        const userAttributes = usersCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const featuredPostAttr = userAttributes.find(
          (attr) => attr.get("$id") === "featured_post",
        );

        expect(featuredPostAttr).toBeDefined();

        // Check child collection also has the relationship (even for one-way)
        const postsCollection = await db.getCollection(postsCollectionId);
        const postAttributes = postsCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const featuredUserAttr = postAttributes.find(
          (attr) => attr.get("$id") === "featured_user",
        );

        expect(featuredUserAttr).toBeDefined();
      });
    });

    describe("OneToMany relationships", () => {
      test("should create one-to-many relationship", async () => {
        const created = await db.createRelationship({
          collectionId: usersCollectionId,
          relatedCollectionId: postsCollectionId,
          type: RelationEnum.OneToMany,
          id: "posts",
          twoWayKey: "author",
          twoWay: true,
        });

        expect(created).toBe(true);

        // Check parent collection (users)
        const usersCollection = await db.getCollection(usersCollectionId);
        const userAttributes = usersCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const postsAttr = userAttributes.find(
          (attr) => attr.get("$id") === "posts",
        );

        expect(postsAttr).toBeDefined();
        expect(postsAttr?.get("options").relationType).toBe(
          RelationEnum.OneToMany,
        );

        // Check child collection (posts)
        const postsCollection = await db.getCollection(postsCollectionId);
        const postAttributes = postsCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const authorAttr = postAttributes.find(
          (attr) => attr.get("$id") === "author",
        );

        expect(authorAttr).toBeDefined();
        expect(authorAttr?.get("options").relationType).toBe(
          RelationEnum.OneToMany,
        );
      });
    });

    describe("ManyToOne relationships", () => {
      test("should create many-to-one relationship", async () => {
        const created = await db.createRelationship({
          collectionId: postsCollectionId,
          relatedCollectionId: usersCollectionId,
          type: RelationEnum.ManyToOne,
          id: "author",
          twoWayKey: "posts",
          twoWay: true,
        });

        expect(created).toBe(true);

        // Check parent collection (posts)
        const postsCollection = await db.getCollection(postsCollectionId);
        const postAttributes = postsCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const authorAttr = postAttributes.find(
          (attr) => attr.get("$id") === "author",
        );

        expect(authorAttr).toBeDefined();
        expect(authorAttr?.get("options").relationType).toBe(
          RelationEnum.ManyToOne,
        );

        // Check child collection (users)
        const usersCollection = await db.getCollection(usersCollectionId);
        const userAttributes = usersCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const postsAttr = userAttributes.find(
          (attr) => attr.get("$id") === "posts",
        );

        expect(postsAttr).toBeDefined();
        expect(postsAttr?.get("options").relationType).toBe(
          RelationEnum.ManyToOne,
        );
      });
    });

    describe("ManyToMany relationships", () => {
      test("should create many-to-many relationship", async () => {
        const created = await db.createRelationship({
          collectionId: postsCollectionId,
          relatedCollectionId: tagsCollectionId,
          type: RelationEnum.ManyToMany,
          id: "tags",
          twoWayKey: "posts",
          twoWay: true,
        });

        expect(created).toBe(true);

        // Check both collections have the relationship attributes
        const postsCollection = await db.getCollection(postsCollectionId);
        const postAttributes = postsCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const tagsAttr = postAttributes.find(
          (attr) => attr.get("$id") === "tags",
        );

        expect(tagsAttr).toBeDefined();
        expect(tagsAttr?.get("options").relationType).toBe(
          RelationEnum.ManyToMany,
        );

        const tagsCollection = await db.getCollection(tagsCollectionId);
        const tagAttributes = tagsCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const postsAttr = tagAttributes.find(
          (attr) => attr.get("$id") === "posts",
        );

        expect(postsAttr).toBeDefined();
        expect(postsAttr?.get("options").relationType).toBe(
          RelationEnum.ManyToMany,
        );
      });
    });

    describe("relationship validation", () => {
      test("should throw error for non-existent related collection", async () => {
        await expect(
          db.createRelationship({
            collectionId: usersCollectionId,
            relatedCollectionId: "nonexistent_collection",
            type: RelationEnum.OneToMany,
            id: "items",
          }),
        ).rejects.toThrow(NotFoundException);
      });

      test("should throw error for duplicate relationship key", async () => {
        // Create first relationship
        await db.createRelationship({
          collectionId: usersCollectionId,
          relatedCollectionId: postsCollectionId,
          type: RelationEnum.OneToMany,
          id: "posts",
          twoWayKey: "author",
        });

        // Try to create duplicate
        await expect(
          db.createRelationship({
            collectionId: usersCollectionId,
            relatedCollectionId: postsCollectionId,
            type: RelationEnum.OneToMany,
            id: "posts", // Duplicate key
            twoWayKey: "creator",
          }),
        ).rejects.toThrow(DuplicateException);
      });

      test("should use default relationship names", async () => {
        const created = await db.createRelationship({
          collectionId: usersCollectionId,
          relatedCollectionId: postsCollectionId,
          type: RelationEnum.OneToMany,
          // No id or twoWayKey specified
        });

        expect(created).toBe(true);

        const usersCollection = await db.getCollection(usersCollectionId);
        const userAttributes = usersCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const relationAttr = userAttributes.find(
          (attr) => attr.get("$id") === postsCollectionId,
        );

        expect(relationAttr).toBeDefined();
      });

      test("should handle onDelete options", async () => {
        const created = await db.createRelationship({
          collectionId: usersCollectionId,
          relatedCollectionId: postsCollectionId,
          type: RelationEnum.OneToMany,
          id: "posts",
          twoWayKey: "author",
          onDelete: OnDelete.Cascade,
        });

        expect(created).toBe(true);

        const usersCollection = await db.getCollection(usersCollectionId);
        const userAttributes = usersCollection.get(
          "attributes",
        ) as Doc<Attribute>[];
        const postsAttr = userAttributes.find(
          (attr) => attr.get("$id") === "posts",
        );

        expect(postsAttr?.get("options").onDelete).toBe(OnDelete.Cascade);
      });
    });
  });

  describe("updateRelationship", () => {
    beforeEach(async () => {
      // Create a relationship to update
      await db.createRelationship({
        collectionId: usersCollectionId,
        relatedCollectionId: postsCollectionId,
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
        twoWay: true,
        onDelete: OnDelete.Restrict,
      });
    });

    test("should update relationship key names", async () => {
      const updated = await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "posts",
        newKey: "user_posts",
        newTwoWayKey: "post_author",
      });

      expect(updated).toBe(true);

      // Check updated keys
      const usersCollection = await db.getCollection(usersCollectionId);
      const userAttributes = usersCollection.get(
        "attributes",
      ) as Doc<Attribute>[];
      const postsAttr = userAttributes.find(
        (attr) => attr.get("$id") === "user_posts",
      );

      expect(postsAttr).toBeDefined();
      expect(postsAttr?.get("options").twoWayKey).toBe("post_author");

      const postsCollection = await db.getCollection(postsCollectionId);
      const postAttributes = postsCollection.get(
        "attributes",
      ) as Doc<Attribute>[];
      const authorAttr = postAttributes.find(
        (attr) => attr.get("$id") === "post_author",
      );

      expect(authorAttr).toBeDefined();
      expect(authorAttr?.get("options").twoWayKey).toBe("user_posts");
    });

    test("should update onDelete behavior", async () => {
      const updated = await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "posts",
        onDelete: OnDelete.Cascade,
      });

      expect(updated).toBe(true);

      const usersCollection = await db.getCollection(usersCollectionId);
      const userAttributes = usersCollection.get(
        "attributes",
      ) as Doc<Attribute>[];
      const postsAttr = userAttributes.find(
        (attr) => attr.get("$id") === "posts",
      );

      expect(postsAttr?.get("options").onDelete).toBe(OnDelete.Cascade);
    });

    test("should update twoWay setting", async () => {
      const updated = await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "posts",
        twoWay: false,
      });

      expect(updated).toBe(true);

      const usersCollection = await db.getCollection(usersCollectionId);
      const userAttributes = usersCollection.get(
        "attributes",
      ) as Doc<Attribute>[];
      const postsAttr = userAttributes.find(
        (attr) => attr.get("$id") === "posts",
      );

      expect(postsAttr?.get("options").twoWay).toBe(false);
    });

    test("should throw error for ManyToMany relationships", async () => {
      // Create ManyToMany relationship first
      await db.createRelationship({
        collectionId: postsCollectionId,
        relatedCollectionId: tagsCollectionId,
        type: RelationEnum.ManyToMany,
        id: "tags",
        twoWayKey: "posts",
      });

      await expect(
        db.updateRelationship({
          collectionId: postsCollectionId,
          id: "tags",
          newKey: "post_tags",
        }),
      ).rejects.toThrow(DatabaseException);
    });

    test("should throw error for non-existent relationship", async () => {
      await expect(
        db.updateRelationship({
          collectionId: usersCollectionId,
          id: "nonexistent",
          newKey: "new_name",
        }),
      ).rejects.toThrow(NotFoundException);
    });
  });

  describe("deleteRelationship", () => {
    test("should delete OneToMany relationship", async () => {
      // Create relationship
      await db.createRelationship({
        collectionId: usersCollectionId,
        relatedCollectionId: postsCollectionId,
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
      });

      const deleted = await db.deleteRelationship(usersCollectionId, "posts");
      expect(deleted).toBe(true);

      // Check relationship was removed from both collections
      const usersCollection = await db.getCollection(usersCollectionId);
      const userAttributes = usersCollection.get(
        "attributes",
      ) as Doc<Attribute>[];
      const postsAttr = userAttributes.find(
        (attr) => attr.get("$id") === "posts",
      );

      expect(postsAttr).toBeUndefined();

      const postsCollection = await db.getCollection(postsCollectionId);
      const postAttributes = postsCollection.get(
        "attributes",
      ) as Doc<Attribute>[];
      const authorAttr = postAttributes.find(
        (attr) => attr.get("$id") === "author",
      );

      expect(authorAttr).toBeUndefined();
    });

    test("should delete ManyToMany relationship and junction collection", async () => {
      // Create ManyToMany relationship
      await db.createRelationship({
        collectionId: postsCollectionId,
        relatedCollectionId: tagsCollectionId,
        type: RelationEnum.ManyToMany,
        id: "tags",
        twoWayKey: "posts",
      });

      const deleted = await db.deleteRelationship(postsCollectionId, "tags");
      expect(deleted).toBe(true);
    });

    test("should throw error for non-existent relationship", async () => {
      await expect(
        db.deleteRelationship(usersCollectionId, "nonexistent"),
      ).rejects.toThrow(NotFoundException);
    });
  });

  describe("relationship data operations", () => {
    beforeEach(async () => {
      // Create relationships for testing
      await db.createRelationship({
        collectionId: usersCollectionId,
        relatedCollectionId: postsCollectionId,
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
        twoWay: true,
      });
    });

    test("should create documents with relationship data", async () => {
      // Create a user
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "John Doe",
          email: "john@example.com",
        }),
      );

      // Create a post with author relationship
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "My First Post",
          content: "Hello World!",
          author: user.getId(),
        }),
      );
      const updatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        (qb) => qb.populate("*"),
      );
      expect(updatedPost.get("author")?.getId()).toBe(user.getId());
    });

    test("should handle relationship queries with populate", async () => {
      // Create test data
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Jane Smith",
          email: "jane@example.com",
        }),
      );

      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Jane's Post",
          content: "Content here",
          author: user.getId(),
        }),
      );

      // Query with populate
      const populatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        [Query.populate("author", [])],
      );

      expect(populatedPost.get("author")).toBeDefined();
      // The populated author should have user data
      const authorData = populatedPost.get("author");
      if (authorData && typeof authorData === "object") {
        expect(authorData.has("name")).toBeTruthy();
        expect(authorData.has("email")).toBeTruthy;
      }
    });

    test("should handle cascade deletion", async () => {
      // Update relationship to cascade delete
      await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "posts",
        onDelete: OnDelete.Cascade,
      });

      // Create test data
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Test User",
          email: "test@example.com",
        }),
      );

      const post1 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 1",
          author: user.getId(),
        }),
      );

      const post2 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 2",
          author: user.getId(),
        }),
      );

      // Delete user (should cascade to posts)
      await db.deleteDocument(usersCollectionId, user.getId());

      // Posts should be deleted too
      const remainingPost1 = await db.getDocument(
        postsCollectionId,
        post1.getId(),
      );
      const remainingPost2 = await db.getDocument(
        postsCollectionId,
        post2.getId(),
      );

      expect(remainingPost1.empty()).toBe(true);
      expect(remainingPost2.empty()).toBe(true);
    });

    test("should handle restrict deletion", async () => {
      // Default is restrict
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Protected User",
          email: "protected@example.com",
        }),
      );

      await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Protected Post",
          author: user.getId(),
        }),
      );

      // Should not be able to delete user with related posts
      await expect(
        db.deleteDocument(usersCollectionId, user.getId()),
      ).rejects.toThrow(RelationshipException);
    });

    test("should handle set null deletion", async () => {
      // Update relationship to set null
      await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "posts",
        onDelete: OnDelete.SetNull,
      });

      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Nullable User",
          email: "nullable@example.com",
        }),
      );

      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Orphaned Post",
          author: user.getId(),
        }),
      );

      // Delete user
      await db.deleteDocument(usersCollectionId, user.getId());

      // Post should still exist but author should be null
      const orphanedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
      );
      expect(orphanedPost.empty()).toBe(false);
      expect(orphanedPost.get("author")).toBeNull();
    });
  });

  describe("edge cases", () => {
    // test('should handle relationships with same collection', async () => {
    //     // Self-referential relationship (e.g., user -> manager)
    //     const created = await db.createRelationship({
    //         collectionId: usersCollectionId,
    //         relatedCollectionId: usersCollectionId,
    //         type: RelationEnum.ManyToOne,
    //         id: 'manager',
    //         twoWayKey: 'subordinates'
    //     });

    //     expect(created).toBe(true);

    //     const collection = await db.getCollection(usersCollectionId);
    //     const attributes = collection.get('attributes') as Doc<Attribute>[];

    //     const managerAttr = attributes.find(attr => attr.get('$id') === 'manager');
    //     const subordinatesAttr = attributes.find(attr => attr.get('$id') === 'subordinates');

    //     expect(managerAttr).toBeDefined();
    //     expect(subordinatesAttr).toBeDefined();
    // });

    test("should handle multiple relationships between same collections", async () => {
      // Create first relationship
      await db.createRelationship({
        collectionId: usersCollectionId,
        relatedCollectionId: postsCollectionId,
        type: RelationEnum.OneToMany,
        id: "authored_posts",
        twoWayKey: "author",
      });

      // Create second relationship
      await db.createRelationship({
        collectionId: usersCollectionId,
        relatedCollectionId: postsCollectionId,
        type: RelationEnum.OneToMany,
        id: "reviewed_posts",
        twoWayKey: "reviewer",
      });

      const usersCollection = await db.getCollection(usersCollectionId);
      const userAttributes = usersCollection.get(
        "attributes",
      ) as Doc<Attribute>[];

      const authoredAttr = userAttributes.find(
        (attr) => attr.get("$id") === "authored_posts",
      );
      const reviewedAttr = userAttributes.find(
        (attr) => attr.get("$id") === "reviewed_posts",
      );

      expect(authoredAttr).toBeDefined();
      expect(reviewedAttr).toBeDefined();
    });
  });
});
