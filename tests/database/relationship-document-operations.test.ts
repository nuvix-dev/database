import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "../helpers.js";
import { Doc } from "@core/doc.js";
import { Query } from "@core/query.js";
import { AttributeEnum, RelationEnum, OnDelete } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";
import { RelationshipException } from "@errors/index.js";

describe("Relationship Document Operations", () => {
  let db: Database;
  let usersCollectionId: string;
  let postsCollectionId: string;
  let tagsCollectionId: string;
  let commentsCollectionId: string;

  const schema = new Date().getTime().toString();

  beforeEach(async () => {
    db = createTestDb({ namespace: `rel_doc_op_${schema}` });
    db.setMeta({ schema });
    await db.create();

    const timestamp = Date.now();
    usersCollectionId = `users_${timestamp}`;
    postsCollectionId = `posts_${timestamp}`;
    tagsCollectionId = `tags_${timestamp}`;
    commentsCollectionId = `comments_${timestamp}`;

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

    // Create Comments collection
    await db.createCollection({
      id: commentsCollectionId,
      attributes: [
        new Doc<Attribute>({
          $id: "content",
          key: "content",
          type: AttributeEnum.String,
          size: 1000,
          required: true,
        }),
      ],
    });
  });

  afterEach(async () => {
    await db.delete();
  });

  describe("OneToOne Relationships", () => {
    beforeEach(async () => {
      // Create OneToOne relationship: User -> Profile (Post)
      await db.createRelationship({
        collectionId: usersCollectionId,
        relatedCollectionId: postsCollectionId,
        type: RelationEnum.OneToOne,
        id: "profile",
        twoWayKey: "user",
        twoWay: true,
      });
    });

    test("should create documents with OneToOne relationship using documentId", async () => {
      // Create a user
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "John Doe",
          email: "john@example.com",
        }),
      );

      // Create a profile post linked to the user
      const profile = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "John's Profile",
          content: "About me...",
          user: user.getId(), // OneToOne: pass documentId
        }),
      );

      const updatedProfile = await db.getDocument(
        postsCollectionId,
        profile.getId(),
        (qb) => qb.populate("*"),
      );

      expect(updatedProfile.get("user")?.getId()).toBe(user.getId());

      // Verify two-way relationship
      const userWithProfile = await db.getDocument(
        usersCollectionId,
        user.getId(),
        [Query.populate("profile", [])],
      );
      expect(userWithProfile.get("profile")).toBeDefined();
    });

    test("should create documents with OneToOne relationship set to null", async () => {
      // Create a post without user relationship
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Orphaned Post",
          content: "No user linked",
          user: null, // OneToOne: pass null
        }),
      );

      expect(post.get("user")).toBeNull();
    });

    test("should update OneToOne relationship to different document", async () => {
      // Create users and posts
      const user1 = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "User 1",
          email: "user1@example.com",
        }),
      );

      const user2 = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "User 2",
          email: "user2@example.com",
        }),
      );

      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Shared Post",
          content: "Content",
          user: user1.getId(),
        }),
      );

      // Update relationship to user2
      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          user: user2.getId(), // Update to different documentId
        }),
      );

      const updatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        (qb) => qb.populate("*"),
      );

      expect(updatedPost.get("user")?.getId()).toBe(user2.getId());

      // Verify old relationship is cleared
      const user1WithProfile = await db.getDocument(
        usersCollectionId,
        user1.getId(),
        [Query.populate("profile", [])],
      );
      expect(user1WithProfile.get("profile")).toBeNull();
    });

    test("should update OneToOne relationship to null", async () => {
      // Create user and post
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "User",
          email: "user@example.com",
        }),
      );

      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "User Post",
          content: "Content",
          user: user.getId(),
        }),
      );

      // Update relationship to null
      const updatedPost = await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          user: null, // Set to null
        }),
      );

      expect(updatedPost.get("user")).toBeNull();

      // Verify user's profile is cleared
      const userWithProfile = await db.getDocument(
        usersCollectionId,
        user.getId(),
        [Query.populate("profile", [])],
      );
      expect(userWithProfile.get("profile")).toBeNull();
    });

    test("should handle OneToOne onDelete restrict", async () => {
      // Create relationship with restrict delete
      await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "profile",
        onDelete: OnDelete.Restrict,
      });

      // Create user and profile
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
          title: "Protected Profile",
          content: "Profile content",
          user: user.getId(),
        }),
      );

      // Should not be able to delete user with related profile
      await expect(
        db.deleteDocument(usersCollectionId, user.getId()),
      ).rejects.toThrow(RelationshipException);
    });

    test("should handle OneToOne onDelete cascade", async () => {
      // Create relationship with cascade delete
      await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "profile",
        onDelete: OnDelete.Cascade,
      });

      // Create user and profile
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Cascade User",
          email: "cascade@example.com",
        }),
      );

      const profile = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Cascade Profile",
          content: "Profile content",
          user: user.getId(),
        }),
      );

      // Delete user (should cascade to profile)
      await db.deleteDocument(usersCollectionId, user.getId());

      // Profile should be deleted
      const deletedProfile = await db.getDocument(
        postsCollectionId,
        profile.getId(),
      );
      expect(deletedProfile.empty()).toBe(true);
    });

    test("should handle OneToOne onDelete setNull", async () => {
      // Create relationship with set null delete
      await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "profile",
        onDelete: OnDelete.SetNull,
      });

      // Create user and profile
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "SetNull User",
          email: "setnull@example.com",
        }),
      );

      const profile = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "SetNull Profile",
          content: "Profile content",
          user: user.getId(),
        }),
      );

      // Delete user
      await db.deleteDocument(usersCollectionId, user.getId());

      // Profile should still exist but user should be null
      const orphanedProfile = await db.getDocument(
        postsCollectionId,
        profile.getId(),
      );
      expect(orphanedProfile.empty()).toBe(false);
      expect(orphanedProfile.get("user")).toBeNull();
    });
  });

  describe("OneToMany Relationships", () => {
    beforeEach(async () => {
      // Create OneToMany relationship: User -> Posts
      await db.createRelationship({
        collectionId: usersCollectionId,
        relatedCollectionId: postsCollectionId,
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
        twoWay: true,
      });
    });

    test("should create documents with OneToMany relationship using {set: [...ids]}", async () => {
      // Create user
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Author",
          email: "author@example.com",
        }),
      );

      // Create posts
      const post1 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 1",
          content: "Content 1",
        }),
      );

      const post2 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 2",
          content: "Content 2",
        }),
      );

      const post3 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 3",
          content: "Content 3",
        }),
      );

      // Update user to set posts relationship
      console.log(
        await db.updateDocument(
          usersCollectionId,
          user.getId(),
          new Doc({
            posts: { set: [post1.getId(), post2.getId()] }, // OneToMany: {set: [...ids]}
          }),
        ),
      );

      let updatedUser = await db.getDocument(
        usersCollectionId,
        user.getId(),
        (qb) => qb.populate("posts"),
      );

      expect(updatedUser.get("posts").map((post: any) => post.getId())).toEqual(
        [post1.getId(), post2.getId()],
      );

      await db.updateDocument(
        usersCollectionId,
        user.getId(),
        new Doc({
          posts: { connect: [post3.getId()] },
        }),
      );

      updatedUser = await db.getDocument(
        usersCollectionId,
        user.getId(),
        (qb) => qb.populate("posts"),
      );

      expect(updatedUser.get("posts").map((post: any) => post.getId())).toEqual(
        [post1.getId(), post2.getId(), post3.getId()],
      );
    });

    test("should create documents with OneToMany relationship using empty set", async () => {
      // Create user with no posts
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Author",
          email: "author@example.com",
          posts: { set: [] }, // Empty set
        }),
      );
      const updatedUser = await db.getDocument(
        usersCollectionId,
        user.getId(),
        (qb) => qb.populate("posts"),
      );
      expect(updatedUser.get("posts")).toEqual([]);
    });

    test("should update OneToMany relationship with {set: []}", async () => {
      // Create user with posts
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Author",
          email: "author@example.com",
        }),
      );

      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post",
          content: "Content",
          author: user.getId(),
        }),
      );

      // Clear all posts
      await db.updateDocument(
        usersCollectionId,
        user.getId(),
        new Doc({
          posts: { set: [] }, // Clear all relationships
        }),
      );

      const updatedUser = await db.getDocument(
        usersCollectionId,
        user.getId(),
        (qb) => qb.populate("posts"),
      );

      expect(updatedUser.get("posts")).toEqual([]);

      // Verify post's author is cleared
      const updatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        (qb) => qb.populate("author"),
      );
      expect(updatedPost.get("author")).toBeNull();
    });

    test("should update OneToMany relationship with {connect: [], disconnect: []}", async () => {
      // Create user and posts
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Author",
          email: "author@example.com",
        }),
      );

      const post1 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 1",
          content: "Content 1",
        }),
      );

      const post2 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 2",
          content: "Content 2",
        }),
      );

      const post3 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 3",
          content: "Content 3",
        }),
      );

      // Connect posts
      await db.updateDocument(
        usersCollectionId,
        user.getId(),
        new Doc({
          posts: { connect: [post1.getId(), post2.getId()] },
        }),
      );

      await db.getDocument(usersCollectionId, user.getId(), [
        Query.populate("posts", []),
      ]);
      let updatedUser = await db.getDocument(
        usersCollectionId,
        user.getId(),
        (qb) => qb.populate("posts"),
      );
      expect(updatedUser.get("posts")).toHaveLength(2);

      // Connect and disconnect
      await db.updateDocument(
        usersCollectionId,
        user.getId(),
        new Doc({
          posts: {
            connect: [post3.getId()],
            disconnect: [post1.getId()],
          },
        }),
      );

      updatedUser = await db.getDocument(usersCollectionId, user.getId(), [
        Query.populate("posts", []),
      ]);
      expect(updatedUser.get("posts")).toHaveLength(2);
      const postIds = updatedUser.get("posts").map((p: any) => p.getId());
      expect(postIds).toContain(post2.getId());
      expect(postIds).toContain(post3.getId());
      expect(postIds).not.toContain(post1.getId());
    });

    test("should handle OneToMany onDelete restrict", async () => {
      // Create relationship with restrict delete
      await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "posts",
        onDelete: OnDelete.Restrict,
      });

      // Create user with posts
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Protected Author",
          email: "protected@example.com",
        }),
      );

      await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Protected Post",
          content: "Content",
          author: user.getId(),
        }),
      );

      // Should not be able to delete user with related posts
      await expect(
        db.deleteDocument(usersCollectionId, user.getId()),
      ).rejects.toThrow(RelationshipException);
    });

    test("should handle OneToMany onDelete cascade", async () => {
      // Create relationship with cascade delete
      await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "posts",
        onDelete: OnDelete.Cascade,
      });

      // Create user with posts
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Cascade Author",
          email: "cascade@example.com",
        }),
      );

      const post1 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 1",
          content: "Content 1",
          author: user.getId(),
        }),
      );

      const post2 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 2",
          content: "Content 2",
          author: user.getId(),
        }),
      );

      // Delete user (should cascade to posts)
      await db.deleteDocument(usersCollectionId, user.getId());

      // Posts should be deleted
      const deletedPost1 = await db.getDocument(
        postsCollectionId,
        post1.getId(),
      );
      const deletedPost2 = await db.getDocument(
        postsCollectionId,
        post2.getId(),
      );
      expect(deletedPost1.empty()).toBe(true);
      expect(deletedPost2.empty()).toBe(true);
    });

    test("should handle OneToMany onDelete setNull", async () => {
      // Create relationship with set null delete
      await db.updateRelationship({
        collectionId: usersCollectionId,
        id: "posts",
        onDelete: OnDelete.SetNull,
      });

      // Create user with posts
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "SetNull Author",
          email: "setnull@example.com",
        }),
      );

      const post1 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 1",
          content: "Content 1",
          author: user.getId(),
        }),
      );

      const post2 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 2",
          content: "Content 2",
          author: user.getId(),
        }),
      );

      // Delete user
      await db.deleteDocument(usersCollectionId, user.getId());

      // Posts should still exist but author should be null
      const orphanedPost1 = await db.getDocument(
        postsCollectionId,
        post1.getId(),
      );
      const orphanedPost2 = await db.getDocument(
        postsCollectionId,
        post2.getId(),
      );
      expect(orphanedPost1.empty()).toBe(false);
      expect(orphanedPost1.get("author")).toBeNull();
      expect(orphanedPost2.empty()).toBe(false);
      expect(orphanedPost2.get("author")).toBeNull();
    });
  });

  describe("ManyToOne Relationships", () => {
    beforeEach(async () => {
      // Create ManyToOne relationship: Posts -> Author (User)
      await db.createRelationship({
        collectionId: postsCollectionId,
        relatedCollectionId: usersCollectionId,
        type: RelationEnum.ManyToOne,
        id: "author",
        twoWayKey: "posts",
        twoWay: true,
      });
    });

    test("should create documents with ManyToOne relationship using documentId", async () => {
      // Create user
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Author",
          email: "author@example.com",
        }),
      );

      // Create post with author relationship
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post",
          content: "Content",
          author: user.getId(), // ManyToOne: pass documentId
        }),
      );

      const updatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        (qb) => qb.populate("*"),
      );

      expect(updatedPost.get("author")?.getId()).toBe(user.getId());

      // Verify two-way relationship
      const userWithPosts = await db.getDocument(
        usersCollectionId,
        user.getId(),
        [Query.populate("posts", [])],
      );
      expect(userWithPosts.get("posts")).toHaveLength(1);
    });

    test("should create documents with ManyToOne relationship set to null", async () => {
      // Create post without author
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Anonymous Post",
          content: "Content",
          author: null, // ManyToOne: pass null
        }),
      );

      expect(post.get("author")).toBeNull();
    });

    test("should update ManyToOne relationship to different document", async () => {
      // Create users
      const user1 = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Author 1",
          email: "author1@example.com",
        }),
      );

      const user2 = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Author 2",
          email: "author2@example.com",
        }),
      );

      // Create post with user1
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post",
          content: "Content",
          author: user1.getId(),
        }),
      );

      // Update to user2
      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          author: user2.getId(), // Update to different documentId
        }),
      );

      const updatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        (qb) => qb.populate("*"),
      );

      expect(updatedPost.get("author")?.getId()).toBe(user2.getId());

      // Verify relationships are updated
      const user1WithPosts = await db.getDocument(
        usersCollectionId,
        user1.getId(),
        [Query.populate("posts", [])],
      );
      const user2WithPosts = await db.getDocument(
        usersCollectionId,
        user2.getId(),
        [Query.populate("posts", [])],
      );
      expect(user1WithPosts.get("posts")).toHaveLength(0);
      expect(user2WithPosts.get("posts")).toHaveLength(1);
    });

    test("should update ManyToOne relationship to null", async () => {
      // Create user and post
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Author",
          email: "author@example.com",
        }),
      );

      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post",
          content: "Content",
          author: user.getId(),
        }),
      );

      // Update to null
      const updatedPost = await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          author: null, // Set to null
        }),
      );

      expect(updatedPost.get("author")).toBeNull();

      // Verify user's posts are updated
      const userWithPosts = await db.getDocument(
        usersCollectionId,
        user.getId(),
        [Query.populate("posts", [])],
      );
      expect(userWithPosts.get("posts")).toHaveLength(0);
    });

    test("should handle ManyToOne onDelete restrict", async () => {
      // Create relationship with restrict delete
      await db.updateRelationship({
        collectionId: postsCollectionId,
        id: "author",
        onDelete: OnDelete.Restrict,
      });

      // Create user and post
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Protected Author",
          email: "protected@example.com",
        }),
      );

      await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Protected Post",
          content: "Content",
          author: user.getId(),
        }),
      );

      // Should not be able to delete user with related posts
      await expect(
        db.deleteDocument(usersCollectionId, user.getId()),
      ).rejects.toThrow(RelationshipException);
    });

    test("should handle ManyToOne onDelete cascade", async () => {
      // Create relationship with cascade delete
      await db.updateRelationship({
        collectionId: postsCollectionId,
        id: "author",
        onDelete: OnDelete.Cascade,
      });

      // Create user and post
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Cascade Author",
          email: "cascade@example.com",
        }),
      );

      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Cascade Post",
          content: "Content",
          author: user.getId(),
        }),
      );

      // Delete user (should cascade to post)
      await db.deleteDocument(usersCollectionId, user.getId());

      // Post should be deleted
      const deletedPost = await db.getDocument(postsCollectionId, post.getId());
      expect(deletedPost.empty()).toBe(true);
    });

    test("should handle ManyToOne onDelete setNull", async () => {
      // Create relationship with set null delete
      await db.updateRelationship({
        collectionId: postsCollectionId,
        id: "author",
        onDelete: OnDelete.SetNull,
      });

      // Create user and post
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "SetNull Author",
          email: "setnull@example.com",
        }),
      );

      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "SetNull Post",
          content: "Content",
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

  describe("ManyToMany Relationships", () => {
    beforeEach(async () => {
      // Create ManyToMany relationship: Posts <-> Tags
      await db.createRelationship({
        collectionId: postsCollectionId,
        relatedCollectionId: tagsCollectionId,
        type: RelationEnum.ManyToMany,
        id: "tags",
        twoWayKey: "posts",
        twoWay: true,
      });
    });

    test("should create documents with ManyToMany relationship using {set: [...ids]}", async () => {
      // Create post
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Tagged Post",
          content: "Content",
        }),
      );

      // Create tags
      const tag1 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "JavaScript",
          color: "#f7df1e",
        }),
      );

      const tag2 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "TypeScript",
          color: "#3178c6",
        }),
      );

      // Update post to set tags relationship
      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          tags: { set: [tag1.getId(), tag2.getId()] }, // ManyToMany: {set: [...ids]}
        }),
      );

      const updatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        (qb) => qb.populate("tags"),
      );

      expect(updatedPost.get("tags").map((tag: any) => tag.getId())).toEqual([
        tag1.getId(),
        tag2.getId(),
      ]);
    });

    test("should create documents with ManyToMany relationship using empty set", async () => {
      // Create post with no tags
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Untagged Post",
          content: "Content",
          tags: { set: [] }, // Empty set
        }),
      );

      const updatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        (qb) => qb.populate("tags"),
      );

      expect(updatedPost.get("tags")).toEqual([]);
    });

    test("should update ManyToMany relationship with {set: []}", async () => {
      // Create post with tags
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post",
          content: "Content",
        }),
      );

      const tag = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Test Tag",
          color: "#000000",
        }),
      );

      // Set tags
      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          tags: { set: [tag.getId()] },
        }),
      );

      // Clear all tags
      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          tags: { set: [] }, // Clear all relationships
        }),
      );
      const updatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        (qb) => qb.populate("tags"),
      );
      expect(updatedPost.get("tags")).toEqual([]);

      // Verify tag's posts are cleared
      const updatedTag = await db.getDocument(tagsCollectionId, tag.getId(), [
        Query.populate("posts", []),
      ]);
      expect(updatedTag.get("posts")).toHaveLength(0);
    });

    test("should update ManyToMany relationship with {connect: [], disconnect: []}", async () => {
      // Create post and tags
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post",
          content: "Content",
        }),
      );

      const tag1 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 1",
          color: "#ff0000",
        }),
      );

      const tag2 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 2",
          color: "#00ff00",
        }),
      );

      const tag3 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 3",
          color: "#0000ff",
        }),
      );

      // Connect tags
      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          tags: { connect: [tag1.getId(), tag2.getId()] },
        }),
      );

      let updatedPost = await db.getDocument(postsCollectionId, post.getId(), [
        Query.populate("tags", []),
      ]);
      expect(updatedPost.get("tags")).toHaveLength(2);

      // Connect and disconnect
      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          tags: {
            connect: [tag3.getId()],
            disconnect: [tag1.getId()],
          },
        }),
      );

      updatedPost = await db.getDocument(postsCollectionId, post.getId(), [
        Query.populate("tags", []),
      ]);
      expect(updatedPost.get("tags")).toHaveLength(2);
      const tagIds = updatedPost.get("tags").map((t: any) => t.getId());
      expect(tagIds).toContain(tag2.getId());
      expect(tagIds).toContain(tag3.getId());
      expect(tagIds).not.toContain(tag1.getId());
    });

    test("should handle ManyToMany onDelete restrict", async () => {
      // Create relationship with restrict delete
      await db.updateRelationship({
        collectionId: postsCollectionId,
        id: "tags",
        onDelete: OnDelete.Restrict,
      });

      // Create post with tags
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Protected Post",
          content: "Content",
        }),
      );

      const tag = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Protected Tag",
          color: "#000000",
        }),
      );

      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          tags: { set: [tag.getId()] },
        }),
      );

      // Should not be able to delete post with related tags
      await expect(
        db.deleteDocument(postsCollectionId, post.getId()),
      ).rejects.toThrow(RelationshipException);
    });

    test("should handle ManyToMany onDelete cascade", async () => {
      // Create relationship with cascade delete
      await db.updateRelationship({
        collectionId: postsCollectionId,
        id: "tags",
        onDelete: OnDelete.Cascade,
      });

      // Create post with tags
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Cascade Post",
          content: "Content",
        }),
      );

      const tag1 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 1",
          color: "#ff0000",
        }),
      );

      const tag2 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 2",
          color: "#00ff00",
        }),
      );

      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          tags: { set: [tag1.getId(), tag2.getId()] },
        }),
      );

      // Delete post (should cascade to tags)
      await db.deleteDocument(postsCollectionId, post.getId());

      // Tags should be deleted
      const deletedTag1 = await db.getDocument(tagsCollectionId, tag1.getId());
      const deletedTag2 = await db.getDocument(tagsCollectionId, tag2.getId());
      expect(deletedTag1.empty()).toBe(true);
      expect(deletedTag2.empty()).toBe(true);
    });

    test("should handle ManyToMany onDelete setNull", async () => {
      // Create relationship with set null delete
      await db.updateRelationship({
        collectionId: postsCollectionId,
        id: "tags",
        onDelete: OnDelete.SetNull,
      });

      // Create post with tags
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "SetNull Post",
          content: "Content",
        }),
      );

      const tag1 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 1",
          color: "#ff0000",
        }),
      );

      const tag2 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 2",
          color: "#00ff00",
        }),
      );

      await db.updateDocument(
        postsCollectionId,
        post.getId(),
        new Doc({
          tags: { set: [tag1.getId(), tag2.getId()] },
        }),
      );

      // Delete post
      await db.deleteDocument(postsCollectionId, post.getId());

      // Tags should still exist but posts should be cleared
      const orphanedTag1 = await db.getDocument(
        tagsCollectionId,
        tag1.getId(),
        [Query.populate("posts", [])],
      );
      const orphanedTag2 = await db.getDocument(
        tagsCollectionId,
        tag2.getId(),
        [Query.populate("posts", [])],
      );
      expect(orphanedTag1.empty()).toBe(false);
      expect(orphanedTag1.get("posts")).toHaveLength(0);
      expect(orphanedTag2.empty()).toBe(false);
      expect(orphanedTag2.get("posts")).toHaveLength(0);
    });
  });

  describe("Complex Relationship Scenarios", () => {
    test("should handle multiple relationship types on same collection", async () => {
      // Create multiple relationships on posts collection
      await db.createRelationship({
        collectionId: postsCollectionId,
        relatedCollectionId: usersCollectionId,
        type: RelationEnum.ManyToOne,
        id: "author",
        twoWayKey: "authored_posts",
      });

      await db.createRelationship({
        collectionId: postsCollectionId,
        relatedCollectionId: usersCollectionId,
        type: RelationEnum.ManyToOne,
        id: "reviewer",
        twoWayKey: "reviewed_posts",
      });

      await db.createRelationship({
        collectionId: postsCollectionId,
        relatedCollectionId: tagsCollectionId,
        type: RelationEnum.ManyToMany,
        id: "tags",
        twoWayKey: "tagged_posts",
      });

      // Create documents
      const author = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Author",
          email: "author@example.com",
        }),
      );

      const reviewer = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "Reviewer",
          email: "reviewer@example.com",
        }),
      );

      const tag = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tech",
          color: "#333333",
        }),
      );

      // Create post with multiple relationships
      const post = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Complex Post",
          content: "Content with multiple relationships",
          author: author.getId(), // ManyToOne
          reviewer: reviewer.getId(), // ManyToOne
          tags: { set: [tag.getId()] }, // ManyToMany
        }),
      );

      console.log("Created Post:", post);

      const updatedPost = await db.getDocument(
        postsCollectionId,
        post.getId(),
        (qb) => qb.populate("*"),
      );

      expect(updatedPost.get("author")?.getId()).toBe(author.getId());
      expect(updatedPost.get("reviewer")?.getId()).toBe(reviewer.getId());
      expect(updatedPost.get("tags").map((t: any) => t.getId())).toEqual([
        tag.getId(),
      ]);
    });

    test("should handle relationship updates with mixed operations and multiple relationships", async () => {
      // Create OneToMany relationship
      await db.createRelationship({
        collectionId: usersCollectionId,
        relatedCollectionId: postsCollectionId,
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
      });

      // Create ManyToMany relationship
      await db.createRelationship({
        collectionId: usersCollectionId,
        relatedCollectionId: tagsCollectionId,
        type: RelationEnum.ManyToMany,
        id: "favorite_tags",
        twoWayKey: "favorited_by",
      });

      // Create user
      const user = await db.createDocument(
        usersCollectionId,
        new Doc({
          name: "User",
          email: "user@example.com",
        }),
      );

      // Create posts
      const post1 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 1",
          content: "Content 1",
        }),
      );

      const post2 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 2",
          content: "Content 2",
        }),
      );

      const post3 = await db.createDocument(
        postsCollectionId,
        new Doc({
          title: "Post 3",
          content: "Content 3",
        }),
      );

      // Create tags
      const tag1 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 1",
          color: "#ff0000",
        }),
      );

      const tag2 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 2",
          color: "#00ff00",
        }),
      );

      const tag3 = await db.createDocument(
        tagsCollectionId,
        new Doc({
          name: "Tag 3",
          color: "#0000ff",
        }),
      );

      // Set initial posts and tags
      await db.updateDocument(
        usersCollectionId,
        user.getId(),
        new Doc({
          posts: { set: [post1.getId(), post2.getId()] },
          favorite_tags: { set: [tag1.getId(), tag2.getId()] },
        }),
      );

      // Verify initial state
      let updatedUser = await db.getDocument(
        usersCollectionId,
        user.getId(),
        (qb) => qb.populate("posts").populate("favorite_tags"),
      );
      expect(updatedUser.get("posts")).toHaveLength(2);
      expect(updatedUser.get("favorite_tags")).toHaveLength(2);

      // Update posts with connect/disconnect - favorite_tags should remain unchanged
      await db.updateDocument(
        usersCollectionId,
        user.getId(),
        new Doc({
          posts: {
            connect: [post3.getId()],
            disconnect: [post1.getId()],
          },
        }),
      );

      // Verify posts updated but favorite_tags unchanged
      updatedUser = await db.getDocument(
        usersCollectionId,
        user.getId(),
        (qb) => qb.populate("posts").populate("favorite_tags"),
      );
      expect(updatedUser.get("posts")).toHaveLength(2);
      const postIds = updatedUser.get("posts").map((p: any) => p.getId());
      expect(postIds).toContain(post2.getId());
      expect(postIds).toContain(post3.getId());
      expect(postIds).not.toContain(post1.getId());

      // Verify favorite_tags were NOT reset
      expect(updatedUser.get("favorite_tags")).toHaveLength(2);
      const tagIds = updatedUser
        .get("favorite_tags")
        .map((t: any) => t.getId());
      expect(tagIds).toContain(tag1.getId());
      expect(tagIds).toContain(tag2.getId());

      // Update favorite_tags with connect/disconnect - posts should remain unchanged
      await db.updateDocument(
        usersCollectionId,
        user.getId(),
        new Doc({
          favorite_tags: {
            connect: [tag3.getId()],
            disconnect: [tag1.getId()],
          },
        }),
      );

      // Verify favorite_tags updated but posts unchanged
      updatedUser = await db.getDocument(
        usersCollectionId,
        user.getId(),
        (qb) => qb.populate("posts").populate("favorite_tags"),
      );
      expect(updatedUser.get("favorite_tags")).toHaveLength(2);
      const finalTagIds = updatedUser
        .get("favorite_tags")
        .map((t: any) => t.getId());
      expect(finalTagIds).toContain(tag2.getId());
      expect(finalTagIds).toContain(tag3.getId());
      expect(finalTagIds).not.toContain(tag1.getId());

      // Verify posts were NOT reset
      expect(updatedUser.get("posts")).toHaveLength(2);
      const finalPostIds = updatedUser.get("posts").map((p: any) => p.getId());
      expect(finalPostIds).toContain(post2.getId());
      expect(finalPostIds).toContain(post3.getId());
    });
  });
});
