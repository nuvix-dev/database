import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { createTestDb } from "./helpers.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, RelationEnum, OnDelete } from "@core/enums.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";

const ns = `db_relations_${Date.now()}`;

describe("Database - relationships", () => {
  const db = createTestDb({ namespace: ns });

  beforeAll(async () => {
    await db.create("public");

    // Create users collection
    await db.createCollection({
      id: "users",
      attributes: [
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
        }),
      ],
      permissions: [Permission.create(Role.any())],
    });

    // Create posts collection
    await db.createCollection({
      id: "posts",
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

  afterAll(async () => {
    await db.getAdapter().$client.disconnect();
  });

  it("creates a one-to-many relationship between users and posts", async () => {
    await db.createRelationship({
      collectionId: "users",
      relatedCollectionId: "posts",
      type: RelationEnum.OneToMany,
      twoWay: true,
      id: "posts",
      twoWayKey: "author",
      onDelete: OnDelete.Cascade,
    });

    const usersCollection = await db.getCollection("users");
    const postsCollection = await db.getCollection("posts");

    // Check that relationship attributes were added
    const userPostsAttr = usersCollection
      .get("attributes")
      .find((attr: any) => attr.get("$id") === "posts");
    const postAuthorAttr = postsCollection
      .get("attributes")
      .find((attr: any) => attr.get("$id") === "author");

    expect(userPostsAttr).toBeTruthy();
    expect(postAuthorAttr).toBeTruthy();
    expect(userPostsAttr?.get("type")).toBe(AttributeEnum.Relationship);
    expect(postAuthorAttr?.get("type")).toBe(AttributeEnum.Relationship);
  });

  it("updates a relationship", async () => {
    await db.updateRelationship({
      collectionId: "users",
      id: "posts",
      newKey: "user_posts",
      newTwoWayKey: "user",
      twoWay: false,
    });

    const usersCollection = await db.getCollection("users");
    const userPostsAttr = usersCollection
      .get("attributes")
      .find((attr: any) => attr.get("$id") === "user_posts");

    expect(userPostsAttr).toBeTruthy();
  });
});
