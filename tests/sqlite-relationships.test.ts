import { afterEach, describe, expect, test } from "bun:test";
import { SQLiteSqlBuilder } from "@adapters/sqlite-sql-builder.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, OnDelete, RelationEnum } from "@core/enums.js";
import type { Database } from "@core/database.js";
import {
  AuthorizationException,
  DatabaseException,
  RelationshipException,
} from "@errors/index.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import type { Attribute } from "@validators/schema.js";
import { createSQLiteTestDb } from "./sqlite-helpers.js";

const databases: Database[] = [];

afterEach(async () => {
  await Promise.all(
    databases
      .splice(0)
      .map((database) => database.getAdapter().$client.disconnect()),
  );
});

function attribute(key: string): Doc<Attribute> {
  return new Doc<Attribute>({
    $id: key,
    key,
    type: AttributeEnum.String,
    size: 128,
  });
}

async function setup(
  namespace: string,
  options: { sharedTables?: boolean; tenantId?: number } = {},
): Promise<Database> {
  const database = createSQLiteTestDb({ namespace, ...options });
  databases.push(database);
  await database.create("main");
  return database;
}

async function collections(
  database: Database,
  first: string,
  second: string,
): Promise<void> {
  await database.createCollection({
    id: first,
    attributes: [attribute("name")],
  });
  await database.createCollection({
    id: second,
    attributes: [attribute("name")],
  });
}

function ids(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((document) => (document as Doc).getId()).sort();
}

describe("SQLite relationship parity", () => {
  describe("relationship schema and deterministic names", () => {
    test("creates, renames, and deletes two-way relationship columns for all non-junction types", async () => {
      // Arrange
      const database = await setup("relationship-schema");
      const cases = [
        [
          RelationEnum.OneToOne,
          "one_a",
          "one_b",
          "partner",
          "owner",
          true,
          true,
        ],
        [
          RelationEnum.OneToMany,
          "many_a",
          "many_b",
          "children",
          "parent",
          false,
          true,
        ],
        [
          RelationEnum.ManyToOne,
          "owner_a",
          "owner_b",
          "parent",
          "children",
          true,
          false,
        ],
      ] as const;

      // Act
      for (const [
        type,
        first,
        second,
        key,
        inverse,
        firstPhysical,
        secondPhysical,
      ] of cases) {
        await collections(database, first, second);
        await database.createRelationship({
          collectionId: first,
          relatedCollectionId: second,
          type,
          id: key,
          twoWayKey: inverse,
          twoWay: true,
        });
        await database.updateRelationship({
          collectionId: first,
          id: key,
          newKey: `${key}_renamed`,
          newTwoWayKey: `${inverse}_renamed`,
        });

        const firstColumns = await database
          .getAdapter()
          .$client.query<{ name: string }>(
            "SELECT name FROM pragma_table_info(?)",
            [
              SQLiteSqlBuilder.getTableName(
                { schema: "main", namespace: "relationship-schema" },
                first,
              ),
            ],
          );
        const secondColumns = await database
          .getAdapter()
          .$client.query<{ name: string }>(
            "SELECT name FROM pragma_table_info(?)",
            [
              SQLiteSqlBuilder.getTableName(
                { schema: "main", namespace: "relationship-schema" },
                second,
              ),
            ],
          );

        // Assert
        const firstNames = firstColumns.rows.map(({ name }) => name);
        const secondNames = secondColumns.rows.map(({ name }) => name);
        expect(firstNames.includes(`${key}_renamed`)).toBe(firstPhysical);
        expect(secondNames.includes(`${inverse}_renamed`)).toBe(secondPhysical);

        const firstMetadata = await database.getCollection(first);
        const secondMetadata = await database.getCollection(second);
        expect(
          (firstMetadata.get("attributes") as Doc<Attribute>[]).some(
            (item) => item.getId() === `${key}_renamed`,
          ),
        ).toBe(true);
        expect(
          (secondMetadata.get("attributes") as Doc<Attribute>[]).some(
            (item) => item.getId() === `${inverse}_renamed`,
          ),
        ).toBe(true);

        await database.deleteRelationship(first, `${key}_renamed`);
        const deletedFirstColumns = await database
          .getAdapter()
          .$client.query<{ name: string }>(
            "SELECT name FROM pragma_table_info(?)",
            [
              SQLiteSqlBuilder.getTableName(
                { schema: "main", namespace: "relationship-schema" },
                first,
              ),
            ],
          );
        expect(deletedFirstColumns.rows.map(({ name }) => name)).not.toContain(
          `${key}_renamed`,
        );
        expect(
          (await database.getCollection(first))
            .get("attributes")
            .some((item: Doc<Attribute>) => item.getId() === `${key}_renamed`),
        ).toBe(false);
      }
    });

    test("uses one deterministic prefixed physical table for a many-to-many junction and removes it", async () => {
      // Arrange
      const namespace = "junction-names";
      const database = await setup(namespace);
      await collections(database, "posts", "tags");
      const posts = await database.getCollection("posts", true);
      const tags = await database.getCollection("tags", true);
      const junction = database
        .getAdapter()
        .getJunctionTable(
          posts.getSequence(),
          tags.getSequence(),
          "tags",
          "posts",
        );
      const expected = SQLiteSqlBuilder.getTableName(
        { schema: "main", namespace },
        junction,
      );

      // Act
      await database.createRelationship({
        collectionId: "posts",
        relatedCollectionId: "tags",
        type: RelationEnum.ManyToMany,
        id: "tags",
        twoWayKey: "posts",
        twoWay: true,
      });
      const firstLookup = await database
        .getAdapter()
        .$client.query<{ name: string }>(
          "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ?",
          [expected],
        );

      // Assert
      expect(
        database
          .getAdapter()
          .getJunctionTable(
            posts.getSequence(),
            tags.getSequence(),
            "tags",
            "posts",
          ),
      ).toBe(junction);
      expect(firstLookup.rows).toEqual([{ name: expected }]);
      expect(expected).toMatch(/^main_junction-names_[a-f0-9]{12}__/);

      await expect(
        database.updateRelationship({
          collectionId: "posts",
          id: "tags",
          newKey: "labels",
        }),
      ).rejects.toBeInstanceOf(DatabaseException);
      await database.deleteRelationship("posts", "tags");
      const afterDelete = await database
        .getAdapter()
        .$client.query(
          "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ?",
          [expected],
        );
      expect(afterDelete.rows).toHaveLength(0);
    });
  });

  describe("document relationship operations", () => {
    test("creates, populates, reassigns, disconnects, and deletes a one-to-one relationship", async () => {
      // Arrange
      const database = await setup("one-to-one");
      await collections(database, "members", "seats");
      await database.createRelationship({
        collectionId: "members",
        relatedCollectionId: "seats",
        type: RelationEnum.OneToOne,
        id: "seat",
        twoWayKey: "member",
        twoWay: true,
      });
      const system = database.system();
      const first = await system.createDocument(
        "members",
        new Doc({ $id: "m1", name: "One" }),
      );
      const second = await system.createDocument(
        "members",
        new Doc({ $id: "m2", name: "Two" }),
      );
      const seat = await system.createDocument(
        "seats",
        new Doc({ $id: "s1", name: "Seat", member: first.getId() }),
      );

      // Act
      await system.updateDocument(
        "seats",
        seat.getId(),
        new Doc({ member: second.getId() }),
      );
      const reassigned = await system.getDocument(
        "seats",
        seat.getId(),
        (query) => query.populate("member"),
      );
      const oldPartner = await system.getDocument(
        "members",
        first.getId(),
        (query) => query.populate("seat"),
      );
      await system.updateDocument(
        "seats",
        seat.getId(),
        new Doc({ member: null }),
      );
      const disconnected = await system.getDocument(
        "members",
        second.getId(),
        (query) => query.populate("seat"),
      );
      await system.deleteDocument("seats", seat.getId());

      // Assert
      expect((reassigned.get("member") as Doc).getId()).toBe(second.getId());
      expect(oldPartner.get("seat")).toBeNull();
      expect(disconnected.get("seat")).toBeNull();
      expect((await system.getDocument("seats", seat.getId())).empty()).toBe(
        true,
      );
    });

    test("creates, populates, updates, disconnects, and deletes one-to-many links", async () => {
      // Arrange
      const database = await setup("one-to-many");
      await collections(database, "authors", "posts");
      await database.createRelationship({
        collectionId: "authors",
        relatedCollectionId: "posts",
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
        twoWay: true,
      });
      const system = database.system();
      const author = await system.createDocument(
        "authors",
        new Doc({ $id: "a1", name: "Author" }),
      );
      const first = await system.createDocument(
        "posts",
        new Doc({ $id: "p1", name: "One" }),
      );
      const second = await system.createDocument(
        "posts",
        new Doc({ $id: "p2", name: "Two" }),
      );

      // Act
      await system.updateDocument(
        "authors",
        author.getId(),
        new Doc({ posts: { set: [first.getId()] } }),
      );
      await system.updateDocument(
        "authors",
        author.getId(),
        new Doc({
          posts: { connect: [second.getId()], disconnect: [first.getId()] },
        }),
      );
      const populated = await system.getDocument(
        "authors",
        author.getId(),
        (query) => query.populate("posts"),
      );
      await system.updateDocument(
        "authors",
        author.getId(),
        new Doc({ posts: { set: [] } }),
      );
      const detached = await system.getDocument(
        "posts",
        second.getId(),
        (query) => query.populate("author"),
      );
      await system.deleteDocument("posts", first.getId());

      // Assert
      expect(ids(populated.get("posts"))).toEqual([second.getId()]);
      expect(detached.get("author")).toBeNull();
      expect((await system.getDocument("posts", first.getId())).empty()).toBe(
        true,
      );
    });

    test("creates, populates, reassigns, disconnects, and deletes a many-to-one link", async () => {
      // Arrange
      const database = await setup("many-to-one");
      await collections(database, "posts", "authors");
      await database.createRelationship({
        collectionId: "posts",
        relatedCollectionId: "authors",
        type: RelationEnum.ManyToOne,
        id: "author",
        twoWayKey: "posts",
        twoWay: true,
      });
      const system = database.system();
      const first = await system.createDocument(
        "authors",
        new Doc({ $id: "a1", name: "One" }),
      );
      const second = await system.createDocument(
        "authors",
        new Doc({ $id: "a2", name: "Two" }),
      );
      const post = await system.createDocument(
        "posts",
        new Doc({ $id: "p1", name: "Post", author: first.getId() }),
      );

      // Act
      await system.updateDocument(
        "posts",
        post.getId(),
        new Doc({ author: second.getId() }),
      );
      const populated = await system.getDocument(
        "posts",
        post.getId(),
        (query) => query.populate("author"),
      );
      const former = await system.getDocument(
        "authors",
        first.getId(),
        (query) => query.populate("posts"),
      );
      await system.updateDocument(
        "posts",
        post.getId(),
        new Doc({ author: null }),
      );
      const detached = await system.getDocument(
        "authors",
        second.getId(),
        (query) => query.populate("posts"),
      );
      await system.deleteDocument("posts", post.getId());

      // Assert
      expect((populated.get("author") as Doc).getId()).toBe(second.getId());
      expect(former.get("posts")).toHaveLength(0);
      expect(detached.get("posts")).toHaveLength(0);
      expect((await system.getDocument("posts", post.getId())).empty()).toBe(
        true,
      );
    });

    test("creates, populates both sides, updates, disconnects, and deletes many-to-many links", async () => {
      // Arrange
      const database = await setup("many-to-many");
      await collections(database, "posts", "tags");
      await database.createRelationship({
        collectionId: "posts",
        relatedCollectionId: "tags",
        type: RelationEnum.ManyToMany,
        id: "tags",
        twoWayKey: "posts",
        twoWay: true,
        onDelete: OnDelete.SetNull,
      });
      const system = database.system();
      const post = await system.createDocument(
        "posts",
        new Doc({ $id: "p1", name: "Post" }),
      );
      const first = await system.createDocument(
        "tags",
        new Doc({ $id: "t1", name: "One" }),
      );
      const second = await system.createDocument(
        "tags",
        new Doc({ $id: "t2", name: "Two" }),
      );
      const third = await system.createDocument(
        "tags",
        new Doc({ $id: "t3", name: "Three" }),
      );

      // Act
      await system.updateDocument(
        "posts",
        post.getId(),
        new Doc({ tags: { set: [first.getId(), second.getId()] } }),
      );
      await system.updateDocument(
        "posts",
        post.getId(),
        new Doc({
          tags: { connect: [third.getId()], disconnect: [first.getId()] },
        }),
      );
      const populated = await system.getDocument(
        "posts",
        post.getId(),
        (query) => query.populate("tags"),
      );
      const inverse = await system.getDocument(
        "tags",
        second.getId(),
        (query) => query.populate("posts"),
      );
      await system.deleteDocument("posts", post.getId());
      const detached = await system.getDocument(
        "tags",
        second.getId(),
        (query) => query.populate("posts"),
      );

      // Assert
      expect(ids(populated.get("tags"))).toEqual(
        [second.getId(), third.getId()].sort(),
      );
      expect(ids(inverse.get("posts"))).toEqual([post.getId()]);
      expect(detached.empty()).toBe(false);
      expect(detached.get("posts")).toHaveLength(0);
    });

    test("rejects a missing related document without leaving a partial link", async () => {
      // Arrange
      const database = await setup("missing-relation");
      await collections(database, "posts", "tags");
      await database.createRelationship({
        collectionId: "posts",
        relatedCollectionId: "tags",
        type: RelationEnum.ManyToMany,
        id: "tags",
        twoWayKey: "posts",
        twoWay: true,
      });
      const system = database.system();
      const post = await system.createDocument(
        "posts",
        new Doc({ $id: "p1", name: "Post" }),
      );

      // Act / Assert
      await expect(
        system.updateDocument(
          "posts",
          post.getId(),
          new Doc({ tags: { connect: ["missing"] } }),
        ),
      ).rejects.toBeInstanceOf(RelationshipException);
      const unchanged = await system.getDocument(
        "posts",
        post.getId(),
        (query) => query.populate("tags"),
      );
      expect(unchanged.get("tags")).toHaveLength(0);
    });
  });

  describe("delete policies", () => {
    test("restrict rejects deleting a related parent", async () => {
      // Arrange
      const database = await setup("restrict-delete");
      await collections(database, "authors", "posts");
      await database.createRelationship({
        collectionId: "authors",
        relatedCollectionId: "posts",
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
        onDelete: OnDelete.Restrict,
      });
      const system = database.system();
      const author = await system.createDocument(
        "authors",
        new Doc({ $id: "a1", name: "Author" }),
      );
      await system.createDocument(
        "posts",
        new Doc({ $id: "p1", name: "Post", author: author.getId() }),
      );

      // Act / Assert
      await expect(
        system.deleteDocument("authors", author.getId()),
      ).rejects.toBeInstanceOf(RelationshipException);
      expect(
        (await system.getDocument("authors", author.getId())).empty(),
      ).toBe(false);
    });

    test("cascade deletes all related children", async () => {
      // Arrange
      const database = await setup("cascade-delete");
      await collections(database, "authors", "posts");
      await database.createRelationship({
        collectionId: "authors",
        relatedCollectionId: "posts",
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
        onDelete: OnDelete.Cascade,
      });
      const system = database.system();
      const author = await system.createDocument(
        "authors",
        new Doc({ $id: "a1", name: "Author" }),
      );
      const post = await system.createDocument(
        "posts",
        new Doc({ $id: "p1", name: "Post", author: author.getId() }),
      );

      // Act
      await system.deleteDocument("authors", author.getId());

      // Assert
      expect((await system.getDocument("posts", post.getId())).empty()).toBe(
        true,
      );
    });

    test("set-null preserves related children and clears their key", async () => {
      // Arrange
      const database = await setup("set-null-delete");
      await collections(database, "authors", "posts");
      await database.createRelationship({
        collectionId: "authors",
        relatedCollectionId: "posts",
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
        twoWay: true,
        onDelete: OnDelete.SetNull,
      });
      const system = database.system();
      const author = await system.createDocument(
        "authors",
        new Doc({ $id: "a1", name: "Author" }),
      );
      const post = await system.createDocument(
        "posts",
        new Doc({ $id: "p1", name: "Post", author: author.getId() }),
      );

      // Act
      await system.deleteDocument("authors", author.getId());
      const orphan = await system.getDocument("posts", post.getId(), (query) =>
        query.populate("author"),
      );

      // Assert
      expect(orphan.empty()).toBe(false);
      expect(orphan.get("author")).toBeNull();
    });
  });

  describe("authorization and tenant isolation", () => {
    test("allows an authorized cascade to maintain inaccessible children and rejects an unauthorized delete", async () => {
      // Arrange
      const database = await setup("relationship-auth");
      await database.createCollection({
        id: "authors",
        attributes: [attribute("name")],
        permissions: [
          Permission.read(Role.any()),
          Permission.delete(Role.user("alice")),
        ],
      });
      await database.createCollection({
        id: "posts",
        attributes: [attribute("name")],
        permissions: [],
        documentSecurity: true,
      });
      await database.createRelationship({
        collectionId: "authors",
        relatedCollectionId: "posts",
        type: RelationEnum.OneToMany,
        id: "posts",
        twoWayKey: "author",
        onDelete: OnDelete.Cascade,
      });
      const system = database.system();
      const denied = await system.createDocument(
        "authors",
        new Doc({ $id: "denied", name: "Denied" }),
      );
      const allowed = await system.createDocument(
        "authors",
        new Doc({ $id: "allowed", name: "Allowed" }),
      );
      const child = await system.createDocument(
        "posts",
        new Doc({ $id: "child", name: "Hidden", author: allowed.getId() }),
      );

      // Act / Assert
      await expect(
        database.for("user:bob").deleteDocument("authors", denied.getId()),
      ).rejects.toBeInstanceOf(AuthorizationException);
      await database
        .for("user:alice")
        .deleteDocument("authors", allowed.getId());
      expect((await system.getDocument("posts", child.getId())).empty()).toBe(
        true,
      );
    });

    test("keeps identical many-to-many IDs and junction mutations isolated by tenant", async () => {
      // Arrange
      const database = await setup("relationship-tenants", {
        sharedTables: true,
        tenantId: 1,
      });
      await collections(database, "posts", "tags");
      await database.createRelationship({
        collectionId: "posts",
        relatedCollectionId: "tags",
        type: RelationEnum.ManyToMany,
        id: "tags",
        twoWayKey: "posts",
        twoWay: true,
      });
      const adapter = database.getAdapter();
      await database
        .system()
        .createDocument(
          "posts",
          new Doc({ $id: "same-post", name: "Tenant One" }),
        );
      await database
        .system()
        .createDocument(
          "tags",
          new Doc({ $id: "same-tag", name: "Tenant One" }),
        );
      await database
        .system()
        .updateDocument(
          "posts",
          "same-post",
          new Doc({ tags: { set: ["same-tag"] } }),
        );
      adapter.setMeta({ tenantId: 2 });
      await database
        .system()
        .createDocument(
          "posts",
          new Doc({ $id: "same-post", name: "Tenant Two" }),
        );
      await database
        .system()
        .createDocument(
          "tags",
          new Doc({ $id: "same-tag", name: "Tenant Two" }),
        );

      // Act
      await database
        .system()
        .updateDocument("posts", "same-post", new Doc({ tags: { set: [] } }));
      const tenantTwo = await database
        .system()
        .getDocument("posts", "same-post", (query) => query.populate("tags"));
      adapter.setMeta({ tenantId: 1 });
      const tenantOne = await database
        .system()
        .getDocument("posts", "same-post", (query) => query.populate("tags"));

      // Assert
      expect(tenantTwo.get("name")).toBe("Tenant Two");
      expect(tenantTwo.get("tags")).toHaveLength(0);
      expect(tenantOne.get("name")).toBe("Tenant One");
      expect(ids(tenantOne.get("tags"))).toEqual(["same-tag"]);
    });
  });
});
