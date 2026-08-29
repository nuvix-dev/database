import { afterEach, describe, expect, test } from "bun:test";
import {
  AttributeEnum,
  IndexEnum,
  PermissionEnum,
  RelationEnum,
} from "@core/enums.js";
import { Doc } from "@core/doc.js";
import { Query } from "@core/query.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import type { Database } from "@core/database.js";
import { createSQLiteTestDb } from "./sqlite-helpers.js";

const databases: Database[] = [];

afterEach(async () => {
  await Promise.all(
    databases.splice(0).map((database) =>
      database.getAdapter().$client.disconnect(),
    ),
  );
});

async function setup(): Promise<Database> {
  const database = createSQLiteTestDb({ namespace: "integration" });
  databases.push(database);
  await database.create("main");
  await database.createCollection({
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
        $id: "score",
        key: "score",
        type: AttributeEnum.Integer,
        required: true,
      }),
    ],
    indexes: [
      new Doc({
        $id: "name_unique",
        key: "name_unique",
        type: IndexEnum.Unique,
        attributes: ["name"],
      }),
    ],
    permissions: [Permission.create(Role.any())],
    documentSecurity: false,
  });
  return database;
}

describe("Database with SQLiteAdapter", () => {
  test("keeps dotted collection and attribute identifiers distinct", async () => {
    const database = createSQLiteTestDb({ namespace: "dotted-identifiers" });
    databases.push(database);
    await database.create("main");

    for (const [collection, attribute] of [
      ["foo.bar", "value.part"],
      ["foobar", "valuepart"],
    ] as const) {
      await database.createCollection({
        id: collection,
        attributes: [
          new Doc({
            $id: attribute,
            key: attribute,
            type: AttributeEnum.String,
            size: 64,
            required: true,
          }),
        ],
        permissions: [Permission.create(Role.any())],
        documentSecurity: false,
      });
      await database.system().createDocument(
        collection,
        new Doc({ [attribute]: collection }),
      );
    }

    const dotted = await database.system().find("foo.bar", [
      Query.equal("value.part", ["foo.bar"]),
    ]);
    const plain = await database.system().find("foobar", [
      Query.equal("valuepart", ["foobar"]),
    ]);

    expect(dotted.map((document) => document.get("value.part"))).toEqual([
      "foo.bar",
    ]);
    expect(plain.map((document) => document.get("valuepart"))).toEqual([
      "foobar",
    ]);
  });

  test("supports collection CRUD, queries, count, and sum", async () => {
    const database = await setup();
    const session = database.system();

    const ada = await session.createDocument(
      "users",
      new Doc({ name: "Ada", score: 10 }),
    );
    await session.createDocument(
      "users",
      new Doc({ name: "Grace", score: 20 }),
    );

    const loaded = await session.getDocument("users", ada.getId());
    const selected = await session.find("users", [
      Query.greaterThan("score", 10),
    ]);

    expect(loaded.get("name")).toBe("Ada");
    expect(selected.map((document) => document.get("name"))).toEqual([
      "Grace",
    ]);
    expect(await session.count("users")).toBe(2);
    expect(await session.sum("users", "score")).toBe(30);
  });

  test("commits and rolls back session transactions", async () => {
    const database = await setup();
    const session = database.system();

    await session.withTransaction(async (transaction) => {
      await transaction.createDocument(
        "users",
        new Doc({ name: "Committed", score: 1 }),
      );
    });

    await expect(
      session.withTransaction(async (transaction) => {
        await transaction.createDocument(
          "users",
          new Doc({ name: "Rolled back", score: 2 }),
        );
        throw new Error("rollback");
      }),
    ).rejects.toThrow("rollback");

    expect(await session.count("users")).toBe(1);
  });

  test("matches document permission roles and permission types", async () => {
    const database = createSQLiteTestDb({ namespace: "permissions" });
    databases.push(database);
    await database.create("main");
    await database.createCollection({
      id: "secured",
      attributes: [
        new Doc({
          $id: "score",
          key: "score",
          type: AttributeEnum.Integer,
          required: true,
        }),
      ],
      permissions: [Permission.create(Role.any())],
      documentSecurity: true,
    });
    await database.system().createDocument(
      "secured",
      new Doc({
        score: 7,
        $permissions: [
          Permission.read(Role.user("reader")),
          Permission.update(Role.user("editor")),
        ],
      }),
    );

    expect(await database.for("user:reader").count("secured")).toBe(1);
    expect(await database.for("user:reader").sum("secured", "score")).toBe(7);
    expect(
      await database.for("user:reader").find("secured", [], PermissionEnum.Update),
    ).toHaveLength(0);
    expect(
      await database.for("user:editor").find("secured", [], PermissionEnum.Update),
    ).toHaveLength(1);
    expect(await database.for("user:other").find("secured")).toHaveLength(0);
  });

  test("populates relationship rows using processFindResults aliases", async () => {
    const database = createSQLiteTestDb({ namespace: "relationships" });
    databases.push(database);
    await database.create("main");
    await database.createCollection({
      id: "authors",
      attributes: [
        new Doc({ $id: "name", key: "name", type: AttributeEnum.String }),
      ],
    });
    await database.createCollection({
      id: "articles",
      attributes: [
        new Doc({ $id: "title", key: "title", type: AttributeEnum.String }),
      ],
    });
    await database.createRelationship({
      collectionId: "authors",
      relatedCollectionId: "articles",
      type: RelationEnum.OneToMany,
      id: "articles",
      twoWay: true,
      twoWayKey: "author",
    });

    const author = await database
      .system()
      .createDocument("authors", new Doc({ name: "Ada" }));
    await database.system().createDocument(
      "articles",
      new Doc({ title: "One", author: author.getId() }),
    );
    await database.system().createDocument(
      "articles",
      new Doc({ title: "Two", author: author.getId() }),
    );

    const loaded = await database.system().getDocument("authors", author.getId(), [
      Query.populate("articles", [Query.select(["title"])]),
    ]);
    const articles = loaded.get("articles") as Doc<any>[];
    expect(articles.map((article) => article.get("title")).sort()).toEqual([
      "One",
      "Two",
    ]);
  });

  test("uses an owned outer transaction scope and nested savepoints", async () => {
    const database = await setup();
    const session = database.system();
    const parentAdapter = database.getAdapter();

    await session.withTransaction(async (outer) => {
      const outerDatabase = (outer as unknown as { database: Database }).database;
      expect(outerDatabase).not.toBe(database);
      expect(outerDatabase.getAdapter()).not.toBe(parentAdapter);
      expect(outerDatabase.getAdapter().$client.__type).toBe("transaction");

      await outer.createDocument("users", new Doc({ name: "Outer", score: 1 }));
      await expect(
        outer.withTransaction(async (nested) => {
          const nestedDatabase = (
            nested as unknown as { database: Database }
          ).database;
          expect(nestedDatabase).toBe(outerDatabase);
          await nested.createDocument(
            "users",
            new Doc({ name: "Nested rollback", score: 2 }),
          );
          throw new Error("nested rollback");
        }),
      ).rejects.toThrow("nested rollback");
      await outer.createDocument(
        "users",
        new Doc({ name: "After savepoint", score: 3 }),
      );
    });

    expect(
      (await session.find("users")).map((item) => item.get("name")).sort(),
    ).toEqual(["After savepoint", "Outer"]);
  });

  test("rejects update-lock reads explicitly", async () => {
    const database = await setup();
    const created = await database
      .system()
      .createDocument("users", new Doc({ name: "Locked", score: 1 }));

    await expect(
      database.system().getDocument("users", created.getId(), [], true),
    ).resolves.toMatchObject({});
  });
});
