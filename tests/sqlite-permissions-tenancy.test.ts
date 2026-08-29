import { afterEach, describe, expect, test } from "bun:test";
import { AttributeEnum, IndexEnum, RelationEnum } from "@core/enums.js";
import { Doc } from "@core/doc.js";
import type { Database } from "@core/database.js";
import { SQLiteSqlBuilder } from "@adapters/sqlite-sql-builder.js";
import { DuplicateException } from "@errors/index.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import type { Attribute } from "@validators/schema.js";
import { createSQLiteTestDb } from "./sqlite-helpers.js";

const databases: Database[] = [];

afterEach(async () => {
  await Promise.all(
    databases.splice(0).map((database) =>
      database.getAdapter().$client.disconnect(),
    ),
  );
});

const stringAttribute = (key: string): Doc<Attribute> =>
  new Doc<Attribute>({
    $id: key,
    key,
    type: AttributeEnum.String,
    size: 128,
    required: true,
  });

const integerAttribute = (key: string): Doc<Attribute> =>
  new Doc<Attribute>({
    $id: key,
    key,
    type: AttributeEnum.Integer,
    required: true,
  });

async function createDatabase(namespace: string): Promise<Database> {
  const database = createSQLiteTestDb({ namespace });
  databases.push(database);
  await database.create("main");
  return database;
}

async function createSharedDatabase(namespace: string): Promise<Database> {
  const database = createSQLiteTestDb({
    namespace,
    sharedTables: true,
    tenantId: 1,
  });
  databases.push(database);
  await database.create("main");
  return database;
}

describe("SQLite authorization", () => {
  test("collection permissions allow matching-role CRUD and deny nonmatching-role CRUD", async () => {
    // Arrange
    const database = await createDatabase("sqlite_collection_permissions");
    await database.createCollection({
      id: "records",
      attributes: [stringAttribute("name")],
      permissions: [
        Permission.create(Role.user("operator")),
        Permission.read(Role.user("operator")),
        Permission.update(Role.user("operator")),
        Permission.delete(Role.user("operator")),
      ],
      documentSecurity: false,
    });
    const allowed = database.for("user:operator");
    const denied = database.for("user:intruder");

    // Act
    const created = await allowed.createDocument(
      "records",
      new Doc({ $id: "allowed", name: "created" }),
    );
    const read = await allowed.getDocument("records", created.getId());
    const updated = await allowed.updateDocument(
      "records",
      created.getId(),
      new Doc({ name: "updated" }),
    );
    await expect(
      denied.createDocument("records", new Doc({ name: "denied" })),
    ).rejects.toThrow();
    await expect(
      denied.getDocument("records", created.getId()),
    ).rejects.toThrow();
    await expect(
      denied.updateDocument(
        "records",
        created.getId(),
        new Doc({ name: "hacked" }),
      ),
    ).rejects.toThrow();
    await expect(
      denied.deleteDocument("records", created.getId()),
    ).rejects.toThrow();
    const deleted = await allowed.deleteDocument("records", created.getId());

    // Assert
    expect(read.get("name")).toBe("created");
    expect(updated.get("name")).toBe("updated");
    expect(deleted).toBe(true);
  });

  test("document permissions allow matching-role reads, updates, and deletes while hiding rows from other roles", async () => {
    // Arrange
    const database = await createDatabase("sqlite_document_permissions");
    await database.createCollection({
      id: "secrets",
      attributes: [stringAttribute("value")],
      permissions: [Permission.create(Role.user("writer"))],
      documentSecurity: true,
    });
    const owner = database.for("user:owner");
    const intruder = database.for("user:intruder");
    const created = await database.for("user:writer").createDocument(
      "secrets",
      new Doc({
        $id: "secret",
        value: "initial",
        $permissions: [
          Permission.read(Role.user("owner")),
          Permission.update(Role.user("owner")),
          Permission.delete(Role.user("owner")),
        ],
      }),
    );

    // Act
    const visible = await owner.getDocument("secrets", created.getId());
    const hidden = await intruder.getDocument("secrets", created.getId());
    const updated = await owner.updateDocument(
      "secrets",
      created.getId(),
      new Doc({ value: "owner update" }),
    );
    await expect(
      intruder.updateDocument(
        "secrets",
        created.getId(),
        new Doc({ value: "intruder update" }),
      ),
    ).rejects.toThrow();
    await expect(
      intruder.deleteDocument("secrets", created.getId()),
    ).rejects.toThrow();
    const deleted = await owner.deleteDocument("secrets", created.getId());

    // Assert
    expect(visible.get("value")).toBe("initial");
    expect(hidden.empty()).toBe(true);
    expect(updated.get("value")).toBe("owner update");
    expect(deleted).toBe(true);
  });

  test("system sessions bypass empty permissions while empty-role sessions are denied", async () => {
    // Arrange
    const database = await createDatabase("sqlite_system_permissions");
    await database.createCollection({
      id: "locked",
      attributes: [stringAttribute("value")],
      permissions: [],
      documentSecurity: false,
    });
    const system = database.system();
    const ordinary = database.for();

    // Act
    await expect(
      ordinary.createDocument("locked", new Doc({ value: "denied" })),
    ).rejects.toThrow();
    const created = await system.createDocument(
      "locked",
      new Doc({ $id: "system-only", value: "created" }),
    );
    const read = await system.getDocument("locked", created.getId());
    const updated = await system.updateDocument(
      "locked",
      created.getId(),
      new Doc({ value: "updated" }),
    );
    await expect(
      ordinary.getDocument("locked", created.getId()),
    ).rejects.toThrow();
    await expect(
      ordinary.updateDocument(
        "locked",
        created.getId(),
        new Doc({ value: "denied" }),
      ),
    ).rejects.toThrow();
    await expect(
      ordinary.deleteDocument("locked", created.getId()),
    ).rejects.toThrow();
    const deleted = await system.deleteDocument("locked", created.getId());

    // Assert
    expect(read.get("value")).toBe("created");
    expect(updated.get("value")).toBe("updated");
    expect(deleted).toBe(true);
  });
});

describe("SQLite shared-table tenant isolation", () => {
  test("isolates identities, unique values, permission rows, bulk operations, upserts, and aggregates", async () => {
    // Arrange
    const namespace = "sqlite_shared_operations";
    const database = await createSharedDatabase(namespace);
    const adapter = database.getAdapter();
    await database.createCollection({
      id: "accounts",
      attributes: [
        stringAttribute("email"),
        stringAttribute("name"),
        integerAttribute("score"),
      ],
      indexes: [
        new Doc({
          $id: "email_unique",
          key: "email_unique",
          type: IndexEnum.Unique,
          attributes: ["email"],
        }),
      ],
      documentSecurity: true,
    });
    const permissions = (tenant: number) => [
      Permission.read(Role.user(`tenant-${tenant}`)),
      Permission.update(Role.user(`tenant-${tenant}`)),
      Permission.delete(Role.user(`tenant-${tenant}`)),
    ];
    await database.system().createDocuments("accounts", [
      new Doc({
        $id: "same-id",
        email: "shared@example.test",
        name: "tenant one shared",
        score: 10,
        $permissions: permissions(1),
      }),
      new Doc({
        $id: "tenant-one-only",
        email: "one@example.test",
        name: "tenant one only",
        score: 20,
        $permissions: permissions(1),
      }),
    ]);
    adapter.setMeta({ tenantId: 2 });
    await database.system().createDocuments("accounts", [
      new Doc({
        $id: "same-id",
        email: "shared@example.test",
        name: "tenant two shared",
        score: 100,
        $permissions: permissions(2),
      }),
      new Doc({
        $id: "tenant-two-only",
        email: "two@example.test",
        name: "tenant two only",
        score: 200,
        $permissions: permissions(2),
      }),
    ]);

    // Act
    adapter.setMeta({ tenantId: 1 });
    const tenantOneBefore = await database.system().find("accounts");
    const updated = await database.system().updateDocuments(
      "accounts",
      new Doc({ name: "tenant one bulk" }),
    );
    const upserted = await database.system().createOrUpdateDocuments(
      "accounts",
      [
        new Doc({
          $id: "same-id",
          email: "shared@example.test",
          name: "tenant one upsert",
          score: 11,
          $permissions: permissions(1),
        }),
      ],
    );
    const crossTenantDelete = await database
      .system()
      .deleteDocument("accounts", "tenant-two-only");
    const deleted = await database
      .system()
      .deleteDocuments("accounts", (query) =>
        query.equal("$id", "tenant-one-only"),
      );
    const tenantOneCount = await database.system().count("accounts");
    const tenantOneSum = await database.system().sum("accounts", "score");
    await expect(
      database.system().createDocument(
        "accounts",
        new Doc({
          email: "shared@example.test",
          name: "same tenant duplicate",
          score: 1,
        }),
      ),
    ).rejects.toBeInstanceOf(DuplicateException);
    adapter.setMeta({ tenantId: 2 });
    const tenantTwoAfter = await database.system().find("accounts");
    const tenantTwoCount = await database.system().count("accounts");
    const tenantTwoSum = await database.system().sum("accounts", "score");
    const table = SQLiteSqlBuilder.getSQLTable(
      { schema: "main", namespace },
      "accounts_perms",
    );
    const permissionRows = await adapter.$client.query<{
      _tenant: number;
      _permissions: string;
    }>(
      `SELECT "_tenant", "_permissions" FROM ${table} ORDER BY "_tenant", "_document", "_type"`,
    );

    // Assert
    expect(tenantOneBefore.map((document) => document.get("name")).sort()).toEqual([
      "tenant one only",
      "tenant one shared",
    ]);
    expect(updated).toBe(2);
    expect(upserted).toBe(1);
    expect(crossTenantDelete).toBe(false);
    expect(deleted).toEqual(["tenant-one-only"]);
    expect(tenantOneCount).toBe(1);
    expect(tenantOneSum).toBe(11);
    expect(tenantTwoAfter.map((document) => document.get("name")).sort()).toEqual([
      "tenant two only",
      "tenant two shared",
    ]);
    expect(tenantTwoCount).toBe(2);
    expect(tenantTwoSum).toBe(300);
    expect(permissionRows.rows.map((row) => row._tenant)).toEqual([
      1,
      1,
      1,
      2,
      2,
      2,
      2,
      2,
      2,
    ]);
    expect(
      permissionRows.rows
        .filter((row) => row._tenant === 1)
        .every((row) => row._permissions.includes("tenant-1")),
    ).toBe(true);
    expect(
      permissionRows.rows
        .filter((row) => row._tenant === 2)
        .every((row) => row._permissions.includes("tenant-2")),
    ).toBe(true);
  });

  test("prevents cross-tenant relationship population when ids overlap", async () => {
    // Arrange
    const database = await createSharedDatabase("sqlite_shared_relations");
    const adapter = database.getAdapter();
    await database.createCollection({
      id: "authors",
      attributes: [stringAttribute("name")],
    });
    await database.createCollection({
      id: "articles",
      attributes: [stringAttribute("title")],
    });
    await database.createRelationship({
      collectionId: "authors",
      relatedCollectionId: "articles",
      type: RelationEnum.OneToMany,
      id: "articles",
      twoWay: true,
      twoWayKey: "author",
    });
    await database.system().createDocument(
      "authors",
      new Doc({ $id: "same-author", name: "tenant one author" }),
    );
    await database.system().createDocument(
      "articles",
      new Doc({
        $id: "same-article",
        title: "tenant one article",
        author: "same-author",
      }),
    );
    adapter.setMeta({ tenantId: 2 });
    await database.system().createDocument(
      "authors",
      new Doc({ $id: "same-author", name: "tenant two author" }),
    );
    await database.system().createDocument(
      "articles",
      new Doc({
        $id: "same-article",
        title: "tenant two article",
        author: "same-author",
      }),
    );

    // Act
    adapter.setMeta({ tenantId: 1 });
    const tenantOne = await database
      .system()
      .getDocument("authors", "same-author", (query) =>
        query.populate("articles"),
      );
    adapter.setMeta({ tenantId: 2 });
    const tenantTwo = await database
      .system()
      .getDocument("authors", "same-author", (query) =>
        query.populate("articles"),
      );

    // Assert
    const tenantOneArticles = tenantOne.get("articles") as unknown as Doc[];
    const tenantTwoArticles = tenantTwo.get("articles") as unknown as Doc[];
    expect(
      tenantOneArticles.map((article) => String(article.get("title"))),
    ).toEqual(["tenant one article"]);
    expect(
      tenantTwoArticles.map((article) => String(article.get("title"))),
    ).toEqual(["tenant two article"]);
  });
});
