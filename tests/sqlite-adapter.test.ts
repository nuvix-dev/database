import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import {
  AttributeEnum,
  EventsEnum,
  IndexEnum,
  PermissionEnum,
} from "@core/enums.js";
import { Doc } from "@core/doc.js";
import { Query } from "@core/query.js";
import {
  SQLiteAdapter,
  type SQLiteAdapterConfig,
} from "@adapters/sqlite-adapter.js";
import { SQLiteClient } from "@adapters/sqlite.js";
import { SQLiteSqlBuilder } from "@adapters/sqlite-sql-builder.js";
import { Logger } from "@utils/logger.js";
import type { Attribute, Collection, Index } from "@validators/schema.js";
import { SYSTEM_CONTEXT } from "@core/auth.js";
import type { ProcessedQuery } from "@core/database.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { DuplicateException } from "@errors/index.js";

const documentAttributes = [
  new Doc<Attribute>({
    $id: "name",
    key: "name",
    type: AttributeEnum.String,
  }),
  new Doc<Attribute>({
    $id: "score",
    key: "score",
    type: AttributeEnum.Integer,
  }),
];
const writeMeta = { schema: "app", namespace: "writes" };
const writeTable = (name: string): string =>
  SQLiteSqlBuilder.getSQLTable(writeMeta, name);

async function createDocumentAdapter(sharedTables = false): Promise<SQLiteAdapter> {
  const adapter = new SQLiteAdapter(":memory:");
  adapter.setMeta({
    schema: "app",
    namespace: "writes",
    sharedTables,
    tenantId: sharedTables ? 1 : undefined,
  });
  await adapter.createCollection({ name: "users", attributes: documentAttributes });
  return adapter;
}

function document(
  id: string,
  name: string,
  score: number,
  tenant?: number,
): Doc<Record<string, any>> {
  return new Doc<Record<string, any>>({
    $id: id,
    $tenant: tenant,
    $createdAt: new Date("2026-01-01T00:00:00.000Z"),
    $updatedAt: new Date("2026-01-01T00:00:00.000Z"),
    $permissions: [Permission.read(Role.any()).toString()],
    name,
    score,
  });
}

function readQuery(
  collection = new Doc<Collection>({
    $id: "users",
    attributes: documentAttributes,
    documentSecurity: false,
  }),
): ProcessedQuery {
  return {
    collection,
    filters: [],
    selections: ["name", "score"],
    populateQueries: [],
    limit: null,
    offset: null,
    orders: {},
    cursor: null,
    cursorDirection: null,
    skipAuth: false,
  };
}

class InspectableSQLiteAdapter extends SQLiteAdapter {
  constructor(client: SQLiteAdapterConfig) {
    super(client);
  }

  public get state(): {
    meta: object;
    logger: Logger;
    transformations: object;
  } {
    return {
      meta: this._meta,
      logger: this.$logger,
      transformations: this.transformations,
    };
  }
}

describe("SQLiteAdapter", () => {
  test("accepts paths, Database handles, and SQLiteClient handles", async () => {
    const fromPath = new SQLiteAdapter(":memory:");
    const database = new Database(":memory:");
    const fromDatabase = new SQLiteAdapter(database);
    const client = new SQLiteClient(":memory:");
    const fromClient = new SQLiteAdapter(client);

    try {
      expect(fromPath.type).toBe("sqlite");
      expect(fromPath.$client.__type).toBe("sqlite");
      expect(fromDatabase.$client.__type).toBe("sqlite");
      expect(fromClient.$client).toBe(client);
      await expect(fromPath.ping()).resolves.toBeUndefined();
    } finally {
      await fromPath.$client.disconnect();
      await fromDatabase.$client.disconnect();
      await fromClient.$client.disconnect();
      database.close(true);
    }
  });

  test("delegates schema operations using live schema and namespace metadata", async () => {
    const adapter = new SQLiteAdapter(":memory:");

    try {
      adapter.setMeta({ schema: "first", namespace: "one" });
      await adapter.createCollection({ name: "users", attributes: [] });

      adapter.setMeta({ schema: "second", namespace: "two" });
      await adapter.createCollection({
        name: "users",
        attributes: [
          new Doc<Attribute>({
            $id: "email",
            key: "email",
            type: AttributeEnum.String,
          }),
        ],
        indexes: [
          new Doc<Index>({
            $id: "email_unique",
            key: "email_unique",
            type: IndexEnum.Unique,
            attributes: ["email"],
          }),
        ],
      });

      const first = SQLiteSqlBuilder.getTableName(
        { schema: "first", namespace: "one" },
        "users",
      );
      const second = SQLiteSqlBuilder.getTableName(
        { schema: "second", namespace: "two" },
        "users",
      );
      const { rows } = await adapter.$client.query<{ name: string }>(
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN (?, ?) ORDER BY name",
        [first, second],
      );

      expect(rows.map(({ name }) => name).sort()).toEqual(
        [first, second].sort(),
      );
      expect(await adapter.exists("second", "users")).toBe(true);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("reports SQLite capabilities", async () => {
    const adapter = new SQLiteAdapter(":memory:");

    try {
      expect(adapter.$supportForIndex).toBe(true);
      expect(adapter.$supportForUniqueIndex).toBe(true);
      expect(adapter.$supportForFulltextIndex).toBe(false);
      expect(adapter.$supportForIndexArray).toBe(false);
      expect(adapter.$supportForJSONOverlaps).toBe(false);
      expect(adapter.$supportForRelationships).toBe(true);
      expect(adapter.$supportForUpdateLock).toBe(false);
      expect(adapter.$supportForTimeouts).toBe(false);
      expect(adapter.$supportForBatchOperations).toBe(false);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("creates single and bulk documents with sequences and permission rows", async () => {
    const adapter = await createDocumentAdapter();
    try {
      const first = document("one", "One", 1);
      const rest = [document("two", "Two", 2), document("three", "Three", 3)];

      expect((await adapter.createDocument(SYSTEM_CONTEXT, "users", first)).getSequence()).toBe(1);
      await adapter.createDocuments(SYSTEM_CONTEXT, "users", rest);

      expect(rest.map((item) => item.getSequence())).toEqual([2, 3]);
      const main = await adapter.$client.query<{ count: number }>(
        `SELECT COUNT(*) AS count FROM ${writeTable("users")}`,
      );
      const permissions = await adapter.$client.query<{
        _document: number;
        _permissions: string;
      }>(`SELECT "_document", "_permissions" FROM ${writeTable("users_perms")} ORDER BY "_document"`);
      expect(main.rows[0]?.count).toBe(3);
      expect(permissions.rows.map((row) => row._document)).toEqual([1, 2, 3]);
      expect(JSON.parse(permissions.rows[0]!._permissions)).toEqual(["any"]);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("uses singular and plural create lifecycle events", async () => {
    const adapter = await createDocumentAdapter();
    const events: EventsEnum[] = [];
    const record = (event: EventsEnum) => (sql: string): string => {
      events.push(event);
      return sql;
    };
    adapter.before(
      EventsEnum.DocumentCreate,
      "test-document-create",
      record(EventsEnum.DocumentCreate),
    );
    adapter.before(
      EventsEnum.DocumentsCreate,
      "test-documents-create",
      record(EventsEnum.DocumentsCreate),
    );

    try {
      await adapter.createDocument(
        SYSTEM_CONTEXT,
        "users",
        document("one", "One", 1),
      );
      await adapter.createDocuments(SYSTEM_CONTEXT, "users", [
        document("two", "Two", 2),
      ]);

      expect(events).toEqual([
        EventsEnum.DocumentCreate,
        EventsEnum.DocumentsCreate,
      ]);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("maps bulk-created sequences by tenant and UID in shared tables", async () => {
    const adapter = await createDocumentAdapter(true);
    try {
      const tenantOne = document("same", "Tenant One", 1, 1);
      const tenantTwo = document("same", "Tenant Two", 2, 2);

      await adapter.createDocuments(SYSTEM_CONTEXT, "users", [
        tenantOne,
        tenantTwo,
      ]);

      expect(tenantOne.getSequence()).not.toBe(tenantTwo.getSequence());
      const rows = await adapter.$client.query<{
        _id: number;
        _tenant: number;
      }>(
        `SELECT "_id", "_tenant" FROM ${writeTable("users")} ORDER BY "_tenant"`,
      );
      expect(rows.rows).toEqual([
        { _id: tenantOne.getSequence(), _tenant: 1 },
        { _id: tenantTwo.getSequence(), _tenant: 2 },
      ]);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("rolls back a document insert when its permission write fails", async () => {
    const adapter = await createDocumentAdapter();
    try {
      await adapter.$client.query(`DROP TABLE ${writeTable("users_perms")}`);
      await expect(
        adapter.createDocument(SYSTEM_CONTEXT, "users", document("one", "One", 1)),
      ).rejects.toThrow();
      const result = await adapter.$client.query<{ count: number }>(
        `SELECT COUNT(*) AS count FROM ${writeTable("users")}`,
      );
      expect(result.rows[0]?.count).toBe(0);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("maps duplicate writes to the typed SQLite exception", async () => {
    const adapter = await createDocumentAdapter();
    try {
      await adapter.createDocument(
        SYSTEM_CONTEXT,
        "users",
        document("duplicate", "First", 1),
      );

      await expect(
        adapter.createDocument(
          SYSTEM_CONTEXT,
          "users",
          document("duplicate", "Second", 2),
        ),
      ).rejects.toBeInstanceOf(DuplicateException);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("updates documents, permissions, counters, and deletes atomically", async () => {
    const adapter = await createDocumentAdapter();
    try {
      const first = document("one", "One", 1);
      const second = document("two", "Two", 2);
      await adapter.createDocuments(SYSTEM_CONTEXT, "users", [first, second]);

      first.set("name", "Updated");
      first.set("$permissions", [Permission.update(Role.any()).toString()]);
      await adapter.updateDocument(SYSTEM_CONTEXT, "users", first);
      const updated = await adapter.$client.query<{
        name: string;
        _permissions: string;
      }>(`SELECT "name", "_permissions" FROM ${writeTable("users")} WHERE "_id" = ?`, [first.getSequence()]);
      expect(updated.rows[0]?.name).toBe("Updated");
      expect(JSON.parse(updated.rows[0]!._permissions)).toEqual(["update(\"any\")"]);

      const affected = await adapter.updateDocuments(
        SYSTEM_CONTEXT,
        "users",
        new Doc<Record<string, any>>({ name: "Bulk" }),
        [first, second],
      );
      expect(affected).toBe(2);
      await adapter.increaseDocumentAttribute({
        ctx: SYSTEM_CONTEXT,
        collection: "users",
        id: "one",
        attribute: "score",
        value: 4,
        updatedAt: new Date("2026-01-02T00:00:00.000Z"),
        max: 10,
      });
      const score = await adapter.$client.query<{ score: number }>(
        `SELECT "score" FROM ${writeTable("users")} WHERE "_uid" = ?`,
        ["one"],
      );
      expect(score.rows[0]?.score).toBe(5);

      expect(await adapter.deleteDocument(SYSTEM_CONTEXT, "users", first)).toBe(true);
      expect(
        await adapter.deleteDocumentsBySequences(
          SYSTEM_CONTEXT,
          "users",
          [second.getSequence()],
          [String(second.getSequence())],
        ),
      ).toBe(1);
      const remaining = await adapter.$client.query<{ count: number }>(
        `SELECT COUNT(*) AS count FROM ${writeTable("users")}`,
      );
      expect(remaining.rows[0]?.count).toBe(0);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("deletes permission rows by numeric sequence when callers pass UID permission IDs", async () => {
    const adapter = await createDocumentAdapter();
    try {
      const created = document("uid-not-sequence", "One", 1);
      await adapter.createDocument(SYSTEM_CONTEXT, "users", created);

      expect(
        await adapter.deleteDocumentsBySequences(
          SYSTEM_CONTEXT,
          "users",
          [created.getSequence()],
          [created.getId()],
        ),
      ).toBe(1);

      const permissions = await adapter.$client.query<{ count: number }>(
        `SELECT COUNT(*) AS count FROM ${writeTable("users_perms")}`,
      );
      expect(permissions.rows[0]?.count).toBe(0);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("upserts by identity and supports increment-on-conflict", async () => {
    const adapter = await createDocumentAdapter();
    try {
      const original = document("one", "One", 2);
      await adapter.createDocument(SYSTEM_CONTEXT, "users", original);
      const replacement = document("one", "Changed", 3);

      const replaced = await adapter.createOrUpdateDocuments(
        SYSTEM_CONTEXT,
        "users",
        "",
        [{ old: original, new: replacement }],
      );
      expect(replaced[0]?.getSequence()).toBe(original.getSequence());

      const increment = document("one", "Ignored", 4);
      await adapter.createOrUpdateDocuments(SYSTEM_CONTEXT, "users", "score", [
        { old: replacement, new: increment },
      ]);
      const stored = await adapter.$client.query<{ name: string; score: number }>(
        `SELECT "name", "score" FROM ${writeTable("users")} WHERE "_uid" = ?`,
        ["one"],
      );
      expect(stored.rows[0]).toEqual({ name: "Changed", score: 7 });
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("does not expose an upsert sequence when permission writes roll back", async () => {
    const adapter = await createDocumentAdapter();
    try {
      await adapter.$client.query(`DROP TABLE ${writeTable("users_perms")}`);
      const inserted = document("one", "One", 1);

      await expect(
        adapter.createOrUpdateDocuments(SYSTEM_CONTEXT, "users", "", [
          { old: new Doc(), new: inserted },
        ]),
      ).rejects.toThrow();

      expect(inserted.getSequence()).toBeNull();
      const result = await adapter.$client.query<{ count: number }>(
        `SELECT COUNT(*) AS count FROM ${writeTable("users")}`,
      );
      expect(result.rows[0]?.count).toBe(0);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("enforces adjusted increase and decrease bounds", async () => {
    const adapter = await createDocumentAdapter();
    try {
      await adapter.createDocument(
        SYSTEM_CONTEXT,
        "users",
        document("one", "One", 5),
      );
      const updatedAt = new Date("2026-01-02T00:00:00.000Z");

      expect(
        await adapter.increaseDocumentAttribute({
          ctx: SYSTEM_CONTEXT,
          collection: "users",
          id: "one",
          attribute: "score",
          value: 4,
          updatedAt,
          max: 4,
        }),
      ).toBe(false);
      expect(
        await adapter.increaseDocumentAttribute({
          ctx: SYSTEM_CONTEXT,
          collection: "users",
          id: "one",
          attribute: "score",
          value: -3,
          updatedAt,
          min: 3,
        }),
      ).toBe(true);
      expect(
        await adapter.increaseDocumentAttribute({
          ctx: SYSTEM_CONTEXT,
          collection: "users",
          id: "one",
          attribute: "score",
          value: -3,
          updatedAt,
          min: 3,
        }),
      ).toBe(false);

      const stored = await adapter.$client.query<{ score: number }>(
        `SELECT "score" FROM ${writeTable("users")} WHERE "_uid" = ?`,
        ["one"],
      );
      expect(stored.rows[0]?.score).toBe(2);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("deletes documents selected by a processed query", async () => {
    const adapter = await createDocumentAdapter();
    try {
      await adapter.createDocuments(SYSTEM_CONTEXT, "users", [
        document("one", "Delete", 1),
        document("two", "Keep", 2),
      ]);
      const query = {
        collection: new Doc<Collection>({ $id: "users", attributes: documentAttributes }),
        filters: [Query.equal("name", ["Delete"])],
        selections: [],
        populateQueries: [],
        limit: null,
        offset: null,
        orders: {},
        cursor: null,
        cursorDirection: null,
        skipAuth: true,
      } satisfies ProcessedQuery;

      expect(await adapter.deleteDocuments(SYSTEM_CONTEXT, "users", query)).toEqual(["one"]);
      const rows = await adapter.$client.query<{ _uid: string }>(
        `SELECT "_uid" FROM ${writeTable("users")}`,
      );
      const permissions = await adapter.$client.query<{ _document: number }>(
        `SELECT "_document" FROM ${writeTable("users_perms")}`,
      );
      expect(rows.rows).toEqual([{ _uid: "two" }]);
      expect(permissions.rows).toHaveLength(1);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("scopes updates, deletes, permissions, and upserts by tenant", async () => {
    const adapter = await createDocumentAdapter(true);
    try {
      const tenantOne = document("same", "One", 1, 1);
      await adapter.createDocument(SYSTEM_CONTEXT, "users", tenantOne);
      adapter.setMeta({ tenantId: 2 });
      const tenantTwo = document("same", "Two", 2, 2);
      await adapter.createDocument(SYSTEM_CONTEXT, "users", tenantTwo);

      tenantTwo.set("name", "Tenant Two");
      await adapter.updateDocument(SYSTEM_CONTEXT, "users", tenantTwo);
      adapter.setMeta({ tenantId: 1 });
      await adapter.createOrUpdateDocuments(SYSTEM_CONTEXT, "users", "", [
        { old: tenantOne, new: document("same", "Tenant One", 10, 1) },
      ]);
      await adapter.deleteDocument(SYSTEM_CONTEXT, "users", tenantOne);

      const rows = await adapter.$client.query<{ _tenant: number; name: string }>(
        `SELECT "_tenant", "name" FROM ${writeTable("users")} ORDER BY "_tenant"`,
      );
      const permissions = await adapter.$client.query<{ _tenant: number }>(
        `SELECT "_tenant" FROM ${writeTable("users_perms")} ORDER BY "_tenant"`,
      );
      expect(rows.rows).toEqual([{ _tenant: 2, name: "Tenant Two" }]);
      expect(permissions.rows).toEqual([{ _tenant: 2 }]);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("does not update permissions for bulk rows outside the active tenant", async () => {
    const adapter = await createDocumentAdapter(true);
    try {
      await adapter.createDocument(
        SYSTEM_CONTEXT,
        "users",
        document("same", "Tenant One", 1, 1),
      );
      adapter.setMeta({ tenantId: 2 });
      const tenantTwo = document("same", "Tenant Two", 2, 2);
      await adapter.createDocument(SYSTEM_CONTEXT, "users", tenantTwo);

      tenantTwo.set("name", "Hacked");
      tenantTwo.set("$permissions", [Permission.update(Role.any()).toString()]);
      adapter.setMeta({ tenantId: 1 });
      const affected = await adapter.updateDocuments(
        SYSTEM_CONTEXT,
        "users",
        new Doc<Record<string, any>>({
          name: "Hacked",
          $permissions: [Permission.update(Role.any()).toString()],
        }),
        [tenantTwo],
      );

      expect(affected).toBe(0);
      const stored = await adapter.$client.query<{
        name: string;
        _permissions: string;
      }>(
        `SELECT "name", "_permissions" FROM ${writeTable("users")} WHERE "_tenant" = ?`,
        [2],
      );
      const permission = await adapter.$client.query<{
        _permissions: string;
      }>(
        `SELECT "_permissions" FROM ${writeTable("users_perms")} WHERE "_tenant" = ?`,
        [2],
      );
      expect(stored.rows[0]?.name).toBe("Tenant Two");
      expect(JSON.parse(stored.rows[0]!._permissions)).toEqual([
        'read("any")',
      ]);
      expect(JSON.parse(permission.rows[0]!._permissions)).toEqual(["any"]);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("isolates find, getDocument, count, and sum by tenant", async () => {
    const adapter = await createDocumentAdapter(true);
    const query = readQuery();
    try {
      await adapter.createDocument(
        SYSTEM_CONTEXT,
        "users",
        document("same", "Tenant One", 10, 1),
      );
      adapter.setMeta({ tenantId: 2 });
      await adapter.createDocument(
        SYSTEM_CONTEXT,
        "users",
        document("same", "Tenant Two", 20, 2),
      );

      adapter.setMeta({ tenantId: 1 });
      expect(
        (await adapter.find(SYSTEM_CONTEXT, "users", query)).map(
          (row) => row["name"],
        ),
      ).toEqual(["Tenant One"]);
      expect(
        (await adapter.getDocument(SYSTEM_CONTEXT, "users", "same", query)).get(
          "name",
        ),
      ).toBe("Tenant One");
      expect(await adapter.count(SYSTEM_CONTEXT, "users")).toBe(1);
      expect(await adapter.sum(SYSTEM_CONTEXT, "users", "score")).toBe(10);

      adapter.setMeta({ tenantId: 2 });
      expect(
        (await adapter.find(SYSTEM_CONTEXT, "users", query)).map(
          (row) => row["name"],
        ),
      ).toEqual(["Tenant Two"]);
      expect(await adapter.count(SYSTEM_CONTEXT, "users")).toBe(1);
      expect(await adapter.sum(SYSTEM_CONTEXT, "users", "score")).toBe(20);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("filters permission side-table rows by both role and type", async () => {
    const adapter = await createDocumentAdapter();
    const secured = new Doc<Collection>({
      $id: "users",
      attributes: documentAttributes,
      documentSecurity: true,
    });
    const readable = document("secured", "Secured", 1);
    readable.set("$permissions", [
      Permission.read(Role.user("reader")).toString(),
      Permission.update(Role.user("editor")).toString(),
    ]);

    try {
      await adapter.createDocument(SYSTEM_CONTEXT, "users", readable);

      expect(
        await adapter.find({ roles: ["user:reader"] }, "users", readQuery(secured)),
      ).toHaveLength(1);
      expect(
        await adapter.find(
          { roles: ["user:reader"] },
          "users",
          readQuery(secured),
          { forPermission: PermissionEnum.Update },
        ),
      ).toHaveLength(0);
      expect(
        await adapter.find(
          { roles: ["user:editor"] },
          "users",
          readQuery(secured),
          { forPermission: PermissionEnum.Update },
        ),
      ).toHaveLength(1);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("applies write lifecycle transformations to bulk creates and upserts", async () => {
    const adapter = await createDocumentAdapter();
    const events: EventsEnum[] = [];
    const record = (event: EventsEnum) => (sql: string): string => {
      events.push(event);
      return sql;
    };
    adapter.before(
      EventsEnum.DocumentsCreate,
      "test-documents-create",
      record(EventsEnum.DocumentsCreate),
    );
    adapter.before(
      EventsEnum.DocumentsUpsert,
      "test-documents-upsert",
      record(EventsEnum.DocumentsUpsert),
    );
    adapter.before(
      EventsEnum.PermissionsUpdate,
      "test-permissions-update",
      record(EventsEnum.PermissionsUpdate),
    );

    try {
      const original = document("one", "One", 1);
      await adapter.createDocuments(SYSTEM_CONTEXT, "users", [original]);
      const replacement = document("one", "Updated", 2);
      replacement.set("$permissions", [
        Permission.read(Role.user("updated")).toString(),
      ]);
      await adapter.createOrUpdateDocuments(SYSTEM_CONTEXT, "users", "", [
        { old: original, new: replacement },
      ]);

      expect(events).toContain(EventsEnum.DocumentsCreate);
      expect(events).toContain(EventsEnum.DocumentsUpsert);
      expect(events).toContain(EventsEnum.PermissionsUpdate);
    } finally {
      await adapter.$client.disconnect();
    }
  });

  test("isolates transaction metadata and transformation containers", async () => {
    const adapter = new InspectableSQLiteAdapter(":memory:");
    const logger = new Logger({ enabled: false });
    adapter.setLogger(logger).setMeta({
      schema: "app",
      namespace: "parent",
      metadata: { request: "parent" },
    });
    adapter.before(EventsEnum.All, "parent", (sql) => `parent:${sql}`);
    const parentState = adapter.state;

    try {
      await adapter.transaction(async (transaction) => {
        const transactionState = transaction.state;
        expect(transactionState.meta).not.toBe(parentState.meta);
        expect(transactionState.transformations).not.toBe(
          parentState.transformations,
        );
        expect(transactionState.logger).toBe(logger);
        expect(transaction.$metadata).toEqual({ request: "parent" });

        transaction.setMeta({
          namespace: "transaction",
          metadata: { request: "transaction" },
        });
        transaction.before(EventsEnum.CollectionCreate, "transaction", (sql) =>
          `transaction:${sql}`,
        );
        expect(transaction.$namespace).toBe("transaction");
        expect(
          transaction.trigger(EventsEnum.CollectionCreate, "SELECT 1"),
        ).toContain(
          "transaction:",
        );
      });

      expect(adapter.$namespace).toBe("parent");
      expect(adapter.$metadata).toEqual({ request: "parent" });
      expect(adapter.trigger(EventsEnum.CollectionCreate, "SELECT 1")).toBe(
        "parent:SELECT 1",
      );
    } finally {
      await adapter.$client.disconnect();
      await logger.close();
    }
  });
});
