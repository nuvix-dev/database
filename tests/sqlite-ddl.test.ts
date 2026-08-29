import { describe, expect, test } from "bun:test";
import { Doc } from "@core/doc.js";
import {
  AttributeEnum,
  IndexEnum,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";
import { DatabaseException } from "@errors/base.js";
import {
  SQLiteDdl,
  type SQLiteDdlContext,
} from "@adapters/sqlite-ddl.js";
import { SQLiteClient } from "@adapters/sqlite.js";
import { SQLiteSqlBuilder } from "@adapters/sqlite-sql-builder.js";
import type { Meta } from "@adapters/base.js";
import type { Attribute, Index } from "@validators/schema.js";

const createDdl = (
  client: SQLiteClient,
  meta: Partial<Meta> = {
    schema: "app",
    namespace: "tests",
    sharedTables: false,
  },
): SQLiteDdl => {
  const context: SQLiteDdlContext = {
    get $schema() {
      return meta.schema ?? "app";
    },
    get $sharedTables() {
      return meta.sharedTables === true;
    },
    get $namespace() {
      return meta.namespace ?? "default";
    },
    get $client() {
      return client;
    },
    sanitize: SQLiteSqlBuilder.sanitize,
    quote: SQLiteSqlBuilder.quote,
    trigger: (_event, sql) => sql,
    getSQLType: SQLiteSqlBuilder.getSQLType,
    getSQLTable: (name) => SQLiteSqlBuilder.getSQLTable(meta, name),
    getSQLIndex: (table, name) =>
      SQLiteSqlBuilder.getSQLIndex(meta, table, name),
    getInternalKeyForAttribute: SQLiteSqlBuilder.getInternalKeyForAttribute,
  };
  return new SQLiteDdl(context);
};

const attribute = (
  id: string,
  type: AttributeEnum,
  options: Partial<Attribute> = {},
): Doc<Attribute> =>
  new Doc<Attribute>({
    $id: id,
    key: id,
    type,
    ...options,
  });

const index = (
  id: string,
  type: IndexEnum,
  attributes: string[],
): Doc<Index> =>
  new Doc<Index>({
    $id: id,
    key: id,
    type,
    attributes,
  });

describe("SQLiteDdl", () => {
  test("uses schema prefixes for create, exists, collection tables, and delete", async () => {
    const client = new SQLiteClient(":memory:");
    const meta = { schema: "app", namespace: "tenant", sharedTables: true };
    const ddl = createDdl(client, meta);

    try {
      expect(await ddl.exists("app")).toBe(false);
      await ddl.create("app");
      expect(await ddl.exists("app")).toBe(true);

      await ddl.createCollection({
        name: "users",
        attributes: [
          attribute("email", AttributeEnum.String),
          attribute("age", AttributeEnum.Integer),
        ],
        indexes: [
          index("email_age", IndexEnum.Key, ["email", "age"]),
          index("email_unique", IndexEnum.Unique, ["email"]),
        ],
      });

      expect(await ddl.exists("app", "users")).toBe(true);
      expect(await ddl.exists("app", "users_perms")).toBe(true);

      const table = SQLiteSqlBuilder.getTableName(meta, "users");
      const permissionTable = SQLiteSqlBuilder.getTableName(meta, "users_perms");
      const { rows: tables } = await client.query<{ name: string }>(
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN (?, ?) ORDER BY name",
        [table, permissionTable],
      );
      expect(tables.map(({ name }) => name).sort()).toEqual(
        [table, permissionTable].sort(),
      );

      const { rows: columns } = await client.query<{
        name: string;
        type: string;
      }>("SELECT name, type FROM pragma_table_info(?) ORDER BY cid", [table]);
      expect(columns).toContainEqual({ name: "_id", type: "INTEGER" });
      expect(columns).toContainEqual({ name: "_tenant", type: "INTEGER" });
      expect(columns).toContainEqual({ name: "_permissions", type: "TEXT" });

      const { rows: foreignKeys } = await client.query<{
        table: string;
        from: string;
        to: string;
      }>("SELECT * FROM pragma_foreign_key_list(?)", [permissionTable]);
      expect(foreignKeys).toContainEqual(
        expect.objectContaining({ table, from: "_document", to: "_id" }),
      );

      const { rows: keyColumns } = await client.query<{ name: string }>(
        "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
        [SQLiteSqlBuilder.getIndexName(meta, "users", "email_age")],
      );
      expect(keyColumns.map(({ name }) => name)).toEqual([
        "_tenant",
        "email",
        "age",
      ]);

      await ddl.analyzeCollection("users");
      await ddl.delete("app");
      expect(await ddl.exists("app")).toBe(false);
    } finally {
      await client.disconnect();
    }
  });

  test("alters attributes transactionally while preserving rows and indexes", async () => {
    const client = new SQLiteClient(":memory:");
    const meta = { schema: "app", namespace: "alter", sharedTables: false };
    const ddl = createDdl(client, meta);
    const table = SQLiteSqlBuilder.getSQLTable(meta, "items");
    const preservedIndex = SQLiteSqlBuilder.getIndexName(
      meta,
      "items",
      "preserved",
    );

    try {
      await ddl.createCollection({
        name: "items",
        attributes: [
          attribute("value", AttributeEnum.String),
          attribute("preserved", AttributeEnum.String),
        ],
        indexes: [index("preserved", IndexEnum.Key, ["preserved"])],
      });
      await client.query(
        `INSERT INTO ${table} ("_uid", "value", "preserved") VALUES (?, ?, ?)`,
        ["one", "42", "kept"],
      );

      await ddl.createAttribute({
        collection: "items",
        key: "added",
        type: AttributeEnum.Boolean,
      });
      await ddl.renameAttribute("items", "added", "renamed");
      await ddl.updateAttribute({
        collection: "items",
        key: "value",
        type: AttributeEnum.Integer,
        newName: "number",
      });
      await ddl.deleteAttribute("items", "renamed");

      const { rows } = await client.query<{
        number: number;
        preserved: string;
      }>(`SELECT "number", "preserved" FROM ${table}`);
      expect(rows).toEqual([{ number: 42, preserved: "kept" }]);

      const schema = await ddl.getSchemaAttributes("items");
      expect(schema.map((column) => column.getId())).toContain("number");
      expect(schema.map((column) => column.getId())).not.toContain("renamed");
      expect(schema.find((column) => column.getId() === "number")?.get("dataType"))
        .toBe("integer");

      const { rows: indexes } = await client.query<{ name: string }>(
        "SELECT name FROM pragma_index_list(?)",
        [SQLiteSqlBuilder.getTableName(meta, "items")],
      );
      expect(indexes.map(({ name }) => name)).toContain(preservedIndex);
    } finally {
      await client.disconnect();
    }
  });

  test("adds, renames, and deletes relationship columns", async () => {
    const client = new SQLiteClient(":memory:");
    const meta = { schema: "app", namespace: "relations", sharedTables: false };
    const ddl = createDdl(client, meta);

    try {
      await ddl.createCollection({ name: "users", attributes: [] });
      await ddl.createCollection({ name: "profiles", attributes: [] });
      await ddl.createRelationship(
        "users",
        "profiles",
        RelationEnum.OneToOne,
        true,
        "profile",
        "user",
      );
      await ddl.updateRelationship(
        "users",
        "profiles",
        RelationEnum.OneToOne,
        true,
        "profile",
        "user",
        RelationSideEnum.Parent,
        "account",
        "owner",
      );

      expect(
        (await ddl.getSchemaAttributes("users")).map((column) => column.getId()),
      ).toContain("account");
      expect(
        (await ddl.getSchemaAttributes("profiles")).map((column) => column.getId()),
      ).toContain("owner");

      await ddl.deleteRelationship(
        "users",
        "profiles",
        RelationEnum.OneToOne,
        true,
        "account",
        "owner",
        RelationSideEnum.Parent,
      );
      expect(
        (await ddl.getSchemaAttributes("users")).map((column) => column.getId()),
      ).not.toContain("account");
      expect(
        (await ddl.getSchemaAttributes("profiles")).map((column) => column.getId()),
      ).not.toContain("owner");
    } finally {
      await client.disconnect();
    }
  });

  test("rejects fulltext and GIN-style indexes before partial DDL", async () => {
    const client = new SQLiteClient(":memory:");
    const meta = { schema: "app", namespace: "unsupported", sharedTables: false };
    const ddl = createDdl(client, meta);

    try {
      await expect(
        ddl.createCollection({
          name: "articles",
          attributes: [attribute("body", AttributeEnum.String)],
          indexes: [index("search", IndexEnum.FullText, ["body"])],
        }),
      ).rejects.toThrow("fulltext indexes are not supported");
      expect(await ddl.exists("app", "articles")).toBe(false);

      await ddl.createCollection({
        name: "articles",
        attributes: [
          attribute("tags", AttributeEnum.String, { array: true }),
        ],
      });
      await expect(
        ddl.createIndex({
          collection: "articles",
          name: "tags",
          type: IndexEnum.Key,
          attributes: ["tags"],
          attributeTypes: {
            tags: {
              $id: "tags",
              key: "tags",
              type: AttributeEnum.String,
              array: true,
            },
          },
        }),
      ).rejects.toThrow("GIN-style array indexes are not supported");

      const { rows } = await client.query(
        "SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ?",
        [SQLiteSqlBuilder.getIndexName(meta, "articles", "tags")],
      );
      expect(rows).toHaveLength(0);
    } finally {
      await client.disconnect();
    }
  });

  test("refuses an unsafe rebuild when enforced inbound foreign keys exist", async () => {
    const client = new SQLiteClient(":memory:");
    const ddl = createDdl(client, {
      schema: "app",
      namespace: "safe",
      sharedTables: false,
    });

    try {
      await client.query("PRAGMA foreign_keys = ON");
      await ddl.createCollection({
        name: "parents",
        attributes: [attribute("value", AttributeEnum.String)],
      });

      await expect(
        ddl.updateAttribute({
          collection: "parents",
          key: "value",
          type: AttributeEnum.Integer,
        }),
      ).rejects.toBeInstanceOf(DatabaseException);
      await expect(
        ddl.updateAttribute({
          collection: "parents",
          key: "value",
          type: AttributeEnum.Integer,
        }),
      ).rejects.toThrow("table rebuild is unsafe");
    } finally {
      await client.disconnect();
    }
  });
});
