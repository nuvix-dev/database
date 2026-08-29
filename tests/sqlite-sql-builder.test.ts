import { describe, expect, test } from "bun:test";
import { Database as SQLiteDatabase } from "bun:sqlite";
import { SQLiteSqlBuilder } from "@adapters/sqlite-sql-builder.js";
import {
  CursorEnum,
  IndexEnum,
  OrderEnum,
  PermissionEnum,
} from "@core/enums.js";
import { Doc } from "@core/doc.js";
import type { ProcessedQuery } from "@core/database.js";
import { Query } from "@core/query.js";
import { DatabaseException } from "@errors/base.js";
import type { Collection } from "@validators/schema.js";
import type { IEntity } from "types.js";

const meta = {
  schema: "app.data",
  namespace: "tenant-one",
  sharedTables: true,
  tenantId: 7,
};

const quoteValue = (value: string): string =>
  `'${value.replace(/'/g, "''")}'`;

const cursor = (values: Record<string, unknown>): Doc<IEntity> =>
  new Doc<IEntity>({
    $id: "cursor",
    $collection: "users",
    $createdAt: null,
    $updatedAt: null,
    $permissions: [],
    $sequence: 0,
    ...values,
  });

describe("SQLiteSqlBuilder identifiers", () => {
  test("builds one deterministic unqualified table identifier", () => {
    const first = SQLiteSqlBuilder.getSQLTable(meta, "users");
    const second = SQLiteSqlBuilder.getSQLTable(meta, "users");

    expect(first).toBe(second);
    expect(first).toMatch(/^"app\.data_tenant-one_[a-f0-9]{12}_users"$/);
  });

  test("keeps accepted periods so identifiers cannot collapse", () => {
    expect(SQLiteSqlBuilder.sanitize("foo.bar")).toBe("foo.bar");
    expect(SQLiteSqlBuilder.sanitize("foo.bar")).not.toBe(
      SQLiteSqlBuilder.sanitize("foobar"),
    );
    expect(SQLiteSqlBuilder.getTableName(meta, "foo.bar")).not.toBe(
      SQLiteSqlBuilder.getTableName(meta, "foobar"),
    );
  });

  test("builds globally distinct deterministic index names", () => {
    const users = SQLiteSqlBuilder.getSQLIndex(meta, "users", "email");
    const teams = SQLiteSqlBuilder.getSQLIndex(meta, "teams", "email");

    expect(users).toBe(SQLiteSqlBuilder.getSQLIndex(meta, "users", "email"));
    expect(users).not.toBe(teams);
    expect(users).toMatch(/_[a-f0-9]{20}"$/);
  });
});

describe("SQLiteSqlBuilder predicates", () => {
  test("uses json_extract for JSON paths", () => {
    const condition = SQLiteSqlBuilder.buildQueryCondition(
      meta,
      Query.equal("profile->>name", ["Ada"]),
      "main",
    );

    expect(condition).toEqual({
      sql: `json_extract("main"."profile", '$."name"') = ?`,
      params: ["Ada"],
    });
  });

  test("rejects PostgreSQL-style array overlap", () => {
    const query = Query.contains("tags", ["sqlite", "bun"]);
    query.setOnArray(true);

    expect(() =>
      SQLiteSqlBuilder.buildQueryCondition(meta, query, "main"),
    ).toThrow("array overlap");
  });

  test("generates executable permission JSON overlap SQL", () => {
    const database = new SQLiteDatabase(":memory:");
    const table = SQLiteSqlBuilder.getSQLTable(meta, "users_perms");
    database.run(
      `CREATE TABLE ${table} (_document INTEGER, _type TEXT, _permissions TEXT, _tenant INTEGER)`,
    );
    database.run(
      `INSERT INTO ${table} VALUES (1, 'read', '["role:reader"]', 7)`,
    );
    const condition = SQLiteSqlBuilder.getSQLPermissionsCondition(
      meta,
      {
        collection: "users",
        roles: ["role:reader"],
        alias: "main",
        type: PermissionEnum.Read,
      },
      quoteValue,
    );
    database.run("CREATE TABLE documents (_id INTEGER)");
    database.run("INSERT INTO documents VALUES (1)");

    try {
      const rows = database
        .query(`SELECT 1 FROM documents AS "main" WHERE ${condition}`)
        .all(7);
      expect(rows).toHaveLength(1);
    } finally {
      database.close();
    }
  });

  test("rejects fulltext queries and indexes", () => {
    expect(() =>
      SQLiteSqlBuilder.buildQueryCondition(
        meta,
        Query.search("title", "sqlite"),
        "main",
      ),
    ).toThrow(DatabaseException);
    expect(() => SQLiteSqlBuilder.getSQLIndexType(IndexEnum.FullText)).toThrow(
      "fulltext",
    );
  });
});

describe("SQLiteSqlBuilder pagination and upserts", () => {
  test("builds mixed-direction cursor predicates", () => {
    const document = cursor({ score: 10, $sequence: 42 });

    const result = SQLiteSqlBuilder.buildCursorConditions(
      document,
      CursorEnum.After,
      { score: OrderEnum.Desc },
      "main",
    );

    expect(result.condition).toBe(
      '(("main"."score" < ?) OR ("main"."score" = ? AND "main"."_id" > ?))',
    );
    expect(result.params).toEqual([10, 10, 42]);
  });

  test("emits executable SQLite ON CONFLICT SQL", () => {
    const localMeta = { ...meta, sharedTables: false };
    const table = SQLiteSqlBuilder.getSQLTable(localMeta, "users");
    const database = new SQLiteDatabase(":memory:");
    database.run(
      `CREATE TABLE ${table} (_uid TEXT PRIMARY KEY, name TEXT, _updatedAt TEXT)`,
    );
    const sql = SQLiteSqlBuilder.getUpsertStatement(
      localMeta,
      "users",
      '("_uid", "name", "_updatedAt")',
      ["(?, ?, ?)"],
      { name: "Ada", _updatedAt: "now" },
    );

    try {
      database.run(sql, ["1", "Ada", "first"]);
      database.run(sql, ["1", "Grace", "second"]);
      const row = database.query(`SELECT name FROM ${table}`).get() as {
        name: string;
      };
      expect(row.name).toBe("Grace");
    } finally {
      database.close();
    }
  });

  test("rejects FOR UPDATE", () => {
    expect(() => SQLiteSqlBuilder.getForUpdateClause(true)).toThrow(
      "FOR UPDATE",
    );
  });
});

describe("SQLiteSqlBuilder complete selects", () => {
  test("emits executable offset-only pagination", () => {
    const localMeta = { ...meta, sharedTables: false };
    const collection = new Doc<Collection>({
      $id: "users",
      $collection: "_metadata",
      name: "Users",
      attributes: [],
      indexes: [],
      documentSecurity: false,
      enabled: true,
    });
    const processed: ProcessedQuery = {
      collection,
      selections: ["name"],
      populateQueries: [],
      filters: [],
      orders: {},
      cursor: null,
      cursorDirection: null,
      limit: null,
      offset: 1,
      skipAuth: true,
    };
    const built = SQLiteSqlBuilder.buildSql(
      localMeta,
      processed,
      { forPermission: PermissionEnum.Read, ctx: { roles: [] } },
      quoteValue,
    );
    const table = SQLiteSqlBuilder.getSQLTable(localMeta, "users");
    const database = new SQLiteDatabase(":memory:");

    try {
      database.run(
        `CREATE TABLE ${table} (_uid TEXT, _id INTEGER, _createdAt TEXT, _updatedAt TEXT, _permissions TEXT, name TEXT)`,
      );
      database.run(
        `INSERT INTO ${table} VALUES ('1', 1, NULL, NULL, '[]', 'Ada'), ('2', 2, NULL, NULL, '[]', 'Grace')`,
      );
      const rows = database.query(built.sql).all(1);

      expect(built.sql).toContain("LIMIT -1 OFFSET ?");
      expect(rows).toHaveLength(1);
    } finally {
      database.close();
    }
  });

  test("includes tenancy, ordering, cursor, limit, and aggregate wrapping", () => {
    const collection = new Doc<Collection>({
      $id: "users",
      $collection: "_metadata",
      name: "Users",
      attributes: [],
      indexes: [],
      documentSecurity: false,
      enabled: true,
    });
    const processed: ProcessedQuery = {
      collection,
      selections: ["name"],
      populateQueries: [],
      filters: [Query.greaterThan("age", 18)],
      orders: { name: OrderEnum.Asc },
      cursor: cursor({ name: "M", $sequence: 4 }),
      cursorDirection: CursorEnum.After,
      limit: 10,
      offset: 2,
      skipAuth: true,
    };

    const built = SQLiteSqlBuilder.buildSql(
      meta,
      processed,
      { forPermission: PermissionEnum.Read, ctx: { roles: [] } },
      quoteValue,
    );
    const aggregate = SQLiteSqlBuilder.buildAggregateSql(
      meta,
      "count",
      processed,
      { forPermission: PermissionEnum.Read, ctx: { roles: [] } },
      quoteValue,
    );

    expect(built.sql).toContain('"main"."_tenant" IN (?)');
    expect(built.sql).toContain(
      'ORDER BY "main"."name" ASC, "main"."_id" ASC',
    );
    expect(built.params).toEqual([18, 7, "M", "M", 4, 10, 2]);
    expect(aggregate.sql).toStartWith('SELECT COUNT(1) AS "sum" FROM (');
  });
});
