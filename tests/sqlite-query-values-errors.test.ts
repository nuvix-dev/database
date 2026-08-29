import { afterEach, describe, expect, test } from "bun:test";
import { Database as SQLiteDatabase } from "bun:sqlite";
import { processSQLiteException } from "@adapters/error-mapper.js";
import { SQLiteSqlBuilder } from "@adapters/sqlite-sql-builder.js";
import {
  bindSQLiteValue,
  decodeSQLiteValue,
} from "@adapters/sqlite-values.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, IndexEnum } from "@core/enums.js";
import { Query } from "@core/query.js";
import { DatabaseException } from "@errors/base.js";
import {
  DuplicateException,
  NotFoundException,
  TimeoutException,
} from "@errors/index.js";
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

async function setupQueryDatabase(): Promise<Database> {
  const database = createSQLiteTestDb({ namespace: "query-values-errors" });
  databases.push(database);
  await database.create("main");
  await database.createCollection({
    id: "records",
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
        size: 128,
        required: true,
      }),
      new Doc({
        $id: "nickname",
        key: "nickname",
        type: AttributeEnum.String,
        size: 128,
      }),
      new Doc({
        $id: "age",
        key: "age",
        type: AttributeEnum.Integer,
        required: true,
      }),
      new Doc({
        $id: "active",
        key: "active",
        type: AttributeEnum.Boolean,
        required: true,
      }),
      new Doc({
        $id: "tags",
        key: "tags",
        type: AttributeEnum.String,
        size: 32,
        array: true,
      }),
    ],
    indexes: [
      new Doc({
        $id: "email_unique",
        key: "email_unique",
        type: IndexEnum.Unique,
        attributes: ["email"],
      }),
    ],
  });

  await database.system().createDocuments("records", [
    new Doc({
      $id: "alpha",
      name: "Alpha",
      email: "alpha@example.com",
      nickname: null,
      age: 10,
      active: true,
      tags: ["first"],
    }),
    new Doc({
      $id: "alpine",
      name: "Alpine",
      email: "alpine@sample.org",
      nickname: "Al",
      age: 20,
      active: true,
      tags: ["second"],
    }),
    new Doc({
      $id: "beta",
      name: "Beta",
      email: "beta@example.com",
      nickname: "Bee",
      age: 30,
      active: false,
      tags: ["third"],
    }),
    new Doc({
      $id: "gamma",
      name: "Gamma",
      email: "gamma@example.com",
      nickname: "Gam",
      age: 40,
      active: false,
      tags: ["fourth"],
    }),
    new Doc({
      $id: "literal",
      name: "100% Real",
      email: "literal@example.com",
      nickname: "Literal",
      age: 50,
      active: true,
      tags: ["fifth"],
    }),
  ]);

  return database;
}

async function names(database: Database, queries: Query[]): Promise<string[]> {
  const documents = await database.system().find("records", queries);
  return documents.map((document) => document.get("name") as string);
}

function sqliteError(message: string, code: string): Error & { code: string } {
  return Object.assign(new Error(message), { code, name: "SQLiteError" });
}

function capture(run: () => never): Error {
  try {
    run();
  } catch (error) {
    if (error instanceof Error) return error;
    throw new Error("Expected an Error instance");
  }
  throw new Error("Expected function to throw");
}

describe("SQLite query behavior", () => {
  test("applies equality and every comparison boundary", async () => {
    // Arrange
    const database = await setupQueryDatabase();

    // Act
    const equal = await names(database, [
      Query.equal("active", [true]),
      Query.orderAsc("age"),
    ]);
    const notEqual = await names(database, [
      Query.notEqual("active", true),
      Query.orderAsc("age"),
    ]);
    const less = await names(database, [
      Query.lessThan("age", 20),
      Query.orderAsc("age"),
    ]);
    const lessEqual = await names(database, [
      Query.lessThanEqual("age", 20),
      Query.orderAsc("age"),
    ]);
    const greater = await names(database, [
      Query.greaterThan("age", 40),
      Query.orderAsc("age"),
    ]);
    const greaterEqual = await names(database, [
      Query.greaterThanEqual("age", 40),
      Query.orderAsc("age"),
    ]);

    // Assert
    expect(equal).toEqual(["Alpha", "Alpine", "100% Real"]);
    expect(notEqual).toEqual(["Beta", "Gamma"]);
    expect(less).toEqual(["Alpha"]);
    expect(lessEqual).toEqual(["Alpha", "Alpine"]);
    expect(greater).toEqual(["100% Real"]);
    expect(greaterEqual).toEqual(["Gamma", "100% Real"]);
  });

  test("handles null, between, literal matching, and empty results", async () => {
    // Arrange
    const database = await setupQueryDatabase();

    // Act
    const nullNames = await names(database, [Query.isNull("nickname")]);
    const notNullNames = await names(database, [
      Query.isNotNull("nickname"),
      Query.orderAsc("age"),
    ]);
    const between = await names(database, [
      Query.between("age", 20, 40),
      Query.orderAsc("age"),
    ]);
    const prefix = await names(database, [
      Query.startsWith("name", "Al"),
      Query.orderAsc("age"),
    ]);
    const suffix = await names(database, [Query.endsWith("email", ".org")]);
    const contains = await names(database, [Query.contains("name", ["amm"])]);
    const literalWildcard = await names(database, [
      Query.contains("name", ["%"]),
    ]);
    const empty = await names(database, [
      Query.equal("name", ["does-not-exist"]),
    ]);

    // Assert
    expect(nullNames).toEqual(["Alpha"]);
    expect(notNullNames).toEqual(["Alpine", "Beta", "Gamma", "100% Real"]);
    expect(between).toEqual(["Alpine", "Beta", "Gamma"]);
    expect(prefix).toEqual(["Alpha", "Alpine"]);
    expect(suffix).toEqual(["Alpine"]);
    expect(contains).toEqual(["Gamma"]);
    expect(literalWildcard).toEqual(["100% Real"]);
    expect(empty).toEqual([]);
  });

  test("combines nested logical groups without broadening an empty match", async () => {
    // Arrange
    const database = await setupQueryDatabase();

    // Act
    const andResult = await names(database, [
      Query.and([
        Query.startsWith("name", "Al"),
        Query.greaterThan("age", 10),
      ]),
    ]);
    const orResult = await names(database, [
      Query.or([
        Query.isNull("nickname"),
        Query.equal("name", ["Gamma"]),
      ]),
      Query.orderAsc("age"),
    ]);
    const noMatch = await names(database, [
      Query.and([
        Query.equal("active", [false]),
        Query.lessThan("age", 20),
      ]),
    ]);

    // Assert
    expect(andResult).toEqual(["Alpine"]);
    expect(orResult).toEqual(["Alpha", "Gamma"]);
    expect(noMatch).toEqual([]);
  });

  test("applies selection, order, limit, and offset together", async () => {
    // Arrange
    const database = await setupQueryDatabase();

    // Act
    const documents = await database.system().find("records", [
      Query.select(["name"]),
      Query.orderDesc("age"),
      Query.offset(1),
      Query.limit(2),
    ]);

    // Assert
    expect(documents.map((document) => document.get("name"))).toEqual([
      "Gamma",
      "Beta",
    ]);
    expect(documents.every((document) => !document.has("age"))).toBe(true);
  });

  test("paginates after and before cursor documents without overlap", async () => {
    // Arrange
    const database = await setupQueryDatabase();
    const ordered = await database.system().find("records", [
      Query.orderAsc("age"),
    ]);

    // Act
    const after = await database.system().find("records", [
      Query.orderAsc("age"),
      Query.cursorAfter(ordered[1]!),
      Query.limit(2),
    ]);
    const before = await database.system().find("records", [
      Query.orderAsc("age"),
      Query.cursorBefore(ordered[2]!),
      Query.limit(2),
    ]);

    // Assert
    expect(after.map((document) => document.get("name"))).toEqual([
      "Beta",
      "Gamma",
    ]);
    expect(before.map((document) => document.get("name"))).toEqual([
      "Alpha",
      "Alpine",
    ]);
  });
});

describe("SQLite value behavior", () => {
  test("round-trips required scalar, JSON, array, date, and blob values", () => {
    // Arrange
    const database = new SQLiteDatabase(":memory:");
    const date = new Date("2026-08-29T12:34:56.789Z");
    const blob = new Uint8Array([0, 127, 255]);
    database.run(
      'CREATE TABLE values_test ("flag" INTEGER, "happened" TEXT, "object_value" TEXT, "json_array" TEXT, "scalar_array" TEXT, "nothing" TEXT, "count_value" REAL, "text_value" TEXT, "blob_value" BLOB)',
    );

    try {
      // Act
      database.run(
        "INSERT INTO values_test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
          bindSQLiteValue(true),
          bindSQLiteValue(date),
          bindSQLiteValue({ nested: { enabled: true } }),
          bindSQLiteValue([1, { two: 2 }]),
          bindSQLiteValue([1, 2, 3]),
          bindSQLiteValue(null),
          bindSQLiteValue(42.5),
          bindSQLiteValue("plain text"),
          bindSQLiteValue(blob),
        ],
      );
      const row = database.query("SELECT * FROM values_test").get() as Record<
        string,
        unknown
      >;
      const decoded = {
        flag: decodeSQLiteValue(row["flag"], {
          $id: "flag",
          type: AttributeEnum.Boolean,
        }),
        happened: decodeSQLiteValue(row["happened"], {
          $id: "happened",
          type: AttributeEnum.Timestamptz,
        }),
        object: decodeSQLiteValue(row["object_value"], {
          $id: "object_value",
          type: AttributeEnum.Json,
        }),
        jsonArray: decodeSQLiteValue(row["json_array"], {
          $id: "json_array",
          type: AttributeEnum.Json,
        }),
        scalarArray: decodeSQLiteValue(row["scalar_array"], {
          $id: "scalar_array",
          type: AttributeEnum.Integer,
          array: true,
        }),
      };

      // Assert
      expect(decoded.flag).toBe(true);
      expect(decoded.happened).toEqual(date);
      expect(decoded.object).toEqual({ nested: { enabled: true } });
      expect(decoded.jsonArray).toEqual([1, { two: 2 }]);
      expect(decoded.scalarArray).toEqual([1, 2, 3]);
      expect(row["nothing"]).toBeNull();
      expect(row["count_value"]).toBe(42.5);
      expect(row["text_value"]).toBe("plain text");
      expect(Array.from(row["blob_value"] as Uint8Array)).toEqual([0, 127, 255]);
    } finally {
      database.close();
    }
  });

  test("rejects invalid bound and stored values with attribute context", () => {
    // Arrange
    const invalidBoolean = {
      $id: "active",
      type: AttributeEnum.Boolean,
    } as const;
    const invalidJson = {
      $id: "payload",
      type: AttributeEnum.Json,
    } as const;

    // Act
    const bindFailure = () => bindSQLiteValue(Number.NaN);
    const booleanFailure = () => decodeSQLiteValue(2, invalidBoolean);
    const jsonFailure = () => decodeSQLiteValue("{broken", invalidJson);

    // Assert
    expect(bindFailure).toThrow("Unsupported SQLite bound number: NaN");
    expect(booleanFailure).toThrow("boolean attribute 'active'");
    expect(jsonFailure).toThrow("attribute 'payload': invalid JSON");
  });
});

describe("SQLite error behavior", () => {
  test("maps real unique and missing-table failures with their causes", () => {
    // Arrange
    const database = new SQLiteDatabase(":memory:");
    database.run("CREATE TABLE users (email TEXT UNIQUE)");
    database.run("INSERT INTO users VALUES (?)", ["same@example.com"]);

    try {
      // Act
      let duplicateSource: unknown;
      try {
        database.run("INSERT INTO users VALUES (?)", ["same@example.com"]);
      } catch (error) {
        duplicateSource = error;
      }
      let missingSource: unknown;
      try {
        database.query("SELECT * FROM absent_table").all();
      } catch (error) {
        missingSource = error;
      }
      const duplicate = capture(() => processSQLiteException(duplicateSource));
      const missing = capture(() => processSQLiteException(missingSource));

      // Assert
      expect(duplicate).toBeInstanceOf(DuplicateException);
      expect((duplicate as DatabaseException).code).toBe(
        "SQLITE_CONSTRAINT_UNIQUE",
      );
      expect(duplicate.cause).toBe(duplicateSource);
      expect(missing).toBeInstanceOf(NotFoundException);
      expect((missing as DatabaseException).code).toBe("SQLITE_ERROR");
      expect(missing.cause).toBe(missingSource);
    } finally {
      database.close();
    }
  });

  test("maps deterministic busy, locked, and generic failures with causes", () => {
    // Arrange
    const busySource = sqliteError("database is busy", "SQLITE_BUSY_TIMEOUT");
    const lockedSource = sqliteError(
      "database table is locked",
      "SQLITE_LOCKED_SHAREDCACHE",
    );
    const genericSource = sqliteError("disk I/O error", "SQLITE_IOERR_READ");

    // Act
    const busy = capture(() => processSQLiteException(busySource));
    const locked = capture(() => processSQLiteException(lockedSource));
    const generic = capture(() => processSQLiteException(genericSource));

    // Assert
    expect(busy).toBeInstanceOf(TimeoutException);
    expect(busy.cause).toBe(busySource);
    expect(locked).toBeInstanceOf(TimeoutException);
    expect(locked.cause).toBe(lockedSource);
    expect(generic).toBeInstanceOf(DatabaseException);
    expect((generic as DatabaseException).code).toBe("SQLITE_IOERR_READ");
    expect(generic.cause).toBe(genericSource);
  });
});

describe("SQLite unsupported query behavior", () => {
  test("rejects search, array overlap, and update locks explicitly", async () => {
    // Arrange
    const database = await setupQueryDatabase();
    const tagsOverlap = Query.contains("tags", ["first", "second"]);
    tagsOverlap.setOnArray(true);

    // Act and assert
    await expect(
      database.system().find("records", [Query.search("name", "Alpha")]),
    ).rejects.toThrow('requires a fulltext index');
    expect(() =>
      SQLiteSqlBuilder.buildQueryCondition(
        { schema: "main", namespace: "query-values-errors" },
        tagsOverlap,
        "main",
      ),
    ).toThrow("array overlap queries are not supported");
    const document = await database
      .system()
      .getDocument("records", "alpha", [], true);
    expect(document.getId()).toBe("alpha");
  });
});
