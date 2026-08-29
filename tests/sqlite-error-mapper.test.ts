import { describe, expect, test } from "bun:test";
import { Database as SQLiteDatabase } from "bun:sqlite";
import {
  processException,
  processSQLiteException,
} from "@adapters/error-mapper.js";
import {
  ConflictException,
  DatabaseException,
  DuplicateException,
  NotFoundException,
  TimeoutException,
  TruncateException,
} from "@errors/index.js";

function sqliteError(message: string, code: string): Error & { code: string } {
  return Object.assign(new Error(message), {
    code,
    name: "SQLiteError",
  });
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

describe("processSQLiteException", () => {
  test("maps a real bun:sqlite unique constraint error", () => {
    const database = new SQLiteDatabase(":memory:");
    database.run("CREATE TABLE users (email TEXT UNIQUE)");
    database.run("INSERT INTO users (email) VALUES (?)", ["user@example.com"]);

    try {
      database.run(
        "INSERT INTO users (email) VALUES (?)",
        ["user@example.com"],
      );
      throw new Error("Expected SQLite to reject the duplicate value");
    } catch (error) {
      const mapped = capture(() => processSQLiteException(error));

      expect(mapped).toBeInstanceOf(DuplicateException);
      expect((mapped as DatabaseException).code).toBe(
        "SQLITE_CONSTRAINT_UNIQUE",
      );
      expect(mapped.cause).toBe(error);
    } finally {
      database.close();
    }
  });

  test("maps unique failures and retains their code and cause", () => {
    const source = sqliteError(
      "UNIQUE constraint failed: users.email",
      "SQLITE_CONSTRAINT_UNIQUE",
    );

    const mapped = capture(() => processSQLiteException(source));

    expect(mapped).toBeInstanceOf(DuplicateException);
    expect((mapped as DatabaseException).code).toBe(
      "SQLITE_CONSTRAINT_UNIQUE",
    );
    expect(mapped.cause).toBe(source);
  });

  test("maps other constraint failures to conflicts", () => {
    const source = sqliteError(
      "NOT NULL constraint failed: users.name",
      "SQLITE_CONSTRAINT_NOTNULL",
    );

    const mapped = capture(() => processSQLiteException(source));

    expect(mapped).toBeInstanceOf(ConflictException);
    expect((mapped as DatabaseException).code).toBe(
      "SQLITE_CONSTRAINT_NOTNULL",
    );
    expect(mapped.cause).toBe(source);
  });

  test("maps missing tables and columns to not found exceptions", () => {
    const tableError = sqliteError("no such table: users", "SQLITE_ERROR");
    const columnError = sqliteError(
      "no such column: users.email",
      "SQLITE_ERROR",
    );

    const missingTable = capture(() => processSQLiteException(tableError));
    const missingColumn = capture(() => processSQLiteException(columnError));

    expect(missingTable).toBeInstanceOf(NotFoundException);
    expect(missingTable.message).toBe("Referenced table not found");
    expect(missingTable.cause).toBe(tableError);
    expect(missingColumn).toBeInstanceOf(NotFoundException);
    expect(missingColumn.message).toBe("Referenced column not found");
    expect(missingColumn.cause).toBe(columnError);
  });

  test("maps busy and locked failures to timeout exceptions", () => {
    const busyError = sqliteError("database is locked", "SQLITE_BUSY");
    const lockedError = sqliteError(
      "database table is locked",
      "SQLITE_LOCKED_SHAREDCACHE",
    );

    const busy = capture(() => processSQLiteException(busyError));
    const locked = capture(() => processSQLiteException(lockedError));

    expect(busy).toBeInstanceOf(TimeoutException);
    expect(busy.cause).toBe(busyError);
    expect(locked).toBeInstanceOf(TimeoutException);
    expect(locked.cause).toBe(lockedError);
  });

  test("wraps generic SQLite failures with their original metadata", () => {
    const source = sqliteError("disk I/O error", "SQLITE_IOERR");

    const mapped = capture(() => processSQLiteException(source));

    expect(mapped).toBeInstanceOf(DatabaseException);
    expect(mapped.message).toBe("disk I/O error");
    expect((mapped as DatabaseException).code).toBe("SQLITE_IOERR");
    expect(mapped.cause).toBe(source);
  });

  test("uses a deterministic message for unknown non-Error values", () => {
    const mapped = capture(() => processSQLiteException({ reason: "unknown" }));

    expect(mapped).toBeInstanceOf(DatabaseException);
    expect(mapped.message).toBe("Unexpected database error");
  });
});

describe("processException PostgreSQL regression", () => {
  test("preserves every SQLSTATE mapping and diagnostic message", () => {
    const cases = [
      ["57014", TimeoutException, "Query execution timed out"],
      ["42P07", DuplicateException, "Collection already exists"],
      ["42701", DuplicateException, "Column already exists"],
      [
        "23505",
        DuplicateException,
        "Unique constraint violation: duplicate row",
      ],
      ["22001", TruncateException, "Value too long: data would be truncated"],
      ["42703", NotFoundException, "Referenced column not found"],
    ] as const;

    for (const [code, ExceptionType, expectedMessage] of cases) {
      const source = Object.assign(new Error("server error"), {
        code,
        detail: "detail",
        hint: "hint",
      });

      const mapped = capture(() => processException(source));

      expect(mapped).toBeInstanceOf(ExceptionType);
      expect(mapped.message).toBe(`${expectedMessage} (detail | hint)`);
      expect((mapped as DatabaseException).code).toBe(code);
    }
  });

  test("continues to rethrow unmapped SQLSTATE errors unchanged", () => {
    const source = Object.assign(new Error("server error"), { code: "XX000" });

    const thrown = capture(() => processException(source));

    expect(thrown).toBe(source);
  });
});
