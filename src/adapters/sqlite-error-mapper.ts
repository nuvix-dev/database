import type { SQLiteError } from "bun:sqlite";
import { DatabaseException } from "@errors/base.js";
import {
  ConflictException,
  DuplicateException,
  NotFoundException,
  TimeoutException,
} from "@errors/index.js";

type ExceptionConstructor = new (
  message: string,
  code?: string,
  cause?: unknown,
) => DatabaseException;

const UNIQUE_CONSTRAINT_CODES = new Set([
  "SQLITE_CONSTRAINT_PRIMARYKEY",
  "SQLITE_CONSTRAINT_ROWID",
  "SQLITE_CONSTRAINT_UNIQUE",
]);

function createException(
  ExceptionType: ExceptionConstructor,
  message: string,
  code: string | undefined,
  cause: unknown,
): DatabaseException {
  const exception = new ExceptionType(message, code, cause);

  // The shared exception constructor currently accepts a cause without
  // assigning it, so retain it here without changing PostgreSQL behavior.
  Object.defineProperty(exception, "cause", {
    configurable: true,
    value: cause,
    writable: true,
  });

  return exception;
}

function isUniqueConstraint(code: string, message: string): boolean {
  if (UNIQUE_CONSTRAINT_CODES.has(code)) return true;

  return (
    code.startsWith("SQLITE_CONSTRAINT") &&
    /(?:UNIQUE|PRIMARY KEY) constraint failed/i.test(message)
  );
}

function isMissing(error: Error, code: string, subject: "table" | "column") {
  return code === "SQLITE_ERROR" && error.message
    .toLowerCase()
    .includes(`no such ${subject}`);
}

/** Maps an error raised by bun:sqlite to the shared typed exception hierarchy. */
export function processSQLiteException(
  error: unknown,
  message?: string,
): never {
  if (error instanceof DatabaseException) throw error;

  if (!(error instanceof Error)) {
    throw createException(
      DatabaseException,
      message ?? "Unexpected database error",
      undefined,
      error,
    );
  }

  const candidate = error as Error & Partial<SQLiteError>;
  const code = typeof candidate.code === "string" ? candidate.code : undefined;

  if (code && isUniqueConstraint(code, error.message)) {
    throw createException(
      DuplicateException,
      "Unique constraint violation: duplicate row",
      code,
      error,
    );
  }

  if (code?.startsWith("SQLITE_CONSTRAINT")) {
    throw createException(
      ConflictException,
      "SQLite constraint violation",
      code,
      error,
    );
  }

  if (code && isMissing(error, code, "table")) {
    throw createException(
      NotFoundException,
      "Referenced table not found",
      code,
      error,
    );
  }

  if (code && isMissing(error, code, "column")) {
    throw createException(
      NotFoundException,
      "Referenced column not found",
      code,
      error,
    );
  }

  if (code === "SQLITE_BUSY" || code?.startsWith("SQLITE_BUSY_")) {
    throw createException(
      TimeoutException,
      "SQLite database is busy",
      code,
      error,
    );
  }

  if (code === "SQLITE_LOCKED" || code?.startsWith("SQLITE_LOCKED_")) {
    throw createException(
      TimeoutException,
      "SQLite database is locked",
      code,
      error,
    );
  }

  throw createException(
    DatabaseException,
    error.message || message || "Unexpected database error",
    code,
    error,
  );
}
