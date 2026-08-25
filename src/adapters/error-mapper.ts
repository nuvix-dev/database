/**
 * Error mapping for database adapters.
 *
 * Extracted from BaseAdapter so that PostgreSQL SQLSTATE → typed-exception
 * mapping lives in a single cohesive, dependency-free module. BaseAdapter
 * delegates its protected processException() facade to the standalone
 * function below.
 */
import { DatabaseException } from "@errors/base.js";
import {
  DuplicateException,
  NotFoundException,
  TimeoutException,
  TruncateException,
} from "@errors/index.js";
import type { DatabaseError } from "./postgres.js";

/**
 * Maps a caught error to a typed exception and throws it.
 *
 * PostgreSQL server errors (SQLSTATE carried in `code`) are translated to
 * TimeoutException, DuplicateException, TruncateException, or
 * NotFoundException as appropriate; DETAIL/HINT diagnostics are appended
 * when present. Non-server errors and unmapped codes are rethrown as-is.
 */
export function processException(
  error: DatabaseError | unknown,
  message?: string,
): never {
  const e = error as DatabaseError;

  if ((e as unknown) instanceof DatabaseException) {
    throw e;
  }

  // Bun.sql surfaces PostgreSQL server errors as Error instances carrying
  // the SQLSTATE in `code`. Anything else is not a server error.
  if (!(e instanceof Error) || typeof e.code !== "string") {
    throw new DatabaseException(
      (e as { message?: string })?.message ??
        message ??
        "Unexpected database error",
      undefined,
      e,
    );
  }

  // Surface PostgreSQL's diagnostic fields (DETAIL/HINT) when present so
  // callers get actionable context without enabling server-side logging.
  const diagnostics = [e.detail, e.hint].filter(Boolean).join(" | ");
  const suffix = diagnostics ? ` (${diagnostics})` : "";

  switch (e.code) {
    case "57014": // Query canceled / timeout
      throw new TimeoutException(
        `Query execution timed out${suffix}`,
        e.code,
        e,
      );

    case "42P07": // Duplicate table
      throw new DuplicateException(
        `Collection already exists${suffix}`,
        e.code,
        e,
      );

    case "42701": // Duplicate column
      throw new DuplicateException(`Column already exists${suffix}`, e.code, e);

    case "23505": // Unique constraint violation (duplicate row)
      throw new DuplicateException(
        `Unique constraint violation: duplicate row${suffix}`,
        e.code,
        e,
      );

    case "22001": // String data right truncation
      throw new TruncateException(
        `Value too long: data would be truncated${suffix}`,
        e.code,
        e,
      );

    case "42703": // Undefined column
      throw new NotFoundException(
        `Referenced column not found${suffix}`,
        e.code,
        e,
      );

    default:
      // For unmapped codes, rethrow to avoid masking potential issues
      throw e;
  }
}
