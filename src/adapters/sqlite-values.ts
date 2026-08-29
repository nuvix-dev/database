import { AttributeEnum } from "@core/enums.js";
import { Doc } from "@core/doc.js";
import { DatabaseException } from "@errors/base.js";
import type { Attribute } from "@validators/schema.js";
import { JsonParam } from "./types.js";

export type SQLiteBoundValue =
  | null
  | string
  | number
  | bigint
  | Uint8Array;

export type SQLiteValueMetadata = Pick<Attribute, "$id" | "type" | "array">;

const SQLITE_INTEGER_MIN = -(2n ** 63n);
const SQLITE_INTEGER_MAX = 2n ** 63n - 1n;

function describe(value: unknown): string {
  try {
    if (value === null) return "null";
    if (Array.isArray(value)) return "array";
    if (value instanceof Date) return "Date";
    if (value instanceof Uint8Array) return value.constructor.name;
    if (value instanceof ArrayBuffer) return "ArrayBuffer";
    return typeof value === "object"
      ? (value.constructor?.name ?? "object")
      : typeof value;
  } catch {
    return typeof value;
  }
}

function validateJson(
  value: unknown,
  context: string,
  path = "$",
  ancestors = new Set<object>(),
): void {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    return;
  }
  if (typeof value === "number" && Number.isFinite(value)) return;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return;

  if (typeof value !== "object") {
    throw new DatabaseException(
      `Failed to bind SQLite ${context} at '${path}': unsupported value type ${describe(value)}`,
    );
  }
  if (ancestors.has(value)) {
    throw new DatabaseException(
      `Failed to bind SQLite ${context} at '${path}': circular reference`,
    );
  }

  if (value instanceof Doc) {
    ancestors.add(value);
    validateJson(value.toObject(), context, path, ancestors);
    ancestors.delete(value);
    return;
  }

  ancestors.add(value);
  if (Array.isArray(value)) {
    value.forEach((item, index) =>
      validateJson(item, context, `${path}[${index}]`, ancestors),
    );
    ancestors.delete(value);
    return;
  }

  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    ancestors.delete(value);
    throw new DatabaseException(
      `Failed to bind SQLite ${context} at '${path}': unsupported value type ${describe(value)}`,
    );
  }
  Object.entries(value).forEach(([key, item]) =>
    validateJson(item, context, `${path}.${key}`, ancestors),
  );
  ancestors.delete(value);
}

function serialize(value: unknown, context: string): string {
  try {
    validateJson(value, context);
    const encoded = JSON.stringify(value);
    if (encoded !== undefined) return encoded;
  } catch (error) {
    if (error instanceof DatabaseException) throw error;
    throw new DatabaseException(
      `Failed to bind SQLite ${context}: value is not JSON serializable`,
      undefined,
      error,
    );
  }

  throw new DatabaseException(
    `Failed to bind SQLite ${context}: value is not JSON serializable`,
  );
}

/** Converts one application value into a value accepted by bun:sqlite. */
function bind(value: unknown): SQLiteBoundValue {
  if (value === null) return null;
  if (value instanceof JsonParam) return serialize(value.value, "JSON value");
  if (value instanceof Date) {
    try {
      return value.toISOString();
    } catch (error) {
      throw new DatabaseException(
        "Failed to bind SQLite Date: invalid date value",
        undefined,
        error,
      );
    }
  }
  if (Array.isArray(value)) return serialize(value, "array value");
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (typeof value === "object") return serialize(value, "JSON value");

  switch (typeof value) {
    case "boolean":
      return value ? 1 : 0;
    case "string":
      return value;
    case "number":
      if (Number.isFinite(value)) return value;
      throw new DatabaseException(
        `Unsupported SQLite bound number: ${String(value)}`,
      );
    case "bigint":
      if (value >= SQLITE_INTEGER_MIN && value <= SQLITE_INTEGER_MAX) {
        return value;
      }
      throw new DatabaseException(
        `Unsupported SQLite bound bigint outside signed 64-bit range: ${value}`,
      );
    default:
      throw new DatabaseException(
        `Unsupported SQLite bound value type: ${describe(value)}`,
      );
  }
}

/** Converts one application value into a value accepted by bun:sqlite. */
export function bindSQLiteValue(value: unknown): SQLiteBoundValue {
  try {
    return bind(value);
  } catch (error) {
    if (error instanceof DatabaseException) throw error;
    throw new DatabaseException(
      `Failed to bind unsupported SQLite value type: ${describe(value)}`,
      undefined,
      error,
    );
  }
}

/** Converts all parameters before a statement is handed to bun:sqlite. */
export function bindSQLiteValues(
  values?: readonly unknown[],
): SQLiteBoundValue[] | undefined {
  return values?.map(bindSQLiteValue);
}

function parse(value: unknown, metadata: SQLiteValueMetadata): unknown {
  if (typeof value !== "string") return value;

  try {
    return JSON.parse(value) as unknown;
  } catch (error) {
    throw new DatabaseException(
      `Failed to decode SQLite value for attribute '${metadata.$id}': invalid JSON`,
      undefined,
      error,
    );
  }
}

function boolean(value: unknown, metadata: SQLiteValueMetadata): boolean {
  if (value === true || value === 1 || value === 1n) return true;
  if (value === false || value === 0 || value === 0n) return false;
  throw new DatabaseException(
    `Failed to decode SQLite value for boolean attribute '${metadata.$id}'`,
  );
}

function date(value: unknown, metadata: SQLiteValueMetadata): Date {
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  if (typeof value !== "string" && typeof value !== "number") {
    throw new DatabaseException(
      `Failed to decode SQLite value for date attribute '${metadata.$id}'`,
    );
  }

  const decoded = new Date(value);
  if (!Number.isNaN(decoded.getTime())) return decoded;
  throw new DatabaseException(
    `Failed to decode SQLite value for date attribute '${metadata.$id}'`,
  );
}

function scalar(value: unknown, metadata: SQLiteValueMetadata): unknown {
  switch (metadata.type) {
    case AttributeEnum.Boolean:
      return boolean(value, metadata);
    case AttributeEnum.Timestamptz:
      return date(value, metadata);
    default:
      return value;
  }
}

/**
 * Restores a SQLite value using schema metadata. JSON-looking strings are
 * parsed only for JSON or array attributes, never by content inspection.
 */
export function decodeSQLiteValue(
  value: unknown,
  metadata?: SQLiteValueMetadata,
): unknown {
  if (value === null || metadata === undefined) return value;
  if (value instanceof JsonParam) return value.value;

  if (metadata.array) {
    const decoded = parse(value, metadata);
    if (!Array.isArray(decoded)) {
      throw new DatabaseException(
        `Failed to decode SQLite array attribute '${metadata.$id}': stored value is not an array`,
      );
    }
    if (metadata.type === AttributeEnum.Json) return decoded;
    return decoded.map((item) =>
      item === null ? null : scalar(item, metadata),
    );
  }

  if (metadata.type === AttributeEnum.Json) return parse(value, metadata);
  return scalar(value, metadata);
}

/** Decodes only row fields that have explicit attribute metadata. */
export function decodeSQLiteRow(
  row: Readonly<Record<string, unknown>>,
  attributes: readonly SQLiteValueMetadata[],
): Record<string, unknown> {
  const metadata = new Map(
    attributes.map((attribute) => [attribute.$id, attribute]),
  );
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [
      key,
      decodeSQLiteValue(value, metadata.get(key)),
    ]),
  );
}
