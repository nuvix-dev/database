import {
  AttributeEnum,
  CursorEnum,
  IndexEnum,
  OrderEnum,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";
import { AuthContext } from "@core/auth.js";
import { Query } from "@core/query.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Attribute } from "@validators/schema.js";

/** Dialect-neutral result returned by adapter query clients. */
export interface QueryResult<T = Record<string, unknown>> {
  rows: T[];
  rowCount: number;
}

/** Common database error metadata used by adapter exception mapping. */
export interface DatabaseError extends Error {
  code?: string;
  severity?: string;
  detail?: string;
  hint?: string;
  constraint?: string;
  table?: string;
  column?: string;
}

export type CreateAttribute = {
  collection: string;
  key: string;
  type: AttributeEnum;
  size?: number;
  array?: boolean;
};

export type UpdateAttribute = CreateAttribute & {
  newName?: string;
};

export type CreateIndex = {
  collection: string;
  name: string;
  type: IndexEnum;
  attributes: string[];
  orders?: (string | null)[];
  attributeTypes: Record<string, Attribute>;
};

export interface ColumnInfo {
  $id: string;
  columnDefault: string | null;
  isNullable: "YES" | "NO";
  dataType: string;
  characterMaximumLength: number | null;
  numericPrecision: number | null;
  numericScale: number | null;
  datetimePrecision: number | null;
  columnType: string;
  columnKey: string;
  extra: string;
}

export interface IncreaseDocumentAttribute {
  ctx: AuthContext;
  collection: string;
  id: string;
  attribute: string;
  value: number;
  updatedAt: Date;
  min?: number;
  max?: number;
}

export interface Find {
  collection: string;
  query?: ((builder: QueryBuilder) => QueryBuilder) | Query[];
  options?: {
    limit?: number;
    offset?: number;
    orderAttributes?: string[];
    orderTypes?: OrderEnum[];
    cursor?: Record<string, string | number>;
    cursorDirection?: CursorEnum;
    permission?: PermissionEnum;
  };
}

export interface CreateRelationship {
  collection: string;
  attribute: string;
  type: RelationEnum;
  twoWay?: boolean;
  target: {
    collection: string;
    attribute?: string;
  };
  junctionCollection?: string;
}

/**
 * Marker wrapper for Json attribute values on the write path.
 *
 * Values must reach the SQL driver as native objects/arrays so they are
 * stored as real jsonb documents. Pre-stringifying caused double-encoding
 * (values stored as jsonb string scalars), which made JSON-path operators
 * (`->`, `->>`) return NULL and silently break filters. The marker also lets
 * `bindValues()` distinguish a jsonb document from a Postgres array literal
 * parameter: wrapped values are passed through untouched, never flattened
 * into `{...}` array-literal strings.
 */
export class JsonParam {
  constructor(public readonly value: unknown) {}
}
