import { DatabaseException } from "@errors/base.js";
import {
  AttributeEnum,
  CursorEnum,
  IndexEnum,
  OrderEnum,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";
import type { AuthContext } from "@core/auth.js";
import type { Doc } from "@core/doc.js";
import type { ProcessedQuery, PopulateQuery } from "@core/database.js";
import { Query, QueryType } from "@core/query.js";
import { OrderException } from "@errors/index.js";
import type { Collection, RelationOptions } from "@validators/schema.js";
import type { QueryBuilder } from "@utils/query-builder.js";
import type { IEntity } from "types.js";
import type { Meta } from "./base.js";
import { INTERNAL_ATTR_KEYS } from "./sql-builder.js";

type QuoteValue = (value: string) => string;
type SqlFragment = { sql: string; params: unknown[] };
type BuildResult = {
  conditions: string[];
  selectionsSql: string[];
  orders: string[];
  params: unknown[];
  joins: string[];
  joinParams: unknown[];
};

const isSystemContext = (ctx: AuthContext): boolean =>
  "system" in ctx && ctx.system === true;

/** Pure SQL generation for SQLite. PostgreSQL generation remains isolated. */
export class SQLiteSqlBuilder {
  static sanitize(value: string): string {
    if (value === null || value === undefined) {
      throw new DatabaseException(
        "Failed to sanitize key: value is null or undefined",
      );
    }

    // A period is valid in public keys. Keep it inside the subsequently
    // quoted SQLite identifier instead of dropping it: dropping accepted
    // characters made distinct keys such as `foo.bar` and `foobar` target
    // the same physical table or column.
    const sanitized = value.replace(/[^A-Za-z0-9_.-]/g, "");
    if (!sanitized) {
      throw new DatabaseException(
        "Failed to sanitize key: filtered value is empty",
      );
    }
    return sanitized;
  }

  static quote(name: string): string {
    if (!name) {
      throw new DatabaseException("Failed to quote name: name is empty");
    }
    return `"${name.replace(/"/g, '""')}"`;
  }

  private static hash(value: string, length = 12): string {
    return new Bun.CryptoHasher("sha1")
      .update(value)
      .digest("hex")
      .slice(0, length);
  }

  private static schemaOf(meta: Partial<Meta>): string {
    if (!meta.schema) {
      throw new DatabaseException(
        "Schema name is not defined in adapter metadata.",
      );
    }
    return meta.schema;
  }

  static getTablePrefix(meta: Partial<Meta>): string {
    const schema = SQLiteSqlBuilder.schemaOf(meta);
    const namespace = meta.namespace ?? "default";
    const readable = `${SQLiteSqlBuilder.sanitize(schema)}_${SQLiteSqlBuilder.sanitize(namespace)}`;
    return `${readable}_${SQLiteSqlBuilder.hash(`${schema}\0${namespace}`)}`;
  }

  static getTableName(meta: Partial<Meta>, name: string): string {
    if (!name) {
      throw new DatabaseException("Failed to get table name: name is empty");
    }
    return `${SQLiteSqlBuilder.getTablePrefix(meta)}_${SQLiteSqlBuilder.sanitize(name)}`;
  }

  static getSQLTable(meta: Partial<Meta>, name: string): string {
    return SQLiteSqlBuilder.quote(SQLiteSqlBuilder.getTableName(meta, name));
  }

  static getIndexName(
    meta: Partial<Meta>,
    table: string,
    name: string,
  ): string {
    const schema = SQLiteSqlBuilder.schemaOf(meta);
    const namespace = meta.namespace ?? "default";
    const identity = `${schema}\0${namespace}\0${table}\0${name}`;
    const readable = [schema, namespace, table, name]
      .map(SQLiteSqlBuilder.sanitize)
      .join("_");
    return `idx_${readable}_${SQLiteSqlBuilder.hash(identity, 20)}`;
  }

  static getSQLIndex(
    meta: Partial<Meta>,
    table: string,
    name: string,
  ): string {
    return SQLiteSqlBuilder.quote(
      SQLiteSqlBuilder.getIndexName(meta, table, name),
    );
  }

  static getSQLType(
    type: AttributeEnum,
    _size?: number,
    array = false,
  ): string {
    if (array || type === AttributeEnum.Json) return "TEXT";

    switch (type) {
      case AttributeEnum.Integer:
      case AttributeEnum.Boolean:
        return "INTEGER";
      case AttributeEnum.Float:
        return "REAL";
      case AttributeEnum.String:
      case AttributeEnum.Timestamptz:
      case AttributeEnum.Relationship:
      case AttributeEnum.Uuid:
        return "TEXT";
      default:
        throw new DatabaseException(`Unsupported SQLite attribute type: ${type}`);
    }
  }

  static getSQLIndexType(type: IndexEnum): string {
    switch (type) {
      case IndexEnum.Key:
        return "INDEX";
      case IndexEnum.Unique:
        return "UNIQUE";
      case IndexEnum.FullText:
        throw new DatabaseException(
          "SQLite fulltext indexes are not supported by this adapter",
        );
      default:
        throw new DatabaseException(`Unsupported SQLite index type: ${type}`);
    }
  }

  static getForUpdateClause(forUpdate = false): string {
    if (forUpdate) {
      throw new DatabaseException(
        "SQLite does not support SELECT FOR UPDATE",
      );
    }
    return "";
  }

  static getInternalKeyForAttribute(attribute: string): string {
    switch (attribute) {
      case "$id":
        return "_uid";
      case "$sequence":
        return "_id";
      case "$collection":
        return "_collection";
      case "$tenant":
        return "_tenant";
      case "$createdAt":
        return "_createdAt";
      case "$updatedAt":
        return "_updatedAt";
      case "$permissions":
        return "_permissions";
      default:
        return attribute;
    }
  }

  static escapeWildcards(value: string): string {
    return value.replace(/[\\%_]/g, (character) => `\\${character}`);
  }

  static getJunctionTable(
    collection: number,
    relatedCollection: number,
    attribute: string,
    relatedAttribute: string,
  ): string {
    return `_${collection}_${relatedCollection}_${attribute}_${relatedAttribute}`;
  }

  static getTenantQuery(
    meta: Partial<Meta>,
    collection: string,
    alias = "",
    tenantCount = 0,
    condition = "AND",
  ): string {
    if (!meta.sharedTables) return "";

    const prefix = alias ? `${SQLiteSqlBuilder.quote(alias)}.` : "";
    const placeholders = Array.from(
      { length: tenantCount || 1 },
      () => "?",
    ).join(",");
    const metadataFallback =
      collection === "_metadata"
        ? ` OR ${prefix}${SQLiteSqlBuilder.quote("_tenant")} IS NULL`
        : "";
    return `${condition} (${prefix}${SQLiteSqlBuilder.quote("_tenant")} IN (${placeholders})${metadataFallback})`;
  }

  static getSQLPermissionsCondition(
    meta: Partial<Meta>,
    {
      collection,
      roles,
      alias,
      type = PermissionEnum.Read,
    }: {
      collection: string;
      roles: readonly string[];
      alias: string;
      type?: PermissionEnum;
    },
    quoteValue: QuoteValue,
  ): string {
    if (!collection || !roles.length || !alias) {
      throw new DatabaseException(
        "Failed to get SQLite permission condition: collection, roles, and alias are required",
      );
    }
    if (!Object.values(PermissionEnum).includes(type)) {
      throw new DatabaseException(`Unknown permission type: ${type}`);
    }

    const roleList = roles.map(quoteValue).join(", ");
    return `EXISTS (
      SELECT 1
      FROM ${SQLiteSqlBuilder.getSQLTable(meta, `${collection}_perms`)} AS ${SQLiteSqlBuilder.quote("p")}
      WHERE ${SQLiteSqlBuilder.quote("p")}.${SQLiteSqlBuilder.quote("_document")} = ${SQLiteSqlBuilder.quote(alias)}.${SQLiteSqlBuilder.quote("_id")}
        AND ${SQLiteSqlBuilder.quote("p")}.${SQLiteSqlBuilder.quote("_type")} = ${quoteValue(type)}
        AND EXISTS (
          SELECT 1 FROM json_each(${SQLiteSqlBuilder.quote("p")}.${SQLiteSqlBuilder.quote("_permissions")}) AS ${SQLiteSqlBuilder.quote("permission")}
          WHERE ${SQLiteSqlBuilder.quote("permission")}.${SQLiteSqlBuilder.quote("value")} IN (${roleList})
        )
        ${SQLiteSqlBuilder.getTenantQuery(meta, collection, "p")}
    )`
      .trim()
      .replace(/\s+/g, " ");
  }

  private static column(attribute: string, tableAlias: string): string {
    const dbKey = SQLiteSqlBuilder.getInternalKeyForAttribute(attribute);
    const parts = dbKey.split(/->>?/);
    const mainColumn = parts.shift();
    if (!mainColumn) {
      throw new DatabaseException("SQLite query attribute is empty");
    }

    const reference = `${SQLiteSqlBuilder.quote(tableAlias)}.${SQLiteSqlBuilder.quote(SQLiteSqlBuilder.sanitize(mainColumn))}`;
    if (!parts.length) return reference;

    const path = parts
      .map((part) => `."${SQLiteSqlBuilder.sanitize(part)}"`)
      .join("");
    return `json_extract(${reference}, '$${path}')`;
  }

  static buildQueryCondition(
    _meta: Partial<Meta>,
    query: Query,
    tableAlias: string,
  ): SqlFragment {
    const method = query.getMethod();
    const values = query.getValues();
    if (method === QueryType.Select || method === QueryType.Populate) {
      return { sql: "", params: [] };
    }
    if (method === QueryType.Search || method === QueryType.NotSearch) {
      throw new DatabaseException(
        "SQLite fulltext search is not supported by this adapter",
      );
    }

    if (method === QueryType.And || method === QueryType.Or) {
      const fragments = (values as Query[])
        .map((nested) =>
          SQLiteSqlBuilder.buildQueryCondition(_meta, nested, tableAlias),
        )
        .filter((fragment) => fragment.sql);
      if (!fragments.length) return { sql: "", params: [] };
      return {
        sql: `(${fragments.map((fragment) => fragment.sql).join(` ${method.toUpperCase()} `)})`,
        params: fragments.flatMap((fragment) => fragment.params),
      };
    }

    if (method === QueryType.Not) {
      const nested = values[0];
      if (!(nested instanceof Query)) {
        throw new DatabaseException("SQLite NOT query requires a nested query");
      }
      const fragment = SQLiteSqlBuilder.buildQueryCondition(
        _meta,
        nested,
        tableAlias,
      );
      return { sql: `NOT (${fragment.sql})`, params: fragment.params };
    }

    const column = SQLiteSqlBuilder.column(query.getAttribute(), tableAlias);
    let fragment: SqlFragment;
    switch (method) {
      case QueryType.Equal:
      case QueryType.NotEqual: {
        if (!values.length) {
          return { sql: method === QueryType.Equal ? "0" : "1", params: [] };
        }
        const negated = method === QueryType.NotEqual;
        fragment = {
          sql:
            values.length === 1
              ? `${column} ${negated ? "!=" : "="} ?`
              : `${column} ${negated ? "NOT IN" : "IN"} (${values.map(() => "?").join(", ")})`,
          params: [...values],
        };
        break;
      }
      case QueryType.LessThan:
      case QueryType.LessThanEqual:
      case QueryType.GreaterThan:
      case QueryType.GreaterThanEqual: {
        const operators: Partial<Record<QueryType, string>> = {
          [QueryType.LessThan]: "<",
          [QueryType.LessThanEqual]: "<=",
          [QueryType.GreaterThan]: ">",
          [QueryType.GreaterThanEqual]: ">=",
        };
        fragment = { sql: `${column} ${operators[method]} ?`, params: [values[0]] };
        break;
      }
      case QueryType.Contains:
      case QueryType.NotContains:
        if (query.onArray()) {
          throw new DatabaseException(
            "SQLite GIN-style array overlap queries are not supported by this adapter",
          );
        }
        fragment = {
          sql: `${column} LIKE ? ESCAPE '\\'`,
          params: [
            `%${SQLiteSqlBuilder.escapeWildcards(String(values[0] ?? ""))}%`,
          ],
        };
        break;
      case QueryType.StartsWith:
      case QueryType.NotStartsWith:
        fragment = {
          sql: `${column} LIKE ? ESCAPE '\\'`,
          params: [
            `${SQLiteSqlBuilder.escapeWildcards(String(values[0] ?? ""))}%`,
          ],
        };
        break;
      case QueryType.EndsWith:
      case QueryType.NotEndsWith:
        fragment = {
          sql: `${column} LIKE ? ESCAPE '\\'`,
          params: [
            `%${SQLiteSqlBuilder.escapeWildcards(String(values[0] ?? ""))}`,
          ],
        };
        break;
      case QueryType.IsNull:
        return { sql: `${column} IS NULL`, params: [] };
      case QueryType.IsNotNull:
        return { sql: `${column} IS NOT NULL`, params: [] };
      case QueryType.Between:
      case QueryType.NotBetween:
        fragment = {
          sql: `${column} BETWEEN ? AND ?`,
          params: [values[0], values[1]],
        };
        break;
      default:
        return { sql: "", params: [] };
    }

    if (
      method === QueryType.NotContains ||
      method === QueryType.NotStartsWith ||
      method === QueryType.NotEndsWith ||
      method === QueryType.NotBetween
    ) {
      return { sql: `NOT (${fragment.sql})`, params: fragment.params };
    }
    return fragment;
  }

  static buildWhereConditions(
    meta: Partial<Meta>,
    queries: Query[],
    tableAlias: string,
    _collection: string,
  ): { conditions: string[]; params: unknown[] } {
    const fragments = queries
      .map((query) =>
        SQLiteSqlBuilder.buildQueryCondition(meta, query, tableAlias),
      )
      .filter((fragment) => fragment.sql);
    return {
      conditions: fragments.map((fragment) => fragment.sql),
      params: fragments.flatMap((fragment) => fragment.params),
    };
  }

  static getSQLCondition(
    meta: Partial<Meta>,
    query: Query,
    binds: unknown[],
    _supportForJSONOverlaps = true,
  ): string {
    const fragment = SQLiteSqlBuilder.buildQueryCondition(
      meta,
      query,
      Query.DEFAULT_ALIAS,
    );
    binds.push(...fragment.params);
    return fragment.sql;
  }

  static getSQLConditions(
    meta: Partial<Meta>,
    queries: Query[],
    binds: unknown[],
    separator = "AND",
    supportForJSONOverlaps = true,
  ): string {
    const conditions = queries
      .filter((query) => query.getMethod() !== QueryType.Select)
      .map((query) =>
        SQLiteSqlBuilder.getSQLCondition(
          meta,
          query,
          binds,
          supportForJSONOverlaps,
        ),
      )
      .filter(Boolean);
    return conditions.length ? `(${conditions.join(` ${separator} `)})` : "";
  }

  static getAttributeSelections(
    queries: QueryBuilder | Query[],
  ): string[] {
    const queryList = Array.isArray(queries) ? queries : queries.build();
    return queryList
      .filter((query) => query.getMethod() === QueryType.Select)
      .flatMap((query) => query.getValues() as string[]);
  }

  static getAttributeProjection(
    meta: Partial<Meta>,
    selections: readonly string[],
    prefix: string,
    collection: string,
  ): string {
    if (!selections.length) {
      throw new DatabaseException("Selections are required internally.");
    }
    const all = [
      "$id",
      "$sequence",
      "$schema",
      "$collection",
      "$createdAt",
      "$updatedAt",
      "$permissions",
      ...selections,
    ];
    return [...new Set(all)]
      .map((key) => {
        if (key === "$schema") {
          return `'${SQLiteSqlBuilder.schemaOf(meta).replace(/'/g, "''")}' AS ${SQLiteSqlBuilder.quote(key)}`;
        }
        if (key === "$collection") {
          return `'${collection.replace(/'/g, "''")}' AS ${SQLiteSqlBuilder.quote(key)}`;
        }
        const dbKey = SQLiteSqlBuilder.getInternalKeyForAttribute(key);
        return `${SQLiteSqlBuilder.quote(prefix)}.${SQLiteSqlBuilder.quote(SQLiteSqlBuilder.sanitize(dbKey))} AS ${SQLiteSqlBuilder.quote(key)}`;
      })
      .join(", ");
  }

  static buildSelections(
    meta: Partial<Meta>,
    selections: string[],
    tableAlias: string,
    collection: Doc<Collection>,
  ): string[] {
    const fields = [
      "$id",
      "$sequence",
      "$createdAt",
      "$updatedAt",
      "$permissions",
      ...selections,
    ];
    const result = [...new Set(fields)].map((field) => {
      const key = SQLiteSqlBuilder.getInternalKeyForAttribute(field);
      return `${SQLiteSqlBuilder.quote(tableAlias)}.${SQLiteSqlBuilder.quote(SQLiteSqlBuilder.sanitize(key))} AS ${SQLiteSqlBuilder.quote(field)}`;
    });
    result.push(
      `'${SQLiteSqlBuilder.schemaOf(meta).replace(/'/g, "''")}' AS ${SQLiteSqlBuilder.quote("$schema")}`,
      `'${collection.getId().replace(/'/g, "''")}' AS ${SQLiteSqlBuilder.quote("$collection")}`,
    );
    if (meta.sharedTables) {
      result.push(
        `${SQLiteSqlBuilder.quote(tableAlias)}.${SQLiteSqlBuilder.quote("_tenant")} AS ${SQLiteSqlBuilder.quote("$tenant")}`,
      );
    }
    return result;
  }

  private static normalizeOrders(
    orders: Readonly<Record<string, OrderEnum>>,
  ): Array<[string, OrderEnum]> {
    const entries = Object.entries(orders) as Array<[string, OrderEnum]>;
    if (!entries.some(([attribute]) =>
      attribute === "$id" || attribute === "$sequence",
    )) {
      entries.push(["$sequence", OrderEnum.Asc]);
    }
    return entries;
  }

  static buildOrderClause(
    orders: Readonly<Record<string, OrderEnum>>,
    tableAlias: string,
  ): string[] {
    return SQLiteSqlBuilder.normalizeOrders(orders).map(([attribute, order]) =>
      `${SQLiteSqlBuilder.column(attribute, tableAlias)} ${order}`,
    );
  }

  static buildCursorConditions(
    cursor: Doc<IEntity> | null = null,
    cursorDirection: CursorEnum | null,
    orders: Readonly<Record<string, OrderEnum>>,
    tableAlias: string,
  ): { condition: string; params: unknown[] } {
    if (!cursor) return { condition: "", params: [] };
    const normalized = SQLiteSqlBuilder.normalizeOrders(orders);
    normalized.forEach(([attribute]) => {
      if (cursor.get(attribute, null) === null) {
        throw new OrderException(
          `Order attribute '${attribute}' is empty`,
          attribute,
        );
      }
    });

    const after = (cursorDirection ?? CursorEnum.After) === CursorEnum.After;
    const params: unknown[] = [];
    const branches = normalized.map(([attribute, order], index) => {
      const equalities = normalized.slice(0, index).map(([previous]) => {
        params.push(cursor.get(previous));
        return `${SQLiteSqlBuilder.column(previous, tableAlias)} = ?`;
      });
      const ascending = order === OrderEnum.Asc;
      const operator = after === ascending ? ">" : "<";
      params.push(cursor.get(attribute));
      return `(${[
        ...equalities,
        `${SQLiteSqlBuilder.column(attribute, tableAlias)} ${operator} ?`,
      ].join(" AND ")})`;
    });
    return { condition: `(${branches.join(" OR ")})`, params };
  }

  static buildJoinCondition(
    meta: Partial<Meta>,
    relationType: RelationEnum,
    parentAlias: string,
    relationAlias: string,
    relationshipKey: string,
    twoWayKey = "",
    side: RelationSideEnum,
    junctionCollection: string,
  ): string | null {
    const parentUid = SQLiteSqlBuilder.column("$id", parentAlias);
    const relationUid = SQLiteSqlBuilder.column("$id", relationAlias);
    const parentRelation = SQLiteSqlBuilder.column(relationshipKey, parentAlias);
    const relatedRelation = SQLiteSqlBuilder.column(twoWayKey, relationAlias);

    switch (relationType) {
      case RelationEnum.OneToOne:
        return side === RelationSideEnum.Parent
          ? `${parentRelation} = ${relationUid}`
          : `${parentUid} = ${relatedRelation}`;
      case RelationEnum.OneToMany:
        return side === RelationSideEnum.Parent
          ? `${parentUid} = ${relatedRelation}`
          : `${parentRelation} = ${relationUid}`;
      case RelationEnum.ManyToOne:
        return side === RelationSideEnum.Child
          ? `${parentUid} = ${relatedRelation}`
          : `${parentRelation} = ${relationUid}`;
      case RelationEnum.ManyToMany: {
        if (!junctionCollection || !twoWayKey) {
          throw new DatabaseException(
            "Junction collection and two-way key are required for a many-to-many relation",
          );
        }
        const junction = SQLiteSqlBuilder.getSQLTable(meta, junctionCollection);
        const parentKey = SQLiteSqlBuilder.quote(
          SQLiteSqlBuilder.sanitize(relationshipKey),
        );
        const relatedKey = SQLiteSqlBuilder.quote(
          SQLiteSqlBuilder.sanitize(twoWayKey),
        );
        return `EXISTS (SELECT 1 FROM ${junction} AS ${SQLiteSqlBuilder.quote("jt")} WHERE ${SQLiteSqlBuilder.quote("jt")}.${parentKey} = ${parentUid} AND ${SQLiteSqlBuilder.quote("jt")}.${relatedKey} = ${relationUid} ${SQLiteSqlBuilder.getTenantQuery(meta, junctionCollection, "jt")})`;
      }
      default:
        return null;
    }
  }

  static handleConditions(
    meta: Partial<Meta>,
    {
      populateQueries = [],
      tableAlias = "main",
      depth = 0,
      forPermission,
      ctx,
      ...query
    }: (ProcessedQuery | PopulateQuery) & {
      tableAlias?: string;
      depth: number;
      forPermission: PermissionEnum;
      ctx: AuthContext;
    },
    quoteValue: QuoteValue,
  ): BuildResult {
    const { collection, filters = [], selections = [], orders, skipAuth } = query;
    const where = SQLiteSqlBuilder.buildWhereConditions(
      meta,
      filters,
      tableAlias,
      collection.getId(),
    );
    const result: BuildResult = {
      conditions: [...where.conditions],
      selectionsSql: SQLiteSqlBuilder.buildSelections(
        meta,
        selections,
        tableAlias,
        collection,
      ),
      orders: SQLiteSqlBuilder.buildOrderClause(orders, tableAlias),
      params: [...where.params],
      joins: [],
      joinParams: [],
    };

    if (
      tableAlias === "main" &&
      !isSystemContext(ctx) &&
      !skipAuth &&
      collection.get("documentSecurity", false)
    ) {
      result.conditions.push(
        SQLiteSqlBuilder.getSQLPermissionsCondition(
          meta,
          {
            collection: collection.getId(),
            roles: ctx.roles,
            alias: tableAlias,
            type: forPermission,
          },
          quoteValue,
        ),
      );
      if (meta.sharedTables) result.params.push(meta.tenantId);
    }
    if (meta.sharedTables && tableAlias === "main") {
      result.conditions.push(
        SQLiteSqlBuilder.getTenantQuery(
          meta,
          collection.getId(),
          tableAlias,
          0,
          "",
        ),
      );
      result.params.push(meta.tenantId);
    }

    populateQueries.forEach((populate, index) => {
      if (!populate.authorized) return;
      const relationship = collection
        .get("attributes", [])
        .find(
          (attribute) =>
            attribute.get("type") === AttributeEnum.Relationship &&
            attribute.get("key", attribute.getId()) === populate.attribute,
        );
      if (!relationship) return;

      const relationAlias = `rel_${depth}_${index}`;
      const options = relationship.get("options", {}) as RelationOptions;
      const relationshipKey = relationship.get("key", relationship.getId());
      const relatedName = SQLiteSqlBuilder.sanitize(options.relatedCollection);
      let junction = "";
      if (options.relationType === RelationEnum.ManyToMany) {
        const parent = options.side === RelationSideEnum.Parent;
        junction = SQLiteSqlBuilder.getJunctionTable(
          parent ? collection.getSequence() : populate.collection.getSequence(),
          parent ? populate.collection.getSequence() : collection.getSequence(),
          parent ? relationship.getId() : options.twoWayKey!,
          parent ? options.twoWayKey! : relationship.getId(),
        );
      }

      const joinCondition = SQLiteSqlBuilder.buildJoinCondition(
        meta,
        options.relationType,
        tableAlias,
        relationAlias,
        relationshipKey,
        options.twoWayKey,
        options.side,
        junction,
      );
      if (!joinCondition) return;

      const joinParts = [
        `LEFT JOIN ${SQLiteSqlBuilder.getSQLTable(meta, relatedName)} AS ${SQLiteSqlBuilder.quote(relationAlias)} ON ${joinCondition}`,
      ];
      if (meta.sharedTables && options.relationType === RelationEnum.ManyToMany) {
        result.joinParams.push(meta.tenantId);
      }
      if (
        !isSystemContext(ctx) &&
        !populate.skipAuth &&
        populate.collection.get("documentSecurity", false)
      ) {
        joinParts.push(
          `AND ${SQLiteSqlBuilder.getSQLPermissionsCondition(
            meta,
            {
              collection: relatedName,
              roles: ctx.roles,
              alias: relationAlias,
              type: forPermission,
            },
            quoteValue,
          )}`,
        );
        if (meta.sharedTables) result.joinParams.push(meta.tenantId);
      }
      if (meta.sharedTables) {
        joinParts.push(
          SQLiteSqlBuilder.getTenantQuery(meta, relatedName, relationAlias),
        );
        result.joinParams.push(meta.tenantId);
      }
      result.joins.push(joinParts.join(" "));

      const nested = SQLiteSqlBuilder.handleConditions(
        meta,
        {
          ...populate,
          tableAlias: relationAlias,
          depth: depth + 1,
          forPermission,
          ctx,
        },
        quoteValue,
      );
      result.selectionsSql.push(
        ...nested.selectionsSql.map((selection) => {
          const [expression, alias] = selection.split(" AS ");
          return expression && alias
            ? `${expression} AS ${SQLiteSqlBuilder.quote(`${relationshipKey}_${alias.replace(/"/g, "")}`)}`
            : selection;
        }),
      );
      result.joins.push(...nested.joins);
      result.joinParams.push(...nested.joinParams);
      result.conditions.push(...nested.conditions);
      result.params.push(...nested.params);
      result.orders.push(...nested.orders);
    });

    return result;
  }

  static buildSql(
    meta: Partial<Meta>,
    query: ProcessedQuery,
    options: {
      forPermission: PermissionEnum;
      ctx: AuthContext;
      forUpdate?: boolean;
    },
    quoteValue: QuoteValue,
  ): {
    sql: string;
    params: unknown[];
    joins: string[];
    selections: string[];
  } {
    SQLiteSqlBuilder.getForUpdateClause(options.forUpdate);
    const mainAlias = "main";
    const orders = Object.fromEntries(
      SQLiteSqlBuilder.normalizeOrders(query.orders),
    ) as Record<string, OrderEnum>;
    const result = SQLiteSqlBuilder.handleConditions(
      meta,
      { ...query, orders, tableAlias: mainAlias, depth: 0, ...options },
      quoteValue,
    );
    const cursor = SQLiteSqlBuilder.buildCursorConditions(
      query.cursor,
      query.cursorDirection,
      orders,
      mainAlias,
    );
    if (cursor.condition) {
      result.conditions.push(cursor.condition);
      result.params.push(...cursor.params);
    }

    const params = [...result.joinParams, ...result.params];
    const limit = query.limit ? "LIMIT ?" : query.offset ? "LIMIT -1" : "";
    const offset = query.offset ? "OFFSET ?" : "";
    if (query.limit) params.push(query.limit);
    if (query.offset) params.push(query.offset);
    const where = result.conditions.length
      ? `WHERE ${result.conditions.join(" AND ")}`
      : "";
    const sql = `
      SELECT DISTINCT ${result.selectionsSql.join(", ")}
      FROM ${SQLiteSqlBuilder.getSQLTable(meta, query.collection.getId())} AS ${SQLiteSqlBuilder.quote(mainAlias)}
      ${result.joins.join(" ")}
      ${where}
      ORDER BY ${result.orders.join(", ")}
      ${limit}
      ${offset}
    `
      .trim()
      .replace(/\s+/g, " ");
    return {
      sql,
      params,
      joins: result.joins,
      selections: result.selectionsSql,
    };
  }

  static buildAggregateSql(
    meta: Partial<Meta>,
    aggregate: "count" | "sum",
    query: ProcessedQuery,
    options: { forPermission: PermissionEnum; ctx: AuthContext },
    quoteValue: QuoteValue,
    attribute?: string,
  ): { sql: string; params: unknown[] } {
    if (aggregate === "sum" && !attribute) {
      throw new DatabaseException("SQLite SUM requires an attribute");
    }
    const projection = aggregate === "sum" ? [attribute!] : ["$sequence"];
    const built = SQLiteSqlBuilder.buildSql(
      meta,
      { ...query, selections: projection },
      options,
      quoteValue,
    );
    const expression =
      aggregate === "count"
        ? "COUNT(1)"
        : `SUM(${SQLiteSqlBuilder.quote(attribute!)})`;
    return {
      sql: `SELECT ${expression} AS ${SQLiteSqlBuilder.quote("sum")} FROM (${built.sql}) AS ${SQLiteSqlBuilder.quote("aggregate_rows")}`,
      params: built.params,
    };
  }

  static getUpsertStatement(
    meta: Partial<Meta>,
    tableName: string,
    columns: string,
    batchKeys: string[],
    attributes: Readonly<Record<string, unknown>>,
    attribute = "",
    internalAttrs: readonly string[] = INTERNAL_ATTR_KEYS,
  ): string {
    const table = SQLiteSqlBuilder.getSQLTable(meta, tableName);
    const sharedTables = !!meta.sharedTables;
    const update = (name: string, increment = false): string => {
      const column = SQLiteSqlBuilder.quote(
        SQLiteSqlBuilder.sanitize(name),
      );
      const excluded = `${SQLiteSqlBuilder.quote("excluded")}.${column}`;
      const value = increment ? `${table}.${column} + ${excluded}` : excluded;
      if (!sharedTables) return `${column} = ${value}`;
      return `${column} = CASE WHEN ${table}.${SQLiteSqlBuilder.quote("_tenant")} = ${SQLiteSqlBuilder.quote("excluded")}.${SQLiteSqlBuilder.quote("_tenant")} THEN ${value} ELSE ${column} END`;
    };

    const updates = attribute
      ? [update(attribute, true), update("_updatedAt")]
      : Object.keys(attributes)
          .filter((name) => !internalAttrs.includes(name))
          .map((name) => update(name));
    if (!updates.length) {
      throw new DatabaseException(
        "SQLite upsert requires at least one update column",
      );
    }
    return `
      INSERT INTO ${table} ${columns}
      VALUES ${batchKeys.join(", ")}
      ON CONFLICT (${SQLiteSqlBuilder.quote("_uid")}${sharedTables ? `, ${SQLiteSqlBuilder.quote("_tenant")}` : ""}) DO UPDATE SET
        ${updates.join(", ")}
    `;
  }
}
