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
import { Database, ProcessedQuery, PopulateQuery } from "@core/database.js";
import { Query, QueryType } from "@core/query.js";
import { OrderException } from "@errors/index.js";
import type { Doc } from "@core/doc.js";
import type { AuthContext } from "@core/auth.js";
import type { Collection, RelationOptions } from "@validators/schema.js";
import type { QueryBuilder } from "@utils/query-builder.js";
import type { Meta } from "./base.js";

/**
 * Local mirror of core/auth's private isSystemContext predicate (same rationale
 * as the copy in base.ts: keeps this module's diff surface inside the adapter
 * layer).
 */
const isSystemContext = (ctx: AuthContext): boolean =>
  "system" in ctx && ctx.system === true;

/**
 * Attribute keys that map to internal document columns. Shared with
 * BaseAdapter (`$internalAttrs`) so both sides read a single source of truth.
 */
export const INTERNAL_ATTR_KEYS: string[] = [
  "$id",
  "$sequence",
  "$collection",
  "$tenant",
  "$createdAt",
  "$updatedAt",
  "$permissions",
];

/**
 * Driver-side value quoting strategy (e.g. `client.quote`). Injected instead
 * of holding a client reference so every builder below stays pure.
 */
type QuoteValue = (value: string) => string;

/**
 * Pure SQL text builders extracted from BaseAdapter.
 *
 * Contract:
 * - Stateless: no instance fields, no mutation of adapter state.
 * - The adapter's `Meta` is an explicit first argument wherever schema,
 *   namespace, sharedTables or tenantId influence the output — so results
 *   always reflect the CURRENT meta, never a stale snapshot.
 * - Generated SQL is byte-identical to the pre-extraction implementations.
 */
export class SqlBuilder {
  /**
   * Generates an upsert (insert or update) SQL statement for batch operations.
   * If `attribute` is provided, it will increment that column on duplicate key.
   */
  static getUpsertStatement(
    meta: Partial<Meta>,
    tableName: string,
    columns: string,
    batchKeys: string[],
    attributes: Record<string, any>,
    attribute: string = "",
    internalAttrs: readonly string[] = INTERNAL_ATTR_KEYS,
  ): string {
    const sharedTables = !!meta.sharedTables;
    const getUpdateClause = (attribute: string, increment = false): string => {
      const quotedAttr = SqlBuilder.quote(SqlBuilder.sanitize(attribute));
      let newValue: string;
      if (increment) {
        newValue = `${SqlBuilder.getSQLTable(meta, tableName)}.${quotedAttr} + EXCLUDED.${quotedAttr}`;
      } else {
        newValue = `EXCLUDED.${quotedAttr}`;
      }
      if (sharedTables) {
        return `${quotedAttr} = CASE WHEN ${SqlBuilder.getSQLTable(meta, tableName)}._tenant = EXCLUDED._tenant THEN ${newValue} ELSE ${quotedAttr} END`;
      }
      return `${quotedAttr} = ${newValue}`;
    };

    let updateColumns: string[];
    if (attribute) {
      // Increment specific column by its new value in place
      updateColumns = [
        getUpdateClause(attribute, true),
        getUpdateClause("_updatedAt"),
      ];
    } else {
      // Update all columns
      updateColumns = Object.keys(attributes)
        .filter((a) => !internalAttrs.includes(a))
        .map((attr) => getUpdateClause(attr));
    }

    const sql = `
      INSERT INTO ${SqlBuilder.getSQLTable(meta, tableName)} ${columns}
      VALUES ${batchKeys.join(", ")}
      ON CONFLICT (_uid${sharedTables ? ", _tenant" : ""}) DO UPDATE SET
          ${updateColumns.join(", ")}
    `;

    return sql;
  }

  static getSQLType(
    type: AttributeEnum,
    size?: number,
    array?: boolean,
  ): string {
    let pgType: string;
    size ??= 0;

    switch (type) {
      case AttributeEnum.String:
        if (size > 255) {
          pgType = "TEXT";
        } else {
          pgType = `VARCHAR(${size})`;
        }
        break;
      case AttributeEnum.Integer:
        if (size <= 2) {
          // Roughly fits SMALLINT (-32768 to +32767)
          pgType = "SMALLINT";
        } else if (size <= 4) {
          // Roughly fits INTEGER (-2147483648 to +2147483647)
          pgType = "INTEGER";
        } else {
          // For larger integers, BIGINT is appropriate
          pgType = "BIGINT";
        }
        break;
      case AttributeEnum.Float:
        pgType = "DOUBLE PRECISION";
        break;
      case AttributeEnum.Boolean:
        pgType = "BOOLEAN";
        break;
      case AttributeEnum.Timestamptz:
        pgType = "TIMESTAMP WITH TIME ZONE";
        break;
      case AttributeEnum.Relationship:
        pgType = "VARCHAR(255)";
        break;
      case AttributeEnum.Json:
        pgType = "JSONB";
        break;
      // case AttributeEnum.Virtual:
      //   pgType = "";
      //   break;
      case AttributeEnum.Uuid:
        pgType = "UUID";
        break;
      default:
        throw new DatabaseException(`Unsupported attribute type: ${type}`);
    }

    if (array && pgType) {
      return `${pgType}[]`;
    } else {
      return pgType;
    }
  }

  /**
   * @deprecated use getSQLIndex
   */
  static getIndexName(coll: string, id: string): string {
    return `${SqlBuilder.sanitize(coll)}_${SqlBuilder.sanitize(id)}`;
  }

  /**@deprecated */
  static getSQLCondition(
    meta: Partial<Meta>,
    query: Query,
    binds: any[],
    supportForJSONOverlaps: boolean,
  ): string {
    query.setAttribute(
      SqlBuilder.getInternalKeyForAttribute(query.getAttribute()),
    );

    const attribute = SqlBuilder.quote(
      SqlBuilder.sanitize(query.getAttribute()),
    );
    const alias = SqlBuilder.quote(Query.DEFAULT_ALIAS);
    const method = query.getMethod();

    switch (method) {
      case QueryType.Or:
      case QueryType.And:
        const conditions: string[] = [];
        for (const q of query.getValues() as Query[]) {
          conditions.push(SqlBuilder.getSQLCondition(meta, q, binds, supportForJSONOverlaps));
        }

        const methodStr = method.toUpperCase();
        return conditions.length === 0
          ? ""
          : ` ${methodStr} (` + conditions.join(" AND ") + ")";

      case QueryType.Search:
        binds.push(SqlBuilder.getFulltextValue(query.getValue() as string));
        return `to_tsvector('${Database.FULLTEXT_LANGUAGE}', ${alias}.${attribute}) @@ plainto_tsquery('${Database.FULLTEXT_LANGUAGE}', ?)`;

      case QueryType.Between:
        const values = query.getValues();
        binds.push(values[0], values[1]);
        return `${alias}.${attribute} BETWEEN ? AND ?`;

      case QueryType.IsNull:
      case QueryType.IsNotNull:
        return `${alias}.${attribute} ${SqlBuilder.getSQLOperator(method)}`;

      // @ts-ignore
      case QueryType.Contains:
        if (supportForJSONOverlaps && query.onArray()) {
          binds.push(JSON.stringify(query.getValues()));
          return `${alias}.${attribute} @> ?::jsonb`;
        }
      // Fall through to default case

      default:
        const defaultConditions: string[] = [];
        for (const value of query.getValues() as string[]) {
          let processedValue = value;
          switch (method) {
            case QueryType.StartsWith:
              processedValue = SqlBuilder.escapeWildcards(value) + "%";
              break;
            case QueryType.EndsWith:
              processedValue = "%" + SqlBuilder.escapeWildcards(value);
              break;
            case QueryType.Contains:
              processedValue = query.onArray()
                ? JSON.stringify(value)
                : "%" + SqlBuilder.escapeWildcards(value) + "%";
              break;
          }

          binds.push(processedValue);
          defaultConditions.push(
            `${alias}.${attribute} ${SqlBuilder.getSQLOperator(method)} ?`,
          );
        }

        return defaultConditions.length === 0
          ? ""
          : "(" + defaultConditions.join(" OR ") + ")";
    }
  }

  /**@deprecated */
  static getSQLOperator(method: string): string {
    switch (method) {
      case QueryType.Equal:
        return "=";
      case QueryType.NotEqual:
        return "!=";
      case QueryType.LessThan:
        return "<";
      case QueryType.LessThanEqual:
        return "<=";
      case QueryType.GreaterThan:
        return ">";
      case QueryType.GreaterThanEqual:
        return ">=";
      case QueryType.IsNull:
        return "IS NULL";
      case QueryType.IsNotNull:
        return "IS NOT NULL";
      case QueryType.StartsWith:
      case QueryType.EndsWith:
      case QueryType.Contains:
        return "LIKE";
      default:
        throw new DatabaseException("Unknown method: " + method);
    }
  }

  static getSQLTable(meta: Partial<Meta>, name: string): string {
    if (!name) {
      throw new DatabaseException("Failed to get SQL table: name is empty");
    }
    return `${SqlBuilder.quote(SqlBuilder.schemaOf(meta))}.${SqlBuilder.quote(SqlBuilder.getTableName(meta, name))}`;
  }

  static getTableName(meta: Partial<Meta>, name: string): string {
    if (!name) {
      throw new DatabaseException("Failed to get table name: name is empty");
    }
    return `${meta.namespace}_${name}`;
  }

  static getSQLIndex(meta: Partial<Meta>, table: string, name: string): string {
    const base = `${SqlBuilder.schemaOf(meta)}_${meta.namespace}_${table}_${name}`;
    // Native CryptoHasher — no Node crypto allocation on the hot path.
    const safeId = new Bun.CryptoHasher("sha1")
      .update(base)
      .digest("hex")
      .slice(0, 40);
    return SqlBuilder.quote(`${safeId}`);
  }

  static getSQLIndexType(type: IndexEnum): string {
    switch (type) {
      case IndexEnum.Unique:
        return "UNIQUE";
      case IndexEnum.FullText:
        return "FULLTEXT";
      case IndexEnum.Key:
        return "INDEX";
      default:
        throw new DatabaseException(`Unsupported index type: ${type}`);
    }
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
    if (!collection || !roles?.length || !alias) {
      throw new DatabaseException(
        "Failed to get SQL permission condition: collection, roles, and alias are required",
      );
    }

    if (type && !Object.values(PermissionEnum).includes(type)) {
      throw new DatabaseException(`Unknown permission type: ${type}`);
    }

    const quotedRolesArray = `ARRAY[${roles.map((role) => quoteValue(role)).join(", ")}]::text[]`;

    return `EXISTS (
            SELECT 1
            FROM ${SqlBuilder.getSQLTable(meta, `${collection}_perms`)} p
            WHERE p.${SqlBuilder.quote("_document")} = ${SqlBuilder.quote(alias)}.${SqlBuilder.quote("_id")}
              AND p.${SqlBuilder.quote("_type")} = ${quoteValue(type)}
              AND p.${SqlBuilder.quote("_permissions")} && ${quotedRolesArray}
              ${SqlBuilder.getTenantQuery(meta, collection, "p")}
        )`.trim();
  }

  /**
   * @deprecated
   * Builds SQL conditions recursively and mutates the provided `binds` array with bound values.
   * @returns SQL condition string with placeholders.
   */
  static getSQLConditions(
    meta: Partial<Meta>,
    queries: Query[],
    binds: any[],
    separator: string = "AND",
    supportForJSONOverlaps: boolean = true,
  ): string {
    const conditions: string[] = [];

    for (const query of queries) {
      if (query.getMethod() === QueryType.Select) {
        continue;
      }

      if (query.isNested()) {
        conditions.push(
          SqlBuilder.getSQLConditions(
            meta,
            query.getValues() as Query[],
            binds,
            query.getMethod(),
            supportForJSONOverlaps,
          ),
        );
      } else {
        conditions.push(
          SqlBuilder.getSQLCondition(meta, query, binds, supportForJSONOverlaps),
        );
      }
    }

    const tmp = conditions.join(` ${separator} `);
    return tmp === "" ? "" : `(${tmp})`;
  }

  static getTenantQuery(
    meta: Partial<Meta>,
    collection: string,
    alias: string = "",
    tenantCount: number = 0,
    condition: string = "AND",
  ): string {
    if (!meta.sharedTables) {
      return "";
    }

    let dot = "";
    let quotedAlias = alias;

    if (alias !== "") {
      dot = ".";
      quotedAlias = SqlBuilder.quote(alias);
    }

    let bindings: string[] = [];
    if (tenantCount === 0) {
      bindings.push("?");
    } else {
      bindings = Array.from({ length: tenantCount }, (_) => `?`);
    }
    const bindingsStr = bindings.join(",");

    let orIsNull = "";
    if (collection === Database.METADATA) {
      orIsNull = ` OR ${quotedAlias}${dot}${SqlBuilder.quote("_tenant")} IS NULL`;
    }

    return `${condition} (${quotedAlias}${dot}${SqlBuilder.quote("_tenant")} IN (${bindingsStr})${orIsNull})`;
  }

  /**
   * Generates a projection string for attributes in a SQL SELECT query.
   *
   * Note: mirrors the original behavior of prepending the internal selection
   * keys to the CALLER'S array (input mutation is intentional and preserved).
   */
  static getAttributeProjection(
    meta: Partial<Meta>,
    selections: string[],
    prefix: string,
    collection: string,
  ): string {
    if (!selections.length)
      throw new DatabaseException("Selections are required internally.");

    const projected: string[] = [];
    selections.unshift(
      "$id",
      "$sequence",
      "$schema",
      "$collection",
      "$createdAt",
      "$updatedAt",
      "$permissions",
    );

    for (let key of selections) {
      switch (key) {
        case "$schema":
          projected.push(`'${SqlBuilder.schemaOf(meta)}' AS ${SqlBuilder.quote(key)}`);
          break;
        case "$collection":
          projected.push(`'${collection}' AS ${SqlBuilder.quote(key)}`);
          break;
        default:
          const dbKey = SqlBuilder.getInternalKeyForAttribute(key);
          projected.push(
            `${SqlBuilder.quote(prefix)}.${SqlBuilder.quote(dbKey)} AS ${SqlBuilder.quote(key)}`,
          );
      }
    }

    return projected.join(", ");
  }

  static quote(name: string): string {
    if (!name) {
      throw new DatabaseException("Failed to quote name: name is empty");
    }
    // Escape embedded double quotes so quoting is injection-safe by
    // construction; sanitized identifiers ([A-Za-z0-9_-]) are unaffected.
    return `"${name.replace(/"/g, '""')}"`;
  }

  /**@deprecated */
  static getAttributeSelections(
    queries: QueryBuilder | Array<Query>,
  ): string[] {
    const selections: string[] = [];
    queries = Array.isArray(queries) ? queries : queries.build();

    for (const query of queries) {
      if (query.getMethod() === QueryType.Select) {
        selections.push(...(query.getValues() as string[]));
      }
    }

    return selections;
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

  static getFulltextValue(value: string): string {
    const exact = value.startsWith('"') && value.endsWith('"');

    // Replace reserved chars with space
    const specialChars = [
      "@",
      "+",
      "-",
      "*",
      ")",
      "(",
      ",",
      "<",
      ">",
      "~",
      '"',
    ];
    let sanitized = value;
    for (const char of specialChars) {
      sanitized = sanitized.split(char).join(" ");
    }
    sanitized = sanitized.replace(/\s+/g, " ").trim();

    if (!sanitized) {
      return "";
    }

    if (exact) {
      sanitized = `"${sanitized}"`;
    } else {
      sanitized += "*";
    }

    return sanitized;
  }

  static escapeWildcards(value: string): string {
    const wildcards = [
      "%",
      "_",
      "[",
      "]",
      "^",
      "-",
      ".",
      "*",
      "+",
      "?",
      "(",
      ")",
      "{",
      "}",
      "|",
    ];

    for (const wildcard of wildcards) {
      const escapedWildcard = wildcard.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      value = value.replace(new RegExp(escapedWildcard, "g"), "\\" + wildcard);
    }

    return value;
  }

  static getJunctionTable(
    coll: number,
    relColl: number,
    attr: string,
    relAttr: string,
  ): string {
    return `_${coll}_${relColl}_${attr}_${relAttr}`;
  }

  static sanitize(value: string): string {
    if (value === null || value === undefined) {
      throw new DatabaseException(
        "Failed to sanitize key: value is null or undefined",
      );
    }

    const sanitized = value.replace(/[^A-Za-z0-9_\-]/g, "");
    if (sanitized === "") {
      throw new DatabaseException(
        "Failed to sanitize key: filtered value is empty",
      );
    }

    return sanitized;
  }

  /**
   * Builds a comprehensive SQL query with joins and filters for n-level relationships
   */
  static buildSql(
    meta: Partial<Meta>,
    query: ProcessedQuery,
    {
      forPermission,
      ctx,
    }: {
      forPermission: PermissionEnum;
      ctx: AuthContext;
    },
    quoteValue: QuoteValue,
  ): {
    sql: string;
    params: any[];
    joins: string[];
    selections: string[];
  } {
    const {
      selections,
      populateQueries = [],
      filters,
      collection,
      ...options
    } = query;
    const mainTableAlias = "main";
    const collectionName = SqlBuilder.sanitize(collection.getId());
    const mainTable = SqlBuilder.getSQLTable(meta, collectionName);

    const cursorConditions = SqlBuilder.buildCursorConditions(
      options.cursor,
      options.cursorDirection,
      options.orders,
      mainTableAlias,
    );

    const result = SqlBuilder.handleConditions(
      meta,
      {
        populateQueries,
        tableAlias: mainTableAlias,
        depth: 0,
        collection,
        filters,
        selections,
        ...options,
        forPermission,
        ctx,
      },
      quoteValue,
    );
    let orderSql = "";

    if (result.orders.length) {
      orderSql = `ORDER BY ${result.orders.join(", ")}`;
    }

    if (cursorConditions.condition) {
      result.conditions.push(cursorConditions.condition);
      result.params.push(...cursorConditions.params);
    }

    const limitClause = options.limit ? `LIMIT ?` : "";
    if (options.limit) result.params.push(options.limit);

    const offsetClause = options.offset ? `OFFSET ?` : "";
    if (options.offset) result.params.push(options.offset);

    const finalWhereClause =
      result.conditions.length > 0
        ? `WHERE ${result.conditions.join(" AND ")}`
        : "";
    const sql = `
            SELECT DISTINCT ${result.selectionsSql.join(", ")}
            FROM ${mainTable} AS ${SqlBuilder.quote(mainTableAlias)}
            ${result.joins.join(" ")}
            ${finalWhereClause}
           ${orderSql}
            ${limitClause}
            ${offsetClause}
        `
      .trim()
      .replace(/\s+/g, " ");

    return {
      sql,
      selections: result.selectionsSql,
      params: result.params,
      joins: result.joins,
    };
  }

  /**
   * Recursively handles building selections, joins, where conditions, and order clauses for main and populated queries.
   */
  static handleConditions(
    meta: Partial<Meta>,
    {
      populateQueries = [],
      tableAlias,
      depth = 0,
      forPermission,
      ctx,
      ...rest
    }: (ProcessedQuery | PopulateQuery) & {
      tableAlias?: string;
      depth: number;
      forPermission: PermissionEnum;
      ctx: AuthContext;
    },
    quoteValue: QuoteValue,
  ) {
    const conditions: string[] = [];
    const selectionsSql: string[] = [];
    const joins: string[] = [];
    let orders: string[] = [];
    const params: any[] = [];
    tableAlias = tableAlias ?? "main";

    const {
      collection,
      filters = [],
      selections = [],
      orders: ordersFromQuery,
      skipAuth,
    } = rest;

    selectionsSql.push(
      ...SqlBuilder.buildSelections(meta, selections, tableAlias, collection),
    );
    const whereInfo = SqlBuilder.buildWhereConditions(
      meta,
      filters,
      tableAlias,
      collection.getId(),
    );
    if (whereInfo.conditions.length) {
      conditions.push(...whereInfo.conditions);
      params.push(...whereInfo.params);
    }

    if (
      tableAlias === "main" &&
      !isSystemContext(ctx) &&
      !skipAuth &&
      collection.get("documentSecurity", false)
    ) {
      conditions.push(
        SqlBuilder.getSQLPermissionsCondition(
          meta,
          {
            collection: collection.getId(),
            roles: [...ctx.roles],
            alias: tableAlias,
            type: forPermission,
          },
          quoteValue,
        ),
      );
      if (meta.sharedTables) params.push(meta.tenantId);
    }

    if (meta.sharedTables) {
      params.push(meta.tenantId);
      conditions.push(
        SqlBuilder.getTenantQuery(meta, collection.getId(), tableAlias, undefined, ""),
      );
    }

    const _orders = SqlBuilder.buildOrderClause(ordersFromQuery, tableAlias);
    if (_orders.length) {
      orders.push(..._orders);
    }

    // Recursively handle populated queries (relationships)
    for (let i = 0; i < populateQueries.length; i++) {
      const populateQuery: PopulateQuery = populateQueries[i]!;
      const { attribute, authorized, ...rest } = populateQuery;
      if (!authorized) continue;
      const relationshipAttr = collection
        .get("attributes", [])
        .find(
          (attr) =>
            attr.get("type") === AttributeEnum.Relationship &&
            attr.get("key", attr.getId()) === attribute,
        );

      if (!relationshipAttr) continue;

      const relationAlias = `rel_${depth}_${i}`;
      const parentAlias = tableAlias;
      const options = relationshipAttr.get("options", {}) as RelationOptions;
      const side = options.side;
      const relationType = options.relationType;
      const twoWayKey = options.twoWayKey;
      const relationshipKey = relationshipAttr.get(
        "key",
        relationshipAttr.getId(),
      );

      const relatedTableName = SqlBuilder.sanitize(options.relatedCollection);
      const relatedTable = SqlBuilder.getSQLTable(meta, relatedTableName);
      let junctionCollection = "";
      if (relationType === RelationEnum.ManyToMany) {
        const parent = side === RelationSideEnum.Parent;
        const coll = parent
          ? collection.getSequence()
          : populateQuery.collection.getSequence();
        const relColl = parent
          ? populateQuery.collection.getSequence()
          : collection.getSequence();
        const attr = parent ? relationshipAttr.getId() : twoWayKey!;
        const relAttr = parent ? twoWayKey! : relationshipAttr.getId();
        junctionCollection = SqlBuilder.getJunctionTable(
          coll,
          relColl,
          attr,
          relAttr,
        );
      }

      const joinCondition = SqlBuilder.buildJoinCondition(
        meta,
        relationType,
        parentAlias,
        relationAlias,
        relationshipKey,
        twoWayKey,
        side,
        junctionCollection,
      );

      if (joinCondition) {
        joins.push(
          `LEFT JOIN ${relatedTable} AS ${SqlBuilder.quote(relationAlias)} ON ${joinCondition}`,
        );

        if (
          !isSystemContext(ctx) &&
          !rest.skipAuth &&
          rest.collection.get("documentSecurity", false)
        ) {
          joins.push(
            `AND ${SqlBuilder.getSQLPermissionsCondition(
              meta,
              {
                collection: relatedTableName,
                roles: [...ctx.roles],
                alias: relationAlias,
                type: forPermission,
              },
              quoteValue,
            )}`,
          );
          if (meta.sharedTables) params.push(meta.tenantId);
        }

        if (meta.sharedTables) {
          joins.push(SqlBuilder.getTenantQuery(meta, relatedTableName, relationAlias));
          params.push(meta.tenantId);
        }
      }

      const nestedResult = SqlBuilder.handleConditions(
        meta,
        {
          attribute,
          ...rest,
          depth: depth + 1,
          tableAlias: relationAlias,
          forPermission,
          ctx,
        },
        quoteValue,
      );

      // Prefix the selections to avoid conflicts
      const prefixedSelections = nestedResult.selectionsSql.map((sel) => {
        const parts = sel.split(" AS ");
        const prefix = relationshipKey;
        if (parts.length === 2 && parts[1]) {
          return `${parts[0]} AS ${SqlBuilder.quote(`${prefix}_${parts[1].replace(/"/g, "")}`)}`;
        }
        return sel;
      });

      if (nestedResult.conditions.length)
        conditions.push(...nestedResult.conditions);
      if (nestedResult.joins.length) joins.push(...nestedResult.joins);
      if (prefixedSelections.length) selectionsSql.push(...prefixedSelections);
      if (nestedResult.orders.length) orders.push(...nestedResult.orders);
      if (nestedResult.params.length) params.push(...nestedResult.params);
    }

    return {
      conditions,
      selectionsSql,
      orders,
      params,
      joins,
    };
  }

  /**
   * Builds selection clauses for the main table and relationship
   */
  static buildSelections(
    meta: Partial<Meta>,
    selections: string[],
    tableAlias: string,
    collection: Doc<Collection>,
  ): string[] {
    const result: string[] = [];
    const internalFields = [
      "$id",
      "$sequence",
      "$createdAt",
      "$updatedAt",
      "$permissions",
    ];
    const allFields = [...new Set([...internalFields, ...selections])];

    for (const field of allFields) {
      const dbKey = SqlBuilder.getInternalKeyForAttribute(field);
      const sanitizedKey = SqlBuilder.sanitize(dbKey);
      result.push(
        `${SqlBuilder.quote(tableAlias)}.${SqlBuilder.quote(sanitizedKey)} AS ${SqlBuilder.quote(field)}`,
      );
    }

    if (meta.sharedTables) {
      result.push(
        `${SqlBuilder.quote(tableAlias)}.${SqlBuilder.quote("_tenant")} AS ${SqlBuilder.quote("$tenant")}`,
      );
    }

    return result;
  }

  /**
   * Builds JOIN condition based on relationship type
   */
  static buildJoinCondition(
    meta: Partial<Meta>,
    relationType: RelationEnum,
    parentAlias: string,
    relationAlias: string,
    relationshipKey: string,
    twoWayKey: string = "",
    side: RelationSideEnum,
    junctionCollection: string,
  ): string | null {
    const parentUidCol = `${SqlBuilder.quote(parentAlias)}.${SqlBuilder.quote("_uid")}`;
    const relationUidCol = `${SqlBuilder.quote(relationAlias)}.${SqlBuilder.quote("_uid")}`;
    const parentRelCol = `${SqlBuilder.quote(parentAlias)}.${SqlBuilder.quote(SqlBuilder.sanitize(relationshipKey))}`;
    const relationRelCol = `${SqlBuilder.quote(relationAlias)}.${SqlBuilder.quote(SqlBuilder.sanitize(twoWayKey))}`;

    switch (relationType) {
      case RelationEnum.OneToOne:
        if (side === RelationSideEnum.Parent) {
          return `${parentRelCol} = ${relationUidCol}`;
        } else {
          return `${parentUidCol} = ${relationRelCol}`;
        }

      case RelationEnum.OneToMany:
        if (side === RelationSideEnum.Parent) {
          return `${parentUidCol} = ${relationRelCol}`;
        } else {
          return `${parentRelCol} = ${relationUidCol}`;
        }

      case RelationEnum.ManyToOne:
        if (side === RelationSideEnum.Child) {
          return `${parentUidCol} = ${relationRelCol}`;
        } else {
          return `${parentRelCol} = ${relationUidCol}`;
        }

      case RelationEnum.ManyToMany: {
        if (!junctionCollection)
          throw new DatabaseException(
            "junction collection is required for many to many relation.",
          );
        const junctionTable = SqlBuilder.getSQLTable(meta, junctionCollection);
        const parentJoinKey = SqlBuilder.quote(SqlBuilder.sanitize(relationshipKey));
        const relationJoinKey = SqlBuilder.quote(SqlBuilder.sanitize(twoWayKey));

        return `EXISTS (
                    SELECT 1
                    FROM ${junctionTable} AS jt
                    WHERE jt.${parentJoinKey} = ${parentUidCol}
                      AND jt.${relationJoinKey} = ${relationUidCol}
                      ${SqlBuilder.getTenantQuery(meta, junctionCollection)}
                )`;
      }
      default:
        return null;
    }
  }

  /**
   * Builds WHERE conditions from queries
   */
  static buildWhereConditions(
    meta: Partial<Meta>,
    queries: Query[],
    tableAlias: string,
    collection: string,
  ): { conditions: string[]; params: any[] } {
    const conditions: string[] = [];
    const conditionParams: any[] = [];

    if (meta.sharedTables) {
      conditions.push(
        SqlBuilder.getTenantQuery(meta, collection, tableAlias, undefined, ""),
      );
      conditionParams.push(meta.tenantId);
    }

    for (const query of queries) {
      const condition = SqlBuilder.buildQueryCondition(meta, query, tableAlias);
      if (condition.sql) {
        conditions.push(condition.sql);
        conditionParams.push(...condition.params);
      }
    }

    return { conditions, params: conditionParams };
  }

  /**
   * Builds a single query condition
   */
  static buildQueryCondition(
    meta: Partial<Meta>,
    query: Query,
    tableAlias: string,
  ): { sql: string; params: any[] } {
    const method = query.getMethod();
    const attribute = query.getAttribute();
    const values = query.getValues();
    const params: any[] = [];

    if (method === QueryType.Select || method === QueryType.Populate) {
      return { sql: "", params: [] };
    }

    const dbKey = SqlBuilder.getInternalKeyForAttribute(attribute);

    let columnRef: string | undefined;

    // Handle JSON path operators (->, ->>)
    if (dbKey.includes("->>") || dbKey.includes("->")) {
      const parts = dbKey.split(/(->>|->)/);
      const mainColumn = parts[0]!;
      const sanitizedMainColumn = SqlBuilder.sanitize(mainColumn);
      const quotedMainColumn = `${SqlBuilder.quote(tableAlias)}.${SqlBuilder.quote(sanitizedMainColumn)}`;

      let pathExpression = quotedMainColumn;

      for (let i = 1; i < parts.length; i += 2) {
        const operator = parts[i]; // -> or ->>
        const path = parts[i + 1];

        if (path) {
          const sanitizedPath = SqlBuilder.sanitize(path);
          pathExpression += `${operator}'${sanitizedPath}'`;
        }
      }

      columnRef = pathExpression;
    } else if (![QueryType.And, QueryType.Or].includes(method)) {
      const sanitizedKey = SqlBuilder.sanitize(dbKey);
      columnRef = `${SqlBuilder.quote(tableAlias)}.${SqlBuilder.quote(sanitizedKey)}`;
    }

    let sql = "";

    switch (method) {
      case QueryType.Equal:
        if (values.length === 1) {
          sql = `${columnRef} = ?`;
          params.push(values[0]);
        } else {
          sql = `${columnRef} IN (${values.map(() => "?").join(", ")})`;
          params.push(...values);
        }
        break;

      case QueryType.NotEqual:
        if (values.length === 1) {
          sql = `${columnRef} != ?`;
          params.push(values[0]);
        } else {
          sql = `${columnRef} NOT IN (${values.map(() => "?").join(", ")})`;
          params.push(...values);
        }
        break;

      case QueryType.LessThan:
        sql = `${columnRef} < ?`;
        params.push(values[0]);
        break;

      case QueryType.LessThanEqual:
        sql = `${columnRef} <= ?`;
        params.push(values[0]);
        break;

      case QueryType.GreaterThan:
        sql = `${columnRef} > ?`;
        params.push(values[0]);
        break;

      case QueryType.GreaterThanEqual:
        sql = `${columnRef} >= ?`;
        params.push(values[0]);
        break;

      case QueryType.Contains:
      case QueryType.NotContains:
        if (query.onArray()) {
          sql = `${columnRef} && ?`;
          params.push(values);
        } else {
          sql = `${columnRef} LIKE ?`;
          params.push(`%${SqlBuilder.escapeWildcards(values[0] as string)}%`);
        }
        break;

      case QueryType.StartsWith:
      case QueryType.NotStartsWith:
        sql = `${columnRef} LIKE ?`;
        params.push(`${SqlBuilder.escapeWildcards(values[0] as string)}%`);
        break;

      case QueryType.EndsWith:
      case QueryType.NotEndsWith:
        sql = `${columnRef} LIKE ?`;
        params.push(`%${SqlBuilder.escapeWildcards(values[0] as string)}`);
        break;

      case QueryType.IsNull:
        sql = `${columnRef} IS NULL`;
        break;

      case QueryType.IsNotNull:
        sql = `${columnRef} IS NOT NULL`;
        break;

      case QueryType.Between:
      case QueryType.NotBetween:
        sql = `${columnRef} BETWEEN ? AND ?`;
        params.push(values[0], values[1]);
        break;

      case QueryType.Search:
      case QueryType.NotSearch:
        sql = `to_tsvector('${Database.FULLTEXT_LANGUAGE}', ${columnRef}) @@ plainto_tsquery('${Database.FULLTEXT_LANGUAGE}', ?)`;
        params.push(values[0]);
        break;

      case QueryType.And:
        const andConditions = (values as Query[]).map((subQuery) =>
          SqlBuilder.buildQueryCondition(meta, subQuery, tableAlias),
        );
        sql = `(${andConditions
          .map((c) => c.sql)
          .filter(Boolean)
          .join(" AND ")})`;
        andConditions.forEach((c) => params.push(...c.params));
        break;

      case QueryType.Or:
        const orConditions = (values as Query[]).map((subQuery) =>
          SqlBuilder.buildQueryCondition(meta, subQuery, tableAlias),
        );
        sql = `(${orConditions
          .map((c) => c.sql)
          .filter(Boolean)
          .join(" OR ")})`;
        orConditions.forEach((c) => params.push(...c.params));
        break;
      case QueryType.Not:
        const notCondition = SqlBuilder.buildQueryCondition(
          meta,
          values[0] as Query,
          tableAlias,
        );
        sql = `NOT (${notCondition.sql})`;
        params.push(...notCondition.params);
        break;
      default:
        break;
    }

    switch (method) {
      case QueryType.NotContains:
      case QueryType.NotSearch:
      case QueryType.NotBetween:
      case QueryType.NotStartsWith:
      case QueryType.NotEndsWith:
        sql = `NOT (${sql})`;
        break;
      default:
        break;
    }

    return { sql, params };
  }

  /**
   * Builds ORDER BY clause
   */
  static buildOrderClause(
    orders: Record<string, OrderEnum>,
    tableAlias: string,
  ): string[] {
    const entries = Object.entries(orders);
    if (entries.length === 0) {
      // Default order by _id
      return [`${SqlBuilder.quote(tableAlias)}.${SqlBuilder.quote("_id")} ASC`];
    }

    const orderParts = entries.map(([attr, type]) => {
      const dbKey = SqlBuilder.getInternalKeyForAttribute(attr);
      const sanitizedKey = SqlBuilder.sanitize(dbKey);
      const orderType = type || "ASC";
      return `${SqlBuilder.quote(tableAlias)}.${SqlBuilder.quote(sanitizedKey)} ${orderType}`;
    });

    return orderParts;
  }

  /**
   * Builds cursor conditions for pagination
   */
  static buildCursorConditions(
    cursor: Doc<any> | null = null,
    cursorDirection: CursorEnum | null,
    orders: Record<string, OrderEnum>,
    tableAlias: string,
  ): { condition: string; params: any[] } {
    const uniqueOrderAttr = orders["$id"] || orders["$sequence"];

    // I know this is not the good place to update the orders, but it works.
    if (!uniqueOrderAttr) {
      orders["$sequence"] = OrderEnum.Asc;
    }

    const orderAttributes = Object.keys(orders);
    if (!cursor || orderAttributes.length === 0) {
      return { condition: "", params: [] };
    }

    for (const attr of orderAttributes) {
      const orderValue = cursor.get(attr, null);
      if (orderValue === null) {
        throw new OrderException(`Order attribute '${attr}' is empty`, attr);
      }
    }

    cursorDirection ??= CursorEnum.After;
    const conditions: string[] = [];
    const params: any[] = [];
    const operator = cursorDirection === CursorEnum.After ? ">" : "<";

    if (orderAttributes.length === 1 && orderAttributes[0] === "$sequence") {
      // single unique attribute
      const attr = orderAttributes[0];
      const dbKey = SqlBuilder.getInternalKeyForAttribute(attr);
      const sanitizedKey = SqlBuilder.sanitize(dbKey);
      conditions.push(
        `${SqlBuilder.quote(tableAlias)}.${SqlBuilder.quote(sanitizedKey)} ${operator} ?`,
      );
      params.push(cursor.get(attr));
    } else {
      // multiple attributes
      for (let i = 0; i < orderAttributes.length; i++) {
        const attr = orderAttributes[i];
        if (!attr) continue;
        const dbKey = SqlBuilder.getInternalKeyForAttribute(attr);
        const sanitizedKey = SqlBuilder.sanitize(dbKey);

        const equalityConditions = orderAttributes
          .slice(0, i)
          .filter((prevAttr): prevAttr is string => prevAttr !== undefined)
          .map((prevAttr) => {
            const prevDbKey = SqlBuilder.getInternalKeyForAttribute(prevAttr);
            const prevSanitizedKey = SqlBuilder.sanitize(prevDbKey);
            params.push(cursor.get(prevAttr));
            return `${SqlBuilder.quote(tableAlias)}.${SqlBuilder.quote(prevSanitizedKey)} = ?`;
          });

        equalityConditions.push(
          `${SqlBuilder.quote(tableAlias)}.${SqlBuilder.quote(sanitizedKey)} ${operator} ?`,
        );
        params.push(cursor.get(attr));

        conditions.push(`(${equalityConditions.join(" AND ")})`);
      }
    }

    return {
      condition: conditions.length > 0 ? `(${conditions.join(" OR ")})` : "",
      params,
    };
  }

  /**
   * Resolves the schema name from adapter metadata, mirroring BaseAdapter's
   * `$schema` getter semantics (throws when unset).
   */
  private static schemaOf(meta: Partial<Meta>): string {
    if (!meta.schema)
      throw new DatabaseException(
        "Schema name is not defined in adapter metadata.",
      );
    return meta.schema;
  }
}
