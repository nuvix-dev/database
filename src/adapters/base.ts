import { DatabaseException } from "@errors/base.js";
import { IClient } from "./interface.js";
import {
  AttributeEnum,
  EventsEnum,
  IndexEnum,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
  CursorEnum,
  OrderEnum,
} from "@core/enums.js";
import { IncreaseDocumentAttribute } from "./types.js";
import { Doc } from "@core/doc.js";
import { Database, PopulateQuery, ProcessedQuery } from "@core/database.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query, QueryType } from "@core/query.js";
import { Entities, IEntity } from "types.js";
import { Logger } from "@utils/logger.js";
import { Authorization } from "@utils/authorization.js";
import { Collection, RelationOptions } from "@validators/schema.js";
import { DatabaseError } from "pg";
import {
  DuplicateException,
  NotFoundException,
  OrderException,
  TimeoutException,
  TruncateException,
} from "@errors/index.js";
import { createHash } from "crypto";
import { EventEmitter } from "events";

export abstract class BaseAdapter extends EventEmitter {
  public readonly type: string = "base";
  protected _meta: Partial<Meta> = { schema: "public" };
  protected abstract client: IClient;
  protected $logger = new Logger();

  protected $timeout: number = 0;

  readonly $limitForString: number = 10485760;
  readonly $limitForInt: bigint = 9223372036854775807n;
  readonly $limitForAttributes: number = 1600;
  readonly $limitForIndexes: number = 64;
  readonly $supportForSchemas: boolean = true;
  readonly $supportForIndex: boolean = true;
  readonly $supportForAttributes: boolean = true;
  readonly $supportForUniqueIndex: boolean = true;
  readonly $supportForFulltextIndex: boolean = true;
  readonly $supportForUpdateLock: boolean = true;
  readonly $supportForAttributeResizing: boolean = true;
  readonly $supportForBatchOperations: boolean = true;
  readonly $supportForGetConnectionId: boolean = false;
  readonly $supportForCacheSkipOnFailure: boolean = true;
  readonly $supportForHostname: boolean = true;
  readonly $documentSizeLimit: number = 16777216;
  readonly $supportForCasting: boolean = true;
  readonly $supportForNumericCasting: boolean = true;
  readonly $supportForQueryContains: boolean = true;
  readonly $supportForIndexArray: boolean = true;
  readonly $supportForCastIndexArray: boolean = true;
  readonly $supportForRelationships: boolean = true;
  readonly $supportForReconnection: boolean = true;
  readonly $supportForBatchCreateAttributes: boolean = true;
  readonly $maxVarcharLength: number = 10485760;
  readonly $maxIndexLength: number = 8191;
  readonly $supportForJSONOverlaps: boolean = true;

  protected transformations: Partial<
    Record<EventsEnum, Array<[string, (query: string) => string]>>
  > = {
    [EventsEnum.All]: [],
  };

  constructor(options: { type?: string } = {}) {
    super();
    if (options.type) {
      this.type = options.type;
    }
  }

  public get $database(): string {
    if (!this.client.$database)
      throw new DatabaseException(
        "Database name is not defined in client metadata.",
      );
    return this.client.$database;
  }

  public get $schema(): string {
    if (!this._meta.schema)
      throw new DatabaseException(
        "Schema name is not defined in adapter metadata.",
      );
    return this._meta.schema;
  }

  public get $sharedTables(): boolean {
    const sharedTables = this._meta.sharedTables;
    if (sharedTables && !this._meta.tenantId) {
      Logger.warn(
        "Shared tables are enabled but tenantId is not defined in adapter metadata. This may lead to unexpected behavior.",
      );
    }
    return !!sharedTables;
  }

  public get $tenantId(): number | undefined {
    return this._meta.tenantId;
  }

  public get $tenantPerDocument(): boolean {
    return !!this._meta.tenantPerDocument;
  }

  public get $namespace(): string {
    return this._meta.namespace ?? "default";
  }

  public get $metadata() {
    return this._meta.metadata ?? {};
  }

  public get $client() {
    return this.client;
  }

  public setMeta(meta: Partial<Meta>) {
    if (this._meta.metadata) {
      this._meta.metadata = { ...this._meta.metadata, ...meta.metadata };
      let metaString: string = "";

      for (const [key, value] of Object.entries(this._meta.metadata)) {
        metaString += `/* ${key}: ${value} */\n`;
      }

      this.before(EventsEnum.All, "metadata", (query: string) => {
        return metaString + query;
      });
    }
    this._meta = { ...this._meta, ...meta };
    return this;
  }

  public setLogger(logger: Logger) {
    this.$logger = logger;
    return this;
  }

  public before(
    event: EventsEnum,
    name: string,
    callback?: (query: string) => string,
  ): void {
    if (!this.transformations[event]) {
      this.transformations[event] = [];
    }
    if (callback) {
      this.transformations[event].push([name, callback]);
    } else {
      const index = this.transformations[event].findIndex(
        (transformation) => transformation[0] === name,
      );
      if (index !== -1) {
        this.transformations[event].splice(index, 1);
      }
    }
  }

  public trigger(event: EventsEnum, query: string): string {
    for (const transformation of this.transformations[EventsEnum.All] || []) {
      query = transformation[1](query);
    }
    for (const transformation of this.transformations[event] || []) {
      query = transformation[1](query);
    }
    return query;
  }

  public sanitize(value: string): string {
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

  public async ping(): Promise<void> {
    return await this.client.ping();
  }

  /**
   * Checks if a database schema or table exists.
   * @param name - Schema name or table name to check
   * @param collection - Optional collection/schema name. If provided, checks for table existence within that schema
   * @returns Promise<boolean> - true if the schema/table exists, false otherwise
   */
  async exists(name: string, collection?: string): Promise<boolean> {
    if (!name?.trim()) {
      throw new DatabaseException(
        "Name parameter is required and cannot be empty",
      );
    }

    try {
      let sql: string;
      const params: string[] = [];

      if (collection) {
        sql = `
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = ? AND table_name = ?
          LIMIT 1
        `;
        params.push(this.sanitize(name), this.getTableName(collection));
      } else {
        sql = `
          SELECT 1
          FROM information_schema.schemata
          WHERE schema_name = ?
          LIMIT 1
        `;
        params.push(this.sanitize(name));
      }

      const { rows } = await this.client.query<any>(sql, params);
      return rows.length > 0;
    } catch (error) {
      this.processException(
        error,
        `Failed to check if ${collection ? "table" : "schema"} exists`,
      );
    }
  }

  /**
   * Retrieves a document from the specified collection by its ID.
   */
  public async getDocument<C extends string & keyof Entities>(
    collection: C,
    id: string,
    queries?: ProcessedQuery | null,
    forUpdate?: boolean,
  ): Promise<Doc<Entities[C]>>;
  public async getDocument<C extends Record<string, any>>(
    collection: string,
    id: string,
    queries?: ProcessedQuery | null,
    forUpdate?: boolean,
  ): Promise<Doc<Partial<IEntity> & C>>;
  public async getDocument(
    collection: string,
    id: string,
    { selections }: ProcessedQuery,
    forUpdate: boolean = false,
  ): Promise<Doc<Partial<IEntity> & Record<string, any>>> {
    if (!collection || !id) {
      throw new DatabaseException(
        "Failed to get document: collection and id are required",
      );
    }

    const table = this.getSQLTable(collection);
    const alias = Query.DEFAULT_ALIAS;
    const params: any[] = [id];

    let sql = `
            SELECT ${this.getAttributeProjection(selections, alias, collection)}
            FROM ${table} AS ${alias}
            WHERE ${this.quote(alias)}.${this.quote("_uid")} = ?
            ${this.getTenantQuery(collection, alias)}
        `;

    if (forUpdate && this.$supportForUpdateLock) {
      sql += " FOR UPDATE";
    }

    if (this.$sharedTables) {
      params.push(this.$tenantId);
    }

    try {
      const { rows } = await this.client.query<any>(sql, params);

      let document = rows[0];

      return new Doc(document);
    } catch (e) {
      this.processException(e);
    }
  }

  /**
   * Deletes a document from the specified collection by its ID.
   */
  public async deleteDocument(
    collection: string,
    document: Doc<any>,
  ): Promise<boolean> {
    if (!collection || document.empty()) {
      throw new DatabaseException(
        "Failed to delete document: collection and id are required",
      );
    }

    try {
      const table = this.getSQLTable(collection);
      const params: any[] = [document.getId()];

      let sql = `
                DELETE FROM ${table}
                WHERE ${this.quote("_uid")} = ?
                ${this.getTenantQuery(collection)}
                RETURNING _id
            `;

      sql = this.trigger(EventsEnum.DocumentDelete, sql);

      if (this.$sharedTables) {
        params.push(this.$tenantId);
      }

      const { rows: result } = await this.client.query<any>(sql, params);

      // Delete permissions
      const permParams: any[] = [document.getSequence()];
      let permSql = `
                DELETE FROM ${this.getSQLTable(collection + "_perms")}
                WHERE ${this.quote("_document")} = ?
                ${this.getTenantQuery(collection)}
                RETURNING _id
            `;

      permSql = this.trigger(EventsEnum.PermissionsDelete, permSql);

      if (this.$sharedTables) {
        permParams.push(this.$tenantId);
      }

      await this.client.query(permSql, permParams);

      return result.length > 0;
    } catch (error) {
      this.processException(error, "Failed to delete document");
    }
  }

  /**
   * Increases a numeric attribute of a document by a specified value.
   */
  public async increaseDocumentAttribute({
    collection,
    id,
    attribute,
    updatedAt,
    value,
    min,
    max,
  }: IncreaseDocumentAttribute): Promise<boolean> {
    const attr = this.quote(attribute);
    const params: any[] = [value, updatedAt, id];

    let sql = `
            UPDATE ${this.getSQLTable(collection)} 
            SET 
                ${attr} = ${attr} + ?,
                ${this.quote("_updatedAt")} = ?
            WHERE _uid = ?
            ${this.getTenantQuery(collection)}
        `;

    if (this.$sharedTables) {
      params.push(this.$tenantId);
    }
    if (max !== undefined && max !== null) {
      sql += ` AND ${attr} <= ?`;
      params.push(max);
    }
    if (min !== undefined && max !== null) {
      sql += ` AND ${attr} >= ?`;
      params.push(min);
    }

    sql = this.trigger(EventsEnum.DocumentUpdate, sql);

    try {
      await this.client.query(sql, params);
      return true;
    } catch (e: any) {
      throw this.processException(e, "Failed to increase document attribute");
    }
  }

  /**
   * Counts the number of documents in a collection based on the provided queries.
   */
  public async count(
    collection: string,
    queries: ((b: QueryBuilder) => QueryBuilder) | Array<Query> = [],
    max?: number,
  ): Promise<number> {
    const name = this.sanitize(collection);
    const roles = Authorization.getRoles();
    const params: any[] = [];
    const where: string[] = [];
    const alias = Query.DEFAULT_ALIAS;

    const queryList = [
      ...(Array.isArray(queries)
        ? queries
        : queries(new QueryBuilder()).build()),
    ];

    const conditions = this.getSQLConditions(queryList, params);
    if (conditions) {
      where.push(conditions);
    }

    if (Authorization.getStatus()) {
      where.push(
        this.getSQLPermissionsCondition({
          collection: name,
          roles,
          alias,
          type: PermissionEnum.Read,
        }),
      );
      if (this.$sharedTables) params.push(this.$tenantId);
    }

    if (this.$sharedTables) {
      params.push(this.$tenantId);
      where.push(this.getTenantQuery(collection, alias, undefined, ""));
    }

    let limit = "";
    if (max !== null && max !== undefined) {
      params.push(max);
      limit = "LIMIT ?";
    }

    const sqlWhere = where.length > 0 ? "WHERE " + where.join(" AND ") : "";

    let sql = `
            SELECT COUNT(1) as sum FROM (
                SELECT 1
                FROM ${this.getSQLTable(name)} AS ${this.quote(alias)}
                ${sqlWhere}
                ${limit}
            ) table_count
        `;

    sql = this.trigger(EventsEnum.DocumentCount, sql);

    try {
      const { rows } = await this.client.query<any>(sql, params);
      const result = rows[0];
      return result?.sum ?? 0;
    } catch (error) {
      throw this.processException(error, "Failed to count documents");
    }
  }

  /**
   * Sums a specific attribute across documents in a collection.
   */
  public async sum(
    collection: string,
    attribute: string,
    queries: ((b: QueryBuilder) => QueryBuilder) | Array<Query> = [],
    max?: number,
  ): Promise<number> {
    const name = this.sanitize(collection);
    const roles = Authorization.getRoles();
    const params: any[] = [];
    const where: string[] = [];
    const alias = Query.DEFAULT_ALIAS;

    const queryList = [
      ...(Array.isArray(queries)
        ? queries
        : queries(new QueryBuilder()).build()),
    ];

    const conditions = this.getSQLConditions(queryList, params);
    if (conditions) {
      where.push(conditions);
    }

    if (Authorization.getStatus()) {
      where.push(
        this.getSQLPermissionsCondition({
          collection: name,
          roles,
          alias,
          type: PermissionEnum.Read,
        }),
      );
      if (this.$sharedTables) params.push(this.$tenantId);
    }

    if (this.$sharedTables) {
      params.push(this.$tenantId);
      where.push(this.getTenantQuery(collection, alias, undefined, ""));
    }

    let limit = "";
    if (max !== null && max !== undefined) {
      params.push(max);
      limit = "LIMIT ?";
    }

    const sqlWhere = where.length > 0 ? "WHERE " + where.join(" AND ") : "";

    let sql = `
            SELECT SUM(${this.quote(attribute)}) as sum FROM (
                SELECT ${this.quote(attribute)}
                FROM ${this.getSQLTable(name)} AS ${this.quote(alias)}
                ${sqlWhere}
                ${limit}
            ) table_count
        `;

    sql = this.trigger(EventsEnum.DocumentSum, sql);

    try {
      const { rows } = await this.client.query<any>(sql, params);
      const result = rows[0];
      return result?.sum ?? 0;
    } catch (error) {
      throw this.processException(error, "Failed to sum documents");
    }
  }

  /**
   * update permissions for a document
   */
  protected async updatePermissions(collection: string, document: Doc) {
    const operations: { sql: string; params: any[] }[] = [];

    // Get current permissions grouped by type
    const sqlParams: any[] = [document.getSequence()];
    let sql = `
            SELECT _type, _permissions
            FROM ${this.getSQLTable(collection + "_perms")}
            WHERE _document = ?
            ${this.getTenantQuery(collection)}
        `;
    sql = this.trigger(EventsEnum.PermissionsRead, sql);

    if (this.$sharedTables) {
      sqlParams.push(this.$tenantId);
    }

    const { rows } = await this.client.query<any>(sql, sqlParams);

    const existingPermissions: Record<string, string[]> = {};
    for (const row of rows) {
      existingPermissions[row._type] = Array.isArray(row._permissions)
        ? row._permissions
        : [];
    }

    // Process each permission type
    for (const type of Database.PERMISSIONS) {
      const newPermissions = document.getPermissionsByType(type);
      const currentPermissions = existingPermissions[type] || [];
      const hasChanged =
        JSON.stringify(newPermissions.sort()) !==
        JSON.stringify(currentPermissions.sort());

      if (!hasChanged) {
        continue;
      }

      if (newPermissions.length === 0) {
        // Delete the row if no permissions
        if (currentPermissions.length > 0) {
          const deleteParams: any[] = [document.getSequence(), type];
          let deleteSql = `
                        DELETE FROM ${this.getSQLTable(collection + "_perms")}
                        WHERE _document = ? AND _type = ?
                        ${this.getTenantQuery(collection)}
                    `;

          if (this.$sharedTables) {
            deleteParams.push(this.$tenantId);
          }

          deleteSql = this.trigger(EventsEnum.PermissionsDelete, deleteSql);
          operations.push({ sql: deleteSql, params: deleteParams });
        }
      } else {
        if (currentPermissions.length > 0) {
          // Update existing row
          const updateParams: any[] = [
            newPermissions,
            document.getSequence(),
            type,
          ];
          let updateSql = `
                        UPDATE ${this.getSQLTable(collection + "_perms")}
                        SET _permissions = ?
                        WHERE _document = ? AND _type = ?
                        ${this.getTenantQuery(collection)}
                    `;

          if (this.$sharedTables) {
            updateParams.push(this.$tenantId);
          }

          updateSql = this.trigger(EventsEnum.PermissionsUpdate, updateSql);
          operations.push({ sql: updateSql, params: updateParams });
        } else {
          // Insert new row
          const insertParams: any[] = [
            document.getSequence(),
            type,
            newPermissions,
          ];
          let insertSql = `
                        INSERT INTO ${this.getSQLTable(collection + "_perms")} 
                        (_document, _type, _permissions
                    `;

          if (this.$sharedTables) {
            insertSql += ", _tenant)";
            insertParams.push(this.$tenantId);
          } else {
            insertSql += ")";
          }

          insertSql += " VALUES (?, ?, ?)";

          if (this.$sharedTables) {
            insertSql = insertSql.replace(
              "VALUES (?, ?, ?)",
              "VALUES (?, ?, ?, ?)",
            );
          }

          insertSql = this.trigger(EventsEnum.PermissionsCreate, insertSql);
          operations.push({ sql: insertSql, params: insertParams });
        }
      }
    }

    return operations;
  }

  /**
   * Generates an upsert (insert or update) SQL statement for batch operations.
   * If `attribute` is provided, it will increment that column on duplicate key.
   */
  public getUpsertStatement(
    tableName: string,
    columns: string,
    batchKeys: string[],
    attributes: Record<string, any>,
    attribute: string = "",
  ): string {
    const getUpdateClause = (attribute: string, increment = false): string => {
      const quotedAttr = this.quote(this.sanitize(attribute));
      let newValue: string;
      if (increment) {
        newValue = `${this.getSQLTable(tableName)}.${quotedAttr} + EXCLUDED.${quotedAttr}`;
      } else {
        newValue = `EXCLUDED.${quotedAttr}`;
      }
      if (this.$sharedTables) {
        return `${quotedAttr} = CASE WHEN ${this.getSQLTable(tableName)}._tenant = EXCLUDED._tenant THEN ${newValue} ELSE ${quotedAttr} END`;
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
        .filter((a) => !this.$internalAttrs.includes(a))
        .map((attr) => getUpdateClause(attr));
    }

    const sql = `
      INSERT INTO ${this.getSQLTable(tableName)} ${columns}
      VALUES ${batchKeys.join(", ")}
      ON CONFLICT (_uid${this.$sharedTables ? ", _tenant" : ""}) DO UPDATE SET
          ${updateColumns.join(", ")}
    `;

    return sql;
  }

  protected getSQLType(
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
  protected getIndexName(coll: string, id: string): string {
    return `${this.sanitize(coll)}_${this.sanitize(id)}`;
  }

  /**@deprecated */
  protected getSQLCondition(query: Query, binds: any[]): string {
    query.setAttribute(this.getInternalKeyForAttribute(query.getAttribute()));

    const attribute = this.quote(this.sanitize(query.getAttribute()));
    const alias = this.quote(Query.DEFAULT_ALIAS);
    const method = query.getMethod();

    switch (method) {
      case QueryType.Or:
      case QueryType.And:
        const conditions: string[] = [];
        for (const q of query.getValues() as Query[]) {
          conditions.push(this.getSQLCondition(q, binds));
        }

        const methodStr = method.toUpperCase();
        return conditions.length === 0
          ? ""
          : ` ${methodStr} (` + conditions.join(" AND ") + ")";

      case QueryType.Search:
        binds.push(this.getFulltextValue(query.getValue() as string));
        return `to_tsvector('${Database.FULLTEXT_LANGUAGE}', ${alias}.${attribute}) @@ plainto_tsquery('${Database.FULLTEXT_LANGUAGE}', ?)`;

      case QueryType.Between:
        const values = query.getValues();
        binds.push(values[0], values[1]);
        return `${alias}.${attribute} BETWEEN ? AND ?`;

      case QueryType.IsNull:
      case QueryType.IsNotNull:
        return `${alias}.${attribute} ${this.getSQLOperator(method)}`;

      // @ts-ignore
      case QueryType.Contains:
        if (this.$supportForJSONOverlaps && query.onArray()) {
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
              processedValue = this.escapeWildcards(value) + "%";
              break;
            case QueryType.EndsWith:
              processedValue = "%" + this.escapeWildcards(value);
              break;
            case QueryType.Contains:
              processedValue = query.onArray()
                ? JSON.stringify(value)
                : "%" + this.escapeWildcards(value) + "%";
              break;
          }

          binds.push(processedValue);
          defaultConditions.push(
            `${alias}.${attribute} ${this.getSQLOperator(method)} ?`,
          );
        }

        return defaultConditions.length === 0
          ? ""
          : "(" + defaultConditions.join(" OR ") + ")";
    }
  }

  protected processException(
    error: DatabaseError | unknown,
    message?: string,
  ): never {
    const e = error as DatabaseError;

    if (!(e instanceof DatabaseError)) {
      if ((e as any) instanceof DatabaseException) {
        throw e;
      }
      throw new DatabaseException(
        (e as { message?: string })?.message ??
          message ??
          "Unexpected database error",
        e,
      );
    }

    switch (e.code) {
      case "57014": // Query canceled / timeout
        throw new TimeoutException("Query execution timed out", e.code, e);

      case "42P07": // Duplicate table
        throw new DuplicateException("Collection already exists", e.code, e);

      case "42701": // Duplicate column
        throw new DuplicateException("Column already exists", e.code, e);

      case "23505": // Unique constraint violation (duplicate row)
        throw new DuplicateException(
          "Unique constraint violation: duplicate row",
          e.code,
          e,
        );

      case "22001": // String data right truncation
        throw new TruncateException(
          "Value too long: data would be truncated",
          e.code,
          e,
        );

      case "42703": // Undefined column
        throw new NotFoundException("Referenced column not found", e.code, e);

      default:
        // For unmapped codes, rethrow to avoid masking potential issues
        throw e;
    }
  }

  readonly $supportForTimeouts = true;
  public get $internalIndexesKeys() {
    return ["primary", "_created_at", "_updated_at", "_tenant_id"];
  }

  public setTimeout(
    milliseconds: number,
    event: EventsEnum = EventsEnum.All,
  ): void {
    if (!this.$supportForTimeouts) {
      return;
    }
    if (milliseconds <= 0) {
      throw new DatabaseException("Timeout must be greater than 0");
    }

    this.$timeout = milliseconds;

    const seconds = milliseconds / 1000;

    this.before(event, "timeout", (sql: string) => {
      return `SET STATEMENT max_statement_time = ${seconds} FOR ${sql}`;
    });
  }

  /**@deprecated */
  protected getSQLOperator(method: string): string {
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

  protected getSQLTable(name: string): string {
    if (!name) {
      throw new DatabaseException("Failed to get SQL table: name is empty");
    }
    return `${this.quote(this.$schema)}.${this.quote(this.getTableName(name))}`;
  }

  protected getTableName(name: string): string {
    if (!name) {
      throw new DatabaseException("Failed to get table name: name is empty");
    }
    return `${this._meta.namespace}_${name}`;
  }

  protected getSQLIndex(table: string, name: string): string {
    const base = `${this.$schema}_${this._meta.namespace}_${table}_${name}`;
    const safeId = createHash("sha1").update(base).digest("hex").slice(0, 40);
    return this.quote(`${safeId}`);
  }

  protected getSQLIndexType(type: IndexEnum): string {
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

  protected getSQLPermissionsCondition({
    collection,
    roles,
    alias,
    type = PermissionEnum.Read,
  }: {
    collection: string;
    roles: string[];
    alias: string;
    type?: PermissionEnum;
  }): string {
    if (!collection || !roles?.length || !alias) {
      throw new DatabaseException(
        "Failed to get SQL permission condition: collection, roles, and alias are required",
      );
    }

    if (type && !Object.values(PermissionEnum).includes(type)) {
      throw new DatabaseException(`Unknown permission type: ${type}`);
    }

    const quotedRolesArray = `ARRAY[${roles.map((role) => this.client.quote(role)).join(", ")}]::text[]`;

    return `EXISTS (
            SELECT 1
            FROM ${this.getSQLTable(`${collection}_perms`)} p
            WHERE p.${this.quote("_document")} = ${this.quote(alias)}.${this.quote("_id")}
              AND p.${this.quote("_type")} = ${this.client.quote(type)}
              AND p.${this.quote("_permissions")} && ${quotedRolesArray}
              ${this.getTenantQuery(collection, "p")}
        )`.trim();
  }

  /**
   * @deprecated
   * Builds SQL conditions recursively and mutates the provided `binds` array with bound values.
   * @returns SQL condition string with placeholders.
   */
  protected getSQLConditions(
    queries: Query[],
    binds: any[],
    separator: string = "AND",
  ): string {
    const conditions: string[] = [];

    for (const query of queries) {
      if (query.getMethod() === QueryType.Select) {
        continue;
      }

      if (query.isNested()) {
        conditions.push(
          this.getSQLConditions(
            query.getValues() as Query[],
            binds,
            query.getMethod(),
          ),
        );
      } else {
        conditions.push(this.getSQLCondition(query, binds));
      }
    }

    const tmp = conditions.join(` ${separator} `);
    return tmp === "" ? "" : `(${tmp})`;
  }

  protected getTenantQuery(
    collection: string,
    alias: string = "",
    tenantCount: number = 0,
    condition: string = "AND",
  ): string {
    if (!this.$sharedTables) {
      return "";
    }

    let dot = "";
    let quotedAlias = alias;

    if (alias !== "") {
      dot = ".";
      quotedAlias = this.quote(alias);
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
      orIsNull = ` OR ${quotedAlias}${dot}${this.quote("_tenant")} IS NULL`;
    }

    return `${condition} (${quotedAlias}${dot}${this.quote("_tenant")} IN (${bindingsStr})${orIsNull})`;
  }

  /**
   * Generates a projection string for attributes in a SQL SELECT query.
   */
  protected getAttributeProjection(
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
          projected.push(`'${this.$schema}' AS ${this.quote(key)}`);
          break;
        case "$collection":
          projected.push(`'${collection}' AS ${this.quote(key)}`);
          break;
        default:
          const dbKey = this.getInternalKeyForAttribute(key);
          projected.push(
            `${this.quote(prefix)}.${this.quote(dbKey)} AS ${this.quote(key)}`,
          );
      }
    }

    return projected.join(", ");
  }

  public quote(name: string): string {
    if (!name) {
      throw new DatabaseException("Failed to quote name: name is empty");
    }
    return `"${name}"`;
  }

  /**@deprecated */
  protected getAttributeSelections(
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

  protected getInternalKeyForAttribute(attribute: string): string {
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

  protected getFulltextValue(value: string): string {
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

  protected escapeWildcards(value: string): string {
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

  protected static POSTGRES_ROW_OVERHEAD_MIN = 24;
  protected static POSTGRES_TOAST_POINTER_SIZE = 20;

  public getAttributeWidth(collection: Doc<Collection>): number {
    let totalEstimatedBytes = BaseAdapter.POSTGRES_ROW_OVERHEAD_MIN;

    // Base columns in the main collection table:
    // "_id" BIGINT: 8 bytes
    // "_uid" VARCHAR(255): 255 (actual data) + 1 (length byte for short strings) = 256 bytes *or* 4 (length byte) + 255 if long string.
    //     For estimating, we often assume max storage. For varchar(255), in-row is often 256.
    // "_createdAt" TIMESTAMP WITH TIME ZONE: 8 bytes
    // "_updatedAt" TIMESTAMP WITH TIME ZONE: 8 bytes
    // "_permissions" TEXT[]: This is an array, so it will be TOASTed if it gets large. 20-byte pointer.

    // Shared table `_tenant` (INTEGER): 4 bytes

    // _id (BIGINT)
    totalEstimatedBytes += 8;
    // _uid (VARCHAR(255)) - for in-row storage, it's roughly actual_length + 1 byte for small, 4 bytes for large.
    // For max length varchar, it will likely be 255 + 1. Let's assume max length for estimation.
    totalEstimatedBytes += 256; // 255 (data) + 1 (length header for small varlena)
    // _createdAt (TIMESTAMP WITH TIME ZONE)
    totalEstimatedBytes += 8;
    // _updatedAt (TIMESTAMP WITH TIME ZONE)
    totalEstimatedBytes += 8;
    totalEstimatedBytes += BaseAdapter.POSTGRES_TOAST_POINTER_SIZE;
    totalEstimatedBytes += 4;

    // Count of fixed columns for NULL bitmap
    let numberOfColumns = 6; // _id, _uid, _createdAt, _updatedAt, _permissions, _tenant

    const attributes = collection.get("attributes", []);

    for (const attr of attributes) {
      const attribute = attr.toObject();
      numberOfColumns++;

      if (attribute.array ?? false) {
        totalEstimatedBytes += BaseAdapter.POSTGRES_TOAST_POINTER_SIZE;
        continue;
      }

      switch (attribute.type) {
        case AttributeEnum.String:
          attribute.size = attribute?.size ?? 255;

          if (attribute.size > this.$maxVarcharLength || attribute.size > 255) {
            totalEstimatedBytes += BaseAdapter.POSTGRES_TOAST_POINTER_SIZE;
          } else {
            // VARCHAR(<=255). It will be in-row.
            // Actual data size + 1 byte for header (if < 128 bytes) or 4 bytes for header (if >= 128 bytes).
            totalEstimatedBytes += attribute.size + 1;
          }
          break;

        case AttributeEnum.Integer:
          attribute.size = attribute?.size ?? 4;
          if (attribute.size <= 2) {
            totalEstimatedBytes += 2; // SMALLINT
          } else if (attribute.size <= 4) {
            totalEstimatedBytes += 4; // INTEGER
          } else {
            // >= 8
            totalEstimatedBytes += 8; // BIGINT
          }
          break;

        case AttributeEnum.Float:
          totalEstimatedBytes += 8;
          break;

        case AttributeEnum.Boolean:
          totalEstimatedBytes += 1;
          break;

        case AttributeEnum.Relationship:
          totalEstimatedBytes += 256;
          break;

        case AttributeEnum.Timestamptz:
          // TIMESTAMP WITH TIME ZONE (8 bytes)
          totalEstimatedBytes += 8;
          break;

        case AttributeEnum.Json:
          totalEstimatedBytes += BaseAdapter.POSTGRES_TOAST_POINTER_SIZE;
          break;

        case AttributeEnum.Uuid:
          // UUID (16 bytes)
          totalEstimatedBytes += 16;
          break;

        case AttributeEnum.Virtual:
          numberOfColumns--;
          break;

        default:
          throw new DatabaseException(
            "Unknown attribute type: " + attribute.type,
          );
      }
    }

    // Add NULL bitmap size: (number_of_columns + 7) / 8, rounded up
    totalEstimatedBytes += Math.ceil(numberOfColumns / 8);
    return totalEstimatedBytes;
  }

  public getCountOfAttributes(collection: Doc<Collection>): number {
    const attributes = collection.get("attributes", []);
    return attributes.length + this.$countOfDefaultAttributes;
  }

  public getCountOfIndexes(collection: Doc<Collection>): number {
    const indexes = collection.get("indexes", []);
    return indexes.length + this.$countOfDefaultIndexes;
  }

  public get $countOfDefaultAttributes(): number {
    return Database.INTERNAL_ATTRIBUTES.length;
  }

  public get $countOfDefaultIndexes(): number {
    return Database.INTERNAL_INDEXES.length;
  }

  protected readonly $internalAttrs = [
    "$id",
    "$sequence",
    "$collection",
    "$tenant",
    "$createdAt",
    "$updatedAt",
    "$permissions",
  ];

  public getJunctionTable(
    coll: number,
    relColl: number,
    attr: string,
    relAttr: string,
  ): string {
    return `_${coll}_${relColl}_${attr}_${relAttr}`;
  }

  /**
   * Builds a comprehensive SQL query with joins and filters for n-level relationships
   */
  protected buildSql(
    query: ProcessedQuery,
    {
      forPermission,
    }: {
      forPermission: PermissionEnum;
    },
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
    const collectionName = this.sanitize(collection.getId());
    const mainTable = this.getSQLTable(collectionName);

    const cursorConditions = this.buildCursorConditions(
      options.cursor,
      options.cursorDirection,
      options.orders,
      mainTableAlias,
    );

    const result = this.handleConditions({
      populateQueries,
      tableAlias: mainTableAlias,
      depth: 0,
      collection,
      filters,
      selections,
      ...options,
      forPermission,
    });
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
            FROM ${mainTable} AS ${this.quote(mainTableAlias)}
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
  protected handleConditions({
    populateQueries = [],
    tableAlias,
    depth = 0,
    forPermission,
    ...rest
  }: (ProcessedQuery | PopulateQuery) & {
    tableAlias?: string;
    depth: number;
    forPermission: PermissionEnum;
  }) {
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
      ...this.buildSelections(selections, tableAlias, collection),
    );
    const whereInfo = this.buildWhereConditions(
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
      Authorization.getStatus() &&
      !skipAuth &&
      collection.get("documentSecurity", false)
    ) {
      const roles = Authorization.getRoles();
      conditions.push(
        this.getSQLPermissionsCondition({
          collection: collection.getId(),
          roles,
          alias: tableAlias,
          type: forPermission,
        }),
      );
      if (this.$sharedTables) params.push(this.$tenantId);
    }

    if (this.$sharedTables) {
      params.push(this.$tenantId);
      conditions.push(
        this.getTenantQuery(collection.getId(), tableAlias, undefined, ""),
      );
    }

    const _orders = this.buildOrderClause(ordersFromQuery, tableAlias);
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

      const relatedTableName = this.sanitize(options.relatedCollection);
      const relatedTable = this.getSQLTable(relatedTableName);
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
        junctionCollection = this.getJunctionTable(
          coll,
          relColl,
          attr,
          relAttr,
        );
      }

      const joinCondition = this.buildJoinCondition(
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
          `LEFT JOIN ${relatedTable} AS ${this.quote(relationAlias)} ON ${joinCondition}`,
        );

        if (
          Authorization.getStatus() &&
          !rest.skipAuth &&
          rest.collection.get("documentSecurity", false)
        ) {
          const roles = Authorization.getRoles();
          joins.push(
            `AND ${this.getSQLPermissionsCondition({
              collection: relatedTableName,
              roles,
              alias: relationAlias,
              type: forPermission,
            })}`,
          );
          if (this.$sharedTables) params.push(this.$tenantId);
        }

        if (this.$sharedTables) {
          joins.push(this.getTenantQuery(relatedTableName, relationAlias));
          params.push(this.$tenantId);
        }
      }

      const nestedResult = this.handleConditions({
        attribute,
        ...rest,
        depth: depth + 1,
        tableAlias: relationAlias,
        forPermission,
      });

      // Prefix the selections to avoid conflicts
      const prefixedSelections = nestedResult.selectionsSql.map((sel) => {
        const parts = sel.split(" AS ");
        const prefix = relationshipKey;
        if (parts.length === 2 && parts[1]) {
          return `${parts[0]} AS ${this.quote(`${prefix}_${parts[1].replace(/"/g, "")}`)}`;
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
  protected buildSelections(
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
      const dbKey = this.getInternalKeyForAttribute(field);
      const sanitizedKey = this.sanitize(dbKey);
      result.push(
        `${this.quote(tableAlias)}.${this.quote(sanitizedKey)} AS ${this.quote(field)}`,
      );
    }

    if (this.$sharedTables) {
      result.push(
        `${this.quote(tableAlias)}.${this.quote("_tenant")} AS ${this.quote("$tenant")}`,
      );
    }

    return result;
  }

  /**
   * Builds JOIN condition based on relationship type
   */
  protected buildJoinCondition(
    relationType: RelationEnum,
    parentAlias: string,
    relationAlias: string,
    relationshipKey: string,
    twoWayKey: string = "",
    side: RelationSideEnum,
    junctionCollection: string,
  ): string | null {
    const parentUidCol = `${this.quote(parentAlias)}.${this.quote("_uid")}`;
    const relationUidCol = `${this.quote(relationAlias)}.${this.quote("_uid")}`;
    const parentRelCol = `${this.quote(parentAlias)}.${this.quote(this.sanitize(relationshipKey))}`;
    const relationRelCol = `${this.quote(relationAlias)}.${this.quote(this.sanitize(twoWayKey))}`;

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
        const junctionTable = this.getSQLTable(junctionCollection);
        const parentJoinKey = this.quote(this.sanitize(relationshipKey));
        const relationJoinKey = this.quote(this.sanitize(twoWayKey));

        return `EXISTS (
                    SELECT 1
                    FROM ${junctionTable} AS jt
                    WHERE jt.${parentJoinKey} = ${parentUidCol}
                      AND jt.${relationJoinKey} = ${relationUidCol}
                      ${this.getTenantQuery(junctionCollection)}
                )`;
      }
      default:
        return null;
    }
  }

  /**
   * Builds WHERE conditions from queries
   */
  protected buildWhereConditions(
    queries: Query[],
    tableAlias: string,
    collection: string,
  ): { conditions: string[]; params: any[] } {
    const conditions: string[] = [];
    const conditionParams: any[] = [];

    if (this.$sharedTables) {
      conditions.push(
        this.getTenantQuery(collection, tableAlias, undefined, ""),
      );
      conditionParams.push(this.$tenantId);
    }

    for (const query of queries) {
      const condition = this.buildQueryCondition(query, tableAlias);
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
  private buildQueryCondition(
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

    const dbKey = this.getInternalKeyForAttribute(attribute);

    let columnRef: string | undefined;

    // Handle JSON path operators (->, ->>)
    if (dbKey.includes("->") || dbKey.includes("->>")) {
      const parts = dbKey.split(/(->>|->)/);
      const mainColumn = parts[0]!;
      const sanitizedMainColumn = this.sanitize(mainColumn);
      const quotedMainColumn = `${this.quote(tableAlias)}.${this.quote(sanitizedMainColumn)}`;

      let pathExpression = quotedMainColumn;

      for (let i = 1; i < parts.length; i += 2) {
        const operator = parts[i]; // -> or ->>
        const path = parts[i + 1];

        if (path) {
          const sanitizedPath = this.sanitize(path);
          pathExpression += `${operator}'${sanitizedPath}'`;
        }
      }

      columnRef = pathExpression;
    } else if (![QueryType.And, QueryType.Or].includes(method)) {
      const sanitizedKey = this.sanitize(dbKey);
      columnRef = `${this.quote(tableAlias)}.${this.quote(sanitizedKey)}`;
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
          params.push(`%${this.escapeWildcards(values[0] as string)}%`);
        }
        break;

      case QueryType.StartsWith:
      case QueryType.NotStartsWith:
        sql = `${columnRef} LIKE ?`;
        params.push(`${this.escapeWildcards(values[0] as string)}%`);
        break;

      case QueryType.EndsWith:
      case QueryType.NotEndsWith:
        sql = `${columnRef} LIKE ?`;
        params.push(`%${this.escapeWildcards(values[0] as string)}`);
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
          this.buildQueryCondition(subQuery, tableAlias),
        );
        sql = `(${andConditions
          .map((c) => c.sql)
          .filter(Boolean)
          .join(" AND ")})`;
        andConditions.forEach((c) => params.push(...c.params));
        break;

      case QueryType.Or:
        const orConditions = (values as Query[]).map((subQuery) =>
          this.buildQueryCondition(subQuery, tableAlias),
        );
        sql = `(${orConditions
          .map((c) => c.sql)
          .filter(Boolean)
          .join(" OR ")})`;
        orConditions.forEach((c) => params.push(...c.params));
        break;
      case QueryType.Not:
        const notCondition = this.buildQueryCondition(
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
  protected buildOrderClause(
    orders: Record<string, OrderEnum>,
    tableAlias: string,
  ): string[] {
    const entries = Object.entries(orders);
    if (entries.length === 0) {
      // Default order by _id
      return [`${this.quote(tableAlias)}.${this.quote("_id")} ASC`];
    }

    const orderParts = entries.map(([attr, type]) => {
      const dbKey = this.getInternalKeyForAttribute(attr);
      const sanitizedKey = this.sanitize(dbKey);
      const orderType = type || "ASC";
      return `${this.quote(tableAlias)}.${this.quote(sanitizedKey)} ${orderType}`;
    });

    return orderParts;
  }

  /**
   * Builds cursor conditions for pagination
   */
  protected buildCursorConditions(
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
      const dbKey = this.getInternalKeyForAttribute(attr);
      const sanitizedKey = this.sanitize(dbKey);
      conditions.push(
        `${this.quote(tableAlias)}.${this.quote(sanitizedKey)} ${operator} ?`,
      );
      params.push(cursor.get(attr));
    } else {
      // multiple attributes
      for (let i = 0; i < orderAttributes.length; i++) {
        const attr = orderAttributes[i];
        if (!attr) continue;
        const dbKey = this.getInternalKeyForAttribute(attr);
        const sanitizedKey = this.sanitize(dbKey);

        const equalityConditions = orderAttributes
          .slice(0, i)
          .filter((prevAttr): prevAttr is string => prevAttr !== undefined)
          .map((prevAttr) => {
            const prevDbKey = this.getInternalKeyForAttribute(prevAttr);
            const prevSanitizedKey = this.sanitize(prevDbKey);
            params.push(cursor.get(prevAttr));
            return `${this.quote(tableAlias)}.${this.quote(prevSanitizedKey)} = ?`;
          });

        equalityConditions.push(
          `${this.quote(tableAlias)}.${this.quote(sanitizedKey)} ${operator} ?`,
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
}

export interface Meta {
  schema: string;
  sharedTables: boolean;
  tenantId: number;
  tenantPerDocument: boolean;
  namespace: string;
  metadata: Record<string, string>;
}
