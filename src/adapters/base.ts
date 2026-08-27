import { DatabaseException } from "@errors/base.js";
import { AttributeEnum, EventsEnum, PermissionEnum } from "@core/enums.js";
import { IncreaseDocumentAttribute } from "./types.js";
import { processException } from "./error-mapper.js";
import { Doc } from "@core/doc.js";
import { Database, PopulateQuery, ProcessedQuery } from "@core/database.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query } from "@core/query.js";
import type { Entities } from "@nuvix/db";
import type { IEntity } from "types.js";
import { Logger } from "@utils/logger.js";
import { AuthContext } from "@core/auth.js";
import { Collection } from "@validators/schema.js";
import type { DatabaseError, PostgresClient, Transaction } from "./postgres.js";
import { EventEmitter } from "node:events";
import { INTERNAL_ATTR_KEYS, SqlBuilder } from "./sql-builder.js";

// Local mirror of core/auth's private isSystemContext predicate (adapter-layer scope).
const isSystemContext = (ctx: AuthContext): boolean =>
  "system" in ctx && ctx.system === true;

export abstract class BaseAdapter extends EventEmitter {
  public readonly type: string = "base";
  protected _meta: Partial<Meta> = { schema: "public" };
  protected abstract client: PostgresClient | Transaction;
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

  protected transformations: Partial<Record<EventsEnum, Array<[string, (query: string) => string]>>> = {
    [EventsEnum.All]: [],
  };

  constructor(options: { type?: string } = {}) {
    super();
    if (options.type) this.type = options.type;
  }

  public get $database(): string {
    const database = this.client.database;
    if (!database)
      throw new DatabaseException(
        "Database name is not defined in client metadata.",
      );
    return database;
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
    return SqlBuilder.sanitize(value);
  }

  public async ping(): Promise<void> {
    if (this.$client.__type === "postgres") return this.$client.ping();
    else throw new DatabaseException("Cannot ping in transaction.");
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
        params.push(
          this.sanitize(name),
          SqlBuilder.getTableName(this._meta, collection),
        );
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
      this.processException(error, `Failed to check if ${collection ? "table" : "schema"} exists`);
    }
  }

  /**
   * Retrieves a document from the specified collection by its ID.
   */
  public async getDocument<C extends string & keyof Entities>(
    ctx: AuthContext,
    collection: C,
    id: string,
    queries?: ProcessedQuery | null,
    forUpdate?: boolean,
  ): Promise<Doc<Entities[C]>>;
  public async getDocument<C extends Record<string, any>>(
    ctx: AuthContext,
    collection: string,
    id: string,
    queries?: ProcessedQuery | null,
    forUpdate?: boolean,
  ): Promise<Doc<Partial<IEntity> & C>>;
  public async getDocument(
    ctx: AuthContext,
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
            SELECT ${SqlBuilder.getAttributeProjection(this._meta, selections, alias, collection)}
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
    ctx: AuthContext,
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
    if (min !== undefined && min !== null) {
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
    ctx: AuthContext,
    collection: string,
    queries: ((b: QueryBuilder) => QueryBuilder) | Array<Query> = [],
    max?: number,
  ): Promise<number> {
    const name = this.sanitize(collection);
    const roles = [...ctx.roles];
    const params: any[] = [];
    const where: string[] = [];
    const alias = Query.DEFAULT_ALIAS;

    const queryList = [
      ...(Array.isArray(queries) ? queries : queries(new QueryBuilder()).build()),
    ];

    const conditions = SqlBuilder.getSQLConditions(
      this._meta, queryList, params, "AND", this.$supportForJSONOverlaps,
    );
    if (conditions) {
      where.push(conditions);
    }

    if (!isSystemContext(ctx)) {
      where.push(
        SqlBuilder.getSQLPermissionsCondition(
          this._meta,
          { collection: name, roles, alias, type: PermissionEnum.Read },
          (value) => this.client.quote(value),
        ),
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
      // COUNT returns bigint, which drivers surface as a string — coerce.
      return result?.sum !== undefined && result?.sum !== null ? Number(result.sum) : 0;
    } catch (error) {
      throw this.processException(error, "Failed to count documents");
    }
  }

  /**
   * Sums a specific attribute across documents in a collection.
   */
  public async sum(
    ctx: AuthContext,
    collection: string,
    attribute: string,
    queries: ((b: QueryBuilder) => QueryBuilder) | Array<Query> = [],
    max?: number,
  ): Promise<number> {
    const name = this.sanitize(collection);
    const roles = [...ctx.roles];
    const params: any[] = [];
    const where: string[] = [];
    const alias = Query.DEFAULT_ALIAS;

    const queryList = [
      ...(Array.isArray(queries) ? queries : queries(new QueryBuilder()).build()),
    ];

    const conditions = SqlBuilder.getSQLConditions(
      this._meta, queryList, params, "AND", this.$supportForJSONOverlaps,
    );
    if (conditions) {
      where.push(conditions);
    }

    if (!isSystemContext(ctx)) {
      where.push(
        SqlBuilder.getSQLPermissionsCondition(
          this._meta,
          { collection: name, roles, alias, type: PermissionEnum.Read },
          (value) => this.client.quote(value),
        ),
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
      // SUM of numerics may surface as a string depending on driver parsing.
      return result?.sum !== undefined && result?.sum !== null ? Number(result.sum) : 0;
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

  /** Facade over SqlBuilder.getUpsertStatement (signature kept stable). */
  public getUpsertStatement(
    tableName: string,
    columns: string,
    batchKeys: string[],
    attributes: Record<string, any>,
    attribute: string = "",
  ): string {
    return SqlBuilder.getUpsertStatement(this._meta, tableName, columns, batchKeys, attributes, attribute, this.$internalAttrs);
  }

  protected getSQLType(type: AttributeEnum, size?: number, array?: boolean): string {
    return SqlBuilder.getSQLType(type, size, array);
  }

  protected processException(
    error: DatabaseError | unknown,
    message?: string,
  ): never {
    return processException(error, message);
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

  protected getSQLTable(name: string): string {
    return SqlBuilder.getSQLTable(this._meta, name);
  }

  protected getSQLIndex(table: string, name: string): string {
    return SqlBuilder.getSQLIndex(this._meta, table, name);
  }

  protected getTenantQuery(
    collection: string,
    alias: string = "",
    tenantCount: number = 0,
    condition: string = "AND",
  ): string {
    return SqlBuilder.getTenantQuery(this._meta, collection, alias, tenantCount, condition);
  }

  public quote(name: string): string {
    return SqlBuilder.quote(name);
  }

  protected getInternalKeyForAttribute(attribute: string): string {
    return SqlBuilder.getInternalKeyForAttribute(attribute);
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

  protected readonly $internalAttrs = INTERNAL_ATTR_KEYS;

  public getJunctionTable(coll: number, relColl: number, attr: string, relAttr: string): string {
    return SqlBuilder.getJunctionTable(coll, relColl, attr, relAttr);
  }

  protected handleConditions(
    query: (ProcessedQuery | PopulateQuery) & {
      tableAlias?: string;
      depth: number;
      forPermission: PermissionEnum;
      ctx: AuthContext;
    },
  ) {
    return SqlBuilder.handleConditions(this._meta, query, (value) =>
      this.client.quote(value),
    );
  }

  protected buildSql(
    query: ProcessedQuery,
    options: { forPermission: PermissionEnum; ctx: AuthContext },
  ): { sql: string; params: any[]; joins: string[]; selections: string[] } {
    return SqlBuilder.buildSql(this._meta, query, options, (value) =>
      this.client.quote(value),
    );
  }

  public async transaction<T>(callback: (tx: this) => Promise<T>): Promise<T> {
    return await this.client.transaction(async (newClient) => {
      // Nested call: Transaction.savepoint() reuses this same client
      // instance, so keep operating on the current adapter (savepoint
      // semantics).
      if (this.client === newClient) {
        return await callback(this);
      }

      // First call: construct a real adapter bound to the transaction
      // client. Unlike a prototype clone, the scoped adapter owns its
      // mutable state (metadata, transformations), so concurrent
      // transactions cannot leak state into each other.
      return await callback(this.createTransactionAdapter(newClient));
    });
  }

  protected abstract createTransactionAdapter(client: any): this;
}

export interface Meta {
  schema: string;
  sharedTables: boolean;
  tenantId: number;
  tenantPerDocument: boolean;
  namespace: string;
  metadata: Record<string, string>;
}
