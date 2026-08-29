import type { Database as BunSQLiteDatabase } from "bun:sqlite";
import type { AuthContext } from "@core/auth.js";
import { Database, type PopulateQuery, type ProcessedQuery } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { Query } from "@core/query.js";
import {
  AttributeEnum,
  EventsEnum,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";
import { DatabaseException } from "@errors/base.js";
import { BaseAdapter } from "./base.js";
import { SQLiteDdl } from "./sqlite-ddl.js";
import { processSQLiteException } from "./sqlite-error-mapper.js";
import { SQLiteSqlBuilder } from "./sqlite-sql-builder.js";
import {
  decodeSQLiteRow,
  type SQLiteValueMetadata,
} from "./sqlite-values.js";
import {
  SQLiteClient,
  SQLiteTransaction,
  type SQLiteConfig,
} from "./sqlite.js";
import type {
  CreateCollectionOptions,
  DatabaseAdapter,
  QueryClient,
} from "./interface.js";
import type {
  ColumnInfo,
  CreateAttribute,
  CreateIndex,
  IncreaseDocumentAttribute,
  UpdateAttribute,
} from "./types.js";
import type { Entities } from "@nuvix/db";
import { QueryBuilder } from "@utils/query-builder.js";
import type { IEntity } from "types.js";
import type { Collection } from "@validators/schema.js";

export type SQLiteAdapterConfig =
  | SQLiteConfig
  | SQLiteClient
  | SQLiteTransaction;

/** Bun-native SQLite adapter for schema and document operations. */
export class SQLiteAdapter extends BaseAdapter implements DatabaseAdapter {
  public readonly type = "sqlite";
  public readonly $supportForSchemas = true;
  public readonly $supportForIndex = true;
  public readonly $supportForAttributes = true;
  public readonly $supportForUniqueIndex = true;
  public readonly $supportForFulltextIndex = false;
  public readonly $supportForUpdateLock = false;
  public readonly $supportForAttributeResizing = true;
  public readonly $supportForBatchOperations = false;
  public readonly $supportForGetConnectionId = false;
  public readonly $supportForHostname = false;
  public readonly $supportForCasting = true;
  public readonly $supportForNumericCasting = true;
  public readonly $supportForQueryContains = true;
  public readonly $supportForIndexArray = false;
  public readonly $supportForCastIndexArray = false;
  public readonly $supportForRelationships = true;
  public readonly $supportForReconnection = false;
  public readonly $supportForBatchCreateAttributes = true;
  public readonly $supportForJSONOverlaps = false;
  public readonly $supportForTimeouts = false;

  protected client: SQLiteClient | SQLiteTransaction;
  private readonly ddl: SQLiteDdl;

  constructor(client: SQLiteAdapterConfig) {
    super({ type: "sqlite" });
    this.client =
      client !== null &&
      typeof client === "object" &&
      ((client as { __type?: string }).__type === "sqlite" ||
        (client as { __type?: string }).__type === "transaction")
        ? (client as SQLiteClient | SQLiteTransaction)
        : new SQLiteClient(client as string | BunSQLiteDatabase);

    const host = this;
    this.ddl = new SQLiteDdl({
      get $schema() {
        return host.$schema;
      },
      get $sharedTables() {
        return host.$sharedTables;
      },
      get $namespace() {
        return host.$namespace;
      },
      get $client() {
        return host.$client;
      },
      sanitize: (value) => host.sanitize(value),
      quote: (name) => host.quote(name),
      trigger: (event, query) => host.trigger(event, query),
      getSQLType: (type, size, array) => host.getSQLType(type, size, array),
      getSQLTable: (name) => host.getSQLTable(name),
      getSQLIndex: (table, name) => host.getSQLIndex(table, name),
      getInternalKeyForAttribute: (attribute) =>
        host.getInternalKeyForAttribute(attribute),
    });
  }

  public async ping(): Promise<void> {
    await this.client.ping();
  }

  public async exists(name: string, collection?: string): Promise<boolean> {
    return this.ddl.exists(name, collection);
  }

  public async create(name: string): Promise<void> {
    await this.ddl.create(name);
  }

  public async delete(name: string): Promise<void> {
    await this.ddl.delete(name);
  }

  public async createCollection(options: CreateCollectionOptions): Promise<void> {
    await this.ddl.createCollection(options);
  }

  public async getSizeOfCollectionOnDisk(collection: string): Promise<number> {
    return this.ddl.getSizeOfCollectionOnDisk(collection);
  }

  public async getSizeOfCollection(collection: string): Promise<number> {
    return this.ddl.getSizeOfCollection(collection);
  }

  public async deleteCollection(id: string): Promise<void> {
    await this.ddl.deleteCollection(id);
  }

  public async analyzeCollection(collection: string): Promise<boolean> {
    return this.ddl.analyzeCollection(collection);
  }

  public async createAttribute(options: CreateAttribute): Promise<void> {
    await this.ddl.createAttribute(options);
  }

  public async createAttributes(
    collection: string,
    attributes: Omit<CreateAttribute, "collection">[],
  ): Promise<void> {
    await this.ddl.createAttributes(collection, attributes);
  }

  public async renameAttribute(
    collection: string,
    oldName: string,
    newName: string,
  ): Promise<void> {
    await this.ddl.renameAttribute(collection, oldName, newName);
  }

  public async deleteAttribute(collection: string, name: string): Promise<void> {
    await this.ddl.deleteAttribute(collection, name);
  }

  public async getSchemaAttributes(
    collection: string,
  ): Promise<Doc<ColumnInfo>[]> {
    return this.ddl.getSchemaAttributes(collection);
  }

  public async createRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay = false,
    id = "",
    twoWayKey = "",
  ): Promise<boolean> {
    return this.ddl.createRelationship(
      collection,
      relatedCollection,
      type,
      twoWay,
      id,
      twoWayKey,
    );
  }

  public async updateRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay: boolean | undefined,
    key: string,
    twoWayKey: string,
    side: RelationSideEnum,
    newKey?: string,
    newTwoWayKey?: string,
  ): Promise<boolean> {
    return this.ddl.updateRelationship(
      collection,
      relatedCollection,
      type,
      twoWay ?? false,
      key,
      twoWayKey,
      side,
      newKey,
      newTwoWayKey,
    );
  }

  public async deleteRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay: boolean,
    key: string,
    twoWayKey: string,
    side: RelationSideEnum,
  ): Promise<boolean> {
    return this.ddl.deleteRelationship(
      collection,
      relatedCollection,
      type,
      twoWay,
      key,
      twoWayKey,
      side,
    );
  }

  public async updateAttribute(options: UpdateAttribute): Promise<void> {
    await this.ddl.updateAttribute(options);
  }

  public async renameIndex(
    collection: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    return this.ddl.renameIndex(collection, oldName, newName);
  }

  public async createIndex(options: CreateIndex): Promise<boolean> {
    return this.ddl.createIndex(options);
  }

  public async deleteIndex(collection: string, id: string): Promise<boolean> {
    return this.ddl.deleteIndex(collection, id);
  }

  public async createDocument<D extends Doc>(
    _ctx: AuthContext,
    collection: string,
    document: D,
  ): Promise<D> {
    const name = this.sanitize(collection);

    try {
      const sequence = await this.client.transaction(async (tx) => {
        const attributes = this.documentAttributes(document);
        const columns = Object.keys(attributes);
        let sql = `INSERT INTO ${this.getSQLTable(name)} (${columns.map((column) => this.quote(column)).join(", ")}) VALUES (${columns.map(() => "?").join(", ")}) RETURNING "_id"`;
        sql = this.trigger(EventsEnum.DocumentCreate, sql);
        const { rows } = await tx.query<{ _id: number }>(
          sql,
          columns.map((column) => attributes[column]),
        );
        const id = rows[0]?._id;
        if (!id) {
          throw new DatabaseException('Error creating document empty "$sequence"');
        }
        await this.insertPermissions(tx, name, document, id);
        return id;
      });

      document.set("$sequence", sequence);
      return document;
    } catch (error) {
      return this.processException(error, "Failed to create document");
    }
  }

  public async createDocuments<D extends Doc>(
    _ctx: AuthContext,
    collection: string,
    documents: D[],
  ): Promise<D[]> {
    if (documents.length === 0) return [];

    const name = this.sanitize(collection);
    try {
      const sequences = await this.client.transaction(async (tx) => {
        const rows = documents.map((document) => this.documentAttributes(document));
        const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))].sort();
        const values = rows.flatMap((row) =>
          columns.map((column) => row[column] ?? null),
        );
        const groups = rows.map(
          () => `(${columns.map(() => "?").join(", ")})`,
        );
        let sql = `INSERT INTO ${this.getSQLTable(name)} (${columns.map((column) => this.quote(column)).join(", ")}) VALUES ${groups.join(", ")} RETURNING "_id", "_uid"${this.$sharedTables ? ', "_tenant"' : ""}`;
        sql = this.trigger(EventsEnum.DocumentsCreate, sql);
        const result = await tx.query<{
          _id: number;
          _uid: string;
          _tenant?: number;
        }>(sql, values);
        const byIdentity = new Map(
          result.rows.map((row) => [
            this.identity(row._uid, row._tenant),
            row._id,
          ]),
        );
        const assigned = documents.map((document) => {
          const sequence = byIdentity.get(
            this.identity(document.getId(), document.getTenant()),
          );
          if (!sequence) {
            throw new DatabaseException(
              `Error creating document '${document.getId()}' empty "$sequence"`,
            );
          }
          return sequence;
        });
        for (let index = 0; index < documents.length; index++) {
          await this.insertPermissions(tx, name, documents[index]!, assigned[index]!);
        }
        return assigned;
      });

      documents.forEach((document, index) =>
        document.set("$sequence", sequences[index]!),
      );
      return documents;
    } catch (error) {
      return this.processException(error, "Failed to create documents");
    }
  }

  public async updateDocument<D extends Doc>(
    _ctx: AuthContext,
    collection: string,
    document: D,
    skipPermissions = false,
  ): Promise<D> {
    const name = this.sanitize(collection);
    try {
      await this.client.transaction(async (tx) => {
        const attributes = this.documentAttributes(document, false);
        const columns = Object.keys(attributes).filter(
          (column) => column !== "_tenant" && column !== "_id",
        );
        const params = columns.map((column) => attributes[column]);
        params.push(document.getSequence());
        if (this.$sharedTables) params.push(this.$tenantId);
        let sql = `UPDATE ${this.getSQLTable(name)} SET ${columns.map((column) => `${this.quote(column)} = ?`).join(", ")} WHERE "_id" = ? ${this.getTenantQuery(collection)}`;
        sql = this.trigger(EventsEnum.DocumentUpdate, sql);
        const result = await tx.query(sql, params);
        if (result.rowCount > 0 && !skipPermissions) {
          await this.replacePermissions(tx, name, document);
        }
      });
      return document;
    } catch (error) {
      return this.processException(error, "Failed to update document");
    }
  }

  public async updateDocuments(
    _ctx: AuthContext,
    collection: string,
    updates: Doc,
    documents: Doc[],
  ): Promise<number> {
    if (documents.length === 0) return 0;

    const attributes: Record<string, unknown> = { ...updates.getAll() };
    if (updates.updatedAt()) attributes["_updatedAt"] = updates.updatedAt();
    if (updates.createdAt()) attributes["_createdAt"] = updates.createdAt();
    if (updates.has("$permissions")) {
      attributes["_permissions"] = updates.getPermissions();
    }
    const entries = Object.entries(attributes)
      .filter(([key]) => ![...this.$internalAttrs, "$skipPermissionsUpdate"].includes(key))
      .map(([key, value]) => [this.sanitize(key), value] as const);
    if (entries.length === 0) return 0;

    try {
      return await this.client.transaction(async (tx) => {
        const sequences = documents.map((document) => document.getSequence());
        const params: unknown[] = [
          ...entries.map(([, value]) => value),
          ...sequences,
        ];
        if (this.$sharedTables) params.push(this.$tenantId);
        let sql = `UPDATE ${this.getSQLTable(collection)} SET ${entries.map(([key]) => `${this.quote(key)} = ?`).join(", ")} WHERE "_id" IN (${sequences.map(() => "?").join(", ")}) ${this.getTenantQuery(collection)} RETURNING "_id"`;
        sql = this.trigger(EventsEnum.DocumentsUpdate, sql);
        const result = await tx.query<{ _id: number }>(sql, params);

        if (updates.has("$permissions")) {
          const updatedSequences = new Set(result.rows.map((row) => row._id));
          for (const document of documents) {
            if (
              updatedSequences.has(document.getSequence()) &&
              !document.get("$skipPermissionsUpdate", false)
            ) {
              await this.replacePermissions(tx, collection, document);
            }
          }
        }
        return result.rowCount;
      });
    } catch (error) {
      return this.processException(error, "Failed to update documents");
    }
  }

  public async deleteDocuments(
    ctx: AuthContext,
    collectionId: string,
    query: ProcessedQuery,
  ): Promise<string[]> {
    const name = this.sanitize(collectionId);
    try {
      return await this.client.transaction(async (tx) => {
        const conditions = this.handleConditions({
          ...query,
          populateQueries: query.populateQueries ?? [],
          tableAlias: "main",
          depth: 0,
          selections: [],
          forPermission: PermissionEnum.Delete,
          ctx,
        });
        const where = conditions.conditions.length
          ? `WHERE ${conditions.conditions.join(" AND ")}`
          : "";
        const selectionSql = `SELECT DISTINCT ${this.quote("main")}.${this.quote("_id")} AS ${this.quote("sequence")}, ${this.quote("main")}.${this.quote("_uid")} AS ${this.quote("id")} FROM ${this.getSQLTable(name)} AS ${this.quote("main")} ${conditions.joins.join(" ")} ${where}`;
        const selected = await tx.query<{ sequence: number; id: string }>(
          selectionSql,
          [...conditions.joinParams, ...conditions.params],
        );
        if (selected.rows.length === 0) return [];

        const sequences = selected.rows.map((row) => row.sequence);
        await this.deleteBySequences(tx, name, sequences);
        return selected.rows.map((row) => row.id);
      });
    } catch (error) {
      return this.processException(
        error,
        `Failed to delete documents from collection '${query.collection.getId()}'`,
      );
    }
  }

  public async deleteDocumentsBySequences(
    _ctx: AuthContext,
    collection: string,
    sequences: number[],
    _permissionIds: string[],
  ): Promise<number> {
    if (sequences.length === 0) return 0;
    try {
      return await this.client.transaction(async (tx) => {
        return this.deleteBySequences(tx, collection, sequences);
      });
    } catch (error) {
      return this.processException(
        error,
        `Failed to delete documents from collection '${collection}'`,
      );
    }
  }

  public async createOrUpdateDocuments(
    _ctx: AuthContext,
    collection: string,
    attribute: string,
    changes: Array<{ old: Doc; new: Doc }>,
  ): Promise<Doc[]> {
    if (changes.length === 0) return [];
    const name = this.sanitize(collection);

    try {
      const assignments = await this.client.transaction(async (tx) => {
        const rows = changes.map(({ new: document }) =>
          this.documentAttributes(document),
        );
        const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))].sort();
        const values = rows.flatMap((row) =>
          columns.map((column) => row[column] ?? null),
        );
        const groups = rows.map(
          () => `(${columns.map(() => "?").join(", ")})`,
        );
        const representative = Object.fromEntries(
          columns
            .filter((column) => !["_id", "_uid", "_tenant", "_createdAt"].includes(column))
            .map((column) => [column, null]),
        );
        let sql = SQLiteSqlBuilder.getUpsertStatement(
          this._meta,
          name,
          `(${columns.map((column) => this.quote(column)).join(", ")})`,
          groups,
          representative,
          attribute ? this.sanitize(attribute) : "",
          this.$internalAttrs,
        );
        sql += ` RETURNING "_id", "_uid"${this.$sharedTables ? ', "_tenant"' : ""}`;
        sql = this.trigger(EventsEnum.DocumentsUpsert, sql);
        const result = await tx.query<{
          _id: number;
          _uid: string;
          _tenant?: number;
        }>(sql, values);
        const byIdentity = new Map(
          result.rows.map((row) => [this.identity(row._uid, row._tenant), row._id]),
        );
        const sequences = changes.map(({ new: document }) => {
          const sequence = byIdentity.get(
            this.identity(document.getId(), document.getTenant()),
          );
          if (!sequence) {
            throw new DatabaseException(
              `Error upserting document '${document.getId()}' empty "$sequence"`,
            );
          }
          return sequence;
        });

        for (let index = 0; index < changes.length; index++) {
          const change = changes[index]!;
          await this.syncPermissions(
            tx,
            name,
            change.old,
            change.new,
            sequences[index]!,
          );
        }
        return sequences;
      });

      changes.forEach((change, index) =>
        change.new.set("$sequence", assignments[index]!),
      );
      return changes.map((change) => change.new);
    } catch (error) {
      return this.processException(
        error,
        `Failed to create or update documents in collection '${collection}'`,
      );
    }
  }

  public async find(
    ctx: AuthContext,
    collection: string,
    query: ProcessedQuery,
    { forPermission = PermissionEnum.Read }: { forPermission?: PermissionEnum } = {},
  ): Promise<Record<string, unknown>[]> {
    const built = this.buildSql(query, { forPermission, ctx });

    try {
      const { rows } = await this.client.query<Record<string, unknown>>(
        built.sql,
        built.params,
      );
      return rows.map((row) =>
        decodeSQLiteRow(row, this.queryMetadata(query)),
      );
    } catch (error) {
      return this.processException(
        error,
        `Failed to execute deep find query for collection '${collection}'`,
      );
    }
  }

  public async getDocument<C extends string & keyof Entities>(
    _ctx: AuthContext,
    _collection: C,
    _id: string,
    _queries?: ProcessedQuery | null,
    _forUpdate?: boolean,
  ): Promise<Doc<Entities[C]>>;
  public async getDocument<C extends Record<string, unknown>>(
    _ctx: AuthContext,
    _collection: string,
    _id: string,
    _queries?: ProcessedQuery | null,
    _forUpdate?: boolean,
  ): Promise<Doc<Partial<IEntity> & C>>;
  public async getDocument(
    ctx: AuthContext,
    collection: string,
    id: string,
    queries?: ProcessedQuery | null,
    forUpdate = false,
  ): Promise<Doc> {
    if (!collection || !id) {
      throw new DatabaseException(
        "Failed to get document: collection and id are required",
      );
    }
    if (!queries) {
      throw new DatabaseException(
        "Failed to get document: processed queries are required",
      );
    }

    const query: ProcessedQuery = {
      ...queries,
      filters: [Query.equal("$id", [id])],
      cursor: null,
      cursorDirection: null,
      limit: 1,
      offset: null,
    };

    try {
      const built = SQLiteSqlBuilder.buildSql(
        this._meta,
        query,
        {
          forPermission: PermissionEnum.Read,
          ctx,
          forUpdate: forUpdate && this.$supportForUpdateLock,
        },
        (value) => this.client.quote(value),
      );
      const { rows } = await this.client.query<Record<string, unknown>>(
        built.sql,
        built.params,
      );
      const row = rows[0];
      return new Doc(
        row ? decodeSQLiteRow(row, this.queryMetadata(query)) : undefined,
      );
    } catch (error) {
      return this.processException(
        error,
        `Failed to get document from collection '${collection}'`,
      );
    }
  }

  public async deleteDocument(
    _ctx: AuthContext,
    collection: string,
    document: Doc,
  ): Promise<boolean> {
    if (!collection || document.empty()) {
      throw new DatabaseException(
        "Failed to delete document: collection and id are required",
      );
    }
    try {
      return await this.client.transaction(async (tx) => {
        const params: unknown[] = [document.getId()];
        if (this.$sharedTables) params.push(this.$tenantId);
        let sql = `DELETE FROM ${this.getSQLTable(collection)} WHERE "_uid" = ? ${this.getTenantQuery(collection)} RETURNING "_id"`;
        sql = this.trigger(EventsEnum.DocumentDelete, sql);
        const result = await tx.query<{ _id: number }>(sql, params);
        if (result.rows.length === 0) return false;
        await this.deletePermissionSequences(
          tx,
          collection,
          result.rows.map((row) => row._id),
        );
        return true;
      });
    } catch (error) {
      return this.processException(error, "Failed to delete document");
    }
  }

  public async increaseDocumentAttribute(
    { collection, id, attribute, updatedAt, value, min, max }: IncreaseDocumentAttribute,
  ): Promise<boolean> {
    const column = this.quote(this.sanitize(attribute));
    const params: unknown[] = [value, updatedAt, id];
    let sql = `UPDATE ${this.getSQLTable(collection)} SET ${column} = ${column} + ?, "_updatedAt" = ? WHERE "_uid" = ? ${this.getTenantQuery(collection)}`;
    if (this.$sharedTables) params.push(this.$tenantId);
    if (max !== undefined && max !== null) {
      sql += ` AND ${column} <= ?`;
      params.push(max);
    }
    if (min !== undefined && min !== null) {
      sql += ` AND ${column} >= ?`;
      params.push(min);
    }
    sql = this.trigger(EventsEnum.DocumentUpdate, sql);

    try {
      const result = await this.client.query(sql, params);
      return result.rowCount > 0;
    } catch (error) {
      return this.processException(error, "Failed to increase document attribute");
    }
  }

  public async count(
    ctx: AuthContext,
    collection: string,
    queries: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    max?: number,
  ): Promise<number> {
    const query = this.aggregateQuery(collection, queries, max);
    const built = SQLiteSqlBuilder.buildAggregateSql(
      this._meta,
      "count",
      query,
      { forPermission: PermissionEnum.Read, ctx },
      (value) => this.client.quote(value),
    );
    const sql = this.trigger(EventsEnum.DocumentCount, built.sql);

    try {
      const { rows } = await this.client.query<{ sum?: number | string | null }>(
        sql,
        built.params,
      );
      return Number(rows[0]?.sum ?? 0);
    } catch (error) {
      return this.processException(error, "Failed to count documents");
    }
  }

  public async sum(
    ctx: AuthContext,
    collection: string,
    attribute: string,
    queries: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    max?: number,
  ): Promise<number> {
    const query = this.aggregateQuery(collection, queries, max);
    const column = this.sanitize(attribute);
    const built = SQLiteSqlBuilder.buildAggregateSql(
      this._meta,
      "sum",
      query,
      { forPermission: PermissionEnum.Read, ctx },
      (value) => this.client.quote(value),
      column,
    );
    const sql = this.trigger(EventsEnum.DocumentSum, built.sql);

    try {
      const { rows } = await this.client.query<{ sum?: number | string | null }>(
        sql,
        built.params,
      );
      return Number(rows[0]?.sum ?? 0);
    } catch (error) {
      return this.processException(error, "Failed to sum documents");
    }
  }

  public sanitize(value: string): string {
    return SQLiteSqlBuilder.sanitize(value);
  }

  public quote(name: string): string {
    return SQLiteSqlBuilder.quote(name);
  }

  protected getSQLType(
    type: AttributeEnum,
    size?: number,
    array?: boolean,
  ): string {
    return SQLiteSqlBuilder.getSQLType(type, size, array);
  }

  protected getSQLTable(name: string): string {
    return SQLiteSqlBuilder.getSQLTable(this._meta, name);
  }

  protected getSQLIndex(table: string, name: string): string {
    return SQLiteSqlBuilder.getSQLIndex(this._meta, table, name);
  }

  protected getTenantQuery(
    collection: string,
    alias = "",
    tenantCount = 0,
    condition = "AND",
  ): string {
    return SQLiteSqlBuilder.getTenantQuery(
      this._meta,
      collection,
      alias,
      tenantCount,
      condition,
    );
  }

  protected getInternalKeyForAttribute(attribute: string): string {
    return SQLiteSqlBuilder.getInternalKeyForAttribute(attribute);
  }

  public getJunctionTable(
    collection: number,
    relatedCollection: number,
    attribute: string,
    relatedAttribute: string,
  ): string {
    return SQLiteSqlBuilder.getJunctionTable(
      collection,
      relatedCollection,
      attribute,
      relatedAttribute,
    );
  }

  protected handleConditions(
    query: (ProcessedQuery | PopulateQuery) & {
      tableAlias?: string;
      depth: number;
      forPermission: PermissionEnum;
      ctx: AuthContext;
    },
  ) {
    return SQLiteSqlBuilder.handleConditions(this._meta, query, (value) =>
      this.client.quote(value),
    );
  }

  protected buildSql(
    query: ProcessedQuery,
    options: { forPermission: PermissionEnum; ctx: AuthContext },
  ): { sql: string; params: unknown[]; joins: string[]; selections: string[] } {
    return SQLiteSqlBuilder.buildSql(this._meta, query, options, (value) =>
      this.client.quote(value),
    );
  }

  protected processException(error: unknown, message?: string): never {
    return processSQLiteException(error, message);
  }

  private documentAttributes(
    document: Doc,
    includeSequence = true,
  ): Record<string, unknown> {
    const attributes: Record<string, unknown> = {};
    for (const [attribute, value] of Object.entries(document.getAll())) {
      if (this.$internalAttrs.includes(attribute)) continue;
      attributes[this.sanitize(attribute)] = value;
    }

    attributes["_uid"] = document.getId();
    attributes["_createdAt"] = document.createdAt();
    attributes["_updatedAt"] = document.updatedAt();
    attributes["_permissions"] = document.getPermissions();
    if (includeSequence && document.getSequence()) {
      attributes["_id"] = document.getSequence();
    }
    if (this.$sharedTables) {
      attributes["_tenant"] =
        document.getCollection() === Database.METADATA
          ? null
          : (document.getTenant() ?? this.$tenantId);
    }
    return attributes;
  }

  private async insertPermissions(
    client: QueryClient,
    collection: string,
    document: Doc,
    sequence = document.getSequence(),
  ): Promise<void> {
    const rows = Database.PERMISSIONS.flatMap((type) => {
      const permissions = document.getPermissionsByType(type);
      return permissions.length ? [{ type, permissions }] : [];
    });
    if (rows.length === 0) return;

    const columns = ["_document", "_type", "_permissions"];
    if (this.$sharedTables) columns.push("_tenant");
    const values = rows.flatMap(({ type, permissions }) => [
      sequence,
      type,
      permissions,
      ...(this.$sharedTables
        ? [document.getTenant() ?? this.$tenantId]
        : []),
    ]);
    const placeholders = rows.map(
      () => `(${columns.map(() => "?").join(", ")})`,
    );
    let sql = `INSERT INTO ${this.getSQLTable(`${collection}_perms`)} (${columns.map((column) => this.quote(column)).join(", ")}) VALUES ${placeholders.join(", ")}`;
    sql = this.trigger(EventsEnum.PermissionsCreate, sql);
    await client.query(sql, values);
  }

  private async replacePermissions(
    client: QueryClient,
    collection: string,
    document: Doc,
    sequence = document.getSequence(),
  ): Promise<void> {
    await this.deletePermissionSequences(
      client,
      collection,
      [sequence],
      document.getTenant(),
    );
    await this.insertPermissions(
      client,
      collection,
      document,
      sequence,
    );
  }

  private async syncPermissions(
    client: QueryClient,
    collection: string,
    oldDocument: Doc,
    newDocument: Doc,
    sequence = newDocument.getSequence(),
  ): Promise<void> {
    const tenant = newDocument.getTenant() ?? this.$tenantId;

    for (const type of Database.PERMISSIONS) {
      const previous = [...oldDocument.getPermissionsByType(type)].sort();
      const next = [...newDocument.getPermissionsByType(type)].sort();
      if (JSON.stringify(previous) === JSON.stringify(next)) continue;

      if (next.length === 0) {
        const params: unknown[] = [sequence, type];
        if (this.$sharedTables) params.push(tenant);
        let sql = `DELETE FROM ${this.getSQLTable(`${collection}_perms`)} WHERE "_document" = ? AND "_type" = ? ${this.getTenantQuery(collection)}`;
        sql = this.trigger(EventsEnum.PermissionsDelete, sql);
        await client.query(sql, params);
        continue;
      }

      if (previous.length === 0) {
        const columns = ["_document", "_type", "_permissions"];
        const params: unknown[] = [sequence, type, next];
        if (this.$sharedTables) {
          columns.push("_tenant");
          params.push(tenant);
        }
        let sql = `INSERT INTO ${this.getSQLTable(`${collection}_perms`)} (${columns.map((column) => this.quote(column)).join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`;
        sql = this.trigger(EventsEnum.PermissionsCreate, sql);
        await client.query(sql, params);
        continue;
      }

      const params: unknown[] = [next, sequence, type];
      if (this.$sharedTables) params.push(tenant);
      let sql = `UPDATE ${this.getSQLTable(`${collection}_perms`)} SET "_permissions" = ? WHERE "_document" = ? AND "_type" = ? ${this.getTenantQuery(collection)}`;
      sql = this.trigger(EventsEnum.PermissionsUpdate, sql);
      await client.query(sql, params);
    }
  }

  private async deleteBySequences(
    client: QueryClient,
    collection: string,
    sequences: number[],
  ): Promise<number> {
    if (sequences.length === 0) return 0;
    const params: unknown[] = [...sequences];
    if (this.$sharedTables) params.push(this.$tenantId);
    let sql = `DELETE FROM ${this.getSQLTable(this.sanitize(collection))} WHERE "_id" IN (${sequences.map(() => "?").join(", ")}) ${this.getTenantQuery(collection)}`;
    sql = this.trigger(EventsEnum.DocumentsDelete, sql);
    const result = await client.query(sql, params);
    await this.deletePermissionSequences(client, collection, sequences);
    return result.rowCount;
  }

  private async deletePermissionSequences(
    client: QueryClient,
    collection: string,
    sequences: Array<number | string>,
    tenant: number | null = this.$tenantId ?? null,
  ): Promise<void> {
    if (sequences.length === 0) return;
    const params: unknown[] = [...sequences];
    if (this.$sharedTables) params.push(tenant ?? this.$tenantId);
    let sql = `DELETE FROM ${this.getSQLTable(`${this.sanitize(collection)}_perms`)} WHERE "_document" IN (${sequences.map(() => "?").join(", ")}) ${this.getTenantQuery(collection)}`;
    sql = this.trigger(EventsEnum.PermissionsDelete, sql);
    await client.query(sql, params);
  }

  private identity(id: string, tenant?: number | null): string {
    return this.$sharedTables ? `${tenant ?? this.$tenantId}\0${id}` : id;
  }

  private aggregateQuery(
    collection: string,
    queries: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    max?: number,
  ): ProcessedQuery {
    const filters = Array.isArray(queries)
      ? queries
      : queries(new QueryBuilder()).build();
    return {
      collection: new Doc<Collection>({
        $id: this.sanitize(collection),
        attributes: [],
        documentSecurity: true,
      }),
      selections: [],
      populateQueries: [],
      filters,
      orders: {},
      cursor: null,
      cursorDirection: null,
      limit: max ?? null,
      offset: null,
      skipAuth: false,
    };
  }

  private queryMetadata(
    query: Pick<ProcessedQuery | PopulateQuery, "collection" | "populateQueries">,
    prefix = "",
  ): SQLiteValueMetadata[] {
    const internal: SQLiteValueMetadata[] = [
      { $id: "$id", type: AttributeEnum.String },
      { $id: "$sequence", type: AttributeEnum.Integer },
      { $id: "$createdAt", type: AttributeEnum.Timestamptz },
      { $id: "$updatedAt", type: AttributeEnum.Timestamptz },
      { $id: "$permissions", type: AttributeEnum.String, array: true },
      { $id: "$tenant", type: AttributeEnum.Integer },
    ];
    const attributes = query.collection
      .get("attributes", [])
      .map((attribute) => ({
        $id: attribute.get("key", attribute.getId()),
        type: attribute.get("type"),
        array: attribute.get("array", false),
      }));
    const current = [...internal, ...attributes].map((attribute) => ({
      ...attribute,
      $id: `${prefix}${attribute.$id}`,
    }));
    const populated = (query.populateQueries ?? []).flatMap((populate) => {
      if (!populate.authorized) return [];
      const relationship = query.collection
        .get("attributes", [])
        .find(
          (attribute) =>
            attribute.get("type") === AttributeEnum.Relationship &&
            attribute.get("key", attribute.getId()) === populate.attribute,
        );
      if (!relationship) return [];
      const key = relationship.get("key", relationship.getId());
      return this.queryMetadata(populate, `${prefix}${key}_`);
    });
    return [...current, ...populated];
  }

  protected createTransactionAdapter(client: QueryClient): this {
    if (client.__type !== "transaction") {
      throw new DatabaseException(
        "SQLite transaction adapter requires a transaction client",
      );
    }

    const adapter = new (this.constructor as new (
      client: SQLiteTransaction,
    ) => this)(client as SQLiteTransaction);
    adapter._meta = { ...this._meta, metadata: { ...this._meta.metadata } };
    adapter.$logger = this.$logger;
    adapter.transformations = Object.fromEntries(
      Object.entries(this.transformations).map(([event, transformations]) => [
        event,
        [...(transformations ?? [])],
      ]),
    ) as typeof adapter.transformations;
    return adapter;
  }
}
