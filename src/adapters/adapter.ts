import type { SQL } from "bun";
import { BaseAdapter } from "./base.js";
import { PostgresClient, Transaction } from "./postgres.js";
import {
  EventsEnum,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";
import { CreateCollectionOptions } from "./interface.js";
import { DatabaseException } from "@errors/base.js";
import { Database, ProcessedQuery } from "@core/database.js";
import { AuthContext } from "@core/auth.js";
import { Doc } from "@core/doc.js";
import {
  ColumnInfo,
  CreateAttribute,
  CreateIndex,
  UpdateAttribute,
} from "./types.js";
import { Ddl } from "./ddl.js";

export class Adapter extends BaseAdapter {
  protected client: PostgresClient | Transaction;

  /** Schema-plane DDL emitters — see ./ddl.ts. One-way dep: ddl never imports adapter. */
  private readonly ddl: Ddl;

  // Accepts a connection string, a pre-configured Bun `SQL` instance, or an
  // existing client handle (`PostgresClient`/`Transaction`) for transaction-
  // scoped adapters. Handles are detected via their `__type` discriminator
  // (same mechanism as BaseAdapter.$client) rather than `instanceof`, because
  // probing foreign objects against Bun's native `SQL` class throws
  // "instanceof called on an object with an invalid prototype property".
  constructor(client: SQL | string | PostgresClient | Transaction) {
    super();
    this.client =
      client !== null &&
      typeof client === "object" &&
      ((client as { __type?: string }).__type === "postgres" ||
        (client as { __type?: string }).__type === "transaction")
        ? (client as PostgresClient | Transaction)
        : new PostgresClient(client as SQL | string);

    // LIVE accessors (not snapshot values): setMeta() changes and
    // transaction-scoped adapters (copied metadata) must stay observable.
    const host = this;
    this.ddl = new Ddl({
      get $schema() { return host.$schema; },
      get $sharedTables() { return host.$sharedTables; },
      get $namespace() { return host.$namespace; },
      get $client() { return host.$client; },
      sanitize: (value) => host.sanitize(value),
      quote: (name) => host.quote(name),
      trigger: (event, query) => host.trigger(event, query),
      exists: (name, collection) => host.exists(name, collection),
      getSQLType: (type, size, array) => host.getSQLType(type, size, array),
      getSQLTable: (name) => host.getSQLTable(name),
      getSQLIndex: (table, name) => host.getSQLIndex(table, name),
      getInternalKeyForAttribute: (attribute) =>
        host.getInternalKeyForAttribute(attribute),
    });
  }

  async create(name: string): Promise<void> {
    await this.ddl.create(name);
  }

  async delete(name: string): Promise<void> {
    await this.ddl.delete(name);
  }

  async createCollection({
    name,
    attributes,
    indexes,
  }: CreateCollectionOptions): Promise<void> {
    await this.ddl.createCollection({ name, attributes, indexes });
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

  public async createAttribute({
    key: name,
    collection,
    size,
    array,
    type,
  }: CreateAttribute): Promise<void> {
    await this.ddl.createAttribute({ key: name, collection, size, array, type });
  }

  public async createAttributes(
    collection: string,
    attributes: Omit<CreateAttribute, "collection">[],
  ): Promise<void> {
    return this.ddl.createAttributes(collection, attributes);
  }

  public async renameAttribute(
    collection: string,
    oldName: string,
    newName: string,
  ): Promise<void> {
    return this.ddl.renameAttribute(collection, oldName, newName);
  }

  public async deleteAttribute(
    collection: string,
    name: string,
  ): Promise<void> {
    return this.ddl.deleteAttribute(collection, name);
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
    twoWay: boolean = false,
    id: string = "",
    twoWayKey: string = "",
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
    twoWay: boolean = false,
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
      twoWay,
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

  public async updateAttribute({
    collection,
    key: name,
    newName,
    array,
    size,
    type,
  }: UpdateAttribute): Promise<void> {
    await this.ddl.updateAttribute({
      collection,
      key: name,
      newName,
      array,
      size,
      type,
    });
  }

  public async renameIndex(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    return this.ddl.renameIndex(collectionId, oldName, newName);
  }

  public async createIndex({
    collection: collectionId,
    name,
    type,
    attributes,
    orders = [],
    attributeTypes = {},
  }: CreateIndex): Promise<boolean> {
    return this.ddl.createIndex({
      collection: collectionId,
      name,
      type,
      attributes,
      orders,
      attributeTypes,
    });
  }

  public async deleteIndex(collection: string, id: string): Promise<boolean> {
    return this.ddl.deleteIndex(collection, id);
  }

  /**
   * Creates a new document in the specified collection.
   * Returns the created document with its sequence ID set.
   */
  public async createDocument<D extends Doc>(
    ctx: AuthContext,
    collection: string,
    document: D,
  ): Promise<D> {
    try {
      const attributes: Record<string, any> = { ...document.getAll() };
      attributes["_createdAt"] = document.createdAt();
      attributes["_updatedAt"] = document.updatedAt();
      attributes["_permissions"] = document.getPermissions();

      if (this.$sharedTables) {
        attributes["_tenant"] = document.getTenant();
      }

      const name = this.sanitize(collection);
      const columns: string[] = [];
      const placeholders: string[] = [];
      const values: any[] = [];

      Object.entries(attributes).forEach(([attribute, value], idx) => {
        if (this.$internalAttrs.includes(attribute)) return;
        const column = this.sanitize(attribute);
        columns.push(this.quote(column));
        placeholders.push("?");
        values.push(value);
      });

      // Insert internal ID if set
      if (document.getSequence()) {
        columns.push("_id");
        placeholders.push("?");
        values.push(document.getSequence());
      }

      columns.push("_uid");
      placeholders.push("?");
      values.push(document.getId());

      let sql = `
                INSERT INTO ${this.getSQLTable(name)} (${columns.join(", ")})
                VALUES (${placeholders.join(", ")}) RETURNING _id
            `;

      sql = this.trigger(EventsEnum.DocumentCreate, sql);
      const { rows } = await this.client.query<{ _id: number }>(sql, values);

      // Set $sequence from insertId
      document.set("$sequence", rows[0]!._id);

      if (!rows[0]!._id) {
        throw new DatabaseException(
          'Error creating document empty "$sequence"',
        );
      }

      const permissions: any[] = [];
      for (const type of Database.PERMISSIONS || []) {
        const perms = document.getPermissionsByType(type);
        if (perms && perms.length) {
          const row: any[] = [type, perms, document.getSequence()];
          if (this.$sharedTables) {
            row.push(document.getTenant());
          }
          permissions.push(row);
        }
      }

      if (permissions.length) {
        const columnsPerm = ["_type", "_permissions", "_document"];
        if (this.$sharedTables) columnsPerm.push("_tenant");
        const placeholdersPerm =
          "(" + columnsPerm.map(() => "?").join(", ") + ")";
        const sqlPermissions = `
                    INSERT INTO ${this.getSQLTable(name + "_perms")} (${columnsPerm.join(", ")})
                    VALUES ${permissions.map(() => placeholdersPerm).join(", ")}
                `;
        const valuesPerm = permissions.flat();
        await this.client.query(sqlPermissions, valuesPerm);
      }

      return document;
    } catch (e: any) {
      throw this.processException(e, "Failed to create document");
    }
  }

  /**
   * Create multiple documents in a collection.
   */
  public async createDocuments<D extends Doc>(
    ctx: AuthContext,
    collection: string,
    documents: D[],
  ): Promise<D[]> {
    if (documents.length === 0) {
      return [];
    }

    const name = this.sanitize(collection);
    const allColumns = new Set<string>();
    const allValues: any[] = [];
    const documentRows: any[][] = [];

    // collect all unique columns and prepare document data
    for (const document of documents) {
      const attributes: Record<string, any> = { ...document.getAll() };
      attributes["_createdAt"] = document.createdAt();
      attributes["_updatedAt"] = document.updatedAt();
      attributes["_permissions"] = document.getPermissions();
      attributes["_uid"] = document.getId();

      if (this.$sharedTables) {
        attributes["_tenant"] = document.getTenant();
      }

      if (document.getSequence()) {
        attributes["_id"] = document.getSequence();
      }

      const rowData: Record<string, any> = {};

      Object.entries(attributes).forEach(([attribute, value]) => {
        if (this.$internalAttrs.includes(attribute)) return;
        const column = this.sanitize(attribute);
        allColumns.add(column);
        rowData[column] = value;
      });

      documentRows.push([document, rowData]);
    }

    const columns = Array.from(allColumns);
    const quotedColumns = columns.map((col) => this.quote(col));

    // build values array with consistent column order
    const valueRows: string[] = [];
    for (const [_, rowData] of documentRows) {
      const values: any[] = [];
      for (const column of columns) {
        values.push(rowData[column] ?? null);
        allValues.push(rowData[column] ?? null);
      }
      valueRows.push(`(${values.map(() => "?").join(", ")})`);
    }

    let sql = `
            INSERT INTO ${this.getSQLTable(name)} (${quotedColumns.join(", ")})
            VALUES ${valueRows.join(", ")}
            RETURNING _id, _uid
        `;

    sql = this.trigger(EventsEnum.DocumentCreate, sql);

    try {
      const { rows } = await this.client.query<{ _id: number; _uid: string }>(sql, allValues);

      // Set $sequence from returned IDs
      for (let i = 0; i < documents.length; i++) {
        documents[i]!.set("$sequence", rows[i]!._id);
      }

      // Handle permissions in batch
      const permissions: any[] = [];
      for (const document of documents) {
        for (const type of Database.PERMISSIONS || []) {
          const perms = document.getPermissionsByType(type);
          if (perms && perms.length) {
            const row: any[] = [type, perms, document.getSequence()];
            if (this.$sharedTables) {
              row.push(document.getTenant());
            }
            permissions.push(row);
          }
        }
      }

      if (permissions.length) {
        const columnsPerm = ["_type", "_permissions", "_document"];
        if (this.$sharedTables) columnsPerm.push("_tenant");
        const placeholdersPerm =
          "(" + columnsPerm.map(() => "?").join(", ") + ")";
        const sqlPermissions = `
                    INSERT INTO ${this.getSQLTable(name + "_perms")} (${columnsPerm.join(", ")})
                    VALUES ${permissions.map(() => placeholdersPerm).join(", ")}
                `;
        const valuesPerm = permissions.flat();
        await this.client.query(sqlPermissions, valuesPerm);
      }

      return documents;
    } catch (e: any) {
      throw this.processException(e, "Failed to create documents");
    }
  }

  /**
   * Updates an existing document in the specified collection.
   */
  public async updateDocument<D extends Doc>(
    ctx: AuthContext,
    collection: string,
    document: D,
    skipPermissions: boolean = false,
  ): Promise<D> {
    try {
      const attributes: Record<string, any> = { ...document.getAll() };
      attributes["_createdAt"] = document.createdAt();
      attributes["_updatedAt"] = document.updatedAt();
      attributes["_permissions"] = document.getPermissions();

      const name = this.sanitize(collection);
      let columns = "",
        permisionOperations: any[] = [];

      if (!skipPermissions) {
        permisionOperations = await this.updatePermissions(name, document);
      }

      // Update attributes
      const updateParams: any[] = [];
      const columnUpdates: string[] = [];

      for (const [attribute, value] of Object.entries(attributes)) {
        if (this.$internalAttrs.includes(attribute)) continue;

        const column = this.sanitize(attribute);
        columnUpdates.push(`${this.quote(column)} = ?`);
        updateParams.push(value);
      }

      columns = columnUpdates.join(", ");

      let sql = `
                    UPDATE ${this.getSQLTable(name)}
                    SET ${columns}, _uid = ?
                    WHERE _id = ?
                    ${this.getTenantQuery(collection)}
                `;

      sql = this.trigger(EventsEnum.DocumentUpdate, sql);

      updateParams.push(document.getId());
      updateParams.push(document.getSequence());
      if (this.$sharedTables) {
        updateParams.push(this.$tenantId);
      }

      await this.client.query(sql, updateParams);

      for (const operation of permisionOperations) {
        if (operation.sql) {
          await this.client.query(operation.sql, operation.params);
        }
      }
    } catch (e: any) {
      throw this.processException(e, "Failed to update document");
    }

    return document;
  }

  /**
   * Updates multiple documents in a collection with the same attributes.
   * Returns the number of affected rows.`
   */
  async updateDocuments(
    ctx: AuthContext,
    collection: string,
    updates: Doc<any>,
    documents: Doc[],
  ): Promise<number> {
    if (documents.length === 0) {
      return 0;
    }

    const attributes = updates.getAll();

    if (updates.updatedAt()) {
      attributes["_updatedAt"] = updates.updatedAt();
    }
    if (updates.createdAt()) {
      attributes["_createdAt"] = updates.createdAt();
    }
    if (updates.get("$permissions", []).length) {
      attributes["_permissions"] = updates.getPermissions();
    }

    if (Object.keys(attributes).length === 0) {
      return 0;
    }

    const columns: string[] = [];
    const updateValues: any[] = [];
    Object.keys(attributes).forEach((key) => {
      if ([...this.$internalAttrs, "$skipPermissionsUpdate"].includes(key))
        return;

      columns.push(`${this.quote(key)} = ?`);
      updateValues.push(attributes[key]);
    });

    const name = this.sanitize(collection);
    const sequences = documents.map((doc) => doc.getSequence());
    const sequencePlaceholders = sequences.map(() => "?").join(", ");
    const whereIn = `"_id" IN (${sequencePlaceholders})`;

    let sql = `
          UPDATE ${this.getSQLTable(name)}
          SET ${columns.join(", ")}
          WHERE ${whereIn}
          ${this.getTenantQuery(collection)}
        `;

    const allValues = [...updateValues, ...sequences];
    if (this.$sharedTables) {
      allValues.push(this.$tenantId);
    }

    const stmt = await this.client.query(sql, allValues);
    const affected = stmt.rowCount;

    if (updates.getPermissions().length) {
      for (const document of documents) {
        if (document.get("$skipPermissionsUpdate", false)) {
          continue;
        }
        const operations = await this.updatePermissions(collection, document);

        for (const { sql, params } of operations) {
          sql && (await this.client.query(sql, params));
        }
      }
    }

    return affected ?? 0;
  }

  /**
   * Deletes multiple documents from a collection.
   * Returns the number of affected rows.
   */
  public async deleteDocuments(
    ctx: AuthContext,
    collectionId: string,
    query: ProcessedQuery,
  ) {
    const name = this.sanitize(collectionId);
    const { populateQueries = [], filters, collection, ...options } = query;
    const mainTableAlias = "main";
    const collectionName = this.sanitize(collection.getId());
    const mainTable = this.getSQLTable(collectionName);

    const { params, ...conditions } = this.handleConditions({
      populateQueries,
      tableAlias: mainTableAlias,
      depth: 0,
      collection,
      filters,
      ...options,
      selections: [],
      forPermission: PermissionEnum.Delete,
      ctx,
    });

    const finalWhereClause =
      conditions.conditions.length > 0
        ? `WHERE ${conditions.conditions.join(" AND ")}`
        : "";

    const sql = `
            DELETE FROM ${mainTable} AS ${this.quote(mainTableAlias)}
            ${conditions.joins.join(" ")}
            ${finalWhereClause}
            RETURNING ${conditions.selectionsSql.join(", ")}
        `.trim();

    try {
      const { rows } = await this.client.query<{ $sequence: number; $id: string }>(sql, params);

      if (rows.length === 0) {
        return [];
      }

      const sequences = rows.map((row) => row.$sequence);
      const sequencePlaceholders = sequences.map(() => "?").join(", ");
      let permsSql = `
                DELETE FROM ${this.getSQLTable(name + "_perms")}
                WHERE "_document" IN (${sequencePlaceholders})
                ${this.getTenantQuery(collectionId)}
            `;

      const permsParams = [...sequences];
      if (this.$sharedTables) {
        params.push(this.$tenantId);
      }

      permsSql = this.trigger(EventsEnum.PermissionsDelete, permsSql);
      await this.client.query(permsSql, permsParams);

      return rows.map((r) => r.$id);
    } catch (e: any) {
      throw this.processException(
        e,
        `Failed to delete documents from collection '${collection.getId()}'`,
      );
    }
  }

  /**
   * Deletes multiple documents by their sequence IDs from a collection.
   * Returns the number of affected rows.
   */
  public async deleteDocumentsBySequences(
    ctx: AuthContext,
    collection: string,
    sequences: number[],
    permissionIds: string[],
  ): Promise<number> {
    if (sequences.length === 0) {
      return 0;
    }

    try {
      const name = this.sanitize(collection);

      const sequencePlaceholders = sequences.map(() => "?").join(", ");
      let sql = `
             DELETE FROM ${this.getSQLTable(name)}
             WHERE _id IN (${sequencePlaceholders})
             ${this.getTenantQuery(collection)}
          `;

      sql = this.trigger(EventsEnum.DocumentsDelete, sql);

      const params: any[] = [...sequences];
      if (this.$sharedTables) {
        params.push(this.$tenantId);
      }

      const stmt = await this.client.query(sql, params);

      if (permissionIds.length > 0) {
        const permissionPlaceholders = permissionIds.map(() => "?").join(", ");
        let permsSql = `
                DELETE FROM ${this.getSQLTable(name + "_perms")}
                WHERE _document IN (${permissionPlaceholders})
                ${this.getTenantQuery(collection)}
             `;

        permsSql = this.trigger(EventsEnum.PermissionsDelete, permsSql);

        const permsParams: any[] = [...permissionIds];
        if (this.$sharedTables) {
          permsParams.push(this.$tenantId);
        }

        await this.client.query(permsSql, permsParams);
      }

      return stmt.rowCount ?? 0;
    } catch (e: any) {
      throw this.processException(
        e,
        `Failed to delete documents from collection '${collection}'`,
      );
    }
  }

  /**
   * Creates or updates multiple documents in a collection with batch processing.
   * Handles incremental updates for a specific attribute and manages permissions.
   */
  public async createOrUpdateDocuments(
    ctx: AuthContext,
    collection: string,
    attribute: string,
    changes: Array<{ old: Doc; new: Doc }>,
  ): Promise<Doc[]> {
    if (changes.length === 0) {
      return changes.map((change) => change.new);
    }

    try {
      const name = this.sanitize(collection);
      const sanitizedAttribute = attribute
        ? this.sanitize(attribute)
        : attribute;

      let attributes: Record<string, any> = {};
      const batchKeys: string[] = [];
      const allValues: any[] = [];

      for (const change of changes) {
        const document = change.new;
        attributes = { ...document.getAll() };
        attributes["_uid"] = document.getId();
        attributes["_createdAt"] = document.createdAt();
        attributes["_updatedAt"] = document.updatedAt();
        attributes["_permissions"] = document.getPermissions();

        if (document.getSequence()) {
          attributes["_id"] = document.getSequence();
        }

        if (this.$sharedTables) {
          attributes["_tenant"] = document.getTenant();
        }

        const sortedKeys = Object.keys(attributes)
          .filter((a) => !this.$internalAttrs.includes(a))
          .sort();
        const bindKeys: string[] = [];
        for (const key of sortedKeys) {
          let value = attributes[key];
          bindKeys.push("?");
          allValues.push(value);
        }

        batchKeys.push(`(${bindKeys.join(", ")})`);
      }

      const sortedKeys = Object.keys(attributes)
        .filter((a) => !this.$internalAttrs.includes(a))
        .sort();
      const columns = `(${sortedKeys.map((key) => this.quote(this.sanitize(key))).join(", ")})`;

      const sql = this.getUpsertStatement(
        name,
        columns,
        batchKeys,
        attributes,
        sanitizedAttribute,
      );
      await this.client.query(sql, allValues);

      // Handle permission changes
      const operations: { sql: string; params: any[] }[] = [];

      for (let index = 0; index < changes.length; index++) {
        const change = changes[index]!;
        const oldDoc = change.old;
        const newDoc = change.new;

        // Get current permissions from old document
        const existingPermissions: Record<string, string[]> = {};
        for (const type of Database.PERMISSIONS || []) {
          existingPermissions[type] = oldDoc.getPermissionsByType(type);
        }

        // Process each permission type
        for (const type of Database.PERMISSIONS || []) {
          const newPermissions = newDoc.getPermissionsByType(type);
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
              const deleteParams: any[] = [newDoc.getSequence(), type];
              let deleteSql = `
                DELETE FROM ${this.getSQLTable(name + "_perms")}
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
                newDoc.getSequence(),
                type,
              ];
              let updateSql = `
                UPDATE ${this.getSQLTable(name + "_perms")}
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
                newDoc.getSequence(),
                type,
                newPermissions,
              ];
              let insertSql = `
                INSERT INTO ${this.getSQLTable(name + "_perms")} 
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
      }

      // Execute all permission operations
      for (const operation of operations) {
        await this.client.query(operation.sql, operation.params);
      }

      return changes.map((change) => change.new);
    } catch (e: any) {
      throw this.processException(
        e,
        `Failed to create or update documents in collection '${collection}'`,
      );
    }
  }

  /**
   * Finds documents in a collection based on a processed query.
   */
  public async find(
    ctx: AuthContext,
    collection: string,
    query: ProcessedQuery,
    {
      forPermission = PermissionEnum.Read,
      ...options
    }: {
      forPermission?: PermissionEnum;
    } = {},
  ): Promise<Record<string, any>[]> {
    const sqlResult = this.buildSql(query, { ...options, forPermission, ctx });

    try {
      const { rows } = await this.client.query(sqlResult.sql, sqlResult.params);
      return rows;
    } catch (e: any) {
      throw this.processException(
        e,
        `Failed to execute deep find query for collection '${collection}'`,
      );
    }
  }

  protected createTransactionAdapter(client: PostgresClient | Transaction): this {
    const adapter = new (this.constructor as any)(client) as this;

    // Own copies of mutable configuration so concurrent transactions cannot
    // clobber each other's metadata or SQL transformations.
    adapter._meta = { ...this._meta, metadata: { ...this._meta.metadata } };
    adapter.$logger = this.$logger;
    adapter.transformations = Object.fromEntries(
      Object.entries(this.transformations).map(([event, list]) => [
        event,
        [...(list ?? [])],
      ]),
    ) as typeof adapter.transformations;

    return adapter;
  }
}
