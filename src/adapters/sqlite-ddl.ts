/** SQLite schema-plane DDL with logical schemas encoded in table prefixes. */
import { Doc } from "@core/doc.js";
import {
  AttributeEnum,
  EventsEnum,
  IndexEnum,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";
import { DatabaseException } from "@errors/base.js";
import type { Attribute } from "@validators/schema.js";
import type { Meta } from "./base.js";
import type { CreateCollectionOptions, QueryClient } from "./interface.js";
import { processSQLiteException } from "./sqlite-error-mapper.js";
import { SQLiteSqlBuilder } from "./sqlite-sql-builder.js";
import type {
  ColumnInfo,
  CreateAttribute,
  CreateIndex,
  UpdateAttribute,
} from "./types.js";

export interface SQLiteDdlContext {
  readonly $schema: string;
  readonly $sharedTables: boolean;
  readonly $namespace: string;
  readonly $client: QueryClient;
  sanitize(value: string): string;
  quote(name: string): string;
  trigger(event: EventsEnum, query: string): string;
  getSQLType(type: AttributeEnum, size?: number, array?: boolean): string;
  getSQLTable(name: string): string;
  getSQLIndex(table: string, name: string): string;
  getInternalKeyForAttribute(attribute: string): string;
}

type SQLiteColumnRow = {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
  hidden?: number;
};

type SQLiteIndexRow = {
  seq: number;
  name: string;
  unique: number;
  origin: "c" | "u" | "pk";
  partial: number;
};

type SQLiteIndexColumnRow = {
  seqno: number;
  cid: number;
  name: string | null;
  desc?: number;
  coll?: string;
  key?: number;
};

type SQLiteSchemaRow = {
  type: string;
  name: string;
  tbl_name: string;
  sql: string | null;
};

type SQLiteForeignKeyRow = {
  id: number;
  seq: number;
  table: string;
  from: string;
  to: string | null;
  on_update: string;
  on_delete: string;
  match: string;
};

type RebuildChange = {
  drop?: string;
  column?: string;
  type?: string;
};

const UNSAFE_REBUILD =
  "SQLite table rebuild is unsafe for this schema and was not performed";

/** Mirrors Ddl's narrow live-context shape without importing an adapter. */
export class SQLiteDdl {
  constructor(private readonly ctx: SQLiteDdlContext) {}

  async create(name: string): Promise<void> {
    const marker = SQLiteSqlBuilder.getSQLTable(this.meta(name), "__schema");
    const sql = this.ctx.trigger(
      EventsEnum.DatabaseCreate,
      `CREATE TABLE IF NOT EXISTS ${marker} ("_id" INTEGER PRIMARY KEY CHECK ("_id" = 1))`,
    );
    await this.run(sql, `Failed to create logical SQLite schema '${name}'`);
  }

  async delete(name: string): Promise<void> {
    const prefix = `${SQLiteSqlBuilder.getTablePrefix(this.meta(name))}_`;

    try {
      const { rows } = await this.ctx.$client.query<Pick<SQLiteSchemaRow, "name">>(
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND substr(name, 1, ?) = ? ORDER BY name DESC",
        [prefix.length, prefix],
      );
      await this.ctx.$client.transaction(async (tx) => {
        for (const row of rows) {
          await tx.query(`DROP TABLE IF EXISTS ${SQLiteSqlBuilder.quote(row.name)}`);
        }
      });
    } catch (error) {
      processSQLiteException(error, `Failed to delete logical SQLite schema '${name}'`);
    }
  }

  async exists(name: string, collection?: string): Promise<boolean> {
    if (!name?.trim()) {
      throw new DatabaseException("Name parameter is required and cannot be empty");
    }

    try {
      if (collection) {
        const table = SQLiteSqlBuilder.getTableName(this.meta(name), collection);
        const { rows } = await this.ctx.$client.query(
          "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ? LIMIT 1",
          [table],
        );
        return rows.length > 0;
      }

      const prefix = `${SQLiteSqlBuilder.getTablePrefix(this.meta(name))}_`;
      const { rows } = await this.ctx.$client.query(
        "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND substr(name, 1, ?) = ? LIMIT 1",
        [prefix.length, prefix],
      );
      return rows.length > 0;
    } catch (error) {
      processSQLiteException(error, "Failed to inspect the logical SQLite schema");
    }
  }

  async createCollection({
    name,
    attributes,
    indexes = [],
  }: CreateCollectionOptions): Promise<void> {
    const collection = this.ctx.sanitize(name);
    const table = this.ctx.getSQLTable(collection);
    const permissionsName = `${collection}_perms`;
    const permissionsTable = this.ctx.getSQLTable(permissionsName);
    const attributeMap = new Map<string, Attribute>();
    const columns = attributes.flatMap((attribute) => {
      const id = this.ctx.sanitize(attribute.getId());
      const value = attribute.toObject();
      attributeMap.set(id, value);
      if (!this.stored(attribute)) return [];
      return [
        `${this.ctx.quote(id)} ${this.ctx.getSQLType(value.type, value.size, value.array)}`,
      ];
    });

    // Validate every requested index before executing any DDL.
    const indexSql = indexes.map((index) => {
      const type = index.get("type");
      const indexAttributes = index.get("attributes", []);
      this.assertIndexSupported(type, indexAttributes, attributeMap);
      return this.indexSql(
        collection,
        index.getId(),
        type,
        indexAttributes,
        index.get("orders", []),
      );
    });

    const tenant = this.ctx.$sharedTables
      ? [`"_tenant" INTEGER${collection === "_metadata" ? "" : " NOT NULL"}`]
      : [];
    const mainColumns = [
      `"_id" INTEGER PRIMARY KEY AUTOINCREMENT`,
      ...tenant,
      `"_uid" TEXT NOT NULL`,
      `"_createdAt" TEXT DEFAULT NULL`,
      `"_updatedAt" TEXT DEFAULT NULL`,
      `"_permissions" TEXT NOT NULL DEFAULT '[]'`,
      ...columns,
    ];
    const permissionColumns = [
      `"_id" INTEGER PRIMARY KEY AUTOINCREMENT`,
      ...tenant,
      `"_type" TEXT NOT NULL`,
      `"_permissions" TEXT NOT NULL DEFAULT '[]'`,
      `"_document" INTEGER NOT NULL`,
      `FOREIGN KEY ("_document") REFERENCES ${table}("_id") ON DELETE CASCADE`,
    ];

    const tableSql = this.ctx.trigger(
      EventsEnum.CollectionCreate,
      `CREATE TABLE ${table} (${mainColumns.join(", ")})`,
    );
    const permissionsSql = this.ctx.trigger(
      EventsEnum.PermissionsCreate,
      `CREATE TABLE ${permissionsTable} (${permissionColumns.join(", ")})`,
    );
    const internalIndexes = this.internalIndexes(collection, table);

    try {
      await this.ctx.$client.transaction(async (tx) => {
        await tx.query(tableSql);
        for (const sql of internalIndexes) await tx.query(sql);
        for (const sql of indexSql) await tx.query(sql);
        await tx.query(permissionsSql);
        for (const sql of this.permissionIndexes(permissionsName, permissionsTable)) {
          await tx.query(sql);
        }
      });
    } catch (error) {
      processSQLiteException(error, `Failed to create collection '${name}'`);
    }
  }

  async getSizeOfCollectionOnDisk(collection: string): Promise<number> {
    return this.size(collection);
  }

  async getSizeOfCollection(collection: string): Promise<number> {
    return this.size(collection);
  }

  async deleteCollection(id: string): Promise<void> {
    const name = this.ctx.sanitize(id);
    const table = this.ctx.getSQLTable(name);
    const permissions = this.ctx.getSQLTable(`${name}_perms`);

    try {
      await this.ctx.$client.transaction(async (tx) => {
        await tx.query(
          this.ctx.trigger(
            EventsEnum.CollectionDelete,
            `DROP TABLE IF EXISTS ${permissions}`,
          ),
        );
        await tx.query(
          this.ctx.trigger(EventsEnum.CollectionDelete, `DROP TABLE IF EXISTS ${table}`),
        );
      });
    } catch (error) {
      processSQLiteException(error, `Failed to delete collection '${id}'`);
    }
  }

  async analyzeCollection(collection: string): Promise<boolean> {
    await this.run(
      `ANALYZE ${this.ctx.getSQLTable(this.ctx.sanitize(collection))}`,
      `Failed to analyze collection '${collection}'`,
    );
    return true;
  }

  async createAttribute(options: CreateAttribute): Promise<void> {
    this.assertAttribute(options);
    const sql = this.ctx.trigger(
      EventsEnum.AttributeCreate,
      `ALTER TABLE ${this.ctx.getSQLTable(options.collection)} ADD COLUMN ${this.ctx.quote(this.ctx.sanitize(options.key))} ${this.ctx.getSQLType(options.type, options.size, options.array)}`,
    );
    await this.run(sql, `Failed to create attribute '${options.key}'`);
  }

  async createAttributes(
    collection: string,
    attributes: Omit<CreateAttribute, "collection">[],
  ): Promise<void> {
    if (!collection || attributes.length === 0) {
      throw new DatabaseException(
        "Failed to create attributes: collection and attributes are required",
      );
    }
    attributes.forEach((attribute) =>
      this.assertAttribute({ ...attribute, collection }),
    );

    try {
      await this.ctx.$client.transaction(async (tx) => {
        for (const attribute of attributes) {
          const sql = `ALTER TABLE ${this.ctx.getSQLTable(collection)} ADD COLUMN ${this.ctx.quote(this.ctx.sanitize(attribute.key))} ${this.ctx.getSQLType(attribute.type, attribute.size, attribute.array)}`;
          await tx.query(this.ctx.trigger(EventsEnum.AttributesCreate, sql));
        }
      });
    } catch (error) {
      processSQLiteException(error, `Failed to create attributes in '${collection}'`);
    }
  }

  async renameAttribute(
    collection: string,
    oldName: string,
    newName: string,
  ): Promise<void> {
    this.assertNames(collection, oldName, newName);
    const sql = this.ctx.trigger(
      EventsEnum.AttributeUpdate,
      `ALTER TABLE ${this.ctx.getSQLTable(collection)} RENAME COLUMN ${this.ctx.quote(this.ctx.sanitize(oldName))} TO ${this.ctx.quote(this.ctx.sanitize(newName))}`,
    );
    await this.run(sql, `Failed to rename attribute '${oldName}'`);
  }

  async deleteAttribute(collection: string, name: string): Promise<void> {
    this.assertNames(collection, name);
    await this.rebuild(collection, { drop: this.ctx.sanitize(name) }, EventsEnum.AttributeDelete);
  }

  async getSchemaAttributes(collection: string): Promise<Doc<ColumnInfo>[]> {
    const table = this.tableName(collection);

    try {
      const { rows: columns } = await this.ctx.$client.query<SQLiteColumnRow>(
        "SELECT * FROM pragma_table_info(?) ORDER BY cid",
        [table],
      );
      const keys = await this.indexKeys(this.ctx.$client, table);
      return columns.map((column) =>
        Doc.from<ColumnInfo>({
          $id: column.name,
          columnDefault: column.dflt_value,
          isNullable: column.notnull === 1 || column.pk > 0 ? "NO" : "YES",
          dataType: this.normalizedType(column.type),
          characterMaximumLength: null,
          numericPrecision: null,
          numericScale: null,
          datetimePrecision: null,
          columnType: column.type.toLowerCase(),
          columnKey: column.pk > 0 ? "PRI" : (keys.get(column.name) ?? ""),
          extra:
            column.pk > 0 && column.type.toUpperCase() === "INTEGER"
              ? "auto_increment"
              : "",
        }),
      );
    } catch (error) {
      processSQLiteException(error, "Failed to get SQLite schema attributes");
    }
  }

  async createRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay = false,
    id = "",
    twoWayKey = "",
  ): Promise<boolean> {
    const changes = this.relationshipColumns(
      collection,
      relatedCollection,
      type,
      twoWay,
      id,
      twoWayKey,
    );
    if (changes.length === 0) return true;

    try {
      await this.ctx.$client.transaction(async (tx) => {
        for (const change of changes) {
          await tx.query(
            this.ctx.trigger(
              EventsEnum.RelationshipCreate,
              `ALTER TABLE ${this.ctx.getSQLTable(change.collection)} ADD COLUMN ${this.ctx.quote(change.key)} TEXT DEFAULT NULL`,
            ),
          );
        }
      });
      return true;
    } catch (error) {
      processSQLiteException(error, "Failed to create SQLite relationship");
    }
  }

  async updateRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay: boolean,
    key: string,
    twoWayKey: string,
    side: RelationSideEnum,
    newKey?: string,
    newTwoWayKey?: string,
  ): Promise<boolean> {
    const changes = this.relationshipTargets(
      collection,
      relatedCollection,
      type,
      twoWay,
      key,
      twoWayKey,
      side,
    ).filter((change) => {
      const replacement = change.key === this.ctx.sanitize(key) ? newKey : newTwoWayKey;
      return replacement && change.key !== this.ctx.sanitize(replacement);
    });
    if (changes.length === 0) return true;

    try {
      await this.ctx.$client.transaction(async (tx) => {
        for (const change of changes) {
          const replacement =
            change.key === this.ctx.sanitize(key) ? newKey : newTwoWayKey;
          await tx.query(
            this.ctx.trigger(
              EventsEnum.RelationshipUpdate,
              `ALTER TABLE ${this.ctx.getSQLTable(change.collection)} RENAME COLUMN ${this.ctx.quote(change.key)} TO ${this.ctx.quote(this.ctx.sanitize(replacement!))}`,
            ),
          );
        }
      });
      return true;
    } catch (error) {
      processSQLiteException(error, "Failed to update SQLite relationship");
    }
  }

  async deleteRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay: boolean,
    key: string,
    twoWayKey: string,
    side: RelationSideEnum,
  ): Promise<boolean> {
    const changes = this.relationshipTargets(
      collection,
      relatedCollection,
      type,
      twoWay,
      key,
      twoWayKey,
      side,
    );
    for (const change of changes) {
      await this.rebuild(
        change.collection,
        { drop: change.key },
        EventsEnum.RelationshipDelete,
      );
    }
    return true;
  }

  async updateAttribute(options: UpdateAttribute): Promise<void> {
    this.assertAttribute(options);
    const oldName = this.ctx.sanitize(options.key);
    const finalName = options.newName
      ? this.ctx.sanitize(options.newName)
      : oldName;
    const type = this.ctx.getSQLType(options.type, options.size, options.array);

    try {
      await this.ctx.$client.transaction(async (tx) => {
        if (oldName !== finalName) {
          await tx.query(
            this.ctx.trigger(
              EventsEnum.AttributeUpdate,
              `ALTER TABLE ${this.ctx.getSQLTable(options.collection)} RENAME COLUMN ${this.ctx.quote(oldName)} TO ${this.ctx.quote(finalName)}`,
            ),
          );
        }
        await this.rebuildWithClient(
          tx,
          options.collection,
          { column: finalName, type },
          EventsEnum.AttributeUpdate,
        );
      });
    } catch (error) {
      processSQLiteException(error, `Failed to update attribute '${options.key}'`);
    }
  }

  async renameIndex(
    collection: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    const oldIndex = this.indexName(collection, oldName);
    const newIndex = this.ctx.getSQLIndex(collection, this.ctx.sanitize(newName));

    try {
      const { rows } = await this.ctx.$client.query<Pick<SQLiteSchemaRow, "sql">>(
        "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
        [oldIndex],
      );
      const source = rows[0]?.sql;
      if (!source) return false;
      const renamed = source.replace(
        /^CREATE\s+(UNIQUE\s+)?INDEX\s+(?:"(?:[^"]|"")*"|\S+)/i,
        (_match, unique: string | undefined) =>
          `CREATE ${unique ?? ""}INDEX ${newIndex}`,
      );
      if (renamed === source) {
        throw new DatabaseException(`${UNSAFE_REBUILD}: index SQL cannot be reconstructed`);
      }
      await this.ctx.$client.transaction(async (tx) => {
        await tx.query(`DROP INDEX ${SQLiteSqlBuilder.quote(oldIndex)}`);
        await tx.query(this.ctx.trigger(EventsEnum.IndexRename, renamed));
      });
      return true;
    } catch (error) {
      processSQLiteException(error, `Failed to rename SQLite index '${oldName}'`);
    }
  }

  async createIndex(options: CreateIndex): Promise<boolean> {
    const map = new Map(Object.entries(options.attributeTypes));
    this.assertIndexSupported(options.type, options.attributes, map);
    const sql = this.ctx.trigger(
      EventsEnum.IndexCreate,
      this.indexSql(
        options.collection,
        options.name,
        options.type,
        options.attributes,
        options.orders ?? [],
      ),
    );
    await this.run(sql, `Failed to create SQLite index '${options.name}'`);
    return true;
  }

  async deleteIndex(collection: string, id: string): Promise<boolean> {
    const name = this.indexName(collection, id);
    const { rows } = await this.ctx.$client.query(
      "SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ? LIMIT 1",
      [name],
    );
    if (rows.length === 0) return false;
    await this.run(
      this.ctx.trigger(
        EventsEnum.IndexDelete,
        `DROP INDEX ${SQLiteSqlBuilder.quote(name)}`,
      ),
      `Failed to delete SQLite index '${id}'`,
    );
    return true;
  }

  private meta(schema = this.ctx.$schema): Partial<Meta> {
    return { schema, namespace: this.ctx.$namespace };
  }

  private tableName(collection: string): string {
    return SQLiteSqlBuilder.getTableName(this.meta(), this.ctx.sanitize(collection));
  }

  private indexName(collection: string, index: string): string {
    return SQLiteSqlBuilder.getIndexName(
      this.meta(),
      this.ctx.sanitize(collection),
      this.ctx.sanitize(index),
    );
  }

  private stored(attribute: Doc<Attribute>): boolean {
    const type = attribute.get("type");
    if (type === AttributeEnum.Virtual) return false;
    if (type !== AttributeEnum.Relationship) return true;
    const options = attribute.get("options", {}) as Record<string, unknown>;
    const relation = options["relationType"];
    const side = options["side"];
    const twoWay = options["twoWay"] === true;
    return !(
      relation === RelationEnum.ManyToMany ||
      (relation === RelationEnum.OneToOne && !twoWay && side === RelationSideEnum.Child) ||
      (relation === RelationEnum.OneToMany && side === RelationSideEnum.Parent) ||
      (relation === RelationEnum.ManyToOne && side === RelationSideEnum.Child)
    );
  }

  private internalIndexes(collection: string, table: string): string[] {
    const tenant = this.ctx.$sharedTables ? `"_tenant", ` : "";
    const suffix = this.ctx.$sharedTables ? "_tenant" : "";
    return [
      `CREATE UNIQUE INDEX ${this.ctx.getSQLIndex(collection, `uid${suffix}`)} ON ${table} (${tenant}"_uid")`,
      `CREATE INDEX ${this.ctx.getSQLIndex(collection, `created_at${suffix}`)} ON ${table} (${tenant}"_createdAt")`,
      `CREATE INDEX ${this.ctx.getSQLIndex(collection, `updated_at${suffix}`)} ON ${table} (${tenant}"_updatedAt")`,
    ];
  }

  private permissionIndexes(name: string, table: string): string[] {
    const columns = this.ctx.$sharedTables
      ? `"_tenant", "_document", "_type"`
      : `"_document", "_type"`;
    const result = [
      `CREATE UNIQUE INDEX ${this.ctx.getSQLIndex(name, "document_type")} ON ${table} (${columns})`,
      `CREATE INDEX ${this.ctx.getSQLIndex(name, "document")} ON ${table} ("_document")`,
    ];
    if (this.ctx.$sharedTables) {
      result.push(
        `CREATE INDEX ${this.ctx.getSQLIndex(name, "tenant")} ON ${table} ("_tenant")`,
      );
    }
    return result;
  }

  private assertIndexSupported(
    type: IndexEnum,
    attributes: string[],
    attributeTypes: Map<string, Attribute>,
  ): void {
    if (type === IndexEnum.FullText) {
      throw new DatabaseException(
        "SQLite fulltext indexes are not supported by this adapter",
      );
    }
    if (type !== IndexEnum.Key && type !== IndexEnum.Unique) {
      throw new DatabaseException(`Unsupported SQLite index type: ${type}`);
    }
    for (const attribute of attributes) {
      const metadata =
        attributeTypes.get(attribute) ?? attributeTypes.get(attribute.toLowerCase());
      if (!metadata) {
        throw new DatabaseException(
          `Attribute '${attribute}' not found in collection metadata.`,
        );
      }
      if (metadata.array) {
        throw new DatabaseException(
          "SQLite GIN-style array indexes are not supported by this adapter",
        );
      }
    }
  }

  private indexSql(
    collection: string,
    name: string,
    type: IndexEnum,
    attributes: string[],
    orders: (string | null)[],
  ): string {
    if (attributes.length === 0) {
      throw new DatabaseException("SQLite indexes require at least one attribute");
    }
    const keys = attributes.map((attribute, index) => {
      const key = this.ctx.quote(
        this.ctx.sanitize(this.ctx.getInternalKeyForAttribute(attribute)),
      );
      const order = orders[index];
      if (!order) return key;
      const normalized = order.toUpperCase();
      if (normalized !== "ASC" && normalized !== "DESC") {
        throw new DatabaseException(`Unsupported SQLite index order: ${order}`);
      }
      return `${key} ${normalized}`;
    });
    if (this.ctx.$sharedTables) keys.unshift('"_tenant"');
    const unique = type === IndexEnum.Unique ? "UNIQUE " : "";
    return `CREATE ${unique}INDEX ${this.ctx.getSQLIndex(collection, this.ctx.sanitize(name))} ON ${this.ctx.getSQLTable(collection)} (${keys.join(", ")})`;
  }

  private assertAttribute(options: CreateAttribute): void {
    if (!options.collection || !options.key || !options.type) {
      throw new DatabaseException(
        "Failed to alter attribute: collection, key, and type are required",
      );
    }
    this.ctx.sanitize(options.collection);
    this.ctx.sanitize(options.key);
    this.ctx.getSQLType(options.type, options.size, options.array);
  }

  private assertNames(...names: string[]): void {
    if (names.some((name) => !name)) {
      throw new DatabaseException("SQLite schema operation requires non-empty names");
    }
    names.forEach((name) => this.ctx.sanitize(name));
  }

  private relationshipColumns(
    collection: string,
    related: string,
    type: RelationEnum,
    twoWay: boolean,
    key: string,
    twoWayKey: string,
  ): Array<{ collection: string; key: string }> {
    this.assertNames(collection, related);
    switch (type) {
      case RelationEnum.OneToOne:
        return [
          { collection, key: this.ctx.sanitize(key) },
          ...(twoWay
            ? [{ collection: related, key: this.ctx.sanitize(twoWayKey) }]
            : []),
        ];
      case RelationEnum.OneToMany:
        return [{ collection: related, key: this.ctx.sanitize(twoWayKey) }];
      case RelationEnum.ManyToOne:
        return [{ collection, key: this.ctx.sanitize(key) }];
      case RelationEnum.ManyToMany:
        return [];
      default:
        throw new DatabaseException("Invalid relationship type");
    }
  }

  private relationshipTargets(
    collection: string,
    related: string,
    type: RelationEnum,
    twoWay: boolean,
    key: string,
    twoWayKey: string,
    side: RelationSideEnum,
  ): Array<{ collection: string; key: string }> {
    const first = { collection, key: this.ctx.sanitize(key) };
    const second = { collection: related, key: this.ctx.sanitize(twoWayKey) };
    switch (type) {
      case RelationEnum.OneToOne:
        if (side === RelationSideEnum.Parent) return twoWay ? [first, second] : [first];
        return twoWay ? [second, first] : [second];
      case RelationEnum.OneToMany:
        return side === RelationSideEnum.Parent ? [second] : [first];
      case RelationEnum.ManyToOne:
        return side === RelationSideEnum.Child ? [second] : [first];
      case RelationEnum.ManyToMany:
        return [];
      default:
        throw new DatabaseException("Invalid relationship type");
    }
  }

  private async indexKeys(
    client: QueryClient,
    table: string,
  ): Promise<Map<string, string>> {
    const { rows: indexes } = await client.query<SQLiteIndexRow>(
      "SELECT * FROM pragma_index_list(?) ORDER BY seq",
      [table],
    );
    const keys = new Map<string, string>();
    for (const index of indexes) {
      const { rows } = await client.query<SQLiteIndexColumnRow>(
        "SELECT * FROM pragma_index_info(?) ORDER BY seqno",
        [index.name],
      );
      for (const column of rows) {
        if (!column.name || keys.has(column.name)) continue;
        keys.set(column.name, index.unique === 1 ? "UNI" : "MUL");
      }
    }
    return keys;
  }

  private normalizedType(type: string): string {
    const upper = type.toUpperCase();
    if (upper.includes("INT")) return "integer";
    if (upper.includes("REAL") || upper.includes("FLOA") || upper.includes("DOUB")) {
      return "double precision";
    }
    if (upper.includes("TEXT") || upper.includes("CHAR") || upper.includes("CLOB")) {
      return "text";
    }
    if (upper.includes("BLOB") || upper === "") return "blob";
    return "numeric";
  }

  private async rebuild(
    collection: string,
    change: RebuildChange,
    event: EventsEnum,
  ): Promise<void> {
    try {
      await this.ctx.$client.transaction((tx) =>
        this.rebuildWithClient(tx, collection, change, event),
      );
    } catch (error) {
      processSQLiteException(error, `Failed to rebuild SQLite table '${collection}'`);
    }
  }

  private async rebuildWithClient(
    client: QueryClient,
    collection: string,
    change: RebuildChange,
    event: EventsEnum,
  ): Promise<void> {
    const tableName = this.tableName(collection);
    const table = SQLiteSqlBuilder.quote(tableName);
    const temporaryName = `${tableName}__nuvix_rebuild`;
    const temporary = SQLiteSqlBuilder.quote(temporaryName);
    const { rows: schemaRows } = await client.query<SQLiteSchemaRow>(
      "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE (type = 'table' AND name = ?) OR (tbl_name = ? AND type IN ('index', 'trigger')) OR (type = 'view' AND instr(sql, ?) > 0)",
      [tableName, tableName, tableName],
    );
    const tableSql = schemaRows.find((row) => row.type === "table")?.sql;
    if (!tableSql) throw new DatabaseException(`SQLite table '${collection}' not found`);
    this.assertRebuildSchema(schemaRows, tableSql);

    const { rows: columns } = await client.query<SQLiteColumnRow>(
      "SELECT * FROM pragma_table_xinfo(?) ORDER BY cid",
      [tableName],
    );
    if (columns.some((column) => (column.hidden ?? 0) !== 0)) {
      throw new DatabaseException(`${UNSAFE_REBUILD}: generated or hidden columns`);
    }
    const kept = columns.filter((column) => column.name !== change.drop);
    if (kept.length === columns.length && change.drop) {
      throw new DatabaseException(`SQLite column '${change.drop}' not found`);
    }
    const { rows: foreignKeys } = await client.query<SQLiteForeignKeyRow>(
      "SELECT * FROM pragma_foreign_key_list(?) ORDER BY id, seq",
      [tableName],
    );
    if (foreignKeys.some((foreignKey) => foreignKey.table === tableName)) {
      throw new DatabaseException(`${UNSAFE_REBUILD}: self-referencing foreign key`);
    }
    await this.assertInboundForeignKeys(client, tableName);

    const { rows: indexes } = await client.query<SQLiteIndexRow>(
      "SELECT * FROM pragma_index_list(?) ORDER BY seq DESC",
      [tableName],
    );
    if (indexes.some((index) => index.origin === "u")) {
      throw new DatabaseException(`${UNSAFE_REBUILD}: table-level UNIQUE constraint`);
    }
    const recreate = await this.rebuildIndexes(client, indexes, change.drop);
    const definitions = this.columnDefinitions(kept, change, tableSql);
    const constraints = this.foreignKeyDefinitions(foreignKeys, change.drop);
    const create = this.ctx.trigger(
      event,
      `CREATE TABLE ${temporary} (${definitions.concat(constraints).join(", ")})`,
    );
    const destinations = kept.map((column) => SQLiteSqlBuilder.quote(column.name));
    const selections = kept.map((column) => {
      const quoted = SQLiteSqlBuilder.quote(column.name);
      return column.name === change.column && change.type
        ? `CAST(${quoted} AS ${change.type})`
        : quoted;
    });

    await client.query(create);
    await client.query(
      `INSERT INTO ${temporary} (${destinations.join(", ")}) SELECT ${selections.join(", ")} FROM ${table}`,
    );
    await client.query(`DROP TABLE ${table}`);
    await client.query(`ALTER TABLE ${temporary} RENAME TO ${table}`);
    for (const sql of recreate) await client.query(sql);
  }

  private assertRebuildSchema(rows: SQLiteSchemaRow[], tableSql: string): void {
    if (rows.some((row) => row.type === "trigger" || row.type === "view")) {
      throw new DatabaseException(`${UNSAFE_REBUILD}: dependent trigger or view`);
    }
    if (/\b(?:CHECK|GENERATED|WITHOUT\s+ROWID|STRICT)\b/i.test(tableSql)) {
      throw new DatabaseException(`${UNSAFE_REBUILD}: unsupported table definition`);
    }
  }

  private async assertInboundForeignKeys(
    client: QueryClient,
    tableName: string,
  ): Promise<void> {
    const { rows: enabled } = await client.query<{ foreign_keys: number }>(
      "PRAGMA foreign_keys",
    );
    if (enabled[0]?.foreign_keys !== 1) return;
    const { rows: tables } = await client.query<Pick<SQLiteSchemaRow, "name">>(
      "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    );
    for (const table of tables) {
      if (table.name === tableName) continue;
      const { rows } = await client.query<SQLiteForeignKeyRow>(
        "SELECT * FROM pragma_foreign_key_list(?)",
        [table.name],
      );
      if (rows.some((foreignKey) => foreignKey.table === tableName)) {
        throw new DatabaseException(
          `${UNSAFE_REBUILD}: foreign key enforcement is enabled and '${table.name}' references '${tableName}'`,
        );
      }
    }
  }

  private columnDefinitions(
    columns: SQLiteColumnRow[],
    change: RebuildChange,
    sourceSql: string,
  ): string[] {
    const primary = columns.filter((column) => column.pk > 0);
    const autoincrement = /\bAUTOINCREMENT\b/i.test(sourceSql);
    const definitions = columns.map((column) => {
      const type = column.name === change.column && change.type
        ? change.type
        : column.type;
      const parts = [SQLiteSqlBuilder.quote(column.name), type || "BLOB"];
      if (primary.length === 1 && column.pk > 0) parts.push("PRIMARY KEY");
      if (primary.length === 1 && column.pk > 0 && autoincrement) {
        if (type.toUpperCase() !== "INTEGER") {
          throw new DatabaseException(
            `${UNSAFE_REBUILD}: AUTOINCREMENT key must remain INTEGER`,
          );
        }
        parts.push("AUTOINCREMENT");
      }
      if (column.notnull === 1 && column.pk === 0) parts.push("NOT NULL");
      if (column.dflt_value !== null) parts.push(`DEFAULT ${column.dflt_value}`);
      return parts.join(" ");
    });
    if (primary.length > 1) {
      const keys = primary
        .sort((left, right) => left.pk - right.pk)
        .map((column) => SQLiteSqlBuilder.quote(column.name));
      definitions.push(`PRIMARY KEY (${keys.join(", ")})`);
    }
    return definitions;
  }

  private foreignKeyDefinitions(
    rows: SQLiteForeignKeyRow[],
    dropped?: string,
  ): string[] {
    const groups = new Map<number, SQLiteForeignKeyRow[]>();
    for (const row of rows) {
      const group = groups.get(row.id) ?? [];
      group.push(row);
      groups.set(row.id, group);
    }
    return Array.from(groups.values()).map((group) => {
      if (group.some((row) => row.from === dropped)) {
        throw new DatabaseException(
          `${UNSAFE_REBUILD}: dropped column participates in a foreign key`,
        );
      }
      const ordered = group.sort((left, right) => left.seq - right.seq);
      const from = ordered.map((row) => SQLiteSqlBuilder.quote(row.from));
      const to = ordered.map((row) => {
        if (!row.to) {
          throw new DatabaseException(`${UNSAFE_REBUILD}: implicit foreign-key target`);
        }
        return SQLiteSqlBuilder.quote(row.to);
      });
      const first = ordered[0]!;
      return `FOREIGN KEY (${from.join(", ")}) REFERENCES ${SQLiteSqlBuilder.quote(first.table)} (${to.join(", ")}) ON UPDATE ${first.on_update} ON DELETE ${first.on_delete} MATCH ${first.match}`;
    });
  }

  private async rebuildIndexes(
    client: QueryClient,
    indexes: SQLiteIndexRow[],
    dropped?: string,
  ): Promise<string[]> {
    const statements: string[] = [];
    for (const index of indexes) {
      if (index.origin !== "c") continue;
      const { rows: columns } = await client.query<SQLiteIndexColumnRow>(
        "SELECT * FROM pragma_index_xinfo(?) ORDER BY seqno",
        [index.name],
      );
      const keyColumns = columns.filter((column) => column.key !== 0);
      if (dropped && keyColumns.some((column) => column.name === dropped)) continue;
      if (keyColumns.some((column) => column.name === null)) {
        throw new DatabaseException(`${UNSAFE_REBUILD}: expression index`);
      }
      const { rows } = await client.query<Pick<SQLiteSchemaRow, "sql">>(
        "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
        [index.name],
      );
      if (!rows[0]?.sql) {
        throw new DatabaseException(`${UNSAFE_REBUILD}: index SQL unavailable`);
      }
      statements.push(rows[0].sql);
    }
    return statements;
  }

  private async size(collection: string): Promise<number> {
    const table = this.tableName(collection);
    const permissions = this.tableName(`${collection}_perms`);
    try {
      const { rows } = await this.ctx.$client.query<{ size: number | null }>(
        "SELECT sum(pgsize) AS size FROM dbstat WHERE name IN (?, ?)",
        [table, permissions],
      );
      return Number(rows[0]?.size ?? 0);
    } catch (error) {
      processSQLiteException(error, `Failed to get size of collection '${collection}'`);
    }
  }

  private async run(sql: string, message: string): Promise<void> {
    try {
      await this.ctx.$client.query(sql);
    } catch (error) {
      processSQLiteException(error, message);
    }
  }
}
