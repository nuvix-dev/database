/**
 * Schema-plane SQL emitters for the PostgreSQL adapter.
 *
 * Extracted from Adapter so that every schema statement — database
 * creation/deletion, collection tables (+ permissions tables and their
 * indexes), attribute ALTERs, relationship columns, index management and
 * schema introspection queries — lives in one cohesive module. Adapter keeps
 * the runtime document plane and delegates to this collaborator; its public
 * API is unchanged.
 *
 * The module deliberately does NOT import Adapter (or BaseAdapter): it
 * receives everything it needs through `DdlContext`, a narrow structural
 * interface of bound accessors supplied by the Adapter constructor. This
 * keeps dependency flow one-way (adapter -> ddl) with no import cycle, and
 * makes the emitters testable in isolation.
 *
 * Bodies were moved verbatim from Adapter; emitted SQL is byte-identical.
 */
import {
  AttributeEnum,
  EventsEnum,
  IndexEnum,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";
import { CreateCollectionOptions } from "./interface.js";
import { DatabaseException } from "@errors/base.js";
import { Database } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { Attribute } from "@validators/schema.js";
import { processException } from "./error-mapper.js";
import { PostgresClient, Transaction } from "./postgres.js";
import {
  ColumnInfo,
  CreateAttribute,
  CreateIndex,
  UpdateAttribute,
} from "./types.js";

/**
 * Everything the DDL emitters need from their hosting adapter: live meta
 * getters ($schema/$sharedTables/$namespace/$client) and the shared SQL
 * helper set. Bound per adapter instance, so transaction-scoped adapters
 * (which copy transformations/metadata) always observe their own state.
 */
export interface DdlContext {
  readonly $schema: string;
  readonly $sharedTables: boolean;
  readonly $namespace: string;
  readonly $client: PostgresClient | Transaction;
  sanitize(value: string): string;
  quote(name: string): string;
  trigger(event: EventsEnum, query: string): string;
  exists(name: string, collection?: string): Promise<boolean>;
  getSQLType(type: AttributeEnum, size?: number, array?: boolean): string;
  getSQLTable(name: string): string;
  getSQLIndex(table: string, name: string): string;
  getInternalKeyForAttribute(attribute: string): string;
}

export class Ddl {
  constructor(private readonly ctx: DdlContext) {}

  async create(name: string): Promise<void> {
    name = this.ctx.quote(name);
    if (await this.ctx.exists(name)) return;

    let sql = `CREATE SCHEMA ${name};`;
    sql = this.ctx.trigger(EventsEnum.DatabaseCreate, sql);

    await this.ctx.$client.query(sql);
  }

  async delete(name: string): Promise<void> {
    name = this.ctx.quote(name);
    await this.ctx.$client.query(`DROP SCHEMA IF EXISTS ${name} CASCADE;`);
  }

  async createCollection({
    name,
    attributes,
    indexes,
  }: CreateCollectionOptions): Promise<void> {
    name = this.ctx.sanitize(name);
    const mainTable = this.ctx.getSQLTable(name);
    const attributeSql: string[] = [];
    const indexSql: string[] = [];
    const attributeHash: Record<string, Attribute> = {};

    attributes.forEach((attribute) => {
      const id = this.ctx.sanitize(attribute.getId());
      if (attribute.get("type") === AttributeEnum.Virtual) {
        return;
      }

      if (attribute.get("type") === AttributeEnum.Relationship) {
        const options = attribute.get("options", {}) as Record<string, any>;
        const relationType = options["relationType"] ?? null;
        const twoWay = options["twoWay"] ?? false;
        const side = options["side"] ?? null;

        if (
          relationType === RelationEnum.ManyToMany ||
          (relationType === RelationEnum.OneToOne &&
            !twoWay &&
            side === "child") ||
          (relationType === RelationEnum.OneToMany &&
            side === RelationSideEnum.Parent) ||
          (relationType === RelationEnum.ManyToOne &&
            side === RelationSideEnum.Child)
        ) {
          return;
        }
      }

      attributeHash[id] = attribute.toObject();
      const type = this.ctx.getSQLType(
        attribute.get("type"),
        attribute.get("size"),
        attribute.get("array"),
      );

      let sql = `${this.ctx.quote(id)} ${type}`;
      attributeSql.push(sql);
    });

    indexes?.forEach((index) => {
      const indexId = index.getId();
      const indexType = index.get("type");
      const indexAttributes = index.get("attributes") as string[];
      const orders = index.get("orders") || [];

      const isFulltext = indexType === IndexEnum.FullText;
      const hasArrayAttribute = indexAttributes.some((attrKey) => {
        const metadata = attributeHash[attrKey];
        return metadata?.array;
      });

      let usingClause = "";
      if (isFulltext || hasArrayAttribute) {
        usingClause = "USING GIN";
      }

      const formattedIndexAttributes = indexAttributes.map(
        (attributeKey, i) => {
          const pgKey = `"${this.ctx.sanitize(this.ctx.getInternalKeyForAttribute(attributeKey))}"`;
          const order = orders[i] && !isFulltext ? ` ${orders[i]}` : "";

          if (isFulltext) {
            return `to_tsvector('english', ${pgKey})`;
          }

          return `${pgKey}${order}`;
        },
      );

      // For multi-column full-text indexes, we must join the `to_tsvector` calls
      let attributesForSql = formattedIndexAttributes.join(", ");
      if (isFulltext && formattedIndexAttributes.length > 1) {
        attributesForSql = formattedIndexAttributes.join(" || ");
      }

      if (this.ctx.$sharedTables && !isFulltext) {
        const pgTenantKey = `"${this.ctx.sanitize("_tenant")}"`;
        attributesForSql = `${pgTenantKey}, ${attributesForSql}`;
      }

      const uniqueClause = isFulltext
        ? ""
        : indexType === IndexEnum.Unique
          ? "UNIQUE "
          : "";

      const pgIndexId = this.ctx.getSQLIndex(name, this.ctx.sanitize(indexId));
      const sql = `CREATE ${uniqueClause}INDEX ${pgIndexId} ON ${mainTable} ${usingClause} (${attributesForSql});`;

      indexSql.push(sql);
    });

    const mainTableColumns = [
      `"_id" BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY`,
      `"_uid" VARCHAR(255) NOT NULL`,
      `"_createdAt" TIMESTAMP WITH TIME ZONE DEFAULT NULL`,
      `"_updatedAt" TIMESTAMP WITH TIME ZONE DEFAULT NULL`,
      `"_permissions" TEXT[] DEFAULT '{}'`,
      ...attributeSql,
    ];

    let primaryKeyDefinition: string;
    const tenantCol = this.ctx.quote("_tenant");

    if (this.ctx.$sharedTables) {
      mainTableColumns.splice(1, 0, `${tenantCol} BIGINT DEFAULT NULL`);
      primaryKeyDefinition = `PRIMARY KEY ("_id", ${tenantCol})`;
    } else {
      primaryKeyDefinition = `PRIMARY KEY ("_id")`;
    }

    const columnsAndConstraints = mainTableColumns.join(",\n");
    let tableSql = `
            CREATE TABLE ${mainTable} (
                ${columnsAndConstraints},
                ${primaryKeyDefinition}
            );
        `;

    const postTableIndexes: string[] = [];
    if (this.ctx.$sharedTables) {
      postTableIndexes.push(
        `CREATE UNIQUE INDEX ${this.ctx.getSQLIndex(name, "uid_tenant")} ON ${mainTable} ("_uid", ${tenantCol});`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.ctx.getSQLIndex(name, "created_at_tenant")} ON ${mainTable} (${tenantCol}, "_createdAt");`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.ctx.getSQLIndex(name, "updated_at_tenant")} ON ${mainTable} (${tenantCol}, "_updatedAt");`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.ctx.getSQLIndex(name, "tenant_id")} ${mainTable} (${tenantCol}, "_id");`,
      );
    } else {
      postTableIndexes.push(
        `CREATE UNIQUE INDEX ${this.ctx.getSQLIndex(name, "uid")} ON ${mainTable} ("_uid");`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.ctx.getSQLIndex(name, "created_at")} ON ${mainTable} ("_createdAt");`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.ctx.getSQLIndex(name, "updated_at")} ON ${mainTable} ("_updatedAt");`,
      );
    }
    postTableIndexes.push(
      `CREATE INDEX ${this.ctx.getSQLIndex(name, "permissions_gin_idx")} ON ${mainTable} USING GIN ("_permissions");`,
    );

    tableSql = this.ctx.trigger(EventsEnum.CollectionCreate, tableSql);

    const permissionsTableName = this.ctx.getSQLTable(name + "_perms");

    const permissionsTableColumns = [
      `"_id" BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY`,
      `"_type" VARCHAR(12) NOT NULL`,
      `"_permissions" TEXT[] NOT NULL DEFAULT '{}'`,
      `"_document" BIGINT NOT NULL`,
      `FOREIGN KEY ("_document") REFERENCES ${mainTable}("_id") ON DELETE CASCADE`,
    ];
    const postPermissionsTableIndexes: string[] = [];
    let permissionsPrimaryKeyDefinition: string;

    if (this.ctx.$sharedTables) {
      permissionsTableColumns.splice(1, 0, `${tenantCol} BIGINT DEFAULT NULL`);
      permissionsPrimaryKeyDefinition = `PRIMARY KEY ("_id", ${tenantCol})`;

      postPermissionsTableIndexes.push(
        `CREATE UNIQUE INDEX ${this.ctx.getSQLIndex(`${name}_perms`, "index1")} ON ${permissionsTableName} ("_document", ${tenantCol}, "_type");`,
      );
      postPermissionsTableIndexes.push(
        `CREATE INDEX ${this.ctx.getSQLIndex(`${name}_perms`, "tenant")} ON ${permissionsTableName} (${tenantCol});`,
      );
    } else {
      permissionsPrimaryKeyDefinition = `PRIMARY KEY ("_id")`;
      postPermissionsTableIndexes.push(
        `CREATE UNIQUE INDEX ${this.ctx.getSQLIndex(`${name}_perms`, "index1")} ON ${permissionsTableName} ("_document", "_type");`,
      );
    }
    postPermissionsTableIndexes.push(
      `CREATE INDEX ${this.ctx.getSQLIndex(`${name}_perms`, "permissions_gin_idx")} ON ${permissionsTableName} USING GIN ("_permissions");`,
    );

    const permissionsColumnsAndConstraints =
      permissionsTableColumns.join(",\n");
    let permissionsTable = `
            CREATE TABLE ${permissionsTableName} (
                ${permissionsColumnsAndConstraints},
                ${permissionsPrimaryKeyDefinition}
            );
        `;

    permissionsTable = this.ctx.trigger(
      EventsEnum.PermissionsCreate,
      permissionsTable,
    );

    try {
      const callback = async (tx: Transaction | PostgresClient) => {
        await tx.query(tableSql);
        for (const sql of postTableIndexes) {
          await tx.query(sql);
        }

        for (const sql of indexSql) {
          await tx.query(sql);
        }

        await tx.query(permissionsTable);
        for (const sql of postPermissionsTableIndexes) {
          await tx.query(sql);
        }
      };

      const client = this.ctx.$client;
      if (client.__type === "postgres") {
        await client.transaction(callback);
      } else {
        callback(client);
      }
    } catch (error) {
      processException(error);
    }
  }

  public async getSizeOfCollectionOnDisk(collection: string): Promise<number> {
    collection = this.ctx.sanitize(collection);
    const collectionTableName = `'${this.ctx.$namespace}_${collection}'`;
    const permissionsTableName = `'${this.ctx.$namespace}_${collection}_perms'`;

    const sql = `
            SELECT
                pg_total_relation_size(${collectionTableName}::regclass) AS collection_size,
                pg_total_relation_size(${permissionsTableName}::regclass) AS permissions_size;
        `;

    try {
      const [rows]: any = await this.ctx.$client.query(sql);
      const collectionSize = Number(rows[0]?.["collection_size"] ?? 0);
      const permissionsSize = Number(rows[0]?.["permissions_size"] ?? 0);
      return collectionSize + permissionsSize;
    } catch (e: any) {
      if (
        e.message.includes("relation") &&
        e.message.includes("does not exist")
      ) {
        return 0;
      }
      processException(
        e,
        `Failed to get size of collection ${collection} on disk: ${e.message}`,
      );
    }
  }

  public async getSizeOfCollection(collection: string): Promise<number> {
    collection = this.ctx.sanitize(collection);
    const collectionTableName = `'${this.ctx.$namespace}_${collection}'`;
    const permissionsTableName = `'${this.ctx.$namespace}_${collection}_perms'`;

    const sql = `
            SELECT
            pg_table_size(${collectionTableName}::regclass) + pg_indexes_size(${collectionTableName}::regclass) AS collection_size,
            pg_table_size(${permissionsTableName}::regclass) + pg_indexes_size(${permissionsTableName}::regclass) AS permissions_size;
        `;

    try {
      const { rows } = await this.ctx.$client.query(sql);
      const collectionSize = Number(rows[0]?.["collection_size"] ?? 0);
      const permissionsSize = Number(rows[0]?.["permissions_size"] ?? 0);
      return collectionSize + permissionsSize;
    } catch (e: any) {
      if (
        e.message.includes("relation") &&
        e.message.includes("does not exist")
      ) {
        return 0;
      }
      processException(
        e,
        `Failed to get size of collection ${collection}: ${e.message}`,
      );
    }
  }

  public async deleteCollection(id: string): Promise<void> {
    const permissionsTableName = this.ctx.getSQLTable(
      this.ctx.sanitize(id + "_perms"),
    );
    const collectionTableName = this.ctx.getSQLTable(this.ctx.sanitize(id));

    let dropPermsSql = `DROP TABLE IF EXISTS ${permissionsTableName} CASCADE;`;
    dropPermsSql = this.ctx.trigger(EventsEnum.CollectionDelete, dropPermsSql);

    let dropCollectionSql = `DROP TABLE IF EXISTS ${collectionTableName} CASCADE;`;
    dropCollectionSql = this.ctx.trigger(
      EventsEnum.CollectionDelete,
      dropCollectionSql,
    );

    try {
      await this.ctx.$client.query(dropPermsSql);
      await this.ctx.$client.query(dropCollectionSql);
    } catch (e: any) {
      processException(e, `Failed to delete collection ${id}`);
    }
  }

  public async analyzeCollection(collection: string): Promise<boolean> {
    const name = this.ctx.sanitize(collection);
    const tableName = this.ctx.getSQLTable(name);

    const sql = `ANALYZE ${tableName}`;

    try {
      await this.ctx.$client.query(sql);
      return true;
    } catch (e: any) {
      processException(e, `Failed to analyze collection ${collection}`);
    }
  }

  public async createAttribute({
    key: name,
    collection,
    size,
    array,
    type,
  }: CreateAttribute): Promise<void> {
    if (!name || !collection || !type) {
      throw new DatabaseException(
        "Failed to create attribute: name, collection, and type are required",
      );
    }

    const sqlType = this.ctx.getSQLType(type, size, array);
    const table = this.ctx.getSQLTable(collection);

    let sql = `
                ALTER TABLE ${table}
                ADD COLUMN ${this.ctx.quote(name)} ${sqlType}
            `;
    sql = this.ctx.trigger(EventsEnum.AttributeCreate, sql);

    try {
      await this.ctx.$client.query(sql);
    } catch (e: any) {
      processException(
        e,
        `Failed to create attribute '${name}' in collection '${collection}'`,
      );
    }
  }

  public async createAttributes(
    collection: string,
    attributes: Omit<CreateAttribute, "collection">[],
  ): Promise<void> {
    if (!Array.isArray(attributes) || attributes.length === 0) {
      throw new DatabaseException(
        "Failed to create attributes: attributes must be a non-empty array",
      );
    }

    const parts: string[] = [];

    for (const attr of attributes) {
      if (!attr.key || !attr.type) {
        throw new DatabaseException(
          "Failed to create attribute: name and type are required",
        );
      }

      const sqlType = this.ctx.getSQLType(attr.type, attr.size, attr.array);
      parts.push(`${this.ctx.quote(attr.key)} ${sqlType}`);
    }

    const columns = parts.join(", ADD COLUMN ");
    const table = this.ctx.getSQLTable(collection);
    let sql = `
                ALTER TABLE ${table}
                ADD COLUMN ${columns}
            `;

    sql = this.ctx.trigger(EventsEnum.AttributesCreate, sql);

    try {
      await this.ctx.$client.query(sql);
    } catch (e: any) {
      processException(
        e,
        `Failed to create attributes in collection '${collection}'`,
      );
    }
  }

  public async renameAttribute(
    collection: string,
    oldName: string,
    newName: string,
  ): Promise<void> {
    if (!oldName || !newName || !collection) {
      throw new DatabaseException(
        "Failed to rename attribute: oldName, newName, and collection are required",
      );
    }

    const table = this.ctx.getSQLTable(collection);
    let sql = `
                ALTER TABLE ${table}
                RENAME COLUMN ${this.ctx.quote(oldName)} TO ${this.ctx.quote(newName)}
            `;

    sql = this.ctx.trigger(EventsEnum.AttributeUpdate, sql);

    try {
      await this.ctx.$client.query(sql);
    } catch (e: any) {
      processException(
        e,
        `Failed to rename attribute '${oldName}' to '${newName}' in collection '${collection}'`,
      );
    }
  }

  public async deleteAttribute(
    collection: string,
    name: string,
  ): Promise<void> {
    if (!name || !collection) {
      throw new DatabaseException(
        "Failed to delete attribute: name and collection are required",
      );
    }

    const table = this.ctx.getSQLTable(collection);
    let sql = `
                ALTER TABLE ${table}
                DROP COLUMN ${this.ctx.quote(name)}
            `;

    sql = this.ctx.trigger(EventsEnum.AttributeDelete, sql);

    try {
      await this.ctx.$client.query(sql);
    } catch (e: any) {
      processException(
        e,
        `Failed to delete attribute '${name}' from collection '${collection}'`,
      );
    }
  }

  public async getSchemaAttributes(
    collection: string,
  ): Promise<Doc<ColumnInfo>[]> {
    const schema = this.ctx.$schema;
    const table = `${this.ctx.$namespace}_${this.ctx.sanitize(collection)}`;

    const sql = `
            SELECT
                cols.column_name AS "$id",
                pg_get_expr(def.adbin, def.adrelid) AS "columnDefault",
                cols.is_nullable AS "isNullable",
                cols.data_type AS "dataType",
                cols.character_maximum_length AS "characterMaximumLength",
                cols.numeric_precision AS "numericPrecision",
                cols.numeric_scale AS "numericScale",
                cols.datetime_precision AS "datetimePrecision",
                cols.udt_name AS "udtName",
                att.attidentity AS "identityFlag",
                CASE WHEN pk.constraint_type = 'PRIMARY KEY' THEN 'PRI' ELSE '' END AS "columnKey"
            FROM
                information_schema.columns AS cols
            JOIN
                pg_class AS cls ON cls.relname = $1
            JOIN
                pg_namespace AS ns ON ns.oid = cls.relnamespace AND ns.nspname = $2
            LEFT JOIN
                pg_attribute AS att ON att.attrelid = cls.oid AND att.attname = cols.column_name
            LEFT JOIN
                pg_attrdef AS def ON def.adrelid = cls.oid AND def.adnum = att.attnum
            LEFT JOIN (
                SELECT
                    kcu.column_name,
                    tc.constraint_type
                FROM
                    information_schema.table_constraints AS tc
                JOIN
                    information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                    AND tc.table_name = kcu.table_name
                WHERE
                    tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = $2
                    AND tc.table_name = $1
            ) AS pk ON pk.column_name = cols.column_name
            WHERE
                cols.table_schema = $2
                AND cols.table_name = $1
            ORDER BY
                cols.ordinal_position;
        `;

    try {
      const result: any = await this.ctx.$client.query(sql, [table, schema]);

      return result.rows.map((row: any) => {
        row.isNullable = row.isNullable === "YES" ? "YES" : "NO";
        if (row.udtName?.startsWith("_")) {
          row.dataType = row.udtName.slice(1) + "[]";
        }
        switch (row.dataType) {
          case "int4":
            row.dataType = "integer";
            break;
          case "int8":
            row.dataType = "bigint";
            break;
          case "float8":
            row.dataType = "double precision";
            break;
          case "bool":
            row.dataType = "boolean";
            break;
          case "timestamptz":
            row.dataType = "timestamptz";
            break;
          case "jsonb":
            row.dataType = "json";
            break;
          case "uuid":
            row.dataType = "uuid";
            break;
          default:
            break;
        }
        row.extra =
          row.identityFlag === "a" || row.identityFlag === "d"
            ? "auto_increment"
            : "";
        delete row.identityFlag;

        return Doc.from(row);
      });
    } catch (e: any) {
      processException(e, "Failed to get schema attributes");
    }
  }

  public async createRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay: boolean = false,
    id: string = "",
    twoWayKey: string = "",
  ): Promise<boolean> {
    const name = this.ctx.sanitize(collection);
    const relatedName = this.ctx.sanitize(relatedCollection);
    const table = this.ctx.getSQLTable(name);
    const relatedTable = this.ctx.getSQLTable(relatedName);
    const sanitizedId = this.ctx.sanitize(id);
    const sanitizedTwoWayKey = this.ctx.sanitize(twoWayKey);
    const sqlType = this.ctx.getSQLType(AttributeEnum.Relationship, 0, false);

    let sql: string;

    switch (type) {
      case RelationEnum.OneToOne:
        sql = `
                    ALTER TABLE ${table} 
                    ADD COLUMN ${this.ctx.quote(sanitizedId)} ${sqlType} DEFAULT NULL;
                `;

        if (twoWay) {
          sql += `
                        ALTER TABLE ${relatedTable} 
                        ADD COLUMN ${this.ctx.quote(sanitizedTwoWayKey)} ${sqlType} DEFAULT NULL;
                    `;
        }
        break;

      case RelationEnum.OneToMany:
        sql = `
                    ALTER TABLE ${relatedTable} 
                    ADD COLUMN ${this.ctx.quote(sanitizedTwoWayKey)} ${sqlType} DEFAULT NULL;
                `;
        break;

      case RelationEnum.ManyToOne:
        sql = `
                    ALTER TABLE ${table} 
                    ADD COLUMN ${this.ctx.quote(sanitizedId)} ${sqlType} DEFAULT NULL;
                `;
        break;

      case RelationEnum.ManyToMany:
        return true;

      default:
        throw new DatabaseException("Invalid relationship type");
    }

    sql = this.ctx.trigger(EventsEnum.AttributeCreate, sql);

    try {
      await this.ctx.$client.query(sql);
      return true;
    } catch (e: any) {
      processException(
        e,
        `Failed to create relationship between '${collection}' and '${relatedCollection}'`,
      );
    }
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
    const name = this.ctx.sanitize(collection);
    const relatedName = this.ctx.sanitize(relatedCollection);
    const table = this.ctx.getSQLTable(name);
    const relatedTable = this.ctx.getSQLTable(relatedName);
    const sanitizedKey = this.ctx.sanitize(key);
    const sanitizedTwoWayKey = this.ctx.sanitize(twoWayKey);

    let sql = "";

    if (newKey) {
      newKey = this.ctx.sanitize(newKey);
    }
    if (newTwoWayKey) {
      newTwoWayKey = this.ctx.sanitize(newTwoWayKey);
    }

    switch (type) {
      case RelationEnum.OneToOne:
        if (sanitizedKey !== newKey) {
          sql = `ALTER TABLE ${table} RENAME COLUMN ${this.ctx.quote(sanitizedKey)} TO ${this.ctx.quote(newKey!)};`;
        }
        if (twoWay && sanitizedTwoWayKey !== newTwoWayKey) {
          sql += `ALTER TABLE ${relatedTable} RENAME COLUMN ${this.ctx.quote(sanitizedTwoWayKey)} TO ${this.ctx.quote(newTwoWayKey!)};`;
        }
        break;
      case RelationEnum.OneToMany:
        if (side === RelationSideEnum.Parent) {
          if (sanitizedTwoWayKey !== newTwoWayKey) {
            sql = `ALTER TABLE ${relatedTable} RENAME COLUMN ${this.ctx.quote(sanitizedTwoWayKey)} TO ${this.ctx.quote(newTwoWayKey!)};`;
          }
        } else {
          if (sanitizedKey !== newKey) {
            sql = `ALTER TABLE ${table} RENAME COLUMN ${this.ctx.quote(sanitizedKey)} TO ${this.ctx.quote(newKey!)};`;
          }
        }
        break;
      case RelationEnum.ManyToOne:
        if (side === RelationSideEnum.Child) {
          if (sanitizedTwoWayKey !== newTwoWayKey) {
            sql = `ALTER TABLE ${relatedTable} RENAME COLUMN ${this.ctx.quote(sanitizedTwoWayKey)} TO ${this.ctx.quote(newTwoWayKey!)};`;
          }
        } else {
          if (sanitizedKey !== newKey) {
            sql = `ALTER TABLE ${table} RENAME COLUMN ${this.ctx.quote(sanitizedKey)} TO ${this.ctx.quote(newKey!)};`;
          }
        }
        break;
      case RelationEnum.ManyToMany:
        // TODO:
        break;
      default:
        throw new DatabaseException("Invalid relationship type");
    }

    if (!sql) {
      return true;
    }

    sql = this.ctx.trigger(EventsEnum.AttributeUpdate, sql);

    try {
      await this.ctx.$client.query(sql);
      return true;
    } catch (e: any) {
      processException(
        e,
        `Failed to update relationship between '${collection}' and '${relatedCollection}'`,
      );
    }
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
    const name = this.ctx.sanitize(collection);
    const relatedName = this.ctx.sanitize(relatedCollection);
    const table = this.ctx.getSQLTable(name);
    const relatedTable = this.ctx.getSQLTable(relatedName);
    const sanitizedKey = this.ctx.sanitize(key);
    const sanitizedTwoWayKey = this.ctx.sanitize(twoWayKey);

    let sql = "";

    switch (type) {
      case RelationEnum.OneToOne:
        if (side === RelationSideEnum.Parent) {
          sql = `ALTER TABLE ${table} DROP COLUMN ${this.ctx.quote(sanitizedKey)};`;
          if (twoWay) {
            sql += `ALTER TABLE ${relatedTable} DROP COLUMN ${this.ctx.quote(sanitizedTwoWayKey)};`;
          }
        } else if (side === RelationSideEnum.Child) {
          sql = `ALTER TABLE ${relatedTable} DROP COLUMN ${this.ctx.quote(sanitizedTwoWayKey)};`;
          if (twoWay) {
            sql += `ALTER TABLE ${table} DROP COLUMN ${this.ctx.quote(sanitizedKey)};`;
          }
        }
        break;
      case RelationEnum.OneToMany:
        if (side === RelationSideEnum.Parent) {
          sql = `ALTER TABLE ${relatedTable} DROP COLUMN ${this.ctx.quote(sanitizedTwoWayKey)};`;
        } else {
          sql = `ALTER TABLE ${table} DROP COLUMN ${this.ctx.quote(sanitizedKey)};`;
        }
        break;
      case RelationEnum.ManyToOne:
        if (side === RelationSideEnum.Child) {
          sql = `ALTER TABLE ${relatedTable} DROP COLUMN ${this.ctx.quote(sanitizedTwoWayKey)};`;
        } else {
          sql = `ALTER TABLE ${table} DROP COLUMN ${this.ctx.quote(sanitizedKey)};`;
        }
        break;
      case RelationEnum.ManyToMany:
        break;
      default:
        throw new DatabaseException("Invalid relationship type");
    }

    if (!sql) {
      return true;
    }

    sql = this.ctx.trigger(EventsEnum.AttributeDelete, sql);

    try {
      await this.ctx.$client.query(sql);
      return true;
    } catch (e: any) {
      processException(
        e,
        `Failed to delete relationship between '${collection}' and '${relatedCollection}'`,
      );
    }
  }

  public async updateAttribute({
    collection,
    key: name,
    newName,
    array,
    size,
    type,
  }: UpdateAttribute): Promise<void> {
    const tableName = this.ctx.getSQLTable(this.ctx.sanitize(collection));
    const columnName = this.ctx.sanitize(name);
    const newColumnName = newName ? this.ctx.sanitize(newName) : null;
    const sqlType = this.ctx.getSQLType(type, size, array);

    let sql: string;
    if (newColumnName) {
      sql = `ALTER TABLE ${tableName} RENAME COLUMN ${this.ctx.quote(columnName)} TO ${this.ctx.quote(newColumnName)};`;
      sql += ` ALTER TABLE ${tableName} ALTER COLUMN ${this.ctx.quote(newColumnName)} TYPE ${sqlType};`;
    } else {
      sql = `ALTER TABLE ${tableName} ALTER COLUMN ${this.ctx.quote(columnName)} TYPE ${sqlType};`;
    }

    sql = this.ctx.trigger(EventsEnum.AttributeUpdate, sql);

    try {
      await this.ctx.$client.query(sql);
    } catch (e: any) {
      processException(e, "Failed to update attribute");
    }
  }

  public async renameIndex(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    const currentPgIndexName = this.ctx.getSQLIndex(collectionId, oldName);
    const newPgIndexName = this.ctx.getSQLIndex(collectionId, newName);

    let sql = `ALTER INDEX ${this.ctx.quote(this.ctx.$schema)}.${currentPgIndexName} RENAME TO ${newPgIndexName};`;
    sql = this.ctx.trigger(EventsEnum.IndexRename, sql);

    try {
      await this.ctx.$client.query(sql);
      return true;
    } catch (e: any) {
      throw processException(
        e,
        `Failed to rename index from ${oldName} to ${newName} for collection ${collectionId}`,
      );
    }
  }

  public async createIndex({
    collection: collectionId,
    name,
    type,
    attributes,
    orders = [],
    attributeTypes = {},
  }: CreateIndex): Promise<boolean> {
    const isUnique = type === IndexEnum.Unique;
    const isFulltext = type === IndexEnum.FullText;

    let usingClause = "";
    if (isFulltext) {
      usingClause = "USING GIN";
    }

    const preparedAttributes = attributes.map((attrId, i) => {
      const collectionAttribute = attributeTypes[attrId.toLowerCase()];

      if (!collectionAttribute) {
        throw new DatabaseException(
          `Attribute '${attrId}' not found in collection metadata.`,
        );
      }

      const internalKey = this.ctx.getInternalKeyForAttribute(attrId);
      const sanitizedKey = this.ctx.sanitize(internalKey);
      const pgKey = this.ctx.quote(sanitizedKey);

      if (isFulltext) {
        // Full-text search indexes on a `TSVECTOR` representation of the column.
        // We use the `to_tsvector` function for this.
        return `to_tsvector('${Database.FULLTEXT_LANGUAGE}', ${pgKey})`;
      }

      if (collectionAttribute.array) {
        usingClause = "USING GIN";
        return pgKey;
      }
      const order = orders[i] && !isFulltext ? ` ${orders[i]}` : "";
      return `${pgKey}${order}`;
    });

    if (isFulltext && preparedAttributes.length > 1) {
      const combinedTsvector = preparedAttributes.join(" || ");
      preparedAttributes.length = 0;
      preparedAttributes.push(combinedTsvector);
    }

    const pgTable = this.ctx.getSQLTable(collectionId);
    const pgIndexId = this.ctx.getSQLIndex(collectionId, name);
    const uniqueClause = isUnique ? "UNIQUE" : "";

    let attributesForSql = preparedAttributes.join(", ");

    if (this.ctx.$sharedTables && !isFulltext) {
      const pgTenantKey = `"${this.ctx.sanitize("_tenant")}"`;
      attributesForSql = `${pgTenantKey}, ${attributesForSql}`;
    }

    const sql = `CREATE ${uniqueClause} INDEX ${pgIndexId} ON ${pgTable} ${usingClause} (${attributesForSql})`;
    const finalSql = this.ctx.trigger(EventsEnum.IndexCreate, sql);

    try {
      await this.ctx.$client.query(finalSql);
      return true;
    } catch (e) {
      throw processException(e);
    }
  }

  public async deleteIndex(collection: string, id: string): Promise<boolean> {
    const pgIndexName = this.ctx.getSQLIndex(collection, id);

    let sql = `DROP INDEX ${this.ctx.quote(this.ctx.$schema)}.${pgIndexName};`;
    sql = this.ctx.trigger(EventsEnum.IndexDelete, sql);

    try {
      await this.ctx.$client.query(sql);
      return true;
    } catch (e: any) {
      return false;
    }
  }
}
