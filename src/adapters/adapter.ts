import type { Pool } from "pg";
import { BaseAdapter } from "./base.js";
import { PostgresClient, Transaction } from "./postgres.js";
import {
  AttributeEnum,
  EventsEnum,
  IndexEnum,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";
import { CreateCollectionOptions } from "./interface.js";
import { DatabaseException } from "@errors/base.js";
import { Database, ProcessedQuery } from "@core/database.js";
import { Doc } from "@core/doc.js";
import { Attribute } from "@validators/schema.js";
import {
  ColumnInfo,
  CreateAttribute,
  CreateIndex,
  UpdateAttribute,
} from "./types.js";

export class Adapter extends BaseAdapter {
  protected client: PostgresClient | Transaction;

  constructor(pool: Pool | PostgresClient | Transaction) {
    super();
    this.client =
      pool instanceof PostgresClient || pool instanceof Transaction
        ? pool
        : new PostgresClient(pool);
  }

  async create(name: string): Promise<void> {
    name = this.quote(name);
    if (await this.exists(name)) return;

    let sql = `CREATE SCHEMA ${name};`;
    sql = this.trigger(EventsEnum.DatabaseCreate, sql);

    await this.client.query(sql);
  }

  async delete(name: string): Promise<void> {
    name = this.quote(name);
    await this.client.query(`DROP SCHEMA IF EXISTS ${name} CASCADE;`);
  }

  async createCollection({
    name,
    attributes,
    indexes,
  }: CreateCollectionOptions): Promise<void> {
    name = this.sanitize(name);
    const mainTable = this.getSQLTable(name);
    const attributeSql: string[] = [];
    const indexSql: string[] = [];
    const attributeHash: Record<string, Attribute> = {};

    attributes.forEach((attribute) => {
      const id = this.sanitize(attribute.getId());
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
      const type = this.getSQLType(
        attribute.get("type"),
        attribute.get("size"),
        attribute.get("array"),
      );

      let sql = `${this.quote(id)} ${type}`;
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
          const pgKey = `"${this.sanitize(this.getInternalKeyForAttribute(attributeKey))}"`;
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

      if (this.$sharedTables && !isFulltext) {
        const pgTenantKey = `"${this.sanitize("_tenant")}"`;
        attributesForSql = `${pgTenantKey}, ${attributesForSql}`;
      }

      const uniqueClause = isFulltext
        ? ""
        : indexType === IndexEnum.Unique
          ? "UNIQUE "
          : "";

      const pgIndexId = this.getSQLIndex(name, this.sanitize(indexId));
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
    const tenantCol = this.quote("_tenant");

    if (this.$sharedTables) {
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
    if (this.$sharedTables) {
      postTableIndexes.push(
        `CREATE UNIQUE INDEX ${this.getSQLIndex(name, "uid_tenant")} ON ${mainTable} ("_uid", ${tenantCol});`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.getSQLIndex(name, "created_at_tenant")} ON ${mainTable} (${tenantCol}, "_createdAt");`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.getSQLIndex(name, "updated_at_tenant")} ON ${mainTable} (${tenantCol}, "_updatedAt");`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.getSQLIndex(name, "tenant_id")} ${mainTable} (${tenantCol}, "_id");`,
      );
    } else {
      postTableIndexes.push(
        `CREATE UNIQUE INDEX ${this.getSQLIndex(name, "uid")} ON ${mainTable} ("_uid");`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.getSQLIndex(name, "created_at")} ON ${mainTable} ("_createdAt");`,
      );
      postTableIndexes.push(
        `CREATE INDEX ${this.getSQLIndex(name, "updated_at")} ON ${mainTable} ("_updatedAt");`,
      );
    }
    postTableIndexes.push(
      `CREATE INDEX ${this.getSQLIndex(name, "permissions_gin_idx")} ON ${mainTable} USING GIN ("_permissions");`,
    );

    tableSql = this.trigger(EventsEnum.CollectionCreate, tableSql);

    const permissionsTableName = this.getSQLTable(name + "_perms");

    const permissionsTableColumns = [
      `"_id" BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY`,
      `"_type" VARCHAR(12) NOT NULL`,
      `"_permissions" TEXT[] NOT NULL DEFAULT '{}'`,
      `"_document" BIGINT NOT NULL`,
      `FOREIGN KEY ("_document") REFERENCES ${mainTable}("_id") ON DELETE CASCADE`,
    ];
    const postPermissionsTableIndexes: string[] = [];
    let permissionsPrimaryKeyDefinition: string;

    if (this.$sharedTables) {
      permissionsTableColumns.splice(1, 0, `${tenantCol} BIGINT DEFAULT NULL`);
      permissionsPrimaryKeyDefinition = `PRIMARY KEY ("_id", ${tenantCol})`;

      postPermissionsTableIndexes.push(
        `CREATE UNIQUE INDEX ${this.getSQLIndex(`${name}_perms`, "index1")} ON ${permissionsTableName} ("_document", ${tenantCol}, "_type");`,
      );
      postPermissionsTableIndexes.push(
        `CREATE INDEX ${this.getSQLIndex(`${name}_perms`, "tenant")} ON ${permissionsTableName} (${tenantCol});`,
      );
    } else {
      permissionsPrimaryKeyDefinition = `PRIMARY KEY ("_id")`;
      postPermissionsTableIndexes.push(
        `CREATE UNIQUE INDEX ${this.getSQLIndex(`${name}_perms`, "index1")} ON ${permissionsTableName} ("_document", "_type");`,
      );
    }
    postPermissionsTableIndexes.push(
      `CREATE INDEX ${this.getSQLIndex(`${name}_perms`, "permissions_gin_idx")} ON ${permissionsTableName} USING GIN ("_permissions");`,
    );

    const permissionsColumnsAndConstraints =
      permissionsTableColumns.join(",\n");
    let permissionsTable = `
            CREATE TABLE ${permissionsTableName} (
                ${permissionsColumnsAndConstraints},
                ${permissionsPrimaryKeyDefinition}
            );
        `;

    permissionsTable = this.trigger(
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

      const client = this.$client;
      if (client.__type === "postgres") {
        await client.transaction(callback);
      } else {
        callback(client);
      }
    } catch (error) {
      this.processException(error);
    }
  }

  public async getSizeOfCollectionOnDisk(collection: string): Promise<number> {
    collection = this.sanitize(collection);
    const collectionTableName = `'${this.$namespace}_${collection}'`;
    const permissionsTableName = `'${this.$namespace}_${collection}_perms'`;

    const sql = `
            SELECT
                pg_total_relation_size(${collectionTableName}::regclass) AS collection_size,
                pg_total_relation_size(${permissionsTableName}::regclass) AS permissions_size;
        `;

    try {
      const [rows]: any = await this.client.query(sql);
      const collectionSize = Number(rows[0]?.collection_size ?? 0);
      const permissionsSize = Number(rows[0]?.permissions_size ?? 0);
      return collectionSize + permissionsSize;
    } catch (e: any) {
      if (
        e.message.includes("relation") &&
        e.message.includes("does not exist")
      ) {
        return 0;
      }
      this.processException(
        e,
        `Failed to get size of collection ${collection} on disk: ${e.message}`,
      );
    }
  }

  public async getSizeOfCollection(collection: string): Promise<number> {
    collection = this.sanitize(collection);
    const collectionTableName = `'${this.$namespace}_${collection}'`;
    const permissionsTableName = `'${this.$namespace}_${collection}_perms'`;

    const sql = `
            SELECT
            pg_table_size(${collectionTableName}::regclass) + pg_indexes_size(${collectionTableName}::regclass) AS collection_size,
            pg_table_size(${permissionsTableName}::regclass) + pg_indexes_size(${permissionsTableName}::regclass) AS permissions_size;
        `;

    try {
      const { rows } = await this.client.query(sql);
      const collectionSize = Number(rows[0]?.collection_size ?? 0);
      const permissionsSize = Number(rows[0]?.permissions_size ?? 0);
      return collectionSize + permissionsSize;
    } catch (e: any) {
      if (
        e.message.includes("relation") &&
        e.message.includes("does not exist")
      ) {
        return 0;
      }
      this.processException(
        e,
        `Failed to get size of collection ${collection}: ${e.message}`,
      );
    }
  }

  public async deleteCollection(id: string): Promise<void> {
    const permissionsTableName = this.getSQLTable(this.sanitize(id + "_perms"));
    const collectionTableName = this.getSQLTable(this.sanitize(id));

    let dropPermsSql = `DROP TABLE IF EXISTS ${permissionsTableName} CASCADE;`;
    dropPermsSql = this.trigger(EventsEnum.CollectionDelete, dropPermsSql);

    let dropCollectionSql = `DROP TABLE IF EXISTS ${collectionTableName} CASCADE;`;
    dropCollectionSql = this.trigger(
      EventsEnum.CollectionDelete,
      dropCollectionSql,
    );

    try {
      await this.client.query(dropPermsSql);
      await this.client.query(dropCollectionSql);
    } catch (e: any) {
      this.processException(e, `Failed to delete collection ${id}`);
    }
  }

  public async analyzeCollection(collection: string): Promise<boolean> {
    const name = this.sanitize(collection);
    const tableName = this.getSQLTable(name);

    const sql = `ANALYZE ${tableName}`;

    try {
      await this.client.query(sql);
      return true;
    } catch (e: any) {
      this.processException(e, `Failed to analyze collection ${collection}`);
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

    const sqlType = this.getSQLType(type, size, array);
    const table = this.getSQLTable(collection);

    let sql = `
                ALTER TABLE ${table}
                ADD COLUMN ${this.quote(name)} ${sqlType}
            `;
    sql = this.trigger(EventsEnum.AttributeCreate, sql);

    try {
      await this.client.query(sql);
    } catch (e: any) {
      this.processException(
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

      const sqlType = this.getSQLType(attr.type, attr.size, attr.array);
      parts.push(`${this.quote(attr.key)} ${sqlType}`);
    }

    const columns = parts.join(", ADD COLUMN ");
    const table = this.getSQLTable(collection);
    let sql = `
                ALTER TABLE ${table}
                ADD COLUMN ${columns}
            `;

    sql = this.trigger(EventsEnum.AttributesCreate, sql);

    try {
      await this.client.query(sql);
    } catch (e: any) {
      this.processException(
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

    const table = this.getSQLTable(collection);
    let sql = `
                ALTER TABLE ${table}
                RENAME COLUMN ${this.quote(oldName)} TO ${this.quote(newName)}
            `;

    sql = this.trigger(EventsEnum.AttributeUpdate, sql);

    try {
      await this.client.query(sql);
    } catch (e: any) {
      this.processException(
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

    const table = this.getSQLTable(collection);
    let sql = `
                ALTER TABLE ${table}
                DROP COLUMN ${this.quote(name)}
            `;

    sql = this.trigger(EventsEnum.AttributeDelete, sql);

    try {
      await this.client.query(sql);
    } catch (e: any) {
      this.processException(
        e,
        `Failed to delete attribute '${name}' from collection '${collection}'`,
      );
    }
  }

  public async getSchemaAttributes(
    collection: string,
  ): Promise<Doc<ColumnInfo>[]> {
    const schema = this.$schema;
    const table = `${this.$namespace}_${this.sanitize(collection)}`;

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
      const result: any = await this.client.query(sql, [table, schema]);

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
      this.processException(e, "Failed to get schema attributes");
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
    const name = this.sanitize(collection);
    const relatedName = this.sanitize(relatedCollection);
    const table = this.getSQLTable(name);
    const relatedTable = this.getSQLTable(relatedName);
    const sanitizedId = this.sanitize(id);
    const sanitizedTwoWayKey = this.sanitize(twoWayKey);
    const sqlType = this.getSQLType(AttributeEnum.Relationship, 0, false);

    let sql: string;

    switch (type) {
      case RelationEnum.OneToOne:
        sql = `
                    ALTER TABLE ${table} 
                    ADD COLUMN ${this.quote(sanitizedId)} ${sqlType} DEFAULT NULL;
                `;

        if (twoWay) {
          sql += `
                        ALTER TABLE ${relatedTable} 
                        ADD COLUMN ${this.quote(sanitizedTwoWayKey)} ${sqlType} DEFAULT NULL;
                    `;
        }
        break;

      case RelationEnum.OneToMany:
        sql = `
                    ALTER TABLE ${relatedTable} 
                    ADD COLUMN ${this.quote(sanitizedTwoWayKey)} ${sqlType} DEFAULT NULL;
                `;
        break;

      case RelationEnum.ManyToOne:
        sql = `
                    ALTER TABLE ${table} 
                    ADD COLUMN ${this.quote(sanitizedId)} ${sqlType} DEFAULT NULL;
                `;
        break;

      case RelationEnum.ManyToMany:
        return true;

      default:
        throw new DatabaseException("Invalid relationship type");
    }

    sql = this.trigger(EventsEnum.AttributeCreate, sql);

    try {
      await this.client.query(sql);
      return true;
    } catch (e: any) {
      this.processException(
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
    const name = this.sanitize(collection);
    const relatedName = this.sanitize(relatedCollection);
    const table = this.getSQLTable(name);
    const relatedTable = this.getSQLTable(relatedName);
    const sanitizedKey = this.sanitize(key);
    const sanitizedTwoWayKey = this.sanitize(twoWayKey);

    let sql = "";

    if (newKey) {
      newKey = this.sanitize(newKey);
    }
    if (newTwoWayKey) {
      newTwoWayKey = this.sanitize(newTwoWayKey);
    }

    switch (type) {
      case RelationEnum.OneToOne:
        if (sanitizedKey !== newKey) {
          sql = `ALTER TABLE ${table} RENAME COLUMN ${this.quote(sanitizedKey)} TO ${this.quote(newKey!)};`;
        }
        if (twoWay && sanitizedTwoWayKey !== newTwoWayKey) {
          sql += `ALTER TABLE ${relatedTable} RENAME COLUMN ${this.quote(sanitizedTwoWayKey)} TO ${this.quote(newTwoWayKey!)};`;
        }
        break;
      case RelationEnum.OneToMany:
        if (side === RelationSideEnum.Parent) {
          if (sanitizedTwoWayKey !== newTwoWayKey) {
            sql = `ALTER TABLE ${relatedTable} RENAME COLUMN ${this.quote(sanitizedTwoWayKey)} TO ${this.quote(newTwoWayKey!)};`;
          }
        } else {
          if (sanitizedKey !== newKey) {
            sql = `ALTER TABLE ${table} RENAME COLUMN ${this.quote(sanitizedKey)} TO ${this.quote(newKey!)};`;
          }
        }
        break;
      case RelationEnum.ManyToOne:
        if (side === RelationSideEnum.Child) {
          if (sanitizedTwoWayKey !== newTwoWayKey) {
            sql = `ALTER TABLE ${relatedTable} RENAME COLUMN ${this.quote(sanitizedTwoWayKey)} TO ${this.quote(newTwoWayKey!)};`;
          }
        } else {
          if (sanitizedKey !== newKey) {
            sql = `ALTER TABLE ${table} RENAME COLUMN ${this.quote(sanitizedKey)} TO ${this.quote(newKey!)};`;
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

    sql = this.trigger(EventsEnum.AttributeUpdate, sql);

    try {
      await this.client.query(sql);
      return true;
    } catch (e: any) {
      this.processException(
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
    const name = this.sanitize(collection);
    const relatedName = this.sanitize(relatedCollection);
    const table = this.getSQLTable(name);
    const relatedTable = this.getSQLTable(relatedName);
    const sanitizedKey = this.sanitize(key);
    const sanitizedTwoWayKey = this.sanitize(twoWayKey);

    let sql = "";

    switch (type) {
      case RelationEnum.OneToOne:
        if (side === RelationSideEnum.Parent) {
          sql = `ALTER TABLE ${table} DROP COLUMN ${this.quote(sanitizedKey)};`;
          if (twoWay) {
            sql += `ALTER TABLE ${relatedTable} DROP COLUMN ${this.quote(sanitizedTwoWayKey)};`;
          }
        } else if (side === RelationSideEnum.Child) {
          sql = `ALTER TABLE ${relatedTable} DROP COLUMN ${this.quote(sanitizedTwoWayKey)};`;
          if (twoWay) {
            sql += `ALTER TABLE ${table} DROP COLUMN ${this.quote(sanitizedKey)};`;
          }
        }
        break;
      case RelationEnum.OneToMany:
        if (side === RelationSideEnum.Parent) {
          sql = `ALTER TABLE ${relatedTable} DROP COLUMN ${this.quote(sanitizedTwoWayKey)};`;
        } else {
          sql = `ALTER TABLE ${table} DROP COLUMN ${this.quote(sanitizedKey)};`;
        }
        break;
      case RelationEnum.ManyToOne:
        if (side === RelationSideEnum.Child) {
          sql = `ALTER TABLE ${relatedTable} DROP COLUMN ${this.quote(sanitizedTwoWayKey)};`;
        } else {
          sql = `ALTER TABLE ${table} DROP COLUMN ${this.quote(sanitizedKey)};`;
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

    sql = this.trigger(EventsEnum.AttributeDelete, sql);

    try {
      await this.client.query(sql);
      return true;
    } catch (e: any) {
      this.processException(
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
    const tableName = this.getSQLTable(this.sanitize(collection));
    const columnName = this.sanitize(name);
    const newColumnName = newName ? this.sanitize(newName) : null;
    const sqlType = this.getSQLType(type, size, array);

    let sql: string;
    if (newColumnName) {
      sql = `ALTER TABLE ${tableName} RENAME COLUMN ${this.quote(columnName)} TO ${this.quote(newColumnName)};`;
      sql += ` ALTER TABLE ${tableName} ALTER COLUMN ${this.quote(newColumnName)} TYPE ${sqlType};`;
    } else {
      sql = `ALTER TABLE ${tableName} ALTER COLUMN ${this.quote(columnName)} TYPE ${sqlType};`;
    }

    sql = this.trigger(EventsEnum.AttributeUpdate, sql);

    try {
      await this.client.query(sql);
    } catch (e: any) {
      this.processException(e, "Failed to update attribute");
    }
  }

  public async renameIndex(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    const currentPgIndexName = this.getSQLIndex(collectionId, oldName);
    const newPgIndexName = this.getSQLIndex(collectionId, newName);

    let sql = `ALTER INDEX ${this.quote(this.$schema)}.${currentPgIndexName} RENAME TO ${newPgIndexName};`;
    sql = this.trigger(EventsEnum.IndexRename, sql);

    try {
      await this.client.query(sql);
      return true;
    } catch (e: any) {
      throw this.processException(
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

      const internalKey = this.getInternalKeyForAttribute(attrId);
      const sanitizedKey = this.sanitize(internalKey);
      const pgKey = this.quote(sanitizedKey);

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

    const pgTable = this.getSQLTable(collectionId);
    const pgIndexId = this.getSQLIndex(collectionId, name);
    const uniqueClause = isUnique ? "UNIQUE" : "";

    let attributesForSql = preparedAttributes.join(", ");

    if (this.$sharedTables && !isFulltext) {
      const pgTenantKey = `"${this.sanitize("_tenant")}"`;
      attributesForSql = `${pgTenantKey}, ${attributesForSql}`;
    }

    const sql = `CREATE ${uniqueClause} INDEX ${pgIndexId} ON ${pgTable} ${usingClause} (${attributesForSql})`;
    const finalSql = this.trigger(EventsEnum.IndexCreate, sql);

    try {
      await this.client.query(finalSql);
      return true;
    } catch (e) {
      throw this.processException(e);
    }
  }

  public async deleteIndex(collection: string, id: string): Promise<boolean> {
    const pgIndexName = this.getSQLIndex(collection, id);

    let sql = `DROP INDEX ${this.quote(this.$schema)}.${pgIndexName};`;
    sql = this.trigger(EventsEnum.IndexDelete, sql);

    try {
      await this.client.query(sql);
      return true;
    } catch (e: any) {
      return false;
    }
  }

  /**
   * Creates a new document in the specified collection.
   * Returns the created document with its sequence ID set.
   */
  public async createDocument<D extends Doc>(
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
      const { rows } = await this.client.query(sql, values);

      // Set $sequence from insertId
      document.set("$sequence", rows[0]["_id"]);

      if (!rows[0]["_id"]) {
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
      const { rows } = await this.client.query(sql, allValues);

      // Set $sequence from returned IDs
      for (let i = 0; i < documents.length; i++) {
        documents[i]!.set("$sequence", rows[i]["_id"]);
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
  public async deleteDocuments(collectionId: string, query: ProcessedQuery) {
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
      const { rows } = await this.client.query(sql, params);

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
    collection: string,
    query: ProcessedQuery,
    {
      forPermission = PermissionEnum.Read,
      ...options
    }: {
      forPermission?: PermissionEnum;
    } = {},
  ): Promise<Record<string, any>[]> {
    const sqlResult = this.buildSql(query, { ...options, forPermission });

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

  protected createTransactionAdapter(client: any): this {
    const adapter = new (this.constructor as any)(client) as this;
    adapter._meta = this._meta;
    adapter.$logger = this.$logger;
    return adapter;
  }
}
