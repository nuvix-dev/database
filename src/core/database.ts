import {
  AttributeEnum,
  EventsEnum,
  IndexEnum,
  OnDelete,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
} from "./enums.js";
import {
  Attribute,
  Collection,
  Index,
  RelationOptions,
} from "@validators/schema.js";
import {
  CreateCollection,
  CreateRelationshipAttribute,
  Filters,
  QueryByType,
  UpdateCollection,
  UpdateRelationshipAttribute,
} from "./types.js";
import { Cache } from "./cache.js";
import type { CacheDriver } from "@cache/types.js";
import { Entities, IEntity } from "types.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query } from "./query.js";
import { Doc } from "./doc.js";
import {
  AuthorizationException,
  ConflictException,
  DatabaseException,
  DependencyException,
  DuplicateException,
  IndexException,
  LimitException,
  NotFoundException,
  QueryException,
  RelationshipException,
  StructureException,
} from "@errors/index.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";
import { Permissions } from "@validators/permissions.js";
import { Index as IndexValidator } from "@validators/index-validator.js";
import { Documents } from "@validators/queries/documents.js";
import { authorize, AuthContext, SYSTEM_CONTEXT } from "./auth.js";
import { ID } from "@utils/id.js";
import { Structure } from "@validators/structure.js";
import { Adapter } from "@adapters/adapter.js";
import { IndexDependency } from "@validators/index-dependency.js";
import { MethodType } from "@validators/query/base.js";
import {
  createIndex,
  deleteIndex,
  renameIndex,
  updateIndexMeta,
} from "./index-manager.js";
import {
  createRelationship,
  deleteRelationship,
  updateRelationship,
} from "./relationship-schema.js";
import {
  createRelationships,
  deleteDocumentRelationships,
  handleManyToMany,
  handleOnDelete,
  updateDocumentRelationships,
} from "./document-relationships.js";
import { Logger, LoggerOptions } from "@utils/logger.js";

/**
 * Module-private key exposing the ctx-first document-plane internals of
 * {@link Database} to {@link Session}. The symbol is never exported, so code
 * outside this file cannot even name it: the document plane is unreachable
 * on Database from other modules, which enforces "sessions are the only
 * document API" at compile time.
 */
const documentPlane = Symbol("nuvix.db.documentPlane");

/**
 * Rebuilds the error text previously produced by the mutable
 * `Authorization.$description` side-channel so thrown messages keep their
 * historical format.
 */
const authorizationError = (
  action: PermissionEnum,
  permissions: string[],
  roles: readonly string[],
): string =>
  permissions.length === 0
    ? `No permissions provided for action '${action}'.`
    : `Missing "${action}" permission for role "${permissions[permissions.length - 1]}". Only "${JSON.stringify(roles)}" scopes are allowed and "${JSON.stringify(permissions)}" was given.`;

/**
 * Pure guard: throws an AuthorizationException carrying the legacy message
 * when `authorize(ctx, permissions, action)` denies the action.
 */
const ensureAuthorized = (
  ctx: AuthContext,
  permissions: string[],
  action: PermissionEnum,
): void => {
  if (!authorize(ctx, permissions, action)) {
    throw new AuthorizationException(
      authorizationError(action, permissions, ctx.roles),
    );
  }
};

export class Database extends Cache {
  private static readonly NUMERIC_ATTRIBUTE_TYPES: ReadonlySet<AttributeEnum> =
    new Set([AttributeEnum.Integer, AttributeEnum.Float]);

  /**
   * Ctx-first document-plane operations bound to this instance. Keyed by the
   * module-private `documentPlane` symbol; see {@link Session}.
   */
  public readonly [documentPlane]: DocumentPlane;

  /**
   * Constructs a real, independently-owned {@link Database} bound to the
   * transaction adapter (see {@link Base.withTransaction}). Unlike the
   * prototype-clone approach it replaces, the scoped instance OWNS every
   * mutable field — filters, caches, relation stack, event listeners and
   * its own `[documentPlane]` closures bound to itself — so concurrent or
   * nested transaction scopes can never alias state into each other or
   * into the parent.
   *
   * Scalar configuration is copied by value; mutable containers are freshly
   * created by the constructor and seeded from this instance, never shared
   * by reference. Per-operation guards (`_relationStack`) and caches
   * (`decodeAttributesCache`) intentionally start fresh.
   */
  protected createTransactionalScope(txAdapter: Adapter): this {
    const scope = new Database(txAdapter, this.cache, {
      filters: { ...this.instanceFilters },
      logger: this.logger,
    });

    // Copy scalar configuration by value — the scope owns its own settings.
    scope.timestamp = this.timestamp;
    scope.filter = this.filter;
    scope.validate = this.validate;
    scope.preserveDates = this.preserveDates;
    scope.maxQueryValues = this.maxQueryValues;
    scope.resolveRelationships = this.resolveRelationships;
    scope.checkRelationshipsExist = this.checkRelationshipsExist;
    scope.isMigrating = this.isMigrating;
    scope._collectionEnabledValidate = this._collectionEnabledValidate;
    scope.attachSchemaInDocument = this.attachSchemaInDocument;

    // Seed the scope-owned collections map with a copy of ours.
    Object.assign(scope.globalCollections, this.globalCollections);

    // Re-register parent listeners on the scope's own emitter so events
    // triggered inside the transaction still reach them.
    this.copyListenersTo(scope);

    return scope as this;
  }

  constructor(
    adapter: Adapter,
    cache: CacheDriver,
    options: DatabaseOptions = {},
  ) {
    super(adapter, cache, options);

    // Bind every document-plane operation to this instance. Sessions share
    // the adapter, cache and instance state through these closures — no new
    // pool or connection is created.
    this[documentPlane] = {
      find: (ctx, collectionId, query, forPermission) =>
        this.find(ctx, collectionId, query, forPermission),
      findOne: (ctx, collectionId, query) =>
        this.findOne(ctx, collectionId, query),
      getDocument: (ctx, collectionId, id, query, forUpdate) =>
        this.getDocument(ctx, collectionId, id, query, forUpdate),
      createDocument: (ctx, collectionId, document) =>
        this.createDocument(ctx, collectionId, document),
      createDocuments: (ctx, collectionId, documents) =>
        this.createDocuments(ctx, collectionId, documents),
      updateDocument: (ctx, collectionId, id, document) =>
        this.updateDocument(ctx, collectionId, id, document),
      updateDocuments: (
        ctx,
        collectionId,
        updates,
        query,
        batchSize,
        onNext,
        onError,
      ) =>
        this.updateDocuments(
          ctx,
          collectionId,
          updates,
          query,
          batchSize,
          onNext,
          onError,
        ),
      deleteDocument: (ctx, collectionId, id) =>
        this.deleteDocument(ctx, collectionId, id),
      deleteDocuments: (ctx, collectionId, query) =>
        this.deleteDocuments(ctx, collectionId, query),
      deleteDocumentsBatch: (
        ctx,
        collectionId,
        query,
        batchSize,
        onNext,
        onError,
      ) =>
        this.deleteDocumentsBatch(
          ctx,
          collectionId,
          query,
          batchSize,
          onNext,
          onError,
        ),
      createOrUpdateDocuments: (ctx, collectionId, documents, batchSize, onNext) =>
        this.createOrUpdateDocuments(ctx, collectionId, documents, batchSize, onNext),
      createOrUpdateDocumentsWithIncrease: (
        ctx,
        collectionId,
        attribute,
        documents,
        batchSize,
        onNext,
      ) =>
        this.createOrUpdateDocumentsWithIncrease(
          ctx,
          collectionId,
          attribute,
          documents,
          batchSize,
          onNext,
        ),
      increaseDocumentAttribute: (ctx, collectionId, id, attribute, value, max) =>
        this.increaseDocumentAttribute(
          ctx,
          collectionId,
          id,
          attribute,
          value,
          max,
        ),
      decreaseDocumentAttribute: (ctx, collectionId, id, attribute, value, min) =>
        this.decreaseDocumentAttribute(
          ctx,
          collectionId,
          id,
          attribute,
          value,
          min,
        ),
      count: (ctx, collectionId, query, max) =>
        this.count(ctx, collectionId, query, max),
      sum: (ctx, collectionId, attribute, query, max) =>
        this.sum(ctx, collectionId, attribute, query, max),
      purgeCachedDocument: (collectionId, doc) =>
        this.purgeCachedDocument(collectionId, doc),
      purgeCachedCollection: (collection) =>
        this.purgeCachedCollection(collection),
      withTransaction: (ctx, callback) =>
        this.withTransaction(async (txDatabase) =>
          callback(new Session(txDatabase as Database, ctx)),
        ),
    };
  }

  /**
   * Opens a scoped session that carries an immutable AuthContext built from
   * the given roles. Accepts either varargs (`db.for("user:1", "team:2")`)
   * or a single array (`db.for(["user:1", "team:2"])`).
   *
   * The session exposes ONLY document-plane operations; schema/admin
   * operations remain on the Database instance itself.
   */
  public for(...roles: string[] | [string[]]): Session {
    const roleList: string[] = Array.isArray(roles[0])
      ? (roles[0] as string[])
      : (roles as string[]);
    return new Session(
      this,
      Object.freeze({ roles: Object.freeze(roleList) }),
    );
  }

  /**
   * Opens a privileged system session carrying SYSTEM_CONTEXT. Every
   * document-plane operation on it bypasses authorization checks — the
   * explicit-ctx replacement for the removed global skip mechanism.
   */
  public system(): Session {
    return new Session(this, SYSTEM_CONTEXT);
  }

  /**
   * Creates a new database.
   */
  public async create(database?: string): Promise<void> {
    database = database ?? this.adapter.$schema;
    await this.adapter.create(database);

    const attributes = [...Database.COLLECTION.attributes].map(
      (attr) => new Doc(attr),
    );
    await this.silent(() =>
      this.createCollection({ id: Database.METADATA, attributes }),
    );

    this.trigger(EventsEnum.DatabaseCreate, database);
  }

  /**
   * Check is database or collection already exists or not.
   */
  public async exists<C extends keyof Entities>(
    database?: string,
    collection?: C,
  ): Promise<boolean>;
  public async exists(database?: string, collection?: string): Promise<boolean>;
  public async exists(
    database?: string,
    collection?: string,
  ): Promise<boolean> {
    database ??= this.adapter.$schema;
    return this.adapter.exists(database, collection);
  }

  /**
   * list of databases.
   */
  public async list(): Promise<string[]> {
    this.trigger(EventsEnum.DatabaseList, []);
    return [];
  }

  /**
   * Delete a database.
   */
  public async delete(database?: string): Promise<void> {
    database ??= this.adapter.$schema;
    await this.adapter.delete(database);

    this.trigger(EventsEnum.DatabaseDelete, database);
    await this.cache.flush();
  }

  /**
   * Creates a new collection in the database.
   */
  public async createCollection({
    id,
    attributes = [],
    indexes = [],
    permissions,
    documentSecurity,
    enabled,
  }: CreateCollection): Promise<Doc<Collection>> {
    permissions ??= [
      Permission.create(Role.any()),
      Permission.read(Role.any()),
      Permission.update(Role.any()),
      Permission.delete(Role.any()),
    ];

    if (this.validate) {
      const perms = new Permissions();
      if (!perms.$valid(permissions)) {
        throw new DatabaseException(perms.$description);
      }
    }

    let collection = await this.silent(() => this.getCollection(id));
    if (!collection.empty() && id !== Database.METADATA) {
      throw new DuplicateException(`Collection '${id}' already exists.`);
    }

    // Fix metadata index orders
    for (let i = 0; i < indexes.length; i++) {
      const index = indexes[i]!;
      const orders: (string | null)[] = index.get("orders", []);

      const indexAttributes = index.get("attributes", []);
      for (let j = 0; j < indexAttributes.length; j++) {
        const attr = indexAttributes[j];
        for (const collectionAttribute of attributes) {
          if (collectionAttribute.get("$id") === attr) {
            const isArray = collectionAttribute.get("array", false);
            if (isArray) {
              orders[j] = null;
            }
            break;
          }
        }
      }

      index.set("orders", orders);
      indexes[i] = index;
    }

    collection = new Doc<Collection>({
      $id: id,
      $permissions: permissions,
      name: id,
      attributes: attributes,
      indexes: indexes,
      documentSecurity: documentSecurity ?? true,
      enabled: enabled ?? true,
    });

    if (this.validate) {
      const validator = new IndexValidator(
        attributes,
        this.adapter.$maxIndexLength,
        this.adapter.$internalIndexesKeys,
        this.adapter.$supportForIndexArray,
      );
      indexes.forEach((index) => {
        if (!validator.$valid(index)) {
          throw new IndexException(validator.$description);
        }
      });
    }

    if (
      indexes.length &&
      this.adapter.getCountOfIndexes(collection) > this.adapter.$limitForIndexes
    ) {
      throw new LimitException(
        `Index limit of ${this.adapter.$limitForIndexes} exceeded. Cannot create collection.`,
      );
    }

    if (attributes.length) {
      if (
        this.adapter.$limitForAttributes &&
        attributes.length > this.adapter.$limitForAttributes
      ) {
        throw new LimitException(
          `Attribute limit of ${this.adapter.$limitForAttributes} exceeded. Cannot create collection.`,
        );
      }
      if (
        this.adapter.$documentSizeLimit &&
        this.adapter.getAttributeWidth(collection) >
          this.adapter.$documentSizeLimit
      ) {
        throw new LimitException(
          `Document size limit of ${this.adapter.$documentSizeLimit} exceeded. Cannot create collection.`,
        );
      }
    }

    try {
      await this.adapter.createCollection({ name: id, attributes, indexes });
    } catch (error) {
      if (error instanceof DuplicateException) {
        // $HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
        if (!this.sharedTables || !this.migrating) {
          throw error;
        }
      } else {
        throw error;
      }
    }

    if (id === Database.METADATA) return new Doc(Database.COLLECTION);

    const createdCollection = await this.silent(() =>
      this.createDocument(SYSTEM_CONTEXT, Database.METADATA, collection),
    );
    this.trigger(EventsEnum.CollectionCreate, createdCollection);

    return createdCollection;
  }
  /**
   * Update collection permissions & documentSecurity.
   */
  public async updateCollection({
    id,
    documentSecurity,
    permissions,
    enabled,
  }: UpdateCollection): Promise<Doc<Collection>> {
    if (permissions.length) {
      if (this.validate) {
        const perms = new Permissions();
        if (!perms.$valid(permissions)) {
          throw new DatabaseException(perms.$description);
        }
      }
    }

    let collection = await this.silent(() => this.getCollection(id, true));

    if (
      this.adapter.$sharedTables &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      throw new NotFoundException(`Collection '${id}' not found`);
    }

    collection.set("$permissions", permissions);
    collection.set("documentSecurity", documentSecurity);
    collection.set("enabled", enabled);

    collection = await this.silent(() =>
      this.updateDocument(
        SYSTEM_CONTEXT,
        Database.METADATA,
        collection.getId(),
        collection,
      ),
    );
    this.trigger(EventsEnum.CollectionUpdate, collection);

    return collection;
  }

  /**
   * Retrieves a collection by its ID.
   * If the collection is not found or does not match the tenant ID, an empty Doc
   */
  public async getCollection(
    id: string,
    throwOnNotFound?: boolean,
  ): Promise<Doc<Collection>> {
    let collection = await this.silent(() =>
      this.getDocument(SYSTEM_CONTEXT, Database.METADATA, id),
    );

    if (
      id !== Database.METADATA &&
      this.adapter.$sharedTables &&
      collection.getTenant() !== null &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      if (throwOnNotFound) {
        throw new NotFoundException(`Collection '${id}' not found`);
      }
      return new Doc<Collection>();
    }

    if (this.assertCollectionEnabled(collection)) {
      collection = new Doc<Collection>();
    }

    this.trigger(EventsEnum.CollectionRead, collection);
    if (collection.empty() && throwOnNotFound) {
      throw new NotFoundException(`Collection '${id}' not found`);
    }

    return collection;
  }

  /**
   * Lists all collections in the database.
   */
  public async listCollections(
    limit: number = 25,
    offset: number = 0,
  ): Promise<Doc<Collection>[]> {
    const query = [Query.limit(limit), Query.offset(offset)];

    return this.find(SYSTEM_CONTEXT, Database.METADATA, query);
  }

  /**
   * Gets the size of a collection.
   */
  public async getSizeOfCollection(collectionId: string): Promise<number> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    if (
      this.adapter.$sharedTables &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      throw new NotFoundException(`Collection '${collectionId}' not found`);
    }

    return this.adapter.getSizeOfCollection(collection.getId());
  }

  /**
   * Gets the size of a collection on Disk.
   */
  public async getSizeOfCollectionOnDisk(
    collectionId: string,
  ): Promise<number> {
    if (this.adapter.$sharedTables && !this.adapter.$tenantId) {
      throw new DatabaseException(
        "Missing tenant. Tenant must be set when table sharing is enabled.",
      );
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    if (
      this.adapter.$sharedTables &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      throw new NotFoundException(`Collection '${collectionId}' not found`);
    }

    return this.adapter.getSizeOfCollectionOnDisk(collection.getId());
  }

  /**
   * Analyze collection.
   */
  public async analyzeCollection(collection: string): Promise<boolean> {
    return this.adapter.analyzeCollection(collection);
  }

  /**
   * Delete a collection by ID.
   */
  public async deleteCollection(id: string): Promise<boolean> {
    const collection = await this.silent(() =>
      this.getDocument(SYSTEM_CONTEXT, Database.METADATA, id),
    );

    if (collection.empty() || collection.getId() === Database.METADATA) {
      throw new NotFoundException(`Collection '${id}' not found`);
    }

    if (
      this.adapter.$sharedTables &&
      collection.getTenant() !== this.adapter.$tenantId
    ) {
      throw new NotFoundException(`Collection '${id}' not found`);
    }

    const relationships = collection
      .get("attributes", [])
      .filter(
        (attribute: Doc<Attribute>) =>
          attribute.get("type") === AttributeEnum.Relationship,
      );

    return await this.withTransaction(async (db) => {
      for (const relationship of relationships) {
        await db.deleteRelationship(
          collection.getId(),
          relationship.get("$id"),
        );
      }

      try {
        await db.adapter.deleteCollection(id);
      } catch (error) {
        if (error instanceof NotFoundException) {
          // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
          if (!db.adapter.$sharedTables || !this.migrating) {
            throw error;
          }
        } else {
          throw error;
        }
      }

      let deleted: boolean;
      if (id === Database.METADATA) {
        deleted = true;
      } else {
        deleted = await db.silent(() =>
          db.deleteDocument(SYSTEM_CONTEXT, Database.METADATA, id),
        );
      }

      if (deleted) {
        // todo:
        this.trigger(EventsEnum.CollectionDelete, collection);
      }

      await this.purgeCachedCollection(id);

      return deleted;
    });
  }

  /**
   * Creates an attribute in a collection.
   */
  public async createAttribute(collectionId: string, attribute: Attribute) {
    const type = attribute.type;
    if (type === AttributeEnum.Relationship || type === AttributeEnum.Virtual) {
      throw new DatabaseException(`Cannot create attribute of type '${type}'.`);
    }

    let collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const attr = await this.validateAttribute(collection, attribute);

    collection.append("attributes", attr);

    try {
      await this.adapter.createAttribute({
        collection: collectionId,
        ...attribute,
      });
    } catch (error) {
      if (error instanceof DuplicateException) {
        // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
        if (!this.adapter.$sharedTables || !this.migrating) {
          throw error;
        }
      } else throw error;
    }

    if (collection.getId() !== Database.METADATA) {
      collection = await this.silent(() =>
        this.updateDocument(SYSTEM_CONTEXT, Database.METADATA, collection.getId(), collection),
      );
    }

    this.trigger(EventsEnum.AttributeCreate, collection, attr);
    return true;
  }

  /**
   * Creates multiple attributes in a collection.
   */
  public async createAttributes(collectionId: string, attributes: Attribute[]) {
    if (attributes.length === 0) {
      throw new DatabaseException("No attributes to create");
    }

    let collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const attrDocs: Doc<Attribute>[] = [];

    for (const attribute of attributes) {
      const attr = await this.validateAttribute(collection, attribute);

      collection.append("attributes", attr);
      attrDocs.push(attr);
    }

    try {
      await this.adapter.createAttributes(collection.getId(), attributes);
    } catch (error) {
      if (error instanceof DuplicateException) {
        // No attributes were in a metadata, but at least one of them was present on the table
        // HACK: Metadata should still be updated, can be removed when null tenant collections are supported.
        if (!this.adapter.$sharedTables || !this.migrating) {
          throw error;
        }
      }
      throw error;
    }

    if (collection.getId() !== Database.METADATA) {
      collection = await this.silent(() =>
        this.updateDocument(SYSTEM_CONTEXT, Database.METADATA, collection.getId(), collection),
      );
    }

    this.purgeCachedCollection(collection);
    this.purgeCachedDocument(Database.METADATA, collection);

    this.trigger(EventsEnum.AttributesCreate, collection, attrDocs);
    return true;
  }

  /**
   * Update index metadata. Utility method for update index methods.
   */
  protected async updateIndexMeta(
    collectionId: string,
    id: string,
    updateCallback: (
      index: Doc<Index>,
      collection: Doc<Collection>,
      indexPosition: number,
    ) => void,
  ): Promise<Doc<Index>> {
    return updateIndexMeta(this, collectionId, id, updateCallback);
  }

  /**
   * Update attribute metadata. Utility method for update attribute methods.
   *
   * Public (widened from protected) so the extracted relationship-schema
   * module can drive metadata updates through the Database instance —
   * standalone functions cannot access protected members.
   */
  public async updateAttributeMeta(
    collectionId: string,
    id: string,
    updateCallback: (
      attribute: Doc<Attribute>,
      collection: Doc<Collection>,
      index: number,
    ) => void | Promise<void>,
  ): Promise<Doc<Attribute>> {
    let collection = await this.silent(() => this.getCollection(collectionId));

    if (collection.getId() === Database.METADATA) {
      throw new DatabaseException("Cannot update metadata attributes");
    }

    const attributes = collection.get("attributes", []);
    const index = attributes.findIndex(
      (attribute: Doc<Attribute>) => attribute.get("$id") === id,
    );

    if (index === -1) {
      throw new NotFoundException("Attribute not found");
    }

    // Execute update from callback
    const res = updateCallback(attributes[index]!, collection, index);
    if (res instanceof Promise) {
      await res;
    }

    // Save
    collection.set("attributes", attributes);
    await this.silent(() =>
      this.updateDocument(SYSTEM_CONTEXT, Database.METADATA, collection.getId(), collection),
    );

    this.trigger(EventsEnum.AttributeUpdate, collection, attributes[index]!);

    return attributes[index]!;
  }

  /**
   * Update required status of attribute.
   */
  public async updateAttributeRequired(
    collectionId: string,
    id: string,
    required: boolean,
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      attribute.set("required", required);
    });
  }

  /**
   * Update format of attribute.
   */
  public async updateAttributeFormat(
    collectionId: string,
    id: string,
    format: string,
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      if (!Structure.hasFormat(format, attribute.get("type"))) {
        throw new DatabaseException(
          `Format "${format}" not available for attribute type "${attribute.get("type")}"`,
        );
      }

      attribute.set("format", format);
    });
  }

  /**
   * Update format options of attribute.
   */
  public async updateAttributeFormatOptions(
    collectionId: string,
    id: string,
    formatOptions: Record<string, any>,
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      attribute.set("formatOptions", formatOptions);
    });
  }

  /**
   * Update filters of attribute.
   */
  public async updateAttributeFilters(
    collectionId: string,
    id: string,
    filters: string[],
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      attribute.set("filters", filters);
    });
  }

  /**
   * Update default value of attribute.
   */
  public async updateAttributeDefault(
    collectionId: string,
    id: string,
    defaultValue: any = null,
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(collectionId, id, (attribute) => {
      if (attribute.get("required") === true) {
        throw new DatabaseException(
          "Cannot set a default value on a required attribute",
        );
      }

      this.validateDefaultTypes(attribute.get("type"), defaultValue);

      attribute.set("default", defaultValue);
    });
  }

  /**
   * Update an attribute in a collection.
   */
  public async updateAttribute(
    collectionId: string,
    id: string,
    options: {
      type?: AttributeEnum;
      size?: number;
      required?: boolean;
      default?: any;
      array?: boolean;
      format?: string;
      formatOptions?: Record<string, any>;
      filters?: string[];
      newKey?: string;
    } = {},
  ): Promise<Doc<Attribute>> {
    return this.updateAttributeMeta(
      collectionId,
      id,
      async (attribute, collection, attributeIndex) => {
        const {
          type = attribute.get("type"),
          size = attribute.get("size"),
          required = attribute.get("required"),
          default: defaultValue = attribute.get("default"),
          array = attribute.get("array"),
          format = attribute.get("format"),
          formatOptions = attribute.get("formatOptions"),
          filters = attribute.get("filters"),
          newKey,
        } = options;

        const altering =
          options.type !== undefined ||
          options.size !== undefined ||
          options.array !== undefined ||
          options.newKey !== undefined;

        const finalDefault =
          required === true && defaultValue !== null ? null : defaultValue;

        switch (type) {
          case AttributeEnum.String:
            if (!size) {
              throw new DatabaseException("Size length is required");
            }
            if (size > this.adapter.$limitForString) {
              throw new DatabaseException(
                `Max size allowed for string is: ${this.adapter.$limitForString}`,
              );
            }
            break;

          case AttributeEnum.Integer:
            if (size && size > this.adapter.$limitForInt) {
              throw new DatabaseException(
                `Max size allowed for int is: ${this.adapter.$limitForInt}`,
              );
            }
            break;

          case AttributeEnum.Float:
          case AttributeEnum.Boolean:
          case AttributeEnum.Json:
          case AttributeEnum.Uuid:
          case AttributeEnum.Timestamptz:
            if (size) {
              throw new DatabaseException("Size must be empty");
            }
            break;
          default:
            throw new DatabaseException(`Unknown attribute type: ${type}`);
        }

        if (format && !Structure.hasFormat(format, type)) {
          throw new DatabaseException(
            `Format "${format}" not available for attribute type "${type}"`,
          );
        }

        // Validate default value
        if (finalDefault !== null) {
          if (required) {
            throw new DatabaseException(
              "Cannot set a default value on a required attribute",
            );
          }
          this.validateDefaultTypes(type, finalDefault);
        }

        const updatedId = newKey ?? id;
        attribute
          .set("$id", updatedId)
          .set("key", updatedId)
          .set("type", type)
          .set("size", size)
          .set("array", array)
          .set("format", format)
          .set("formatOptions", formatOptions)
          .set("filters", filters)
          .set("required", required)
          .set("default", finalDefault);

        const attributes = collection.get("attributes", []);
        attributes[attributeIndex] = attribute;
        collection.set("attributes", attributes);

        if (
          this.adapter.$documentSizeLimit > 0 &&
          this.adapter.getAttributeWidth(collection) >=
            this.adapter.$documentSizeLimit
        ) {
          throw new LimitException(
            "Row width limit reached. Cannot update attribute.",
          );
        }

        if (altering) {
          const indexes = collection.get("indexes", []);

          // Update index attribute references if key changed
          if (newKey && id !== newKey) {
            indexes.forEach((index) => {
              const indexAttributes = index.get("attributes", []);
              if (indexAttributes.includes(id)) {
                const updatedAttributes = indexAttributes.map((attr) =>
                  attr === id ? newKey : attr,
                );
                index.set("attributes", updatedAttributes);
              }
            });
          }

          if (this.validate) {
            const validator = new IndexValidator(
              attributes,
              this.adapter.$maxIndexLength,
              this.adapter.$internalIndexesKeys,
              this.adapter.$supportForIndexArray,
            );

            indexes.forEach((index) => {
              if (!validator.$valid(index)) {
                throw new IndexException(validator.$description);
              }
            });
          }

          await this.adapter.updateAttribute({
            key: id,
            collection: collectionId,
            type,
            size,
            array,
            newName: newKey,
          });
          await this.purgeCachedCollection(collection);
        }

        await this.purgeCachedDocument(Database.METADATA, collection);
      },
    );
  }

  /**
   * Deletes an attribute from a collection.
   */
  public async deleteAttribute(
    collectionId: string,
    attributeId: string,
  ): Promise<boolean> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    if (collection.getId() === Database.METADATA) {
      throw new DatabaseException("Cannot delete metadata attributes");
    }

    const attributes = collection.get("attributes", []);
    const indexes = collection.get("indexes", []);

    const attributeIndex = attributes.findIndex(
      (attr: Doc<Attribute>) => attr.get("$id") === attributeId,
    );
    if (attributeIndex === -1) {
      throw new NotFoundException("Attribute not found");
    }

    const attribute = attributes[attributeIndex]!;
    if (attribute.get("type") === AttributeEnum.Relationship) {
      throw new DatabaseException("Cannot delete relationship as an attribute");
    }
    if (attribute.get("type") === AttributeEnum.Virtual) {
      throw new DatabaseException("Cannot delete virtual attribute");
    }

    if (this.validate) {
      const validator = new IndexDependency(
        indexes,
        this.adapter.$supportForCastIndexArray,
      );

      if (!validator.$valid(attribute)) {
        throw new DependencyException(validator.$description);
      }
    }

    // Remove attribute from indexes
    for (const index of indexes) {
      const indexAttributes = index.get("attributes", []);
      const updatedAttributes = indexAttributes.filter(
        (attr) => attr !== attributeId,
      );

      if (updatedAttributes.length === 0) {
        indexes.splice(indexes.indexOf(index), 1);
      } else {
        index.set("attributes", updatedAttributes);
      }
    }

    // Remove attribute from collection
    attributes.splice(attributeIndex, 1);
    collection.set("attributes", attributes);
    collection.set("indexes", indexes);

    try {
      await this.adapter.deleteAttribute(collection.getId(), attributeId);
    } catch (error) {
      if (!(error instanceof NotFoundException)) {
        throw error;
      }
    }

    if (collection.getId() !== Database.METADATA) {
      await this.silent(() =>
        this.updateDocument(SYSTEM_CONTEXT, Database.METADATA, collection.getId(), collection),
      );
    }

    await this.purgeCachedCollection(collection);
    await this.purgeCachedDocument(Database.METADATA, collection);

    this.trigger(EventsEnum.AttributeDelete, collection, attribute);

    return true;
  }

  /**
   * Renames an attribute in a collection.
   */
  public async renameAttribute(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    if (collection.empty()) {
      throw new NotFoundException(`Collection '${collectionId}' not found`);
    }

    const attributes = collection.get("attributes", []);
    const indexes = collection.get("indexes", []);

    const attribute = attributes.find((attr) => attr.get("$id") === oldName);
    if (!attribute) {
      throw new NotFoundException(`Attribute '${oldName}' not found`);
    }

    if (attributes.some((attr) => attr.get("$id") === newName)) {
      throw new DuplicateException(`Attribute name '${newName}' already used`);
    }

    if (this.validate) {
      const validator = new IndexDependency(
        indexes,
        this.adapter.$supportForCastIndexArray,
      );

      if (!validator.$valid(attribute)) {
        throw new DependencyException(validator.$description);
      }
    }

    attribute.set("$id", newName);
    attribute.set("key", newName);

    for (const index of indexes) {
      const indexAttributes = index.get("attributes", []);
      const updatedAttributes = indexAttributes.map((attr) =>
        attr === oldName ? newName : attr,
      );
      index.set("attributes", updatedAttributes);
    }

    await this.adapter.renameAttribute(collection.getId(), oldName, newName);

    collection.set("attributes", attributes);
    collection.set("indexes", indexes);

    if (collection.getId() !== Database.METADATA) {
      await this.silent(() =>
        this.updateDocument(SYSTEM_CONTEXT, Database.METADATA, collection.getId(), collection),
      );
    }

    this.trigger(EventsEnum.AttributeUpdate, collection, attribute);

    return true;
  }

  /**
   * Creates a relationship between two collections.
   */
  public async createRelationship({
    collectionId,
    relatedCollectionId,
    type,
    twoWay = false,
    id,
    twoWayKey,
    onDelete = OnDelete.Restrict,
  }: CreateRelationshipAttribute): Promise<boolean> {
    return createRelationship(this, {
      collectionId,
      relatedCollectionId,
      type,
      twoWay,
      id,
      twoWayKey,
      onDelete,
    });
  }

  /**
   * Updates an existing relationship in a collection.
   */
  public async updateRelationship({
    collectionId,
    id,
    newKey,
    newTwoWayKey,
    twoWay,
    onDelete,
  }: UpdateRelationshipAttribute): Promise<boolean> {
    return updateRelationship(this, {
      collectionId,
      id,
      newKey,
      newTwoWayKey,
      twoWay,
      onDelete,
    });
  }

  /**
   * Deletes a relationship between two collections.
   */
  public async deleteRelationship(
    collectionId: string,
    id: string,
  ): Promise<boolean> {
    return deleteRelationship(this, collectionId, id);
  }

  /**
   * Renames an index in a collection.
   */
  public async renameIndex(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    return renameIndex(this, collectionId, oldName, newName);
  }

  /**
   * Creates an index in a collection.
   */
  public async createIndex(
    collectionId: string,
    id: string,
    type: string,
    attributes: string[],
    orders: (string | null)[] = [],
  ): Promise<boolean> {
    return createIndex(
      this,
      this.validate,
      collectionId,
      id,
      type,
      attributes,
      orders,
    );
  }

  /**
   * Delete an index in a collection.
   */
  public async deleteIndex(collectionId: string, id: string): Promise<boolean> {
    return deleteIndex(this, collectionId, id);
  }

  /**
   * Get a document by ID.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}. The session's AuthContext gates cached and fresh reads
   * against collection/document read permissions.
   */
  private async getDocument(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    forUpdate: boolean = false,
  ): Promise<any> {
    if (collectionId === Database.METADATA && id === Database.METADATA) {
      return new Doc(Database.COLLECTION);
    }

    if (!collectionId) {
      throw new NotFoundException(`Collection '${collectionId}' not found.`);
    }
    if (!id) {
      return new Doc();
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const processedQuery = await this.processQueries(ctx, query, collection, {
      forUpdate,
      overrideValidators: [MethodType.Populate, MethodType.Select],
    });
    const processedQueryClone = { ...processedQuery };

    let doc: Doc<any>;
    const { collectionKey, documentKey, filtersHash } = this.getCacheKeys(
      collectionId,
      id,
      processedQueryClone,
    );
    const cacheKey = `${documentKey}${filtersHash ? ":" + filtersHash : ""}`;

    if (!processedQuery.populateQueries?.length) {
      const documentSecurity = collection.get("documentSecurity", false);

      let cached: any;

      try {
        cached = await this.cache.get(cacheKey, {
          ttl: Database.TTL,
          tags: [collectionKey, documentKey!],
        });
      } catch (e) {
        this.logger.warn(`Failed to load document '${id}' from cache: ${e}`);
      }

      if (cached) {
        doc = new Doc(cached);

        if (collection.getId() !== Database.METADATA) {
          const readPermissions = [
            ...collection.getRead(),
            ...(documentSecurity ? doc.getRead() : []),
          ];

          if (!authorize(ctx, readPermissions, PermissionEnum.Read)) {
            return new Doc();
          }
        }

        this.trigger(EventsEnum.DocumentRead, doc);
        return doc;
      }

      doc =
        (await this.adapter.getDocument(
          SYSTEM_CONTEXT,
          collection.getId(),
          id,
          processedQuery,
          forUpdate,
        )) || new Doc();

      if (!doc.empty() && collection.getId() !== Database.METADATA) {
        const readPermissions = [
          ...collection.getRead(),
          ...(documentSecurity ? doc.getRead() : []),
        ];

        if (!authorize(ctx, readPermissions, PermissionEnum.Read)) {
          return new Doc();
        }
      }
    } else {
      if (
        collection.getId() !== Database.METADATA &&
        !authorize(ctx, collection.getRead(), PermissionEnum.Read)
      ) {
        return new Doc();
      }

      const queryWithId = {
        ...processedQuery,
        filters: [Query.equal("$id", [id])],
      };

      const documents = await this.adapter.find(
        SYSTEM_CONTEXT,
        collectionId,
        queryWithId,
      );
      const processedDocuments = this.processFindResults(
        documents,
        queryWithId,
      );
      doc = processedDocuments[0] || new Doc();
    }

    if (doc.empty()) {
      return doc;
    }

    doc = this.cast(collection, doc);
    doc = await this.decode(processedQuery, doc);

    if (!processedQuery.populateQueries?.length) {
      try {
        await this.cache.set(cacheKey, doc.toObject(), {
          ttl: Database.TTL,
          tags: [collectionKey, documentKey!],
        });
      } catch (e) {
        this.logger.warn(`Failed to save document '${id}' to cache: ${e}`);
      }
    }

    this.trigger(EventsEnum.DocumentRead, doc);
    return doc;
  }

  /**
   * Create a new document.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async createDocument(
    ctx: AuthContext,
    collectionId: string,
    document: Doc<Record<string, any>> | Record<string, any>,
  ): Promise<Doc<any>> {
    if (
      collectionId !== Database.METADATA &&
      this.adapter.$sharedTables &&
      !this.adapter.$tenantPerDocument &&
      !this.adapter.$tenantId
    ) {
      throw new DatabaseException(
        "Missing tenant. Tenant must be set when table sharing is enabled.",
      );
    }

    if (!this.adapter.$sharedTables && this.adapter.$tenantPerDocument) {
      throw new DatabaseException(
        "Shared tables must be enabled if tenant per document is enabled.",
      );
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    if (collection.getId() !== Database.METADATA) {
      ensureAuthorized(ctx, collection.getCreate(), PermissionEnum.Create);
    }

    const time = new Date().toISOString();
    let doc: Doc<any> = document instanceof Doc ? document : new Doc(document);

    const createdAt = doc.get("$createdAt");
    const updatedAt = doc.get("$updatedAt");

    doc
      .set("$id", doc.getId() ?? ID.unique())
      .set("$collection", collection.getId())
      .set(
        "$createdAt",
        createdAt === null || createdAt === undefined || !this.preserveDates
          ? time
          : createdAt,
      )
      .set(
        "$updatedAt",
        updatedAt === null || updatedAt === undefined || !this.preserveDates
          ? time
          : updatedAt,
      );

    if (this.adapter.$sharedTables) {
      if (this.adapter.$tenantPerDocument) {
        if (
          collection.getId() !== Database.METADATA &&
          doc.getTenant() === null
        ) {
          throw new DatabaseException(
            "Missing tenant. Tenant must be set when tenant per document is enabled.",
          );
        }
      } else {
        doc.set("$tenant", this.adapter.$tenantId);
      }
    }

    doc = await this.encode(collection, doc);

    if (this.validate) {
      const validator = new Permissions();
      if (!validator.$valid(doc.get("$permissions", []))) {
        throw new DatabaseException(validator.$description);
      }
    }

    const structure = new Structure(collection);
    if (!(await structure.$valid(doc, true))) {
      throw new StructureException(structure.$description);
    }

    const result = await this.withTransaction(async (db) => {
      doc = await this.silent(() => db.createRelationships(collection, doc));
      return db.adapter.createDocument(SYSTEM_CONTEXT, collection.getId(), doc);
    });

    const castedResult = this.cast(collection, result);
    const decodedResult = await this.decode(
      { collection, populateQueries: [] },
      castedResult,
    );

    this.trigger(EventsEnum.DocumentCreate, decodedResult);

    return decodedResult;
  }

  /**
   * Recursion-guard stack for relationship maintenance loops.
   *
   * Public (widened from the protected Base field `_relationStack`) so the
   * extracted document-relationships module can guard against infinite
   * recursion — standalone functions cannot access protected members.
   */
  public get relationStack(): Set<string> {
    return this._relationStack;
  }

  /**
   * Whether related documents must exist when linking relationships.
   *
   * Public (widened from the protected Base field `checkRelationshipsExist`)
   * so the extracted document-relationships module can honor
   * skipCheckRelationshipsExist — standalone functions cannot access
   * protected members.
   */
  public get checksRelationshipsExist(): boolean {
    return this.checkRelationshipsExist;
  }

  private async createRelationships(
    collection: Doc<Collection>,
    document: Doc<any>,
  ): Promise<Doc<any>> {
    return createRelationships(this, collection, document);
  }

  /**
   * Many-to-Many handling
   */
  private async handleManyToMany(
    collection: Doc<Collection>,
    document: Doc<any>,
    relationship: Doc<Attribute>,
    options: RelationOptions,
    setIds: string[] | null | undefined = undefined,
    connectIds: string[] = [],
    disconnectIds: string[] = [],
  ): Promise<void> {
    return handleManyToMany(
      this,
      collection,
      document,
      relationship,
      options,
      setIds,
      connectIds,
      disconnectIds,
    );
  }

  /**
   * Create multiple documents in a collection.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async createDocuments<D extends Doc<Record<string, any>>>(
    ctx: AuthContext,
    collectionId: string,
    documents: D[],
  ): Promise<Doc[]> {
    if (!documents || documents.length === 0) {
      return [];
    }
    if (collectionId === Database.METADATA) {
      throw new DatabaseException(
        "Cannot create documents in metadata collection",
      );
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    if (collection.getId() !== Database.METADATA) {
      ensureAuthorized(ctx, collection.getCreate(), PermissionEnum.Create);
    }

    const time = new Date().toISOString();
    const createdDocuments: Doc<any>[] = [];
    for (const document of documents) {
      let doc: Doc<any> =
        document instanceof Doc ? document : new Doc(document);

      const createdAt = doc.get("$createdAt");
      const updatedAt = doc.get("$updatedAt");

      doc
        .set("$id", doc.getId() ?? ID.unique())
        .set("$collection", collection.getId())
        .set(
          "$createdAt",
          createdAt === null || createdAt === undefined || !this.preserveDates
            ? time
            : createdAt,
        )
        .set(
          "$updatedAt",
          updatedAt === null || updatedAt === undefined || !this.preserveDates
            ? time
            : updatedAt,
        );

      if (this.adapter.$sharedTables) {
        if (this.adapter.$tenantPerDocument) {
          if (
            collection.getId() !== Database.METADATA &&
            doc.getTenant() === null
          ) {
            throw new DatabaseException(
              "Missing tenant. Tenant must be set when tenant per document is enabled.",
            );
          }
        } else {
          doc.set("$tenant", this.adapter.$tenantId);
        }
      }

      doc = await this.encode(collection, doc);

      if (this.validate) {
        const validator = new Permissions();
        if (!validator.$valid(doc.get("$permissions", []))) {
          throw new DatabaseException(validator.$description);
        }
      }

      const structure = new Structure(collection);
      if (!(await structure.$valid(doc, true))) {
        throw new StructureException(structure.$description);
      }

      createdDocuments.push(doc);
    }

    const updatedDocuments = await this.withTransaction(async (db) => {
      const resolvedDocuments = await Promise.all(
        createdDocuments.map((doc) => db.createRelationships(collection, doc)),
      );
      return db.adapter.createDocuments(
        SYSTEM_CONTEXT,
        collection.getId(),
        resolvedDocuments,
      );
    });
    const castedDocuments = updatedDocuments.map((doc) =>
      this.cast(collection, doc),
    );
    const decodedDocuments = await Promise.all(
      castedDocuments.map((doc) =>
        this.decode({ collection, populateQueries: [] }, doc),
      ),
    );

    return decodedDocuments as any[];
  }

  /**
   * Update a document.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async updateDocument(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    document: Doc<Record<string, any>> | Record<string, any>,
  ): Promise<Doc<any>> {
    if (!id) {
      throw new DatabaseException("Must define $id attribute");
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );
    const newUpdatedAt = document.updatedAt();
    const updatedDocument = await this.withTransaction(async (db) => {
      const time = new Date().toISOString();
      const old = await this.silent(() =>
        db.getDocument(SYSTEM_CONTEXT, collection.getId(), id, [], true),
      );

      if (old.empty()) {
        return new Doc();
      }

      let skipPermissionsUpdate = true;

      if (document.getPermissions()) {
        const originalPermissions = old.getPermissions();
        const currentPermissions = document.getPermissions();

        originalPermissions.sort();
        currentPermissions.sort();

        skipPermissionsUpdate =
          JSON.stringify(originalPermissions) ===
          JSON.stringify(currentPermissions);
      }

      const createdAt = document.createdAt();

      const mergedDocument: Record<string, any> = {
        ...old.toObject(),
        ...(document instanceof Doc ? document.toObject() : document),
        $collection: old.get("$collection"),
        $createdAt:
          createdAt === null || !this.preserveDates
            ? old.get("$createdAt")
            : createdAt,
      };

      if (db.adapter.$sharedTables) {
        mergedDocument["$tenant"] = old.get("$tenant");
      }

      const relationships = collection
        .get("attributes", [])
        .filter((attr) => attr.get("type") === AttributeEnum.Relationship);

      let shouldUpdate = false;

      if (collection.getId() !== Database.METADATA) {
        const documentSecurity = collection.get("documentSecurity", false);

        for (const key in mergedDocument) {
          const value = mergedDocument[key];
          const oldValue = old.get(key);

          if (relationships.some((rel) => rel.get("key") === key)) {
            if (value !== undefined) {
              shouldUpdate = true;
            }
          }

          if (value !== oldValue) {
            shouldUpdate = true;
            break;
          }
        }

        const updatePermissions = [
          ...collection.getUpdate(),
          ...(documentSecurity ? old.getUpdate() : []),
        ];

        const readPermissions = [
          ...collection.getRead(),
          ...(documentSecurity ? old.getRead() : []),
        ];

        if (
          shouldUpdate &&
          !authorize(ctx, updatePermissions, PermissionEnum.Update)
        ) {
          throw new AuthorizationException("Update not authorized");
        } else if (
          !shouldUpdate &&
          !authorize(ctx, readPermissions, PermissionEnum.Read)
        ) {
          throw new AuthorizationException("Read not authorized");
        }
      }

      if (shouldUpdate) {
        mergedDocument["$updatedAt"] =
          newUpdatedAt === null || !this.preserveDates ? time : newUpdatedAt;
      }
      let doc = new Doc(mergedDocument);
      const structureValidator = new Structure(collection);
      if (!structureValidator.$valid(doc)) {
        throw new StructureException(structureValidator.$description);
      }

      const encodedDocument = await db.encode(collection, doc);

      if (relationships.length > 0) {
        doc = await db.updateDocumentRelationships(collection, encodedDocument);
      }
      await db.adapter.updateDocument(
        SYSTEM_CONTEXT,
        collection.getId(),
        doc as Doc<IEntity>,
        skipPermissionsUpdate,
      );
      await db.purgeCachedDocument(collection.getId(), encodedDocument);

      return encodedDocument;
    });

    if (updatedDocument.empty()) {
      return updatedDocument;
    }

    const castedDocument = this.cast(collection, updatedDocument);
    const decodedDocument = await this.decode(
      { collection, populateQueries: [] },
      castedDocument,
    );

    this.trigger(EventsEnum.DocumentUpdate, decodedDocument);

    return decodedDocument;
  }

  /**
   * Update relationships of a document.
   */
  private async updateDocumentRelationships(
    collection: Doc<Collection>,
    document: Doc<Record<string, any>>,
  ) {
    return updateDocumentRelationships(this, collection, document);
  }

  /**
   * Update multiple documents in a collection.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async updateDocuments(
    ctx: AuthContext,
    collectionId: string,
    updates: Doc<Partial<IEntity> & Record<string, any>>,
    query: Query[] | ((qb: QueryBuilder) => QueryBuilder) = [],
    batchSize: number = Database.DEFAULT_BATCH_SIZE,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number> {
    if (updates.empty()) {
      return 0;
    }
    let queries: Query[];
    if (typeof query === "function") {
      queries = query(new QueryBuilder()).build();
    } else queries = query;

    batchSize = Math.min(1000, Math.max(1, batchSize));
    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    const documentSecurity = collection.get("documentSecurity", false);
    const updatePermissions = collection.getUpdate();
    const skipAuth = authorize(ctx, updatePermissions, PermissionEnum.Update);

    if (
      !skipAuth &&
      !documentSecurity &&
      collection.getId() !== Database.METADATA
    ) {
      throw new AuthorizationException(
        authorizationError(PermissionEnum.Update, updatePermissions, ctx.roles),
      );
    }

    const attributes = collection.get("attributes", []);
    const indexes = collection.get("indexes", []);

    if (this.validate) {
      const validator = new Documents(attributes, indexes, this.maxQueryValues);

      if (!validator.$valid(queries)) {
        throw new QueryException(validator.$description);
      }
    }

    const grouped = Query.groupByType(queries);
    let { limit, cursor } = grouped;

    if (cursor && cursor.getCollection() !== collection.getId()) {
      throw new DatabaseException(
        "Cursor document must be from the same Collection.",
      );
    }

    // Prepare updates document
    const updatesClone = updates.clone();
    updatesClone.delete("$id");
    updatesClone.delete("$tenant");

    if (updatesClone.createdAt() === null || !this.preserveDates) {
      updatesClone.delete("$createdAt");
    } else {
      updatesClone.set("$createdAt", updatesClone.createdAt());
    }

    if (this.adapter.$sharedTables) {
      updatesClone.set("$tenant", this.adapter.$tenantId);
    }

    const updatedAt = updatesClone.updatedAt();
    const time = new Date().toISOString();
    updatesClone.set(
      "$updatedAt",
      updatedAt === null || !this.preserveDates ? time : updatedAt,
    );

    const encodedUpdates = await this.encode(collection, updatesClone);

    // Validate structure
    const validator = new Structure(collection);
    if (!validator.$valid(encodedUpdates, false)) {
      throw new StructureException(validator.$description);
    }

    const originalLimit = limit;
    let last = cursor as Doc<any>;
    let modified = 0;

    while (true) {
      let currentBatchSize = batchSize;
      if (originalLimit !== null && originalLimit < batchSize) {
        currentBatchSize = originalLimit;
      }

      const batchQueries = [Query.limit(currentBatchSize)];
      if (last) {
        batchQueries.push(Query.cursorAfter(last));
      }

      const batch = await this.silent(() =>
        this.find(
          ctx,
          collection.getId(),
          [...batchQueries, ...queries],
          PermissionEnum.Update,
        ),
      );

      if (batch.length === 0) {
        break;
      }

      const currentPermissions = encodedUpdates.getPermissions();
      currentPermissions.sort();

      await this.withTransaction(async (db) => {
        const processedBatch: Doc<any>[] = [];

        for (let index = 0; index < batch.length; index++) {
          const document = batch[index]!;
          let skipPermissionsUpdate = true;

          if (encodedUpdates.has("$permissions")) {
            if (!document.has("$permissions")) {
              throw new QueryException("Permission document missing in select");
            }

            const originalPermissions = document.getPermissions();
            originalPermissions.sort();

            skipPermissionsUpdate =
              JSON.stringify(originalPermissions) ===
              JSON.stringify(currentPermissions);
          }

          document.set("$skipPermissionsUpdate", skipPermissionsUpdate);
          const newDocument = await this.silent(() =>
            db.updateDocumentRelationships(collection, document),
          );

          const merged = new Doc({
            ...newDocument.toObject(),
            ...encodedUpdates.toObject(),
          });

          // Check if document was updated after the request timestamp
          const oldUpdatedAt = new Date(document.updatedAt()!);
          if (this.timestamp && oldUpdatedAt > this.timestamp) {
            throw new ConflictException(
              "Document was updated after the request timestamp",
            );
          }

          const encodedDocument = await db.encode(collection, merged);
          processedBatch.push(encodedDocument);
        }

        await db.adapter.updateDocuments(
          SYSTEM_CONTEXT,
          collection.getId(),
          encodedUpdates,
          processedBatch,
        );
      });

      for (const doc of batch) {
        doc.delete("$skipPermissionsUpdate");

        await this.purgeCachedDocument(collection.getId(), doc.getId());
        const castedDoc = this.cast(collection, doc);
        const decodedDoc = await this.decode(
          { collection, populateQueries: [] },
          castedDoc,
        );

        try {
          if (onNext) {
            const result = onNext(decodedDoc);
            if (result instanceof Promise) {
              await result;
            }
          }
        } catch (error) {
          if (onError) {
            const errorResult = onError(error as Error);
            if (errorResult instanceof Promise) {
              await errorResult;
            }
          } else {
            throw error;
          }
        }
        modified++;
      }

      if (batch.length < currentBatchSize) {
        break;
      }

      last = batch[batch.length - 1]!;
    }

    this.trigger(
      EventsEnum.DocumentsUpdate,
      new Doc({
        $collection: collection.getId(),
        modified: modified,
      }),
    );

    return modified;
  }

  /**
   * Delete document by ID.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async deleteDocument(
    ctx: AuthContext,
    collectionId: string,
    id: string,
  ): Promise<boolean> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    let document!: Doc;
    const deleted = await this.withTransaction(async (db) => {
      // The pre-delete fetch is engine-internal: the permission decision is
      // made below against the caller's ctx (formerly a global-skip wrap).
      document = await this.silent(() =>
        db.getDocument(SYSTEM_CONTEXT, collection.getId(), id, [], true),
      );

      if (document.empty()) {
        return false;
      }

      if (collection.getId() !== Database.METADATA) {
        const documentSecurity = collection.get("documentSecurity", true);
        ensureAuthorized(
          ctx,
          [
            ...collection.getDelete(),
            ...(documentSecurity ? document.getDelete() : []),
          ],
          PermissionEnum.Delete,
        );
      }

      // Check if document was updated after the request timestamp
      const oldUpdatedAt = new Date(document.updatedAt()!);
      if (this.timestamp && oldUpdatedAt > this.timestamp) {
        throw new ConflictException(
          "Document was updated after the request timestamp",
        );
      }

      await this.silent(() =>
        db.deleteDocumentRelationships(collection, document),
      );
      const result = await db.adapter.deleteDocument(
        SYSTEM_CONTEXT,
        collection.getId(),
        document,
      );

      await db.purgeCachedDocument(collection.getId(), id);

      return result;
    });

    this.trigger(
      EventsEnum.DocumentDelete,
      deleted ? document : new Doc({ $id: id }),
    );

    return deleted;
  }

  /**
   * Delete multiple documents in a collection.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async deleteDocuments(
    ctx: AuthContext,
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
  ): Promise<string[]> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );
    let queries: Query[];
    if (typeof query === "function") {
      queries = query(new QueryBuilder()).build();
    } else queries = query ?? [];

    const deletedIds = await this.withTransaction(async (db) => {
      const processedQueries = await db.processQueries(ctx, queries, collection, {
        forPermission: PermissionEnum.Delete,
      });
      const result = await db.adapter.deleteDocuments(
        SYSTEM_CONTEXT,
        collection.getId(),
        processedQueries,
      );
      for (const id of result) {
        await db.purgeCachedDocument(collection.getId(), id);
        await db.silent(() =>
          db.deleteDocumentRelationships(
            collection,
            new Doc({
              $id: id,
              $collection: collection.getId(),
            }),
          ),
        );
      }
      return result;
    });

    return deletedIds;
  }

  /**
   * Delete multiple documents in a collection with batch processing.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async deleteDocumentsBatch(
    ctx: AuthContext,
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize: number = Database.DELETE_BATCH_SIZE,
    onNext?: (doc: Doc<any>, old: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number> {
    if (this.adapter.$sharedTables && !this.adapter.$tenantId) {
      throw new DatabaseException(
        "Missing tenant. Tenant must be set when table sharing is enabled.",
      );
    }

    batchSize = Math.min(Database.DELETE_BATCH_SIZE, Math.max(1, batchSize));
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    if (collection.empty()) {
      throw new NotFoundException("Collection not found");
    }

    const documentSecurity = collection.get("documentSecurity", false);
    const deletePermissions = collection.getDelete();
    const skipAuth = authorize(ctx, deletePermissions, PermissionEnum.Delete);

    if (
      !skipAuth &&
      !documentSecurity &&
      collection.getId() !== Database.METADATA
    ) {
      throw new AuthorizationException(
        authorizationError(PermissionEnum.Delete, deletePermissions, ctx.roles),
      );
    }

    let queries: Query[];
    if (typeof query === "function") {
      queries = query(new QueryBuilder()).build();
    } else {
      queries = query ?? [];
    }

    const attributes = collection.get("attributes", []);
    const indexes = collection.get("indexes", []);

    if (this.validate) {
      const validator = new Documents(attributes, indexes, this.maxQueryValues);

      if (!validator.$valid(queries)) {
        throw new QueryException(validator.$description);
      }
    }

    const grouped = Query.groupByType(queries);
    let { limit, cursor } = grouped;

    if (cursor && cursor.getCollection() !== collection.getId()) {
      throw new DatabaseException(
        "Cursor document must be from the same Collection.",
      );
    }

    const originalLimit = limit;
    let last = cursor as Doc<any>;
    let modified = 0;

    while (true) {
      let currentBatchSize = batchSize;
      if (limit && limit < batchSize && limit > 0) {
        currentBatchSize = limit;
      } else if (limit) {
        limit -= batchSize;
      }

      const batchQueries = [Query.limit(currentBatchSize)];
      if (last) {
        batchQueries.push(Query.cursorAfter(last));
      }

      const batch = await this.silent(() =>
        this.find(
          ctx,
          collection.getId(),
          [...batchQueries, ...queries],
          PermissionEnum.Delete,
        ),
      );

      if (batch.length === 0) {
        break;
      }

      const old = batch.map((doc) => doc.clone());
      const sequences: number[] = [];
      const permissionIds: string[] = [];

      await this.withTransaction(async (db) => {
        for (const document of batch) {
          sequences.push(document.getSequence());
          if (document.getPermissions().length > 0) {
            permissionIds.push(document.getId());
          }

          if (this.resolveRelationships) {
            await this.silent(() =>
              db.deleteDocumentRelationships(collection, document),
            );
          }

          // Check if document was updated after the request timestamp
          const oldUpdatedAt = new Date(document.updatedAt()!);
          if (this.timestamp && oldUpdatedAt > this.timestamp) {
            throw new ConflictException(
              "Document was updated after the request timestamp",
            );
          }
        }

        await db.adapter.deleteDocumentsBySequences(
          SYSTEM_CONTEXT,
          collection.getId(),
          sequences,
          permissionIds,
        );
      });

      for (let index = 0; index < batch.length; index++) {
        const document = batch[index]!;
        const oldDocument = old[index]!;

        if (this.adapter.$sharedTables && this.adapter.$tenantPerDocument) {
          await this.withTenant(document.getTenant(), () =>
            this.purgeCachedDocument(collection.getId(), document.getId()),
          );
        } else {
          await this.purgeCachedDocument(collection.getId(), document.getId());
        }

        try {
          if (onNext) {
            const result = onNext(document, oldDocument);
            if (result instanceof Promise) {
              await result;
            }
          }
        } catch (error) {
          if (onError) {
            const errorResult = onError(error as Error);
            if (errorResult instanceof Promise) {
              await errorResult;
            }
          } else {
            throw error;
          }
        }
        modified++;
      }

      if (batch.length < currentBatchSize) {
        break;
      } else if (originalLimit && modified >= originalLimit) {
        break;
      }

      last = batch[batch.length - 1]!;
    }

    this.trigger(
      EventsEnum.DocumentsDelete,
      new Doc({
        $collection: collection.getId(),
        modified: modified,
      }),
    );

    return modified;
  }

  /**
   * Delete all relationships of a document.
   */
  private async deleteDocumentRelationships(
    collection: Doc<Collection>,
    document: Doc<Record<string, any>>,
  ) {
    return deleteDocumentRelationships(this, collection, document);
  }

  /**
   * Handle deletion of related documents based on the relationship options.
   * This method is called when a document is deleted and handles the cascading effects
   * according to the `onDelete` option specified in the relationship.
   */
  private async handleOnDelete(
    collection: Doc<Collection>,
    document: Doc<Record<string, any>>,
    relationship: Doc<Attribute>,
    options: RelationOptions,
  ): Promise<void> {
    return handleOnDelete(this, collection, document, relationship, options);
  }

  /**
   * Create or update documents in a collection.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async createOrUpdateDocuments(
    ctx: AuthContext,
    collectionId: string,
    documents: Doc<Record<string, any>>[],
    batchSize: number = Database.DEFAULT_BATCH_SIZE,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number> {
    return this.createOrUpdateDocumentsWithIncrease(
      ctx,
      collectionId,
      "",
      documents,
      batchSize,
      onNext,
    );
  }

  /**
   * Create or update documents, increasing the value of the given attribute by the value in each document.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async createOrUpdateDocumentsWithIncrease(
    ctx: AuthContext,
    collectionId: string,
    attribute: string,
    documents: Doc<Record<string, any>>[],
    batchSize: number = 1000,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number> {
    if (!documents || documents.length === 0) {
      return 0;
    }

    batchSize = Math.min(1000, Math.max(1, batchSize));
    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );
    const documentSecurity = collection.get("documentSecurity", false);
    const collectionAttributes = collection.get("attributes", []);
    const time = new Date().toISOString();
    let created = 0;
    let updated = 0;
    const seenIds: string[] = [];

    const processedDocuments: Array<{
      old: Doc<any>;
      new: Doc<any>;
    }> = [];

    for (let i = 0; i < documents.length; i++) {
      const document = documents[i]!;

      let old: Doc<any>;
      if (this.adapter.$sharedTables && this.adapter.$tenantPerDocument) {
        old = await this.withTenant(document.getTenant(), () =>
          this.silent(() =>
            this.getDocument(
              SYSTEM_CONTEXT,
              collection.getId(),
              document.getId(),
            ),
          ),
        );
      } else {
        old = await this.silent(() =>
          this.getDocument(SYSTEM_CONTEXT, collection.getId(), document.getId()),
        );
      }

      let skipPermissionsUpdate = true;

      if (document.has("$permissions")) {
        const originalPermissions = old.getPermissions();
        const currentPermissions = document.getPermissions();

        originalPermissions.sort();
        currentPermissions.sort();

        skipPermissionsUpdate =
          JSON.stringify(originalPermissions) ===
          JSON.stringify(currentPermissions);
      }

      if (
        !attribute &&
        skipPermissionsUpdate &&
        JSON.stringify(old.toObject([], ["$permissions"])) ===
          JSON.stringify(document.toObject([], ["$permissions"]))
      ) {
        // If not updating a single attribute and the
        // document is the same as the old one, skip it
        continue;
      }

      // Check permissions
      if (old.empty()) {
        ensureAuthorized(ctx, collection.getCreate(), PermissionEnum.Create);
      } else {
        ensureAuthorized(
          ctx,
          [
            ...collection.getUpdate(),
            ...(documentSecurity ? old.getUpdate() : []),
          ],
          PermissionEnum.Update,
        );
      }

      const updatedAt = document.updatedAt();
      const createdAt = document.createdAt();

      document
        .set("$id", document.getId() || ID.unique())
        .set("$collection", collection.getId())
        .set(
          "$updatedAt",
          updatedAt === null || !this.preserveDates ? time : updatedAt,
        )
        .delete("$sequence");

      if (createdAt === null || !this.preserveDates) {
        document.set("$createdAt", old.empty() ? time : old.createdAt());
      } else {
        document.set("$createdAt", createdAt);
      }

      // Force matching optional parameter sets
      for (const attr of collectionAttributes) {
        if (!attr.get("required") && !document.has(attr.get("$id"))) {
          document.set(
            attr.get("$id"),
            old.get(attr.get("$id"), attr.get("default", null)),
          );
        }
      }

      if (skipPermissionsUpdate) {
        document.set("$permissions", old.getPermissions());
      }

      if (this.adapter.$sharedTables) {
        if (this.adapter.$tenantPerDocument) {
          if (document.getTenant() === null) {
            throw new DatabaseException(
              "Missing tenant. Tenant must be set when tenant per document is enabled.",
            );
          }
          if (!old.empty() && old.getTenant() !== document.getTenant()) {
            throw new DatabaseException("Tenant cannot be changed.");
          }
        } else {
          document.set("$tenant", this.adapter.$tenantId);
        }
      }

      const encodedDocument = await this.encode(collection, document);
      const structureValidator = new Structure(collection);
      if (!(await structureValidator.$valid(encodedDocument))) {
        throw new StructureException(structureValidator.$description);
      }

      if (!old.empty()) {
        // Check if document was updated after the request timestamp
        const oldUpdatedAt = new Date(old.updatedAt()!);
        if (this.timestamp && oldUpdatedAt > this.timestamp) {
          throw new ConflictException(
            "Document was updated after the request timestamp",
          );
        }
      }

      if (this.resolveRelationships) {
        await this.silent(() =>
          this.createRelationships(collection, encodedDocument),
        );
      }

      seenIds.push(encodedDocument.getId());
      processedDocuments.push({
        old,
        new: encodedDocument,
      });
    }

    // Required because *some* DBs will allow duplicate IDs for upsert
    if (seenIds.length !== new Set(seenIds).size) {
      throw new DuplicateException(
        "Duplicate document IDs found in the input array.",
      );
    }

    // Process in batches
    const chunks = [];
    for (let i = 0; i < processedDocuments.length; i += batchSize) {
      chunks.push(processedDocuments.slice(i, i + batchSize));
    }

    for (const chunk of chunks) {
      const batch = await this.withTransaction((db) =>
        db.adapter.createOrUpdateDocuments(
          SYSTEM_CONTEXT,
          collection.getId(),
          attribute,
          chunk,
        ),
      );

      for (const change of chunk) {
        if (change.old.empty()) {
          created++;
        } else {
          updated++;
        }
      }

      for (const doc of batch) {
        let processedDoc = doc;
        processedDoc = this.cast(collection, processedDoc);
        processedDoc = await this.decode(
          { collection, populateQueries: [] },
          processedDoc,
        );

        if (this.adapter.$sharedTables && this.adapter.$tenantPerDocument) {
          await this.withTenant(processedDoc.getTenant(), () =>
            this.purgeCachedDocument(collection.getId(), processedDoc.getId()),
          );
        } else {
          await this.purgeCachedDocument(
            collection.getId(),
            processedDoc.getId(),
          );
        }

        if (onNext) {
          const result = onNext(processedDoc);
          if (result instanceof Promise) {
            await result;
          }
        }
      }
    }

    this.trigger(
      EventsEnum.DocumentsUpsert,
      new Doc({
        $collection: collection.getId(),
        created: created,
        updated: updated,
      }),
    );

    return created + updated;
  }

  /**
   * Increase a numeric attribute value in a document.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async increaseDocumentAttribute(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    attribute: string,
    value: number = 1,
    max?: number,
  ): Promise<Doc<any>> {
    if (value <= 0) {
      throw new DatabaseException("Value must be numeric and greater than 0");
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    const attr = collection
      .get("attributes", [])
      .find(
        (a: Doc<Attribute>) =>
          a.get("$id") === attribute || a.get("key") === attribute,
      );

    if (!attr) {
      throw new NotFoundException("Attribute not found");
    }

    if (
      !Database.NUMERIC_ATTRIBUTE_TYPES.has(attr.get("type")) ||
      attr.get("array")
    ) {
      throw new DatabaseException(
        "Attribute must be an integer or float and can not be an array.",
      );
    }

    const document = await this.withTransaction(async (db) => {
      // Engine-internal fetch; permission decision happens below against ctx
      // (formerly a global-skip wrap).
      const doc = await this.silent(() =>
        db.getDocument(SYSTEM_CONTEXT, collection.getId(), id, [], true),
      );

      if (doc.empty()) {
        throw new NotFoundException("Document not found");
      }

      if (collection.getId() !== Database.METADATA) {
        const documentSecurity = collection.get("documentSecurity", false);
        ensureAuthorized(
          ctx,
          [
            ...collection.getUpdate(),
            ...(documentSecurity ? doc.getUpdate() : []),
          ],
          PermissionEnum.Update,
        );
      }

      const currentValue = doc.get(attribute);
      if (max !== undefined && currentValue + value > max) {
        throw new LimitException(
          `Attribute value exceeds maximum limit: ${max}`,
        );
      }

      const time = new Date().toISOString();
      const updatedAt = doc.get("$updatedAt");
      const finalUpdatedAt =
        !updatedAt || !this.preserveDates ? time : updatedAt;
      const maxValue = max !== undefined ? max - value : undefined;

      await db.adapter.increaseDocumentAttribute({
        ctx: SYSTEM_CONTEXT,
        collection: collection.getId(),
        id,
        attribute,
        value,
        updatedAt: finalUpdatedAt as Date,
        max: maxValue,
      });

      return doc.set(attribute, currentValue + value);
    });

    await this.purgeCachedDocument(collection.getId(), id);

    this.trigger(EventsEnum.DocumentIncrease, document, value);

    return document;
  }

  /**
   * Decrease a numeric attribute value in a document.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async decreaseDocumentAttribute(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    attribute: string,
    value: number = 1,
    min?: number,
  ): Promise<Doc<any>> {
    if (value <= 0) {
      throw new DatabaseException("Value must be numeric and greater than 0");
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    const attr = collection
      .get("attributes", [])
      .find(
        (a: Doc<Attribute>) =>
          a.get("$id") === attribute || a.get("key") === attribute,
      );

    if (!attr) {
      throw new NotFoundException("Attribute not found");
    }

    if (
      !Database.NUMERIC_ATTRIBUTE_TYPES.has(attr.get("type")) ||
      attr.get("array")
    ) {
      throw new DatabaseException(
        "Attribute must be an integer or float and can not be an array.",
      );
    }

    const document = await this.withTransaction(async (db) => {
      // Engine-internal fetch; permission decision happens below against ctx
      // (formerly a global-skip wrap).
      const doc = await this.silent(() =>
        db.getDocument(SYSTEM_CONTEXT, collection.getId(), id, [], true),
      );

      if (doc.empty()) {
        throw new NotFoundException("Document not found");
      }

      if (collection.getId() !== Database.METADATA) {
        const documentSecurity = collection.get("documentSecurity", false);
        ensureAuthorized(
          ctx,
          [
            ...collection.getUpdate(),
            ...(documentSecurity ? doc.getUpdate() : []),
          ],
          PermissionEnum.Update,
        );
      }

      const currentValue = doc.get(attribute);
      if (min !== undefined && currentValue - value < min) {
        throw new LimitException(
          `Attribute value exceeds minimum limit: ${min}`,
        );
      }

      const time = new Date().toISOString();
      const updatedAt = doc.get("$updatedAt");
      const finalUpdatedAt = !updatedAt || !db.preserveDates ? time : updatedAt;
      const minValue = min !== undefined ? min + value : undefined;

      await db.adapter.increaseDocumentAttribute({
        ctx: SYSTEM_CONTEXT,
        collection: collection.getId(),
        id,
        attribute,
        value: value * -1,
        updatedAt: finalUpdatedAt as Date,
        min: minValue,
      });

      return doc.set(attribute, currentValue - value);
    });

    await this.purgeCachedDocument(collection.getId(), id);

    this.trigger(EventsEnum.DocumentDecrease, document, value);

    return document;
  }

  /**
   * find documents.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}. The ctx flows to the adapter, which applies the
   * per-document `_perms` filter for documentSecurity collections.
   */
  private async find(
    ctx: AuthContext,
    collectionId: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    forPermission: PermissionEnum = PermissionEnum.Read,
  ): Promise<Doc<any>[]> {
    if (!collectionId) {
      throw new NotFoundException(`Collection '${collectionId}' not found.`);
    }

    const collection = await this.silent(() =>
      this.getCollection(collectionId, true),
    );

    const queries: Query[] =
      typeof query === "function" ? query(new QueryBuilder()).build() : query;

    const processedQueries = await this.processQueries(
      ctx,
      queries,
      collection,
      { forPermission },
    );

    if (!processedQueries.authorized) {
      return [];
    }

    const rows = await this.adapter.find(ctx, collectionId, processedQueries);
    const result = this.processFindResults(rows, processedQueries);

    const castedResult = result.map((doc) => this.cast(collection, doc));
    const documents = await Promise.all(
      castedResult.map(async (doc) => {
        return this.filter ? await this.decode(processedQueries, doc) : doc;
      }),
    );

    this.trigger(EventsEnum.DocumentsFind, documents);

    return documents;
  }

  /**
   * find a single document.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async findOne(
    ctx: AuthContext,
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
  ): Promise<Doc> {
    const queries: Query[] = [Query.limit(1)];
    if (query && typeof query === "function") {
      queries.push(...query(new QueryBuilder()).build());
    } else {
      queries.push(...(query ?? []));
    }

    const result = await this.silent(() => this.find(ctx, collectionId, queries));
    this.trigger(EventsEnum.DocumentFind, result[0]);

    if (!result[0]) {
      return new Doc();
    }

    return result[0];
  }

  /**
   * Count documents in a collection.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async count(
    ctx: AuthContext,
    collectionId: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    max?: number,
  ): Promise<number> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );

    const queries: Query[] =
      typeof query === "function" ? query(new QueryBuilder()).build() : query;

    // A collection-level read grant sees every document, so the aggregate
    // runs privileged (SYSTEM_CONTEXT skips the adapter's per-document
    // `_perms` filter); otherwise the ctx filters rows per document.
    const skipAuth = authorize(ctx, collection.getRead(), PermissionEnum.Read);

    const processedQueries = await this.processQueries(
      ctx,
      queries,
      collection,
      {
        forPermission: PermissionEnum.Read,
        overrideValidators: [MethodType.Filter],
      },
    );

    const count = await this.adapter.count(
      skipAuth ? SYSTEM_CONTEXT : ctx,
      collection.getId(),
      processedQueries.filters,
      max,
    );

    this.trigger(EventsEnum.DocumentCount, count);

    return count;
  }

  /**
   * Sum an attribute for all documents in a collection.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  private async sum(
    ctx: AuthContext,
    collectionId: string,
    attribute: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    max?: number,
  ): Promise<number> {
    const collection = await this.silent(() =>
      this.getCollection(collectionId),
    );
    const queries: Query[] =
      typeof query === "function"
        ? query(new QueryBuilder()).build()
        : (query ?? []);

    const processedQueries = await this.processQueries(
      ctx,
      queries,
      collection,
      {
        forPermission: PermissionEnum.Read,
        overrideValidators: [MethodType.Filter],
      },
    );

    // Mirror find()/count(): a collection-level read grant sees every
    // document, so the aggregate must skip the per-document `_perms` filter.
    // Without this, sum() under-counts relative to find() for privileged
    // callers on documentSecurity collections.
    const skipAuth = authorize(ctx, collection.getRead(), PermissionEnum.Read);

    const sum = await this.adapter.sum(
      skipAuth ? SYSTEM_CONTEXT : ctx,
      collection.getId(),
      attribute,
      processedQueries.filters,
      max,
    );

    this.trigger(EventsEnum.DocumentSum, sum);

    return sum;
  }

  /**
   * Processes queries for a collection, validating and authorizing them.
   *
   * Internal ctx-first implementation; only reachable within this module.
   */
  private async processQueries(
    ctx: AuthContext,
    queries: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    collection: Doc<Collection>,
    {
      forPermission = PermissionEnum.Read,
      allowedValidators = Object.values(MethodType),
      throwOnUnAuthorization = true,
      overrideValidators,
      ...metadata
    }: Partial<Attribute["options"]> & {
      populated?: boolean;
      attribute?: string;
      allowedValidators?: MethodType[];
      overrideValidators?: MethodType[];
      forPermission?: PermissionEnum;
      throwOnUnAuthorization?: boolean;
    } = {},
  ): Promise<ProcessedQuery> {
    if (typeof queries === "function") {
      queries = queries(new QueryBuilder()).build();
    }
    let authorized = true;
    let skipAuth = false;
    const validators = overrideValidators ?? allowedValidators;

    const permissions = collection.getPermissionsByType(forPermission);
    skipAuth = authorize(ctx, permissions, forPermission);
    if (
      collection.getId() !== Database.METADATA &&
      !skipAuth &&
      !collection.get("documentSecurity", false)
    ) {
      if (!metadata.populated) {
        throw new AuthorizationException(
          authorizationError(forPermission, permissions, ctx.roles),
        );
      }
      if (throwOnUnAuthorization && metadata.populated) {
        throw new AuthorizationException(
          `Collection '${collection.getId()}' not authorized for '${forPermission}'. ${authorizationError(forPermission, permissions, ctx.roles)}`,
        );
      }
      authorized = false;
    }

    if (this.validate && queries.length && authorized) {
      const validator = new Documents(
        collection.get("attributes", []),
        collection.get("indexes", []),
        this.maxQueryValues,
        Object.fromEntries(validators.map((v) => [v, true])),
      );
      if (!validator.$valid(queries)) {
        throw new QueryException(validator.$description);
      }
    }

    let { populateQueries, selections, cursor, ...rest } =
      Query.groupByType(queries);
    const attributes = collection.get("attributes", []);
    const hasWildcardSelecton = selections.some((s) =>
      (s.getValues() as string[]).includes("*"),
    );

    if (selections.length > 0 && !hasWildcardSelecton) {
      const attributeMap = new Map(
        attributes.map((attr) => [attr.get("$id"), attr]),
      );

      for (const query of selections.map((s) => s.getValues()).flat()) {
        const attributeId = query as string;
        const attribute = attributeMap.get(attributeId);

        if (!attribute) {
          throw new QueryException(
            `Attribute '${attributeId}' not found in collection '${collection.getId()}'.`,
          );
        }

        const attributeType = attribute.get("type");
        if (
          attributeType === AttributeEnum.Relationship ||
          attributeType === AttributeEnum.Virtual
        ) {
          throw new QueryException(
            `Attribute '${attributeId}' of type '${attributeType}' cannot be selected directly. Use populate instead.`,
          );
        }
      }
    } else {
      selections = [
        Query.select(
          attributes
            .filter(
              (a) =>
                a.get("type") !== AttributeEnum.Relationship &&
                a.get("type") !== AttributeEnum.Virtual,
            )
            .map((a) => a.get("key", a.getId())),
        ),
      ];
    }

    if (cursor) {
      if (typeof cursor === "string") {
        // Engine-internal cursor resolution: the surrounding operation has
        // already been authorized against ctx.
        cursor = (await this.silent(() =>
          this.getDocument(
            SYSTEM_CONTEXT,
            collection.getId(),
            cursor as unknown as string,
          ),
        )) as Doc<IEntity>;
      }
      if (cursor.empty()) {
        throw new NotFoundException(
          `Cursor document not found in collection '${collection.getId()}'.`,
        );
      }
      if (cursor.getCollection() !== collection.getId()) {
        throw new QueryException(
          `Cursor document must be in the same collection '${collection.getId()}'.`,
        );
      }
    }

    if (!populateQueries.size) {
      return {
        collection,
        cursor,
        selections: selections
          .map((q) => q.getValues() as unknown as string[])
          .flat(),
        populateQueries: [],
        attribute: metadata.attribute,
        authorized,
        skipAuth,
        ...rest,
      };
    }

    if (populateQueries.has("*")) {
      // TODO: Handle case where '*' is used with other populate queries like ?populate=*,author={populate: *}
      if (populateQueries.size > 1) {
        throw new QueryException(
          `Cannot use '*' with other populate queries. Use '*' alone to populate all relationships.`,
        );
      }
      populateQueries = new Map();
      for (const attribute of attributes) {
        if (attribute.get("type") === AttributeEnum.Relationship) {
          const options = attribute.get("options", {}) as RelationOptions;
          if (!options.twoWay && options.side !== RelationSideEnum.Parent)
            continue;
          populateQueries.set(attribute.get("$id"), []);
        }
      }
    }

    const processedPopulateQueries: PopulateQuery[] = [];

    for (const [attribute, values] of populateQueries.entries()) {
      const attributeDoc = attributes.find(
        (attr) => attr.get("$id") === attribute,
      );
      if (!attributeDoc) {
        throw new QueryException(
          `Attribute '${attribute}' not found in collection '${collection.getId()}'.`,
        );
      }

      if (attributeDoc.get("type") !== AttributeEnum.Relationship) {
        throw new QueryException(
          `Attribute '${attribute}' is not a relationship and cannot be populated.`,
        );
      }

      if (!Array.isArray(values)) {
        throw new QueryException(
          `Populate query for attribute '${attribute}' must be an array of queries.`,
        );
      }

      const options = attributeDoc.get("options", {}) as RelationOptions;

      if (!options.twoWay && options.side !== RelationSideEnum.Parent) {
        throw new QueryException(
          `Attribute '${attribute}' is not a parent relationship and cannot be populated.`,
        );
      }

      const relatedCollectionId = options["relatedCollection"];
      const relatedCollection = await this.silent(() =>
        this.getCollection(relatedCollectionId),
      );
      if (relatedCollection.empty()) {
        throw new QueryException(
          `Collection '${relatedCollectionId}' not found for attribute '${attribute}'.`,
        );
      }

      const processedQueries = await this.processQueries(
        ctx,
        values,
        relatedCollection,
        {
          populated: true,
          attribute,
          ...options,
          allowedValidators: [
            MethodType.Select,
            MethodType.Populate,
            MethodType.Filter,
            MethodType.Order,
          ],
          overrideValidators,
          throwOnUnAuthorization,
          forPermission,
        },
      );
      processedPopulateQueries.push(processedQueries as PopulateQuery);
    }

    return {
      collection,
      selections: selections
        .map((q) => q.getValues() as unknown as string[])
        .flat(),
      populateQueries: processedPopulateQueries,
      attribute: metadata.attribute,
      authorized,
      skipAuth,
      cursor,
      ...rest,
    };
  }
}

export interface ProcessedQuery extends Omit<
  QueryByType,
  "selections" | "populateQueries"
> {
  collection: Doc<Collection>;
  selections: string[];
  populateQueries?: PopulateQuery[];
  attribute?: string;
  authorized?: boolean;
  skipAuth: boolean;
}

export type PopulateQuery = Omit<
  ProcessedQuery,
  "limit" | "offset" | "attribute"
> & {
  attribute: string;
};

export type DatabaseOptions = {
  tenant?: number;
  filters?: Filters;
  logger?: LoggerOptions | Logger;
};

/**
 * The ctx-first document-plane surface of a {@link Database} instance.
 *
 * Implemented by the module-private `documentPlane` symbol member on
 * Database and consumed exclusively by {@link Session}. Every operation
 * receives the session's immutable AuthContext as its first parameter.
 */
export interface DocumentPlane {
  find(
    ctx: AuthContext,
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forPermission?: PermissionEnum,
  ): Promise<Doc<any>[]>;
  findOne(
    ctx: AuthContext,
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
  ): Promise<Doc>;
  getDocument(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forUpdate?: boolean,
  ): Promise<any>;
  createDocument(
    ctx: AuthContext,
    collectionId: string,
    document: Doc<Record<string, any>> | Record<string, any>,
  ): Promise<Doc<any>>;
  createDocuments<D extends Doc<Record<string, any>>>(
    ctx: AuthContext,
    collectionId: string,
    documents: D[],
  ): Promise<Doc[]>;
  updateDocument(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    document: Doc<Record<string, any>> | Record<string, any>,
  ): Promise<Doc<any>>;
  updateDocuments(
    ctx: AuthContext,
    collectionId: string,
    updates: Doc<Partial<IEntity> & Record<string, any>>,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  deleteDocument(
    ctx: AuthContext,
    collectionId: string,
    id: string,
  ): Promise<boolean>;
  deleteDocuments(
    ctx: AuthContext,
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
  ): Promise<string[]>;
  deleteDocumentsBatch(
    ctx: AuthContext,
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: Doc<any>, old: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  createOrUpdateDocuments(
    ctx: AuthContext,
    collectionId: string,
    documents: Doc<Record<string, any>>[],
    batchSize?: number,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number>;
  createOrUpdateDocumentsWithIncrease(
    ctx: AuthContext,
    collectionId: string,
    attribute: string,
    documents: Doc<Record<string, any>>[],
    batchSize?: number,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number>;
  increaseDocumentAttribute(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    attribute: string,
    value?: number,
    max?: number,
  ): Promise<Doc<any>>;
  decreaseDocumentAttribute(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    attribute: string,
    value?: number,
    min?: number,
  ): Promise<Doc<any>>;
  count(
    ctx: AuthContext,
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    max?: number,
  ): Promise<number>;
  sum(
    ctx: AuthContext,
    collectionId: string,
    attribute: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    max?: number,
  ): Promise<number>;
  purgeCachedDocument(collectionId: string, doc: Doc<any> | string): Promise<void>;
  purgeCachedCollection(collection: Doc<Collection> | string): Promise<void>;
  withTransaction<T>(
    ctx: AuthContext,
    callback: (session: Session) => Promise<T>,
  ): Promise<T>;
}

/**
 * A scoped, document-plane view of a {@link Database} instance carrying an
 * immutable {@link AuthContext}.
 *
 * Sessions share the owning Database's adapter, cache and instance state —
 * creating one never opens a new pool or connection. They expose ONLY
 * document-plane operations; schema/admin operations remain on Database.
 * Authorization decisions are made against `session.ctx`, replacing the
 * removed static/global `Authorization` state.
 *
 * Obtain sessions via `db.for(...roles)` (scoped) or `db.system()`
 * (privileged, bypasses all checks).
 */
export class Session {
  constructor(
    private readonly database: Database,
    public readonly ctx: AuthContext,
  ) {}

  find<C extends string & keyof Entities>(
    collectionId: C,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
    forPermission?: PermissionEnum,
  ): Promise<Doc<Entities[C]>[]>;
  find<C extends string>(
    collectionId: C,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forPermission?: PermissionEnum,
  ): Promise<Doc<Partial<IEntity> & Record<string, any>>[]>;
  find<D extends Record<string, any>>(
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forPermission?: PermissionEnum,
  ): Promise<Doc<Partial<IEntity> & D>[]>;
  find(
    collectionId: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    forPermission: PermissionEnum = PermissionEnum.Read,
  ): Promise<Doc<any>[]> {
    return this.database[documentPlane].find(
      this.ctx,
      collectionId,
      query,
      forPermission,
    );
  }

  findOne<C extends string & keyof Entities>(
    collectionId: C,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
  ): Promise<Doc<Entities[C]>>;
  findOne<C extends string>(
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
  ): Promise<Doc<Partial<IEntity> & Record<string, any>>>;
  findOne(
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
  ): Promise<Doc> {
    return this.database[documentPlane].findOne(this.ctx, collectionId, query);
  }

  getDocument<C extends string & keyof Entities>(
    collectionId: C,
    id: string,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
    forUpdate?: boolean,
  ): Promise<Doc<Entities[C]>>;
  getDocument<D extends Record<string, any>>(
    collectionId: string,
    id: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forUpdate?: boolean,
  ): Promise<Doc<Partial<IEntity> & D>>;
  getDocument(
    collectionId: string,
    id: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forUpdate?: boolean,
  ): Promise<any> {
    return this.database[documentPlane].getDocument(
      this.ctx,
      collectionId,
      id,
      query,
      forUpdate,
    );
  }

  createDocument<C extends keyof Entities>(
    collectionId: C,
    document: Doc<Entities[C]> | Entities[C],
  ): Promise<Doc<Entities[C]>>;
  createDocument<D extends Record<string, any>>(
    collectionId: string,
    document: Doc<D> | D,
  ): Promise<Doc<D>>;
  createDocument(
    collectionId: string,
    document: Doc<Partial<IEntity>> | Partial<IEntity>,
  ): Promise<Doc<Partial<IEntity>>> {
    return this.database[documentPlane].createDocument(
      this.ctx,
      collectionId,
      document,
    );
  }

  createDocuments<C extends string & keyof Entities>(
    collectionId: C,
    documents: Doc<Entities[C]>[] | Entities[C][],
  ): Promise<Doc<Entities[C]>[]>;
  createDocuments<D extends Doc<Record<string, any>>>(
    collectionId: string,
    documents: D[],
  ): Promise<Doc[]>;
  createDocuments(
    collectionId: string,
    documents: Doc<any>[] | Record<string, any>[],
  ): Promise<Doc[]> {
    return this.database[documentPlane].createDocuments(
      this.ctx,
      collectionId,
      documents as Doc<any>[],
    );
  }

  updateDocument<C extends string & keyof Entities>(
    collectionId: C,
    id: string,
    document: Entities[C] | Doc<Entities[C]>,
  ): Promise<Doc<Entities[C]>>;
  updateDocument<D extends Doc<Record<string, any>>>(
    collectionId: string,
    id: string,
    document: D | Doc<D>,
  ): Promise<D>;
  updateDocument(
    collectionId: string,
    id: string,
    document: Doc<Record<string, any>> | Record<string, any>,
  ): Promise<Doc<any>> {
    return this.database[documentPlane].updateDocument(
      this.ctx,
      collectionId,
      id,
      document,
    );
  }

  updateDocuments<C extends string & keyof Entities>(
    collectionId: C,
    updates: Doc<Entities[C]>,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: Doc<Entities[C]>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  updateDocuments(
    collectionId: string,
    updates: Doc<any>,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  updateDocuments(
    collectionId: string,
    updates: Doc<any>,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number> {
    return this.database[documentPlane].updateDocuments(
      this.ctx,
      collectionId,
      updates,
      query,
      batchSize,
      onNext,
      onError,
    );
  }

  deleteDocument(collectionId: string, id: string): Promise<boolean> {
    return this.database[documentPlane].deleteDocument(
      this.ctx,
      collectionId,
      id,
    );
  }

  deleteDocuments<C extends string & keyof Entities>(
    collectionId: C,
    query?: Query[] | ((qb: QueryBuilder<C>) => QueryBuilder<C>),
  ): Promise<string[]>;
  deleteDocuments(
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
  ): Promise<string[]>;
  deleteDocuments(
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
  ): Promise<string[]> {
    return this.database[documentPlane].deleteDocuments(
      this.ctx,
      collectionId,
      query,
    );
  }

  deleteDocumentsBatch<C extends string & keyof Entities>(
    collectionId: C,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (
      doc: Doc<Entities[C]>,
      old: Doc<Entities[C]>,
    ) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  deleteDocumentsBatch(
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: Doc<any>, old: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number>;
  deleteDocumentsBatch(
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize?: number,
    onNext?: (doc: Doc<any>, old: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number> {
    return this.database[documentPlane].deleteDocumentsBatch(
      this.ctx,
      collectionId,
      query,
      batchSize,
      onNext,
      onError,
    );
  }

  createOrUpdateDocuments<C extends string & keyof Entities>(
    collectionId: C,
    documents: Doc<Entities[C]>[],
    batchSize?: number,
    onNext?: (doc: Doc<Entities[C]>) => void | Promise<void>,
  ): Promise<number>;
  createOrUpdateDocuments(
    collectionId: string,
    documents: Doc<any>[],
    batchSize?: number,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number>;
  createOrUpdateDocuments(
    collectionId: string,
    documents: Doc<any>[],
    batchSize?: number,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number> {
    return this.database[documentPlane].createOrUpdateDocuments(
      this.ctx,
      collectionId,
      documents,
      batchSize,
      onNext,
    );
  }

  createOrUpdateDocumentsWithIncrease<C extends string & keyof Entities>(
    collectionId: C,
    attribute: keyof Entities[C] & string,
    documents: Doc<Entities[C]>[],
    batchSize?: number,
    onNext?: (doc: Doc<Entities[C]>) => void | Promise<void>,
  ): Promise<number>;
  createOrUpdateDocumentsWithIncrease(
    collectionId: string,
    attribute: string,
    documents: Doc<any>[],
    batchSize?: number,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number>;
  createOrUpdateDocumentsWithIncrease(
    collectionId: string,
    attribute: string,
    documents: Doc<any>[],
    batchSize?: number,
    onNext?: (doc: Doc<any>) => void | Promise<void>,
  ): Promise<number> {
    return this.database[documentPlane].createOrUpdateDocumentsWithIncrease(
      this.ctx,
      collectionId,
      attribute,
      documents,
      batchSize,
      onNext,
    );
  }

  increaseDocumentAttribute<C extends string & keyof Entities>(
    collectionId: C,
    id: string,
    attribute: keyof Entities[C] & string,
    value?: number,
    max?: number,
  ): Promise<Doc<Entities[C]>>;
  increaseDocumentAttribute(
    collectionId: string,
    id: string,
    attribute: string,
    value?: number,
    max?: number,
  ): Promise<Doc<any>>;
  increaseDocumentAttribute(
    collectionId: string,
    id: string,
    attribute: string,
    value?: number,
    max?: number,
  ): Promise<Doc<any>> {
    return this.database[documentPlane].increaseDocumentAttribute(
      this.ctx,
      collectionId,
      id,
      attribute,
      value,
      max,
    );
  }

  decreaseDocumentAttribute<C extends string & keyof Entities>(
    collectionId: C,
    id: string,
    attribute: keyof Entities[C] & string,
    value?: number,
    min?: number,
  ): Promise<Doc<Entities[C]>>;
  decreaseDocumentAttribute(
    collectionId: string,
    id: string,
    attribute: string,
    value?: number,
    min?: number,
  ): Promise<Doc<any>>;
  decreaseDocumentAttribute(
    collectionId: string,
    id: string,
    attribute: string,
    value?: number,
    min?: number,
  ): Promise<Doc<any>> {
    return this.database[documentPlane].decreaseDocumentAttribute(
      this.ctx,
      collectionId,
      id,
      attribute,
      value,
      min,
    );
  }

  count<C extends string & keyof Entities>(
    collectionId: C,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
    max?: number,
  ): Promise<number>;
  count(
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    max?: number,
  ): Promise<number>;
  count(
    collectionId: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    max?: number,
  ): Promise<number> {
    return this.database[documentPlane].count(
      this.ctx,
      collectionId,
      query,
      max,
    );
  }

  sum<C extends string & keyof Entities>(
    collectionId: C,
    attribute: keyof Entities[C] & string,
    query?: ((builder: QueryBuilder<C>) => QueryBuilder<C>) | Query[],
    max?: number,
  ): Promise<number>;
  sum(
    collectionId: string,
    attribute: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    max?: number,
  ): Promise<number>;
  sum(
    collectionId: string,
    attribute: string,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    max?: number,
  ): Promise<number> {
    return this.database[documentPlane].sum(
      this.ctx,
      collectionId,
      attribute,
      query,
      max,
    );
  }

  /**
   * Purges all cached variants of a document. Cache maintenance only —
   * no authorization is involved.
   */
  purgeCachedDocument(collectionId: string, doc: Doc<any> | string): Promise<void> {
    return this.database[documentPlane].purgeCachedDocument(collectionId, doc);
  }

  /**
   * Purges every cached entry tagged for a collection. Cache maintenance
   * only — no authorization is involved.
   */
  purgeCachedCollection(collection: Doc<Collection> | string): Promise<void> {
    return this.database[documentPlane].purgeCachedCollection(collection);
  }

  /**
   * Runs the callback inside a transaction. The callback receives a
   * transactional session bound to the SAME AuthContext, so nested
   * document operations keep their authorization scope.
   */
  withTransaction<T>(callback: (txSession: Session) => Promise<T>): Promise<T> {
    return this.database[documentPlane].withTransaction(this.ctx, callback);
  }
}
