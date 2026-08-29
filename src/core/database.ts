import {
  AttributeEnum,
  EventsEnum,
  OnDelete,
  PermissionEnum,
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
import type { Entities } from "@nuvix/db";
import type { IEntity } from "types.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query } from "./query.js";
import { Doc } from "./doc.js";
import {
  AuthorizationException,
  ConflictException,
  DatabaseException,
  DuplicateException,
  LimitException,
  NotFoundException,
  QueryException,
  StructureException,
} from "@errors/index.js";
import { Permissions } from "@validators/permissions.js";
import { Documents } from "@validators/queries/documents.js";
import { authorize, AuthContext, SYSTEM_CONTEXT } from "./auth.js";
import { ID } from "@utils/id.js";
import { Structure } from "@validators/structure.js";
import type { DatabaseAdapter } from "@adapters/interface.js";
import { MethodType } from "@validators/query/base.js";
import {
  createRelationships,
  deleteDocumentRelationships,
  handleManyToMany,
  handleOnDelete,
  updateDocumentRelationships,
} from "./document-relationships.js";
import { Logger, LoggerOptions } from "@utils/logger.js";
import { SchemaManager } from "./schema-manager.js";
import { Session } from "./session.js";
import { documentPlane } from "./document-plane.js";
import type { DocumentPlane } from "./document-plane.js";
import { DocumentStore } from "./document-store.js";

/**
 * Rebuilds the error text previously produced by the mutable
 * former shared description side-channel so thrown messages keep their
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
  /**
   * Ctx-first document-plane operations bound to this instance. Keyed by the
   * `documentPlane` symbol from document-plane.ts; see {@link Session}.
   */
  public readonly [documentPlane]: DocumentPlane;

  /**
   * Admin/schema plane collaborator (see {@link SchemaManager}). Every
   * schema operation on Database delegates here. The injected closures read
   * mutable flags lazily, so transactional scopes observe their own
   * configuration (flags are copied onto scopes after construction).
   */
  private readonly schemaManager = new SchemaManager(this, {
    validate: () => this.validate,
    metadataCollection: Database.COLLECTION,
    validateDefaultTypes: (type, value) =>
      this.validateDefaultTypes(type, value),
    validateAttribute: (collection, attribute) =>
      this.validateAttribute(collection, attribute),
    assertCollectionEnabled: (collection) =>
      this.assertCollectionEnabled(collection),
  });

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
  protected createTransactionalScope(txAdapter: DatabaseAdapter): this {
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
    adapter: DatabaseAdapter,
    cache: CacheDriver,
    options: DatabaseOptions = {},
  ) {
    super(adapter, cache, options);

    this[documentPlane] = new DocumentStore(this, {
      validate: () => this.validate,
      filter: () => this.filter,
      timestamp: () => this.timestamp,
      preserveDates: () => this.preserveDates,
      maxQueryValues: () => this.maxQueryValues,
      resolveRelationships: () => this.resolveRelationships,
      logger: () => this.logger,
      metadataCollection: () => Database.COLLECTION,
      cast: (db, collection, document) => db.cast(collection, document),
      encode: (db, collection, document) => db.encode(collection, document),
      decode: (db, query, document) => db.decode(query, document),
      processFindResults: (db, documents, query) =>
        db.processFindResults(documents, query),
      getCacheKeys: (db, collectionId, id, processedQuery) =>
        db.getCacheKeys(collectionId, id, processedQuery),
    });
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

  /** Creates a new database. */
  public async create(database?: string): Promise<void> {
    return this.schemaManager.create(database);
  }

  /** Check is database or collection already exists or not. */
  public async exists<C extends keyof Entities>(
    database?: string,
    collection?: C,
  ): Promise<boolean>;
  public async exists(database?: string, collection?: string): Promise<boolean>;
  public async exists(
    database?: string,
    collection?: string,
  ): Promise<boolean> {
    return this.schemaManager.exists(database, collection);
  }

  /** list of databases. */
  public async list(): Promise<string[]> {
    return this.schemaManager.list();
  }

  /** Delete a database. */
  public async delete(database?: string): Promise<void> {
    return this.schemaManager.delete(database);
  }

  /** Creates a new collection in the database. */
  public async createCollection({
    id,
    attributes = [],
    indexes = [],
    permissions,
    documentSecurity,
    enabled,
  }: CreateCollection): Promise<Doc<Collection>> {
    return this.schemaManager.createCollection({
      id,
      attributes,
      indexes,
      permissions,
      documentSecurity,
      enabled,
    });
  }
  /** Update collection permissions & documentSecurity. */
  public async updateCollection({
    id,
    documentSecurity,
    permissions,
    enabled,
  }: UpdateCollection): Promise<Doc<Collection>> {
    return this.schemaManager.updateCollection({
      id,
      documentSecurity,
      permissions,
      enabled,
    });
  }

  /**
   * Retrieves a collection by its ID.
   * If the collection is not found or does not match the tenant ID, an empty Doc
   */
  public async getCollection(
    id: string,
    throwOnNotFound?: boolean,
  ): Promise<Doc<Collection>> {
    return this.schemaManager.getCollection(id, throwOnNotFound);
  }

  /** Lists all collections in the database. */
  public async listCollections(
    limit: number = 25,
    offset: number = 0,
  ): Promise<Doc<Collection>[]> {
    return this.schemaManager.listCollections(limit, offset);
  }

  /** Gets the size of a collection. */
  public async getSizeOfCollection(collectionId: string): Promise<number> {
    return this.schemaManager.getSizeOfCollection(collectionId);
  }

  /** Gets the size of a collection on Disk. */
  public async getSizeOfCollectionOnDisk(
    collectionId: string,
  ): Promise<number> {
    return this.schemaManager.getSizeOfCollectionOnDisk(collectionId);
  }

  /** Analyze collection. */
  public async analyzeCollection(collection: string): Promise<boolean> {
    return this.schemaManager.analyzeCollection(collection);
  }

  /** Delete a collection by ID. */
  public async deleteCollection(id: string): Promise<boolean> {
    return this.schemaManager.deleteCollection(id);
  }

  /** Creates an attribute in a collection. */
  public async createAttribute(collectionId: string, attribute: Attribute) {
    return this.schemaManager.createAttribute(collectionId, attribute);
  }
  /** Creates multiple attributes in a collection. */
  public async createAttributes(collectionId: string, attributes: Attribute[]) {
    return this.schemaManager.createAttributes(collectionId, attributes);
  }

  /** Update index metadata. Utility method for update index methods. */
  protected async updateIndexMeta(
    collectionId: string,
    id: string,
    updateCallback: (
      index: Doc<Index>,
      collection: Doc<Collection>,
      indexPosition: number,
    ) => void,
  ): Promise<Doc<Index>> {
    return this.schemaManager.updateIndexMeta(collectionId, id, updateCallback);
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
    return this.schemaManager.updateAttributeMeta(
      collectionId,
      id,
      updateCallback,
    );
  }

  /** Update required status of attribute. */
  public async updateAttributeRequired(
    collectionId: string,
    id: string,
    required: boolean,
  ): Promise<Doc<Attribute>> {
    return this.schemaManager.updateAttributeRequired(
      collectionId,
      id,
      required,
    );
  }
  /** Update format of attribute. */
  public async updateAttributeFormat(
    collectionId: string,
    id: string,
    format: string,
  ): Promise<Doc<Attribute>> {
    return this.schemaManager.updateAttributeFormat(collectionId, id, format);
  }
  /** Update format options of attribute. */
  public async updateAttributeFormatOptions(
    collectionId: string,
    id: string,
    formatOptions: Record<string, any>,
  ): Promise<Doc<Attribute>> {
    return this.schemaManager.updateAttributeFormatOptions(
      collectionId,
      id,
      formatOptions,
    );
  }
  /** Update filters of attribute. */
  public async updateAttributeFilters(
    collectionId: string,
    id: string,
    filters: string[],
  ): Promise<Doc<Attribute>> {
    return this.schemaManager.updateAttributeFilters(collectionId, id, filters);
  }
  /** Update default value of attribute. */
  public async updateAttributeDefault(
    collectionId: string,
    id: string,
    defaultValue: any = null,
  ): Promise<Doc<Attribute>> {
    return this.schemaManager.updateAttributeDefault(
      collectionId,
      id,
      defaultValue,
    );
  }

  /** Update an attribute in a collection. */
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
    return this.schemaManager.updateAttribute(collectionId, id, options);
  }

  /** Deletes an attribute from a collection. */
  public async deleteAttribute(
    collectionId: string,
    attributeId: string,
  ): Promise<boolean> {
    return this.schemaManager.deleteAttribute(collectionId, attributeId);
  }

  /** Renames an attribute in a collection. */
  public async renameAttribute(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    return this.schemaManager.renameAttribute(collectionId, oldName, newName);
  }

  /** Creates a relationship between two collections. */
  public async createRelationship({
    collectionId,
    relatedCollectionId,
    type,
    twoWay = false,
    id,
    twoWayKey,
    onDelete = OnDelete.Restrict,
  }: CreateRelationshipAttribute): Promise<boolean> {
    return this.schemaManager.createRelationship({
      collectionId,
      relatedCollectionId,
      type,
      twoWay,
      id,
      twoWayKey,
      onDelete,
    });
  }

  /** Updates an existing relationship in a collection. */
  public async updateRelationship({
    collectionId,
    id,
    newKey,
    newTwoWayKey,
    twoWay,
    onDelete,
  }: UpdateRelationshipAttribute): Promise<boolean> {
    return this.schemaManager.updateRelationship({
      collectionId,
      id,
      newKey,
      newTwoWayKey,
      twoWay,
      onDelete,
    });
  }

  /** Deletes a relationship between two collections. */
  public async deleteRelationship(
    collectionId: string,
    id: string,
  ): Promise<boolean> {
    return this.schemaManager.deleteRelationship(collectionId, id);
  }

  /** Renames an index in a collection. */
  public async renameIndex(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean> {
    return this.schemaManager.renameIndex(collectionId, oldName, newName);
  }

  /** Creates an index in a collection. */
  public async createIndex(
    collectionId: string,
    id: string,
    type: string,
    attributes: string[],
    orders: (string | null)[] = [],
  ): Promise<boolean> {
    return this.schemaManager.createIndex(
      collectionId,
      id,
      type,
      attributes,
      orders,
    );
  }

  /** Delete an index in a collection. */
  public async deleteIndex(collectionId: string, id: string): Promise<boolean> {
    return this.schemaManager.deleteIndex(collectionId, id);
  }

  public get relationStack(): Set<string> {
    return this._relationStack;
  }

  public get checksRelationshipsExist(): boolean {
    return this.checkRelationshipsExist;
  }

}

export type { ProcessedQuery, PopulateQuery } from "./document-store.js";

export type DatabaseOptions = {
  tenant?: number;
  filters?: Filters;
  logger?: LoggerOptions | Logger;
};

/**
 * The document plane lives in dedicated modules since Phase 2:
 * - session.ts        — the Session class (re-exported below)
 * - document-plane.ts — the shared `documentPlane` symbol and its surface
 * Both are re-exported here so the package's public export surface is
 * unchanged for consumers importing from "@nuvix/db".
 */
export { Session } from "./session.js";
export type { DocumentPlane } from "./document-plane.js";
