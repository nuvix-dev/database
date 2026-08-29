/**
 * Document plane of the {@link Database} facade.
 *
 * Extracted from the Database god class (Phase 2). Owns every ctx-first
 * document operation (find/get/create/update/delete/upsert/count/sum plus
 * query processing); Database installs an instance of this collaborator as
 * its `[documentPlane]` symbol member and {@link Session} dispatches every
 * document operation through it. Public behavior is unchanged.
 *
 * Database is imported type-only to avoid runtime circular imports —
 * the same pattern used by schema-manager.ts, index-manager.ts and
 * relationship-schema.ts.
 *
 * Members that standalone modules cannot reach through the type system
 * (the protected Base flags, introspection helpers and the metadata
 * collection constant) are injected by the Database constructor as bound
 * closures. The closures read state lazily and receive the owning Database
 * as their first argument, so every transactional scope observes its own
 * configuration and protected access stays inside database.ts.
 */
import type { Database } from "./database.js";
import { Base } from "./base.js";
import {
  AttributeEnum,
  EventsEnum,
  PermissionEnum,
} from "./enums.js";
import type { Attribute, Collection, RelationOptions } from "@validators/schema.js";
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
import { MethodType } from "@validators/query/base.js";
import { Logger } from "@utils/logger.js";
import type { IEntity } from "types.js";
import { Session } from "./session.js";
import { documentPlane } from "./document-plane.js";
import type { DocumentPlane } from "./document-plane.js";
import type { QueryByType } from "./types.js";
import { RelationSideEnum } from "./enums.js";
import {
  createRelationships,
  deleteDocumentRelationships,
  updateDocumentRelationships,
} from "./document-relationships.js";

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

/** Attribute types whose values may be incremented/decremented atomically. */
const NUMERIC_ATTRIBUTE_TYPES: ReadonlySet<AttributeEnum> = new Set([
  AttributeEnum.Integer,
  AttributeEnum.Float,
]);

/**
 * Privileged operations the document plane needs from its owning Database.
 * Injected rather than reached through the type system because the
 * underlying Base members are protected; each closure receives the Database
 * instance to operate on (the transaction scope), so protected access stays
 * inside database.ts while scope-bound semantics are preserved.
 */
export interface DocumentStoreInternals {
  /** Current validation flag (mutable per scope via enable/disableValidation). */
  readonly validate: () => boolean;
  /** Current filter flag (mutable per scope). */
  readonly filter: () => boolean;
  /** Current timestamps flag (mutable per scope). */
  readonly timestamp: () => Date | undefined;
  /** Current preserveDates flag (mutable per scope). */
  readonly preserveDates: () => boolean;
  /** Current maxQueryValues limit (mutable per scope). */
  readonly maxQueryValues: () => number;
  /** Current resolveRelationships flag (mutable per scope). */
  readonly resolveRelationships: () => boolean;
  /** The instance logger. */
  readonly logger: () => Logger;
  /** The metadata collection definition (`Base.COLLECTION`, read-only usage). */
  readonly metadataCollection: () => Collection;
  /** Protected Base.cast, invoked with the receiving Database. */
  cast(
    db: Database,
    collection: Doc<Collection>,
    document: Doc<any>,
  ): Doc<any>;
  /** Protected Base.encode, invoked with the receiving Database. */
  encode(
    db: Database,
    collection: Doc<Collection>,
    document: Doc<any>,
  ): Promise<Doc<any>>;
  /** Protected Base.decode, invoked with the receiving Database. */
  decode(
    db: Database,
    query: Pick<ProcessedQuery | PopulateQuery, "collection" | "populateQueries">,
    document: Doc<any>,
  ): Promise<Doc<any>>;
  /** Protected Base.processFindResults, invoked with the receiving Database. */
  processFindResults(
    db: Database,
    documents: Doc<any>[],
    query: ProcessedQuery,
  ): Doc<any>[];
  /** Protected Cache.getCacheKeys, invoked with the receiving Database. */
  getCacheKeys(
    db: Database,
    collectionId: string,
    id: string,
    processedQuery: ProcessedQuery,
  ): {
    collectionKey: string;
    documentKey?: string;
    filtersHash?: string;
  };
}

/**
 * The ctx-first document-plane operations of a {@link Database} instance,
 * extracted onto their own collaborator. Every operation receives the
 * caller's immutable AuthContext as its first parameter.
 */
export class DocumentStore implements DocumentPlane {
  /** The owning facade's adapter, resolved per call (scopes swap adapters). */
  private get adapter() {
    return this.db.getAdapter();
  }

  /** The owning facade's cache driver, resolved per call. */
  private get cache() {
    return this.db.getCache();
  }

  /** Suppresses events for the duration of the callback (Emitter.silent). */
  private silent<T>(callback: () => Promise<T>): Promise<T> {
    return this.db.silent(callback);
  }

  /** Emits a database event on the owning facade's emitter. */
  private trigger(event: EventsEnum, ...args: any[]): void {
    this.db.trigger(event as any, ...(args as any));
  }

  private get logger() {
    return this.internals.logger();
  }

  private get timestamp() {
    return this.internals.timestamp();
  }

  private get filter() {
    return this.internals.filter();
  }

  private get validate() {
    return this.internals.validate();
  }

  private get preserveDates() {
    return this.internals.preserveDates();
  }

  private get maxQueryValues() {
    return this.internals.maxQueryValues();
  }

  private get resolveRelationships() {
    return this.internals.resolveRelationships();
  }

  private get metadataCollection() {
    return this.internals.metadataCollection();
  }

  constructor(
    private readonly db: Database,
    private readonly internals: DocumentStoreInternals,
  ) {}

  // Injected protected Base helpers, bound to the owning instance for
  // `this.X(...)` call sites and invoked with an explicit receiver for
  // transaction-scope `db.X(...)` call sites.

  private cast(collection: Doc<Collection>, document: Doc<any>): Doc<any> {
    return this.internals.cast(this.db, collection, document);
  }

  private encode(
    collection: Doc<Collection>,
    document: Doc<any>,
  ): Promise<Doc<any>> {
    return this.internals.encode(this.db, collection, document);
  }

  private decode(
    query: Pick<ProcessedQuery | PopulateQuery, "collection" | "populateQueries">,
    document: Doc<any>,
  ): Promise<Doc<any>> {
    return this.internals.decode(this.db, query, document);
  }

  private processFindResults(
    documents: Doc<any>[],
    query: ProcessedQuery,
  ): Doc<any>[] {
    return this.internals.processFindResults(this.db, documents, query);
  }

  private getCacheKeys(
    collectionId: string,
    id: string,
    processedQuery: ProcessedQuery,
  ): {
    collectionKey: string;
    documentKey?: string;
    filtersHash?: string;
  } {
    return this.internals.getCacheKeys(this.db, collectionId, id, processedQuery);
  }

  /**
   * Get a document by ID.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}. The session's AuthContext gates cached and fresh reads
   * against collection/document read permissions.
   */
  public async getDocument(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    forUpdate: boolean = false,
  ): Promise<any> {
    if (collectionId === Base.METADATA && id === Base.METADATA) {
      return new Doc(this.metadataCollection);
    }

    if (!collectionId) {
      throw new NotFoundException(`Collection '${collectionId}' not found.`);
    }
    if (!id) {
      return new Doc();
    }

    const collection = await this.silent(() =>
      this.db.getCollection(collectionId, true),
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
          ttl: Base.TTL,
          tags: [collectionKey, documentKey!],
        });
      } catch (e) {
        this.logger.warn(`Failed to load document '${id}' from cache: ${e}`);
      }

      if (cached) {
        doc = new Doc(cached);

        if (collection.getId() !== Base.METADATA) {
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

      if (!doc.empty() && collection.getId() !== Base.METADATA) {
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
        collection.getId() !== Base.METADATA &&
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
        documents as unknown as Doc<any>[],
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
          ttl: Base.TTL,
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
  public async createDocument(
    ctx: AuthContext,
    collectionId: string,
    document: Doc<Record<string, any>> | Record<string, any>,
  ): Promise<Doc<any>> {
    if (
      collectionId !== Base.METADATA &&
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
      this.db.getCollection(collectionId, true),
    );

    if (collection.getId() !== Base.METADATA) {
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
          collection.getId() !== Base.METADATA &&
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

    const result = await this.db.withTransaction(async (db) => {
      doc = await this.silent(() => createRelationships(db, collection, doc));
      return db.getAdapter().createDocument(SYSTEM_CONTEXT, collection.getId(), doc);
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
   * Create multiple documents in a collection.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  public async createDocuments<D extends Doc<Record<string, any>>>(
    ctx: AuthContext,
    collectionId: string,
    documents: D[],
  ): Promise<Doc[]> {
    if (!documents || documents.length === 0) {
      return [];
    }
    if (collectionId === Base.METADATA) {
      throw new DatabaseException(
        "Cannot create documents in metadata collection",
      );
    }

    const collection = await this.silent(() =>
      this.db.getCollection(collectionId, true),
    );
    if (collection.getId() !== Base.METADATA) {
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
            collection.getId() !== Base.METADATA &&
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

    const updatedDocuments = await this.db.withTransaction(async (db) => {
      const resolvedDocuments = await Promise.all(
        createdDocuments.map((doc) => createRelationships(db, collection, doc)),
      );
      return db.getAdapter().createDocuments(
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
  public async updateDocument(
    ctx: AuthContext,
    collectionId: string,
    id: string,
    document: Doc<Record<string, any>> | Record<string, any>,
  ): Promise<Doc<any>> {
    if (!id) {
      throw new DatabaseException("Must define $id attribute");
    }

    const collection = await this.silent(() =>
      this.db.getCollection(collectionId),
    );
    const newUpdatedAt = document.updatedAt();
    const updatedDocument = await this.db.withTransaction(async (db) => {
      const time = new Date().toISOString();
      const old = await this.silent(() =>
        db[documentPlane].getDocument(
          SYSTEM_CONTEXT,
          collection.getId(),
          id,
          [],
          true,
        ),
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

      if (db.getAdapter().$sharedTables) {
        mergedDocument["$tenant"] = old.get("$tenant");
      }

      const relationships = collection
        .get("attributes", [])
        .filter((attr) => attr.get("type") === AttributeEnum.Relationship);

      let shouldUpdate = false;

      if (collection.getId() !== Base.METADATA) {
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

      const encodedDocument = await this.internals.encode(db, collection, doc);

      if (relationships.length > 0) {
        doc = await updateDocumentRelationships(db, collection, encodedDocument);
      }
      await db.getAdapter().updateDocument(
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
   * Update multiple documents in a collection.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  public async updateDocuments(
    ctx: AuthContext,
    collectionId: string,
    updates: Doc<Partial<IEntity> & Record<string, any>>,
    query: Query[] | ((qb: QueryBuilder) => QueryBuilder) = [],
    batchSize: number = Base.DEFAULT_BATCH_SIZE,
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
      this.db.getCollection(collectionId, true),
    );

    const documentSecurity = collection.get("documentSecurity", false);
    const updatePermissions = collection.getUpdate();
    const skipAuth = authorize(ctx, updatePermissions, PermissionEnum.Update);

    if (
      !skipAuth &&
      !documentSecurity &&
      collection.getId() !== Base.METADATA
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

      await this.db.withTransaction(async (db) => {
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
            updateDocumentRelationships(db, collection, document),
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

          const encodedDocument = await this.internals.encode(db, collection, merged);
          processedBatch.push(encodedDocument);
        }

        await db.getAdapter().updateDocuments(
          SYSTEM_CONTEXT,
          collection.getId(),
          encodedUpdates,
          processedBatch,
        );
      });

      for (const doc of batch) {
        doc.delete("$skipPermissionsUpdate");

        await this.db.purgeCachedDocument(collection.getId(), doc.getId());
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
  public async deleteDocument(
    ctx: AuthContext,
    collectionId: string,
    id: string,
  ): Promise<boolean> {
    const collection = await this.silent(() =>
      this.db.getCollection(collectionId),
    );

    let document!: Doc;
    const deleted = await this.db.withTransaction(async (db) => {
      // The pre-delete fetch is engine-internal: the permission decision is
      // made below against the caller's ctx (formerly a global-skip wrap).
      document = await this.silent(() =>
        db[documentPlane].getDocument(
          SYSTEM_CONTEXT,
          collection.getId(),
          id,
          [],
          true,
        ),
      );

      if (document.empty()) {
        return false;
      }

      if (collection.getId() !== Base.METADATA) {
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
        deleteDocumentRelationships(db, collection, document),
      );
      const result = await db.getAdapter().deleteDocument(
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
  public async deleteDocuments(
    ctx: AuthContext,
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
  ): Promise<string[]> {
    const collection = await this.silent(() =>
      this.db.getCollection(collectionId),
    );
    let queries: Query[];
    if (typeof query === "function") {
      queries = query(new QueryBuilder()).build();
    } else queries = query ?? [];

    const deletedIds = await this.db.withTransaction(async (db) => {
      const processedQueries = await db[documentPlane].processQueries(
        ctx,
        queries,
        collection,
        {
          forPermission: PermissionEnum.Delete,
        },
      );
      const result = await db.getAdapter().deleteDocuments(
        SYSTEM_CONTEXT,
        collection.getId(),
        processedQueries,
      );
      for (const id of result) {
        await db.purgeCachedDocument(collection.getId(), id);
        await db.silent(() =>
          deleteDocumentRelationships(
            db,
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
  public async deleteDocumentsBatch(
    ctx: AuthContext,
    collectionId: string,
    query?: Query[] | ((qb: QueryBuilder) => QueryBuilder),
    batchSize: number = Base.DELETE_BATCH_SIZE,
    onNext?: (doc: Doc<any>, old: Doc<any>) => void | Promise<void>,
    onError?: (error: Error) => void | Promise<void>,
  ): Promise<number> {
    if (this.adapter.$sharedTables && !this.adapter.$tenantId) {
      throw new DatabaseException(
        "Missing tenant. Tenant must be set when table sharing is enabled.",
      );
    }

    batchSize = Math.min(Base.DELETE_BATCH_SIZE, Math.max(1, batchSize));
    const collection = await this.silent(() =>
      this.db.getCollection(collectionId),
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
      collection.getId() !== Base.METADATA
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

      await this.db.withTransaction(async (db) => {
        for (const document of batch) {
          sequences.push(document.getSequence());
          if (document.getPermissions().length > 0) {
            permissionIds.push(document.getId());
          }

          if (this.resolveRelationships) {
            await this.silent(() =>
              deleteDocumentRelationships(db, collection, document),
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

        await db.getAdapter().deleteDocumentsBySequences(
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
          await this.db.withTenant(document.getTenant(), () =>
            this.db.purgeCachedDocument(collection.getId(), document.getId()),
          );
        } else {
          await this.db.purgeCachedDocument(collection.getId(), document.getId());
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
   * Create or update documents in a collection.
   *
   * Internal ctx-first implementation; exposed publicly only through
   * {@link Session}.
   */
  public async createOrUpdateDocuments(
    ctx: AuthContext,
    collectionId: string,
    documents: Doc<Record<string, any>>[],
    batchSize: number = Base.DEFAULT_BATCH_SIZE,
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
  public async createOrUpdateDocumentsWithIncrease(
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
      this.db.getCollection(collectionId, true),
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
        old = await this.db.withTenant(document.getTenant(), () =>
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
          createRelationships(this.db, collection, encodedDocument),
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
      const batch = await this.db.withTransaction((db) =>
        db.getAdapter().createOrUpdateDocuments(
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
          await this.db.withTenant(processedDoc.getTenant(), () =>
            this.db.purgeCachedDocument(collection.getId(), processedDoc.getId()),
          );
        } else {
          await this.db.purgeCachedDocument(
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
  public async increaseDocumentAttribute(
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
      this.db.getCollection(collectionId, true),
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
      !NUMERIC_ATTRIBUTE_TYPES.has(attr.get("type")) ||
      attr.get("array")
    ) {
      throw new DatabaseException(
        "Attribute must be an integer or float and can not be an array.",
      );
    }

    const document = await this.db.withTransaction(async (db) => {
      // Engine-internal fetch; permission decision happens below against ctx
      // (formerly a global-skip wrap).
      const doc = await this.silent(() =>
        db[documentPlane].getDocument(
          SYSTEM_CONTEXT,
          collection.getId(),
          id,
          [],
          true,
        ),
      );

      if (doc.empty()) {
        throw new NotFoundException("Document not found");
      }

      if (collection.getId() !== Base.METADATA) {
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

      await db.getAdapter().increaseDocumentAttribute({
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

    await this.db.purgeCachedDocument(collection.getId(), id);

    this.trigger(EventsEnum.DocumentIncrease, document, value);

    return document;
  }

  /** Decrease a numeric attribute value in a document. */
  public async decreaseDocumentAttribute(
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
      this.db.getCollection(collectionId, true),
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

    if (!NUMERIC_ATTRIBUTE_TYPES.has(attr.get("type")) || attr.get("array")) {
      throw new DatabaseException(
        "Attribute must be an integer or float and can not be an array.",
      );
    }

    const document = await this.db.withTransaction(async (db) => {
      const doc = await this.silent(() =>
        db[documentPlane].getDocument(
          SYSTEM_CONTEXT,
          collection.getId(),
          id,
          [],
          true,
        ),
      );

      if (doc.empty()) {
        throw new NotFoundException("Document not found");
      }

      if (collection.getId() !== Base.METADATA) {
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
      const finalUpdatedAt =
        !updatedAt || !this.preserveDates ? time : updatedAt;
      const minValue = min !== undefined ? min + value : undefined;

      await db.getAdapter().increaseDocumentAttribute({
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

    await this.db.purgeCachedDocument(collection.getId(), id);

    this.trigger(EventsEnum.DocumentDecrease, document, value);

    return document;
  }

  /** Find documents. */
  public async find(
    ctx: AuthContext,
    collectionId: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    forPermission: PermissionEnum = PermissionEnum.Read,
  ): Promise<Doc<any>[]> {
    if (!collectionId) {
      throw new NotFoundException(`Collection '${collectionId}' not found.`);
    }

    const collection = await this.silent(() =>
      this.db.getCollection(collectionId, true),
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

    const rows = await this.adapter.find(ctx, collectionId, processedQueries, {
      forPermission,
    });
    const result = this.processFindResults(
      rows as unknown as Doc<any>[],
      processedQueries,
    );

    const castedResult = result.map((doc) => this.cast(collection, doc));
    const documents = await Promise.all(
      castedResult.map(async (doc) => {
        return this.filter ? await this.decode(processedQueries, doc) : doc;
      }),
    );

    this.trigger(EventsEnum.DocumentsFind, documents);

    return documents;
  }

  /** Find a single document. */
  public async findOne(
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

  /** Count documents in a collection. */
  public async count(
    ctx: AuthContext,
    collectionId: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    max?: number,
  ): Promise<number> {
    const collection = await this.silent(() =>
      this.db.getCollection(collectionId),
    );

    const queries: Query[] =
      typeof query === "function" ? query(new QueryBuilder()).build() : query;
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

  /** Sum an attribute for all documents in a collection. */
  public async sum(
    ctx: AuthContext,
    collectionId: string,
    attribute: string,
    query: ((builder: QueryBuilder) => QueryBuilder) | Query[] = [],
    max?: number,
  ): Promise<number> {
    const collection = await this.silent(() =>
      this.db.getCollection(collectionId),
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

  /** Processes queries for a collection, validating and authorizing them. */
  public async processQueries(
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
    const validators = overrideValidators ?? allowedValidators;

    const permissions = collection.getPermissionsByType(forPermission);
    const skipAuth = authorize(ctx, permissions, forPermission);
    if (
      collection.getId() !== Base.METADATA &&
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
      if (populateQueries.size > 1) {
        throw new QueryException(
          "Cannot use '*' with other populate queries. Use '*' alone to populate all relationships.",
        );
      }
      populateQueries = new Map();
      for (const attribute of attributes) {
        if (attribute.get("type") === AttributeEnum.Relationship) {
          const options = attribute.get("options", {}) as RelationOptions;
          if (!options.twoWay && options.side !== RelationSideEnum.Parent) {
            continue;
          }
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
        this.db.getCollection(relatedCollectionId),
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

  public purgeCachedDocument(
    collectionId: string,
    doc: Doc<any> | string,
  ): Promise<void> {
    return this.db.purgeCachedDocument(collectionId, doc);
  }

  public purgeCachedCollection(
    collection: Doc<Collection> | string,
  ): Promise<void> {
    return this.db.purgeCachedCollection(collection);
  }

  public withTransaction<T>(
    ctx: AuthContext,
    callback: (session: Session) => Promise<T>,
  ): Promise<T> {
    return this.db.withTransaction(async (txDatabase) =>
      callback(new Session(txDatabase as Database, ctx)),
    );
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
