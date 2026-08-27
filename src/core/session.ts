/**
 * Scoped, document-plane views of a {@link Database} instance.
 *
 * Extracted from database.ts (Phase 2). A Session shares the owning
 * Database's adapter, cache and instance state — creating one never opens
 * a new pool or connection. Sessions expose ONLY document-plane
 * operations; schema/admin operations remain on Database. Authorization
 * decisions are made against `session.ctx`, replacing the removed
 * static/global `Authorization` state.
 *
 * Obtain sessions via `db.for(...roles)` (scoped) or `db.system()`
 * (privileged, bypasses all checks).
 *
 * Database is imported type-only: the only runtime dependency is the
 * shared `documentPlane` symbol from document-plane.ts, so there is no
 * module cycle at runtime.
 */
import type { Database } from "./database.js";
import type { AuthContext } from "./auth.js";
import type { Entities } from "@nuvix/db";
import type { IEntity } from "types.js";
import { QueryBuilder } from "@utils/query-builder.js";
import { Query } from "./query.js";
import { Doc } from "./doc.js";
import type { Collection } from "@validators/schema.js";
import { PermissionEnum } from "./enums.js";
import { documentPlane } from "./document-plane.js";

type DynamicCollection<C extends string> = Exclude<
  keyof Entities,
  "_metadata"
> extends never
  ? C
  : string extends C
    ? C
    : never;
type SessionCollection<C extends string> = C extends keyof Entities
  ? C
  : DynamicCollection<C>;

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
    collectionId: DynamicCollection<C>,
    query?: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    forPermission?: PermissionEnum,
  ): Promise<Doc<Partial<IEntity> & Record<string, any>>[]>;
  find<D extends Record<string, any>, C extends string = string>(
    collectionId: DynamicCollection<C>,
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
    collectionId: DynamicCollection<C>,
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
  getDocument<D extends Record<string, any>, C extends string = string>(
    collectionId: DynamicCollection<C>,
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
  createDocument<D extends Record<string, any>, C extends string = string>(
    collectionId: DynamicCollection<C>,
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
  createDocuments<
    D extends Doc<Record<string, any>>,
    C extends string = string,
  >(
    collectionId: DynamicCollection<C>,
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
  updateDocument<
    D extends Doc<Record<string, any>>,
    C extends string = string,
  >(
    collectionId: DynamicCollection<C>,
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
  updateDocuments<C extends string>(
    collectionId: DynamicCollection<C>,
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

  deleteDocument<C extends string>(
    collectionId: SessionCollection<C>,
    id: string,
  ): Promise<boolean> {
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
  deleteDocuments<C extends string>(
    collectionId: DynamicCollection<C>,
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
  deleteDocumentsBatch<C extends string>(
    collectionId: DynamicCollection<C>,
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
  createOrUpdateDocuments<C extends string>(
    collectionId: DynamicCollection<C>,
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
  createOrUpdateDocumentsWithIncrease<C extends string>(
    collectionId: DynamicCollection<C>,
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
  increaseDocumentAttribute<C extends string>(
    collectionId: DynamicCollection<C>,
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
  decreaseDocumentAttribute<C extends string>(
    collectionId: DynamicCollection<C>,
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
  count<C extends string>(
    collectionId: DynamicCollection<C>,
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
  sum<C extends string>(
    collectionId: DynamicCollection<C>,
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
