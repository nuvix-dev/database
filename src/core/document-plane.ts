/**
 * The document-plane contract shared between {@link Database} and
 * {@link Session}.
 *
 * The `documentPlane` symbol keys the ctx-first operation table that
 * Database installs on every instance; Session dispatches every document
 * operation through it. Keeping the symbol in its own module lets
 * database.ts and session.ts share it without a runtime import cycle:
 * session.ts consumes the symbol at runtime, while database.ts only needs
 * the interface as a type — so the dependency graph stays acyclic.
 *
 * This module is INTERNAL to src/core. Importing the symbol from outside
 * the library internals is unsupported; sessions (`db.for` / `db.system`)
 * remain the only public document API.
 */
import type { AuthContext } from "./auth.js";
import type { QueryBuilder } from "@utils/query-builder.js";
import type { Query } from "./query.js";
import type { Doc } from "./doc.js";
import type { IEntity } from "types.js";
import type { Collection } from "@validators/schema.js";
import type { PermissionEnum } from "./enums.js";
import type { Session } from "./session.js";
import type { Attribute } from "@validators/schema.js";
import type { MethodType } from "@validators/query/base.js";
import type { ProcessedQuery } from "./document-store.js";

export const documentPlane = Symbol("nuvix.db.documentPlane");

/**
 * The ctx-first document-plane surface of a {@link Database} instance.
 *
 * Implemented by the `documentPlane` symbol member on Database and
 * consumed exclusively by {@link Session}. Every operation receives the
 * session's immutable AuthContext as its first parameter.
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
  processQueries(
    ctx: AuthContext,
    queries: ((builder: QueryBuilder) => QueryBuilder) | Query[],
    collection: Doc<Collection>,
    options?: Partial<Attribute["options"]> & {
      populated?: boolean;
      attribute?: string;
      allowedValidators?: MethodType[];
      overrideValidators?: MethodType[];
      forPermission?: PermissionEnum;
      throwOnUnAuthorization?: boolean;
    },
  ): Promise<ProcessedQuery>;
  purgeCachedDocument(collectionId: string, doc: Doc<any> | string): Promise<void>;
  purgeCachedCollection(collection: Doc<Collection> | string): Promise<void>;
  withTransaction<T>(
    ctx: AuthContext,
    callback: (session: Session) => Promise<T>,
  ): Promise<T>;
}
