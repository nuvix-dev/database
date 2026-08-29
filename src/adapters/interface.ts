import type { AuthContext } from "@core/auth.js";
import type { ProcessedQuery } from "@core/database.js";
import { Doc } from "@core/doc.js";
import type {
  EventsEnum,
  PermissionEnum,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";
import type { Logger } from "@utils/logger.js";
import type { Attribute, Index } from "@validators/schema.js";
import type { BaseAdapter, Meta } from "./base.js";
import type {
  ColumnInfo,
  CreateAttribute,
  CreateIndex,
  QueryResult,
  UpdateAttribute,
} from "./types.js";

/** Minimal query execution surface shared by all database dialects. */
export interface QueryClient {
  readonly __type: string;
  readonly database: string;
  query<T = Record<string, unknown>>(
    sql: string,
    values?: unknown[],
  ): Promise<QueryResult<T>>;
  transaction<T>(
    callback: (client: TransactionClient) => Promise<T>,
    maxRetries?: number,
  ): Promise<T>;
  disconnect(): Promise<void>;
  quote(value: string): string;
  ping(): Promise<void>;
}

/** Query client bound to an active transaction. */
export interface TransactionClient extends QueryClient {
  readonly __type: "transaction";
  savepoint(): Promise<string>;
  releaseSavepoint(name: string): Promise<void>;
  rollbackTo(name: string): Promise<void>;
}

export interface CreateCollectionOptions {
  name: string;
  attributes: Doc<Attribute>[];
  indexes?: Doc<Index>[];
}

/** Dialect operations implemented above the shared BaseAdapter behavior. */
export interface AdapterOperations {
  create(name: string): Promise<void>;
  delete(name: string): Promise<void>;
  createCollection(options: CreateCollectionOptions): Promise<void>;
  getSizeOfCollectionOnDisk(collection: string): Promise<number>;
  getSizeOfCollection(collection: string): Promise<number>;
  deleteCollection(id: string): Promise<void>;
  analyzeCollection(collection: string): Promise<boolean>;
  createAttribute(options: CreateAttribute): Promise<void>;
  createAttributes(
    collection: string,
    attributes: Omit<CreateAttribute, "collection">[],
  ): Promise<void>;
  renameAttribute(
    collection: string,
    oldName: string,
    newName: string,
  ): Promise<void>;
  deleteAttribute(collection: string, name: string): Promise<void>;
  getSchemaAttributes(collection: string): Promise<Doc<ColumnInfo>[]>;
  createRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay?: boolean,
    id?: string,
    twoWayKey?: string,
  ): Promise<boolean>;
  updateRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay: boolean | undefined,
    key: string,
    twoWayKey: string,
    side: RelationSideEnum,
    newKey?: string,
    newTwoWayKey?: string,
  ): Promise<boolean>;
  deleteRelationship(
    collection: string,
    relatedCollection: string,
    type: RelationEnum,
    twoWay: boolean,
    key: string,
    twoWayKey: string,
    side: RelationSideEnum,
  ): Promise<boolean>;
  updateAttribute(options: UpdateAttribute): Promise<void>;
  renameIndex(
    collectionId: string,
    oldName: string,
    newName: string,
  ): Promise<boolean>;
  createIndex(options: CreateIndex): Promise<boolean>;
  deleteIndex(collection: string, id: string): Promise<boolean>;
  createDocument<D extends Doc>(
    ctx: AuthContext,
    collection: string,
    document: D,
  ): Promise<D>;
  createDocuments<D extends Doc>(
    ctx: AuthContext,
    collection: string,
    documents: D[],
  ): Promise<D[]>;
  updateDocument<D extends Doc>(
    ctx: AuthContext,
    collection: string,
    document: D,
    skipPermissions?: boolean,
  ): Promise<D>;
  updateDocuments(
    ctx: AuthContext,
    collection: string,
    updates: Doc,
    documents: Doc[],
  ): Promise<number>;
  deleteDocuments(
    ctx: AuthContext,
    collectionId: string,
    query: ProcessedQuery,
  ): Promise<string[]>;
  deleteDocumentsBySequences(
    ctx: AuthContext,
    collection: string,
    sequences: number[],
    permissionIds: string[],
  ): Promise<number>;
  createOrUpdateDocuments(
    ctx: AuthContext,
    collection: string,
    attribute: string,
    changes: Array<{ old: Doc; new: Doc }>,
  ): Promise<Doc[]>;
  find(
    ctx: AuthContext,
    collection: string,
    query: ProcessedQuery,
    options?: { forPermission?: PermissionEnum },
  ): Promise<Record<string, unknown>[]>;
}

/** Complete adapter surface consumed by the database core. */
export interface DatabaseAdapter extends BaseAdapter, AdapterOperations {
  readonly $client: QueryClient;
  setMeta(meta: Partial<Meta>): this;
  setLogger(logger: Logger): this;
  before(
    event: EventsEnum,
    name: string,
    callback?: (query: string) => string,
  ): void;
}
