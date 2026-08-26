import type { CacheDriver } from "@cache/types.js";
import { Base } from "./base.js";
import { Collection } from "@validators/schema.js";
import { Doc } from "./doc.js";
import type { ProcessedQuery } from "./database.js";
import { fnv1a128 } from "@utils/index.js";

export abstract class Cache extends Base {
  protected cacheName: string = "default";

  public getCache(): CacheDriver {
    return this.cache;
  }

  public async purgeCachedCollection(collection: Doc<Collection> | string) {
    const collectionId =
      typeof collection === "string" ? collection : collection.getId();
    const { collectionKey } = this.getCacheKeys(collectionId);
    try {
      await this.cache.flushByTags([collectionKey]);
    } catch (e) {
      this.logger.warn(
        `Failed to remove collection '${collectionId}' from cache: ${e}`,
      );
    }
  }

  public async purgeCachedDocument(collection: string, doc: Doc<any> | string) {
    const documentId = typeof doc === "string" ? doc : doc.getId();
    const { documentKey } = this.getCacheKeys(collection, documentId);
    if (documentKey) {
      try {
        await this.cache.flushByTags([documentKey]);
      } catch (e) {
        this.logger.warn(
          `Failed to remove document '${documentId}' from cache: ${e}`,
        );
      }
    }
  }

  protected getCacheKeys(
    collectionId: string,
    documentId?: string,
    filters?: ProcessedQuery,
  ) {
    const baseKey = `db:${this.cacheName}:${this.namespace ?? null}:${this.schema}:${this.tenantId ?? null}`;
    const collectionKey = `${baseKey}:${collectionId}`;
    let documentKey: string | undefined;
    let filtersHash: string | undefined;

    if (documentId) {
      documentKey = `${collectionKey}:${documentId}`;
    }

    if (filters) {
      filtersHash = this.hashFilters(filters);
    }

    return {
      baseKey,
      collectionKey,
      documentKey,
      filtersHash,
    };
  }

  private hashFilters(query: ProcessedQuery): string {
    // Hash the full query shape. Hashing only selections would let distinct
    // queries (different filters/limit/cursor) collide on one cached document.
    const payload = {
      selections: [...query.selections].sort(),
      filters: (query.filters ?? []).map((q) => q.toObject()),
      limit: query.limit ?? null,
      offset: query.offset ?? null,
      cursor:
        query.cursor instanceof Doc
          ? query.cursor.getId()
          : (query.cursor ?? null),
      cursorDirection: query.cursorDirection ?? null,
    };
    return fnv1a128(JSON.stringify(payload));
  }
}
