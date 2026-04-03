import { Cache as NuvixCache } from "@nuvix/cache";
import { Base } from "./base.js";
import { Collection } from "@validators/schema.js";
import { Doc } from "./doc.js";
import type { ProcessedQuery } from "./database.js";
import { fnv1a128 } from "@utils/index.js";

export class Cache extends Base {
  protected cacheName: string = "default";

  public getCache(): NuvixCache {
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
    // const payload = {
    //   selections: query.selections,
    //   filters: query.filters ?? [],
    //   limit: query.limit ?? null,
    //   offset: query.offset ?? null,
    //   cursor: query.cursor ? query.cursor?.getId() : null,
    //   cursorDirection: query.cursorDirection ?? null,
    // };
    const selections = [...query.selections].sort();
    return fnv1a128(JSON.stringify(selections));
  }
}
