/**
 * Options for cache read/write operations.
 */
export interface CacheOperationOptions {
  /** Time-to-live in seconds. */
  ttl?: number;
  /** Tags used for grouped invalidation via {@link CacheDriver.flushByTags}. */
  tags?: string[];
}

/**
 * Minimal cache contract required by `@nuvix/db`.
 *
 * The database only performs document caching and tag-based invalidation,
 * so any implementation exposing these four operations works — including
 * `Memory` and `Redis` from `@nuvix/cache`, which satisfy this interface
 * structurally.
 */
export interface CacheDriver {
  /**
   * Retrieves a cached value by key.
   *
   * @param key - The cache key.
   * @param options - Read options.
   * @returns The cached value, or `null` on miss.
   */
  get<T = any>(key: string, options?: CacheOperationOptions): Promise<T | null>;

  /**
   * Stores a value under a key.
   *
   * @param key - The cache key.
   * @param value - The value to store.
   * @param options - Write options.
   * @returns `true` when the write succeeded.
   */
  set<T = any>(
    key: string,
    value: T,
    options?: CacheOperationOptions,
  ): Promise<boolean>;

  /**
   * Invalidates all entries carrying any of the given tags.
   *
   * @param tags - Tags whose entries should be invalidated.
   * @returns `true` when invalidation succeeded.
   */
  flushByTags(tags: string[]): Promise<boolean>;

  /**
   * Clears the entire cache.
   *
   * @returns `true` when the flush succeeded.
   */
  flush(): Promise<boolean>;
}
