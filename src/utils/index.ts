export * from "./authorization.js";
export * from "./filters.js";
export * from "./generate-types.js";
export * from "./id.js";
export * from "./logger.js";
export * from "./permission.js";
export * from "./query-builder.js";
export * from "./role.js";

/**
 * Computes a 128-bit hash of a string, returned as a fixed 32-char hex string.
 *
 * Backed by Bun's native `CryptoHasher` (MD5). This is NOT cryptographic —
 * it is used for non-security purposes such as cache-key fingerprints,
 * where speed matters and collision resistance of MD5 is more than
 * sufficient. Replaces the previous pure-JS FNV-1a implementation, which
 * allocated BigInts per character on hot paths.
 */
export function fnv1a128(str: string): string {
  return new Bun.CryptoHasher("md5").update(str).digest("hex");
}
