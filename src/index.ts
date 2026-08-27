import type { Entities as CoreEntities } from "./types.js";

/**
 * Public entity registry used by typed collection APIs.
 *
 * Generated type files opt in by augmenting this interface in the
 * `@nuvix/db` module. Keeping the registry at the package root makes the
 * generated file's import sufficient to activate typing without changing
 * any runtime code.
 */
export interface Entities extends CoreEntities {}

export * from "./adapters/adapter.js";
export * from "./core/database.js";
export * from "./core/doc.js";
export * from "./core/query.js";
export * from "./core/emitter.js";
// Auth context model — replaces the removed static auth class.
// `Session` itself is exported via the ./core/database.js star export above.
export { authorize } from "./core/auth.js";
export type { AuthContext, SystemAuthContext } from "./core/auth.js";
export type { Filter, RelationshipUpdates, FilterValue } from "./core/types.js";
export type { CacheDriver, CacheOperationOptions } from "./cache/index.js";

export {
  AttributeEnum as AttributeType,
  PermissionEnum as PermissionType,
  RelationSideEnum as RelationSide,
  EventsEnum as Events,
  OnDelete as OnDeleteAction,
  RelationEnum as RelationType,
  PermissionEnum as Perms,
  OrderEnum as Order,
  OnDelete,
  IndexEnum as IndexType,
  CursorEnum as Cursor,
} from "./core/enums.js";

export * from "./errors/index.js";
export * from "./types.js";
export * from "./utils/index.js";
export * from "./validators/index.js";

// Export config types for external use
export type { NuvixDBConfig, CLIOptions } from "./config/types.js";
