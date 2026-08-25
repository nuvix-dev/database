export * from "./adapters/adapter.js";
export * from "./core/database.js";
export * from "./core/doc.js";
export * from "./core/query.js";
export * from "./core/emitter.js";
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
export * from "./types.js";

// Export config types for external use
export type { NuvixDBConfig, CLIOptions } from "./config/types.js";
