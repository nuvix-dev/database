# Architecture

`@nuvix/db` is a document-oriented data layer over PostgreSQL. Collections,
attributes, relationships, and permissions are modeled as documents themselves
and mapped onto relational tables at runtime. The library is Bun-native: the
connection pool is Bun's built-in `SQL` client (Bun >= 1.4 required).

## Layering

```
Application code
      │
 ┌────▼─────┐        ┌─────────────────────────────────┐
 │ Database │ ─────▶ │ CacheDriver                     │
 │ (facade) │        │ (@nuvix/cache Memory/Redis,     │
 └────┬─────┘        │  or any structurally-compatible │
      │              │  4-method implementation)       │
      │ encode / validate / authorize / populate
 ┌────▼───────────┐
 │  BaseAdapter   │ SQL generation, tenancy, permission predicates
 └────┬───────────┘
 ┌────▼───────────┐
 │    Adapter     │ PostgreSQL DDL/DML: tables, indexes, _perms tables
 └────┬───────────┘
 ┌────▼──────────────┐
 │  PostgresClient   │ Bun SQL pool, ?→$n params, transactions
 └───────────────────┘
```

The core classes form an inheritance chain — `Base` → `Cache` → `Database` —
where each level adds exactly one concern:

| Class | File | Responsibility |
|---|---|---|
| `Base` | `src/core/base.ts` | Document encode/decode through per-attribute filter pipelines, validation orchestration, relationship population of result rows, scoped execution (`silent()`, `Authorization.skip()`, tenant/schema overrides), transaction cloning |
| `Cache` | `src/core/cache.ts` | Cache-key scheme and tag-based purge helpers |
| `Database` | `src/core/database.ts` | Public facade: collections, attributes, indexes, document CRUD, batched bulk operations, aggregations |

## Module map

| Directory | Contents |
|---|---|
| `src/adapters/` | `Adapter` (concrete PostgreSQL DDL/DML), `BaseAdapter` (SQL building shared logic), `PostgresClient` (Bun `SQL` wrapper), `error-mapper` (SQLSTATE → typed errors) |
| `src/core/` | `Database`/`Cache`/`Base`, `Doc<T>` document wrapper, `Query`, `Emitter`, enums, relationship resolution, index manager |
| `src/validators/` | Schema definitions (`Attribute`, `Index`, `Collection`), document structure/permission validators, query validators (`filter`, `limit`, `offset`, `order`, `cursor`, `select`, `populate`), per-type value validators |
| `src/utils/` | `Authorization`, ID generation, permission/role helpers, fluent `QueryBuilder`, type generator |
| `src/errors/` | Typed error hierarchy (`Authorization`, `Structure`, `Query`, `Conflict`, `Duplicate`, `NotFound`, `Timeout`, `Transaction`, …) |
| `src/cache/` | `CacheDriver` interface — four async methods (`get`, `set`, `flushByTags`, `flush`); satisfied structurally, so any compatible driver works without inheriting |
| `src/config/` + `src/cli/` | Config loader and the `generate-types` CLI |

Public API surface is defined entirely by `src/index.ts`.

## Read path

`db.find(...)` / `db.getDocument(...)`:

1. Collection metadata is fetched via `silent()` (system read — authorization skipped).
2. `processQueries` validates every filter/select/populate against the schema and resolves relationship attributes into a join plan.
3. Authorization: collection `$permissions` are checked; with `documentSecurity` enabled, role predicates are pushed into the SQL instead.
4. `adapter.find` builds a single SELECT: LEFT JOINs for one-to-one/many-to-one relations, junction-table joins for many-to-many, cursor pagination, tenant predicates.
5. Joined rows are reassembled into nested documents (`processFindResults`), values are decoded through attribute filter pipelines, and the result is cached under a query-hash key.

## Write path

`createDocument` / `updateDocument` / bulk variants:

1. Authorization check for the operation.
2. Validation: permission strings (`Permissions` validator), document shape against collection schema (`Structure`), per-type value validation during encode.
3. `encode` runs each value through its attribute's filter pipeline (e.g., datetime normalization, JSON encoding).
4. Execution happens inside `withTransaction`; permissions are synced into the `<collection>_perms` side table; cache tags for the document/collection are flushed.

## Storage model

- **One table per collection.** Columns are sanitized attribute keys plus internals: `_id` ($id), `$sequence`, `$createdAt`, `$updatedAt`, `_uid` (stable row identity used for join reassembly). A `_tenant` column is added for shared tables.
- **Naming**: tables are schema-qualified as `"{schema}"."{namespace}_{collection}"`. Index names use `{schema}_{namespace}_{table}_{index}` and are shortened with a SHA1 suffix when they exceed PostgreSQL's 63-character identifier limit.
- **Permissions live in a side table** `<collection>_perms` (`_type`, `_permissions`, `_document`) keyed by document sequence. This keeps main rows narrow and lets permission checks execute as pure SQL rather than in application code.
- **JSON attributes** are queried with `->` / `->>` path operators; fulltext indexes are `tsvector` expressions.

## Multi-tenancy

Configured on the adapter via `setMeta`:

- `namespace` — prefixes every table name; isolates unrelated apps sharing one database.
- `sharedTables: true` + `tenantId` — injects `_tenant` predicates into all queries.
- `tenantPerDocument` — tenant resolved per row instead of per connection.

## Authorization

Two levels:

- **Collection-level**: `$permissions` on collection metadata, checked before each operation.
- **Document-level**: `documentSecurity: true` consults the `_perms` side table.

Enforcement happens *inside* generated SQL: `Authorization.getRoles()` feeds role predicates into WHERE clauses, so a read can never return rows the caller cannot see. System operations bypass checks with `Authorization.skip()` / `silent()` rather than granting broad roles.

## Caching

- Key scheme: `db:{name}:{namespace}:{schema}:{tenantId}:{collection}[:docId][:filtersHash]`.
- The filters hash covers the **entire query shape** (selections, filters, limit, offset, cursor) — hashing only selections would let distinct queries collide on one cached entry.
- Invalidation is tag-based: writes flush the document tag; schema changes flush the whole collection tag.

## Transactions

`db.withTransaction(async (tx) => ...)` clones the `Database` and binds it to a transaction-scoped adapter, so every operation inside shares one transaction. Bulk operations chunk internally and run per-chunk transactions with per-document `onNext`/`onError` callbacks.

## Query validation

Every incoming query passes through `processQueries` before reaching SQL:

- Attribute existence and type for filters and selects.
- Populate targets must be real relationships; nested populates validated recursively.
- Limit/offset/order/cursor bounds.
- `IndexedQueries` assumes `$id`/`$createdAt`/`$updatedAt` indexes always exist and requires a fulltext index for `search` queries.

This pipeline is why malformed or unindexed query shapes fail fast with typed errors instead of producing SQL errors.

## Events

`Emitter` supports wildcard listeners (`Events.All`) and listener silencing. The adapter emits around DDL/DML execution; the database layer emits document lifecycle events (`DocumentCreate`, …).

## Type generation

`generateTypes(collections, options)` (`src/utils/generate-types.ts`) emits a self-describing TypeScript file from collection metadata: a local `IEntity` interface plus a `Doc` import. `IEntity` is intentionally **not** imported from the package — emitting it locally avoids declaration conflicts (TS2440) while keeping generated files standalone.

The CLI (`src/cli/generate-types.ts`) loads a `NuvixDBConfig` via `ConfigLoader`, which discovers the config file from the working directory and imports it as ESM or CJS.

## Error handling

`PostgresClient` surfaces raw server errors carrying SQLSTATE codes; `error-mapper` translates them into the typed hierarchy from `src/errors/`, so callers catch `DuplicateException`/`NotFoundException`-style errors instead of inspecting driver error objects.
