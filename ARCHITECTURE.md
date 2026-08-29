# Architecture

`@nuvix/db` is a document-oriented data layer over PostgreSQL and SQLite.
Collections, attributes, relationships, and permissions are modeled as
documents themselves and mapped onto relational tables at runtime. Both
dialects use Bun-native database clients (Bun >= 1.4 required).

## Layering

```
Application code
      │
 ┌────▼──────────────┐   ┌─────────────────────────────────┐
 │ Database / Session│──▶│ CacheDriver                     │
 │ public facades    │   │ (@nuvix/cache or compatible)    │
 └────┬──────────────┘   └─────────────────────────────────┘
      │
 ┌────▼─────────────────────────────┐
 │ SchemaManager / DocumentStore    │ validation, authorization, population
 └────┬─────────────────────────────┘
 ┌────▼─────────────────────────────────────────────────────┐
 │ Shared contracts: DatabaseAdapter / QueryClient          │
 │ BaseAdapter: metadata, events, common document behavior  │
 └────┬───────────────────────────────┬──────────────────────┘
      │                               │
 ┌────▼────────────────────┐     ┌────▼────────────────────────┐
 │ PostgreSQL              │     │ SQLite                      │
 │ Adapter                 │     │ SQLiteAdapter               │
 │ SqlBuilder / Ddl        │     │ SQLiteSqlBuilder / SQLiteDdl │
 │ PostgresClient          │     │ SQLiteClient                │
 └─────────────────────────┘     └─────────────────────────────┘
```

`DatabaseAdapter`, `AdapterOperations`, `QueryClient`, and
`TransactionClient` are dialect-neutral contracts in
`src/adapters/interface.ts`. The core depends on these contracts rather than a
driver. `BaseAdapter` owns behavior that is safe to share; each concrete
adapter owns dialect SQL, DDL, capability flags, error mapping, and client
lifecycle.

The core classes form an inheritance chain — `Base` → `Cache` → `Database` —
where each level adds exactly one concern:

| Class | File | Responsibility |
|---|---|---|
| `Base` | `src/core/base.ts` | Document encode/decode through per-attribute filter pipelines, validation orchestration, relationship population of result rows, explicit `AuthContext` threading into encode/decode/populate/query processing, scoped execution (`silent()` scopes, tenant/schema overrides), transactional scope construction |
| `Cache` | `src/core/cache.ts` | Cache-key scheme and tag-based purge helpers |
| `Database` | `src/core/database.ts` | Public admin facade delegating schema operations to `SchemaManager`; creates scoped `Session` instances for document operations |
| `Session` | `src/core/session.ts` | Public document facade bound to an immutable `AuthContext`; delegates CRUD, queries, bulk operations, and transactions to `DocumentStore` |
| `SchemaManager` | `src/core/schema-manager.ts` | Collection, attribute, relationship, index, and database lifecycle implementation behind `Database` |
| `DocumentStore` | `src/core/document-store.ts` | Document CRUD, query processing, validation, authorization, caching, population, and transaction implementation behind `Session` |

## Module map

| Path | Contents |
|---|---|
| `src/adapters/interface.ts` + `base.ts` | Dialect-neutral `DatabaseAdapter`, `AdapterOperations`, `QueryClient`, and `TransactionClient` contracts; `BaseAdapter` metadata, events, and shared document behavior |
| `src/adapters/adapter.ts` + `ddl.ts` + `sql-builder.ts` + `postgres.ts` | PostgreSQL `Adapter`, schema-qualified DDL/query generation, Bun `SQL` client, transaction wrapper, and SQLSTATE error mapping |
| `src/adapters/sqlite-adapter.ts` + `sqlite-ddl.ts` + `sqlite-sql-builder.ts` + `sqlite.ts` | `SQLiteAdapter`, SQLite-specific DDL/query generation, deterministic logical-schema naming, Bun SQLite client, and savepoint transactions |
| `src/core/` | `Database` and `Session` facades, `SchemaManager`, `DocumentStore`, `Cache`/`Base`, the immutable `AuthContext` model + pure `authorize()` (`auth.ts`), `Doc<T>`, `Query`, `Emitter`, relationship resolution, index manager |
| `src/validators/` | Schema definitions (`Attribute`, `Index`, `Collection`), document structure/permission validators, query validators (`filter`, `limit`, `offset`, `order`, `cursor`, `select`, `populate`), per-type value validators |
| `src/utils/` | ID generation, permission/role helpers, fluent `QueryBuilder`, type generator |
| `src/errors/` | Typed error hierarchy (`Authorization`, `Structure`, `Query`, `Conflict`, `Duplicate`, `NotFound`, `Timeout`, `Transaction`, …) |
| `src/cache/` | `CacheDriver` interface — four async methods (`get`, `set`, `flushByTags`, `flush`); satisfied structurally, so any compatible driver works without inheriting |
| `src/config/` + `src/cli/` | Config loader and the `generate-types` CLI |

Public API surface is defined entirely by `src/index.ts`.

The decomposed collaborators are implementation details. `Database` delegates
admin-plane calls to `SchemaManager`, and `Session` reaches `DocumentStore`
through the private `documentPlane` symbol. `BaseAdapter` provides the common
facade, while `Adapter` delegates PostgreSQL schema mutations to `Ddl` and
`SQLiteAdapter` delegates SQLite mutations to `SQLiteDdl`. Query generation
follows the same split through `SqlBuilder` and `SQLiteSqlBuilder`. These
facades preserve the core API while keeping dialect decisions and transaction
ownership below it.

## Read path

A session's `find(...)` / `getDocument(...)`:

1. Collection metadata is fetched via `silent()` — a privileged internal read running under `SYSTEM_CONTEXT`.
2. `processQueries` validates every filter/select/populate against the schema and resolves relationship attributes into a join plan.
3. Authorization: the session's `AuthContext` is checked against collection `$permissions` by the pure `authorize()` function; with `documentSecurity` enabled, role predicates derived from the context's roles are pushed into the SQL instead.
4. `adapter.find` builds a single SELECT: LEFT JOINs for one-to-one/many-to-one relations, junction-table joins for many-to-many, cursor pagination, tenant predicates.
5. Joined rows are reassembled into nested documents (`processFindResults`), values are decoded through attribute filter pipelines, and the result is cached under a query-hash key.

## Write path

`createDocument` / `updateDocument` / bulk variants:

1. Authorization check for the operation against the session's `AuthContext`.
2. Validation: permission strings (`Permissions` validator), document shape against collection schema (`Structure`), per-type value validation during encode.
3. `encode` runs each value through its attribute's filter pipeline (e.g., datetime normalization, JSON encoding).
4. Execution happens inside `withTransaction`; permissions are synced into the `<collection>_perms` side table; cache tags for the document/collection are flushed.

## Storage model

- **One table per collection.** Columns are sanitized attribute keys plus internals: `_id` ($id), `$sequence`, `$createdAt`, `$updatedAt`, `_uid` (stable row identity used for join reassembly). A `_tenant` column is added for shared tables.
- **PostgreSQL naming**: physical schemas provide isolation, so tables are
  rendered as `"{schema}"."{namespace}_{collection}"`. Index names derive from
  schema, namespace, table, and index identity and are shortened with a SHA-1
  suffix when they exceed PostgreSQL's 63-character identifier limit.
- **SQLite naming**: SQLite has no equivalent physical schema namespace. The
  logical schema and namespace are encoded into every table prefix as
  `<sanitized-schema>_<sanitized-namespace>_<sha1-12(schema + NUL + namespace)>`;
  the collection name is appended as `_<sanitized-collection>`. The same
  inputs always produce the same name, while different logical schemas or
  namespaces retain distinct hash identities even when sanitization collides.
  Indexes likewise include schema, namespace, table, and index identity with a
  20-character SHA-1 suffix.
- **Permissions live in a side table** `<collection>_perms` (`_type`, `_permissions`, `_document`) keyed by document sequence. This keeps main rows narrow and lets permission checks execute as pure SQL rather than in application code.
- **Dialect storage**: PostgreSQL uses native `JSONB`, arrays, `->` / `->>`
  operators, and `tsvector` fulltext indexes. SQLite stores JSON and array
  values as text and emits only SQLite-compatible query expressions.

## Multi-tenancy

Configured on the adapter via `setMeta`:

- `namespace` — prefixes every table name; isolates unrelated apps sharing one database.
- `sharedTables: true` + `tenantId` — injects `_tenant` predicates into all queries.
- `tenantPerDocument` — tenant resolved per row instead of per connection.

## Authorization

Authorization is built on an **immutable auth context** (`src/core/auth.ts`):

- `AuthContext` — `{ roles: readonly string[] }`, frozen at creation. Roles are plain strings produced by `Role.toString()`: `"roleName"`, `"roleName:id"`, `"roleName/dim"`, or `"roleName:id/dim"`.
- `SystemAuthContext` — extends `AuthContext` with a branded `system: true` flag; obtainable only via `db.system()`.
- `SYSTEM_CONTEXT` — a single frozen shared instance used for internal operations.

Checks are performed by the pure function `authorize(ctx, permissions, action)`, evaluated in order:

1. A system context authorizes everything.
2. Empty permissions deny.
3. Otherwise access is granted when any granted permission's role string is present in `ctx.roles`.

Two levels of permissions are enforced:

- **Collection-level**: `$permissions` on collection metadata, checked before each operation.
- **Document-level**: `documentSecurity: true` consults the `_perms` side table.

Enforcement happens *inside* generated SQL: the context's roles feed permission predicates into WHERE clauses, so a read can never return rows the caller cannot see. Internal operations pass `SYSTEM_CONTEXT` explicitly down their own call chains rather than granting broad roles.

### Sessions: document plane vs admin plane

Document operations live on `Session`, not on `Database`. Callers obtain a session bound to a fixed context — `db.for(...roles)` accepts varargs or an array of role strings, and `db.system()` returns a privileged session. Sessions share the database's adapter, cache and state (no new connection pool) and expose only document-plane methods, which makes calling a document operation without a session a compile-time error. The context travels as an explicit parameter through `Base.encode/decode/populate/processQueries` and into the adapter's SQL builders — there are no ambient reads, so authorization cannot drift between the check and the query it guards.

### Emitter silencing

`silent(fn)` suppresses selected lifecycle events while a callback runs. The set of silenced listener names is an immutable, frozen `ReadonlySet` threaded as a parameter through the emission path; a scope-local async context bridges it across await points inside the scope only. Concurrent emissions outside the scope are unaffected, and overlapping scopes stay independent.

## Caching

- Key scheme: `db:{name}:{namespace}:{schema}:{tenantId}:{collection}[:docId][:filtersHash]`.
- The filters hash covers the **entire query shape** (selections, filters, limit, offset, cursor) — hashing only selections would let distinct queries collide on one cached entry.
- Invalidation is tag-based: writes flush the document tag; schema changes flush the whole collection tag.

## Transactions

`session.withTransaction(async (tx) => ...)` constructs a fresh,
independently-owned `Database` scope bound to a transaction-scoped adapter, so
every operation inside uses the transaction client. The scope preserves the
parent's `AuthContext`, metadata, listeners, filters, and SQL transformations,
but owns fresh mutable containers and document-plane closures. Concurrent or
nested scopes therefore cannot leak state or escape onto the parent client.

The client layer owns transaction boundaries:

1. PostgreSQL delegates the outer lifecycle and dedicated pooled connection to
   Bun's `sql.begin()`; nested calls use savepoints on that connection.
2. SQLite serializes access to its single handle for the full async callback.
   `SQLiteClient` owns `BEGIN`, `COMMIT`, and outer `ROLLBACK`.
3. A nested SQLite transaction reuses the active `SQLiteTransaction` and owns a
   uniquely named `nuvix_sp_N` savepoint. Success releases it; failure performs
   `ROLLBACK TO SAVEPOINT` followed by `RELEASE SAVEPOINT`, then rethrows so the
   caller may decide whether outer work continues.

Bulk operations chunk internally and run per-chunk transactions with
per-document `onNext`/`onError` callbacks.

## Dialect parity

The shared adapter contract covers collection and attribute DDL, document CRUD
and bulk operations, supported filters and pagination, key/unique indexes,
relationships, permissions, shared-table tenancy, and nested transactions.
Parity means these behaviors have the same core contract, not that both engines
accept the same SQL or expose every PostgreSQL feature.

| Boundary | PostgreSQL `Adapter` | `SQLiteAdapter` |
|---|---|---|
| Fulltext index and search | Native `tsvector` support | Deferred; rejects fulltext indexes and searches explicitly |
| GIN and array-overlap queries | Native arrays and GIN-backed overlap | Deferred; rejects GIN-style array overlap explicitly |
| Update locks | Emits `SELECT ... FOR UPDATE` | Deferred; uses SQLite transaction serialization without row locks |

SQLite never emits PostgreSQL syntax for deferred behavior. Unsupported index
and query forms report an explicit `DatabaseException`; update-lock reads use
the documented no-lock contract.

## Query validation

Every incoming query passes through `processQueries` before reaching SQL:

- Attribute existence and type for filters and selects.
- Populate targets must be real relationships; nested populates validated recursively.
- Limit/offset/order/cursor bounds.
- `IndexedQueries` assumes `$id`/`$createdAt`/`$updatedAt` indexes always exist and requires a fulltext index for `search` queries.

This pipeline is why malformed or unindexed query shapes fail fast with typed errors instead of producing SQL errors.

## Events

`Emitter` supports wildcard listeners (`Events.All`). Listener silencing is parameter-based: the set of silenced listener names is an immutable, frozen `ReadonlySet` passed down the emission path instead of mutated shared state, so silencing one scope never leaks into concurrent emissions. The adapter emits around DDL/DML execution; the database layer emits document lifecycle events (`DocumentCreate`, …).

## Type generation

`generateTypes(collections, options)` (`src/utils/generate-types.ts`) emits a
self-describing TypeScript file from collection metadata: a local `IEntity`
interface, collection interfaces, `Doc` aliases, and an `Entities` map keyed by
collection ID. The output also augments the public `@nuvix/db` `Entities`
interface. Importing the generated file as a type opts a consumer into typed
collection IDs, `Doc` results, and attribute-checked `QueryBuilder` callbacks.
Without that import, the registry has no user collections and the existing
string/untyped fallback remains available; widened runtime strings retain the
fallback even when generated types are loaded.

`IEntity` is intentionally **not** imported from the package. Emitting it
locally avoids declaration conflicts (TS2440) while keeping generated files
standalone.

The CLI (`src/cli/generate-types.ts`) loads a `NuvixDBConfig` via `ConfigLoader`, which discovers the config file from the working directory and imports it as ESM or CJS.

## Error handling

`PostgresClient` surfaces raw server errors carrying SQLSTATE codes; `error-mapper` translates them into the typed hierarchy from `src/errors/`, so callers catch `DuplicateException`/`NotFoundException`-style errors instead of inspecting driver error objects.
