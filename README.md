# @nuvix/db

[![npm version](https://img.shields.io/npm/v/@nuvix/db.svg)](https://www.npmjs.com/package/@nuvix/db)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.8+-blue.svg)](https://www.typescriptlang.org/)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/github/workflow/status/Nuvix-Tech/database/CI)](https://github.com/Nuvix-Tech/database/actions)

A modular and performant database library for Nuvix, with internal complexity abstracted from developers. Built with TypeScript, this library provides a high-level interface for PostgreSQL databases with support for relationships, validation, caching, and more.

## Features

🚀 **High Performance** - Optimized queries and connection pooling  
🔒 **Type Safe** - Full TypeScript support with generated types  
📊 **Relationships** - OneToOne, OneToMany, ManyToOne, and ManyToMany relationships  
🛡️ **Security** - Race-free authorization via scoped sessions (`db.for`/`db.system`) and document-level permissions  
✅ **Validation** - Comprehensive data validation and structure checking  
🎯 **Query Builder** - Fluent query interface with filters, sorting, and pagination  
📇 **Indexing** - Support for key, unique, and fulltext indexes  
🔄 **Transactions** - ACID transaction support  
💾 **Caching** - Integrated caching layer for improved performance  
🏢 **Multi-tenancy** - Built-in support for shared tables and tenant isolation  
📝 **Migrations** - Schema migration support

## Installation

```bash
npm install @nuvix/db
# or
yarn add @nuvix/db
# or
bun add @nuvix/db
```

## Quick Start

```typescript
import {
  Database,
  Adapter,
  Doc,
  AttributeEnum,
  Permission,
  Role,
} from "@nuvix/db";
import { Memory } from "@nuvix/cache";

// Create database adapter (Bun-native PostgreSQL driver)
const adapter = new Adapter("postgres://user:pass@localhost:5432/mydb");

// Initialize database
const db = new Database(adapter, new Memory());

// Admin plane: schema management stays on the Database instance
await db.create();

// Create a collection (admin plane)
await db.createCollection({
  id: "users",
  attributes: [
    new Doc({
      $id: "name",
      key: "name",
      type: AttributeEnum.String,
      size: 100,
      required: true,
    }),
    new Doc({
      $id: "email",
      key: "email",
      type: AttributeEnum.String,
      size: 255,
      required: true,
    }),
    new Doc({
      $id: "age",
      key: "age",
      type: AttributeEnum.Integer,
      size: 4,
    }),
  ],
  permissions: [Permission.create(Role.any())],
});

// Document plane: open a session carrying the caller's roles.
// Role strings follow Role.toString(): "user:123", "role:admin", ...
const session = db.for("user:123", "role:admin");

// Create a document (authorized as the session's roles)
const user = await session.createDocument(
  "users",
  new Doc({
    name: "John Doe",
    email: "john@example.com",
    age: 30,
    $permissions: [Permission.read(Role.any()).toString()],
  }),
);

// Read a document
const retrieved = await session.getDocument("users", user.getId());

// Query documents
const users = await session.find("users", (qb) => qb.equal("age", 30).limit(10));

// Update a document
await session.updateDocument(
  "users",
  user.getId(),
  new Doc({
    age: 31,
  }),
);

// Delete a document
await session.deleteDocument("users", user.getId());

// Privileged internal work bypasses authorization checks entirely
const sys = db.system();
await sys.createDocument("audit_log", new Doc({ event: "user.created" }));
```

The API is split into two planes. **Schema operations** — `db.create()`,
`db.createCollection(...)`, attributes, relationships, indexes — stay on the
`Database` instance. **Document operations** require a session:
`db.for(...roles)` scopes every operation to the given roles, while
`db.system()` returns a privileged session that bypasses authorization
checks.

### SQLiteAdapter

`Adapter` remains the PostgreSQL adapter. For SQLite, use `SQLiteAdapter`,
which uses Bun's built-in `bun:sqlite` driver and adds no SQLite dependency.

Use an in-memory database for tests or temporary data:

```typescript
import { SQLiteAdapter } from "@nuvix/db";

const adapter = new SQLiteAdapter(":memory:");
await adapter.ping();
await adapter.$client.disconnect();
```

Pass a file path when data must persist between runs:

```typescript
import { SQLiteAdapter } from "@nuvix/db";

const adapter = new SQLiteAdapter("./app.sqlite");
await adapter.ping();
await adapter.$client.disconnect();
```

SQLite supports the same collection and document APIs for permissions,
relationships, and key or unique indexes. Adapter metadata behaves as follows:

- **Logical schemas** - SQLite has no PostgreSQL-style schemas. The `schema`
  and `namespace` values form a deterministic physical table-name prefix.
- **Shared-table tenancy** - `sharedTables` and `tenantId` scope both document
  rows and permission records to the current tenant.
- **Nested transactions** - Outer transactions use SQLite transactions;
  nested `withTransaction` calls use savepoints, so failed nested work can
  roll back without discarding successful outer work.

The following PostgreSQL features are deferred for SQLite:

- Fulltext indexes and fulltext search.
- GIN-style array indexes and array-overlap queries. Index creation is rejected
  before partial DDL is created.
- Update-lock reads do not emit `SELECT FOR UPDATE`; they follow SQLite's
  transaction serialization without providing PostgreSQL-style row locks.

## Core Concepts

### Generated Types

The type generator emits collection interfaces and an `Entities` registry that
augments `@nuvix/db`. Generate the file from your database configuration, then
include it with a type-only import in your application entry point:

```bash
bun run types:init
# Edit nuvix-db.config.ts, then generate the configured collections.
bun run generate-types
```

```typescript
import type {} from "./types/generated.js";
import { Database } from "@nuvix/db";

declare const db: Database;
const session = db.for("role:reader");

const users = await session.find("users", (query) =>
  query.equal("email", "reader@example.com"),
);

// With generated types loaded, unknown collection and attribute literals fail
// at compile time. Results are inferred as Doc<Users>[].
```

The import is opt-in: it activates the generated module augmentation without a
runtime dependency. Without generated types, collection IDs and query
attributes remain ordinary strings and document results use the untyped
fallback. A runtime-selected collection also keeps that fallback after the
registry is loaded:

```typescript
declare const collectionId: string;
const documents = await session.find(collectionId);
```

### Collections and Attributes

Collections are like tables in traditional databases. Each collection has attributes that define the structure of documents:

```typescript
// Define collection attributes
const attributes = [
  new Doc({
    $id: "title",
    key: "title",
    type: AttributeEnum.String,
    size: 200,
    required: true,
  }),
  new Doc({
    $id: "content",
    key: "content",
    type: AttributeEnum.String,
    size: 5000,
  }),
  new Doc({
    $id: "published",
    key: "published",
    type: AttributeEnum.Boolean,
    default: false,
  }),
  new Doc({
    $id: "tags",
    key: "tags",
    type: AttributeEnum.String,
    size: 50,
    array: true, // Array of strings
  }),
];
```

### Supported Attribute Types

- `AttributeEnum.String` - Text with specified size limit
- `AttributeEnum.Integer` - Integer numbers with optional size
- `AttributeEnum.Float` - Floating point numbers
- `AttributeEnum.Boolean` - True/false values
- `AttributeEnum.Json` - JSON objects
- `AttributeEnum.Uuid` - UUID strings
- `AttributeEnum.Timestamptz` - Timestamps with timezone
- `AttributeEnum.Relationship` - References to other documents

### Relationships

Create relationships between collections:

```typescript
// One-to-Many: User has many Posts
await db.createRelationship({
  collectionId: "users",
  relatedCollectionId: "posts",
  type: RelationEnum.OneToMany,
  id: "posts",
  twoWay: true,
  twoWayKey: "author",
});

// Many-to-Many: Posts have many Tags
await db.createRelationship({
  collectionId: "posts",
  relatedCollectionId: "tags",
  type: RelationEnum.ManyToMany,
  id: "tags",
  twoWay: true,
  twoWayKey: "posts",
});
```

### Querying

Use the fluent query builder for complex queries:

```typescript
const session = db.for("role:reader");

// Simple queries
const users = await session.find("users", (qb) =>
  qb
    .equal("status", "active")
    .greaterThan("age", 18)
    .limit(50)
    .offset(0)
    .orderBy("name", "ASC"),
);

// Complex queries with multiple conditions
const posts = await session.find("posts", (qb) =>
  qb
    .equal("published", true)
    .search("title", "typescript")
    .between("created_at", "2024-01-01", "2024-12-31")
    .contains("tags", ["tutorial", "guide"])
    .populate(["author", "comments"])
    .select(["title", "content", "author.name"]),
);

// Using Query objects directly
const results = await session.find("users", [
  Query.equal("status", ["active"]),
  Query.greaterThan("age", 18),
  Query.limit(25),
  Query.orderBy("name"),
]);
```

### Indexing

Create indexes for better query performance:

```typescript
// Key index for faster lookups
await db.createIndex("users", "idx_email", IndexEnum.Key, ["email"]);

// Unique index to enforce uniqueness
await db.createIndex("users", "idx_username", IndexEnum.Unique, ["username"]);

// Fulltext index for search
await db.createIndex("posts", "idx_content", IndexEnum.FullText, [
  "title",
  "content",
]);

// Composite index
await db.createIndex("posts", "idx_author_date", IndexEnum.Key, [
  "author",
  "created_at",
]);
```

### Permissions and Security

Authorization is explicit and race-free: the library holds no global auth
state. Every document operation runs inside a **session** that carries an
immutable `AuthContext` — the role strings the caller acts as:

```typescript
// Roles use Role.toString() format: "roleName", "roleName:id", ...
const editor = db.for("user:alice", "role:editor");

// Privileged internal work bypasses all checks
const sys = db.system();
```

Access decisions are made by the pure `authorize(ctx, permissions, action)`
function, evaluated in order:

1. A system context (`db.system()`) authorizes everything.
2. Empty permissions deny access.
3. Otherwise, access is granted when any granted permission string is one of
   the context's roles.

Permissions attach at two levels:

```typescript
// Collection-level permissions
await db.createCollection({
  id: 'posts',
  attributes: [...],
  permissions: [
    Permission.create(Role.user()),
    Permission.read(Role.any()),
    Permission.update(Role.user()),
    Permission.delete(Role.user())
  ],
  documentSecurity: true // Enable document-level permissions
});

// Document-level permissions
await editor.createDocument('posts', new Doc({
  title: 'My Post',
  content: 'Content here...',
  $permissions: [
    Permission.read(Role.any()).toString(),
    Permission.update(Role.user('user123')).toString(),
    Permission.delete(Role.user('user123')).toString()
  ]
}));
```

With the document above, any session can read the post, but only a session
carrying `user:123` may update or delete it. Unauthorized operations throw an
`AuthorizationException`:

```typescript
try {
  await db.for("user:bob").deleteDocument("posts", postId);
} catch (error) {
  if (error instanceof AuthorizationException) {
    // user:bob lacks the delete permission
  }
}
```

## Advanced Usage

### Transactions

Ensure data consistency with transactions. `withTransaction` lives on the
session; the callback receives a transactional session bound to the SAME
`AuthContext`, so nested document operations keep their authorization scope:

```typescript
const session = db.for("user:123");

await session.withTransaction(async (tx) => {
  const user = await tx.createDocument("users", userData);
  const profile = await tx.createDocument("profiles", {
    userId: user.getId(),
    ...profileData,
  });
  // Both operations succeed or both fail
});
```

### Multi-tenancy

Support multiple tenants in shared infrastructure:

```typescript
// Configure adapter for shared tables
adapter.setMeta({
  sharedTables: true,
  tenantId: 123,
  namespace: "tenant_app",
});

// All operations will be scoped to the tenant
const session = db.for("role:app");
const users = await session.find("users"); // Only returns tenant 123's users
```

### Caching

Leverage built-in caching for better performance:

```typescript
// Cache is automatically managed
const session = db.for("role:app");
const user = await session.getDocument("users", "user123"); // Fetches from DB
const userAgain = await session.getDocument("users", "user123"); // Returns from cache

// Manual cache control
await session.purgeCachedDocument("users", "user123");
await session.purgeCachedCollection("users");
```

### Event Handling

Listen to database events:

```typescript
db.on(EventsEnum.DocumentCreate, (document) => {
  console.log("Document created:", document.getId());
});

db.on(EventsEnum.CollectionCreate, (collection) => {
  console.log("Collection created:", collection.getId());
});
```

## Project Structure

```
src/
├── adapters/          # Database adapters (PostgreSQL)
│   ├── adapter.ts     # PostgreSQL runtime DML facade
│   ├── base.ts        # Shared adapter facade
│   ├── ddl.ts         # PostgreSQL DDL implementation
│   ├── postgres.ts    # PostgreSQL client wrapper
│   ├── sql-builder.ts # Shared SELECT/condition SQL construction
│   └── types.ts       # Adapter types
├── core/              # Core database functionality
│   ├── database.ts    # Admin-plane facade
│   ├── auth.ts        # AuthContext model and pure authorize()
│   ├── document-store.ts # Document CRUD/query implementation
│   ├── doc.ts         # Document class
│   ├── query.ts       # Query building
│   ├── cache.ts       # Caching layer
│   ├── schema-manager.ts # Collection/schema implementation
│   ├── session.ts     # Scoped document-plane facade
│   └── enums.ts       # Type enums
├── errors/            # Custom error classes
├── utils/             # Utility functions
│   ├── id.ts          # ID generation
│   ├── permission.ts  # Permission utilities
│   └── query-builder.ts # Query builder
└── validators/        # Data validation
    ├── schema.ts      # Schema validation
    ├── queries/       # Query validation
    └── permissions.ts # Permission validation
```

## Development

### Prerequisites

- [Bun](https://bun.sh) 1.4 or later (required — the library is built on Bun-native APIs)
- PostgreSQL 12 or later
- Docker (optional, for local test services via `docker compose up -d`)

### Setup

```bash
# Clone the repository
git clone https://github.com/Nuvix-Tech/database.git
cd database

# Install dependencies
bun install

# Set up environment variables
cp .env.example .env
# Edit .env with your database configuration

# Run tests
bun test

# Build the library
bun run build

# Type checking
bun run typecheck

# Lint code
bun run lint
```

### Running Tests

```bash
# Run all tests
bun test

# Watch mode
bun run test:watch

# Test specific file
bun test tests/database.basic.test.ts
```

### Environment Variables

```bash
# PostgreSQL connection string
PG_URL=postgres://postgres:postgres@localhost:5432/test_db
```

## Configuration

### Database Adapter Options

The adapter is powered by Bun's native SQL client. It accepts exactly two
forms: a connection string, or a pre-configured Bun `SQL` instance. Plain
option objects are **not** supported — use the `SQL` constructor for pool
tuning:

```typescript
import { Adapter } from "@nuvix/db";
import { SQL } from "bun";

// Connection string (recommended)
const adapter = new Adapter("postgres://user:pass@localhost:5432/myapp");

// Or a pre-configured Bun SQL instance for pool tuning
const adapter = new Adapter(
  new SQL("postgres://user:pass@localhost:5432/myapp", {
    max: 20, // maximum pool connections
    idleTimeout: 30, // seconds
    connectionTimeout: 30, // seconds
  }),
);

// Set metadata for multi-tenancy
adapter.setMeta({
  database: "myapp",
  schema: "public",
  sharedTables: false,
  tenantId: undefined,
  tenantPerDocument: false,
  namespace: "default",
});
```

### Cache Configuration

`@nuvix/db` depends on a minimal `CacheDriver` interface — four async
operations (`get`, `set`, `flushByTags`, `flush`). Any object satisfying it
works, including `Memory` and `Redis` from [`@nuvix/cache`](https://www.npmjs.com/package/@nuvix/cache),
which implement the interface structurally:

```typescript
import { Memory, Redis } from "@nuvix/cache";

// In-memory cache (development)
const db = new Database(adapter, new Memory());

// Redis cache (production)
const db = new Database(
  adapter,
  new Redis({ host: "localhost", port: 6379, password: "redis-password" }),
);
```

Or bring your own driver:

```typescript
import type { CacheDriver } from "@nuvix/db";

const myCache: CacheDriver = {
  async get(key) {
    /* return cached value or null */
  },
  async set(key, value, options) {
    /* options: { ttl?, tags? } */
  },
  async flushByTags(tags) {
    /* invalidate entries by tag */
  },
  async flush() {
    /* clear everything */
  },
};

const db = new Database(adapter, myCache);
```

## API Reference

The public surface is split into two planes:

- **Admin plane — `Database`**: schema, collections, attributes,
  relationships and indexes. No auth context involved.
- **Document plane — `Session`**: every document read/write. Open a session
  with `db.for(...roles)` (caller-scoped) or `db.system()` (privileged).

### Database Methods (admin plane)

- `for(...roles | rolesArray)` - Open a scoped session carrying the given roles
- `system()` - Open a privileged session that bypasses authorization checks

#### Collection Management

- `create(database?)` - Create database schema
- `createCollection(options)` - Create a new collection
- `getCollection(id)` - Get collection metadata
- `listCollections(limit?, offset?)` - List all collections
- `updateCollection(options)` - Update collection permissions
- `deleteCollection(id)` - Delete a collection

#### Attribute Management

- `createAttribute(collectionId, attribute)` - Add attribute to collection
- `updateAttribute(collectionId, id, options)` - Update attribute properties
- `deleteAttribute(collectionId, id)` - Remove attribute from collection
- `renameAttribute(collectionId, oldName, newName)` - Rename an attribute

#### Relationship Management

- `createRelationship(options)` - Create relationship between collections
- `updateRelationship(options)` - Update existing relationship
- `deleteRelationship(collectionId, id)` - Delete a relationship

#### Index Management

- `createIndex(collectionId, id, type, attributes)` - Create an index
- `deleteIndex(collectionId, id)` - Delete an index
- `renameIndex(collectionId, oldName, newName)` - Rename an index

### Session Methods (document plane)

- `find(collectionId, query?, forPermission?)` - Query documents
- `findOne(collectionId, query?)` - Find a single document
- `getDocument(collectionId, id, query?, forUpdate?)` - Get document by ID
- `createDocument(collectionId, document)` - Create a document
- `createDocuments(collectionId, documents)` - Create multiple documents
- `updateDocument(collectionId, id, updates)` - Update a document
- `updateDocuments(collectionId, updates, query?, batchSize?, onNext?, onError?)` - Update multiple documents
- `deleteDocument(collectionId, id)` - Delete a document
- `deleteDocuments(collectionId, query?)` - Delete multiple documents
- `deleteDocumentsBatch(collectionId, query?, batchSize?, onNext?, onError?)` - Delete multiple documents in batches
- `createOrUpdateDocuments(collectionId, documents, batchSize?, onNext?)` - Upsert documents
- `createOrUpdateDocumentsWithIncrease(collectionId, attribute, documents, batchSize?, onNext?)` - Upsert documents, incrementing an attribute on update
- `increaseDocumentAttribute(collectionId, id, attribute, value?, max?)` - Increment an attribute
- `decreaseDocumentAttribute(collectionId, id, attribute, value?, min?)` - Decrement an attribute
- `count(collectionId, query?, max?)` - Count documents
- `sum(collectionId, attribute, query?, max?)` - Sum an attribute across documents
- `purgeCachedDocument(collectionId, doc)` - Purge all cached variants of a document
- `purgeCachedCollection(collection)` - Purge every cached entry for a collection
- `withTransaction(callback)` - Run the callback in a transaction; receives a transactional session preserving the parent AuthContext

## Contributing

We welcome contributions! Please follow these guidelines:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Write** tests for your changes
4. **Ensure** all tests pass (`bun test`)
5. **Commit** your changes (`git commit -m 'Add amazing feature'`)
6. **Push** to the branch (`git push origin feature/amazing-feature`)
7. **Open** a Pull Request

### Code Style

- Use TypeScript with strict type checking
- Follow existing code formatting (Prettier)
- Write comprehensive tests for new features
- Update documentation for API changes

### Testing

All contributions must include tests. We use Bun's built-in test runner:

```typescript
import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { createTestDb } from "./helpers.js";

describe("Feature", () => {
  const db = createTestDb();

  beforeAll(async () => {
    await db.create();
  });

  afterAll(async () => {
    await db.getAdapter().$client.disconnect();
  });

  it("should work correctly", async () => {
    // Test implementation
    expect(result).toBe(expected);
  });
});
```

Tests require a running PostgreSQL instance — start one with
`docker compose up -d` and set `PG_URL` (see
[Environment Variables](#environment-variables)).

## License

This project is licensed under the BSD 3-Clause License - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 [Documentation](https://docs.nuvix.in/database)
- 🐛 [Issue Tracker](https://github.com/Nuvix-Tech/database/issues)
- 💬 [Discussions](https://github.com/Nuvix-Tech/database/discussions)
- 📧 [Email Support](mailto:support@nuvix.in)

---

Built with ❤️ by the Nuvix team
