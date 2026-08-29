import { Adapter, Database, SQLiteAdapter } from "@nuvix/db";
import type { CacheDriver } from "@nuvix/db";

declare const cache: CacheDriver;

const memory = new SQLiteAdapter(":memory:");
const file = new SQLiteAdapter("app.sqlite");

const memoryDatabase = new Database(memory, cache);
new Database(file, cache);

// Calling getAdapter() without a type argument preserves the historical
// PostgreSQL return type. SQLite callers can request their concrete adapter.
const legacyAdapter: Adapter = memoryDatabase.getAdapter();
const sqliteAdapter = memoryDatabase.getAdapter<SQLiteAdapter>();

void memory;
void file;
void legacyAdapter;
void sqliteAdapter;
