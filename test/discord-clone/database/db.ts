export { db, dmThreadKey }

import * as fs from 'node:fs'
import * as path from 'node:path'
import Database from 'better-sqlite3'
import { drizzle } from 'drizzle-orm/better-sqlite3'
import * as schema from './schema'

// SQLite via Drizzle. `DISCORD_CLONE_DB` overrides the file (the e2e suite uses `:memory:`).
//
// The handle is latched on `globalThis`: in dev, the HTTP server entry and the (Vite-SSR
// transformed) telefunction modules live in different module graphs, so a plain module-level
// `new Database(':memory:')` would run twice and split the app across two databases.
const g = globalThis as { __discordCloneSqlite?: Database.Database }
const sqlite = (g.__discordCloneSqlite ??= openDatabase())

function openDatabase(): Database.Database {
  const file = process.env.DISCORD_CLONE_DB ?? path.join(process.cwd(), '.data', 'discord-clone.db')
  if (file !== ':memory:') fs.mkdirSync(path.dirname(file), { recursive: true })
  const handle = new Database(file)
  handle.pragma('journal_mode = WAL')
  handle.pragma('foreign_keys = ON')
  return handle
}

// Idempotent bootstrap DDL, kept in sync with schema.ts (this demo app skips migration
// tooling on purpose; a real deployment would generate migrations with drizzle-kit).
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    color TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 0,
    is_bot INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    expires_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS channels (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    author_id TEXT NOT NULL,
    author_name TEXT NOT NULL,
    author_color TEXT NOT NULL,
    author_is_bot INTEGER NOT NULL DEFAULT 0,
    text TEXT NOT NULL,
    at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS messages_channel_at ON messages(channel_id, at);
  CREATE TABLE IF NOT EXISTS dms (
    id TEXT PRIMARY KEY,
    thread_key TEXT NOT NULL,
    from_id TEXT NOT NULL,
    from_name TEXT NOT NULL,
    to_id TEXT NOT NULL,
    to_name TEXT NOT NULL,
    text TEXT NOT NULL,
    at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS dms_thread_at ON dms(thread_key, at);
`)

const db = drizzle(sqlite, { schema })

function dmThreadKey(userIdA: string, userIdB: string): string {
  return [userIdA, userIdB].sort().join(':')
}
