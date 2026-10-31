export { db, dmThreadKey }

import * as fs from 'node:fs'
import * as path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { DDL } from './schema'

// Node's built-in SQLite. `DISCORD_CLONE_DB` overrides the file (the e2e suite uses `:memory:`).
//
// The handle is latched on `globalThis`: in dev, the HTTP server entry and the (Vite-SSR
// transformed) telefunction modules live in different module graphs, so a plain module-level
// `new DatabaseSync(':memory:')` would run twice and split the app across two databases.
const g = globalThis as { __discordCloneSqlite?: DatabaseSync }
const db = (g.__discordCloneSqlite ??= openDatabase())

function openDatabase(): DatabaseSync {
  const file = process.env.DISCORD_CLONE_DB ?? path.join(process.cwd(), '.data', 'discord-clone.db')
  if (file !== ':memory:') fs.mkdirSync(path.dirname(file), { recursive: true })
  const handle = new DatabaseSync(file)
  handle.exec('PRAGMA journal_mode = WAL')
  handle.exec('PRAGMA foreign_keys = ON')
  handle.exec(DDL) // idempotent bootstrap (this demo app skips migration tooling on purpose)
  return handle
}

function dmThreadKey(userIdA: string, userIdB: string): string {
  return [userIdA, userIdB].sort().join(':')
}
