export { dbA, dbB, ensureTablesReady, todos, notes }

import { PGlite } from '@electric-sql/pglite'
import { pgTable, serial, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'

const todos = pgTable('todos', { id: serial('id').primaryKey(), text: text('text').notNull() })
const notes = pgTable('notes', { id: serial('id').primaryKey(), body: text('body').notNull() })

// TWO SERVER INSTANCES, ONE DATABASE. `dbA` and `dbB` are distinct drizzle objects over the SAME PGlite
// client, so they share the data but NOT their live-query runtime: each gets its own registry, graphs and
// change-transport subscriptions. That is exactly the cross-instance topology this test has to prove —
// instance A writes, instance B serves the browser's live reads, and the invalidation has to travel between
// them over the shared in-process change transport rather than through a shared graph.
const client = new PGlite()
const dbA = drizzle({ client })
const dbB = drizzle({ client })

// Created once per server process, awaited by every telefunction (module init can't await).
let ready: Promise<void> | undefined
function ensureTablesReady(): Promise<void> {
  if (!ready) {
    ready = (async () => {
      await client.exec('create table if not exists todos (id serial primary key, text text not null)')
      await client.exec('create table if not exists notes (id serial primary key, body text not null)')
    })()
  }
  return ready
}
