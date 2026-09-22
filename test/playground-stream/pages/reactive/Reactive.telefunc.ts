export { onGetTodos, onGetNotes, onAddTodo, onAddTx, onAddCoarse }

import { reactiveDrizzle } from '@telefunc/drizzle-experimental'
import { sql } from 'drizzle-orm'
import { dbA, dbB, ensureTablesReady, notes, todos } from './db'

// MODULE-LEVEL reactive dbs, wrapped once and shared by every telefunction below — the intended shape, and
// the thing this page proves end-to-end: `reactiveDrizzle()` binds to nothing request-scoped, so these are
// created at import time, long before any request exists. Every `.live()` below is also called AFTER an
// await, which the previous per-request design could not support.
//
// READS run on instance B — these serve the browser's two live queries.
// WRITES run on instance A — the "other server". Reaching B's live reads proves cross-instance delivery.
const reactiveB = reactiveDrizzle(dbB)
const reactiveA = reactiveDrizzle(dbA)

async function onGetTodos() {
  await ensureTablesReady()
  return reactiveB.select().from(todos).live()
}

/** The CONTROL query. Nothing any of the writes below touch should ever invalidate this — except the
 *  transaction, which deliberately writes to `notes` too. */
async function onGetNotes() {
  await ensureTablesReady()
  return reactiveB.select().from(notes).live()
}

async function onAddTodo(text: string) {
  await ensureTablesReady()
  await reactiveA.insert(todos).values({ text })
  return { ok: true }
}

/** One transaction touching TWO tables: the whole batch is published ONCE and must apply as ONE atomic
 *  tick on the receiving instance, invalidating both tables' live queries. */
async function onAddTx() {
  await ensureTablesReady()
  await reactiveA.transaction(async (tx) => {
    await tx.insert(todos).values({ text: 'tx-todo' })
    await tx.insert(notes).values({ body: 'tx-note' })
  })
  return { ok: true }
}

/** A FAIL-CLOSED COARSE write: raw SQL, whose touched tables are unknowable without parsing it. It must
 *  still invalidate cross-instance — via a coarse-all announcement, since instance A cannot know which
 *  tables instance B watches. */
async function onAddCoarse() {
  await ensureTablesReady()
  await reactiveA.execute(sql`insert into todos (text) values ('coarse')`)
  return { ok: true }
}
