export { onGetTodos, onGetNotes, onAddTodo, onAddTx, onAddCoarse }

import { reactiveDrizzle } from '@telefunc/drizzle'
import { sql } from 'drizzle-orm'
import { dbA, dbB, ensureTablesReady, notes, todos } from './db'

// READS run on instance B — these serve the browser's two live queries.
// `reactiveDrizzle(db)` must be called BEFORE the first await (it reads the per-request context).

async function onGetTodos() {
  const db = reactiveDrizzle(dbB)
  await ensureTablesReady()
  return db.select().from(todos).live()
}

/** The CONTROL query. Nothing any of the writes below touch should ever invalidate this — except the
 *  transaction, which deliberately writes to `notes` too. */
async function onGetNotes() {
  const db = reactiveDrizzle(dbB)
  await ensureTablesReady()
  return db.select().from(notes).live()
}

// WRITES run on instance A — the "other server". Reaching B's live reads proves cross-instance delivery.

async function onAddTodo(text: string) {
  const db = reactiveDrizzle(dbA)
  await ensureTablesReady()
  await db.insert(todos).values({ text })
  return { ok: true }
}

/** One transaction touching TWO tables: the batch must reach both tables' topics and apply as ONE atomic
 *  tick on the receiving instance (per-table topics + the deterministic cross-topic dedupe rule). */
async function onAddTx() {
  const db = reactiveDrizzle(dbA)
  await ensureTablesReady()
  await db.transaction(async (tx) => {
    await tx.insert(todos).values({ text: 'tx-todo' })
    await tx.insert(notes).values({ body: 'tx-note' })
  })
  return { ok: true }
}

/** A FAIL-CLOSED COARSE write: raw SQL, whose touched tables are unknowable without parsing it. It must
 *  still invalidate cross-instance — via the wildcard coarse channel, since instance A cannot know which
 *  tables instance B watches. */
async function onAddCoarse() {
  const db = reactiveDrizzle(dbA)
  await ensureTablesReady()
  await db.execute(sql`insert into todos (text) values ('coarse')`)
  return { ok: true }
}
