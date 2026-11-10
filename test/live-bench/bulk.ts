// What one bulk write costs: an `UPDATE` of ROWS rows over a plain `pg.Pool` and through `liveDrizzle()`, while
// N clients each watch their own user's latest todos.
//
//   PG_PORT=<port> ROWS=10000 N=1000 RUNS=7 node --expose-gc bulk.ts
//
// Reports the medians of RUNS updates per pool: the time until the statement's promise resolved, the bytes Postgres
// sent back, the heap the update left allocated before a collection, and the time spent matching its changes.

import { AsyncLocalStorage } from 'node:async_hooks'
import { liveDrizzle } from '@telefunc/live/drizzle'
import { desc, eq, lte, sql } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/node-postgres'
import { boolean, integer, pgTable, serial, text, timestamp } from 'drizzle-orm/pg-core'
import pg from 'pg'
import type { LiveCell } from '../../packages/live/dist/live.js'
import { Router } from '../../packages/live/dist/match/router.js'

const ROWS = Number(process.env.ROWS ?? 10_000)
const N = Number(process.env.N ?? 1000)
const RUNS = Number(process.env.RUNS ?? 7)
const USERS = 10_000
const TODOS_PER_USER = 20

const todos = pgTable('todos', {
  id: serial('id').primaryKey(),
  userId: integer('user_id').notNull(),
  text: text('text').notNull(),
  done: boolean('done').notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull(),
})

const url = `postgres://postgres:postgres@127.0.0.1:${process.env.PG_PORT}/${process.env.PG_DATABASE ?? 'postgres'}`
const admin = new pg.Client({ connectionString: url })
await admin.connect()
await admin.query(`
  drop table if exists todos;
  create table todos (id serial primary key, user_id int not null, text text not null, done boolean not null, updated_at timestamptz not null);
  create index on todos (user_id, updated_at desc);
  insert into todos (user_id, text, done, updated_at)
    select u, 'todo ' || t, t % 2 = 0, '2026-11-10'::timestamptz + (t || ' minutes')::interval
    from generate_series(1, ${TODOS_PER_USER}) t, generate_series(1, ${USERS}) u;
  analyze todos;
`)
await admin.end()

/** A pool that counts the bytes its connections received. */
function countingPool() {
  const pool = new pg.Pool({ connectionString: url, max: 4 })
  const sockets: { bytesRead: number }[] = []
  pool.on('connect', (client: any) => sockets.push(client.connection.stream))
  return { pool, bytesRead: () => sockets.reduce((sum, socket) => sum + socket.bytesRead, 0) }
}
const plain = countingPool()
const live = countingPool()
const plainDb = drizzle({ client: plain.pool })
const liveDb = liveDrizzle(drizzle({ client: live.pool }))

// ── Clients watching their own user's todos ──

const store: AsyncLocalStorage<object> = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??=
  new AsyncLocalStorage())
const REQUEST_CONTEXT = Symbol.for('telefunc.requestContext')

let invalidations = 0
/** Users whose read changed: they fetch again after the sample, so that their reads aren't timed with the write. */
const stale: number[] = []
async function watch(user: number): Promise<void> {
  const context: Record<symbol, unknown> = { [REQUEST_CONTEXT]: { onClose: () => {} } }
  const live = (await store.run(context, () =>
    liveDb.select().from(todos).where(eq(todos.userId, user)).orderBy(desc(todos.updatedAt)).limit(20).live(),
  )) as unknown as LiveCell<unknown>
  // As the response sending it does.
  live.send()
  live.onStale(() => {
    invalidations++
    stale.push(user)
  })
}
async function watchAll(users: number[]): Promise<void> {
  for (let i = 0; i < users.length; i += 50) await Promise.all(users.slice(i, i + 50).map(watch))
}
await watchAll(Array.from({ length: N }, (_, i) => 1 + i))

let engineNs = 0n
const route = Router.prototype.route
Router.prototype.route = function (this: Router, change: unknown) {
  const start = process.hrtime.bigint()
  route.call(this, change as never)
  engineNs += process.hrtime.bigint() - start
}

// ── The bulk update, alternating between the pools ──

type Sample = { ms: number; bytes: number; heapMB: number; engineMs: number }
async function bulkUpdate(db: typeof plainDb, bytesRead: () => number, run: number): Promise<Sample> {
  global.gc?.()
  const [bytes, heap, engine] = [bytesRead(), process.memoryUsage().heapUsed, engineNs]
  const start = performance.now()
  // Ids are user-minor: the first ROWS ids are the latest-but-19 todo of each of up to USERS users.
  await db
    .update(todos)
    .set({ text: sql`'bulk ' || ${run}::text`, done: sql`not ${todos.done}` })
    .where(lte(todos.id, ROWS))
  return {
    ms: performance.now() - start,
    bytes: bytesRead() - bytes,
    heapMB: (process.memoryUsage().heapUsed - heap) / 2 ** 20,
    engineMs: Number(engineNs - engine) / 1e6,
  }
}

const samples: Record<'plain' | 'live', Sample[]> = { plain: [], live: [] }
for (let run = 0; run <= RUNS; run++) {
  const plainSample = await bulkUpdate(plainDb, plain.bytesRead, run)
  const liveSample = await bulkUpdate(liveDb, live.bytesRead, run)
  // The first run of each warms up.
  if (run === 0) continue
  samples.plain.push(plainSample)
  samples.live.push(liveSample)
  await watchAll(stale.splice(0))
}

const median = (values: number[]) => values.toSorted((a, b) => a - b)[Math.floor(values.length / 2)]!
const summary = (list: Sample[]) => ({
  ms: Number(median(list.map((s) => s.ms)).toFixed(1)),
  kB: Math.round(median(list.map((s) => s.bytes)) / 1024),
  heapMB: Number(median(list.map((s) => s.heapMB)).toFixed(1)),
  engineMs: Number(median(list.map((s) => s.engineMs)).toFixed(2)),
})
console.log(
  JSON.stringify({
    rows: ROWS,
    N,
    plain: summary(samples.plain),
    live: summary(samples.live),
    invalidationsPerUpdate: Math.round(invalidations / (RUNS + 1)),
  }),
)
await Promise.all([plain.pool.end(), live.pool.end()])
