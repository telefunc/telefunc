// Engine cost per write: N users, each watching one live query; a stream of writes like a busy todo app's.
// Times the synchronous matching of each committed change against the live reads (`Router.route`).
//
//   N=1000 VARIANT=topk|join|count WRITES=2000 node --expose-gc engine.ts
//
// Workload from the scale study of #455: 60 todos per user; 80% updates (half text + updated_at, half `done`),
// 15% inserts, 5% deletes, users picked uniformly; plain Drizzle writes without `.returning()`.

import { AsyncLocalStorage } from 'node:async_hooks'
import { PGlite } from '@electric-sql/pglite'
import { withLiveQueries } from '@telefunc/live'
import { count, desc, eq, sql } from 'drizzle-orm'
import { boolean, integer, pgTable, serial, text, timestamp } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { Router } from '../../packages/live/dist/match/router.js'
import { LiveRead } from '../../packages/live/dist/match/liveRead.js'

const N = Number(process.env.N ?? 1000)
const VARIANT = process.env.VARIANT ?? 'topk'
const WRITES = Number(process.env.WRITES ?? 2000)
const WARMUP = 200

const users = pgTable('users', { id: serial('id').primaryKey(), name: text('name').notNull() })
const todos = pgTable('todos', {
  id: serial('id').primaryKey(),
  userId: integer('user_id').notNull(),
  text: text('text').notNull(),
  done: boolean('done').notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull(),
})

const pglite = new PGlite()
await pglite.exec(`
  create table users (id serial primary key, name text not null);
  create table todos (id serial primary key, user_id int not null, text text not null, done boolean not null, updated_at timestamptz not null);
  create index on todos (user_id, updated_at desc);
  insert into users (name) select 'user ' || i from generate_series(1, ${N}) i;
  insert into todos (user_id, text, done, updated_at)
    select u, 'todo ' || t, t % 2 = 0, '2026-11-10'::timestamptz + (t || ' minutes')::interval from generate_series(1, ${N}) u, generate_series(1, 60) t;
`)
withLiveQueries(pglite)
const db = drizzle({ client: pglite })

const queries = {
  topk: (user: number) =>
    db.select().from(todos).where(eq(todos.userId, user)).orderBy(desc(todos.updatedAt)).limit(50),
  join: (user: number) =>
    db
      .select({ id: todos.id, text: todos.text, name: users.name })
      .from(todos)
      .innerJoin(users, eq(users.id, todos.userId))
      .where(eq(todos.userId, user))
      .orderBy(desc(todos.updatedAt))
      .limit(50),
  count: (user: number) =>
    db.select({ done: todos.done, n: count() }).from(todos).where(eq(todos.userId, user)).groupBy(todos.done),
}
const query = queries[VARIANT as keyof typeof queries]

// ── Live reads, one per user ──

const store: AsyncLocalStorage<object> = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??=
  new AsyncLocalStorage())
const REQUEST_CONTEXT = Symbol.for('telefunc.requestContext')
const LIVE_READS = Symbol.for('telefunc.liveReads')
let invalidations = 0
const stale = new Set<number>()

async function subscribe(user: number) {
  const context: Record<symbol, unknown> = { [REQUEST_CONTEXT]: { onClose() {} } }
  await store.run(context, async () => await query(user))
  ;(context[LIVE_READS] as { onChange(cb: () => void): () => void }).onChange(() => {
    invalidations++
    stale.add(user)
  })
}

const setupStart = performance.now()
global.gc?.()
const heapBefore = process.memoryUsage().heapUsed
for (let user = 1; user <= N; user++) await subscribe(user)
global.gc?.()
const heapPerRead = (process.memoryUsage().heapUsed - heapBefore) / N
const setupSeconds = (performance.now() - setupStart) / 1000

// ── Timing ──

let engineNs = 0n
let reached = 0
const route = Router.prototype.route
Router.prototype.route = function (this: Router, change: unknown) {
  const start = process.hrtime.bigint()
  route.call(this, change as never)
  engineNs += process.hrtime.bigint() - start
}
const consider = LiveRead.prototype.consider
LiveRead.prototype.consider = function (this: LiveRead, change: unknown) {
  reached++
  return consider.call(this, change as never)
}

let seed = 1
const random = () => {
  seed = (seed * 16807) % 2147483647
  return seed / 2147483647
}
let nextTodo = N * 60 + 1
const ids = Array.from({ length: N * 60 }, (_, i) => i + 1)

async function write(i: number) {
  const r = random()
  const user = 1 + Math.floor(random() * N)
  const todo = ids[Math.floor(random() * ids.length)]!
  const at = new Date(Date.UTC(2026, 10, 11, 0, 0, i))
  if (r < 0.4)
    await db
      .update(todos)
      .set({ text: `edited ${i}`, updatedAt: at })
      .where(eq(todos.id, todo))
  else if (r < 0.8)
    await db
      .update(todos)
      .set({ done: sql`not ${todos.done}` })
      .where(eq(todos.id, todo))
  else if (r < 0.95) {
    await db.insert(todos).values({ userId: user, text: `new ${i}`, done: false, updatedAt: at })
    ids.push(nextTodo++)
  } else {
    await db.delete(todos).where(eq(todos.id, todo))
    ids.splice(ids.indexOf(todo), 1)
  }
}

for (let i = 0; i < WARMUP; i++) {
  await write(i)
  for (const user of stale) await subscribe(user)
  stale.clear()
}
engineNs = 0n
reached = 0
invalidations = 0
let totalMs = 0
for (let i = 0; i < WRITES; i++) {
  const start = performance.now()
  await write(WARMUP + i)
  totalMs += performance.now() - start
  for (const user of stale) await subscribe(user)
  stale.clear()
}
const engineUs = Number(engineNs / 1000n) / WRITES
console.log(
  JSON.stringify({
    variant: VARIANT,
    N,
    writes: WRITES,
    engineUsPerWrite: Number(engineUs.toFixed(1)),
    engineMaxWritesPerSecond: Math.round(1e6 / engineUs),
    readsReachedPerWrite: Number((reached / WRITES).toFixed(2)),
    invalidationsPerWrite: Number((invalidations / WRITES).toFixed(3)),
    writeMsWithPglite: Number((totalMs / WRITES).toFixed(3)),
    heapPerLiveReadKB: Number((heapPerRead / 1024).toFixed(1)),
    setupSeconds: Number(setupSeconds.toFixed(1)),
    load: (await import('node:os')).loadavg()[0]!.toFixed(2),
  }),
)
await pglite.close()
