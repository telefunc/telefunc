// What the user pays on Postgres: the same Drizzle app over a plain `pg.Pool` and over `withLiveQueries(pool)`, with
// writes sent open-loop at a fixed rate, reads at a fixed rate, and N clients each keeping one live read fresh.
//
//   PG_PORT=<port> MODE=plain|live RATE=1000 READ_RATE=200 SECONDS=10 N=1000 FEED=0|1 node --expose-gc latency.ts
//
// Each client watches its own user's latest todos, or with FEED=1 the latest todos of everyone (a public feed). A
// client refetches when its read is invalidated, with at most one refetch in flight and one more queued, as `watch()`
// does. Latency is measured from when a statement was due, so a saturated pool shows as queueing.

import { AsyncLocalStorage } from 'node:async_hooks'
import { withLiveQueries } from '@telefunc/live'
import { desc, eq, sql } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/node-postgres'
import { boolean, integer, pgTable, serial, text, timestamp } from 'drizzle-orm/pg-core'
import pg from 'pg'
import { Router } from '../../packages/live/dist/match/router.js'

const MODE = process.env.MODE ?? 'live'
const RATE = Number(process.env.RATE ?? 1000)
const READ_RATE = Number(process.env.READ_RATE ?? 200)
const SECONDS = Number(process.env.SECONDS ?? 10)
const N = Number(process.env.N ?? 1000)
const FEED = process.env.FEED === '1'
const USERS = Math.max(N, 1000)
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
  create index on todos (updated_at desc);
  insert into todos (user_id, text, done, updated_at)
    select u, 'todo ' || t, t % 2 = 0, '2026-11-10'::timestamptz + (t || ' minutes')::interval
    from generate_series(1, ${USERS}) u, generate_series(1, ${TODOS_PER_USER}) t;
  analyze todos;
`)
await admin.end()

const pool = new pg.Pool({ connectionString: url, max: 20 })
if (MODE === 'live') withLiveQueries(pool)
const db = drizzle({ client: pool })
const latest = (user: number) =>
  FEED
    ? db.select().from(todos).orderBy(desc(todos.updatedAt)).limit(20)
    : db.select().from(todos).where(eq(todos.userId, user)).orderBy(desc(todos.updatedAt)).limit(20)

// ── Telefunction calls ──

const store: AsyncLocalStorage<object> = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??=
  new AsyncLocalStorage())
const REQUEST_CONTEXT = Symbol.for('telefunc.requestContext')
const LIVE_READS = Symbol.for('telefunc.liveReads')

/** Runs `read` as a telefunction call would; returns how to watch what it read, if anything. */
async function call(read: () => Promise<unknown>) {
  const closing: (() => void)[] = []
  const context: Record<symbol, unknown> = { [REQUEST_CONTEXT]: { onClose: (cb: () => void) => closing.push(cb) } }
  await store.run(context, async () => await read())
  return {
    watch: (onChange: () => void) =>
      (context[LIVE_READS] as { onChange(cb: () => void): () => void } | undefined)?.onChange(onChange),
    close: () => closing.forEach((cb) => cb()),
  }
}

// ── Clients keeping a live read fresh ──

let invalidations = 0
let refetches = 0
let refetchesInFlight = 0
class Client {
  private inFlight = false
  private queued = false
  constructor(private readonly user: number) {}
  async fetch(): Promise<void> {
    this.inFlight = true
    const { watch } = await call(() => latest(this.user))
    watch(() => this.invalidated())
    this.inFlight = false
    if (this.queued) this.refetch()
  }
  private invalidated() {
    invalidations++
    this.refetch()
  }
  private refetch() {
    if (this.inFlight) {
      this.queued = true
      return
    }
    this.queued = false
    refetches++
    refetchesInFlight++
    void this.fetch().finally(() => refetchesInFlight--)
  }
}

global.gc?.()
const heapBefore = process.memoryUsage().heapUsed
const clients = Array.from({ length: N }, (_, i) => new Client(1 + (i % USERS)))
for (let i = 0; i < clients.length; i += 50) await Promise.all(clients.slice(i, i + 50).map((c) => c.fetch()))
global.gc?.()
const heapPerClientKB = N > 0 ? (process.memoryUsage().heapUsed - heapBefore) / N / 1024 : 0
invalidations = 0
refetches = 0

let engineNs = 0n
const route = Router.prototype.route
Router.prototype.route = function (this: Router, change: unknown) {
  const start = process.hrtime.bigint()
  route.call(this, change as never)
  engineNs += process.hrtime.bigint() - start
}

// ── Open-loop load ──

let seed = 1
const random = () => {
  seed = (seed * 16807) % 2147483647
  return seed / 2147483647
}
let maxId = USERS * TODOS_PER_USER

async function write(i: number) {
  const r = random()
  const todo = 1 + Math.floor(random() * maxId)
  const at = new Date(Date.UTC(2026, 10, 11, 0, 0, 0, i))
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
    maxId++
    await db
      .insert(todos)
      .values({ userId: 1 + Math.floor(random() * USERS), text: `new ${i}`, done: false, updatedAt: at })
  } else await db.delete(todos).where(eq(todos.id, todo))
}

async function read() {
  const { close } = await call(() => latest(1 + Math.floor(random() * USERS)))
  close()
}

/** Starts `run(i)` at `rate` per second for `SECONDS`, without waiting for earlier ones; returns each one's latency. */
async function openLoop(rate: number, run: (i: number) => Promise<void>): Promise<number[]> {
  const latencies: number[] = []
  const pending: Promise<void>[] = []
  const start = performance.now()
  const total = Math.round(rate * SECONDS)
  for (let i = 0; i < total; i++) {
    const due = start + (i * 1000) / rate
    const wait = due - performance.now()
    if (wait > 1) await new Promise((resolve) => setTimeout(resolve, wait))
    pending.push(run(i).then(() => void latencies.push(performance.now() - due)))
  }
  await Promise.all(pending)
  return latencies
}

const loadStart = performance.now()
const [writeLatencies, readLatencies] = await Promise.all([openLoop(RATE, write), openLoop(READ_RATE, read)])
const loadSeconds = (performance.now() - loadStart) / 1000
while (refetchesInFlight > 0) await new Promise((resolve) => setTimeout(resolve, 10))

const percentile = (values: number[], p: number) => {
  const sorted = [...values].sort((a, b) => a - b)
  return Number(sorted[Math.min(sorted.length - 1, Math.floor((sorted.length * p) / 100))]!.toFixed(2))
}
console.log(
  JSON.stringify({
    mode: MODE,
    feed: FEED,
    N,
    rate: RATE,
    writesPerSecond: Math.round(writeLatencies.length / loadSeconds),
    writeMs: { p50: percentile(writeLatencies, 50), p99: percentile(writeLatencies, 99) },
    readMs: { p50: percentile(readLatencies, 50), p99: percentile(readLatencies, 99) },
    engineUsPerWrite: Number((Number(engineNs / 1000n) / writeLatencies.length).toFixed(1)),
    invalidationsPerSecond: Math.round(invalidations / loadSeconds),
    refetchesPerSecond: Math.round(refetches / loadSeconds),
    heapPerClientKB: Number(heapPerClientKB.toFixed(1)),
    load: (await import('node:os')).loadavg()[0]!.toFixed(2),
  }),
)
await pool.end()
