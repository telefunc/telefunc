// Differential oracle for @telefunc/live: after every write, each live read is run again; a result that changed
// without an invalidation is a miss (exit 1). Extra invalidations (refetches that return the same result) are reported.
//
//   ORM=drizzle|kysely|prisma DB=pglite|pg PG_PORT=<port> PG_DATABASE=postgres SEED=1 STEPS=300 CHURN=1 SERVERS=2
//   SHAPES=byUser,recent node oracle.ts
//
// SERVERS=2 (pooled Postgres): the live reads are on one server, the writes on another, connected by a transport that
// delivers out of order and twice.
//
// A telefunction call is simulated by running the read with a call context, as telefunc's server does.

import { AsyncLocalStorage } from 'node:async_hooks'
import { PGlite } from '@electric-sql/pglite'
import { withLiveQueries } from '@telefunc/live'
import pg from 'pg'
import { createDrizzle } from './orms/drizzle.ts'
import { createKysely } from './orms/kysely.ts'
import { createPrisma } from './orms/prisma.ts'
import type { OrmDriver, ReadArgs, WriteArgs } from './orms/types.ts'
import { seed, USERS } from './schema.ts'

const ORM = process.env.ORM ?? 'drizzle'
const DB = process.env.DB ?? 'pglite'
const SEED = Number(process.env.SEED ?? 1)
const STEPS = Number(process.env.STEPS ?? 300)
const CHURN = process.env.CHURN === '1' && DB !== 'pglite'
const VERBOSE = process.env.VERBOSE === '1'
const SERVERS = DB === 'pglite' ? 1 : Number(process.env.SERVERS ?? 1)
const SHAPES = process.env.SHAPES?.split(',')

const REQUEST_CONTEXT = Symbol.for('telefunc.requestContext')
const LIVE_READS = Symbol.for('telefunc.liveReads')
type LiveReads = { onChange(listener: () => void): () => void }

const random = mulberry32(SEED)
const pick = <T>(items: readonly T[]): T => items[Math.floor(random() * items.length)]!

// ── Database and ORM ──

/** A `BroadcastAdapter` between the two servers of this process that delivers late, out of order, and some messages
 *  twice. */
class UnreliableTransport {
  private readonly listeners = new Map<
    string,
    Set<(serialized: string, info: { seq: number; timestamp: number }) => void>
  >()
  private readonly seqs = new Map<string, number>()
  subscribe(key: string, onMessage: (serialized: string, info: { seq: number; timestamp: number }) => void) {
    if (!this.listeners.has(key)) this.listeners.set(key, new Set())
    this.listeners.get(key)!.add(onMessage)
    return () => this.listeners.get(key)!.delete(onMessage)
  }
  publish(key: string, serialized: string) {
    const info = { seq: (this.seqs.get(key) ?? 0) + 1, timestamp: Date.now() }
    this.seqs.set(key, info.seq)
    const deliver = () => this.listeners.get(key)?.forEach((onMessage) => onMessage(serialized, info))
    // Two turns at most, so that a step's checks run after every delivery.
    setImmediate(random() < 0.3 ? () => setImmediate(deliver) : deliver)
    if (random() < 0.1) setImmediate(deliver)
    return info
  }
  subscribeBinary(): () => void {
    throw new Error('Unused')
  }
  publishBinary(): never {
    throw new Error('Unused')
  }
}

const telefuncGlobals = (globalThis as any)._telefunc
const store: AsyncLocalStorage<object> = (telefuncGlobals['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())

// A racing read's rows reach the app 10 ms late, so that the write racing it can commit after the read's snapshot
// and be announced before the read's result is known.
const LATE = Symbol('late')
const pgQuery = pg.Client.prototype.query as (...args: unknown[]) => unknown
pg.Client.prototype.query = function (this: pg.Client, ...args: unknown[]) {
  const result = pgQuery.apply(this, args)
  if (!(store.getStore() as Record<symbol, unknown> | undefined)?.[LATE] || !(result instanceof Promise)) return result
  return result.then(async (rows) => {
    await new Promise((resolve) => setTimeout(resolve, 10))
    return rows
  })
} as never

const { driver, writer, query, close } = await connect()

async function connect(): Promise<{
  driver: OrmDriver
  writer: OrmDriver
  query: (text: string) => Promise<any[]>
  close: () => Promise<void>
}> {
  if (DB === 'pglite') {
    const pglite = new PGlite()
    await pglite.exec(seed(random))
    withLiveQueries(pglite)
    const driver = ORM === 'kysely' ? createKysely(pglite, 'pglite') : createDrizzle(pglite, 'pglite')
    return {
      driver,
      writer: driver,
      query: async (text) => (await pglite.query(text)).rows,
      close: () => pglite.close(),
    }
  }
  const url = `postgres://postgres:postgres@127.0.0.1:${process.env.PG_PORT}/${process.env.PG_DATABASE ?? 'postgres'}`
  const admin = new pg.Client({ connectionString: url })
  await admin.connect()
  await admin.query(seed(random))
  const create = (pool: pg.Pool) =>
    ORM === 'prisma' ? createPrisma(pool) : ORM === 'kysely' ? createKysely(pool, 'pg') : createDrizzle(pool, 'pg')
  const transport = SERVERS === 2 ? new UnreliableTransport() : undefined
  const pool = withLiveQueries(new pg.Pool({ connectionString: url, max: 8 }), { transport })
  const writerPool =
    SERVERS === 2 ? withLiveQueries(new pg.Pool({ connectionString: url, max: 8 }), { transport }) : pool
  const driver = create(pool)
  const writer = SERVERS === 2 ? create(writerPool) : driver
  return {
    driver,
    writer,
    query: async (text) => (await admin.query(text)).rows,
    close: async () => {
      await driver.close()
      await pool.end()
      if (writerPool !== pool) await writerPool.end()
      await admin.end()
    },
  }
}

// ── Telefunction calls ──

type Subscriber = { shape: string; args: ReadArgs; result: string; invalidated: boolean; stop: () => void }

async function subscribe(sub: Subscriber, late = false): Promise<void> {
  sub.stop()
  sub.invalidated = false
  const closing: (() => void)[] = []
  const context: Record<symbol, unknown> = {
    [REQUEST_CONTEXT]: { onClose: (cb: () => void) => closing.push(cb) },
    [LATE]: late,
  }
  // A telefunction is an async function: the query runs (and is awaited) inside its call.
  const result = await store.run(context, async () => await driver.reads[sub.shape]!(sub.args))
  sub.result = canonical(sub.shape, result)
  const reads = context[LIVE_READS] as LiveReads | undefined
  sub.stop = reads?.onChange(() => (sub.invalidated = true)) ?? (() => {})
  for (const cb of closing) cb()
}

// ── The run ──

const subscribers: Subscriber[] = []
for (const shape of Object.keys(driver.reads).filter((s) => !SHAPES || SHAPES.includes(s))) {
  for (let i = 0; i < 3; i++) {
    const user = 1 + Math.floor(random() * USERS)
    const low = Math.floor(random() * 4)
    const args: ReadArgs = {
      user,
      user2: 1 + ((user + 2) % USERS),
      team: 1 + Math.floor(random() * 3),
      low,
      high: low + 1,
    }
    subscribers.push({ shape, args, result: '', invalidated: false, stop: () => {} })
  }
}
for (const sub of subscribers) await subscribe(sub)

type Counts = { invalidations: number; changes: number; extra: number; missed: number }
const byShape = new Map<string, Counts>()
const byWrite = new Map<string, Counts>()
const count = (map: Map<string, Counts>, key: string) => {
  if (!map.has(key)) map.set(key, { invalidations: 0, changes: 0, extra: 0, missed: 0 })
  return map.get(key)!
}
const misses: string[] = []
const writeNames = Object.keys(driver.writes)

for (let step = 0; step < STEPS; step++) {
  const write = pick(writeNames)
  const args = await writeArgs()
  const racing = CHURN && random() < 0.3 ? pick(subscribers) : undefined
  try {
    if (racing) await Promise.all([subscribe(racing, true), writer.writes[write]!(args)])
    else await writer.writes[write]!(args)
  } catch (err) {
    // A write the data doesn't allow (a deleted row's foreign key): it changed nothing.
    if (VERBOSE) console.log(`write ${write} failed: ${String(err).slice(0, 120)}`)
  }
  // Lets the transport deliver what the write published.
  for (let i = 0; i < 3; i++) await new Promise((resolve) => setImmediate(resolve))
  for (const sub of subscribers) {
    const now = canonical(sub.shape, await driver.reads[sub.shape]!(sub.args))
    const changed = now !== sub.result
    for (const counts of [count(byShape, sub.shape), count(byWrite, write)]) {
      if (changed) counts.changes++
      if (sub.invalidated) counts.invalidations++
      if (sub.invalidated && !changed) counts.extra++
      if (changed && !sub.invalidated) counts.missed++
    }
    if (changed && !sub.invalidated) {
      misses.push(
        `step ${step} ${write}(${JSON.stringify(args)}) → ${sub.shape}(${JSON.stringify(sub.args)})\n  before ${sub.result}\n  after  ${now}`,
      )
    }
    if (changed || sub.invalidated) await subscribe(sub)
  }
}

for (const sub of subscribers) sub.stop()
await close()

const total = [...byShape.values()].reduce(
  (a, c) => ({
    invalidations: a.invalidations + c.invalidations,
    changes: a.changes + c.changes,
    extra: a.extra + c.extra,
    missed: a.missed + c.missed,
  }),
  { invalidations: 0, changes: 0, extra: 0, missed: 0 },
)
console.log(
  `# oracle ORM=${ORM} DB=${DB}${process.env.PG_PORT ? ` (port ${process.env.PG_PORT})` : ''} SEED=${SEED} STEPS=${STEPS} CHURN=${CHURN ? 1 : 0} SERVERS=${SERVERS} subscribers=${subscribers.length}`,
)
console.log(
  `total: missed=${total.missed} changes=${total.changes} invalidations=${total.invalidations} extra=${total.extra} (extra per write ${(total.extra / STEPS).toFixed(3)})`,
)
console.log('per shape (extra per write):')
for (const [shape, c] of byShape)
  console.log(
    `  ${shape.padEnd(16)} missed=${c.missed} changes=${c.changes} invalidations=${c.invalidations} extra=${c.extra} (${(c.extra / STEPS).toFixed(3)}/write)`,
  )
if (VERBOSE)
  for (const [write, c] of byWrite)
    console.log(`  write ${write.padEnd(18)} missed=${c.missed} changes=${c.changes} extra=${c.extra}`)
for (const miss of misses.slice(0, 10)) console.log(`MISSED ${miss}`)
process.exit(total.missed > 0 ? 1 : 0)

// ── Helpers ──

async function writeArgs(): Promise<WriteArgs> {
  const ids = async (table: string) => (await query(`select id from ${table} order by id`)).map((r) => Number(r.id))
  // One admin connection: one query at a time.
  const todos = await ids('todos')
  const tags = await ids('tags')
  const users = await ids('users')
  const value = Math.floor(random() * 1000)
  return {
    todo: todos.length && random() < 0.95 ? pick(todos) : 9999,
    tag: tags.length ? pick(tags) : 9999,
    user: users.length ? pick(users) : 1,
    otherUser: users.length ? pick(users) : 1,
    value,
    // Mixed case: Postgres sorts it by the database's collation, unlike JavaScript.
    text: `${'abcAB'[value % 5]}${value}`,
    date: new Date(Date.UTC(2026, 10, 10, 8, 0, value)),
  }
}

/** The result as text; for a top-k with ties, which tied rows come back isn't defined, so only their values count. */
function canonical(shape: string, result: unknown): string {
  const text = (value: unknown) => JSON.stringify(value, (_key, v) => (typeof v === 'bigint' ? String(v) : v))
  if (shape !== 'priorityTies' || !Array.isArray(result)) return text(result)
  const rows = result as { id: number; priority: number }[]
  const last = rows.at(-1)?.priority
  return text({
    priorities: rows.map((r) => r.priority),
    above: rows.filter((r) => r.priority !== last).map((r) => r.id),
  })
}

function mulberry32(seed: number): () => number {
  return () => {
    seed |= 0
    seed = (seed + 0x6d2b79f5) | 0
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
