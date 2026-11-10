// Mutation probes: each breaks one rule of @telefunc/live, and the oracle must then report misses.
//
//   PROBE=old-image ORM=drizzle DB=pg PG_PORT=<port> CHURN=1 node --import ./probes.ts oracle.ts
//
// Probes patch the package's built classes; their methods are private to TypeScript only.

import { AsyncResource } from 'node:async_hooks'
import { PGlite } from '@electric-sql/pglite'
import '../../packages/live/dist/live.js'
import { LiveDatabase } from '../../packages/live/dist/liveDatabase.js'
import { UNNAMED_TABLES } from '../../packages/live/dist/capture/transactions.js'
import { EVENT_TAG } from './orms/eventQuery.ts'
import { LiveRead } from '../../packages/live/dist/match/liveRead.js'
import { Operands } from '../../packages/live/dist/match/operands.js'
import { Tally } from '../../packages/live/dist/match/tally.js'
import { Refetcher } from '../../packages/live/dist/refetcher.js'
import { WatchedRead } from '../../packages/live/dist/sharedReads.js'
import { staleSignals } from '../../packages/live/dist/staleSignals.js'

const live = LiveRead.prototype as any
const database = LiveDatabase.prototype as any
const tally = Tally.prototype as any

const probes: Record<string, () => void> = {
  /** A write's old image is ignored: an update counts as an insert of its new row, a delete as nothing. */
  'old-image'() {
    const affectsThrough = live.affectsThrough
    live.affectsThrough = function (alias: unknown, change: any) {
      if (change.kind === 'delete') return false
      if (change.kind === 'update') change = { table: change.table, kind: 'insert', new: change.new }
      return affectsThrough.call(this, alias, change)
    }
  },
  /** What an update's `SET` wrote is ignored: its new row is its old one. */
  'set-columns'() {
    const affectsThrough = live.affectsThrough
    live.affectsThrough = function (alias: unknown, change: any) {
      return affectsThrough.call(this, alias, change.kind === 'update' ? { ...change, new: change.old } : change)
    }
  },
  /** A row past the last row of a full page is taken as unable to enter it. */
  boundary() {
    live.sortsBefore = () => false
  },
  /** Changes arriving while the read runs are dropped instead of judged against its result. */
  fence() {
    const consider = live.consider
    live.consider = function (change: unknown) {
      if (this.pending) return
      consider.call(this, change)
    }
  },
  /** Text is compared in JavaScript under any collation. */
  collation() {
    const loadCatalog = database.loadCatalog
    database.loadCatalog = async function (query: unknown) {
      await loadCatalog.call(this, query)
      for (const table of this.catalog.tables.values()) for (const column of table.columns) column.orderableText = true
    }
  },
  /** A write, and what it fires, is judged by the volatility of the functions it calls: a `STABLE` function counts
   *  as computing a value. */
  'write-volatility'() {
    catalogRow((row) => {
      row.functions = row.functions.map(([name, writing, volatile, defaults]: any[]) => [
        name,
        writing && volatile,
        volatile,
        defaults,
      ])
    })
  },
  /** A read is judged by the language of the functions it calls, as a write is: a read calling a `STABLE` SQL helper
   *  counts as a write, and its `.live()` fails. */
  'read-language'() {
    catalogRow((row) => {
      row.functions = row.functions.map(([name, writing, , defaults]: any[]) => [name, writing, true, defaults])
    })
  },
  /** A function's default arguments are taken to call nothing. */
  'default-calls'() {
    catalogRow((row) => {
      row.functions = row.functions.map(([name, writing, volatile]: any[]) => [name, writing, volatile, null])
    })
  },
  /** A typed operand, a cast or a number, is compared with a column whatever its type, as if Postgres compared both
   *  as the column's type. */
  'operand-type'() {
    const operands = Operands.prototype as any
    const of = operands.of
    operands.of = function (node: any, comparison: any) {
      const constant = node.A_Const
      const typed = node.TypeCast || constant?.ival || constant?.fval || constant?.boolval
      return of.call(this, node, typed ? { ...comparison, as: undefined } : comparison)
    }
  },
  /** A `char(n)` value is matched against a pattern without its padding. */
  'char-padding'() {
    const loadCatalog = database.loadCatalog
    database.loadCatalog = async function (query: unknown) {
      await loadCatalog.call(this, query)
      for (const table of this.catalog.tables.values()) for (const column of table.columns) delete column.charLength
    }
  },
  /** A foreign key is taken to reference its parent column's name in every table, not only in its parent. */
  'fk-target'() {
    const loadCatalog = database.loadCatalog
    database.loadCatalog = async function (query: unknown) {
      await loadCatalog.call(this, query)
      const tables = [...this.catalog.tables.values()]
      for (const table of tables)
        table.references = table.references.flatMap((key: any) =>
          tables.map((parent) => ({ ...key, parent: parent.id })),
        )
    }
  },
  /** Foreign key cascades change nothing in the child tables. */
  cascade() {
    const changesOfWrite = database.changesOfWrite
    database.changesOfWrite = function (plan: any, rows: unknown) {
      return changesOfWrite.call(this, plan, rows).filter((c: any) => c.table === plan.table.id)
    }
  },
  /** An empty result's join seeds are taken as empty: the row of seeds its follow-up query sent is ignored. */
  seed() {
    const resultOf = live.resultOf
    live.resultOf = function (rows: unknown[], _seedRow: unknown) {
      const result = resultOf.call(this, rows, undefined)
      if (rows.length === 0) for (const hidden of result.seeds.keys()) result.seeds.set(hidden, new Set())
      return result
    }
  },
  /** A row leaving an `EXISTS` or a `DISTINCT` is never the last one. */
  counts() {
    for (const method of ['existsChanged', 'distinctChanged']) {
      const original = live[method]
      live[method] = function (alias: unknown, _old: unknown, now: unknown, ...rest: unknown[]) {
        return original.call(this, alias, undefined, now, ...rest)
      }
    }
  },
  /** A call waiting for a pool's connection runs its statements in the call of whoever freed one. */
  'pool-context'() {
    AsyncResource.bind = ((callback: unknown) => callback) as typeof AsyncResource.bind
  },
  /** A group the read returned is forgotten when the counts, taken after the read, no longer have it. */
  'tally-view'() {
    const of = (Tally as any).of
    ;(Tally as any).of = function (...args: unknown[]) {
      const counted = of.apply(this, args)
      for (const [key, group] of counted?.groups ?? []) if (group.counts.size === 0) counted.groups.delete(key)
      return counted
    }
  },
  /** A stale signal arriving while a refetch runs is dropped: the last change of a burst is never fetched. */
  coalesce() {
    const stale = (Refetcher.prototype as any).stale
    ;(Refetcher.prototype as any).stale = function (this: { running: boolean }) {
      if (!this.running) stale.call(this)
    }
  },
  /** A stale signal held back while the server is busy is never sent. */
  pace() {
    const signals = staleSignals as any
    const tick = signals.tick
    signals.tick = function (this: { waiting: unknown[] }) {
      this.waiting.length = 0
      tick.call(this)
    }
  },
  /** A read that joins an identical one running gets its result but isn't told when it goes stale. */
  join() {
    const watch = (WatchedRead.prototype as any).watch
    ;(WatchedRead.prototype as any).watch = function (
      this: { watchers: Set<unknown> },
      query: { add(r: unknown): void },
    ) {
      if (this.watchers.size === 0) return watch.call(this, query)
      query.add(this)
    }
  },
  /** An operand's value is the text the app sent, its casts dropped, rather than the value Postgres gave it. */
  'operand-text'() {
    const resolveWhenAnswered = database.resolveWhenAnswered
    database.resolveWhenAnswered = function (watched: any, shape: any, ...rest: any[]) {
      const [rows, stateRows, operandRows, key] = rest
      const asSent = operandRows?.then(
        (answered: any) => answered && [sentTexts(shape, watched.read.params, answered[0])],
      )
      return resolveWhenAnswered.call(this, watched, shape, rows, stateRows, asSent, key)
    }
  },
  /** Each copy of the package tracks `derived()`'s dependencies on its own: values one copy reads aren't seen by
   *  another copy's `derived()`. */
  tracking() {
    delete (globalThis as any)[Symbol.for('@telefunc/live')].tracking
  },
  /** A `NULL` list (`= ANY(NULL)`) is taken as an empty one: `ANY` is `FALSE` and `ALL` `TRUE`, where SQL gives `NULL`. */
  'null-list'() {
    const resolveWhenAnswered = database.resolveWhenAnswered
    database.resolveWhenAnswered = function (watched: any, shape: any, ...rest: any[]) {
      const [rows, stateRows, operandRows, key] = rest
      const asEmpty = operandRows?.then(
        (answered: any) =>
          answered && [
            answered[0].map((value: unknown, i: number) => (shape.operands[i].list && value === null ? '[]' : value)),
          ],
      )
      return resolveWhenAnswered.call(this, watched, shape, rows, stateRows, asEmpty, key)
    }
  },
  /** Unqualified names keep resolving by the loading session's `search_path` after a session may have changed it. */
  'search-path'() {
    const sessionsStartDiffering = database.sessionsStartDiffering
    database.sessionsStartDiffering = function () {
      const { searchPath } = this.catalog
      sessionsStartDiffering.call(this)
      this.catalog.searchPath = searchPath
    }
  },
  /** Deferred triggers fire after the transaction's locks are read: the tables they write go unannounced. */
  immediate() {
    queriesBeforeCommit((queries) => queries.filter((query) => !query.text.startsWith('SET CONSTRAINTS')))
  },
  /** The transaction's locks are read as none: what a rule or a function of the app's wrote goes unannounced. */
  locks() {
    queriesBeforeCommit((queries) =>
      queries.map((query) => (query.text.includes('pg_locks') ? { ...query, done: () => query.done([]) } : query)),
    )
  },
  /** No write marks its transaction as changing tables the catalog can't name: its locks go unread, and what a rule or
   *  a function of the app's wrote goes unannounced. */
  'unnamed-flag'() {
    for (const method of ['changesOf', 'changesOfWrite']) {
      const original = database[method]
      database[method] = function (...args: unknown[]) {
        return original.apply(this, args).filter((change: unknown) => change !== UNNAMED_TABLES)
      }
    }
  },
  /** PGlite's own statements while it starts are captured like the app's: they wait for the catalog, which waits for
   *  PGlite to have started. The run hangs. */
  startup() {
    Object.defineProperty(PGlite.prototype, 'ready', { get: () => false })
  },
  /** The write of an event-based `pg.Query` isn't announced. */
  'event-query'() {
    const statementFor = database.statementFor
    database.statementFor = function (connection: unknown, text: string, ...rest: unknown[]) {
      const statement = statementFor.call(this, connection, text, ...rest)
      return text === EVENT_TAG ? { ...statement, succeeded: () => [] } : statement
    }
  },
  /** Whether PGlite started is read as false when it's wrapped: an already started database's first query is then taken
   *  for one of its own, and isn't captured. */
  'pglite-ready'() {
    const ready = Object.getOwnPropertyDescriptor(PGlite.prototype, 'ready')!.get!
    const askedOnce = new WeakSet<object>()
    Object.defineProperty(PGlite.prototype, 'ready', {
      get(this: PGlite) {
        if (askedOnce.has(this)) return ready.call(this)
        askedOnce.add(this)
        return false
      },
    })
  },
  /** A cascade only reaches the groups the counts have. */
  'tally-cascade'() {
    const dropAll = tally.dropAll
    tally.dropAll = function (source: unknown, row: unknown) {
      return dropAll.call(this, source, row).filter((key: string) => this.groups.get(key).counts.size > 0)
    }
  },
}

/** Changes the catalog as its query returns it (`CatalogRow`), before it's read. */
function catalogRow(change: (row: any) => void) {
  const loadCatalog = database.loadCatalog
  database.loadCatalog = async function (query: (text: string) => Promise<string>) {
    await loadCatalog.call(this, async (text: string) => {
      const answer = await query(text)
      if (!text.startsWith('select json_build_object(')) return answer
      const row = JSON.parse(answer)
      change(row)
      return JSON.stringify(row)
    })
  }
}

type QueryBeforeCommit = { text: string; done(rows: unknown[][] | undefined): void }

/** Changes the queries a statement sends before its transaction commits (`Statement.beforeCommit`). */
function queriesBeforeCommit(change: (queries: QueryBeforeCommit[]) => QueryBeforeCommit[]) {
  for (const method of ['commit', 'inOwnTransaction']) {
    const original = database[method]
    database[method] = function (...args: unknown[]) {
      const statement = original.apply(this, args)
      if (!statement.beforeCommit) return statement
      return {
        ...statement,
        beforeCommit: { ...statement.beforeCommit, queries: change(statement.beforeCommit.queries) },
      }
    }
  }
}

/** Each operand as the app sent it: a parameter's value or a constant's text, under any casts. */
function sentTexts(shape: any, params: unknown[], evaluated: unknown[]): unknown[] {
  return shape.operands.map((operand: any, i: number) => {
    const sent = /^\(*(\$(\d+)|'((?:[^']|'')*)')/.exec(operand.expression)
    if (operand.list || !sent) return evaluated[i]
    if (sent[3] !== undefined) return sent[3].replace(/''/g, "'")
    const value = params[Number(sent[2]) - 1]
    return value === null || value === undefined ? null : value instanceof Date ? value.toISOString() : String(value)
  })
}

const probe = process.env.PROBE
if (probe) {
  if (!probes[probe]) throw new Error(`Unknown PROBE=${probe}; one of ${Object.keys(probes).join(', ')}`)
  probes[probe]()
}
