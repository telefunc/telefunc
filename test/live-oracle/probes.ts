// Mutation probes: each breaks one rule of @telefunc/live, and the oracle must then report misses.
//
//   PROBE=old-image ORM=drizzle DB=pg PG_PORT=<port> CHURN=1 node --import ./probes.ts oracle.ts
//
// Probes patch the package's built classes; their methods are private to TypeScript only.

import { AsyncResource } from 'node:async_hooks'
import '../../packages/live/dist/live.js'
import { LiveDatabase } from '../../packages/live/dist/liveDatabase.js'
import { LiveRead } from '../../packages/live/dist/match/liveRead.js'
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
    delete (globalThis as any)[Symbol.for('@telefunc/live/tracking')]
  },
  /** A cascade only reaches the groups the counts have. */
  'tally-cascade'() {
    const dropAll = tally.dropAll
    tally.dropAll = function (source: unknown, row: unknown) {
      return dropAll.call(this, source, row).filter((key: string) => this.groups.get(key).counts.size > 0)
    }
  },
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
