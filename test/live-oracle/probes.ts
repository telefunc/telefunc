// Mutation probes: each breaks one rule of @telefunc/live, and the oracle must then report misses.
//
//   PROBE=old-image ORM=drizzle DB=pg PG_PORT=<port> CHURN=1 node --import ./probes.ts oracle.ts
//
// Probes patch the package's built classes; their methods are private to TypeScript only.

import { AsyncResource } from 'node:async_hooks'
import { LiveDatabase } from '../../packages/live/dist/liveDatabase.js'
import { LiveRead } from '../../packages/live/dist/match/liveRead.js'
import { Tally } from '../../packages/live/dist/match/tally.js'

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
  /** A cascade only reaches the groups the counts have. */
  'tally-cascade'() {
    const dropAll = tally.dropAll
    tally.dropAll = function (source: unknown, row: unknown) {
      return dropAll.call(this, source, row).filter((key: string) => this.groups.get(key).counts.size > 0)
    }
  },
}

const probe = process.env.PROBE
if (probe) {
  if (!probes[probe]) throw new Error(`Unknown PROBE=${probe}; one of ${Object.keys(probes).join(', ')}`)
  probes[probe]()
}
