// Mutation probes: each breaks one rule of @telefunc/live, and the oracle must then report misses.
//
//   PROBE=old-image ORM=drizzle DB=pg PG_PORT=<port> CHURN=1 node --import ./probes.ts oracle.ts
//
// Probes patch the package's built classes; their methods are private to TypeScript only.

import { LiveDatabase } from '../../packages/live/dist/liveDatabase.js'
import { LiveRead } from '../../packages/live/dist/match/liveRead.js'

const live = LiveRead.prototype as any
const database = LiveDatabase.prototype as any

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
  /** A join seed that didn't come back (an empty result, too many values) is taken as empty. */
  seed() {
    const resultOf = live.resultOf
    live.resultOf = function (rows: unknown) {
      const result = resultOf.call(this, rows)
      for (const [hidden, keys] of result.seeds) if (!keys) result.seeds.set(hidden, new Set())
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
}

const probe = process.env.PROBE
if (probe) {
  if (!probes[probe]) throw new Error(`Unknown PROBE=${probe}; one of ${Object.keys(probes).join(', ')}`)
  probes[probe]()
}
