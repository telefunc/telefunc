// The injected HydrationExecutor (R1, T5.A0), built HERE in the binding layer so `graph/`
// stays drizzle-free. It re-emits each input's σ residual as a real WHERE from the opaque
// `src` handles the extractor retained on the predicate leaves, scans the σ-scoped, column-
// pruned baseline, and re-fetches specific PKs during the drain. A residual the codec cannot
// safely reconstruct (an OR / NOT / a leaf missing its `src`) degrades the SCAN to a superset
// (no WHERE) — never unsound, because warming σ-filters every scanned row in memory before it
// enters state; only the read widens, never the state.

export { hydrationExecutorOf }

import { type SQL, and, sql } from 'drizzle-orm'
import type { SeedDescriptor } from '../compile/compile.js'
import type { HydrationExecutor } from '../graph/hydrate.js'
import type { Predicate, SqlSource } from '../ir/types.js'
import { executeSql } from './database.js'

type Row = Record<string, unknown>

function hydrationExecutorOf(db: unknown): HydrationExecutor {
  return {
    scan: (descriptor) => executeSql(db as never, scanQuery(descriptor)),
    fetchByKeys: (descriptor, keys) =>
      keys.length === 0 ? Promise.resolve([]) : executeSql(db as never, scanQuery(descriptor, keys)),
  }
}

function scanQuery(descriptor: SeedDescriptor, keys?: Row[]): SQL {
  const alias = sql.identifier(descriptor.alias)
  const from = sql`${sql.identifier(descriptor.table)} ${alias}`
  const projection =
    descriptor.columns === '*'
      ? sql`${alias}.*`
      : sql.join(
          descriptor.columns.map((column) => sql`${alias}.${sql.identifier(column)}`),
          sql`, `,
        )
  const conditions = residualConditions(descriptor.residual)
  if (keys) conditions.push(keyCondition(descriptor, keys))
  const where = conditions.length > 0 ? sql` where ${and(...conditions)}` : sql``
  return sql`select ${projection} from ${from}${where}`
}

/** The σ residual as WHERE fragments, or `[]` when it cannot be reconstructed precisely (the
 *  in-memory σ-filter then refines a superset scan). */
function residualConditions(pred: Predicate): SQL[] {
  const leaves = pred.kind === 'true' ? [] : pred.kind === 'and' ? pred.parts : [pred]
  const conditions: SQL[] = []
  for (const leaf of leaves) {
    const src = leafSrc(leaf)
    if (src === undefined) return [] // unreconstructable residual → scan a superset, filter in memory
    conditions.push(src)
  }
  return conditions
}

function leafSrc(pred: Predicate): SQL | undefined {
  switch (pred.kind) {
    case 'compare':
    case 'in':
    case 'like':
    case 'isNull':
    case 'between':
    case 'exists':
    case 'unknown':
      return pred.src as SqlSource as SQL | undefined
    default:
      return undefined
  }
}

/** `pk IN (…)` for single or composite primary keys (row-value IN for composites). */
function keyCondition(descriptor: SeedDescriptor, keys: Row[]): SQL {
  const alias = sql.identifier(descriptor.alias)
  const pkCols = descriptor.primaryKey
  if (pkCols.length === 1) {
    const column = sql`${alias}.${sql.identifier(pkCols[0]!)}`
    const values = keys.map((key) => sql`${key[pkCols[0]!]}`)
    return sql`${column} in (${sql.join(values, sql`, `)})`
  }
  const tuple = sql`(${sql.join(
    pkCols.map((column) => sql`${alias}.${sql.identifier(column)}`),
    sql`, `,
  )})`
  const rows = keys.map(
    (key) =>
      sql`(${sql.join(
        pkCols.map((column) => sql`${key[column]}`),
        sql`, `,
      )})`,
  )
  return sql`${tuple} in (${sql.join(rows, sql`, `)})`
}
