// The injected HydrationExecutor, built HERE in the binding layer so `graph/` stays
// drizzle-free. It re-emits each input's σ residual as a real WHERE from the opaque `src` handles
// the extractor retained on the predicate leaves, and scans the σ-scoped, column-pruned baseline. A
// residual the codec cannot safely reconstruct (an OR / NOT / a leaf missing its `src`) degrades the
// SCAN to a superset (no WHERE) — never unsound, because the graph σ-filters every scanned row in
// memory before it enters state; only the read widens, never the state.

export { hydrationExecutorOf }

import { type SQL, and, sql } from 'drizzle-orm'
import type { SeedDescriptor } from '../compile/compile.js'
import type { HydrationExecutor } from '../graph/hydrate.js'
import type { Predicate, SqlSource } from '../ir/types.js'
import { executeSql } from './database.js'

function hydrationExecutorOf(db: unknown): HydrationExecutor {
  return {
    scan: (descriptor) => executeSql(db as never, scanQuery(descriptor)),
  }
}

function scanQuery(descriptor: SeedDescriptor): SQL {
  const alias = sql.identifier(descriptor.alias)
  const relation = descriptor.schema
    ? sql`${sql.identifier(descriptor.schema)}.${sql.identifier(descriptor.table)}`
    : sql.identifier(descriptor.table)
  const from = sql`${relation} ${alias}`
  const projection =
    descriptor.columns === '*'
      ? sql`${alias}.*`
      : sql.join(
          descriptor.columns.map((column) => sql`${alias}.${sql.identifier(column)}`),
          sql`, `,
        )
  const conditions = residualConditions(descriptor.residual)
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
