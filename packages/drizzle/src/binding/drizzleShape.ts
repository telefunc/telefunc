export { selectConfigOf, usedTablesOf, isPartialSelect }
export type { DrizzleSelect, SelectConfig, JoinConfig, SetOperatorConfig }

import type { SQL } from 'drizzle-orm'

// The one place that reads drizzle's internal select-builder shape. It is pinned to
// the v1 rc line and version-brittle by contract, so every read is guarded: an
// unrecognized shape yields `null` and the caller falls back to a CoarseShape
// (table-level invalidation) rather than guessing. Field names verified live
// against drizzle-orm 1.0.0-rc.4. Public entry points take `unknown` because the real
// builder's `config`/`usedTables` are `protected` — we read them through a cast.

/** The structural surface we read off a drizzle select query builder. */
type DrizzleSelect = {
  config?: unknown
  _?: { config?: unknown }
  usedTables?: unknown
  isPartialSelect?: boolean
  toSQL?: () => { sql: string; params: unknown[] }
}

type SelectConfig = {
  table: unknown // PgTable | Subquery | View | SQL
  fields: Record<string, unknown>
  distinct?: boolean | { on: unknown[] }
  joins?: JoinConfig[]
  where?: SQL
  groupBy?: unknown[]
  having?: SQL
  orderBy?: unknown[]
  limit?: unknown // number | Placeholder
  offset?: unknown
  setOperators?: SetOperatorConfig[]
  withList?: unknown[]
}

type JoinConfig = { on: SQL | undefined; table: unknown; joinType: string; alias?: string; lateral?: boolean }

type SetOperatorConfig = {
  type: string
  isAll: boolean
  rightSelect: DrizzleSelect
  orderBy?: unknown[]
  limit?: unknown
  offset?: unknown
}

/** Read a select builder's config, or null when its shape isn't the pinned one.
 *  `builder.config` is the primary field; set-operation arms surface it under `_`. */
function selectConfigOf(builder: unknown): SelectConfig | null {
  const b = builder as DrizzleSelect
  const config = b.config ?? b._?.config
  if (!isRecord(config)) return null
  if (!isRecord(config.fields) || !('table' in config)) return null
  if ('joins' in config && config.joins !== undefined && !Array.isArray(config.joins)) return null
  if ('setOperators' in config && config.setOperators !== undefined && !Array.isArray(config.setOperators)) return null
  return config as SelectConfig
}

/** Base relations (FROM + JOINs) the rendered SQL references, as bare names —
 *  drizzle records them `schema.name` in a Set. Used both for coarse-shape recovery
 *  and as the authoritative rendered-table set the extraction is cross-checked
 *  against. Note: subquery relations are NOT included here (drizzle omits them);
 *  extraction captures those separately, so it always covers a superset. */
function usedTablesOf(builder: unknown): string[] {
  const used = (builder as DrizzleSelect).usedTables
  const names = used == null ? [] : isIterable(used) ? [...used] : []
  return names
    .filter((name): name is string => typeof name === 'string')
    .map((name) => name.slice(name.indexOf('.') + 1))
}

function isIterable(value: unknown): value is Iterable<unknown> {
  return typeof (value as { [Symbol.iterator]?: unknown })?.[Symbol.iterator] === 'function'
}

/** Whether the builder projects an explicit field map (`select({...})`) rather than
 *  a whole-row `select()`. */
function isPartialSelect(builder: unknown): boolean {
  return (builder as DrizzleSelect).isPartialSelect === true
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}
