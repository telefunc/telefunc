export {
  tableRefOf,
  colRefOf,
  tableOf,
  realTableNameOf,
  primaryKeyOf,
  tableFingerprint,
  schemaFingerprint,
  collectTables,
  demandedColumns,
}

import { Column, type SQL, Subquery, type Table, getTableColumns, getTableName, is, isTable } from 'drizzle-orm'
import { entityKindOf } from '../binding/database.js'
import type { ColRef, ProjItem, Predicate, QueryShape, ScalarExpr, TableRef } from '../ir/types.js'

// drizzle's per-table metadata lives under globally-registered symbols; the public
// types don't surface them, so read them directly.
const IsAlias = Symbol.for('drizzle:IsAlias')
const OriginalName = Symbol.for('drizzle:OriginalName')
const Schema = Symbol.for('drizzle:Schema')
const ExtraConfigBuilder = Symbol.for('drizzle:ExtraConfigBuilder')
const ExtraConfigColumns = Symbol.for('drizzle:ExtraConfigColumns')
const EnableRLS = Symbol.for('drizzle:EnableRLS')

/** The table a column belongs to (not exposed on the public `Column` type). */
function tableOf(column: Column): Table {
  return (column as unknown as { table: Table }).table
}

/** Real table name; an aliased table (self-join) resolves to the name write events
 *  arrive under, not the alias. */
function realTableNameOf(table: Table): string {
  const symbols = table as unknown as Record<symbol, unknown>
  return symbols[IsAlias] ? (symbols[OriginalName] as string) : getTableName(table)
}

function tableRefOf(table: Table): TableRef {
  const symbols = table as unknown as Record<symbol, unknown>
  return {
    name: realTableNameOf(table),
    alias: getTableName(table),
    schema: symbols[Schema] as string | undefined,
    primaryKey: primaryKeyOf(table),
  }
}

function colRefOf(column: Column): ColRef {
  return { table: getTableName(tableOf(column)), column: column.name }
}

/** Primary-key column database names. Single-column keys are flagged on the column;
 *  composite keys live in the table's extra-config builder (`primaryKey({ columns })`). */
function primaryKeyOf(table: Table): string[] {
  const columns = getTableColumns(table)
  const single = Object.values(columns)
    .filter((column) => column.primary)
    .map((column) => column.name)
  if (single.length > 0) return single

  const build = (table as unknown as Record<symbol, unknown>)[ExtraConfigBuilder]
  if (typeof build !== 'function') return []
  const extraColumns = (table as unknown as Record<symbol, unknown>)[ExtraConfigColumns] ?? columns
  let built: unknown
  try {
    built = build(extraColumns)
  } catch {
    return []
  }
  const entries = Array.isArray(built) ? built : [built]
  for (const entry of entries) {
    const columnsOf = (entry as { columns?: Column[] } | null)?.columns
    if (/PrimaryKey/.test(entityKindOf(entry) ?? '') && Array.isArray(columnsOf)) {
      return columnsOf.map((column) => column.name)
    }
  }
  return []
}

/** Structural fingerprint of a relation, keyed by its real name so aliases
 *  fingerprint identically. Feeds planKey so a schema change to a referenced relation
 *  bumps the cached plan (a referenced-relation epoch). Canonical inputs, stably
 *  ordered: relation identity, then per-column name/type/nullability (column-sorted),
 *  the PK shape, and the schema-declared RLS bit. */
function tableFingerprint(table: Table): string {
  const ref = tableRefOf(table)
  const cols = Object.values(getTableColumns(table))
    .map((column) => `${column.name}:${column.columnType}:${(column as { notNull?: boolean }).notNull ? 1 : 0}`)
    .sort()
  const rls = (table as unknown as Record<symbol, unknown>)[EnableRLS] ? 1 : 0
  return `rel=${ref.schema ?? ''}.${ref.name}|cols=${cols.join(',')}|pk=${[...ref.primaryKey].sort().join(',')}|rls=${rls}`
}

/** Combined fingerprint of every distinct relation a query references. */
function schemaFingerprint(tables: Table[]): string {
  const byName = new Map<string, string>()
  for (const table of tables) {
    const name = realTableNameOf(table)
    if (!byName.has(name)) byName.set(name, tableFingerprint(table))
  }
  return [...byName.keys()]
    .sort()
    .map((name) => byName.get(name))
    .join('|')
}

/** Real table names reachable from any drizzle SQL-ish value (conditions,
 *  subqueries, embedded builders, columns, tables). A `seen` set terminates the
 *  self-reference that leaf SQLWrappers return from `getSQL()`. */
function collectTables(value: unknown, into: Set<string>, seen = new Set<object>()): void {
  if (typeof value !== 'object' || value === null || seen.has(value)) return
  seen.add(value)
  if (is(value, Column)) {
    into.add(realTableNameOf(tableOf(value)))
  } else if (isTable(value)) {
    into.add(realTableNameOf(value))
  } else if (is(value, Subquery)) {
    // drizzle records used tables as "schema.name"; write events arrive under the bare name
    const used = (value as unknown as { _?: { usedTables?: string[] } })._?.usedTables ?? []
    for (const name of used) into.add(name.slice(name.indexOf('.') + 1))
  } else if ('getSQL' in value && typeof (value as { getSQL: unknown }).getSQL === 'function') {
    const inner = (value as { getSQL: () => SQL }).getSQL()
    if (inner !== value) collectTables(inner, into, seen)
    else collectSqlChunks(value as SQL, into, seen)
  }
}

function collectSqlChunks(sql: SQL, into: Set<string>, seen: Set<object>): void {
  const chunks = (sql as unknown as { queryChunks?: unknown[] }).queryChunks
  if (Array.isArray(chunks)) for (const chunk of chunks) collectTables(chunk, into, seen)
}

// ── Pruned-column demand ────────────────────────────────────────────
// The database columns each relation must supply for change capture: everything a
// predicate, join key, projection, grouping or ordering reads. Keyed by real table
// name via the shape's alias→relation map. Coarse shapes demand nothing here
// (they invalidate at table granularity).

function demandedColumns(shape: QueryShape): Map<string, Set<string>> {
  const demand = new Map<string, Set<string>>()
  if (shape.kind !== 'select') return demand

  const aliasToName = new Map<string, string>()
  const register = (ref: TableRef) => {
    aliasToName.set(ref.alias, ref.name)
    for (const pk of ref.primaryKey) add(demand, ref.name, pk) // PK always needed for retraction
  }
  register(shape.from)
  for (const join of shape.joins) register(join.table)

  const want = (ref: ColRef) => add(demand, aliasToName.get(ref.table) ?? ref.table, ref.column)

  if (shape.where) forEachColRef(shape.where, want)
  for (const join of shape.joins) if (join.on) forEachColRef(join.on, want)
  if (shape.having) forEachColRef(shape.having, want)
  for (const item of shape.projection.items) forEachProjColRef(item, want)
  for (const ref of shape.groupBy) want(ref)
  for (const key of shape.orderBy)
    if (key.expr.kind === 'col') want(key.expr.ref)
    else for (const ref of key.expr.columns) want(ref)
  return demand
}

function add(demand: Map<string, Set<string>>, table: string, column: string): void {
  let set = demand.get(table)
  if (!set) demand.set(table, (set = new Set()))
  set.add(column)
}

function forEachProjColRef(item: ProjItem, visit: (ref: ColRef) => void): void {
  if (item.kind === 'col') visit(item.ref)
  else if (item.kind === 'agg') {
    if (item.call.arg) visit(item.call.arg)
  } else for (const ref of item.columns) visit(ref)
}

function forEachColRef(pred: Predicate, visit: (ref: ColRef) => void): void {
  const scalar = (expr: ScalarExpr) => {
    if (expr.kind === 'col') visit(expr.ref)
  }
  switch (pred.kind) {
    case 'and':
    case 'or':
      for (const part of pred.parts) forEachColRef(part, visit)
      return
    case 'not':
      forEachColRef(pred.operand, visit)
      return
    case 'compare':
      scalar(pred.left)
      scalar(pred.right)
      return
    case 'in':
      scalar(pred.expr)
      for (const value of pred.values) scalar(value)
      return
    case 'like':
    case 'isNull':
      scalar(pred.expr)
      return
    case 'between':
      scalar(pred.expr)
      scalar(pred.low)
      scalar(pred.high)
      return
    default:
      return
  }
}
