export { extractQueryShape, crossCheckRenderedTables }

import { Column, Placeholder, SQL, is, isTable } from 'drizzle-orm'
import { isPartialSelect, selectConfigOf, usedTablesOf } from '../binding/drizzleShape.js'
import type {
  Bound,
  ColRef,
  CoarseShape,
  Dialect,
  Distinct,
  JoinShape,
  OrderKey,
  Predicate,
  ProjItem,
  Projection,
  QueryShape,
  SetOp,
  SetOpKind,
} from '../ir/types.js'
import { colRefOf, collectTables, realTableNameOf, tableOf, tableRefOf } from './columns.js'
import { extractPredicate } from './predicate.js'
import { type SqlToken, readAggCall, tokenize } from './sqlChunks.js'

/** Read a drizzle select builder into a QueryShape. Every mechanic the compiler
 *  understands (from/joins/where/projection/group/having/distinct/set-ops/order/
 *  limit) is captured precisely; a shape that can't be read — an unreadable config,
 *  a subquery/CTE in FROM or a join — degrades to a CoarseShape whose recovered
 *  table set still invalidates soundly. */
function extractQueryShape(builder: unknown, opts: { dialect: Dialect }): QueryShape {
  const config = selectConfigOf(builder)
  if (!config) return coarse(usedTablesOf(builder), 'select config could not be read')
  if (!isTable(config.table))
    return coarse(recoverTables(config.table, usedTablesOf(builder)), 'non-table FROM (subquery/CTE/raw SQL)')

  const dialect = opts.dialect
  const tables = new Set<string>()
  const from = tableRefOf(config.table)
  tables.add(from.name)

  const joins: JoinShape[] = []
  for (const join of config.joins ?? []) {
    if (!isTable(join.table))
      return coarse(recoverTables(join.table, [...tables, ...usedTablesOf(builder)]), 'non-table join')
    const table = tableRefOf(join.table)
    tables.add(table.name)
    joins.push({ type: joinType(join.joinType), table, on: predicateInto(join.on, dialect, tables) })
  }

  const where = predicateInto(config.where, dialect, tables)
  const having = predicateInto(config.having, dialect, tables)
  const { projection, window } = extractProjection(config.fields, isPartialSelect(builder), tables)

  const groupBy: ColRef[] = []
  let groupByOpaque = false
  for (const expr of config.groupBy ?? []) {
    if (is(expr, Column)) {
      tables.add(realTableNameOf(tableOf(expr)))
      groupBy.push(colRefOf(expr))
    } else {
      groupByOpaque = true
    }
  }

  const setOps: SetOp[] = []
  for (const op of config.setOperators ?? []) {
    const right = extractQueryShape(op.rightSelect, { dialect })
    for (const name of right.tables) tables.add(name)
    setOps.push({
      type: setOpKind(op.type, op.isAll),
      right,
      orderBy: (op.orderBy ?? []).map((entry) => orderKeyOf(entry, tables)),
      limit: boundOf(op.limit),
      offset: boundOf(op.offset),
    })
  }

  const shape: QueryShape = {
    kind: 'select',
    dialect,
    from,
    joins,
    where,
    projection,
    groupBy,
    groupByOpaque,
    having,
    distinct: distinctOf(config.distinct, tables),
    orderBy: (config.orderBy ?? []).map((entry) => orderKeyOf(entry, tables)),
    limit: boundOf(config.limit),
    offset: boundOf(config.offset),
    setOps,
    window,
    tables: [...tables],
  }
  return crossCheckRenderedTables(shape, usedTablesOf(builder))
}

/** Soundness cross-check: every base relation the rendered SQL references must appear
 *  in the extracted table set. The direction matters — extraction may over-cover
 *  (extra tables only over-invalidate), but omitting a rendered relation would miss
 *  invalidations, so any omission degrades to a CoarseShape over the union of both
 *  sets rather than shipping an unsound precise shape. */
function crossCheckRenderedTables(shape: QueryShape, rendered: string[]): QueryShape {
  if (shape.kind !== 'select') return shape
  const extracted = new Set(shape.tables)
  const missing = rendered.filter((name) => !extracted.has(name))
  if (missing.length === 0) return shape
  return coarse([...shape.tables, ...rendered], `extraction omitted rendered relation(s): ${missing.join(', ')}`)
}

// ── Coarse fallback ─────────────────────────────────────────────────

function coarse(tables: string[], reason: string): CoarseShape {
  return { kind: 'coarse', tables: [...new Set(tables)], reason }
}

function recoverTables(value: unknown, extra: string[]): string[] {
  const set = new Set(extra)
  collectTables(value, set)
  return [...set]
}

// ── Clauses ─────────────────────────────────────────────────────────

function predicateInto(condition: SQL | undefined, dialect: Dialect, tables: Set<string>): Predicate | undefined {
  if (!condition) return undefined
  const result = extractPredicate(condition, { dialect })
  for (const name of result.tables) tables.add(name)
  return result.predicate
}

const JOIN_TYPES: Record<string, JoinShape['type']> = {
  inner: 'inner',
  left: 'left',
  right: 'right',
  full: 'full',
  cross: 'cross',
}

function joinType(value: string): JoinShape['type'] {
  return JOIN_TYPES[value] ?? 'inner'
}

const SET_OPS = new Set<SetOpKind>(['union', 'unionAll', 'intersect', 'intersectAll', 'except', 'exceptAll'])

// drizzle stores the base kind in `type` and the ALL variant in a separate `isAll`
// flag (`unionAll` = type:'union', isAll:true), so recombine them.
function setOpKind(type: string, isAll: boolean): SetOpKind {
  const base = type.replace(/all$/i, '')
  const combined = isAll ? `${base}All` : base
  return SET_OPS.has(combined as SetOpKind) ? (combined as SetOpKind) : (type as SetOpKind)
}

function distinctOf(value: SQL | boolean | { on: unknown[] } | undefined, tables: Set<string>): Distinct {
  if (value === true) return { on: true }
  if (value && typeof value === 'object' && Array.isArray((value as { on?: unknown[] }).on)) {
    const columns = (value as { on: unknown[] }).on
      .filter((c): c is Column => is(c, Column))
      .map((c) => {
        tables.add(realTableNameOf(tableOf(c)))
        return colRefOf(c)
      })
    return { on: 'columns', columns }
  }
  return { on: false }
}

function orderKeyOf(entry: unknown, tables: Set<string>): OrderKey {
  if (is(entry, Column)) {
    tables.add(realTableNameOf(tableOf(entry)))
    return { expr: { kind: 'col', ref: colRefOf(entry) }, direction: 'asc' }
  }
  if (is(entry, SQL)) {
    const local = new Set<string>()
    const columns = collectColumnRefs(entry, local)
    for (const name of local) tables.add(name)
    const text = joinedText(entry).toLowerCase()
    const direction = /\bdesc\b/.test(text) ? 'desc' : 'asc'
    const nulls = /nulls\s+first/.test(text) ? 'first' : /nulls\s+last/.test(text) ? 'last' : undefined
    const expr: OrderKey['expr'] =
      columns.length === 1 ? { kind: 'col', ref: columns[0]! } : { kind: 'opaque', columns }
    return { expr, direction, nulls }
  }
  return { expr: { kind: 'opaque', columns: [] }, direction: 'asc' }
}

function boundOf(value: unknown): Bound | undefined {
  if (value === undefined || value === null) return undefined
  if (typeof value === 'number') return { kind: 'value', value }
  if (is(value, Placeholder)) return { kind: 'placeholder', name: value.name }
  return { kind: 'placeholder', name: '(dynamic)' } // an expression limit is dynamic → dirty at compile
}

// ── Projection ──────────────────────────────────────────────────────

function extractProjection(
  fields: Record<string, unknown>,
  partial: boolean,
  tables: Set<string>,
): { projection: Projection; window: boolean } {
  const items: ProjItem[] = []
  let window = false

  const visit = (as: string | undefined, value: unknown): void => {
    if (is(value, Column)) {
      tables.add(realTableNameOf(tableOf(value)))
      items.push({ kind: 'col', ref: colRefOf(value), as })
      return
    }
    if (is(value, SQL.Aliased)) {
      visit((value as { fieldAlias?: string }).fieldAlias ?? as, value.sql)
      return
    }
    if (is(value, SQL)) {
      const agg = readAggCall(value)
      if (agg) {
        if (agg.arg) tables.add(realTableNameOf(tableOf(agg.arg)))
        items.push({
          kind: 'agg',
          call: { fn: agg.fn, arg: agg.arg ? colRefOf(agg.arg) : undefined, star: agg.star, distinct: agg.distinct },
          as,
        })
        return
      }
      const local = new Set<string>()
      const columns = collectColumnRefs(value, local)
      for (const name of local) tables.add(name)
      const isWindow = containsWindow(value)
      window = window || isWindow
      items.push({ kind: 'opaque', columns, tables: [...local], window: isWindow, as })
      return
    }
    if (value && typeof value === 'object') {
      // a table group ({ colKey: Column, ... }) from a joined whole-row select
      for (const [key, nested] of Object.entries(value)) visit(key, nested)
      return
    }
    items.push({ kind: 'opaque', columns: [], tables: [], window: false, as })
  }

  for (const [key, value] of Object.entries(fields)) visit(key, value)
  return { projection: { items, star: !partial }, window }
}

// ── SQL walking ─────────────────────────────────────────────────────

function collectColumnRefs(sql: SQL, tables: Set<string>): ColRef[] {
  const refs: ColRef[] = []
  const seen = new Set<string>()
  walkTokens(tokenize(sql.queryChunks), {
    onColumn: (column) => {
      tables.add(realTableNameOf(tableOf(column)))
      const ref = colRefOf(column)
      const key = `${ref.table}.${ref.column}`
      if (!seen.has(key)) {
        seen.add(key)
        refs.push(ref)
      }
    },
    onOpaque: (value) => collectTables(value, tables),
  })
  return refs
}

function walkTokens(
  tokens: SqlToken[],
  visit: { onColumn: (c: Column) => void; onOpaque: (v: unknown) => void },
): void {
  for (const token of tokens) {
    if (token.kind === 'column') visit.onColumn(token.column)
    else if (token.kind === 'opaque') visit.onOpaque(token.value)
    else if (token.kind === 'sql') walkTokens(token.tokens, visit)
    else if (token.kind === 'list') walkTokens(token.items, visit)
  }
}

function joinedText(sql: SQL): string {
  let text = ''
  const walk = (tokens: SqlToken[]) => {
    for (const token of tokens) {
      if (token.kind === 'text') text += `${token.text} `
      else if (token.kind === 'sql') walk(token.tokens)
    }
  }
  walk(tokenize(sql.queryChunks))
  return text
}

function containsWindow(sql: SQL): boolean {
  return /\bover\s*\(/i.test(joinedText(sql))
}
