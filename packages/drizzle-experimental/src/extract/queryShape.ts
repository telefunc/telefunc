export { extractQueryShape, crossCheckRenderedTables, renderedRelationsFromSQL }

import { Column, Placeholder, SQL, is, isTable } from 'drizzle-orm'
import { isPartialSelect, selectConfigOf } from '../binding/drizzleShape.js'
import type {
  Bound,
  ColRef,
  CoarseShape,
  Correlation,
  Dialect,
  Distinct,
  JoinShape,
  OrderKey,
  Predicate,
  ProjItem,
  Projection,
  QueryShape,
  SelectShape,
  SetOp,
  SetOpKind,
} from '../ir/types.js'
import { relationIdOf } from '../ir/relation.js'
import { colRefOf, collectTables, relationKeyOf, tableOf, tableRefOf } from './columns.js'
import { extractPredicate } from './predicate.js'
import { type SqlToken, readAggCall, tokenize } from './sqlChunks.js'
import { isSelectBuilder } from './predicate.js'

/** Read a drizzle select builder into a QueryShape. Every mechanic the compiler
 *  understands (from/joins/where/projection/group/having/distinct/set-ops/order/
 *  limit) is captured precisely; a shape that can't be read — an unreadable config,
 *  a subquery/CTE in FROM or a join — degrades to a CoarseShape whose relations are
 *  recovered from the rendered SQL, or to an untrackable rejection when none can be. */
function extractQueryShape(builder: unknown, opts: { dialect: Dialect }): QueryShape {
  const config = selectConfigOf(builder)
  if (!config) return coarse(renderedRelationsFromSQL(builder), 'select config could not be read')
  if (!isTable(config.table)) return coarse(renderedRelationsFromSQL(builder), 'non-table FROM (subquery/CTE/raw SQL)')

  const dialect = opts.dialect
  const tables = new Set<string>()
  const from = tableRefOf(config.table)
  tables.add(from.id)

  const joins: JoinShape[] = []
  for (const join of config.joins ?? []) {
    if (!isTable(join.table)) return coarse(renderedRelationsFromSQL(builder), 'non-table join')
    const table = tableRefOf(join.table)
    tables.add(table.id)
    joins.push({ type: joinType(join.joinType), table, on: predicateInto(join.on, dialect, tables) })
  }

  const where = predicateInto(config.where, dialect, tables)
  const having = predicateInto(config.having, dialect, tables)
  const { projection, window } = extractProjection(config.fields, isPartialSelect(builder), dialect, tables)

  const groupBy: ColRef[] = []
  let groupByOpaque = false
  for (const expr of config.groupBy ?? []) {
    if (is(expr, Column)) {
      tables.add(relationKeyOf(tableOf(expr)))
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

  const shape: SelectShape = {
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
  enrichSubqueries(shape, dialect)
  return crossCheckRenderedTables(shape, renderedRelationsFromSQL(builder))
}

/** Soundness cross-check: every relation the *rendered SQL* references must appear in
 *  the extracted table set. The rendered set is read from `builder.toSQL().sql`, not
 *  drizzle's internal `usedTables` — a builder whose config underreports its joins
 *  still renders them, and this catches that. Direction matters: extraction may
 *  over-cover (extra tables only over-invalidate), but omitting a rendered relation
 *  would miss invalidations, so any omission degrades to a CoarseShape. */
function crossCheckRenderedTables(shape: QueryShape, rendered: string[]): QueryShape {
  if (shape.kind !== 'select') return shape
  const extracted = new Set(shape.tables)
  const missing = rendered.filter((name) => !extracted.has(name))
  if (missing.length === 0) return shape
  return coarse([...shape.tables, ...rendered], `extraction omitted rendered relation(s): ${missing.join(', ')}`)
}

/** Relation IDENTITIES (see ir/relation.ts) named after FROM/JOIN in the rendered SQL, across every
 *  identifier quoting drizzle or a raw fragment can render: double quotes (what drizzle emits on pg and
 *  sqlite) and backticks (which SQLite also accepts as identifier quoting — verified against node:sqlite —
 *  so a raw fragment can carry them into an otherwise-extractable query). Requiring a quoted
 *  name avoids matching the word "from" inside a string literal; drizzle never emits bracket quoting,
 *  so brackets are (correctly) not recognized. A reference renders with exactly the qualification its
 *  declaration carries — `"analytics"."users"` for a pgSchema table, bare `"users"` otherwise — so the
 *  identity built from the rendered segments equals the one `tableRefOf` builds from the declaration.
 *  That equality is what lets the cross-check below compare the two sets. */
function renderedRelationsFromSQL(builder: unknown): string[] {
  const toSQL = (builder as { toSQL?: () => { sql: string } }).toSQL
  if (typeof toSQL !== 'function') return []
  let sqlText: string
  try {
    sqlText = toSQL.call(builder).sql
  } catch {
    return []
  }
  const quoted = '(?:"(?:[^"]|"")*"|`(?:[^`]|``)*`)'
  const relationRe = new RegExp(`\\b(?:from|join)\\s+(${quoted}(?:\\s*\\.\\s*${quoted})?)`, 'gi')
  const segmentRe = new RegExp(quoted, 'g')
  const ids = new Set<string>()
  for (let m = relationRe.exec(sqlText); m !== null; m = relationRe.exec(sqlText)) {
    const segments = m[1]!.match(segmentRe) ?? []
    const name = segments[segments.length - 1]
    if (!name) continue
    const schema = segments.length > 1 ? segments[segments.length - 2] : undefined
    ids.add(relationIdOf({ name: unquote(name), schema: schema && unquote(schema) }))
  }
  return [...ids]
}

function unquote(segment: string): string {
  const q = segment[0]!
  return segment
    .slice(1, -1)
    .split(q + q)
    .join(q)
}

// ── Coarse fallback ─────────────────────────────────────────────────

function coarse(tables: string[], reason: string): CoarseShape {
  const unique = [...new Set(tables)]
  // A coarse shape that recovered no relations has no routing inputs — flag it
  // untrackable (a typed rejection) and warn, so downstream rejects it rather than
  // silently treating it as an unscoped/global subscription.
  if (unique.length === 0) {
    console.warn(`[@telefunc/drizzle-experimental] untrackable read, no relations recovered: ${reason}`)
    return { kind: 'coarse', tables: [], reason, untrackable: true }
  }
  return { kind: 'coarse', tables: unique, reason }
}

// ── Subquery correlation ────────────────────────────────────────────
// Fill each subquery-bearing node with its recursively-extracted inner shape (and, for
// EXISTS / IN, the equi-correlations linking an outer column to an inner column) so the
// compiler can decorrelate into a semi/anti join. Predicates are freshly built, so
// mutating the nodes in place is safe.

function enrichSubqueries(shape: SelectShape, dialect: Dialect): void {
  const outerAliases = new Set([shape.from.alias, ...shape.joins.map((join) => join.table.alias)])
  const walk = (pred?: Predicate): void => {
    if (!pred) return
    if (pred.kind === 'and' || pred.kind === 'or') pred.parts.forEach(walk)
    else if (pred.kind === 'not') walk(pred.operand)
    else if (pred.kind === 'exists' && pred.src !== undefined) {
      const inner = extractQueryShape(pred.src, { dialect })
      pred.inner = inner
      pred.correlations = correlationsOf(inner, outerAliases, pred.inColumn)
    } else if (pred.kind === 'unknown' && pred.subquery !== undefined) {
      const inner = extractQueryShape(pred.subquery, { dialect })
      pred.inner = inner
      pred.correlations = correlationsOf(inner, outerAliases) // correlated scalar subquery in WHERE
    }
  }
  walk(shape.where)
  walk(shape.having)
  for (const join of shape.joins) walk(join.on)
  // correlated scalar subqueries in the projection
  for (const item of shape.projection.items) {
    if (item.kind === 'opaque' && item.inner) item.correlations = correlationsOf(item.inner, outerAliases)
  }
}

function correlationsOf(inner: QueryShape, outerAliases: Set<string>, inColumn?: ColRef): Correlation[] {
  if (inner.kind !== 'select') return []
  const innerAliases = new Set([inner.from.alias, ...inner.joins.map((join) => join.table.alias)])
  const out: Correlation[] = []
  // `col IN (subquery)`: the outer column semi-joins the inner's projected column.
  if (inColumn) {
    const projected = firstProjectedColumn(inner)
    if (projected) out.push({ outer: inColumn, inner: projected })
  }
  const visit = (pred?: Predicate): void => {
    if (!pred) return
    if (pred.kind === 'and' || pred.kind === 'or') pred.parts.forEach(visit)
    else if (pred.kind === 'not') visit(pred.operand)
    else if (pred.kind === 'compare' && pred.op === '=' && pred.left.kind === 'col' && pred.right.kind === 'col') {
      const l = pred.left.ref
      const r = pred.right.ref
      if (outerAliases.has(l.table) && innerAliases.has(r.table)) out.push({ outer: l, inner: r })
      else if (outerAliases.has(r.table) && innerAliases.has(l.table)) out.push({ outer: r, inner: l })
    }
  }
  visit(inner.where)
  for (const join of inner.joins) visit(join.on)
  return out
}

function firstProjectedColumn(inner: SelectShape): ColRef | undefined {
  for (const item of inner.projection.items) if (item.kind === 'col') return item.ref
  return undefined
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
        tables.add(relationKeyOf(tableOf(c)))
        return colRefOf(c)
      })
    return { on: 'columns', columns }
  }
  return { on: false }
}

function orderKeyOf(entry: unknown, tables: Set<string>): OrderKey {
  if (is(entry, Column)) {
    tables.add(relationKeyOf(tableOf(entry)))
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
  dialect: Dialect,
  tables: Set<string>,
): { projection: Projection; window: boolean } {
  const items: ProjItem[] = []
  let window = false

  const visit = (as: string | undefined, value: unknown): void => {
    if (is(value, Column)) {
      tables.add(relationKeyOf(tableOf(value)))
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
        if (agg.arg) tables.add(relationKeyOf(tableOf(agg.arg)))
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
      const sub = embeddedSubquery(value) // a scalar subquery wrapped in sql`(...)`
      const inner = sub !== undefined ? extractQueryShape(sub, { dialect }) : undefined
      items.push({ kind: 'opaque', columns, tables: [...local], window: isWindow, inner, as })
      return
    }
    if (isSelectBuilder(value)) {
      // a bare scalar-subquery builder used directly as a projected field
      const inner = extractQueryShape(value, { dialect })
      for (const name of inner.tables) tables.add(name)
      items.push({ kind: 'opaque', columns: [], tables: [...inner.tables], window: false, inner, as })
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

/** The first select-builder embedded in an SQL expression (a wrapped scalar subquery). */
function embeddedSubquery(sql: SQL): unknown | undefined {
  let found: unknown
  walkTokens(tokenize(sql.queryChunks), {
    onColumn: () => {},
    onOpaque: (value) => {
      if (found === undefined && isSelectBuilder(value)) found = value
    },
  })
  return found
}

// ── SQL walking ─────────────────────────────────────────────────────

function collectColumnRefs(sql: SQL, tables: Set<string>): ColRef[] {
  const refs: ColRef[] = []
  const seen = new Set<string>()
  walkTokens(tokenize(sql.queryChunks), {
    onColumn: (column) => {
      tables.add(relationKeyOf(tableOf(column)))
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
