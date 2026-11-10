export { analyzeRead }
export type { ReadShape, Alias, Route, SortKey }

import type { Catalog, Table, TableSet } from '../catalog.js'
import { functionNamesIn, names, walk, type Node, type RangeVar } from '../sql.js'
import { allOf, ALWAYS, compileCondition, inListCondition, type Condition, type ValueSource } from './condition.js'
import type { Column } from './values.js'

/** What a `SELECT` reads, from its text alone: one entry per table occurrence, and how to bound it by its result. */
type ReadShape = {
  /** `rows`: a list of table rows, bounded by its result (hidden columns carry the keys); `aggregate`: `count`, `sum`
   *  and `avg`, changed exactly by writes to the rows they count; `other`: changed by any write to a matching row. */
  kind: 'rows' | 'aggregate' | 'other'
  aliases: Alias[]
  /** Tables read without a known filter: views' tables, row-level security policies' tables, user functions. */
  alsoReads: TableSet
  /** Expressions appended to the select list of a `rows` read, each `::text`. */
  hiddenColumns: string[]
  limit?: ValueSource
  offset?: ValueSource
  /** `undefined`: the order can't be evaluated on a row (an expression, or columns of several tables). */
  sort?: SortKey[]
}

type Alias = {
  name: string
  table: Table
  /** On the null-extended side of an outer join: its rows don't decide which rows of the other side appear. */
  nullable: boolean
  /** True on every row that can be part of the result (conditions on this table alone). */
  condition: Condition
  routes: Route[]
  /** The columns the statement uses; `all` for `*` or a whole-row reference. */
  referenced: Set<string> | 'all'
  /** `top`: in the outer `FROM` of a `rows` read; `child`: in a subquery that only fills columns of the outer rows (a
   *  relational include), tied to an outer row by `correlations`; `other`: changes to matching rows always count. */
  role: 'top' | 'child' | 'other'
  /** Indexes into the hidden columns: this table's primary key, for `top`. */
  primaryKeyHidden?: number[]
  correlations: Correlation[]
  /** For a `top` alias on the null-extended side: the outer rows it attaches to. */
  partners: Correlation[]
}

/** `column = parent.parentColumn`, with `hidden` the index of the parent column among the hidden columns. */
type Correlation = { column: Column; parent: Alias; parentColumn: Column; hidden?: number }

/** `column = value` or `column IN (values)`: a row whose value isn't listed can't be part of the result. */
type Route = { column: Column; values: ValueSource[] } | { column: Column; anyOf: number }

type SortKey = { column: Column; alias: Alias; descending: boolean; nullsFirst: boolean; hidden: number }

type Scope = {
  parent: Scope | undefined
  aliases: Map<string, Alias>
  /** Names in `FROM` that aren't tables: subqueries, functions, CTEs. Their columns are unknown here. */
  opaque: Set<string>
  ctes: Set<string>
  outputOnly: boolean
}

/** `alias.column = other.otherColumn` holds on every row of `alias` that can be part of the result. */
type Edge = { alias: Alias; column: Column; other: Alias; otherColumn: Column }

type Analysis = { catalog: Catalog; aliases: Alias[]; alsoReads: Set<string> | 'all' }

const AGGREGATES = new Set([
  'count',
  'sum',
  'avg',
  'min',
  'max',
  'array_agg',
  'json_agg',
  'jsonb_agg',
  'string_agg',
  'json_object_agg',
  'jsonb_object_agg',
  'bool_and',
  'bool_or',
  'every',
  'bit_and',
  'bit_or',
  'stddev',
  'variance',
])
const EXACT_AGGREGATES = new Set(['count', 'sum', 'avg'])

function analyzeRead(select: Node, catalog: Catalog): ReadShape {
  const analysis: Analysis = { catalog, aliases: [], alsoReads: new Set() }
  const top = analyzeSelect(select, undefined, { top: true, outputOnly: false }, analysis)
  const kind = top ? readKind(select) : 'other'
  const shape: ReadShape = { kind, aliases: analysis.aliases, alsoReads: analysis.alsoReads, hiddenColumns: [] }
  if (kind === 'rows' && top) addResultKeys(shape, select, top)
  else for (const alias of shape.aliases) alias.role = 'other'
  return shape
}

// ── Scopes ──

/** Returns the scope of a plain `SELECT … FROM`, `undefined` for a set operation or `VALUES`. */
function analyzeSelect(
  stmt: Node,
  parent: Scope | undefined,
  flags: { top: boolean; outputOnly: boolean },
  analysis: Analysis,
): Scope | undefined {
  if (stmt.op && stmt.op !== 'SETOP_NONE') {
    analyzeSelect(stmt.larg, parent, { top: false, outputOnly: flags.outputOnly }, analysis)
    analyzeSelect(stmt.rarg, parent, { top: false, outputOnly: flags.outputOnly }, analysis)
    analyzeOrdering(stmt, parent, flags, analysis)
    return undefined
  }
  const scope: Scope = { parent, aliases: new Map(), opaque: new Set(), ctes: new Set(), outputOnly: flags.outputOnly }
  for (const cte of (stmt.withClause?.ctes ?? []) as Node[]) {
    const query = cte.CommonTableExpr.ctequery
    if (query.SelectStmt)
      analyzeSelect(query.SelectStmt, parent, { top: false, outputOnly: flags.outputOnly }, analysis)
    else readsEverything(analysis)
    scope.ctes.add(cte.CommonTableExpr.ctename)
  }
  for (const name of functionNamesIn(stmt)) if (analysis.catalog.functions.has(name)) readsEverything(analysis)

  // This scope's own tables first: subqueries below derive their routes from them.
  const filters: Filter[] = []
  const subqueries: (() => void)[] = []
  for (const item of (stmt.fromClause ?? []) as Node[])
    addFromItem(item, false, scope, flags, analysis, filters, subqueries)
  for (const conjunct of splitAnd(stmt.whereClause)) filters.push({ conjunct })
  const edges = applyFilters(filters, scope)

  const subFlags = { top: false, outputOnly: flags.outputOnly }
  for (const analyzeSubquery of subqueries) analyzeSubquery()
  for (const { conjunct } of filters) analyzeSublinks(conjunct, scope, subFlags, analysis)
  for (const target of (stmt.targetList ?? []) as Node[]) {
    analyzeSublinks(target, scope, { top: false, outputOnly: flags.top || flags.outputOnly }, analysis)
    markReferenced(target, scope)
  }
  for (const clause of [stmt.groupClause, stmt.havingClause, stmt.distinctClause, stmt.windowClause]) {
    analyzeSublinks(clause, scope, subFlags, analysis)
    markReferenced(clause, scope)
  }
  analyzeOrdering(stmt, scope, flags, analysis)

  for (const { alias, column, other, otherColumn } of edges) {
    const correlation = { column, parent: other, parentColumn: otherColumn }
    if (flags.outputOnly && other.role === 'top') {
      alias.role = 'child'
      alias.correlations.push(correlation)
    }
    if (alias.role === 'top' && alias.nullable && other.role === 'top' && !other.nullable)
      alias.partners.push(correlation)
  }
  return scope
}

type Filter = { conjunct: Node; nullableOnly?: Alias[] }

/** Compiles each alias's own conditions and routes; returns the equalities tying an alias to another. */
function applyFilters(filters: Filter[], scope: Scope): Edge[] {
  const edges: Edge[] = []
  const ownFilters = new Map<Alias, Node[]>()
  for (const { conjunct, nullableOnly } of filters) {
    markReferenced(conjunct, scope)
    if (hasSublink(conjunct)) continue
    const aliases = aliasesOf(conjunct, scope)
    if (!aliases) continue
    const own = (alias: Alias) => scope.aliases.get(alias.name) === alias && appliesTo(alias, nullableOnly)
    for (const edge of equalityEdges(conjunct, scope)) if (own(edge.alias)) edges.push(edge)
    const [alias] = aliases
    if (aliases.size === 1 && own(alias!)) ownFilters.set(alias!, [...(ownFilters.get(alias!) ?? []), conjunct])
  }
  for (const alias of scope.aliases.values()) {
    const conjuncts = ownFilters.get(alias) ?? []
    const resolve = (ref: Node) => {
      const found = resolveColumn(ref, scope)
      return found?.alias === alias ? found.column : undefined
    }
    alias.condition = allOf(conjuncts.map((c) => compileCondition(c, resolve)))
    alias.routes = conjuncts.flatMap((c) => routeOf(c, resolve) ?? [])
  }
  deriveRoutes(edges)
  return edges
}

function addFromItem(
  item: Node,
  nullable: boolean,
  scope: Scope,
  flags: { top: boolean; outputOnly: boolean },
  analysis: Analysis,
  filters: Filter[],
  subqueries: (() => void)[],
): Alias[] {
  if (item.RangeVar) {
    const rv = item.RangeVar as RangeVar
    const name = rv.alias?.aliasname ?? rv.relname
    if (!rv.schemaname && isCte(rv.relname, scope)) {
      scope.opaque.add(name)
      return []
    }
    const table = analysis.catalog.find(rv)
    if (!table) {
      readsEverything(analysis)
      scope.opaque.add(name)
      return []
    }
    addAlsoReads(table, analysis)
    const alias: Alias = {
      name,
      table,
      nullable,
      condition: ALWAYS,
      routes: [],
      referenced: new Set(),
      role: flags.top ? 'top' : 'other',
      correlations: [],
      partners: [],
    }
    scope.aliases.set(name, alias)
    analysis.aliases.push(alias)
    return [alias]
  }
  if (item.JoinExpr) {
    const join = item.JoinExpr
    const type = join.jointype as string
    const leftNullable = nullable || type === 'JOIN_RIGHT' || type === 'JOIN_FULL'
    const left = addFromItem(join.larg, leftNullable, scope, flags, analysis, filters, subqueries)
    const rightNullable = nullable || type === 'JOIN_LEFT' || type === 'JOIN_FULL'
    const right = addFromItem(join.rarg, rightNullable, scope, flags, analysis, filters, subqueries)
    // An `ON` condition decides which rows of the null-extended side join; the other side's rows appear anyway.
    const filtered =
      type === 'JOIN_INNER' ? undefined : type === 'JOIN_LEFT' ? right : type === 'JOIN_RIGHT' ? left : []
    for (const conjunct of splitAnd(join.quals)) filters.push({ conjunct, nullableOnly: filtered })
    for (const column of names(join.usingClause)) {
      const conjunct = usingEquality(column, left, right)
      if (conjunct) filters.push({ conjunct, nullableOnly: filtered })
    }
    return [...left, ...right]
  }
  if (item.RangeSubselect) {
    const sub = item.RangeSubselect
    const lateralOutput = sub.lateral && nullable && (flags.top || flags.outputOnly)
    const subFlags = { top: false, outputOnly: flags.outputOnly || lateralOutput }
    subqueries.push(() =>
      analyzeSelect(sub.subquery.SelectStmt, sub.lateral ? scope : scope.parent, subFlags, analysis),
    )
    if (sub.alias) scope.opaque.add(sub.alias.aliasname)
    return []
  }
  if (item.RangeFunction?.alias) scope.opaque.add(item.RangeFunction.alias.aliasname)
  else if (!item.RangeFunction) readsEverything(analysis)
  return []
}

function analyzeOrdering(stmt: Node, scope: Scope | undefined, flags: { outputOnly: boolean }, analysis: Analysis) {
  for (const clause of [stmt.sortClause, stmt.limitCount, stmt.limitOffset]) {
    if (scope) analyzeSublinks(clause, scope, { top: false, outputOnly: flags.outputOnly }, analysis)
    if (scope) markReferenced(clause, scope)
  }
}

function analyzeSublinks(
  node: unknown,
  scope: Scope,
  flags: { top: boolean; outputOnly: boolean },
  analysis: Analysis,
) {
  walk(node, (key, value) => {
    if (key !== 'SubLink') return
    analyzeSelect(value.subselect.SelectStmt, scope, flags, analysis)
    if (value.testexpr) markReferenced(value.testexpr, scope)
    return false
  })
}

// ── Columns ──

function resolveColumn(ref: Node, scope: Scope): { alias: Alias; column: Column } | undefined {
  const fields = names(ref.ColumnRef.fields)
  const name = fields.at(-1)!
  if (name === '*') return undefined
  if (fields.length >= 2) {
    const qualifier = fields.at(-2)!
    for (let s: Scope | undefined = scope; s; s = s.parent) {
      if (s.opaque.has(qualifier)) return undefined
      const alias = s.aliases.get(qualifier)
      if (alias) {
        const column = alias.table.column.get(name)
        return column ? { alias, column } : undefined
      }
    }
    return undefined
  }
  for (let s: Scope | undefined = scope; s; s = s.parent) {
    const candidates = [...s.aliases.values()].filter((a) => a.table.column.has(name))
    if (candidates.length === 1 && s.opaque.size === 0)
      return { alias: candidates[0]!, column: candidates[0]!.table.column.get(name)! }
    if (candidates.length > 0 || s.opaque.size > 0) return undefined
  }
  return undefined
}

/** Marks every column `node` uses on the aliases it may belong to; an unresolvable or whole-row reference marks
 *  them all. */
function markReferenced(node: unknown, scope: Scope): void {
  walk(node, (key, value) => {
    if (key === 'SubLink') return false
    if (key !== 'ColumnRef') return
    const ref = { ColumnRef: value }
    const found = resolveColumn(ref, scope)
    if (found) {
      if (found.alias.referenced !== 'all') found.alias.referenced.add(found.column.name)
      return
    }
    const fields = names(value.fields)
    const name = fields.at(-1)!
    for (let s: Scope | undefined = scope; s; s = s.parent) {
      for (const alias of s.aliases.values()) {
        const qualifierMatches = fields.length >= 2 ? fields.at(-2) === alias.name : true
        const wholeRow = fields.length === 1 && name === alias.name
        if (!qualifierMatches && !wholeRow) continue
        if (name === '*' || wholeRow) alias.referenced = 'all'
        else if (alias.referenced !== 'all' && alias.table.column.has(name)) alias.referenced.add(name)
      }
    }
  })
}

function aliasesOf(node: Node, scope: Scope): Set<Alias> | undefined {
  const aliases = new Set<Alias>()
  let resolved = true
  walk(node, (key, value) => {
    if (key !== 'ColumnRef') return
    const found = resolveColumn({ ColumnRef: value }, scope)
    if (found) aliases.add(found.alias)
    else resolved = false
  })
  return resolved ? aliases : undefined
}

/** `a.x = b.y` as an edge from each side to the other. */
function equalityEdges(conjunct: Node, scope: Scope): Edge[] {
  const expr = conjunct.A_Expr
  if (!expr || expr.kind !== 'AEXPR_OP' || names(expr.name)[0] !== '=') return []
  if (!expr.lexpr.ColumnRef || !expr.rexpr.ColumnRef) return []
  const left = resolveColumn(expr.lexpr, scope)
  const right = resolveColumn(expr.rexpr, scope)
  if (!left || !right || left.alias === right.alias) return []
  return [
    { alias: left.alias, column: left.column, other: right.alias, otherColumn: right.column },
    { alias: right.alias, column: right.column, other: left.alias, otherColumn: left.column },
  ]
}

function usingEquality(column: string, left: Alias[], right: Alias[]): Node | undefined {
  const l = left.find((a) => a.table.column.has(column))
  const r = right.find((a) => a.table.column.has(column))
  if (!l || !r) return undefined
  const ref = (alias: string) => ({
    ColumnRef: { fields: [{ String: { sval: alias } }, { String: { sval: column } }] },
  })
  return { A_Expr: { kind: 'AEXPR_OP', name: [{ String: { sval: '=' } }], lexpr: ref(l.name), rexpr: ref(r.name) } }
}

// ── Routes ──

function routeOf(conjunct: Node, resolve: (ref: Node) => Column | undefined): Route | undefined {
  const expr = conjunct.A_Expr
  if (!expr || names(expr.name)[0] !== '=' || !expr.lexpr.ColumnRef) return undefined
  const column = resolve(expr.lexpr)
  if (!column) return undefined
  const value = (node: Node): ValueSource | undefined => {
    const inner = node.TypeCast ? node.TypeCast.arg : node
    if (inner.ParamRef) return { kind: 'param', index: inner.ParamRef.number - 1 }
    if (inner.A_Const && !inner.A_Const.isnull) {
      const c = inner.A_Const
      const text = c.ival ? String(c.ival.ival ?? 0) : c.sval ? (c.sval.sval ?? '') : c.fval ? c.fval.fval : undefined
      return text === undefined ? undefined : { kind: 'const', text }
    }
    return undefined
  }
  if (expr.kind === 'AEXPR_OP') {
    const v = value(expr.rexpr)
    return v ? { column, values: [v] } : undefined
  }
  if (expr.kind === 'AEXPR_IN') {
    const values = ((expr.rexpr.List?.items ?? []) as Node[]).map(value)
    return values.every((v) => v) ? { column, values: values as ValueSource[] } : undefined
  }
  if (expr.kind === 'AEXPR_OP_ANY') {
    const v = value(expr.rexpr)
    return v?.kind === 'param' ? { column, anyOf: v.index } : undefined
  }
  return undefined
}

/** `b.y = a.x` and `a.x = $1`: `b.y = $1` too. A `b` row with another value can only join `a` rows the result
 *  excludes. */
function deriveRoutes(edges: Edge[]): void {
  for (let changed = true; changed; ) {
    changed = false
    for (const { alias, column, other, otherColumn } of edges) {
      for (const route of other.routes) {
        if (route.column !== otherColumn || alias.routes.some((r) => r.column === column && sameValues(r, route)))
          continue
        const derived: Route = 'anyOf' in route ? { column, anyOf: route.anyOf } : { column, values: route.values }
        alias.routes.push(derived)
        if ('values' in derived) alias.condition = allOf([alias.condition, inListCondition(column, derived.values)])
        changed = true
      }
    }
  }
}

function sameValues(a: Route, b: Route): boolean {
  return JSON.stringify('anyOf' in a ? a.anyOf : a.values) === JSON.stringify('anyOf' in b ? b.anyOf : b.values)
}

// ── Result keys ──

function readKind(select: Node): ReadShape['kind'] {
  if (select.distinctClause || select.havingClause || select.windowClause) return 'other'
  let aggregates = select.groupClause ? 1 : 0
  let inexact = false
  walk(select.targetList, (key, value) => {
    if (key === 'SubLink') return false
    if (key !== 'FuncCall') return
    const name = names(value.funcname).at(-1)!
    if (value.over) inexact = true
    if (value.agg_star || AGGREGATES.has(name)) {
      aggregates++
      if (!EXACT_AGGREGATES.has(name) || value.agg_distinct) inexact = true
    }
  })
  if (inexact) return 'other'
  return aggregates > 0 ? 'aggregate' : 'rows'
}

function addResultKeys(shape: ReadShape, select: Node, top: Scope): void {
  const hidden = (alias: Alias, column: string) => {
    const expression = `${quote(alias.name)}.${quote(column)}::text`
    const known = shape.hiddenColumns.indexOf(expression)
    if (known !== -1) return known
    shape.hiddenColumns.push(expression)
    return shape.hiddenColumns.length - 1
  }
  for (const alias of top.aliases.values()) {
    if (alias.table.primaryKey.length > 0) alias.primaryKeyHidden = alias.table.primaryKey.map((c) => hidden(alias, c))
  }
  for (const alias of shape.aliases) {
    for (const c of [...alias.correlations, ...alias.partners]) c.hidden = hidden(c.parent, c.parentColumn.name)
  }
  shape.limit = limitValue(select.limitCount)
  shape.offset = limitValue(select.limitOffset)
  shape.sort = sortKeys(select, top, (alias, column) => hidden(alias, column.name))
}

function limitValue(node: Node | undefined): ValueSource | undefined {
  if (!node || node.A_Const?.isnull) return undefined
  if (node.ParamRef) return { kind: 'param', index: node.ParamRef.number - 1 }
  if (node.A_Const?.ival) return { kind: 'const', text: String(node.A_Const.ival.ival ?? 0) }
  return { kind: 'unknown' }
}

function sortKeys(select: Node, scope: Scope, hidden: (alias: Alias, column: Column) => number): SortKey[] | undefined {
  const keys: SortKey[] = []
  for (const item of (select.sortClause ?? []) as Node[]) {
    const sortBy = item.SortBy
    const found = sortColumn(sortBy.node, select, scope)
    if (!found || sortBy.useOp) return undefined
    const descending = sortBy.sortby_dir === 'SORTBY_DESC'
    const nullsFirst =
      sortBy.sortby_nulls === 'SORTBY_NULLS_DEFAULT' ? descending : sortBy.sortby_nulls === 'SORTBY_NULLS_FIRST'
    keys.push({ ...found, descending, nullsFirst, hidden: hidden(found.alias, found.column) })
  }
  return keys
}

function sortColumn(node: Node, select: Node, scope: Scope): { alias: Alias; column: Column } | undefined {
  const targets = (select.targetList ?? []) as Node[]
  if (node.A_Const?.ival) {
    const target = targets[(node.A_Const.ival.ival ?? 0) - 1]?.ResTarget
    return target?.val.ColumnRef ? resolveColumn(target.val, scope) : undefined
  }
  if (!node.ColumnRef) return undefined
  const fields = names(node.ColumnRef.fields)
  if (fields.length === 1) {
    const target = targets.find((t) => t.ResTarget.name === fields[0])?.ResTarget
    if (target) return target.val.ColumnRef ? resolveColumn(target.val, scope) : undefined
  }
  return resolveColumn(node, scope)
}

// ── Helpers ──

function splitAnd(node: Node | undefined): Node[] {
  if (!node) return []
  if (node.BoolExpr?.boolop === 'AND_EXPR') return (node.BoolExpr.args as Node[]).flatMap(splitAnd)
  return [node]
}

function hasSublink(node: Node): boolean {
  let found = false
  walk(node, (key) => {
    if (key === 'SubLink') found = true
  })
  return found
}

function appliesTo(alias: Alias, nullableOnly: Alias[] | undefined): boolean {
  return nullableOnly ? nullableOnly.includes(alias) : !alias.nullable
}

function isCte(name: string, scope: Scope): boolean {
  for (let s: Scope | undefined = scope; s; s = s.parent) if (s.ctes.has(name)) return true
  return false
}

function addAlsoReads(table: Table, analysis: Analysis, seen = new Set<string>()): void {
  if (analysis.alsoReads === 'all' || seen.has(table.id)) return
  seen.add(table.id)
  if (table.alsoReads === 'all') return readsEverything(analysis)
  for (const id of table.alsoReads) {
    analysis.alsoReads.add(id)
    const other = analysis.catalog.tables.get(id)
    if (other) addAlsoReads(other, analysis, seen)
  }
}

function readsEverything(analysis: Analysis): void {
  analysis.alsoReads = 'all'
}

function quote(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`
}
