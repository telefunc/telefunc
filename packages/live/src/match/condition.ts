export { compileCondition, inListCondition, allOf, mayBeTrue, isTrue, ALWAYS, TRUE, FALSE, NULL, and, or, not }
export { valueSource, COMPARISONS }
export type { Condition, Row, Truth, ValueSource }

import type { Node } from '../sql.js'
import { names } from '../sql.js'
import { compareValues, paramText, type Column } from './values.js'

/** A row image: each column's value in Postgres's text form, `null` for SQL `NULL`. A column absent from the object
 *  has an unknown value. */
type Row = Readonly<Record<string, string | null>>

/** The SQL truth values a condition may take on a row, as bits: `TRUE`, `FALSE`, `NULL`. Several bits are set when
 *  the outcome can't be told from the row (an unknown column, an operator not reproduced here). */
type Truth = number
const TRUE = 1
const FALSE = 2
const NULL = 4
const ALWAYS: Condition = () => TRUE | FALSE | NULL

type Condition = (row: Row, params: readonly unknown[]) => Truth

/** Where a scalar's value comes from: a column of the row being tested, a bound parameter, or a constant. */
type ValueSource =
  | { kind: 'column'; column: Column }
  | { kind: 'param'; index: number }
  | { kind: 'const'; text: string | null }
  | { kind: 'unknown' }

/** Compiles `node` into a test of one row. `resolve` maps a column reference to the row's column, or `undefined` when
 *  it belongs to another table: then the condition says nothing about the row (all outcomes possible). */
function compileCondition(node: Node, resolve: (columnRef: Node) => Column | undefined): Condition {
  const scalar = (n: Node) => valueSource(n, resolve)
  if (n(node, 'BoolExpr')) {
    const { boolop, args } = node.BoolExpr
    const parts = (args as Node[]).map((a) => compileCondition(a, resolve))
    if (boolop === 'NOT_EXPR') return (row, params) => not(parts[0]!(row, params))
    const combine = boolop === 'AND_EXPR' ? and : or
    return (row, params) =>
      parts.reduce((acc, part) => combine(acc, part(row, params)), boolop === 'AND_EXPR' ? TRUE : FALSE)
  }
  if (n(node, 'A_Const')) {
    const constant = node.A_Const
    if (constant.boolval !== undefined) return () => (constant.boolval.boolval ? TRUE : FALSE)
    return ALWAYS
  }
  if (n(node, 'NullTest')) {
    const source = scalar(node.NullTest.arg)
    const isNull = node.NullTest.nulltesttype === 'IS_NULL'
    return (row, params) => {
      const value = read(source, row, params)
      if (value === undefined) return TRUE | FALSE
      return (value === null) === isNull ? TRUE : FALSE
    }
  }
  if (n(node, 'BooleanTest')) {
    const inner = compileCondition(node.BooleanTest.arg, resolve)
    const test = booleanTest(node.BooleanTest.booltesttype)
    return (row, params) => mapTruth(inner(row, params), test)
  }
  if (n(node, 'ColumnRef')) {
    const column = resolve(node)
    if (!column || column.category !== 'B') return ALWAYS
    return (row) => {
      const value = row[column.name]
      if (value === undefined) return ALWAYS(row, [])
      if (value === null) return NULL
      return compareValues(value, 'true', column) === 0 ? TRUE : FALSE
    }
  }
  if (n(node, 'A_Expr')) return compileExpression(node.A_Expr, scalar)
  return ALWAYS
}

function compileExpression(expr: Node, scalar: (n: Node) => ValueSource): Condition {
  const operator = names(expr.name)[0]!
  const left = scalar(expr.lexpr)
  switch (expr.kind) {
    case 'AEXPR_OP': {
      const right = scalar(expr.rexpr)
      if (!COMPARISONS[operator]) return ALWAYS
      return comparison(operator, left, right)
    }
    case 'AEXPR_IN': {
      const list = (expr.rexpr.List?.items ?? []) as Node[]
      const equalities = list.map((item) => comparison('=', left, scalar(item)))
      const any: Condition = (row, params) => equalities.reduce((acc, eq) => or(acc, eq(row, params)), FALSE)
      return operator === '=' ? any : (row, params) => not(any(row, params))
    }
    case 'AEXPR_OP_ANY':
    case 'AEXPR_OP_ALL': {
      const right = scalar(expr.rexpr)
      if (!COMPARISONS[operator] || right.kind !== 'param') return ALWAYS
      const isAny = expr.kind === 'AEXPR_OP_ANY'
      return (row, params) => {
        const list = params[right.index]
        if (!Array.isArray(list)) return ALWAYS(row, params)
        const parts = list.map((item) =>
          compare(operator, left, { kind: 'const', text: paramText(item) ?? null }, row, params),
        )
        return parts.reduce((acc, t) => (isAny ? or : and)(acc, t), isAny ? FALSE : TRUE)
      }
    }
    case 'AEXPR_BETWEEN':
    case 'AEXPR_NOT_BETWEEN': {
      const [low, high] = (expr.rexpr.List.items as Node[]).map(scalar)
      const between: Condition = (row, params) =>
        and(compare('>=', left, low!, row, params), compare('<=', left, high!, row, params))
      return expr.kind === 'AEXPR_BETWEEN' ? between : (row, params) => not(between(row, params))
    }
    case 'AEXPR_LIKE':
    case 'AEXPR_ILIKE':
      return like(left, scalar(expr.rexpr), expr.kind === 'AEXPR_ILIKE', operator.startsWith('!'))
    default:
      return ALWAYS
  }
}

/** `column IN (values)` built from values the matcher derived itself. */
function inListCondition(column: Column, values: ValueSource[]): Condition {
  const source: ValueSource = { kind: 'column', column }
  const equalities = values.map((v) => comparison('=', source, v))
  return (row, params) => equalities.reduce((acc, eq) => or(acc, eq(row, params)), FALSE)
}

function allOf(conditions: Condition[]): Condition {
  if (conditions.length === 0) return ALWAYS
  if (conditions.length === 1) return conditions[0]!
  return (row, params) => conditions.reduce((acc, condition) => and(acc, condition(row, params)), TRUE)
}

function mayBeTrue(truth: Truth): boolean {
  return (truth & TRUE) !== 0
}

function isTrue(truth: Truth): boolean {
  return truth === TRUE
}

// ── Values ──

function valueSource(node: Node, resolve: (columnRef: Node) => Column | undefined): ValueSource {
  if (n(node, 'TypeCast')) {
    const inner = valueSource(node.TypeCast.arg, resolve)
    // A cast changes how a column compares; a parameter or constant is read as the column's type anyway.
    return inner.kind === 'column' ? { kind: 'unknown' } : inner
  }
  if (n(node, 'ParamRef')) return { kind: 'param', index: node.ParamRef.number - 1 }
  if (n(node, 'A_Const')) return { kind: 'const', text: constantText(node.A_Const) }
  if (n(node, 'ColumnRef')) {
    const column = resolve(node)
    return column ? { kind: 'column', column } : { kind: 'unknown' }
  }
  return { kind: 'unknown' }
}

function constantText(constant: Node): string | null {
  if (constant.isnull) return null
  if (constant.ival) return String(constant.ival.ival ?? 0)
  if (constant.fval) return constant.fval.fval
  if (constant.sval) return constant.sval.sval ?? ''
  if (constant.boolval) return constant.boolval.boolval ? 'true' : 'false'
  if (constant.bsval) return null
  return null
}

/** `undefined`: unknown. */
function read(source: ValueSource, row: Row, params: readonly unknown[]): string | null | undefined {
  switch (source.kind) {
    case 'column':
      return row[source.column.name]
    case 'param':
      return paramText(params[source.index])
    case 'const':
      return source.text
    case 'unknown':
      return undefined
  }
}

// ── Comparisons ──

const COMPARISONS: Record<string, (c: -1 | 0 | 1) => boolean> = {
  '=': (c) => c === 0,
  '<>': (c) => c !== 0,
  '!=': (c) => c !== 0,
  '<': (c) => c < 0,
  '<=': (c) => c <= 0,
  '>': (c) => c > 0,
  '>=': (c) => c >= 0,
}

function comparison(operator: string, left: ValueSource, right: ValueSource): Condition {
  return (row, params) => compare(operator, left, right, row, params)
}

function compare(operator: string, left: ValueSource, right: ValueSource, row: Row, params: readonly unknown[]): Truth {
  const column = left.kind === 'column' ? left.column : right.kind === 'column' ? right.column : undefined
  const a = read(left, row, params)
  const b = read(right, row, params)
  if (a === undefined || b === undefined) return TRUE | FALSE | NULL
  if (a === null || b === null) return NULL
  // Two columns of different types, or no column at all: the type to compare as isn't known.
  if (!column || (left.kind === 'column' && right.kind === 'column' && left.column.type !== right.column.type)) {
    return TRUE | FALSE
  }
  const order = compareValues(a, b, column)
  if (order === undefined) return TRUE | FALSE
  return COMPARISONS[operator]!(order) ? TRUE : FALSE
}

function like(left: ValueSource, pattern: ValueSource, caseInsensitive: boolean, negated: boolean): Condition {
  return (row, params) => {
    const value = read(left, row, params)
    const text = read(pattern, row, params)
    if (value === undefined || text === undefined) return TRUE | FALSE | NULL
    if (value === null || text === null) return NULL
    const column = left.kind === 'column' ? left.column : undefined
    // Case folding beyond ASCII depends on the database's locale.
    const ascii = /^[\x00-\x7f]*$/
    if (!column || column.category !== 'S' || !column.deterministic) return TRUE | FALSE
    if (caseInsensitive && (!ascii.test(value) || !ascii.test(text))) return TRUE | FALSE
    const matches = likeToRegExp(text, caseInsensitive).test(value)
    return matches !== negated ? TRUE : FALSE
  }
}

function likeToRegExp(pattern: string, caseInsensitive: boolean): RegExp {
  let source = ''
  for (let i = 0; i < pattern.length; i++) {
    const c = pattern[i]!
    if (c === '\\' && i + 1 < pattern.length) source += escapeRegExp(pattern[++i]!)
    else if (c === '%') source += '[\\s\\S]*'
    else if (c === '_') source += '[\\s\\S]'
    else source += escapeRegExp(c)
  }
  return new RegExp(`^${source}$`, caseInsensitive ? 'iu' : 'u')
}

function escapeRegExp(c: string): string {
  return c.replace(/[.*+?^${}()|[\]\\/]/g, '\\$&')
}

// ── Three-valued logic over sets of outcomes ──

function and(a: Truth, b: Truth): Truth {
  return combine(a, b, (x, y) => (x === FALSE || y === FALSE ? FALSE : x === NULL || y === NULL ? NULL : TRUE))
}

function or(a: Truth, b: Truth): Truth {
  return combine(a, b, (x, y) => (x === TRUE || y === TRUE ? TRUE : x === NULL || y === NULL ? NULL : FALSE))
}

function not(a: Truth): Truth {
  return mapTruth(a, (x) => (x === TRUE ? FALSE : x === FALSE ? TRUE : NULL))
}

function combine(a: Truth, b: Truth, op: (x: number, y: number) => number): Truth {
  let result = 0
  for (const x of [TRUE, FALSE, NULL]) {
    if (!(a & x)) continue
    for (const y of [TRUE, FALSE, NULL]) if (b & y) result |= op(x, y)
  }
  return result
}

function mapTruth(a: Truth, op: (x: number) => number): Truth {
  let result = 0
  for (const x of [TRUE, FALSE, NULL]) if (a & x) result |= op(x)
  return result
}

function booleanTest(kind: string): (x: number) => number {
  const is = (wanted: number, negate: boolean) => (x: number) => ((x === wanted) !== negate ? TRUE : FALSE)
  switch (kind) {
    case 'IS_TRUE':
      return is(TRUE, false)
    case 'IS_NOT_TRUE':
      return is(TRUE, true)
    case 'IS_FALSE':
      return is(FALSE, false)
    case 'IS_NOT_FALSE':
      return is(FALSE, true)
    case 'IS_UNKNOWN':
      return is(NULL, false)
    default:
      return is(NULL, true)
  }
}

function n(node: Node, kind: string): boolean {
  return node !== null && typeof node === 'object' && kind in node
}
