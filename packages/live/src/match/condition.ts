export { compileCondition, inListCondition, allOf, mayBeTrue, isTrue, ALWAYS, TRUE, FALSE, NULL, and, or, not }
export { COMPARISONS }
export type { Condition, Row, Truth, ValueSource }

import type { Node } from '../sql.js'
import { names } from '../sql.js'
import { isOperand, type Comparison, type OperandValue, type Operands } from './operands.js'
import { compareValues, type Column } from './values.js'

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

/** A test of one row, given the read's operands (`Operands`) as Postgres gave them. */
type Condition = (row: Row, operands: readonly OperandValue[]) => Truth

/** Where a scalar's value comes from: a column of the row being tested, or an operand of the read. */
type ValueSource = { kind: 'column'; column: Column } | { kind: 'operand'; index: number } | { kind: 'unknown' }

/** Compiles `node` into a test of one row. `resolve` maps a column reference to the row's column, or `undefined` when
 *  it belongs to another table: then the condition says nothing about the row (all outcomes possible). Parameters
 *  and constants become operands in `registry`. */
function compileCondition(node: Node, resolve: (columnRef: Node) => Column | undefined, registry: Operands): Condition {
  const scalar = (n: Node, comparison: Comparison = {}) => valueSource(n, resolve, registry, comparison)
  if (n(node, 'BoolExpr')) {
    const { boolop, args } = node.BoolExpr
    const parts = (args as Node[]).map((a) => compileCondition(a, resolve, registry))
    if (boolop === 'NOT_EXPR') return (row, operands) => not(parts[0]!(row, operands))
    const combine = boolop === 'AND_EXPR' ? and : or
    return (row, operands) =>
      parts.reduce((acc, part) => combine(acc, part(row, operands)), boolop === 'AND_EXPR' ? TRUE : FALSE)
  }
  if (n(node, 'A_Const')) {
    const constant = node.A_Const
    if (constant.boolval !== undefined) return () => (constant.boolval.boolval ? TRUE : FALSE)
    return ALWAYS
  }
  if (n(node, 'NullTest')) {
    const source = scalar(node.NullTest.arg, { form: 'nullness' })
    const isNull = node.NullTest.nulltesttype === 'IS_NULL'
    return (row, operands) => {
      const value = read(source, row, operands)
      if (value === undefined) return TRUE | FALSE
      return (value === null) === isNull ? TRUE : FALSE
    }
  }
  if (n(node, 'BooleanTest')) {
    const inner = compileCondition(node.BooleanTest.arg, resolve, registry)
    const test = booleanTest(node.BooleanTest.booltesttype)
    return (row, operands) => mapTruth(inner(row, operands), test)
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

function compileExpression(expr: Node, scalar: (n: Node, comparison?: Comparison) => ValueSource): Condition {
  const operator = names(expr.name)[0]!
  const left = scalar(expr.lexpr)
  // An operand compared with a column is read as the column's type.
  const as = left.kind === 'column' ? left.column : undefined
  switch (expr.kind) {
    case 'AEXPR_OP': {
      const right = scalar(expr.rexpr, { as })
      if (!COMPARISONS[operator]) return ALWAYS
      const leftOperand = right.kind === 'column' ? scalar(expr.lexpr, { as: right.column }) : left
      return comparison(operator, leftOperand, right)
    }
    case 'AEXPR_IN': {
      const list = (expr.rexpr.List?.items ?? []) as Node[]
      const equalities = list.map((item) => comparison('=', left, scalar(item, { as })))
      const any: Condition = (row, operands) => equalities.reduce((acc, eq) => or(acc, eq(row, operands)), FALSE)
      return operator === '=' ? any : (row, operands) => not(any(row, operands))
    }
    case 'AEXPR_OP_ANY':
    case 'AEXPR_OP_ALL': {
      const right = scalar(expr.rexpr, { as, form: 'list' })
      if (!COMPARISONS[operator] || right.kind !== 'operand') return ALWAYS
      const isAny = expr.kind === 'AEXPR_OP_ANY'
      return (row, operands) => {
        const list = operands[right.index]
        const value = read(left, row, operands)
        // `ANY` and `ALL` of a `NULL` array are `NULL`.
        if (list === null) return NULL
        if (!Array.isArray(list) || left.kind !== 'column') return ALWAYS(row, operands)
        const parts = list.map((item) => truthOf(operator, value, item, left.column))
        return parts.reduce((acc, t) => (isAny ? or : and)(acc, t), isAny ? FALSE : TRUE)
      }
    }
    case 'AEXPR_BETWEEN':
    case 'AEXPR_NOT_BETWEEN': {
      const [low, high] = (expr.rexpr.List.items as Node[]).map((bound) => scalar(bound, { as }))
      const between: Condition = (row, operands) =>
        and(compare('>=', left, low!, row, operands), compare('<=', left, high!, row, operands))
      return expr.kind === 'AEXPR_BETWEEN' ? between : (row, operands) => not(between(row, operands))
    }
    case 'AEXPR_LIKE':
    case 'AEXPR_ILIKE':
      return like(left, scalar(expr.rexpr, { form: 'pattern' }), expr.kind === 'AEXPR_ILIKE', operator.startsWith('!'))
    default:
      return ALWAYS
  }
}

/** `column IN (values)` built from values the matcher derived itself. */
function inListCondition(column: Column, values: ValueSource[]): Condition {
  const source: ValueSource = { kind: 'column', column }
  const equalities = values.map((v) => comparison('=', source, v))
  return (row, operands) => equalities.reduce((acc, eq) => or(acc, eq(row, operands)), FALSE)
}

function allOf(conditions: Condition[]): Condition {
  if (conditions.length === 0) return ALWAYS
  if (conditions.length === 1) return conditions[0]!
  return (row, operands) => conditions.reduce((acc, condition) => and(acc, condition(row, operands)), TRUE)
}

function mayBeTrue(truth: Truth): boolean {
  return (truth & TRUE) !== 0
}

function isTrue(truth: Truth): boolean {
  return truth === TRUE
}

// ── Values ──

function valueSource(
  node: Node,
  resolve: (columnRef: Node) => Column | undefined,
  registry: Operands,
  comparison: Comparison,
): ValueSource {
  if (n(node, 'ColumnRef')) {
    const column = resolve(node)
    return column ? { kind: 'column', column } : { kind: 'unknown' }
  }
  // A cast of a column changes how it compares.
  if (n(node, 'TypeCast') && !isOperand(node)) return { kind: 'unknown' }
  const index = registry.of(node, comparison)
  return index === undefined ? { kind: 'unknown' } : { kind: 'operand', index }
}

/** `undefined`: unknown. */
function read(source: ValueSource, row: Row, operands: readonly OperandValue[]): string | null | undefined {
  switch (source.kind) {
    case 'column':
      return row[source.column.name]
    case 'operand': {
      const value = operands[source.index]
      return typeof value === 'string' || value === null ? value : undefined
    }
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
  return (row, operands) => compare(operator, left, right, row, operands)
}

function compare(
  operator: string,
  left: ValueSource,
  right: ValueSource,
  row: Row,
  operands: readonly OperandValue[],
): Truth {
  const a = read(left, row, operands)
  const b = read(right, row, operands)
  // Two columns of different types: the type to compare as isn't known.
  if (left.kind === 'column' && right.kind === 'column' && left.column.type !== right.column.type)
    return truthOf(operator, a, b, undefined)
  const column = left.kind === 'column' ? left.column : right.kind === 'column' ? right.column : undefined
  return truthOf(operator, a, b, column)
}

/** `a operator b`, both in Postgres's text form, compared as values of `column` (`undefined`: unknown). */
function truthOf(
  operator: string,
  a: string | null | undefined,
  b: string | null | undefined,
  column: Column | undefined,
): Truth {
  if (a === undefined || b === undefined) return TRUE | FALSE | NULL
  if (a === null || b === null) return NULL
  if (!column) return TRUE | FALSE
  const order = compareValues(a, b, column)
  if (order === undefined) return TRUE | FALSE
  return COMPARISONS[operator]!(order) ? TRUE : FALSE
}

function like(left: ValueSource, pattern: ValueSource, caseInsensitive: boolean, negated: boolean): Condition {
  return (row, operands) => {
    const value = read(left, row, operands)
    const text = read(pattern, row, operands)
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
