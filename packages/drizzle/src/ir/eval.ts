export { eval3, rowView, compareValues }

import type { CompareOp, Predicate, RowView, ScalarExpr, Tri } from './types.js'

/** Evaluate a predicate against one row in SQL three-valued (Kleene) logic.
 *  `undefined` is the unknown truth value.
 *
 *  This is the faithful SQL semantics — every comparison touching NULL is unknown,
 *  not false — which is what makes NNF a truth-preserving rewrite (`eval3(nnf(p))
 *  === eval3(p)`). A NULL and a not-captured column (MISSING) are both unknown to a
 *  comparison, but only NULL is a definite value to `is null`; that is the whole
 *  distinction between them. How unknown folds into a keep/drop decision — NULL
 *  excludes like a real WHERE, MISSING widens to a match — is the σ layer's policy,
 *  not this evaluator's. */
function eval3(pred: Predicate, row: RowView): Tri {
  switch (pred.kind) {
    case 'true':
      return true
    case 'false':
      return false
    case 'and':
      return and3(pred.parts.map((p) => eval3(p, row)))
    case 'or':
      return or3(pred.parts.map((p) => eval3(p, row)))
    case 'not':
      return invert(eval3(pred.operand, row))
    case 'compare':
      return cmp(scalar(pred.left, row), scalar(pred.right, row), COMPARATORS[pred.op])
    case 'in':
      return evalIn(pred, row)
    case 'like':
      return evalLike(pred, row)
    case 'isNull':
      return evalIsNull(pred, row)
    case 'between': {
      const value = scalar(pred.expr, row)
      return and3([cmp(value, scalar(pred.low, row), gte), cmp(value, scalar(pred.high, row), lte)])
    }
    case 'exists':
      // Correlated existence is decorrelated into a semi/anti join at compile time
      // and can't be judged from a single captured row.
      return undefined
    case 'unknown':
      return undefined
  }
}

/** A `RowView` over a plain record keyed by database column name; an absent key is
 *  a not-captured column (MISSING), a present `null` is a real SQL NULL. */
function rowView(record: Record<string, unknown>): RowView {
  return { get: (column) => (column in record ? { present: true, value: record[column] } : { present: false }) }
}

// ── Scalar operands ─────────────────────────────────────────────────

/** An operand value, or UNKNOWN for a not-captured column or opaque operand. */
const UNKNOWN: unique symbol = Symbol('unknown-operand')
type Operand = unknown | typeof UNKNOWN

function scalar(expr: ScalarExpr, row: RowView): Operand {
  if (expr.kind === 'col') {
    const cell = row.get(expr.ref.column)
    return cell.present ? cell.value : UNKNOWN
  }
  if (expr.kind === 'lit') return expr.value
  // Placeholders are unknown at row-eval time (the graph resolves them per instance).
  return UNKNOWN
}

// ── Atoms ───────────────────────────────────────────────────────────

const COMPARATORS: Record<CompareOp, (order: number) => boolean> = {
  '=': (o) => o === 0,
  '<>': (o) => o !== 0,
  '>': (o) => o > 0,
  '>=': (o) => o >= 0,
  '<': (o) => o < 0,
  '<=': (o) => o <= 0,
}

const gte = (o: number) => o >= 0
const lte = (o: number) => o <= 0

function cmp(a: Operand, b: Operand, f: (order: number) => boolean): Tri {
  if (a === UNKNOWN || b === UNKNOWN) return undefined
  if (a === null || b === null) return undefined // comparison with NULL is unknown, not false
  const order = compareValues(a, b)
  return order === undefined ? undefined : f(order)
}

function evalIn(pred: Extract<Predicate, { kind: 'in' }>, row: RowView): Tri {
  const value = scalar(pred.expr, row)
  if (value === UNKNOWN || value === null) return undefined
  let sawUnknown = false
  let matched = false
  for (const candidate of pred.values) {
    const c = scalar(candidate, row)
    if (c === UNKNOWN || c === null) {
      sawUnknown = true
      continue
    }
    if (compareValues(value, c) === 0) {
      matched = true
      break
    }
  }
  const base: Tri = matched ? true : sawUnknown ? undefined : false
  return pred.negated ? invert(base) : base
}

function evalLike(pred: Extract<Predicate, { kind: 'like' }>, row: RowView): Tri {
  const value = scalar(pred.expr, row)
  if (value === UNKNOWN || value === null) return undefined
  if (pred.pattern === null) return undefined // opaque / placeholder pattern
  if (typeof value !== 'string') return undefined
  const matched = regexFor(pred, pred.pattern).test(value)
  return pred.negated ? !matched : matched
}

function evalIsNull(pred: Extract<Predicate, { kind: 'isNull' }>, row: RowView): Tri {
  const value = scalar(pred.expr, row)
  if (value === UNKNOWN) return undefined // MISSING: we can't tell whether it's null
  const isNull = value === null
  return pred.negated ? !isNull : isNull
}

// ── Three-valued combinators ────────────────────────────────────────

function and3(values: Tri[]): Tri {
  let result: Tri = true
  for (const value of values) {
    if (value === false) return false
    if (value === undefined) result = undefined
  }
  return result
}

function or3(values: Tri[]): Tri {
  let result: Tri = false
  for (const value of values) {
    if (value === true) return true
    if (value === undefined) result = undefined
  }
  return result
}

function invert(value: Tri): Tri {
  return value === undefined ? undefined : !value
}

// ── Value comparison across producers ───────────────────────────────
// One order bridging producer differences: CDC delivers pg-parsed values, ORM
// capture delivers the user's JS values, SQLite stores booleans as 0/1 and dates
// as strings. Returns undefined for genuinely incomparable pairs.

function compareValues(a: unknown, b: unknown): number | undefined {
  if (a instanceof Date || b instanceof Date) {
    const at = toTime(a)
    const bt = toTime(b)
    return at === undefined || bt === undefined ? undefined : order(at, bt)
  }
  if (typeof a === 'boolean' || typeof b === 'boolean') {
    const an = toBoolNumber(a)
    const bn = toBoolNumber(b)
    return an === undefined || bn === undefined ? undefined : order(an, bn)
  }
  if (typeof a === 'bigint' || typeof b === 'bigint') {
    try {
      return order(BigInt(a as string | number | bigint), BigInt(b as string | number | bigint))
    } catch {
      return undefined
    }
  }
  if (typeof a === 'number' || typeof b === 'number') {
    const an = toNumber(a)
    const bn = toNumber(b)
    return an === undefined || bn === undefined ? undefined : order(an, bn)
  }
  if (typeof a === 'string' && typeof b === 'string') return order(a, b)
  if (typeof a === 'object' && a !== null && typeof b === 'object' && b !== null) {
    // pg arrays / json columns: equality is decidable, ordering is not
    return JSON.stringify(a) === JSON.stringify(b) ? 0 : undefined
  }
  return undefined
}

function order<T extends number | string | bigint>(a: T, b: T): number {
  return a < b ? -1 : a > b ? 1 : 0
}

function toTime(value: unknown): number | undefined {
  if (value instanceof Date) return value.getTime()
  if (typeof value === 'string' || typeof value === 'number') {
    const time = new Date(value).getTime()
    return Number.isNaN(time) ? undefined : time
  }
  return undefined
}

function toBoolNumber(value: unknown): number | undefined {
  if (typeof value === 'boolean') return Number(value)
  if (value === 0 || value === 1) return value
  return undefined
}

function toNumber(value: unknown): number | undefined {
  if (typeof value === 'number') return value
  if (typeof value === 'string' && value !== '' && Number.isFinite(Number(value))) return Number(value)
  return undefined
}

// ── LIKE ────────────────────────────────────────────────────────────
// Patterns compile to a RegExp once per predicate node (cached by node identity).

const likeRegexCache = new WeakMap<Predicate, RegExp>()

function regexFor(node: Extract<Predicate, { kind: 'like' }>, pattern: string): RegExp {
  let regex = likeRegexCache.get(node)
  if (!regex) {
    regex = likeToRegExp(pattern, node.caseInsensitive)
    likeRegexCache.set(node, regex)
  }
  return regex
}

function likeToRegExp(pattern: string, caseInsensitive: boolean): RegExp {
  const escaped = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    .replace(/%/g, '.*')
    .replace(/_/g, '.')
  return new RegExp(`^${escaped}$`, caseInsensitive ? 'is' : 's')
}
