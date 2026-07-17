export { evalK, eval3, collapseQuad, rowView, compareValues }

import type { CompareOp, Predicate, Quad, RowView, ScalarExpr, Tri, UnknownCause } from './types.js'

/** Evaluate a predicate against one row in four-valued Kleene logic, keeping the
 *  *cause* of an unknown: `'null'` (a captured SQL NULL — a definite value a WHERE
 *  excludes) vs `'missing'` (a not-captured column or opaque operand — which a
 *  σ-filter must widen to a match). This is the signal the compile layer needs to
 *  apply NULL-excludes / MISSING-widens without re-deriving cause. When two unknown
 *  causes meet, `'missing'` dominates (widening is always sound). `eval3` is the
 *  faithful three-valued collapse used by the NNF equivalence property. */
function evalK(pred: Predicate, row: RowView): Quad {
  switch (pred.kind) {
    case 'true':
      return true
    case 'false':
      return false
    case 'and':
      return andK(pred.parts.map((p) => evalK(p, row)))
    case 'or':
      return orK(pred.parts.map((p) => evalK(p, row)))
    case 'not':
      return notK(evalK(pred.operand, row))
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
      return andK([cmp(value, scalar(pred.low, row), gte), cmp(value, scalar(pred.high, row), lte)])
    }
    case 'exists':
      // Correlated existence is decorrelated into a semi/anti join at compile time
      // and can't be judged from a single captured row; widen.
      return 'missing'
    case 'unknown':
      return 'missing'
  }
}

/** Faithful SQL three-valued logic: `undefined` is the unknown truth value. NNF is a
 *  truth-preserving rewrite here (`eval3(nnf(p)) === eval3(p)`), which is exactly why
 *  the compiler can push negations to the leaves. */
function eval3(pred: Predicate, row: RowView): Tri {
  return collapseQuad(evalK(pred, row))
}

function collapseQuad(q: Quad): Tri {
  return q === true || q === false ? q : undefined
}

/** A `RowView` over a plain record keyed by database column name; an absent key is
 *  a not-captured column (MISSING), a present `null` is a real SQL NULL. Presence is
 *  own-property only — an inherited name like `toString` is MISSING, never a match. */
function rowView(record: Record<string, unknown>): RowView {
  return {
    get: (column) => (Object.hasOwn(record, column) ? { present: true, value: record[column] } : { present: false }),
  }
}

// ── Scalar operands ─────────────────────────────────────────────────

/** A not-captured column or opaque operand, tagged so comparisons yield `'missing'`. */
const MISSING: unique symbol = Symbol('missing-operand')
type Operand = unknown | typeof MISSING

function scalar(expr: ScalarExpr, row: RowView): Operand {
  if (expr.kind === 'col') {
    const cell = row.get(expr.ref.column)
    return cell.present ? cell.value : MISSING
  }
  if (expr.kind === 'lit') return expr.value
  // Placeholders are unknown at row-eval time (the graph resolves them per instance) → widen.
  return MISSING
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

function cmp(a: Operand, b: Operand, f: (order: number) => boolean): Quad {
  if (a === MISSING || b === MISSING) return 'missing'
  if (a === null || b === null) return 'null' // comparison with NULL is unknown-by-null
  const order = compareValues(a, b)
  return order === undefined ? 'missing' : f(order) // incomparable types → widen
}

function evalIn(pred: Extract<Predicate, { kind: 'in' }>, row: RowView): Quad {
  const value = scalar(pred.expr, row)
  if (value === MISSING) return maybeNegate('missing', pred.negated)
  if (value === null) return maybeNegate('null', pred.negated)
  let sawMissing = false
  let sawNull = false
  let matched = false
  for (const candidate of pred.values) {
    const c = scalar(candidate, row)
    if (c === MISSING) sawMissing = true
    else if (c === null) sawNull = true
    else if (compareValues(value, c) === 0) {
      matched = true
      break
    }
  }
  const base: Quad = matched ? true : sawMissing ? 'missing' : sawNull ? 'null' : false
  return maybeNegate(base, pred.negated)
}

function evalLike(pred: Extract<Predicate, { kind: 'like' }>, row: RowView): Quad {
  const value = scalar(pred.expr, row)
  if (value === MISSING) return maybeNegate('missing', pred.negated)
  if (value === null) return maybeNegate('null', pred.negated)
  if (pred.pattern === null) return maybeNegate('missing', pred.negated) // opaque / placeholder pattern
  if (typeof value !== 'string') return maybeNegate('missing', pred.negated)
  const matched: Quad = regexFor(pred, pred.pattern).test(value)
  return maybeNegate(matched, pred.negated)
}

function evalIsNull(pred: Extract<Predicate, { kind: 'isNull' }>, row: RowView): Quad {
  const value = scalar(pred.expr, row)
  if (value === MISSING) return 'missing' // can't tell whether it's null
  const isNull = value === null
  return pred.negated ? !isNull : isNull // NULL is a definite value to `is null`
}

// ── Four-valued Kleene combinators ──────────────────────────────────

function andK(values: Quad[]): Quad {
  let cause: UnknownCause | undefined
  for (const value of values) {
    if (value === false) return false
    if (value !== true) cause = mergeCause(cause, value)
  }
  return cause ?? true
}

function orK(values: Quad[]): Quad {
  let cause: UnknownCause | undefined
  for (const value of values) {
    if (value === true) return true
    if (value !== false) cause = mergeCause(cause, value)
  }
  return cause ?? false
}

function notK(value: Quad): Quad {
  return value === true ? false : value === false ? true : value // cause preserved
}

function maybeNegate(value: Quad, negated: boolean): Quad {
  return negated ? notK(value) : value
}

/** `'missing'` dominates `'null'`: if any uncertainty could widen, the whole result widens. */
function mergeCause(a: UnknownCause | undefined, b: UnknownCause): UnknownCause {
  return a === 'missing' || b === 'missing' ? 'missing' : 'null'
}

// ── Value comparison across producers ───────────────────────────────
// One order spanning how differently the same value arrives: capture hands back the user's own JS
// values, while SQLite stores booleans as 0/1 and dates as strings. Returns undefined for genuinely
// incomparable pairs.

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
// The SQL escape character makes an escaped %/_ a literal; without an escape char
// (sqlite default) %/_ are always wildcards.

const likeRegexCache = new WeakMap<Predicate, RegExp>()

function regexFor(node: Extract<Predicate, { kind: 'like' }>, pattern: string): RegExp {
  let regex = likeRegexCache.get(node)
  if (!regex) {
    regex = likeToRegExp(pattern, node.caseInsensitive, node.escapeChar)
    likeRegexCache.set(node, regex)
  }
  return regex
}

function likeToRegExp(pattern: string, caseInsensitive: boolean, escapeChar: string | null): RegExp {
  let out = ''
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i]!
    if (escapeChar !== null && ch === escapeChar && i + 1 < pattern.length) {
      out += escapeRegexLiteral(pattern[++i]!) // the next char is a literal (e.g. \% → literal %)
    } else if (ch === '%') {
      out += '.*'
    } else if (ch === '_') {
      out += '.'
    } else {
      out += escapeRegexLiteral(ch)
    }
  }
  return new RegExp(`^${out}$`, caseInsensitive ? 'is' : 's')
}

function escapeRegexLiteral(ch: string): string {
  return ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}
