export { extractPredicate, parsePredicate, toNNF }
export { isSelectBuilder }

import type { Column, SQL } from 'drizzle-orm'
import type { CompareOp, Dialect, Predicate, ScalarExpr, SqlSource } from '../../ir/types.js'
import { colRefOf, collectTables, relationKeyOf, tableOf } from './columns.js'
import { type SqlToken, isText, splitOn, stripParens, textOf, tokenize } from './sqlChunks.js'

type PredicateResult = { predicate: Predicate; tables: string[]; exact: boolean }

type ParseContext = { tables: Set<string>; exact: boolean; caseInsensitiveLike: boolean; escapeChar: string | null }

/** Parse a drizzle WHERE/ON condition into predicate IR, then push negations to the
 *  leaves (NNF). After NNF every leaf is positive, so a compiler can widen an
 *  unknown leaf to TRUE with one structural rule. `exact` is false when some atom
 *  didn't parse (a raw `sql``, a subquery operand) and had to become an unknown leaf. */
function extractPredicate(condition: SQL | undefined, opts?: { dialect?: Dialect }): PredicateResult {
  const parsed = parsePredicate(condition, opts)
  return { ...parsed, predicate: toNNF(parsed.predicate) }
}

function parsePredicate(condition: SQL | undefined, opts?: { dialect?: Dialect }): PredicateResult {
  if (!condition) return { predicate: { kind: 'true' }, tables: [], exact: true }
  const dialect = opts?.dialect ?? 'pg'
  const ctx: ParseContext = {
    tables: new Set(),
    exact: true,
    caseInsensitiveLike: dialect !== 'pg', // pg LIKE is case-sensitive; sqlite is not
    escapeChar: dialect === 'sqlite' ? null : '\\', // pg LIKE default-escapes with backslash; sqlite has none
  }
  const predicate = parseExpression(tokenize(condition.queryChunks), ctx, condition)
  return { predicate, tables: [...ctx.tables], exact: ctx.exact }
}

// ── Grammar ─────────────────────────────────────────────────────────
// Sub-conditions (the operands of and/or/not) arrive as nested SQL tokens, which
// preserves grouping. `src` is the nearest enclosing SQL, kept on each node so
// hydration can later rebuild a residual WHERE with drizzle's own and(...).

function parseExpression(tokens: SqlToken[], ctx: ParseContext, src: SqlSource): Predicate {
  const inner = stripParens(tokens)

  // Atoms first: `between` carries a literal ` and ` that must not be split on.
  const atom = parseAtom(inner, ctx, src)
  if (atom) return atom

  for (const [separator, combine] of [
    [' or ', mkOr],
    [' and ', mkAnd],
  ] as const) {
    const groups = splitOn(inner, separator)
    if (groups.length > 1) return combine(groups.map((group) => parseExpression(group, ctx, src)))
  }

  if (isText(inner[0], 'not ')) return { kind: 'not', operand: parseExpression(inner.slice(1), ctx, src) }

  const only = inner.length === 1 ? inner[0] : undefined
  if (only?.kind === 'sql') return parseExpression(only.tokens, ctx, only.sql)

  // Unrecognized shape (raw sql``, exotic operators): an unknown leaf that widens.
  return unknownLeaf(inner, ctx, src)
}

const COMPARE_OPS: Record<string, CompareOp> = { '=': '=', '<>': '<>', '>': '>', '>=': '>=', '<': '<', '<=': '<=' }
const LIKE_OPS = new Set(['like', 'not like', 'ilike', 'not ilike'])

function parseAtom(tokens: SqlToken[], ctx: ParseContext, src: SqlSource): Predicate | null {
  // inArray([]) compiles to literal "false", notInArray([]) to "true"
  if (tokens.length === 1 && tokens[0]!.kind === 'text') {
    if (tokens[0]!.text === 'false') return { kind: 'false' }
    if (tokens[0]!.text === 'true') return { kind: 'true' }
    return null
  }

  const head = textOf(tokens[0])
  if (head === 'exists ' || head === 'not exists ') return existsLeaf(tokens, head === 'not exists ', ctx, src)

  // `(col is [not] null)` — drizzle fuses the closing paren into the operator text
  const nullMatch = matchIsNull(tokens)
  if (nullMatch) {
    ctx.tables.add(relationKeyOf(tableOf(nullMatch.column)))
    return { kind: 'isNull', expr: { kind: 'col', ref: colRefOf(nullMatch.column) }, negated: nullMatch.negated, src }
  }

  const first = tokens[0]
  if (first?.kind !== 'column') return null
  const op = textOf(tokens[1])?.trim()
  if (op === undefined) return null
  const leftRef = colRefOf(first.column)
  const left: ScalarExpr = { kind: 'col', ref: leftRef }
  ctx.tables.add(relationKeyOf(tableOf(first.column)))

  const compareOp = COMPARE_OPS[op]
  if (compareOp && tokens.length === 3) {
    const right = readScalar(tokens[2]!, ctx)
    if (!right) return unknownLeaf(tokens, ctx, src) // a scalar-subquery / raw operand — inner recovered by the extractor
    return { kind: 'compare', op: compareOp, left, right, src }
  }

  if ((op === 'in' || op === 'not in') && tokens.length === 3) {
    const operand = tokens[2]!
    if (operand.kind === 'list') {
      const values = operand.items.map((item) => readScalar(item, ctx) ?? ({ kind: 'unknown' } as ScalarExpr))
      return { kind: 'in', expr: left, values, negated: op === 'not in', src }
    }
    if (operand.kind === 'opaque') {
      // col IN (subquery) → a semi/anti-join; carry the subquery + the outer column so the
      // extractor can fill the inner shape and the IN correlation.
      const tables = new Set<string>()
      collectTables(operand.value, tables)
      for (const name of tables) ctx.tables.add(name)
      return { kind: 'exists', negated: op === 'not in', tables: [...tables], src: operand.value, inColumn: leftRef }
    }
    return unknownLeaf(tokens, ctx, src)
  }

  if (LIKE_OPS.has(op) && tokens.length === 3) {
    const patternToken = tokens[2]!
    let pattern: string | null = null
    if (patternToken.kind === 'param' && typeof patternToken.value === 'string') pattern = patternToken.value
    else if (patternToken.kind !== 'placeholder') ctx.exact = false // opaque pattern
    const caseInsensitive = op.includes('ilike') || ctx.caseInsensitiveLike
    return {
      kind: 'like',
      expr: left,
      pattern,
      caseInsensitive,
      escapeChar: ctx.escapeChar,
      negated: op.startsWith('not '),
      src,
    }
  }

  if ((op === 'between' || op === 'not between') && tokens.length === 5 && isText(tokens[3], ' and ')) {
    const low = readScalar(tokens[2]!, ctx)
    const high = readScalar(tokens[4]!, ctx)
    if (!low || !high) return unknownLeaf(tokens, ctx, src)
    const between: Predicate = { kind: 'between', expr: left, low, high, src }
    return op === 'not between' ? { kind: 'not', operand: between } : between
  }

  return null
}

function readScalar(token: SqlToken, ctx: ParseContext): ScalarExpr | null {
  if (token.kind === 'column') {
    ctx.tables.add(relationKeyOf(tableOf(token.column)))
    return { kind: 'col', ref: colRefOf(token.column) }
  }
  if (token.kind === 'param') return { kind: 'lit', value: token.value }
  if (token.kind === 'placeholder') return { kind: 'placeholder', name: token.name }
  return null // sql / list / opaque — not a scalar operand
}

/** `is [not] null`, tolerating drizzle's wrapping parens (`(col is null)`). */
function matchIsNull(tokens: SqlToken[]): { column: Column; negated: boolean } | null {
  let ts = tokens
  if (isText(ts[0], '(')) ts = ts.slice(1)
  if (ts.length !== 2) return null
  const [colToken, opToken] = ts
  if (colToken?.kind !== 'column' || opToken?.kind !== 'text') return null
  const op = opToken.text.replace(/\)+$/, '').trim()
  if (op === 'is null') return { column: colToken.column, negated: false }
  if (op === 'is not null') return { column: colToken.column, negated: true }
  return null
}

function existsLeaf(tokens: SqlToken[], negated: boolean, ctx: ParseContext, src: SqlSource): Predicate {
  const tables = new Set<string>()
  let inner: SqlSource = src
  for (const token of tokens) {
    if (token.kind === 'opaque') inner = token.value
    collectTokenTables(token, tables)
  }
  for (const name of tables) ctx.tables.add(name)
  return { kind: 'exists', negated, tables: [...tables], src: inner }
}

function unknownLeaf(tokens: SqlToken[], ctx: ParseContext, src: SqlSource): Predicate {
  const tables = new Set<string>()
  let subquery: SqlSource | undefined
  for (const token of tokens) {
    collectTokenTables(token, tables)
    if (token.kind === 'opaque' && isSelectBuilder(token.value)) subquery = token.value
  }
  for (const name of tables) ctx.tables.add(name)
  ctx.exact = false
  return { kind: 'unknown', tables: [...tables], src, subquery }
}

/** A drizzle select query builder (has a `toSQL()`), as opposed to a raw SQL fragment. */
function isSelectBuilder(value: unknown): boolean {
  return typeof value === 'object' && value !== null && typeof (value as { toSQL?: unknown }).toSQL === 'function'
}

function collectTokenTables(token: SqlToken, into: Set<string>): void {
  switch (token.kind) {
    case 'column':
      into.add(relationKeyOf(tableOf(token.column)))
      return
    case 'opaque':
      collectTables(token.value, into)
      return
    case 'list':
      for (const item of token.items) collectTokenTables(item, into)
      return
    case 'sql':
      for (const item of token.tokens) collectTokenTables(item, into)
      return
    default:
      return
  }
}

function mkAnd(parts: Predicate[]): Predicate {
  const flat = parts.flatMap((p) => (p.kind === 'and' ? p.parts : [p]))
  return flat.length === 1 ? flat[0]! : { kind: 'and', parts: flat }
}

function mkOr(parts: Predicate[]): Predicate {
  const flat = parts.flatMap((p) => (p.kind === 'or' ? p.parts : [p]))
  return flat.length === 1 ? flat[0]! : { kind: 'or', parts: flat }
}

// ── Negation normal form ────────────────────────────────────────────
// Push every `not` to the leaves via De Morgan and native operator negations
// (eq↔ne, in↔notIn, isNull↔isNotNull, like↔notLike, exists↔antiExists). Unknown
// leaves stay unknown. NNF is truth-preserving in Kleene logic, so `eval3` agrees
// on p and nnf(p).

function toNNF(pred: Predicate): Predicate {
  switch (pred.kind) {
    case 'and':
      return { kind: 'and', parts: pred.parts.map(toNNF) }
    case 'or':
      return { kind: 'or', parts: pred.parts.map(toNNF) }
    case 'not':
      return pushNot(pred.operand)
    default:
      return pred
  }
}

const NEGATED_OP: Record<CompareOp, CompareOp> = { '=': '<>', '<>': '=', '>': '<=', '<=': '>', '<': '>=', '>=': '<' }

function pushNot(pred: Predicate): Predicate {
  switch (pred.kind) {
    case 'true':
      return { kind: 'false' }
    case 'false':
      return { kind: 'true' }
    case 'and':
      return { kind: 'or', parts: pred.parts.map(pushNot) }
    case 'or':
      return { kind: 'and', parts: pred.parts.map(pushNot) }
    case 'not':
      return toNNF(pred.operand) // double negation
    case 'compare':
      return { kind: 'compare', op: NEGATED_OP[pred.op], left: pred.left, right: pred.right, src: pred.src }
    case 'in':
      return { ...pred, negated: !pred.negated }
    case 'like':
      return { ...pred, negated: !pred.negated }
    case 'isNull':
      return { ...pred, negated: !pred.negated }
    case 'between':
      // not(low <= x <= high) = (x < low) or (x > high)
      return {
        kind: 'or',
        parts: [
          { kind: 'compare', op: '<', left: pred.expr, right: pred.low, src: pred.src },
          { kind: 'compare', op: '>', left: pred.expr, right: pred.high, src: pred.src },
        ],
      }
    case 'exists':
      return { ...pred, negated: !pred.negated }
    case 'unknown':
      return pred // not(unknown) is still unknown — it widens to TRUE and is dirty-tapped
  }
}
