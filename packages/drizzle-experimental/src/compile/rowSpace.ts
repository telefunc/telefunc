// The row primitive shared by every compile stage. A row flowing through the dataflow
// is a plain record keyed by a *qualified* alias+column key, because evalK reads only
// the bare `column` and a joined row has two `id`s — uniqueness must live in the key.
// This module owns the qualified-key scheme, projection of a captured row into it, the
// predicate requalification that lets evalK evaluate over a joined row, the σ widening
// filter (NULL excludes / MISSING widens), and the canonical row encoding that makes
// consolidate/distinct cancel equal projections and only equal projections.

export { type Row, qualifiedKey, projectRaw, requalify, qualifiedRowView, sigmaMatch, canonicalKeys, rowChanged }

import { evalK } from '../ir/eval.js'
import type { ColRef, Predicate, RowView, ScalarExpr } from '../ir/types.js'
import { frame } from '../utils/canonical.js'
import { canonicalRow } from '../ir/encoding.js'

type Row = Record<string, unknown>

/** Length-framed so `alias`+`column` is injective for any identifiers — a quoted column
 *  containing separators can never collide with a different alias/column split. */
function qualifiedKey(alias: string, column: string): string {
  return frame(alias) + frame(column)
}

/** Project a captured row (keyed by bare db column) into qualified keys for one alias.
 *  `'*'` copies every captured column (a SELECT-* or opaque input demands them all);
 *  a column list copies exactly those. The present/absent distinction is preserved —
 *  an absent key stays MISSING, a present `null` stays a real SQL NULL. */
function projectRaw(alias: string, raw: Row, columns: string[] | '*'): Row {
  const out: Row = {}
  const names = columns === '*' ? Object.keys(raw) : columns
  for (const column of names) {
    if (Object.hasOwn(raw, column)) out[qualifiedKey(alias, column)] = raw[column]
  }
  return out
}

/** Rewrite every column reference to its qualified key so evalK can evaluate the
 *  predicate over a joined row. Structure and operators are untouched (truth-preserving);
 *  only the column name is qualified. */
function requalify(pred: Predicate): Predicate {
  const scalar = (expr: ScalarExpr): ScalarExpr =>
    expr.kind === 'col' ? { kind: 'col', ref: qualifyRef(expr.ref) } : expr
  switch (pred.kind) {
    case 'and':
      return { kind: 'and', parts: pred.parts.map(requalify) }
    case 'or':
      return { kind: 'or', parts: pred.parts.map(requalify) }
    case 'not':
      return { kind: 'not', operand: requalify(pred.operand) }
    case 'compare':
      return { ...pred, left: scalar(pred.left), right: scalar(pred.right) }
    case 'in':
      return { ...pred, expr: scalar(pred.expr), values: pred.values.map(scalar) }
    case 'like':
      return { ...pred, expr: scalar(pred.expr) }
    case 'isNull':
      return { ...pred, expr: scalar(pred.expr) }
    case 'between':
      return { ...pred, expr: scalar(pred.expr), low: scalar(pred.low), high: scalar(pred.high) }
    default:
      return pred // true / false / exists / unknown carry no column operands to qualify
  }
}

function qualifyRef(ref: ColRef): ColRef {
  return { table: ref.table, column: qualifiedKey(ref.table, ref.column) }
}

/** The qualified keys for one alias's columns. */
function canonicalKeys(alias: string, columns: string[]): string[] {
  return columns.map((column) => qualifiedKey(alias, column))
}

function qualifiedRowView(row: Row): RowView {
  return {
    get: (column) => (Object.hasOwn(row, column) ? { present: true, value: row[column] } : { present: false }),
  }
}

/** The σ widening filter used for the *data* stream: a row passes on `true` or
 *  `'missing'` (an uncaptured column / opaque operand widens to a match — a sound
 *  superset), and fails on `false` or `'null'` (SQL NULL excludes, mirroring WHERE). */
function sigmaMatch(pred: Predicate, row: RowView): boolean {
  const q = evalK(pred, row)
  return q === true || q === 'missing'
}

/** Whether a change actually changes anything: an insert (no old) or delete (no new) always does; a
 *  pure-replay update (old canonically equals new) does not. Shared by the compile coarse/stateless
 *  fast-paths and the live graph's per-change witness — one row-equality definition in one place. */
function rowChanged(change: { old?: Row; new?: Row }): boolean {
  if (!change.old || !change.new) return true
  return canonicalRow(change.old) !== canonicalRow(change.new)
}
