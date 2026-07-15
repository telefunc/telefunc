// σ-pushdown and the per-input adapter (final-plan §5.2). Top-level AND conjuncts of
// WHERE that reference exactly one input push down to that input's residual and are
// evaluated in ROW SPACE before the dataflow — both old and new tuples visible, so an
// update crossing the σ boundary resolves exactly. Column pruning demands only what a
// predicate, join key, projection, grouping or ordering reads (plus the PK for
// retraction, plus every opaque-contributing column), and SELECT-* / opaque inputs widen
// to all columns. `applyChange` is the adapter: a captured change becomes a pruned,
// qualified data delta (no-op updates fold away) and a per-change dirty decision.

export { type InputPlan, type Change, type InputDelta, pushdownOf, applyChange }

import { conjunctsOf } from '../extract/predicate.js'
import type { ColRef, Predicate, ScalarExpr, SelectShape, TableRef } from '../ir/types.js'
import { containsUnknown, dirtyFrontier } from './dirty.js'
import { type Row, projectRaw, qualifiedRowView, requalify, rowString, sigmaMatch } from './rowSpace.js'

type InputPlan = {
  alias: string
  table: string
  primaryKey: string[]
  /** The demanded db columns (pruned), or `'*'` when the whole row is observable
   *  (SELECT *) so any column change must be seen. */
  columns: string[] | '*'
  /** The σ conjuncts pushed to this input, requalified — the data widening filter. */
  residual: Predicate
  /** The exact necessary filter above this input's dirty tap, requalified. */
  gate: Predicate
  /** This input carries an unprovable leaf (opaque predicate/projection/order), so a
   *  dirty witness is tapped here. */
  dirtyActive: boolean
  /** A stateful operator downstream will keep pre-existing rows for this input — the
   *  ticket-5 shadow signal. */
  shadowNeed: boolean
}

type Change = { table: string; kind: 'insert' | 'update' | 'delete'; old?: Row; new?: Row }
type InputDelta = { data: Array<[Row, number]>; dirty: boolean }

/** Build the per-alias input plans (FROM + every JOIN) and the residual WHERE conjuncts
 *  that reference more than one input (the join/where stage owns those). */
function pushdownOf(shape: SelectShape): { inputs: InputPlan[]; crossResidual?: Predicate } {
  const refs = [shape.from, ...shape.joins.map((join) => join.table)]
  const realToAliases = new Map<string, string[]>()
  for (const ref of refs) {
    const list = realToAliases.get(ref.name) ?? []
    list.push(ref.alias)
    realToAliases.set(ref.name, list)
  }

  const demand = demandOf(shape)
  const opaque = opaqueAliases(shape, realToAliases)
  const stateful = isStateful(shape)

  const perAlias = new Map<string, Predicate[]>()
  const cross: Predicate[] = []
  for (const conjunct of shape.where ? conjunctsOf(shape.where) : []) {
    const aliases = predicateAliases(conjunct, realToAliases)
    if (aliases.size <= 1) {
      const alias = aliases.size === 1 ? [...aliases][0]! : shape.from.alias
      const list = perAlias.get(alias) ?? []
      list.push(conjunct)
      perAlias.set(alias, list)
    } else {
      cross.push(conjunct)
    }
  }

  const inputs = refs.map((ref) => {
    const conjuncts = perAlias.get(ref.alias) ?? []
    const residual: Predicate = conjuncts.length ? { kind: 'and', parts: conjuncts } : { kind: 'true' }
    const frontier = dirtyFrontier(residual)
    const star = shape.projection.star
    const columns: string[] | '*' = star ? '*' : [...(demand.get(ref.alias) ?? new Set<string>())]
    return {
      alias: ref.alias,
      table: ref.name,
      primaryKey: ref.primaryKey,
      columns,
      residual: requalify(residual),
      gate: requalify(frontier.gate),
      dirtyActive: frontier.active || opaque.has(ref.alias),
      shadowNeed: stateful,
    }
  })
  return { inputs, crossResidual: cross.length ? { kind: 'and', parts: cross } : undefined }
}

/** The §5.2 input adapter: a captured change → a pruned, qualified data delta and a
 *  dirty decision. Row-space σ decides membership on each side; identical pruned
 *  projections of an update fold away (a no-op update never invalidates); a dirty input
 *  fires when the full captured row actually changed and satisfies the tap gate. */
function applyChange(plan: InputPlan, change: Change): InputDelta {
  const oldQ = change.old ? projectRaw(plan.alias, change.old, plan.columns) : undefined
  const newQ = change.new ? projectRaw(plan.alias, change.new, plan.columns) : undefined
  const oldIn = oldQ ? sigmaMatch(plan.residual, qualifiedRowView(oldQ)) : false
  const newIn = newQ ? sigmaMatch(plan.residual, qualifiedRowView(newQ)) : false

  const data: Array<[Row, number]> = []
  const dataKeys = [...new Set([...(oldQ ? Object.keys(oldQ) : []), ...(newQ ? Object.keys(newQ) : [])])]
  const noopData = oldIn && newIn && !!oldQ && !!newQ && rowString(oldQ, dataKeys) === rowString(newQ, dataKeys)
  if (!noopData) {
    if (oldIn && oldQ) data.push([oldQ, -1])
    if (newIn && newQ) data.push([newQ, 1])
  }

  return { data, dirty: dirtyDecision(plan, change) }
}

/** A dirty input fires when the captured row is not a pure replay and the tap gate admits
 *  it — evaluated over the FULL row, because an opaque leaf may read a column the demand
 *  didn't prune. Over-firing here is a sound licensed degradation; a miss is not. */
function dirtyDecision(plan: InputPlan, change: Change): boolean {
  if (!plan.dirtyActive) return false
  const oldFull = change.old ? projectRaw(plan.alias, change.old, '*') : undefined
  const newFull = change.new ? projectRaw(plan.alias, change.new, '*') : undefined
  const allKeys = [...new Set([...(oldFull ? Object.keys(oldFull) : []), ...(newFull ? Object.keys(newFull) : [])])]
  const replay = !!oldFull && !!newFull && rowString(oldFull, allKeys) === rowString(newFull, allKeys)
  if (replay) return false
  const gateOld = oldFull ? sigmaMatch(plan.gate, qualifiedRowView(oldFull)) : false
  const gateNew = newFull ? sigmaMatch(plan.gate, qualifiedRowView(newFull)) : false
  return gateOld || gateNew
}

// ── Demand + alias analysis ─────────────────────────────────────────

/** Per-alias demanded db columns: PK (retraction) + every column a predicate, join key,
 *  projection item, grouping or ordering reads. */
function demandOf(shape: SelectShape): Map<string, Set<string>> {
  const demand = new Map<string, Set<string>>()
  const want = (ref: ColRef) => {
    let set = demand.get(ref.table)
    if (!set) demand.set(ref.table, (set = new Set()))
    set.add(ref.column)
  }
  const seed = (ref: TableRef) => {
    if (!demand.has(ref.alias)) demand.set(ref.alias, new Set())
    for (const pk of ref.primaryKey) want({ table: ref.alias, column: pk })
  }
  seed(shape.from)
  for (const join of shape.joins) seed(join.table)

  if (shape.where) eachCol(shape.where, want)
  for (const join of shape.joins) if (join.on) eachCol(join.on, want)
  if (shape.having) eachCol(shape.having, want)
  for (const item of shape.projection.items) {
    if (item.kind === 'col') want(item.ref)
    else if (item.kind === 'agg') {
      if (item.call.arg) want(item.call.arg)
    } else for (const ref of item.columns) want(ref)
  }
  for (const ref of shape.groupBy) want(ref)
  for (const key of shape.orderBy) {
    if (key.expr.kind === 'col') want(key.expr.ref)
    else for (const ref of key.expr.columns) want(ref)
  }
  return demand
}

/** Aliases whose contribution the compiler cannot evaluate exactly — an opaque
 *  projection expression, a window function, or an opaque ORDER key. Their inputs fire a
 *  dirty witness on any change past the WHERE gate. */
function opaqueAliases(shape: SelectShape, realToAliases: Map<string, string[]>): Set<string> {
  const set = new Set<string>()
  const addReal = (real: string) => {
    for (const alias of realToAliases.get(real) ?? []) set.add(alias)
  }
  for (const item of shape.projection.items) {
    if (item.kind !== 'opaque') continue
    for (const ref of item.columns) set.add(ref.table)
    for (const real of item.tables) addReal(real)
  }
  for (const key of shape.orderBy) {
    if (key.expr.kind === 'opaque') for (const ref of key.expr.columns) set.add(ref.table)
  }
  if (shape.window) for (const ref of [shape.from, ...shape.joins.map((j) => j.table)]) set.add(ref.alias)
  return set
}

/** The aliases a conjunct references — column refs by their own alias, plus every alias
 *  of a real table named in an opaque/exists leaf (which carries real names, not refs). */
function predicateAliases(pred: Predicate, realToAliases: Map<string, string[]>): Set<string> {
  const set = new Set<string>()
  eachCol(pred, (ref) => set.add(ref.table))
  eachLeafTable(pred, (real) => {
    for (const alias of realToAliases.get(real) ?? []) set.add(alias)
  })
  return set
}

function eachCol(pred: Predicate, visit: (ref: ColRef) => void): void {
  const scalar = (expr: ScalarExpr) => {
    if (expr.kind === 'col') visit(expr.ref)
  }
  switch (pred.kind) {
    case 'and':
    case 'or':
      for (const part of pred.parts) eachCol(part, visit)
      return
    case 'not':
      eachCol(pred.operand, visit)
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

function eachLeafTable(pred: Predicate, visit: (real: string) => void): void {
  switch (pred.kind) {
    case 'and':
    case 'or':
      for (const part of pred.parts) eachLeafTable(part, visit)
      return
    case 'not':
      eachLeafTable(pred.operand, visit)
      return
    case 'exists':
    case 'unknown':
      for (const real of pred.tables) visit(real)
      return
    default:
      return
  }
}

function isStateful(shape: SelectShape): boolean {
  return (
    shape.joins.length > 0 ||
    shape.groupBy.length > 0 ||
    shape.groupByOpaque ||
    shape.projection.items.some((item) => item.kind === 'agg') ||
    shape.distinct.on !== false ||
    shape.setOps.length > 0 ||
    (shape.limit !== undefined && shape.orderBy.length > 0) ||
    (shape.where ? containsUnknown(shape.where) && hasExistsLeaf(shape.where) : false)
  )
}

function hasExistsLeaf(pred: Predicate): boolean {
  switch (pred.kind) {
    case 'exists':
      return true
    case 'and':
    case 'or':
      return pred.parts.some(hasExistsLeaf)
    case 'not':
      return hasExistsLeaf(pred.operand)
    default:
      return false
  }
}
