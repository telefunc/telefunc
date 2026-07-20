// Join compilation. Equi-pairs are read from the `[Col,'=',Col]` conjuncts of each ON;
// both sides key by the canonical composite of those columns and db-ivm's keyed join
// (inner/left/right/full) does the retraction-correct matching. Two soundness rules:
// (1) a SQL NULL equi-key never matches — such a row is keyed to a per-content sentinel
// that no other row shares (so it drops from an inner join and null-extends from an
// outer one, exactly like SQL); (2) an OUTER join whose ON carries a non-equi residual,
// or any join with no equi key at all, cannot be a plain keyed-join-plus-filter without
// wrongly dropping null-extended rows — so it degrades to a live coarse (dirty) plan
// rather than silently miss. INNER equi-joins keep their non-equi residual as a widening
// filter with the dirty tap on the candidate pairs.

export { applyJoins, joinsExact }

import { conjunctsOf } from '../extract/predicate.js'
import { type IStreamBuilder, type KeyValue, filter, join as joinOp, keyBy, map } from '../graph/ivm.js'
import type { JoinShape, Predicate, SelectShape } from '../ir/types.js'
import type { DirtySink } from './dirty.js'
import { containsUnknown, dirtyFrontier } from './dirty.js'
import { type Row, canonicalKeys, qualifiedRowView, requalify, sigmaMatch } from './rowSpace.js'
import { conjunction } from '../ir/predicateAlgebra.js'
import { equiKey } from '../ir/encoding.js'

type JoinResult = { kind: 'exact'; stream: IStreamBuilder<Row> } | { kind: 'degrade' }

/** Fold the joins left-deep into one row stream, or report a degradation the caller turns
 *  into a live coarse plan. `crossResidual` is the multi-input WHERE (applied after the
 *  joins, exactly as SQL evaluates WHERE after the join). */
function applyJoins(
  shape: SelectShape,
  streams: Map<string, IStreamBuilder<Row>>,
  crossResidual: Predicate | undefined,
  dirty: DirtySink,
): JoinResult {
  let leftStream = streams.get(shape.from.alias)!
  const leftAliases = new Set<string>([shape.from.alias])
  const residuals: Predicate[] = []

  for (const join of shape.joins) {
    const rightStream = streams.get(join.table.alias)
    if (!rightStream) return { kind: 'degrade' }
    const { equi, residual } = splitOn(join, leftAliases)
    if (equi.length === 0) return { kind: 'degrade' } // no equi key → no cross product, dirty from both σ-inputs
    const outer = join.type !== 'inner'
    if (outer && residual) return { kind: 'degrade' } // outer + non-equi residual → residual-aware or degrade

    const leftKeys = equi.map((pair) => pair.left)
    const rightKeys = equi.map((pair) => pair.right)
    const keyedLeft = leftStream.pipe(keyBy((row: Row) => equiKey(row, leftKeys)))
    const keyedRight = rightStream.pipe(keyBy((row: Row) => equiKey(row, rightKeys)))
    leftStream = keyedLeft.pipe(
      joinOp<string, Row, Row, KeyValue<string, Row>>(keyedRight, modeOf(join.type)),
      map(mergePair),
    )
    if (residual) residuals.push(residual)
    leftAliases.add(join.table.alias)
  }

  if (crossResidual) residuals.push(crossResidual)
  const stream = residuals.length ? applyResidual(leftStream, conjunction(residuals), dirty) : leftStream
  return { kind: 'exact', stream }
}

/** Static check (before building a graph): can every join be an exact keyed join? False
 *  when any join is a cross join, has no equi key, or is an outer join with a non-equi ON
 *  residual (which a plain keyed-join-plus-filter would mishandle). */
function joinsExact(shape: SelectShape): boolean {
  const leftAliases = new Set<string>([shape.from.alias])
  for (const join of shape.joins) {
    if (join.type === 'cross') return false
    const { equi, residual } = splitOn(join, leftAliases)
    if (equi.length === 0) return false
    if (join.type !== 'inner' && residual) return false
    leftAliases.add(join.table.alias)
  }
  return true
}

// ── Residual (non-equi ON + multi-input WHERE) ──────────────────────

/** Filter the joined rows by the residual (unknown leaves widen to a match), tapping the
 *  candidate pairs that pass the exact conjuncts into the dirty witness when the residual
 *  carries an unprovable leaf (A AND unknown → tap after A). */
function applyResidual(stream: IStreamBuilder<Row>, residual: Predicate, dirty: DirtySink): IStreamBuilder<Row> {
  const qualified = requalify(residual)
  if (containsUnknown(residual)) {
    const gate = requalify(dirtyFrontier(residual).gate)
    dirty.tap(stream.pipe(filter((row: Row) => sigmaMatch(gate, qualifiedRowView(row)))))
  }
  return stream.pipe(filter((row: Row) => sigmaMatch(qualified, qualifiedRowView(row))))
}

function modeOf(type: JoinShape['type']): 'inner' | 'left' | 'right' | 'full' {
  return type === 'left' || type === 'right' || type === 'full' ? type : 'inner'
}

function mergePair([, [left, right]]: KeyValue<string, [Row | null, Row | null]>): Row {
  return { ...(left ?? {}), ...(right ?? {}) }
}

type EquiPair = { left: string; right: string }

/** Partition an ON into equi-pairs (`leftAlias.col = rightAlias.col`) and the non-equi
 *  residual. Column keys are qualified so a folded left side spanning several aliases
 *  keys unambiguously. */
function splitOn(join: JoinShape, leftAliases: Set<string>): { equi: EquiPair[]; residual?: Predicate } {
  const equi: EquiPair[] = []
  const rest: Predicate[] = []
  const rightAlias = join.table.alias
  for (const conjunct of join.on ? conjunctsOf(join.on) : []) {
    if (
      conjunct.kind === 'compare' &&
      conjunct.op === '=' &&
      conjunct.left.kind === 'col' &&
      conjunct.right.kind === 'col'
    ) {
      const l = conjunct.left.ref
      const r = conjunct.right.ref
      if (leftAliases.has(l.table) && r.table === rightAlias) {
        equi.push({ left: key(l.table, l.column), right: key(r.table, r.column) })
        continue
      }
      if (leftAliases.has(r.table) && l.table === rightAlias) {
        equi.push({ left: key(r.table, r.column), right: key(l.table, l.column) })
        continue
      }
    }
    rest.push(conjunct)
  }
  return { equi, residual: rest.length ? conjunction(rest) : undefined }
}

function key(alias: string, column: string): string {
  return canonicalKeys(alias, [column])[0]!
}
