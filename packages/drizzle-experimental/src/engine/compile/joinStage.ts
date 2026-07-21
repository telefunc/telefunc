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

import { conjunctsOf } from '../../drizzle/extract/predicate.js'
import { type IStreamBuilder, type KeyValue, filter, join as joinOp, keyBy, map } from '../graph/ivm.js'
import type { JoinShape, Predicate, SelectShape } from '../../ir/types.js'
import type { DirtySink } from './dirty.js'
import { containsUnknown, dirtyFrontier } from './dirty.js'
import { type Row, canonicalKeys, qualifiedRowView, requalify, sigmaMatch } from './rowSpace.js'
import { conjunction } from '../../ir/predicateAlgebra.js'
import { equiKey } from '../../ir/encoding.js'

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
  const plans = classifyJoins(shape)
  if (!plans) return { kind: 'degrade' }

  let leftStream = streams.get(shape.from.alias)!
  const residuals: Predicate[] = []

  for (const { join, equi, residual } of plans) {
    const rightStream = streams.get(join.table.alias)
    if (!rightStream) return { kind: 'degrade' }

    const leftKeys = equi.map((pair) => pair.left)
    const rightKeys = equi.map((pair) => pair.right)
    const keyedLeft = leftStream.pipe(keyBy((row: Row) => equiKey(row, leftKeys)))
    const keyedRight = rightStream.pipe(keyBy((row: Row) => equiKey(row, rightKeys)))
    leftStream = keyedLeft.pipe(
      joinOp<string, Row, Row, KeyValue<string, Row>>(keyedRight, modeOf(join.type)),
      map(mergePair),
    )
    if (residual) residuals.push(residual)
  }

  if (crossResidual) residuals.push(crossResidual)
  const stream = residuals.length ? applyResidual(leftStream, conjunction(residuals), dirty) : leftStream
  return { kind: 'exact', stream }
}

/** Static check (before building a graph): can every join be an exact keyed join? */
function joinsExact(shape: SelectShape): boolean {
  return classifyJoins(shape) !== undefined
}

/** One join, judged and decomposed: the equi-pairs it keys on and the non-equi ON residual left over. */
type JoinPlan = { join: JoinShape; equi: EquiPair[]; residual?: Predicate }

/** THE ONE PLACE A JOIN IS JUDGED EXACT-OR-NOT, and the reason both the static check and the builder can
 *  be trusted to agree: they are the same walk. Encoded twice, they drifted in spelling (the static side
 *  named `cross` explicitly, the builder let it fall out of an empty equi set) and a rule added to one
 *  would silently not reach the other — a classifier that promises precision over a builder that degrades
 *  is a missed invalidation, which is the one failure class here that is not a safe over-fire.
 *
 *  Left-deep, so each join is judged against the aliases folded in so far. `undefined` at the first join
 *  that cannot be an exact keyed join: a cross join or any join with no equi key (no key, no keyed join —
 *  the cross product would have to come from somewhere), or an OUTER join carrying a non-equi ON residual,
 *  which a keyed-join-plus-filter would mishandle by dropping the null-extended rows the filter rejects. */
function classifyJoins(shape: SelectShape): JoinPlan[] | undefined {
  const leftAliases = new Set<string>([shape.from.alias])
  const plans: JoinPlan[] = []
  for (const join of shape.joins) {
    // A CROSS join is rejected on its type, not left to fall out of an empty equi set. The two are the
    // same for anything the extractor builds (drizzle's crossJoin takes no ON), but `JoinShape` permits
    // a cross carrying one, and reading that as a join key would compile a cross product as an inner
    // join — narrower than SQL, so a miss rather than an over-fire.
    if (join.type === 'cross') return undefined
    const { equi, residual } = splitOn(join, leftAliases)
    if (equi.length === 0) return undefined
    if (join.type !== 'inner' && residual) return undefined
    plans.push({ join, equi, residual })
    leftAliases.add(join.table.alias)
  }
  return plans
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
