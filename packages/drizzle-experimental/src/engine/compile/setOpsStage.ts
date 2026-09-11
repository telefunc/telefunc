// Set operations over the projected-row streams of the branches. Exact evaluation covers only a
// HOMOGENEOUS chain of the two concat-from-one-branch operators: all UNION ALL (pure concat) or
// all UNION (concat then ONE global distinct — UNION is associative/commutative w.r.t. dedup).
// INTERSECT / EXCEPT (and their ALL variants) need BOTH branches at once, and a MIX of UNION and
// UNION ALL breaks a single global distinct — SQL set ops are LEFT-ASSOCIATIVE, so a duplicate a
// trailing UNION ALL must preserve would be wrongly dropped (a false negative). Both cases degrade
// to a live coarse (dirty) plan whose witness fires from a change in ANY branch. A
// branch is a stream of canonical projected strings; the caller compiles each branch (its own
// inputs and projection) and hands them here.

export { type SetOpBranch, applySetOps, isDirtySetOp }

import { type IStreamBuilder, concat, distinct, keyBy, map } from '../graph/ivm.js'
import type { SetOpKind } from '../../ir/types.js'
import type { DirtySink } from './dirty.js'

type SetOpBranch = { kind: SetOpKind; stream: IStreamBuilder<string> }

/** Combine the main branch with the set-op branches. Returns the combined data stream, or
 *  `undefined` when the operation is a dirty degradation (no exact data — every branch is
 *  tapped so a change in any of them invalidates). */
function applySetOps(
  main: IStreamBuilder<string>,
  branches: SetOpBranch[],
  dirty: DirtySink,
): IStreamBuilder<string> | undefined {
  // Exact only for a HOMOGENEOUS chain of the non-dirty operators (all UNION, or all UNION ALL).
  // INTERSECT/EXCEPT (any) or a MIX of UNION and UNION ALL degrades to dirty: tap every arm.
  const kinds = new Set(branches.map((branch) => branch.kind))
  if (kinds.size !== 1 || isDirtySetOp([...kinds][0]!)) {
    dirty.tap(main)
    for (const branch of branches) dirty.tap(branch.stream)
    return undefined
  }
  let combined = main
  for (const branch of branches) combined = combined.pipe(concat(branch.stream))
  if ([...kinds][0] === 'union') {
    combined = combined.pipe(
      keyBy((value: string) => value),
      distinct(),
      map(([, value]) => value),
    )
  }
  return combined
}

/** INTERSECT / EXCEPT (and their ALL variants) cannot be evaluated as a superset from one
 *  branch and so are dirty degradations. */
function isDirtySetOp(kind: SetOpKind): boolean {
  return kind !== 'union' && kind !== 'unionAll'
}
