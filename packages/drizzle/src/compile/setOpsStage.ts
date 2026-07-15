// Set operations over the projected-row streams of the branches. UNION ALL is a concat;
// UNION is a concat then a distinct. INTERSECT / INTERSECT ALL / EXCEPT / EXCEPT ALL are
// not computed exactly — a row's membership in the result depends on BOTH branches at once
// — so they degrade to a live coarse (dirty) plan whose witness fires from a change in
// EITHER branch (§5.4, T4.B7). A branch is a stream of canonical projected strings; the
// caller compiles each branch (its own inputs and projection) and hands them here.

export { type SetOpBranch, applySetOps, isDirtySetOp }

import { type IStreamBuilder, concat, distinct, keyBy, map } from '../graph/ivm.js'
import type { SetOpKind } from '../ir/types.js'
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
  if (branches.some((branch) => isDirtySetOp(branch.kind))) {
    dirty.tap(main)
    for (const branch of branches) dirty.tap(branch.stream)
    return undefined
  }
  let combined = main
  for (const branch of branches) combined = combined.pipe(concat(branch.stream))
  if (branches.some((branch) => branch.kind === 'union')) {
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
