// The σ-scoped shadow: for one stateful input, a PK → pruned-tuple
// index over EXACTLY the σ-matching rows, maintained in lockstep with operator state by the
// same deltas. It is what resolves a key-only retraction without guessing: a hit yields
// the exact old tuple to retract; a miss on COMPLETE state proves the row never σ-matched
// (drop); a miss on INCOMPLETE state cannot prove irrelevance (coarse). This module also owns
// the σ rules the shadow is scoped by — membership, pruning, and the PK key — so "σ-matching"
// means one thing in exactly one place. Shadow rows count toward the per-input state bound.

export { type ShadowIndex, createShadow, matchesResidual, pruneRow, pkOf }

import type { SeedDescriptor } from '../compile/compile.js'
import { type Row, projectRaw, qualifiedRowView, sigmaMatch } from '../compile/rowSpace.js'
import { canonicalValue } from '../../ir/canonical.js'

/** The outcome of resolving a retraction against the shadow. `drop` = provably not σ-matching
 *  on complete state; `coarse` = cannot prove irrelevance (incomplete state). */
type ShadowMatch = { kind: 'hit'; old: Row } | { kind: 'drop' } | { kind: 'coarse' }

type ShadowIndex = {
  put(pk: string, pruned: Row): void
  /** Resolve a retraction by PK: hit → stored tuple (removed); miss → drop when complete, coarse when incomplete. */
  resolve(pk: string): ShadowMatch
  /** Remove a key (no resolution needed — the old row was inline). */
  remove(pk: string): void
  /** The pruned rows to feed the engine as the baseline. */
  rows(): Row[]
  readonly size: number
  complete: boolean
}

function createShadow(): ShadowIndex {
  const byPk = new Map<string, Row>()
  return {
    put(pk, pruned) {
      byPk.set(pk, pruned)
    },
    resolve(pk) {
      const old = byPk.get(pk)
      if (old !== undefined) {
        byPk.delete(pk)
        return { kind: 'hit', old }
      }
      return this.complete ? { kind: 'drop' } : { kind: 'coarse' }
    },
    remove(pk) {
      byPk.delete(pk)
    },
    rows: () => [...byPk.values()],
    get size() {
      return byPk.size
    },
    complete: false,
  }
}

/** Whether a captured row satisfies this input's σ residual — the SAME widening filter the
 *  input adapter applies to the data stream (NULL excludes, MISSING widens), so the shadow
 *  and operator state agree on membership. */
function matchesResidual(descriptor: SeedDescriptor, raw: Row): boolean {
  const qualified = projectRaw(descriptor.alias, raw, descriptor.columns)
  return sigmaMatch(descriptor.residual, qualifiedRowView(qualified))
}

/** The demanded-column (bare-keyed) subset of a captured row — the tuple the shadow stores
 *  and re-emits so a reconstructed retraction projects identically to its original insert. */
function pruneRow(descriptor: SeedDescriptor, raw: Row): Row {
  const names = descriptor.columns === '*' ? Object.keys(raw) : descriptor.columns
  const out: Row = {}
  for (const column of names) if (Object.hasOwn(raw, column)) out[column] = raw[column]
  return out
}

/** The canonical PK key for a row, or `undefined` when a PK column is absent (no PK →
 *  the input cannot shadow-resolve, so its graph is born coarse). */
function pkOf(descriptor: SeedDescriptor, raw: Row): string | undefined {
  if (descriptor.primaryKey.length === 0) return undefined
  const values: unknown[] = []
  for (const column of descriptor.primaryKey) {
    if (!Object.hasOwn(raw, column)) return undefined
    values.push(raw[column])
  }
  return canonicalValue(values)
}
