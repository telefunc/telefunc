// The single authority for the dirty witness (T4.G6). Two responsibilities live here and
// nowhere else: (1) deciding, from a predicate, WHERE the dirty tap sits — the exact
// necessary filter above the relaxation (the §5.4 composition rule) — and (2) collecting
// every dirty stream into one terminal that fires when anything flows, guaranteeing the
// BYPASS: dirty reaches the terminal through concat only, never a distinct/consolidate/
// filter that could cancel a witnessed change. Over-firing is sound; a miss never is.

export { createDirtySink, dirtyFrontier, containsUnknown }
export type { DirtySink, DirtyFrontier }

import { conjunctsOf } from '../extract/predicate.js'
import { type IStreamBuilder, concat, map, output } from '../graph/ivm.js'
import type { Predicate } from '../ir/types.js'

/** A constant marker: the terminal only asks "did anything flow", so the payload is
 *  irrelevant — but the dirty path is never consolidated, so markers never cancel. */
const DIRTY = 1

type DirtyFrontier = {
  /** The predicate has an unprovable leaf, so a dirty witness is required. */
  active: boolean
  /** The exact necessary filter above the tap: dirty fires only for rows that satisfy
   *  it. `A AND unknown` yields gate `A` (tap after A); `A OR unknown` / `NOT unknown`
   *  yields gate `true` (tap before — fire on any change). */
  gate: Predicate
}

/** Decompose a predicate into its dirty tap frontier over the top-level AND conjuncts.
 *  The fully-exact conjuncts form the gate (a sound necessary filter); the presence of
 *  any conjunct carrying an unprovable leaf makes the witness active. */
function dirtyFrontier(pred: Predicate): DirtyFrontier {
  const conjuncts = conjunctsOf(pred)
  const exact = conjuncts.filter((conjunct) => !containsUnknown(conjunct))
  return { active: exact.length !== conjuncts.length, gate: conjunction(exact) }
}

function conjunction(parts: Predicate[]): Predicate {
  if (parts.length === 0) return { kind: 'true' }
  if (parts.length === 1) return parts[0]!
  return { kind: 'and', parts }
}

/** Whether a predicate tree carries a leaf the compiler cannot prove in the data stream —
 *  an opaque `unknown` leaf or a decorrelated `exists` (a semi/anti join whose membership
 *  a single captured row cannot decide). */
function containsUnknown(pred: Predicate): boolean {
  switch (pred.kind) {
    case 'unknown':
    case 'exists':
      return true
    case 'and':
    case 'or':
      return pred.parts.some(containsUnknown)
    case 'not':
      return containsUnknown(pred.operand)
    default:
      return false
  }
}

type DirtySink = {
  /** A direct dirty fire — the row-space taps (stateless σ) and the event tier. */
  signal(): void
  /** A dataflow tap: route a stream's rows into the witness as markers (no filter — the
   *  bypass guarantee). Firing when the stream is non-empty is the tap semantics. */
  tap(stream: IStreamBuilder<unknown>): void
  /** Wire the collected taps into the terminal; call once before `graph.finalize()`. */
  buildTerminal(): void
  fired(): boolean
  reset(): void
}

function createDirtySink(): DirtySink {
  let fired = false
  const taps: IStreamBuilder<number>[] = []
  return {
    signal() {
      fired = true
    },
    tap(stream) {
      taps.push(stream.pipe(map(() => DIRTY)))
    },
    buildTerminal() {
      if (taps.length === 0) return
      let merged = taps[0]!
      for (let i = 1; i < taps.length; i++) merged = merged.pipe(concat(taps[i]!))
      merged.pipe(
        output((data) => {
          if (data.getInner().length > 0) fired = true
        }),
      )
    },
    fired: () => fired,
    reset() {
      fired = false
    },
  }
}
