// A build-time guard against STRANDED INPUTS — the one defect class that produces a MISS rather than a
// safe over-fire, and the only class that has now shipped twice.
//
// Every db-backed input is a root stream that `pump` feeds when a change arrives. If the rows it emits
// reach neither the output pipeline nor the dirty witness, the change is swallowed: the graph reports
// `invalidated: false` on a plan that is otherwise PRECISE, and the live query silently keeps stale rows.
// Both historical instances had exactly this shape, and both came from a stage abandoning a stream it had
// already built:
//
//   1. a degraded MAIN branch under a set-op returned before the dirty fallback, leaving every ARM's roots
//      built, registered, fed — and consumed by nothing;
//   2. a join degradation tapped the branch's own streams but returned before `applyExists`, leaving the
//      EXISTS inner roots in the same state.
//
// Fixing those one at a time treats instances of a class. This audits the class: declare every root as it
// is created, link each derived stream to what it was derived FROM, and mark a stream consumed when it is
// piped to the output or handed to the dirty witness. At finalization every declared root must be reachable
// from something consumed — otherwise the build FAILS LOUDLY here, at compile time, instead of going quiet
// at runtime.
//
// Over-firing is sound and needs no audit; this only asks whether a fed input can be heard at all.

export { createInputAudit }
export type { InputAudit }

import type { IStreamBuilder } from '../graph/ivm.js'
import { assertUsage } from '../utils/assert.js'

/** Any stream node, root or derived — identity is all this needs. */
type Node = IStreamBuilder<unknown>

type InputAudit = {
  /** A db-backed root input was created; it must end up reachable from a consumed stream. */
  declare(root: Node, label: string): void
  /** `derived` was built FROM `sources`: consuming it accounts for them. */
  link(derived: Node, sources: Iterable<Node>): void
  /** This stream reaches the output or the dirty witness — account for everything behind it. */
  consume(stream: Node | undefined): void
  /** Throw if any declared root is unaccounted for. Call once, before `graph.finalize()`. */
  assertNoStrandedInputs(): void
}

function createInputAudit(): InputAudit {
  const declared = new Map<Node, string>()
  const sourcesOf = new Map<Node, Node[]>()
  const accounted = new Set<Node>()

  return {
    declare(root, label) {
      declared.set(root, label)
    },
    link(derived, sources) {
      const existing = sourcesOf.get(derived) ?? []
      existing.push(...sources)
      sourcesOf.set(derived, existing)
    },
    consume(stream) {
      if (!stream) return
      // Walk the derivation DAG. `seen` also terminates the cycle a self-referential link would create.
      const seen = new Set<Node>()
      const queue: Node[] = [stream]
      while (queue.length > 0) {
        const node = queue.pop()!
        if (seen.has(node)) continue
        seen.add(node)
        accounted.add(node)
        for (const source of sourcesOf.get(node) ?? []) queue.push(source)
      }
    },
    assertNoStrandedInputs() {
      const stranded = [...declared].filter(([root]) => !accounted.has(root)).map(([, label]) => label)
      assertUsage(
        stranded.length === 0,
        `live-query compile: ${stranded.length} input(s) would be fed but never observed — a change to ${stranded.join(', ')} could not invalidate. This is a compiler bug: every built input must reach the output or the dirty witness.`,
      )
    },
  }
}
