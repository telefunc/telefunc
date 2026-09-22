export { conjunction, conjunctsOf }

import type { Predicate } from './types.js'

// Predicate construction, in one place. The compiler builds AND-trees in four situations — the dirty
// witness's exact gate, a join's residual, a stateful input's leftover filter — and each had written the
// same three lines for itself.

/** The predicate that holds exactly when every part does.
 *
 *  An EMPTY conjunction is `true`: nothing is required, so nothing can fail. Two of the four call sites
 *  guard against empty before calling and never see it; the dirty witness's gate does not guard, and `true`
 *  is what it means there (no conjunct was inexact, so the gate lets everything through). Collapsing a
 *  single part rather than wrapping it keeps the tree shallow, which is what the stage compilers read. */
function conjunction(parts: Predicate[]): Predicate {
  if (parts.length === 0) return { kind: 'true' }
  if (parts.length === 1) return parts[0]!
  return { kind: 'and', parts }
}

/** The top-level AND conjuncts, each of which can be pushed to the single input it references. */
function conjunctsOf(predicate: Predicate): Predicate[] {
  return predicate.kind === 'and' ? predicate.parts : [predicate]
}
