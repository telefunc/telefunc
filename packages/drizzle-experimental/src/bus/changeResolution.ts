export { createResolvable }
export type { Resolvable }

// A RESOLVABLE is a value a db settles on once and then refers to forever: its change transport, its change
// namespace. Both are configured before use, resolved on first use (to the configuration or to a fallback),
// and FROZEN at that moment — because subscriptions, readiness proofs and published topics all refer to
// whatever was resolved, and changing it underneath them strands them.
//
// Two slots, one machine, because the interesting part is where they DIFFER and that difference is worth
// stating rather than discovering. Each slot declares WHEN it may rotate:
//
//   transport   rotatable while the db is QUIESCENT — what the freeze protects is live subscriptions and
//               the readiness they proved, so once there are none there is nothing left to strand.
//   namespace   NEVER — the identity is what REMOTE instances agree on, and their subscriptions are not
//               observable from here, so no local condition can prove a rotation safe.
//
// Written twice, that asymmetry was an emergent property of two near-identical families of WeakMaps (two
// for one, three for the other) that a reader had to diff to notice. Here it is one argument.

/** When a slot's resolution may still be replaced. Read from live state at each check, never cached. */
type RotationPolicy = (db: object) => boolean

/** Never rotatable — for a value other processes have already agreed on. */
const NEVER: RotationPolicy = () => false

type Resolvable<T> = {
  /** Reject a conflicting value WITHOUT recording anything. Split from `commit` so a caller configuring
   *  several slots can validate them all before installing any — otherwise one slot installs and the next
   *  throws, leaving the db half-configured. */
  check(db: object, value: T | undefined): void
  /** Validate, then record. A rotation that passed the policy clears the old resolution, so the next
   *  `resolve` re-resolves rather than handing back what it froze earlier. */
  commit(db: object, value: T | undefined): void
  /** The resolved value, resolving and FREEZING it on first call: the configured value if there is one,
   *  else the slot's fallback. */
  resolve(db: object, fallback: (db: object) => T): T
}

function createResolvable<T>({
  rotatableWhile = NEVER,
  conflictMessage,
}: { rotatableWhile?: RotationPolicy; conflictMessage: string }): Resolvable<T> {
  const configured = new WeakMap<object, T>() // explicit, registered before first use
  const resolved = new WeakMap<object, T>() // frozen at first use (the configuration OR the fallback)

  /** Nothing to configure. Both slots treated absence this way before they shared a machine, and the union
   *  is deliberate: the transport's guard was falsy-based and the namespace's was `undefined`-only, so an
   *  empty-string namespace is a real value (it configures) while a null transport is not (it is ignored). */
  const absent = (value: T | undefined): boolean => value === undefined || value === null

  function check(db: object, value: T | undefined): void {
    if (absent(value)) return
    if (rotatableWhile(db)) return // nothing this freeze protects is currently alive
    const frozen = resolved.get(db)
    if (frozen !== undefined && frozen !== value) throw new Error(conflictMessage)
    // NOT redundant with the check above. A subscription makes a db non-quiescent SYNCHRONOUSLY (`refs++`,
    // `settling++`) while the `resolve` that settles it runs a microtask later — so there is a real window in
    // which a db is in use and has a value configured but not yet resolved. A rotation admitted in that
    // window would be resolved by the very subscription now being established. Pinned by test.
    const existing = configured.get(db)
    if (existing !== undefined && existing !== value) throw new Error(conflictMessage)
  }

  function commit(db: object, value: T | undefined): void {
    if (absent(value)) return
    check(db, value)
    if (resolved.get(db) === value) return // already resolved to this same value — nothing to record
    resolved.delete(db) // a permitted rotation to a DIFFERENT value: unfreeze so the next resolve re-resolves
    configured.set(db, value as T)
  }

  function resolve(db: object, fallback: (db: object) => T): T {
    let value = resolved.get(db)
    if (value === undefined) {
      value = configured.get(db) ?? fallback(db)
      resolved.set(db, value)
    }
    return value
  }

  return { check, commit, resolve }
}
