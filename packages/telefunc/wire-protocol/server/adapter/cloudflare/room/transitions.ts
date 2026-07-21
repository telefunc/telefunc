// The NORMATIVE head-transition validation of spi.md §2, realized identically to the memory reference so
// every backend rejects the same set with the same messages (the conformance suite matches on the
// message substrings). Pure: it decides legality from a head SNAPSHOT and a "does this inc still have a
// listed generation" predicate — the CF backend feeds it the SQL `gen` table, the memory backend its Map.

import { type HeadCx, type HeadNext, MAX_CLOSE_LEASE_MS, MIN_CLOSE_LEASE_MS } from '../../../../backend/spi.js'
import { bytesEqual } from './codec.js'

// The stored head as the validators see it (rev/inc/state/config/lease — no physical TTL column).
export type HeadSnapshot = {
  currentInc: string | null
  state: 'open' | 'closing' | 'closed'
  config: Uint8Array
  closeLease?: { id: string; until: number }
}

export type HeadWriteNext = Extract<HeadNext, { head: unknown }>

// Shape rules that hold regardless of what is stored: the lease is present iff the head is closing, its
// duration is finite and bounded, a tombstone is the only head that carries a TTL, and only a tombstone
// has a null incarnation.
export function assertHeadNextWellFormed(next: HeadNext): void {
  if ('delete' in next) return
  const { head, ttlMs } = next
  if (head.state === 'closing') {
    if (head.closeLease === undefined) throw new Error('head CX: a head entering closing must carry a close lease')
    const { durationMs } = head.closeLease
    if (!(durationMs >= MIN_CLOSE_LEASE_MS && durationMs <= MAX_CLOSE_LEASE_MS)) {
      throw new Error(
        `head CX: close lease durationMs ${durationMs} outside [${MIN_CLOSE_LEASE_MS}, ${MAX_CLOSE_LEASE_MS}]`,
      )
    }
  } else if (head.closeLease !== undefined) {
    throw new Error(`head CX: a '${head.state}' head must not carry a close lease`)
  }
  if (ttlMs !== undefined && head.state !== 'closed') {
    throw new Error(`head CX: ttlMs is only valid for a 'closed' tombstone, got '${head.state}'`)
  }
  if (head.state === 'closed' && head.currentInc !== null) {
    throw new Error('head CX: a closed tombstone must clear currentInc to null')
  }
  if (head.state !== 'closed' && head.currentInc === null) {
    throw new Error(`head CX: a '${head.state}' head must name an incarnation`)
  }
}

export type HeadCxForm = 'absent' | 'generic' | 'takeover' | 'finalize'

export function headCxForm(cx: HeadCx): HeadCxForm {
  if (cx.expect === 'absent') return 'absent'
  if ('closingLeaseExpired' in cx.expect) return 'takeover'
  if ('closingLease' in cx.expect) return 'finalize'
  return 'generic'
}

// The tombstone-expiry delete is the one operation whose legality is decided BEFORE any compare, so
// misuse throws even where the compare would have conflicted (spi.md §2 transition table).
export function assertDeleteLegal(next: HeadNext, current: HeadSnapshot | null): void {
  if (!('delete' in next)) return
  if (current?.state !== 'closed') {
    throw new Error(`head CX: {delete} is legal only against a 'closed' tombstone, not '${current?.state ?? 'absent'}'`)
  }
}

// Exhaustive by the triple (current state, HeadCx form, next state). Anything off the table is a
// programming error and THROWS — never a conflict. This is what makes the I13 guards unreachable through
// any other compare form.
export function assertTransitionAllowed(
  cx: HeadCx,
  next: HeadWriteNext,
  current: HeadSnapshot | null,
  hasGeneration: (inc: string) => boolean,
): void {
  const from = current === null ? 'absent' : current.state
  const transition = `${from} + ${headCxForm(cx)} -> ${next.head.state}`
  switch (transition) {
    case 'absent + absent -> open':
    case 'closed + generic -> open':
      assertFreshIncarnation(next.head.currentInc, hasGeneration)
      return
    case 'open + generic -> open':
    case 'open + generic -> closing':
      if (next.head.currentInc !== current?.currentInc) {
        throw new Error(`head CX: ${transition} must keep the same incarnation`)
      }
      return
    case 'closing + takeover -> closing':
      assertReplacesOnlyTheLease(next.head, current as HeadSnapshot)
      return
    case 'closing + finalize -> closed':
      return
    default:
      throw new Error(`head CX: '${transition}' is not a legal head transition`)
  }
}

// FRESH-INC on create/recreate: the incarnation a room is (re)created on must have NO surviving
// generation state (a LISTED generation entry), checked against the very storage listGenerations reads.
// A fully-dropped id is harmless by construction, because no state survives to expose.
export function assertFreshIncarnation(inc: string | null, hasGeneration: (inc: string) => boolean): void {
  if (inc !== null && hasGeneration(inc)) {
    throw new Error(
      `head CX: incarnation '${inc}' still has surviving generation state — creating a room on it would resurrect it`,
    )
  }
}

function assertReplacesOnlyTheLease(next: HeadWriteNext['head'], current: HeadSnapshot): void {
  if (next.currentInc !== current.currentInc) {
    throw new Error('head CX: an expired-close takeover must keep the same incarnation')
  }
  if (!bytesEqual(next.config, current.config)) {
    throw new Error('head CX: an expired-close takeover must not change the config — it replaces only the lease')
  }
  if (next.closeLease?.id === current.closeLease?.id) {
    throw new Error('head CX: an expired-close takeover must mint a different lease id')
  }
}
