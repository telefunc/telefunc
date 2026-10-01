export { headCxMatches, commitPreconditionHolds, isOpenIncarnation, materializeHead, nextOrderMark }
export type { StoredHead }

import type { HeadCx, HeadNext, LaneId, RoomHead } from './contract.js'
import type { OrderingInfo } from '../../ordering-frame.js'

// The authority rules both TypeScript drivers apply; Redis spells the same rules in Lua.

/** A head as an authority stores it; `expiresAt` is when a closed head's tombstone lapses. */
type StoredHead = RoomHead & { expiresAt: number | null }

/** The CX's next head as stored: its expiry and close-lease deadline are minted here, from authority time, and never
 *  supplied by a caller. */
function materializeHead(next: HeadNext, now: number, rev: string): StoredHead {
  const { currentInc, state, config, closeLease } = next.head
  return {
    rev,
    currentInc,
    state,
    config,
    expiresAt: next.ttlMs === undefined ? null : now + next.ttlMs,
    ...(closeLease === undefined ? {} : { closeLease: { id: closeLease.id, until: now + closeLease.durationMs } }),
  }
}

function isOpenIncarnation(head: RoomHead | null, inc: string): boolean {
  return head !== null && head.currentInc === inc && head.state === 'open'
}

function headCxMatches(cx: HeadCx, current: RoomHead | null, now: number): boolean {
  if (cx.form === 'absent') return current === null
  if (current === null || current.rev !== cx.rev) return false
  switch (cx.form) {
    case 'rev':
      return true
    case 'takeover':
      return current.state === 'closing' && current.closeLease !== undefined && current.closeLease.until < now
    case 'finalize':
      return current.state === 'closing' && current.closeLease?.id === cx.lease
  }
}

/** A lane commit needs its incarnation open; only the close's own control commit lands while it is closing. */
function commitPreconditionHolds(
  head: RoomHead | null,
  inc: string,
  laneKind: LaneId['kind'],
  closingLease: string | undefined,
  now: number,
): boolean {
  if (head === null || head.currentInc !== inc) return false
  if (closingLease === undefined) return head.state === 'open'
  return (
    laneKind === 'control' &&
    head.state === 'closing' &&
    head.closeLease?.id === closingLease &&
    now <= head.closeLease.until
  )
}

/** `seq` strictly increases per domain; `timestamp` is authority time, clamped so it never goes back. */
function nextOrderMark(previous: OrderingInfo | undefined, now: number): OrderingInfo {
  return { seq: (previous?.seq ?? 0) + 1, timestamp: Math.max(now, previous?.timestamp ?? 0) }
}
