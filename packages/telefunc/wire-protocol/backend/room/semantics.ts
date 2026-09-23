export { headCxMatches, commitPreconditionHolds, nextOrderMark }
export type { OrderMark }

import type { HeadCx, LaneId, RoomHead } from './contract.js'

// The authority rules both TypeScript drivers apply; Redis spells the same rules in Lua.

type OrderMark = { seq: number; timestamp: number }

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
function nextOrderMark(previous: OrderMark | undefined, now: number): OrderMark {
  if (previous?.seq === Number.MAX_SAFE_INTEGER) throw new Error('sequence exhausted for the ordering domain')
  return { seq: (previous?.seq ?? 0) + 1, timestamp: Math.max(now, previous?.timestamp ?? 0) }
}
