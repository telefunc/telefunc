export {
  headCxMatches,
  commitPreconditionHolds,
  isOpenIncarnation,
  materializeHead,
  publicHead,
  nextOrderMark,
  countCell,
  encodeCellCounter,
  decodeCellCounter,
}
export type { StoredHead }

import type { CellCount, CellCounter, HeadCx, HeadNext, LaneId, RoomHead } from './contract.js'
import type { OrderingInfo } from '../../ordering-frame.js'

// The authority rules both TypeScript drivers apply; Redis spells the same rules in Lua.

const counterTextEncoder = new TextEncoder()
const counterTextDecoder = new TextDecoder()

/** A head as an authority stores it; `expiresAt` is when it lapses. */
type StoredHead = Omit<RoomHead, 'expiresAt'> & { expiresAt: number | null }

/** The CX's next head as stored: its expiry and close-lease deadline are minted here, from authority time, and never
 *  supplied by a caller. */
function materializeHead(next: HeadNext, now: number, rev: string): StoredHead {
  const { currentInc, state, config, closeLease } = next.head
  const { ttlMs } = next
  const stored: StoredHead = { rev, currentInc, state, config, expiresAt: ttlMs === undefined ? null : now + ttlMs }
  if (closeLease !== undefined) stored.closeLease = { id: closeLease.id, until: now + closeLease.durationMs }
  return stored
}

/** A stored head as a read hands it out, with the `config` bytes the driver hands out. */
function publicHead(head: StoredHead, config: Uint8Array): RoomHead {
  const view: RoomHead = { rev: head.rev, currentInc: head.currentInc, state: head.state, config }
  if (head.closeLease !== undefined) view.closeLease = { id: head.closeLease.id, until: head.closeLease.until }
  if (head.expiresAt !== null) view.expiresAt = head.expiresAt
  return view
}

function isOpenIncarnation<H extends Omit<RoomHead, 'expiresAt'>>(head: H | null, inc: string): head is H {
  return head !== null && head.currentInc === inc && head.state === 'open'
}

function headCxMatches(cx: HeadCx, current: Omit<RoomHead, 'expiresAt'> | null, now: number): boolean {
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
  head: Omit<RoomHead, 'expiresAt'> | null,
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

/** A counter cell after `count` at authority time `now`: `written` when it changed,
 *  null when absent and not created. */
function countCell(
  current: Uint8Array | undefined,
  { delta, create }: CellCount,
  now: number,
): { counter: CellCounter; written: boolean } | null {
  if (current === undefined) return create ? { counter: { value: delta, changedAt: now }, written: true } : null
  const counter = decodeCellCounter(current)
  return delta === 0
    ? { counter, written: false }
    : { counter: { value: counter.value + delta, changedAt: now }, written: true }
}

function encodeCellCounter(counter: CellCounter): Uint8Array {
  return counterTextEncoder.encode(JSON.stringify(counter))
}

function decodeCellCounter(bytes: Uint8Array): CellCounter {
  return JSON.parse(counterTextDecoder.decode(bytes)) as CellCounter
}
