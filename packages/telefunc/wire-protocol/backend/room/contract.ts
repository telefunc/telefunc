export type {
  CellMutation,
  CommitAccepted,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  RoomBackend,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
}

import type { BackendReceiver, BackendSubscription, SubscriptionDriver } from '../subscription.js'

/** Fixed channels/order domains: semantic shares RoomOrder; control uses ControlSeq; binary uses
 * per-(member,track) LaneSeq; inbox uses per-member InboxSeq. */
type LaneId =
  | { kind: 'semantic' }
  | { kind: 'control' }
  | { kind: 'binary'; member: string; track: string }
  | { kind: 'inbox'; member: string }

type RoomHead = {
  rev: string
  currentInc: string | null
  state: 'open' | 'closing' | 'closed'
  config: Uint8Array
  /** Closing only. CX mints `until` from authority time and returns the stored head; this lease alone
   * authorizes closing-control commit and finalization. */
  closeLease?: { id: string; until: number }
}

/** Drivers enforce exported `HEAD_TRANSITIONS` for writes and permit deletion only from `closed`. */
type HeadCx =
  | { expect: 'absent' }
  | { expect: { rev: string } }
  | { expect: { rev: string; closingLeaseExpired: true } }
  | { expect: { rev: string; closingLease: string } }

type HeadNext =
  | {
      head: Omit<RoomHead, 'rev' | 'closeLease'> & { closeLease?: { id: string; durationMs: number } }
      ttlMs?: number
    }
  | { delete: true }

type CellMutation = { key: string; set?: { bytes: Uint8Array } }

type CxResult = 'committed' | 'conflict' | 'stale-inc'

type CommitAccepted = {
  accepted: true
  seq: number
  timestamp: number
  receivers?: number
  /** One at-most-once backend-defined handoff: never retries/poisons. */
  delivery: Promise<void>
}

type CommitResult = CommitAccepted | { stale: true }

type RoomSubscriptionSource = {
  roomId: string
  inc: string
  lane: LaneId
}

/** Raw author contract for durable Room storage and subscriptions. */
type RoomDriver = {
  readHead(roomId: string): Promise<{ head: RoomHead } | null>
  compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<{ ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }>
  readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }>
  compareExchangeCells(roomId: string, inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult>
  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitResult>
  readRetained(
    roomId: string,
    inc: string,
    lane: LaneId,
  ): Promise<{ payload: Uint8Array; seq: number; timestamp: number } | null>
  listRetained(roomId: string, inc: string): Promise<LaneId[]>
  deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void>
  dropGeneration(roomId: string, inc: string): Promise<void>
  directoryPut(roomId: string, incTag: string): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void>
  directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }>
  readonly subscriptions: SubscriptionDriver<RoomSubscriptionSource>
}

/** Internal supervised Room consumer contract. */
type RoomBackend = Omit<RoomDriver, 'subscriptions'> & {
  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: BackendReceiver): BackendSubscription
  dispose(): Promise<void>
}
