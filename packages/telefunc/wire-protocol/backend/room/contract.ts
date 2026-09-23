export type {
  CellMutation,
  CellSelector,
  CellsRead,
  CommitOptions,
  DirectoryPage,
  RetainedFrame,
  CommitAccepted,
  CommitResult,
  StaleCommit,
  CxResult,
  HeadCx,
  HeadCxResult,
  HeadNext,
  LaneId,
  RoomBackend,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
}

import type { BackendReceiver, BackendSubscription, SubscriptionDriver } from '../subscription.js'
import type { OrderingInfo } from '../../ordering-frame.js'

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

/** Drivers compare-exchange; core is the only writer and decides every transition. */
type HeadCx =
  | { form: 'absent' }
  | { form: 'rev'; rev: string }
  /** At `rev`, closing, and the close lease has lapsed. */
  | { form: 'takeover'; rev: string }
  /** At `rev`, closing under `lease`. */
  | { form: 'finalize'; rev: string; lease: string }

type HeadNext = {
  head: {
    currentInc: string | null
    state: RoomHead['state']
    config: Uint8Array
    closeLease?: { id: string; durationMs: number }
  }
  ttlMs?: number
}

type HeadCxResult = { head: RoomHead } | { conflict: true; current: RoomHead | null }

/** `bytes: null` deletes the cell. */
type CellMutation = { key: string; bytes: Uint8Array | null }

type CellSelector = { keys: string[] } | { prefix: string }

/** Cells at one revision, or the incarnation is no longer the head's. */
type CellsRead = { revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }

type CxResult = 'committed' | 'conflict' | 'stale-inc'

type CommitOptions = { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] }

type RetainedFrame = OrderingInfo & { payload: Uint8Array }

type DirectoryPage = { entries: { roomId: string; incTag: string }[]; cursor?: string }

type CommitAccepted = {
  accepted: true
  seq: number
  timestamp: number
  receivers?: number
  /** One at-most-once backend-defined handoff: never retries/poisons. */
  delivery: Promise<void>
}

/** Why a commit was refused: the incarnation isn't open (or its closing lease lapsed), or a required cell is gone. */
type StaleCommit = { stale: 'incarnation' } | { stale: 'cell'; key: string }

type CommitResult = CommitAccepted | StaleCommit

type RoomSubscriptionSource = {
  roomId: string
  inc: string
  lane: LaneId
}

/** What a backend implements for Room: durable heads, cells, lanes and retained frames, and lane subscriptions. */
type RoomDriver = {
  readHead(roomId: string): Promise<RoomHead | null>
  compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult>
  readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead>
  compareExchangeCells(roomId: string, inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult>
  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult>
  readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null>
  listRetained(roomId: string, inc: string): Promise<LaneId[]>
  deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void>
  dropGeneration(roomId: string, inc: string): Promise<void>
  directoryPut(roomId: string, incTag: string): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void>
  directoryList(prefix: string, cursor?: string): Promise<DirectoryPage>
  readonly subscriptions: SubscriptionDriver<RoomSubscriptionSource>
}

/** Internal supervised Room consumer contract. */
type RoomBackend = Omit<RoomDriver, 'subscriptions'> & {
  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: BackendReceiver): BackendSubscription
  dispose(): Promise<void>
}
