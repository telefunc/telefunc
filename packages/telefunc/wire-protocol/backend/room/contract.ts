export type {
  CellMutation,
  CellExpectation,
  CellCount,
  CellCounter,
  CellsCx,
  CellSelector,
  CellsRead,
  CommitOptions,
  DirectoryPage,
  EveryTrackLane,
  RetainedFrame,
  RetainedOf,
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

/** Each lane is its own ordering domain: one semantic, one control, one per member for inbox, and one per
 *  (member, track) for binary. */
type LaneId =
  | { kind: 'semantic' }
  | { kind: 'control' }
  | { kind: 'binary'; member: string; track: string }
  | { kind: 'inbox'; member: string }

/** Subscribe-only: every binary commit of `member`, each with the seq of its own (member, track) lane. */
type EveryTrackLane = { kind: 'binary-all'; member: string }

type RoomHead = {
  rev: string
  currentInc: string | null
  state: 'open' | 'closing' | 'closed'
  config: Uint8Array
  /** Closing only. CX mints `until` from authority time and returns the stored head; this lease alone
   * authorizes closing-control commit and finalization. */
  closeLease?: { id: string; until: number }
  /** Authority time at which the head lapses and reads as absent. */
  expiresAt?: number
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
  /** The head lapses `ttlMs` after this CX; `'keep'` keeps the current head's lapse. */
  ttlMs?: number | 'keep'
}

type HeadCxResult = { head: RoomHead } | { conflict: true; current: RoomHead | null }

/** `bytes: null` deletes the cell. */
type CellMutation = { key: string; bytes: Uint8Array | null }

type CellSelector = { keys: string[] } | { prefix: string }

/** The cells found, each with its version, or the incarnation is no longer the head's. A write gives each cell it
 *  writes a version that cell never had in its incarnation. */
type CellsRead = { cells: Map<string, Uint8Array>; versions: Map<string, string> } | { staleInc: true }

/** A cell still at the version read; `null`: still absent. */
type CellExpectation = { key: string; version: string | null }

/** A counter cell's bytes are this as JSON; `changedAt` is the authority time its value last changed. */
type CellCounter = { value: number; changedAt: number }

/** Adds `delta` to a counter cell. An absent counter stays absent, unless `create` makes it at `delta`. */
type CellCount = { key: string; delta: number; create?: true }

/** Commits only while every expectation holds: applies the mutations and the counts, which never conflict. */
type CellsCx = { expect: CellExpectation[]; mutations: CellMutation[]; counts?: CellCount[] }

/** A commit returns each counted counter as it left it. */
type CxResult = { committed: Map<string, CellCounter> } | 'conflict' | 'stale-inc'

type CommitOptions = { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] }

type RetainedFrame = OrderingInfo & { payload: Uint8Array }

/** One member's binary lanes, or one track's across members. */
type RetainedOf = { member: string } | { track: string }

type DirectoryPage = { entries: { roomId: string; incTag: string }[]; cursor?: string }

type CommitAccepted = {
  accepted: true
  seq: number
  timestamp: number
  receivers?: number
  /** One at-most-once handoff that never retries; a rejection means the handoff was lost. */
  delivery: Promise<void>
}

/** Why a commit was refused: the incarnation isn't open (or its closing lease lapsed), or a required cell is gone. */
type StaleCommit = { stale: 'incarnation' } | { stale: 'cell'; key: string }

type CommitResult = CommitAccepted | StaleCommit

type RoomSubscriptionSource = {
  roomId: string
  inc: string
  lane: LaneId | EveryTrackLane
}

/** What a backend implements for Room: durable heads, cells, lanes and retained frames, and lane subscriptions. */
type RoomDriver = {
  readHead(roomId: string): Promise<RoomHead | null>
  compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult>
  readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead>
  /** The cells under `prefix`, a bounded page at a time: one present for the whole read comes exactly once, one written
   *  or removed during it may or may not. A `staleInc` page is the last. */
  readCellPages(roomId: string, inc: string, prefix: string): AsyncIterable<CellsRead>
  compareExchangeCells(roomId: string, inc: string, cx: CellsCx): Promise<CxResult>
  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult>
  readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null>
  /** The lanes holding a retained frame; with `of`, only that member's or that track's binary lanes, at a cost of
   *  their count. */
  listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]>
  deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void>
  /** Forgets the order of `member`'s inbox and binary lanes; called once no commit to them can land. */
  dropLaneOrders(roomId: string, inc: string, member: string): Promise<void>
  dropGeneration(roomId: string, inc: string): Promise<void>
  /** Lists `roomId` under `incTag`, due at `checkAt` (ms); an entry due at Infinity is never due. */
  directoryPut(roomId: string, incTag: string, checkAt: number): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void>
  directoryList(prefix: string, cursor?: string): Promise<DirectoryPage>
  /** Up to a page of the entries due at `now`, earliest first; an entry stays due until it is put again or deleted. */
  directoryDue(now: number): Promise<DirectoryPage['entries']>
  readonly subscriptions: SubscriptionDriver<RoomSubscriptionSource>
}

/** Internal supervised Room consumer contract. */
type RoomBackend = Omit<RoomDriver, 'subscriptions'> & {
  subscribeLane(
    roomId: string,
    inc: string,
    lane: LaneId | EveryTrackLane,
    receiver: BackendReceiver,
  ): BackendSubscription
  dispose(): Promise<void>
}
