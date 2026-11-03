export { CELL_PAGE_SIZE, DIRECTORY_PAGE_SIZE }
export type {
  CellMutation,
  CellExpectation,
  CellCount,
  CellCounter,
  CellsExchange,
  CellsExchangeResult,
  CellSelector,
  CellsRead,
  ExpiringCell,
  ExpiringCellsRead,
  CommitOptions,
  CommitTarget,
  DirectoryPage,
  EveryMemberLane,
  EveryTrackLane,
  RetainedFrame,
  RetainedOf,
  CommitAccepted,
  CommitResult,
  StaleCommit,
  StaleIncarnation,
  HeadExpectation,
  HeadExchangeResult,
  HeadNext,
  LaneId,
  OperationResult,
  OperationTx,
  RoomDriver,
  RoomOperation,
  RoomHead,
  RoomPlaneKey,
  RoomPlaneKit,
  RoomSubscriptionSource,
}

import type { BackendReceiver, DriverAttempt, OrderingInfo, PlaneKey, SubscriptionDriver } from 'telefunc/__internal'
import type { recordOperation } from './operation.js'

/** The cells a prefix read's page holds. */
const CELL_PAGE_SIZE = 1024
/** The entries a due read's page holds, and a directory list's per region: a Cloudflare page merges each region's. */
const DIRECTORY_PAGE_SIZE = 100

/** The key a backend serves Room's plane by:
 *  `servePlane(planeKey<RoomPlaneKey>('telefunc.room.plane'), (kit) => …)`. */
type RoomPlaneKey = PlaneKey<'telefunc.room.plane', RoomPlaneKit, RoomDriver>

/** Room's lane keys, page sizes and operation recorder, which `backendPlane()` hands a backend's Room driver: a backend
 *  package imports Room for types only, as a Broadcast-only app doesn't install it. */
type RoomPlaneKit = {
  encodeLaneKey(lane: LaneId | EveryTrackLane | EveryMemberLane): string
  decodeLaneKey(key: string): LaneId
  everyMemberLaneKey(track: string): string
  everyTrackLaneKey(member: string): string
  CELL_PAGE_SIZE: number
  DIRECTORY_PAGE_SIZE: number
  recordOperation: typeof recordOperation
  OPERATION_COMMIT_LANE: LaneId
}

/** Each lane is its own ordering domain: one semantic, one control, one demand, one per member for inbox, and one per
 *  (member, track) for binary. */
type LaneId =
  | { kind: 'semantic' }
  | { kind: 'control' }
  | { kind: 'demand' }
  | { kind: 'binary'; member: string; track: string }
  | { kind: 'inbox'; member: string }

/** Subscribe-only: every binary commit of `member`, each with the seq of its own (member, track) lane. */
type EveryTrackLane = { kind: 'binary-all'; member: string }

/** Subscribe-only: every binary commit on `track`, of any member, each with the seq of its own (member, track) lane. */
type EveryMemberLane = { kind: 'binary-track'; track: string }

type RoomHead = {
  rev: string
  currentInc: string | null
  state: 'open' | 'closing' | 'closed'
  config: Uint8Array
  /** Closing only. The head exchange mints `until` from authority time and returns the stored head; this lease alone
   * authorizes closing-control commit and finalization. */
  closeLease?: { id: string; until: number }
  /** Authority time at which the head lapses and reads as absent. */
  expiresAt?: number
}

/** What a head exchange expects the stored head to be. Drivers compare-exchange; core is the only writer and decides
 *  every transition. */
type HeadExpectation =
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
  /** The head lapses `ttlMs` after this exchange. */
  ttlMs?: number
  /** Counted in the head's `currentInc` within the exchange: a create counts its incarnation's hold counters with its
   *  head. */
  counts?: CellCount[]
}

/** The incarnation a call names is no longer the head's open one: Room's one refusal for a room that closed or was
 *  recreated. */
type StaleIncarnation = { kind: 'stale-incarnation' }

type HeadExchangeResult = { kind: 'committed'; head: RoomHead } | { kind: 'conflict'; current: RoomHead | null }

/** `bytes: null` deletes the cell. */
type CellMutation = { key: string; bytes: Uint8Array | null }

type CellSelector = { keys: string[] } | { prefix: string }

/** The cells found, each with its version, and the authority time of the read; or the incarnation is no longer the
 *  head's. A write gives each cell it writes a version that cell never had in its incarnation. */
type CellsRead =
  | { kind: 'read'; cells: Map<string, Uint8Array>; versions: Map<string, string>; now: number }
  | StaleIncarnation

/** A cell still at the version read; `null`: still absent. */
type CellExpectation = { key: string; version: string | null }

/** A counter cell's bytes are this as JSON; `changedAt` is the authority time its value last changed. */
type CellCounter = { value: number; changedAt: number }

/** Adds `delta` to a counter cell. An absent counter stays absent, unless `create` makes it at `delta`. */
type CellCount = { key: string; delta: number; create?: true }

/** Commits only while every expectation holds: applies the mutations and the counts, which never conflict. */
type CellsExchange = { expect: CellExpectation[]; mutations: CellMutation[]; counts?: CellCount[] }

/** A committed exchange returns each counted counter as it left it. */
type CellsExchangeResult =
  | { kind: 'committed'; counters: Map<string, CellCounter> }
  | { kind: 'conflict' }
  | StaleIncarnation

/** `owner`: with `retain`, the member the frame is from; `dropMemberLanes()` of that member drops it. */
type CommitOptions = { retain?: boolean; owner?: string; closingLease?: string; requiredCellKeys?: string[] }

type RetainedFrame = OrderingInfo & { payload: Uint8Array }

/** One member's binary lanes, or one track's across members. */
type RetainedOf = { member: string } | { track: string }

type DirectoryPage = { entries: { roomId: string; incTag: string }[]; cursor?: string }

type CommitAccepted = {
  kind: 'accepted'
  seq: number
  timestamp: number
  /** An inbox or control commit's one at-most-once handoff that never retries; a rejection means the handoff was
   *  lost. Other commits have none. */
  delivery?: Promise<void>
}

/** Why a commit was refused: the incarnation isn't open (or its closing lease lapsed), or a required cell is gone. */
type StaleCommit = StaleIncarnation | { kind: 'stale-cell'; key: string }

type CommitResult = CommitAccepted | StaleCommit

/** What an operation's plan reads, the room's state as the operation found it, and the writes it records. */
type OperationTx = {
  readonly now: number
  cell(key: string): Uint8Array | undefined
  write(mutation: CellMutation): void
  count(count: CellCount): void
  /** Deletes every cell under `prefix`, before the operation's writes. */
  dropPrefix(prefix: string): void
  /** Forgets `member`'s inbox and binary lanes, their order and their retained frames, and the semantic lane's
   *  retained frame while `member` owns it, as the executor finds it when it applies the operation. */
  dropMemberLanes(member: string): void
  /** Commits `payload` on the control lane; once per operation. */
  commit(payload: Uint8Array): void
  /** Writes an expiring cell: once `ttlMs` has passed since this write, no read returns it and the backend deletes it,
   *  with or without a server on the room. It is read only by `readExpiringCells()`: neither a plan nor a cell read
   *  sees it. */
  writeExpiring(cell: ExpiringCell): void
}

type ExpiringCell = { key: string; bytes: Uint8Array; ttlMs: number }

/** The expiring cells of the keys read that haven't expired. */
type ExpiringCellsRead = { kind: 'read'; cells: Map<string, Uint8Array> } | StaleIncarnation

/** A pure plan, by the name a Cloudflare room authority runs it by. Its args and value cross an RPC there. */
type RoomOperation<Args, Value> = { readonly name: string; plan(tx: OperationTx, args: Args): Value }

/** An operation that ran: its plan's value, each counted counter as it left it, and its commit. */
type OperationResult<Value> =
  | { kind: 'done'; value: Value; counters: Map<string, CellCounter>; commit?: CommitAccepted }
  | StaleIncarnation

/** The lane a commit lands on, in `inc` of `roomId`. */
type CommitTarget = { roomId: string; inc: string; lane: LaneId }

type RoomSubscriptionSource = {
  roomId: string
  inc: string
  lane: LaneId | EveryTrackLane | EveryMemberLane
}

/** What a backend implements for Room: durable heads, cells, lanes and retained frames, and lane subscriptions. */
type RoomDriver = SubscriptionDriver<RoomSubscriptionSource> & {
  readHead(roomId: string): Promise<RoomHead | null>
  compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult>
  readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead>
  readExpiringCells(roomId: string, inc: string, keys: readonly string[]): Promise<ExpiringCellsRead>
  /** The cells under `prefix`, a bounded page at a time, each page read at once:
   *  - The whole read has one order, which keeps the keys sharing the prefix's next `:`-ended segment together (key
   *    order on Redis and Cloudflare). No key comes twice.
   *  - A key present for the whole read comes.
   *  - A key written during the read, and kept, comes if the order puts it after every key returned before the write.
   *    Any other key written or removed during the read may or may not come.
   *  - A `stale-incarnation` page is the last. */
  readCellPages(roomId: string, inc: string, prefix: string): AsyncIterable<CellsRead>
  compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult>
  /** Runs `operation`'s plan while `inc` is open, and applies the writes of the run it validates:
   *  - The plan may run more than once. Redis runs it in this process, again after a run read a cell it hadn't read
   *    yet, and again from the start when a cell it read changed before the writes applied. Cloudflare and memory run
   *    it once, where the room's state lives.
   *  - In a run, a cell the executor hasn't read yet reads as absent.
   *  - `tx.cell()` reads the state the operation found, never the run's own writes.
   *  - Only the validated run's writes are applied, all at once; none if the plan throws.
   *  - The commit's delivery comes after the writes, separately (`CommitAccepted.delivery`). */
  runOperation<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
  ): Promise<OperationResult<Value>>
  /** Keeps `payload` past the call only as a copy: the caller reuses its bytes. */
  commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult>
  /** The lane's last commit's seq and timestamp; null before its first, or once its incarnation or member is gone. */
  readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null>
  readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null>
  /** The lanes holding a retained frame; with `of`, only that member's or that track's binary lanes, at a cost of
   *  their count. */
  listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]>
  dropIncarnation(roomId: string, inc: string): Promise<void>
  /** Lists `roomId` under `incTag`, due at `checkAt` (authority ms) and no sooner than `minDelayMs` from the
   *  authority's now; an entry due at Infinity is never due. */
  directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void>
  /** The tag `roomId` is listed under, if it is listed. */
  directoryGet(roomId: string): Promise<string | undefined>
  directoryList(prefix: string, cursor?: string): Promise<DirectoryPage>
  /** Up to a page of the entries due at the authority's now, earliest first; an entry stays due until it is put again
   *  or deleted. */
  directoryDue(): Promise<DirectoryPage['entries']>
  /** As a Broadcast driver's `subscribe`; an incarnation that isn't open is a refusal. */
  subscribe(source: RoomSubscriptionSource, receiver: BackendReceiver): DriverAttempt
}
