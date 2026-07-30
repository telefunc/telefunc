// Telefunc's one backend seam: Broadcast uses cheap routes; Room adds fenced durable verbs. The sole
// unscoped record is head(roomId); gen(roomId, inc)/{cell,order,retained} owns durable state and the
// tag-guarded directory is eventually consistent. Head CAS atomically creates, fences, and transitions,
// so config cannot outlive its fence and every scoped operation validates currentInc plus state.

export const BACKEND_SPI_VERSION = 1 as const

/** A cheap text/binary route; both kinds share one per-key ordering domain. */
export type BroadcastLane = { key: string; kind: 'text' | 'binary' }

export type PublishResult = {
  seq: number
  timestamp: number
  receivers?: number
  meta?: Record<string, unknown>
}

/** Fixed channels/order domains: semantic shares RoomOrder; control uses ControlSeq; binary uses
 * per-(member,track) LaneSeq; inbox uses per-member InboxSeq. */
export type LaneId =
  | { kind: 'semantic' } // participant text + Room.announce
  | { kind: 'control' } // presence/lifecycle
  | { kind: 'binary'; member: string; track: string }
  | { kind: 'inbox'; member: string } // DMs + ack replies; stale DMs remain fenced

export type RoomHead = {
  rev: string // opaque head revision; changes on every successful head CX
  currentInc: string | null // closing→closed clears it, permitting old-generation deletion
  state: 'open' | 'closing' | 'closed'
  config: Uint8Array // opaque to backends (policy: meta + LWW stamp; small)
  /** Closing only. CX mints `until` from authority time and returns the stored head; this lease alone
   * authorizes closing-control commit and finalization. */
  closeLease?: { id: string; until: number }
}

export type HeadCx =
  | { expect: 'absent' } // create on a truly fresh id
  | { expect: { rev: string } } // transition from a read head
  | { expect: { rev: string; closingLeaseExpired: true } }
  | { expect: { rev: string; closingLease: string } }
/** Takeover atomically requires matching rev, closing state, and authority-time expiry, then replaces
 * only the lease (same inc, different id, backend-minted deadline), so it cannot steal a live close.
 * Finalization requires matching rev/state/lease id (not expiry); a replaced holder cannot retry via
 * generic rev, while an accepted holder may finalize after its deadline. */

export type HeadNext =
  | {
      head: Omit<RoomHead, 'rev' | 'closeLease'> & { closeLease?: { id: string; durationMs: number } }
      ttlMs?: number // ttlMs only for state:'closed' tombstones
    }
  // Entering closing supplies a fresh id and positive duration, never an absolute deadline; CX stores
  // authorityNow + duration and returns that stored head.
  | { delete: true } // tombstone expiry path (backends with no native TTL)

export type CellMutation = { key: string; set?: { bytes: Uint8Array; ttlMs?: number } } // no set => delete

export type CxResult = 'committed' | 'conflict' | 'stale-inc'
// stale-inc is the in-CX head precondition; conflict is a stale read set; backend errors throw.

export type CommitAccepted = {
  accepted: true
  seq: number // positive safe-integer cursor within this incarnation+lane domain
  timestamp: number // non-decreasing safe-integer authority time, independent of seq
  receivers?: number // global acceptance-time target count, if knowable
  /** One at-most-once backend-defined handoff: never retries/poisons. Callback completion and
   * per-target failure visibility are backend capabilities, not cross-backend guarantees. */
  delivery: Promise<void>
}
export type CommitResult = CommitAccepted | { stale: true }

export type SubscriptionState = 'establishing' | 'ready' | 'lost' | 'closed'
/** Raw-only ownership terminal. Core maps this to public `closed`; consumers never receive it. */
export type SubscriptionAttemptState = SubscriptionState | 'terminated'
// Raw establishment has no deadline; consumers own recovery SLAs.
export type BackendSubscription = {
  /** Current readiness generation: replaced after loss, rejected on terminal failure. */
  readonly ready: Promise<void>
  state(): SubscriptionState
  onStateChange(cb: (s: SubscriptionState) => void): () => void
  unsubscribe(): Promise<void>
}

export type BackendReceiver = (payload: Uint8Array, info: { seq: number; timestamp: number }) => void

/** Neutral raw source; core owns canonical keys, fan-out, refcounting, and supervision. */
export type BackendSubscriptionSource =
  | { kind: 'broadcast'; lane: BroadcastLane }
  | { kind: 'durable'; roomId: string; inc: string; lane: LaneId }

/** One raw backend establishment attempt. Its readiness may remain pending indefinitely. */
export type SubscriptionAttempt = {
  readonly ready: Promise<void>
  state(): SubscriptionAttemptState
  onStateChange(cb: (state: SubscriptionAttemptState) => void): () => void
  unsubscribe(): Promise<void>
}

/** Captured synchronously under local ownership. `partition` is an opaque value key; `open` is
 * ambient-free and `valid` is a synchronous, side-effect-free, non-throwing ownership read. */
export type SubscriptionBinding = {
  readonly partition: string
  valid(): boolean
  open(receiver: BackendReceiver, localReceiverCount: () => number): SubscriptionAttempt
}

/** Sole backend-specific subscription edge: one ownership capture and one raw attempt; core owns
 * identity, stale-attempt rejection, fan-out, ownership checks, and readiness. */
export type SubscriptionDriver<Source = BackendSubscriptionSource> = {
  bind(source: Source): SubscriptionBinding
}

/** Supervised consumer contract; authors implement `BackendDriver`, never this type directly. */
export type BackendSpi = {
  readonly spiVersion: typeof BACKEND_SPI_VERSION

  // ── cheap Broadcast ──
  publish(lane: BroadcastLane, payload: Uint8Array): PublishResult | Promise<PublishResult>
  subscribe(lane: BroadcastLane, receiver: BackendReceiver): BackendSubscription

  // ── head ──
  readHead(roomId: string): Promise<{ head: RoomHead } | null> // always consistent
  compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<
    | { ok: true; head: RoomHead } // success of a {head: ...} next — ALWAYS the STORED head
    | { ok: true; deleted: true } // success of a {delete: true} next — no head exists afterwards
    | { conflict: true; current: RoomHead | null }
  >
  // Writes return the stored head (new rev and minted lease); delete returns only {deleted:true}.

  // ── generation cells (always inc-scoped; fixed read sets; no callbacks) ──
  readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }>
  // Revision covers the read set; a coarser per-generation revision may cause only spurious conflicts.
  compareExchangeCells(roomId: string, inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult>
  // Backends do not retry conflicts.

  // ── lane commit: ATOMIC ACCEPTANCE, separate delivery attempt ──
  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitResult>
  // Default acceptance atomically requires open/current inc and every required cell live, advances the
  // lane domain, and optionally retains; missing required cells are stale. The sole exception is a control
  // commit during closing with the matching unexpired authority-time lease. Other lanes, wrong/expired
  // leases, and every post-close commit are stale; matching closed-event retries remain accepted.
  // Delivery starts after acceptance and is ordered per lane. Each accepted commit advances its persistent
  // generation/lane cursor by one while timestamp clamps independently to authority time. The cursor has
  // no TTL; generation drop removes it, and safe-integer exhaustion rejects before any effect.

  // ── retained (consistent; whole payloads; chunking is BACKEND-INTERNAL) ──
  readRetained(
    roomId: string,
    inc: string,
    lane: LaneId,
  ): Promise<{ payload: Uint8Array; seq: number; timestamp: number } | null>
  listRetained(roomId: string, inc: string): Promise<LaneId[]>
  deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void>
  // `ifSeq` requires a lane and deletes only an exact current sequence; mismatch/missing is a no-op.

  // ── subscriptions (per lane channel; INCARNATION-SCOPED) ──
  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: BackendReceiver): BackendSubscription
  // Establishment rejects readiness unless the head is open/current; (inc,lane) route keys prevent
  // a surviving old-inc subscription from receiving recreated-room frames.

  // ── generation lifecycle (janitor) ──
  listGenerations(roomId: string): Promise<string[]> // incs with any surviving state
  dropGeneration(roomId: string, inc: string): Promise<void> // MUST refuse head.currentInc === inc

  // ── directory ──
  directoryPut(roomId: string, incTag: string): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void> // deletes iff stored tag matches
  directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> // paginated, may be stale

  dispose(): Promise<void>
}

/** Author contract: storage verbs plus one raw subscription driver, supervised at installation. */
export type BackendDriver = Omit<BackendSpi, 'subscribe' | 'subscribeLane'> & {
  readonly subscriptions: SubscriptionDriver
}
