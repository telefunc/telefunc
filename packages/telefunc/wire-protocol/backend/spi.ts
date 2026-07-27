// Telefunc's one backend seam. Generic Broadcast uses the cheap publish/subscribe verbs; Room uses
// the fenced/durable verbs as well. Both share the same codec, subscription lifecycle and fan-out.
//
// Physical model, verbatim for every backend:
//
//   HEAD (unscoped, one per roomId — the ONLY unscoped room record):
//     head(roomId) = { rev, currentInc | null, state: open|closing|closed, config, closeLease? }
//   GENERATION SUBTREE (per incarnation; all durable room state):
//     gen(roomId, inc) / cell/<key>       opaque policy cells (member records, markers, policy state)
//                      / order/<domain>   order watermarks (see the lane table)
//                      / retained/<laneKey>  retained generations (backend-internal chunking)
//   DIRECTORY (optional capability): roomId -> incTag   eventually consistent, tag-guarded, repairable
//
// Head CAS is the single lifecycle primitive: create, fence, and state transition are one atomic
// record, so a config-without-fence zombie is unrepresentable and every other operation's
// `expectedInc` check resolves against `head.currentInc` + `state`. Nothing but the head is unscoped.

export const BACKEND_SPI_VERSION = 1 as const

/** A cheap Broadcast route. Text and binary use separate delivery routes but share `key`'s ordering
 * domain, preserving Broadcast's single per-key sequence across both payload kinds. */
export type BroadcastLane = { key: string; kind: 'text' | 'binary' }

export type PublishResult = {
  seq: number
  timestamp: number
  receivers?: number
  meta?: Record<string, unknown>
}

// ── Lanes: every lane names its order domain and its channel ──
export type LaneId =
  | { kind: 'semantic' } // participant text + Room.announce
  | { kind: 'control' } // presence/lifecycle events
  | { kind: 'binary'; member: string; track: string }
  | { kind: 'inbox'; member: string } // DMs + ack replies — stale DMs are fenced too

// Fixed lane table (normative):
//  lane      order domain                      channel            notes
//  semantic  RoomOrder (monotonic seq)         ONE channel        text AND announce share domain AND channel
//  control   ControlSeq (per-room counter)     control channel    fenced; projections stay idempotent
//  binary    LaneSeq (per member,track)        per-lane channel   separate domains preserved by constraint
//  inbox     InboxSeq (per member)             per-inbox channel  separate domain preserved by constraint

export type RoomHead = {
  rev: string // opaque head revision; changes on every successful head CX
  currentInc: string | null // null on a 'closed' tombstone — closing→closed CLEARS it, which is
  // what makes dropGeneration(oldInc) legal after close (I9)
  state: 'open' | 'closing' | 'closed'
  config: Uint8Array // opaque to backends (policy: meta + LWW stamp; small)
  closeLease?: { id: string; until: number } // present iff state === 'closing'. `until` is ALWAYS
  // computed BY THE BACKEND inside the head CX (authorityNow + durationMs — see HeadNext): a caller can
  // never supply an absolute deadline, so caller clock skew can neither pre-expire a fresh lease nor make
  // one effectively permanent. The STORED head (incl. the minted lease) is returned with the CX success
  // result — that is how the closer learns its deadline. The lease is the ONLY credential that can
  // authorize a commit while the head is closing (closingLease below) and the ONLY credential that can
  // finalize closing→closed.
}

export type HeadCx =
  | { expect: 'absent' } // create on a truly fresh id
  | { expect: { rev: string } } // transition from a read head
  | { expect: { rev: string; closingLeaseExpired: true } }
  // EXPIRED-CLOSE TAKEOVER (recovery): succeeds iff head.rev matches ∧ state==='closing' ∧
  // closeLease.until < authorityNow — the expiry compare runs in AUTHORITY time INSIDE the CX
  // the expiry compare runs on the backend's authoritative clock inside the same atomic operation;
  // a local caller clock is never consulted. `next` MUST be closing(SAME inc, fresh lease with a
  // DIFFERENT id) — takeover replaces only the lease, never the incarnation; the backend mints the
  // new `until` (HeadNext durationMs rule). An unexpired lease makes the CX fail like any rev
  // conflict, so takeover can never steal a live closer's room (I13).
  | { expect: { rev: string; closingLease: string } }
// LEASE-GUARDED FINALIZATION: REQUIRED for every closing→closed / currentInc:null transition.
// Succeeds iff head.rev matches ∧ state==='closing' ∧ closeLease.id === closingLease — id equality
// ONLY, not expiry: a closer whose closed event was accepted may still finalize after time passes, but
// a REPLACED lease (takeover happened) fails here. Losing this precondition ABORTS that closer's tail —
// no re-read/retry with the generic {rev} form is permitted for finalization; only the current lease
// holder may continue (I13).

export type HeadNext =
  | {
      head: Omit<RoomHead, 'rev' | 'closeLease'> & { closeLease?: { id: string; durationMs: number } }
      ttlMs?: number // ttlMs only for state:'closed' tombstones
    }
  // Any head entering 'closing' (open→closing AND expired-close takeover) carries a fresh lease id plus
  // a FINITE durationMs — never an absolute deadline. Bounds are normative: MIN_CLOSE_LEASE_MS ≤
  // durationMs ≤ MAX_CLOSE_LEASE_MS (out of bounds ⇒ throw). The backend stores
  // until = authorityNow + durationMs inside the SAME atomic CX and returns the stored head, so the
  // installed lease is always fresh in backend authority time regardless of caller skew (I13).
  | { delete: true } // tombstone expiry path (backends with no native TTL)

// Normative close-lease duration bounds for every HeadNext installing 'closing'.
export const MIN_CLOSE_LEASE_MS = 1_000
export const MAX_CLOSE_LEASE_MS = 60_000

export type CellMutation = { key: string; set?: { bytes: Uint8Array; ttlMs?: number } }
// set absent ⇒ delete. ttl per cell (member records etc.)

export type CxResult = 'committed' | 'conflict' | 'stale-inc'
// stale-inc: head.currentInc !== inc || head.state !== 'open'  (head precondition is INSIDE the CX)
// conflict : read-set revision no longer current
// Errors (transport/backend) throw; they are never encoded as CxResult.

export type CommitAccepted = {
  accepted: true
  seq: number // positive safe integer; standalone cursor within this incarnation+lane domain
  timestamp: number // safe integer; non-decreasing authority time, independent of seq advancement
  receivers: number // targets snapshotted at acceptance
  delivery: Promise<void> // the backend's ONE at-most-once HANDOFF attempt: settles when its
  // backend-defined handoff settles. Receiver-callback completion is NOT a cross-backend guarantee;
  // per-target failure visibility is a per-backend trace/capability. NEVER retries; NEVER poisons.
}
export type CommitResult = CommitAccepted | { stale: true }

export type SubscriptionState = 'establishing' | 'ready' | 'lost' | 'closed'
// A raw backend establishment attempt has no settlement deadline; the shared subscription manager
// bounds every attempt before exposing this supervised subscription at the SPI.
export type BackendSubscription = {
  /** The current readiness generation. It changes after a ready subscription is lost and rejects
   * after bounded replacement exhaustion; callers must read it at the point they need readiness. */
  readonly ready: Promise<void>
  state(): SubscriptionState
  onStateChange(cb: (s: SubscriptionState) => void): () => void
  unsubscribe(): Promise<void>
}

export type BackendReceiver = (payload: Uint8Array, info: { seq: number; timestamp: number }) => void

/** The neutral source description handed to a backend author's raw subscription driver. Core owns
 * canonical keys, local fan-out, refcounting and supervision; the driver sees one establishment attempt. */
export type BackendSubscriptionSource =
  | { kind: 'broadcast'; lane: BroadcastLane }
  | { kind: 'durable'; roomId: string; inc: string; lane: LaneId }

/** One raw backend establishment attempt. Its readiness may remain pending indefinitely; core bounds
 * the attempt before exposing any subscription to an SPI consumer. */
export type SubscriptionAttempt = {
  readonly ready: Promise<void>
  state(): SubscriptionState
  onStateChange(cb: (state: SubscriptionState) => void): () => void
  unsubscribe(): Promise<void>
}

/** The only backend-specific subscription edge. An author implements acknowledgement and cleanup for
 * one raw attempt; core supplies epochs, fan-out, readiness, bounded replacement and the watchdog. */
export type SubscriptionDriver<Source = BackendSubscriptionSource> = {
  open(source: Source, receiver: BackendReceiver, localReceiverCount: () => number): SubscriptionAttempt
}

/** The supervised backend contract consumed by Telefunc through `getBackend()`. Backend authors do not
 * implement this type directly and never receive a supervised subscription. They implement
 * `BackendDriver`; core composes its raw subscription driver into this consumer-facing SPI. */
export type BackendSpi = {
  readonly spiVersion: typeof BACKEND_SPI_VERSION
  readonly capabilities: {
    receivers: 'global' | 'node-local' | 'none'
    maxRetainedPayloadBytes: number // aggregate cap; commit with larger retain REJECTS (throws)
    clusterSafe: boolean
    directory: boolean // directory verbs present iff true
  }

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
  // The success result discriminates by operation: every head WRITE returns the stored head — including
  // the new rev and, for closing states, the backend-minted closeLease {id, until} the caller's tail must
  // carry (I13 minting rule); a delete returns {deleted: true} and never fabricates a RoomHead.

  // ── generation cells (always inc-scoped; fixed read sets; no callbacks) ──
  readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }>
  // `revision` covers exactly the returned read set. A backend MAY use one per-generation monotonic
  // revision (coarser ⇒ more spurious conflicts, never wrong results).
  compareExchangeCells(roomId: string, inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult>
  // Retry on 'conflict' is OWNED BY ROOM CORE: bounded 16 attempts, backoff 1→64 ms jittered, then
  // RoomError('contention'). Backends NEVER loop.

  // ── lane commit: ATOMIC ACCEPTANCE, separate delivery attempt ──
  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string },
  ): Promise<CommitResult>
  // DEFAULT precondition (single backend-atomic step): head check (currentInc===inc ∧ state==='open')
  //   + advance lane's order domain + (retain ? install retained generation : nothing).
  // CLOSING-CONTROL precondition (narrow; the ONLY exception): `closingLease` is honored iff
  //   lane.kind==='control' ∧ head.state==='closing' ∧ head.currentInc===inc ∧
  //   head.closeLease.id===closingLease ∧ now ≤ head.closeLease.until (AUTHORITY time — an expired lease
  //   is stale even with the correct id) — it exists EXACTLY for the room's closed control event.
  //   Supplying it on a semantic/binary/inbox commit ⇒ { stale } regardless of head state; a wrong lease
  //   id ⇒ { stale }; while the head remains 'closing' with the matching lease, repeated closed-event
  //   commits are ACCEPTED (a resumed closer may retry; downstream application is idempotent); after
  //   closing→closed, every commit — including control with the old lease — ⇒ { stale }.
  // Delivery attempt: AFTER acceptance, ordered per lane, exposed via `delivery` so PRODUCT POLICY
  //   decides what the public publish promise awaits.
  // Ordering position: one persistent cursor per existing (incarnation instance, lane) domain.
  //   Every accepted commit advances seq by exactly one; timestamp is independently clamped to authority
  //   time. The cursor has no live TTL and is removed only when its generation is dropped. Exhaustion at
  //   Number.MAX_SAFE_INTEGER rejects before acceptance, retained installation, or delivery effects.

  // ── retained (consistent; whole payloads; chunking is BACKEND-INTERNAL) ──
  readRetained(
    roomId: string,
    inc: string,
    lane: LaneId,
  ): Promise<{ payload: Uint8Array; seq: number; timestamp: number } | null>
  listRetained(roomId: string, inc: string): Promise<LaneId[]>
  deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void>
  // With `ifSeq`, delete only when the lane's currently installed retained generation has that exact
  // sequence. A mismatch or missing generation is a silent no-op. The guard requires `lane`.

  // ── subscriptions (per lane channel; INCARNATION-SCOPED) ──
  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: BackendReceiver): BackendSubscription
  // Establishment performs an open-head check (head.currentInc === inc ∧ state === 'open'); a mismatch
  // rejects `ready`. Channels/routes are keyed by (inc, lane): a surviving subscription from a previous
  // incarnation can never receive a recreated room's frames (I11).

  // ── generation lifecycle (janitor) ──
  listGenerations(roomId: string): Promise<string[]> // incs with any surviving state
  dropGeneration(roomId: string, inc: string): Promise<void> // MUST refuse head.currentInc === inc

  // ── directory (iff capabilities.directory) ──
  directoryPut(roomId: string, incTag: string): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void> // deletes iff stored tag matches
  directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> // paginated, may be stale

  dispose(): Promise<void>
}

/** The contract a backend author implements. It is the durable/cheap storage surface plus one raw
 * subscription driver; core turns it into `BackendSpi` at installation time. */
export type BackendDriver = Omit<BackendSpi, 'subscribe' | 'subscribeLane'> & {
  readonly subscriptions: SubscriptionDriver
}
