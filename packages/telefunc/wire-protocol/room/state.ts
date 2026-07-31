export { RoomState, RoomStateView, remoteBacking }

import { assertUsage } from '../../utils/assert.js'
import { isPromise } from '../../utils/isPromise.js'
import type { ChannelPublishInfo } from '../channel.js'
import { adoptSubordinateOf, makeDisposer, releaseSubordinate } from '../wrapProxy.js'
import {
  DEFAULT_TRACK,
  emptyTrackWants,
  isRoomTrack,
  ownMetadata,
  stampNewer,
  type BinaryWants,
  type MemberSnapshot,
  type MemberWants,
  type TrackWants,
} from './protocol.js'
import type {
  BinaryFrameInfo,
  LeaveCause,
  ParticipantMeta,
  ParticipantSnapshotView,
  RemoteParticipant,
  RoomMeta,
  RoomSnapshotView,
  Sender,
} from './types.js'
// RoomState — the local view of a room, driven by the event stream
/** A binary listener's track filter: `undefined` = every track, `null` = the default lane only, a name = that track only. */
type TrackFilter = string | null | undefined
type MemberEntry = {
  id: string
  meta: ParticipantMeta
  joinedAt: number
  identity: string | null
  /** Latest applied meta revision — stale and echoed `p-meta` events are absorbed. */
  metaSeq: number
  /** Named tracks the member is known to publish — grown by `track` events and rosters, never shrunk (tracks live as long as the member). Drives all-track key subscriptions. */
  tracks: Set<string>
  /** An off-presence participant — a member for routing/discovery, excluded from every presence read (`count`, `snapshot`, `onJoin`/`onLeave`/`onEmpty`). Any number per room. */
  hidden: boolean
  /** Structural view keeps the WeakRef implementation detail out of the public declaration. */
  remoteRef: { deref(): RemoteParticipant | undefined } | null
  left: boolean
  leaveCause?: LeaveCause
  dataCbs: Array<(data: unknown, info: ChannelPublishInfo) => unknown>
  binaryCbs: Array<{
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo) => unknown
    track: TrackFilter
  }>
  updateCbs: Array<(meta: ParticipantMeta, prev: ParticipantMeta) => void>
  leaveCbs: Array<(cause?: LeaveCause) => void>
}
type RoomStateOptions = {
  roomId: string
  meta: RoomMeta
  /** Either the authoritative roster, or just its member count — a lazy view seeds with `{ count }` and learns the members from its first `reconcile()` (KV read / streamed roster). */
  seed: { members: MemberSnapshot[] } | { count: number }
  /** The LWW stamp of the config `meta` was read from (see `applyRoomUpdate`). */
  updateStamp: { at: number; by: string }
  closed?: boolean
  /** Fired whenever the number of attached listeners changes — lets the owner (de)activate its event source (adapter subscription, wire subscription). */
  onListenersChanged: () => void
  /** A user callback threw — the owner decides how to report it. */
  onCallbackError: (err: unknown) => void
}
/** Exact-keyed backing lets the serializer recover (room, member) without exposing a public brand. */
const ROOM_REMOTE_BACKINGS: unique symbol = Symbol.for('telefunc.RoomRemoteParticipantBackings')
type RemoteBacking = { state: RoomState; entry: MemberEntry }
const remoteBackingGlobal = globalThis as typeof globalThis & {
  [ROOM_REMOTE_BACKINGS]?: WeakMap<object, RemoteBacking>
}
const remoteBackings = (remoteBackingGlobal[ROOM_REMOTE_BACKINGS] ??= new WeakMap())
/** The `RoomState` backing of a minted `RemoteParticipant` — `null` for anything else. */
function remoteBacking(value: unknown): RemoteBacking | null {
  return typeof value === 'object' && value !== null ? (remoteBackings.get(value) ?? null) : null
}
/** The side-neutral public view over a `RoomState`; client/server keep their own I/O and lifecycle. */
abstract class RoomStateView {
  protected abstract readonly _state: RoomState
  get id(): string {
    return this._state.roomId
  }
  get meta(): RoomMeta {
    return this._state.meta
  }
  get count(): number {
    return this._state.count
  }
  get isEmpty(): boolean {
    return this._state.count === 0
  }
  get isClosed(): boolean {
    return this._state.closed
  }
  subscribe(callback: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._state.subscribe(callback)
  }
  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown,
    options?: { track?: string | null },
  ): () => void {
    return this._state.subscribeBinary(callback, options)
  }
  onJoin(callback: (member: RemoteParticipant) => void): () => void {
    return this._state.onJoin(callback)
  }
  onLeave(callback: (member: RemoteParticipant, cause?: LeaveCause) => void): () => void {
    return this._state.onLeave(callback)
  }
  onParticipantUpdate(
    callback: (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void,
  ): () => void {
    return this._state.onParticipantUpdate(callback)
  }
  onUpdate(callback: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this._state.onUpdate(callback)
  }
  onEmpty(callback: () => void): () => void {
    return this._state.onEmpty(callback)
  }
  onClose(callback: () => void): () => void {
    return this._state.onClose(callback)
  }
  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this._state.onAnnounce(callback)
  }
  // Detached use is documented for React's `useSyncExternalStore(room.onChange, room.snapshot)`.
  onChange = (callback: () => void): (() => void) => this._state.onChange(callback)
}
/**
 * The local, event-driven view of a room: membership, metadata, and every user-facing callback.
 * Server and client share this class so event semantics are identical on both sides; only the
 * event *source* differs (adapter subscription vs relayed wire frames).
 *
 * Event application is idempotent — a `join` for a known member or a `leave` for an unknown one
 * is absorbed silently. This lets owners seed state from a snapshot and apply a concurrently
 * produced event stream without double-firing.
 */
class RoomState {
  /** @internal — the owning `ServerRoom`/`ClientRoom`, for serialization backing. */
  _owner: unknown = null
  readonly roomId: string
  meta: RoomMeta
  closed: boolean
  /** Bumped on every membership change — guards async KV reconciles against going stale. */
  membershipVersion = 0
  /** Bumped on every observable change (membership, participant meta, room config, closure) — drives `onChange`/`snapshot()` cache invalidation. */
  private _stateVersion = 0
  private _snapshotCache: { version: number; value: WeakRef<RoomSnapshotView> } | null = null
  private readonly _listenerCleanups = new Map<object, Set<() => void>>()
  private readonly _changeCbs: Array<() => void> = []
  /** Reconcile coalescing: while a batch is open, `_bumpState` still advances the version (so
   *  `snapshot()` recomputes) but holds the single `onChange` until the batch closes — a reconcile that
   *  touched N members invalidates the view once, not once per member. Re-entrant via the depth count. */
  private _changeBatchDepth = 0
  private _changeDeferred = false
  private readonly _members = new Map<string, MemberEntry>()
  private readonly _onListenersChanged: () => void
  private readonly _onCallbackError: (err: unknown) => void
  private readonly _roomDataCbs: Array<(data: unknown, info: ChannelPublishInfo, from: Sender) => unknown> = []
  private readonly _roomBinaryCbs: Array<{
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown
    track: TrackFilter
  }> = []
  private readonly _joinCbs: Array<(member: RemoteParticipant) => void> = []
  private readonly _leaveCbs: Array<(member: RemoteParticipant, cause?: LeaveCause) => void> = []
  private readonly _participantUpdateCbs: Array<
    (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void
  > = []
  private readonly _updateCbs: Array<(meta: RoomMeta, prev: RoomMeta) => void> = []
  private readonly _emptyCbs: Array<() => void> = []
  private readonly _closeCbs: Array<() => void> = []
  private readonly _announceCbs: Array<(data: unknown, info: ChannelPublishInfo) => void> = []
  private _listenerCount = 0
  private _updateStamp: { at: number; by: string }
  private _rosterKnown: boolean
  private _seedCount = 0
  constructor(opts: RoomStateOptions) {
    this.roomId = opts.roomId
    this.meta = ownMetadata(opts.meta)
    this.closed = opts.closed === true
    this._updateStamp = opts.updateStamp
    this._onListenersChanged = opts.onListenersChanged
    this._onCallbackError = opts.onCallbackError
    if ('members' in opts.seed) {
      this._rosterKnown = true
      for (const member of opts.seed.members) this._createEntry(member)
    } else {
      this._rosterKnown = false
      this._seedCount = opts.seed.count
    }
  }
  // ── Reads ──
  /** Exact once the roster is known (seeded or reconciled), and presence-accurate before that too:
   *  the seed (`Room.get`/`Room.list`) already excludes hidden participants — members for routing,
   *  never counted — so the pre-roster count is that seed adjusted by the events applied since. */
  get count(): number {
    if (!this._rosterKnown) return this._seedCount
    return this._members.size - this._hiddenCount()
  }
  /** The off-presence participants (`join({ hidden: true })`) — a server authority, a bot, a
   *  recorder. Members for routing and discovery, excluded from every presence read; read here via
   *  `getParticipants({ hidden: true })`. Any number per room. */
  listHidden(): RemoteParticipant[] {
    return [...this._members.values()].filter((entry) => entry.hidden).map((entry) => this._remote(entry))
  }
  private _hiddenCount(): number {
    let n = 0
    for (const entry of this._members.values()) if (entry.hidden) n++
    return n
  }
  /** Whether this view holds the authoritative member list (vs just a count). */
  get rosterKnown(): boolean {
    return this._rosterKnown
  }
  /** Total attached listeners; owners derive lane-specific wants from the callback lists. */
  get listenerCount(): number {
    return this._listenerCount
  }
  /** Whether this holder consumes room-authored messages on the semantic lane. */
  get wantsAnnounce(): boolean {
    return this._announceCbs.length > 0
  }
  /** Which (member, track) binary streams this holder needs delivered — drives the wire/adapter subscriptions on both sides (client declares it, server aggregates it per stub). */
  binaryWants(): BinaryWants {
    const members: Record<string, TrackWants> = Object.create(null)
    for (const entry of this._members.values()) {
      if (entry.binaryCbs.length > 0) members[entry.id] = trackWantsOf(entry.binaryCbs)
    }
    return { everyMember: trackWantsOf(this._roomBinaryCbs), members }
  }
  /** Named tracks the member is known to publish — `[]` for unknown members. */
  memberTracks(id: string): string[] {
    const entry = this._members.get(id)
    return entry ? [...entry.tracks] : []
  }
  /** Whether this member is off-presence (`join({ hidden: true })`) — `false` for unknown members. */
  isHidden(id: string): boolean {
    return this._members.get(id)?.hidden === true
  }
  /** The text-lane twin of `binaryWants()`: `all` while room-level `subscribe()`rs exist, otherwise exactly the members with participant-scoped listeners. */
  textWants(): MemberWants {
    if (this._roomDataCbs.length > 0) return { all: true, members: [] }
    const members: string[] = []
    for (const entry of this._members.values()) {
      if (entry.dataCbs.length > 0) members.push(entry.id)
    }
    return { all: false, members }
  }
  getRemote(id: string): RemoteParticipant | null {
    const entry = this._members.get(id)
    return entry ? this._remote(entry) : null
  }
  listRemotes(): RemoteParticipant[] {
    return [...this._members.values()].filter((entry) => !entry.hidden).map((entry) => this._remote(entry))
  }
  /** Member IDs currently known — drives the per-member binary key subscriptions. */
  listMemberIds(): string[] {
    return [...this._members.keys()]
  }
  snapshotMembers(): MemberSnapshot[] {
    return [...this._members.values()].map(({ id, meta, joinedAt, metaSeq, identity, tracks, hidden }) => ({
      id,
      meta,
      joinedAt,
      metaSeq,
      identity,
      ...(tracks.size === 0 ? {} : { tracks: [...tracks] }),
      ...(hidden ? { hidden: true } : {}),
    }))
  }
  /** Revival side of a serialized `RemoteParticipant`: the live view wins when it already knows the member; otherwise the entry is seeded silently from the snapshot — no events fire, no count
   * adjustment (the seed count already included the member), and the streamed roster reconciles it like any other pre-roster knowledge.
   */
  ensureRemoteFromSnapshot(snap: MemberSnapshot): RemoteParticipant {
    const existing = this._members.get(snap.id)
    if (existing) return this._remote(existing)
    const remote = this._remote(this._createEntry(snap))
    this._bumpState()
    return remote
  }
  // ── Listener registration (all return an unlisten function) ──
  subscribe(cb: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._register(this._roomDataCbs, cb)
  }
  subscribeBinary(
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown,
    opts?: { track?: string | null },
  ): () => void {
    return this._register(this._roomBinaryCbs, { cb, track: normalizeTrackFilter(opts) })
  }
  onJoin(cb: (member: RemoteParticipant) => void): () => void {
    return this._register(this._joinCbs, cb)
  }
  onLeave(cb: (member: RemoteParticipant, cause?: LeaveCause) => void): () => void {
    return this._register(this._leaveCbs, cb)
  }
  onParticipantUpdate(
    cb: (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void,
  ): () => void {
    return this._register(this._participantUpdateCbs, cb)
  }
  onUpdate(cb: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this._register(this._updateCbs, cb)
  }
  onEmpty(cb: () => void): () => void {
    return this._register(this._emptyCbs, cb)
  }
  onClose(cb: () => void): () => void {
    return this._register(this._closeCbs, cb)
  }
  onChange(cb: () => void): () => void {
    return this._register(this._changeCbs, cb)
  }
  /** A member published its first frame on a new named track (idempotent — echoes, rosters, and the owner's local apply all land here). Unknown members are absorbed like any other pre-roster event.
   */
  applyTrack(id: string, track: string): void {
    const entry = this._members.get(id)
    if (!entry) {
      this.membershipVersion++
      return
    }
    entry.tracks.add(track)
  }
  /** Immutable view of the whole room — cached by state version, so the reference is stable until something actually changes (the `useSyncExternalStore` contract). */
  snapshot(): RoomSnapshotView {
    const cached = this._snapshotCache?.version === this._stateVersion ? this._snapshotCache.value.deref() : undefined
    if (cached) return cached
    const participants = Object.freeze(
      [...this._members.values()]
        .filter((entry) => !entry.hidden)
        .map(({ id, identity, meta, joinedAt }) => Object.freeze({ id, identity, meta, joinedAt })),
    )
    const value = Object.freeze({
      id: this.roomId,
      meta: this.meta,
      count: this.count,
      isClosed: this.closed,
      participants,
    })
    releaseSubordinate(value)
    this._snapshotCache = { version: this._stateVersion, value: new WeakRef(value) }
    return value
  }
  /** State changed observably — invalidate the snapshot (via the version) and tell `onChange`
   *  subscribers, unless a change batch is open, in which case the single notification is deferred to
   *  its close (see `_batchChange`). */
  private _bumpState(): void {
    this._stateVersion++
    if (this._changeBatchDepth > 0) this._changeDeferred = true
    else this._fireAll(this._changeCbs)
  }
  /** Run `fn` with `onChange` coalesced: bumps inside still advance the version, but the subscriber notification fires once when the outermost batch closes, and only if something actually bumped.
   * Semantic callbacks (`onJoin`/`onLeave`/`onUpdate`) still fire per event — only the snapshot- invalidation signal is batched.
   */
  private _batchChange<T>(fn: () => T): T {
    this._changeBatchDepth++
    try {
      return fn()
    } finally {
      if (--this._changeBatchDepth === 0 && this._changeDeferred) {
        this._changeDeferred = false
        this._fireAll(this._changeCbs)
      }
    }
  }
  /** Membership changed: guard async KV reconciles against going stale, and narrate the change. */
  private _bumpMembership(): void {
    this.membershipVersion++
    this._bumpState()
  }
  onAnnounce(cb: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this._register(this._announceCbs, cb)
  }
  // ── Event application ──
  applyJoin(id: string, meta: ParticipantMeta, joinedAt: number, identity?: string | null, hidden?: boolean): void {
    if (this.closed) return
    const existing = this._members.get(id)
    if (existing) {
      // The origin absorbing its own join echo. The event carries the seq-0 join meta, so it must not regress a value a later p-meta already advanced; `joinedAt` is immutable, so it's a no-op.
      if (existing.metaSeq === 0) existing.meta = ownMetadata(meta)
      return
    }
    const entry = this._createEntry({ id, meta, joinedAt, metaSeq: 0, identity, hidden })
    // A hidden participant is not counted: it never moves the count, fires no `onJoin`, and can't fill the room — it's not narrated as a presence event. But the roster did change, so `onChange` still
    // fires and observers re-read (its join is announced on the control lane, so already-connected observers learn of it live, not only from a fresh roster).
    if (entry.hidden) {
      this._bumpMembership()
      return
    }
    this._seedCount++ // pre-reconcile, `count` tracks the seed adjusted by applied events
    this._bumpMembership()
    this._fireAll(this._joinCbs, this._remote(entry))
  }
  applyLeave(id: string, cause?: LeaveCause): void {
    const entry = this._members.get(id)
    if (!entry) {
      this.membershipVersion++
      return
    }
    entry.left = true
    entry.leaveCause = cause
    const remote = this._remote(entry)
    this._members.delete(id)
    // A hidden participant leaving is invisible to presence — no count change, no room-level `onLeave`, and it's never the "last participant" that empties the room. Its own leave handler and listener
    // release still run. The participant path keeps its exact original ordering.
    if (!entry.hidden) this._seedCount = Math.max(0, this._seedCount - 1)
    this._bumpMembership()
    this._fireAll(entry.leaveCbs, cause)
    if (!entry.hidden) this._fireAll(this._leaveCbs, remote, cause)
    this._releaseEntryListeners(entry)
    releaseSubordinate(remote)
    if (entry.hidden) return
    if (this.count === 0) this._fireAll(this._emptyCbs)
  }
  /** Applies only revisions newer than the entry's — the origin's echo (same seq) and events arriving behind a fresher reconcile are absorbed. */
  applyParticipantMeta(id: string, meta: ParticipantMeta, seq: number): void {
    const entry = this._members.get(id)
    if (!entry) {
      this.membershipVersion++
      return
    }
    if (seq <= entry.metaSeq) return
    const prev = entry.meta
    entry.metaSeq = seq
    const next = ownMetadata(meta)
    entry.meta = next
    this._bumpState()
    this._fireAll(entry.updateCbs, next, prev)
    this._fireAll(this._participantUpdateCbs, this._remote(entry), next, prev)
  }
  /** Last-writer-wins by `(at, by)`: concurrent `Room.setMeta()`s converge to the same winner on every node regardless of arrival order, and the origin's echo (same stamp) is absorbed. `prev` is
   * derived here, not shipped: it's the meta THIS view is transitioning away from, which under LWW can differ per node (a view that skipped an intermediate update never held the writer's `prev`).
   */
  applyRoomUpdate(meta: RoomMeta, at: number, by: string): void {
    if (!stampNewer({ at, by }, this._updateStamp)) return
    const prev = this.meta
    this._updateStamp = { at, by }
    const next = ownMetadata(meta)
    this.meta = next
    this._bumpState()
    this._fireAll(this._updateCbs, next, prev)
  }
  /** The stamp of the config this view currently reflects (serialized into room snapshots). */
  get updateStamp(): { at: number; by: string } {
    return this._updateStamp
  }
  /** Room closed: member-level cleanup callbacks run (decoders etc.), then `onClose`. Room-level `onLeave`/`onEmpty` intentionally don't fire — `onClose` is the signal. */
  applyClosed(cause: LeaveCause = { type: 'closed' }): void {
    if (this.closed) return
    this._batchChange(() => {
      const departed = [...this._members.values()]
      this.closed = true
      this._rosterKnown = true // authoritatively empty
      this._members.clear()
      this._bumpMembership()
      // State and snapshot are already closed-and-empty when cleanup callbacks run.
      for (const entry of departed) {
        entry.left = true
        entry.leaveCause = cause
        this._fireAll(entry.leaveCbs, cause)
        this._releaseEntryListeners(entry)
        const remote = entry.remoteRef?.deref()
        if (remote) releaseSubordinate(remote)
      }
    })
    this._fireAll(this._closeCbs)
    this._releaseAllListeners()
  }
  applyAnnounce(data: unknown, info: ChannelPublishInfo): void {
    this._fireAll(this._announceCbs, data, info)
  }
  /** Messages never wait on the roster: `from` is the live `RemoteParticipant` when this view knows the sender, else the `{ id, meta }` snapshot the sender's node stamped into the envelope. Control
   * and data travel on separate lanes, so a message can beat its sender's join — identity is in the message, delivery is immediate, and nothing drops.
   */
  applyData(
    from: string,
    fromMeta: ParticipantMeta,
    fromIdentity: string | null,
    data: unknown,
    info: ChannelPublishInfo,
  ): void {
    const entry = this._members.get(from)
    this._fireAll(
      this._roomDataCbs,
      data,
      info,
      entry ? this._remote(entry) : { id: from, meta: fromMeta, identity: fromIdentity },
    )
    if (entry) this._fireAll(entry.dataCbs, data, info)
  }
  /** Binary frames carry only the sender's ID — a pre-join frame surfaces as `{ id, meta: {} }` (rare: binary pipelines attach per member via `onJoin`, so the roster is normally ahead). `track`/`meta`
   * come from the frame header; listeners with a `track` filter receive only that track's frames.
   */
  applyBinary(
    from: string,
    payload: Uint8Array,
    track: string | null,
    meta: Record<string, unknown> | null,
    info: ChannelPublishInfo,
  ): void {
    const frameInfo: ChannelPublishInfo & BinaryFrameInfo = { ...info, track, meta }
    const entry = this._members.get(from)
    const sender = entry ? this._remote(entry) : { id: from, meta: {}, identity: null }
    this._fireTrackFiltered(this._roomBinaryCbs, track, (cb) => cb(payload, frameInfo, sender))
    if (entry) this._fireTrackFiltered(entry.binaryCbs, track, (cb) => cb(payload, frameInfo))
  }
  /** Fire the listeners whose track filter admits `track` (`undefined` = every track). */
  private _fireTrackFiltered<CB>(
    cbs: Array<{ cb: CB; track: TrackFilter }>,
    track: string | null,
    invoke: (cb: CB) => unknown,
  ): void {
    for (const { cb, track: want } of [...cbs]) {
      if (want !== undefined && want !== track) continue
      this._invoke(invoke, cb)
    }
  }
  /** Resync against an authoritative membership snapshot. The first reconcile is the roster *load* and is silent (the documented pattern reads `getParticipants()` and then follows events — narrating
   * the load would double-render it). Every later reconcile is discovered *drift*, and an observed view never mutates silently: the diff replays through the normal appliers, so
   * `onJoin`/`onLeave`/`onUpdate` fire exactly once per real change — whichever path (event or reconcile) learns of it first, the other is absorbed as an echo. Returns whether anything drifted, so
   * the owner can re-sync downstream views (client stubs).
   */
  reconcile(members: MemberSnapshot[], rosterOmitsHidden = false): boolean {
    // Coalesce the whole resync into one `onChange`: a reconcile that drifts N members invalidates the
    // view once, not once per member, while each member's onJoin/onLeave/onUpdate still fires as usual.
    return this._batchChange(() => {
      // A client's streamed roster carries only presence members (hidden ones are server-only), so a hidden entry it holds is a directly-granted handle, not roster-managed — never reap it here.
      const roster = rosterOmitsHidden ? members.filter((m) => !m.hidden) : members
      const keepsHidden = (id: string) => rosterOmitsHidden && this.isHidden(id)
      if (!this._rosterKnown) {
        // First load: silent — but entries can already exist (pre-roster join events, revived views). Those objects must survive: listeners hang off them and revived handles must stay `===` with the
        // view. Keep the object, refresh the facts; entries the authoritative roster doesn't know left before the load — their leave is narrated like any other.
        this._rosterKnown = true
        const seen = new Set<string>()
        for (const member of roster) {
          seen.add(member.id)
          const existing = this._members.get(member.id)
          if (!existing) {
            this._createEntry(member)
            continue
          }
          if (member.metaSeq > existing.metaSeq) {
            existing.meta = ownMetadata(member.meta)
            existing.metaSeq = member.metaSeq
          }
          existing.joinedAt = member.joinedAt
          for (const track of member.tracks ?? []) existing.tracks.add(track)
        }
        for (const id of [...this._members.keys()]) {
          if (!seen.has(id) && !keepsHidden(id)) this.applyLeave(id)
        }
        // Bump strictly after the roster is populated: the batch's single `onChange` fires at close, so a subscriber that synchronously reads `snapshot()` (the `useSyncExternalStore` contract) sees
        // the full roster — the silent `_createEntry`s above never bump, so this is what invalidates it.
        this._bumpMembership()
        return false
      }
      let drifted = false
      let silentChange = false // a `tracks` refresh no applier narrated (`joinedAt` is immutable)
      const seen = new Set<string>()
      for (const member of roster) {
        seen.add(member.id)
        const entry = this._members.get(member.id)
        if (!entry) {
          this.applyJoin(member.id, member.meta, member.joinedAt, member.identity, member.hidden)
          const created = this._members.get(member.id)
          if (created) {
            created.metaSeq = member.metaSeq
            for (const track of member.tracks ?? []) created.tracks.add(track)
          }
          drifted = true
        } else {
          if (member.metaSeq > entry.metaSeq) {
            this.applyParticipantMeta(member.id, member.meta, member.metaSeq)
            drifted = true
          }
          entry.joinedAt = member.joinedAt
          for (const track of member.tracks ?? []) {
            if (!entry.tracks.has(track)) {
              entry.tracks.add(track)
              silentChange = true
            }
          }
        }
      }
      for (const id of [...this._members.keys()]) {
        if (!seen.has(id) && !keepsHidden(id)) {
          this.applyLeave(id)
          drifted = true
        }
      }
      // Every applier bump (join/meta/leave) and the tracks-only refresh below coalesce into the one batched `onChange`. A tracks-only refresh no applier narrated still needs its own bump so the
      // snapshot isn't stranded; an echo reconcile that changed nothing bumps nothing, so it fires none.
      if (silentChange) this._bumpMembership()
      return drifted
    })
  }
  // ── Private ──
  private _createEntry(entrySeed: MemberSnapshot): MemberEntry {
    const { id, meta, joinedAt } = entrySeed
    const entry: MemberEntry = {
      id,
      meta: ownMetadata(meta),
      joinedAt,
      identity: entrySeed.identity ?? null,
      metaSeq: entrySeed.metaSeq,
      tracks: new Set(entrySeed.tracks),
      hidden: entrySeed.hidden === true,
      remoteRef: null,
      left: false,
      dataCbs: [],
      binaryCbs: [],
      updateCbs: [],
      leaveCbs: [],
    }
    this._members.set(id, entry)
    return entry
  }
  /** Preserve public handle identity while userland holds it, without making live state its owner. */
  private _remote(entry: MemberEntry): RemoteParticipant {
    let remote = entry.remoteRef?.deref()
    if (!remote) {
      remote = {
        id: entry.id,
        get meta() {
          return entry.meta
        },
        get joinedAt() {
          return entry.joinedAt
        },
        get identity() {
          return entry.identity
        },
        subscribe: (cb) => this._register(entry.dataCbs, cb),
        subscribeBinary: (cb, opts) => this._register(entry.binaryCbs, { cb, track: normalizeTrackFilter(opts) }),
        onUpdate: (cb) => this._register(entry.updateCbs, cb),
        onLeave: (cb: (cause?: LeaveCause) => void) => {
          if (!entry.left) return this._register(entry.leaveCbs, cb)
          this._invoke(cb, entry.leaveCause)
          return makeDisposer()
        },
      }
      entry.remoteRef = new WeakRef(remote)
      remoteBackings.set(remote, { state: this, entry })
    }
    if (typeof this._owner === 'object' && this._owner !== null) adoptSubordinateOf(this._owner, remote)
    return remote
  }
  private _register<T>(list: T[], cb: T): () => void {
    list.push(cb)
    this._bumpListenerCount(1)
    let cleanups = this._listenerCleanups.get(list)
    if (!cleanups) this._listenerCleanups.set(list, (cleanups = new Set()))
    const unlisten = makeDisposer(() => {
      const i = list.indexOf(cb)
      if (i >= 0) {
        list.splice(i, 1)
        this._bumpListenerCount(-1)
      }
    }, cleanups)
    if (typeof this._owner === 'object' && this._owner !== null) adoptSubordinateOf(this._owner, unlisten)
    return unlisten
  }
  /** A member entry is being discarded — its listeners die with it. Releasing them keeps the
   *  counters truthful (callers rarely unsubscribe in `onLeave`), which lets the owners drop
   *  wire/adapter subscriptions the departed member was holding open. */
  private _releaseEntryListeners(entry: MemberEntry): void {
    for (const list of [entry.dataCbs, entry.binaryCbs, entry.updateCbs, entry.leaveCbs]) {
      for (const unlisten of [...(this._listenerCleanups.get(list) ?? [])]) unlisten()
      this._listenerCleanups.delete(list)
    }
  }
  private _releaseAllListeners(): void {
    for (const cleanups of [...this._listenerCleanups.values()]) for (const unlisten of [...cleanups]) unlisten()
  }
  private _bumpListenerCount(delta: number): void {
    this._listenerCount += delta
    this._onListenersChanged()
  }
  private _fireAll<Args extends unknown[]>(cbs: Array<(...args: Args) => unknown>, ...args: Args): void {
    for (const cb of [...cbs]) this._invoke(cb, ...args)
  }
  private _invoke<Args extends unknown[]>(cb: (...args: Args) => unknown, ...args: Args): void {
    try {
      const result = cb(...args)
      if (isPromise(result)) void Promise.resolve(result).catch((err) => this._onCallbackError(err))
    } catch (err) {
      this._onCallbackError(err)
    }
  }
}
/** Validate a `subscribeBinary` track option: `undefined` = every track, `null` = the default lane, a non-empty name = that track. */
function normalizeTrackFilter(opts: { track?: string | null } | undefined): TrackFilter {
  assertUsage(
    opts === undefined || (typeof opts === 'object' && opts !== null && !Array.isArray(opts)),
    'subscribeBinary() options should be an object',
  )
  const track = opts?.track
  if (track === undefined || track === null) return track
  assertUsage(isRoomTrack(track) && track.length > 0, 'subscribeBinary() track should be a valid non-empty string')
  return track
}
/** Fold a listener list's track filters into the `TrackWants` they add up to. */
function trackWantsOf(cbs: ReadonlyArray<{ track: TrackFilter }>): TrackWants {
  const wants = emptyTrackWants()
  for (const { track } of cbs) {
    if (track === undefined) return { all: true, tracks: [] }
    const asTrack = track ?? DEFAULT_TRACK
    if (!wants.tracks.includes(asTrack)) wants.tracks.push(asTrack)
  }
  return wants
}
