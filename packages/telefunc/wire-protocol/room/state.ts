export { RoomState, RoomStateView, remoteBacking }
export type { MembershipEvent }

import { assert, assertUsage } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { invokeChannelListener, type ChannelPublishInfo } from '../channel.js'
import { untether } from '../wrapProxy.js'
import { makeDisposer } from './disposer.js'
import { ROOM_PLAN_SLICE_MS } from './constants.js'
import { nextTurnOf } from '../flow-control/macrotask-yield.js'
import { Listeners } from '../../utils/Listeners.js'
import { isNamedTrack, laneTrack, type BinaryFrame, type BinaryWantChange } from './binary.js'
import { assertKnownOptions, ownLeaveCause, ownMetadata, removedCause, senderOf } from './model.js'
import { RoomMetaState } from './meta-state.js'
import { MemberMeta } from './member-meta.js'
import type {
  AcceptedMeta,
  MemberMetaUpdate,
  MemberSnapshot,
  MemberWants,
  RoomDataEvent,
  RoomMetaEntry,
  RoomMetaUpdate,
  SenderStamp,
} from './protocol.js'
import type {
  BinaryFrameInfo,
  LeaveCause,
  ParticipantMeta,
  RemoteParticipant,
  RoomMeta,
  RoomSnapshotView,
  Sender,
} from './types.js'
/** A binary listener's track filter: `undefined` = every track, `null` = the default lane only, a name = that track only. */
type TrackFilter = string | null | undefined
/** A membership event as this view got it, whether it applied it or not. */
type MembershipEvent = { join: MemberSnapshot } | { leave: string } | { meta: { id: string; update: MemberMetaUpdate } }
/** A binary listener list's filters as counts: every-track listeners, and listeners per lane track. */
type TrackCounts = { all: number; named: Map<string, number> }
type CountedTrack = { counts: TrackCounts; track: TrackFilter }
type MemberEntry = {
  id: string
  /** Stale and echoed `p-meta` events are absorbed. */
  meta: MemberMeta
  joinedAt: number
  identity: string | null
  /** An off-presence participant: a member for routing/discovery, excluded from every presence read (`count`, `snapshot`, `onJoin`/`onLeave`/`onEmpty`). Any number per room. */
  hidden: boolean
  remote: RemoteParticipant | null
  left: boolean
  leaveCause?: LeaveCause
  // Each list is made when its first listener is added: most members never get one.
  dataCbs: Listeners<(data: unknown, info: ChannelPublishInfo) => unknown> | null
  binary: {
    cbs: Listeners<{
      cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo) => unknown
      track: TrackFilter
    }>
    tracks: TrackCounts
  } | null
  updateCbs: Listeners<(meta: ParticipantMeta, prev: ParticipantMeta) => void> | null
  leaveCbs: Listeners<(cause: LeaveCause) => void> | null
}
type RoomStateOptions = {
  owner: RoomStateView
  roomId: string
  /** In key order. */
  meta: readonly RoomMetaEntry[]
  /** Either the authoritative roster, or just its member count. A lazy view seeds with `{ count }` and learns the members from its first roster (read on the server, streamed to the client). */
  seed: { members: MemberSnapshot[] } | { count: number }
  /** How many meta writes `meta` reflects (see `applyRoomUpdate`). */
  metaSeq: number
  closed?: boolean
  /** Fired whenever the number of attached listeners changes, with the member whose own listeners changed (`null` for
   *  a room-level one), and the binary want it turned on or off, if any. Lets the owner (de)activate its event source
   *  (adapter subscription, wire subscription). */
  onListenersChanged: (member: string | null, binary: BinaryWantChange | null) => void
  /** A user callback threw. The owner decides how to report it. */
  onCallbackError: (err: unknown) => void
  /** Every leave, from an event or a reconciled roster, after this view's callbacks; `hidden` is null for a member
   *  this view didn't know. */
  onLeave: (id: string, cause: LeaveCause, hidden: boolean | null) => void
  /** A room meta change arrived ahead of one before it, and waits for it. */
  onMetaWaiting?: () => void
  /** How many tracks its binary listeners may name, each `(member or room, track)` pair once; unbounded if omitted. */
  trackWantsMax?: number
  /** A member of a roster still streaming in or applying, which an unstamped message may name. */
  pendingMember?: (id: string) => MemberSnapshot | undefined
  /** A member's meta change arrived ahead of one before it, which never arrived: the next roster read repairs it. */
  onMemberMetaGap?: () => void
}
/** Exact-keyed backing lets the serializer recover (room, member) without exposing a public brand. */
type RemoteBacking = { state: RoomState; entry: MemberEntry }
const { remoteBackings } = getGlobalObject('wire-protocol/room/state.ts', () => ({
  remoteBackings: new WeakMap<object, RemoteBacking>(),
}))
/** The `RoomState` backing of a minted `RemoteParticipant` (`null` for anything else). */
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
  onLeave(callback: (member: RemoteParticipant, cause: LeaveCause) => void): () => void {
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
  onChange(callback: () => void): () => void {
    return this._state.onChange(callback)
  }
}
/** A room's local view and callbacks, shared by server and client. A `join` for a known member or a `leave` for an
 *  unknown one fires no listener, so a snapshot and a concurrent event stream compose without double-firing. */
class RoomState {
  /** @internal The owning `ServerRoom`/`ClientRoom`, for serialization backing. */
  readonly _owner: RoomStateView
  readonly roomId: string
  closed: boolean
  /** Bumped on every observable change (membership, participant meta, room config, closure). Drives `onChange`/`snapshot()` cache invalidation. */
  private _stateVersion = 0
  private _snapshotCache: { version: number; value: WeakRef<RoomSnapshotView> } | null = null
  private readonly _listenerCleanups = new Map<object, Set<() => void>>()
  /** The membership events the in-flight roster read has seen so far. */
  private _recording: MembershipEvent[] | null = null
  private readonly _changeCbs: Listeners<() => void> = new Listeners()
  private readonly _members = new Map<string, MemberEntry>()
  private _hiddenMembers = 0
  private readonly _onListenersChanged: RoomStateOptions['onListenersChanged']
  private readonly _onCallbackError: (err: unknown) => void
  private readonly _onLeave: RoomStateOptions['onLeave']
  private readonly _roomDataCbs: Listeners<(data: unknown, info: ChannelPublishInfo, from: Sender) => unknown> =
    new Listeners()
  private readonly _roomBinaryCbs: Listeners<{
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown
    track: TrackFilter
  }> = new Listeners()
  private readonly _roomBinaryTracks: TrackCounts = newTrackCounts()
  /** The members with a `subscribe()` listener. */
  private readonly _textWanted = new Set<string>()
  private readonly _joinCbs: Listeners<(member: RemoteParticipant) => void> = new Listeners()
  private readonly _leaveCbs: Listeners<(member: RemoteParticipant, cause: LeaveCause) => void> = new Listeners()
  private readonly _participantUpdateCbs: Listeners<
    (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void
  > = new Listeners()
  private readonly _updateCbs: Listeners<(meta: RoomMeta, prev: RoomMeta) => void> = new Listeners()
  private readonly _emptyCbs: Listeners<() => void> = new Listeners()
  private readonly _closeCbs: Listeners<() => void> = new Listeners()
  private readonly _announceCbs: Listeners<(data: unknown, info: ChannelPublishInfo) => void> = new Listeners()
  private _listenerCount = 0
  /** The tracks its binary listeners name, each `(member or room, track)` pair once. */
  private _trackWants = 0
  private readonly _trackWantsMax: number
  private readonly _meta: RoomMetaState
  private readonly _onMetaWaiting: () => void
  private readonly _pendingMember: (id: string) => MemberSnapshot | undefined
  private readonly _onMemberMetaGap: () => void
  private _rosterKnown: boolean
  private _closedCause: LeaveCause = ownLeaveCause({ type: 'closed' })
  private _seedCount = 0
  /** The meta changes per member that reached this view before its first roster, in order. */
  private readonly _preRosterMeta = new Map<string, MemberMetaUpdate[]>()
  constructor(opts: RoomStateOptions) {
    this._owner = opts.owner
    this.roomId = opts.roomId
    this._meta = new RoomMetaState(opts.meta, opts.metaSeq)
    this.closed = opts.closed === true
    this._onMetaWaiting = opts.onMetaWaiting ?? (() => {})
    this._onListenersChanged = opts.onListenersChanged
    this._onCallbackError = opts.onCallbackError
    this._onLeave = opts.onLeave
    this._trackWantsMax = opts.trackWantsMax ?? Number.POSITIVE_INFINITY
    this._pendingMember = opts.pendingMember ?? (() => undefined)
    this._onMemberMetaGap = opts.onMemberMetaGap ?? (() => {})
    if ('members' in opts.seed) {
      this._rosterKnown = true
      for (const member of opts.seed.members) this._createEntry(member)
    } else {
      this._rosterKnown = false
      this._seedCount = opts.seed.count
    }
  }
  // ── Reads ──
  /** Before the roster is known: the seed count (which excludes hidden members) adjusted by the events since. */
  get count(): number {
    if (!this._rosterKnown) return this._seedCount
    return this._members.size - this._hiddenMembers
  }
  /** `join({ hidden: true })` members: routable, excluded from every presence read. */
  listHidden(): RemoteParticipant[] {
    return [...this._members.values()].filter((entry) => entry.hidden).map((entry) => this._remote(entry))
  }
  /** How the room closed for this view; `null` while it is open. */
  get closedCause(): LeaveCause | null {
    return this.closed ? this._closedCause : null
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
    return this._announceCbs.size > 0
  }
  /** A member's meta with the revision it was accepted at. */
  acceptedMeta(id: string): { meta: ParticipantMeta; seq: number } | null {
    const entry = this._members.get(id)
    return entry ? { meta: entry.meta.object, seq: entry.meta.seq } : null
  }
  /** How many meta writes of the member this view reflects; null for a member it doesn't know. */
  memberMetaSeq(id: string): number | null {
    return this._members.get(id)?.meta.seq ?? null
  }
  hasMember(id: string): boolean {
    return this._members.has(id)
  }
  /** Null for a member this view doesn't know. */
  memberHidden(id: string): boolean | null {
    return this._members.get(id)?.hidden ?? null
  }
  /** `all` while room-level `subscribe()`rs exist, and the members with participant-scoped listeners either way. */
  textWants(): MemberWants {
    return { all: this._roomDataCbs.size > 0, members: this._textWanted }
  }
  getRemote(id: string): RemoteParticipant | null {
    const entry = this._members.get(id)
    return entry ? this._remote(entry) : null
  }
  listVisible(): RemoteParticipant[] {
    return [...this._members.values()].filter((entry) => !entry.hidden).map((entry) => this._remote(entry))
  }
  /** Member IDs currently known, live. Drives the per-member binary key subscriptions. */
  memberIds(): IterableIterator<string> {
    return this._members.keys()
  }
  /** A member `memberIds()` listed, as a roster carries it. */
  memberSnapshot(id: string): MemberSnapshot {
    const { meta, joinedAt, identity, hidden } = this._members.get(id)!
    return { id, meta: meta.object, joinedAt, metaSeq: meta.seq, identity, ...(hidden ? { hidden: true } : {}) }
  }
  /** Bumped on every observable change: a roster read at one version lists the same members as any other read at it. */
  get version(): number {
    return this._stateVersion
  }
  /** A revived `RemoteParticipant`: the live entry if known, else seeded silently from the snapshot (its seed count already includes it). */
  ensureRemoteFromSnapshot(snap: MemberSnapshot): RemoteParticipant {
    const existing = this._members.get(snap.id)
    if (existing) return this._remote(existing)
    if (this.closed) {
      // A closed room has no members: one it hands out left with it.
      const entry = this._newEntry(snap)
      entry.left = true
      entry.leaveCause = this._closedCause
      return this._remote(entry)
    }
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
    const listener = binaryListener(cb, opts)
    return this._register(this._roomBinaryCbs, listener, null, {
      counts: this._roomBinaryTracks,
      track: listener.track,
    })
  }
  onJoin(cb: (member: RemoteParticipant) => void): () => void {
    return this._register(this._joinCbs, cb)
  }
  onLeave(cb: (member: RemoteParticipant, cause: LeaveCause) => void): () => void {
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
    if (!this.closed) return this._register(this._closeCbs, cb)
    // Terminal, like a departed member's onLeave: a late listener hears it at once.
    invokeChannelListener(cb, [], this._onCallbackError)
    return makeDisposer()
  }
  onChange(cb: () => void): () => void {
    return this._register(this._changeCbs, cb)
  }
  onAnnounce(cb: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this._register(this._announceCbs, cb)
  }
  /** Immutable view of the whole room, cached by state version, so the reference is stable until something actually changes (the `useSyncExternalStore` contract). */
  snapshot(): RoomSnapshotView {
    const cached = this._snapshotCache?.version === this._stateVersion ? this._snapshotCache.value.deref() : undefined
    if (cached) return cached
    const participants = Object.freeze(
      [...this._members.values()]
        .filter((entry) => !entry.hidden)
        .map(({ id, identity, meta, joinedAt }) => Object.freeze({ id, identity, meta: meta.object, joinedAt })),
    )
    const value = Object.freeze({
      id: this.roomId,
      meta: this.meta,
      count: this.count,
      isClosed: this.closed,
      participants,
    })
    untether(value) // plain data: holding a snapshot must not keep the room's wrapper alive
    this._snapshotCache = { version: this._stateVersion, value: new WeakRef(value) }
    return value
  }
  /** State changed observably: invalidate the snapshot and tell `onChange` subscribers. */
  private _bumpState(): void {
    this._stateVersion++
    this._fireAll(this._changeCbs)
  }
  /** Runs `read`, then `settle` with what it read, recording the membership events this view gets until `settle` ends;
   *  `settle` sees the events grow. */
  async recordingMembership<T, R>(
    read: () => Promise<T>,
    settle: (value: T, events: readonly MembershipEvent[]) => R | Promise<R>,
  ): Promise<R> {
    const events = this.recordMembership()
    try {
      return await settle(await read(), events)
    } finally {
      this.stopRecordingMembership()
    }
  }
  /** Records the membership events this view gets into the returned list, until `stopRecordingMembership()`. */
  recordMembership(): readonly MembershipEvent[] {
    assert(this._recording === null)
    this._recording = []
    return this._recording
  }
  stopRecordingMembership(): void {
    this._recording = null
  }
  private _record(event: MembershipEvent): void {
    this._recording?.push(event)
  }
  // ── Event application ──
  applyJoin(member: MemberSnapshot): boolean {
    this._record({ join: member })
    if (this.closed) return false
    // A known member's join echo has nothing new: its meta is the admission meta, frozen before any guard.
    if (this._members.has(member.id)) return false
    const entry = this._createEntry(member)
    // A hidden participant is no presence event (no count, no `onJoin`), but the roster changed, so `onChange` fires.
    if (entry.hidden) {
      this._bumpState()
      return true
    }
    if (!this._rosterKnown) this._seedCount++ // pre-roster, `count` is the seed adjusted by applied events
    this._bumpState()
    if (this._joinCbs.size > 0) this._fireAll(this._joinCbs, this._remote(entry))
    return true
  }
  applyLeave(id: string, cause: LeaveCause): void {
    this._record({ leave: id })
    const entry = this._members.get(id)
    if (entry) this._removeEntry(entry, ownLeaveCause(cause))
    this._onLeave(id, cause, entry ? entry.hidden : null)
  }
  private _removeEntry(entry: MemberEntry, cause: LeaveCause): void {
    entry.left = true
    entry.leaveCause = cause
    this._members.delete(entry.id)
    if (entry.hidden) this._hiddenMembers--
    // A hidden participant's leave is no presence event either; its own handlers and listener release still run.
    if (!entry.hidden && !this._rosterKnown) this._seedCount = Math.max(0, this._seedCount - 1)
    this._bumpState()
    if (entry.leaveCbs) this._fireAll(entry.leaveCbs, cause)
    if (!entry.hidden && this._leaveCbs.size > 0) this._fireAll(this._leaveCbs, this._remote(entry), cause)
    this._releaseEntryListeners(entry)
    if (entry.hidden) return
    if (this.count === 0) this._fireAll(this._emptyCbs)
  }
  /** Applies updates in seq order (see `MemberMeta.apply`): the origin's echo (same seq) and events arriving behind a
   *  fresher reconcile are absorbed. Only a listener builds the meta object per update. */
  applyParticipantMeta(id: string, update: MemberMetaUpdate): boolean {
    this._record({ meta: { id, update } })
    const entry = this._members.get(id)
    if (!entry) {
      // Before the first roster every member is unknown, and the first roster applies the changes after its own.
      if (!this._rosterKnown) {
        const early = this._preRosterMeta.get(id)
        if (early === undefined) this._preRosterMeta.set(id, [update])
        else early.push(update)
      }
      return false
    }
    const listened = entry.updateCbs !== null || this._participantUpdateCbs.size > 0
    const prev = listened ? entry.meta.object : null
    const applied = entry.meta.apply(update)
    if (applied === 'gap') this._onMemberMetaGap()
    if (applied !== 'applied') return false
    this._bumpState()
    if (prev === null) return true
    const next = entry.meta.object
    if (entry.updateCbs) this._fireAll(entry.updateCbs, next, prev)
    this._fireAll(this._participantUpdateCbs, this._remote(entry), next, prev)
    return true
  }
  /** Applies updates in `seq` order (see `RoomMetaState.apply`), so every instance converges; returns the ones applied.
   *  `prev` is this view's own previous meta, which can differ per instance. */
  applyRoomUpdate(update: RoomMetaUpdate): RoomMetaUpdate[] {
    const prev = this._updateCbs.size > 0 ? this._meta.object : null
    const applied = this._meta.apply(update)
    if (this._meta.waiting) this._onMetaWaiting()
    if (applied.length === 0) return applied
    this._bumpState()
    if (prev !== null) this._fireAll(this._updateCbs, this._meta.object, prev)
    return applied
  }
  /** Built when read, and kept until the next change. */
  get meta(): RoomMeta {
    return this._meta.object
  }
  /** In key order, as a snapshot or a whole `update` carries it. */
  metaEntries(): RoomMetaEntry[] {
    return this._meta.entries()
  }
  /** How many meta writes this view's meta reflects (serialized into room snapshots). */
  get metaSeq(): number {
    return this._meta.seq
  }
  get metaWaiting(): boolean {
    return this._meta.waiting
  }
  /** Room closed: member-level cleanup callbacks run (decoders etc.), then `onClose`. Room-level `onLeave`/`onEmpty` intentionally don't fire: `onClose` is the signal. */
  applyClosed(cause: LeaveCause = { type: 'closed' }): boolean {
    if (this.closed) return false
    cause = ownLeaveCause(cause)
    this._closedCause = cause
    const departed = [...this._members.values()]
    this.closed = true
    this._rosterKnown = true // authoritatively empty
    this._members.clear()
    this._hiddenMembers = 0
    this._bumpState()
    // State and snapshot are already closed-and-empty when cleanup callbacks run.
    for (const entry of departed) {
      entry.left = true
      entry.leaveCause = cause
      if (entry.leaveCbs) this._fireAll(entry.leaveCbs, cause)
      this._releaseEntryListeners(entry)
    }
    this._fireAll(this._closeCbs)
    this._releaseAllListeners()
    return true
  }
  applyAnnounce(data: unknown, info: ChannelPublishInfo): void {
    this._fireAll(this._announceCbs, data, info)
  }
  /** Never waits on the roster: an unknown sender (its join may be behind on the control lane) is the snapshot its instance stamped. */
  applyData(event: RoomDataEvent, info: ChannelPublishInfo): void {
    const sender = this.resolveSender(event)
    this._fireAll(this._roomDataCbs, event.data, info, sender)
    const entry = this._members.get(event.from)
    if (entry?.dataCbs) this._fireAll(entry.dataCbs, event.data, info)
  }
  /** A message's sender as of its send: a stamp newer than the member's meta here applies first, as the meta update it
   *  is. Unstamped, the sender is one this view was sent, maybe in a roster still applying. */
  resolveSender({ from, fromMeta, fromMetaSeq, fromIdentity }: { from: string } & Partial<SenderStamp>): Sender {
    const entry = this._members.get(from)
    if (fromMeta !== undefined && fromMetaSeq !== undefined) {
      if (!entry) return senderOf(from, fromMeta, fromIdentity ?? null)
      if (entry.meta.seq < fromMetaSeq) this.applyParticipantMeta(from, { seq: fromMetaSeq, meta: fromMeta })
      return this._remote(entry)
    }
    const pending = this._pendingMember(from)
    if (entry) {
      if (pending !== undefined && pending.metaSeq > entry.meta.seq)
        this.applyParticipantMeta(from, { seq: pending.metaSeq, meta: pending.meta })
      return this._remote(entry)
    }
    assert(pending !== undefined)
    // With the meta changes relayed after the member's roster part.
    const meta = new MemberMeta(pending.meta, pending.metaSeq)
    for (const update of this._preRosterMeta.get(from) ?? []) meta.apply(update)
    return senderOf(from, meta.object, pending.identity ?? null)
  }
  /** A binary frame names only its sender's id: one from a member not yet known surfaces as `{ id, meta: {} }`. */
  applyBinary({ from, payload, track, meta }: BinaryFrame, info: ChannelPublishInfo): void {
    const frameInfo: ChannelPublishInfo & BinaryFrameInfo = { ...info, track, meta }
    const entry = this._members.get(from)
    const sender = entry ? this._remote(entry) : senderOf(from, {}, null)
    this._fireTrackFiltered(this._roomBinaryCbs, track, (cb) => cb(payload, frameInfo, sender))
    if (entry?.binary) this._fireTrackFiltered(entry.binary.cbs, track, (cb) => cb(payload, frameInfo))
  }
  /** Fire the listeners whose track filter admits `track` (`undefined` = every track). */
  private _fireTrackFiltered<CB>(
    cbs: Listeners<{ cb: CB; track: TrackFilter }>,
    track: string | null,
    invoke: (cb: CB) => unknown,
  ): void {
    for (const { cb, track: want } of cbs.list()) {
      if (want !== undefined && want !== track) continue
      invokeChannelListener(invoke, [cb], this._onCallbackError)
    }
  }
  /** The first roster loads silently; later ones narrate the drift they correct as events. A departing member stays
   *  until its leave event, which carries the cause. Runs `reconcilingRoster` a `ROOM_PLAN_SLICE_MS` slice per turn;
   *  null if the room closed between slices. */
  async reconcileRoster(
    roster: { members: MemberSnapshot[]; departing: ReadonlySet<string> },
    events: readonly MembershipEvent[],
  ): Promise<boolean | null> {
    const steps = this.reconcilingRoster(roster, events)
    let step = steps.next()
    for (let deadline = performance.now() + ROOM_PLAN_SLICE_MS; step.done !== true; step = steps.next()) {
      if (performance.now() < deadline) continue
      await nextTurnOf()
      if (this.closed) return null
      deadline = performance.now() + ROOM_PLAN_SLICE_MS
    }
    return step.value
  }
  /** Reconciles with a roster read a member per step, the membership `events` since the read began, which may grow
   *  between steps, replayed onto it first, so no step undoes one. Returns whether a drift was narrated. */
  *reconcilingRoster(
    roster: { members: MemberSnapshot[]; departing: ReadonlySet<string> },
    events: readonly MembershipEvent[],
  ): Generator<void, boolean> {
    const listed = new Map<string, MemberSnapshot>()
    for (const member of roster.members) {
      listed.set(member.id, member)
      yield
    }
    const departing = new Set(roster.departing)
    let replayed = 0
    const replay = () => {
      for (; replayed < events.length; replayed++) replayMembershipEvent(listed, departing, events[replayed]!)
    }
    replay()
    // The first roster narrates nothing, and `count` stays the seed until its members are in.
    const narrate = this._rosterKnown
    let narratedDrift = false
    // A live iteration: a member the replay removes is skipped, one it adds is merged.
    for (const member of listed.values()) {
      narratedDrift = this._mergeRosterMember(member, narrate) || narratedDrift
      yield
      replay()
    }
    if (!narrate) {
      this._rosterKnown = true
      this._applyPreRosterMeta()
      this._bumpState()
    }
    for (const id of [...this._members.keys()]) {
      if (this._members.has(id) && !listed.has(id) && !departing.has(id)) {
        // No leave event reached this view, so its cause is unknown: it is reported as removed.
        this.applyLeave(id, removedCause(undefined))
        narratedDrift = true
      }
      yield
      replay()
    }
    return narrate && narratedDrift
  }
  /** Whether the member's drift was narrated as events. */
  private _mergeRosterMember(member: MemberSnapshot, narrate: boolean): boolean {
    const entry = this._members.get(member.id)
    if (!entry) {
      if (!narrate) {
        this._createEntry(member)
        return false
      }
      this.applyJoin(member)
      return true
    }
    if (member.metaSeq <= entry.meta.seq) return false
    if (narrate) {
      this.applyParticipantMeta(member.id, { seq: member.metaSeq, meta: member.meta })
      return true
    }
    entry.meta.apply({ seq: member.metaSeq, meta: member.meta })
    return false
  }
  private _applyPreRosterMeta(): void {
    for (const [id, updates] of this._preRosterMeta) {
      const entry = this._members.get(id)
      if (entry) for (const update of updates) entry.meta.apply(update)
    }
    this._preRosterMeta.clear()
  }
  // ── Private ──
  private _createEntry(entrySeed: MemberSnapshot): MemberEntry {
    const entry = this._newEntry(entrySeed)
    this._members.set(entry.id, entry)
    if (entry.hidden) this._hiddenMembers++
    return entry
  }
  private _newEntry(entrySeed: MemberSnapshot): MemberEntry {
    const { id, meta, joinedAt } = entrySeed
    const entry: MemberEntry = {
      id,
      meta: new MemberMeta(meta, entrySeed.metaSeq),
      joinedAt,
      identity: entrySeed.identity ?? null,
      hidden: entrySeed.hidden === true,
      remote: null,
      left: false,
      dataCbs: null,
      binary: null,
      updateCbs: null,
      leaveCbs: null,
    }
    return entry
  }
  private _remote(entry: MemberEntry): RemoteParticipant {
    let remote = entry.remote
    if (!remote) {
      remote = {
        id: entry.id,
        get meta() {
          return entry.meta.object
        },
        get joinedAt() {
          return entry.joinedAt
        },
        get identity() {
          return entry.identity
        },
        subscribe: (cb) => this._registerLive(entry, (entry.dataCbs ??= new Listeners()), cb),
        subscribeBinary: (cb, opts) => {
          const listener = binaryListener(cb, opts)
          const binary = (entry.binary ??= { cbs: new Listeners(), tracks: newTrackCounts() })
          return this._registerLive(entry, binary.cbs, listener, { counts: binary.tracks, track: listener.track })
        },
        onUpdate: (cb) => this._registerLive(entry, (entry.updateCbs ??= new Listeners()), cb),
        onLeave: (cb: (cause: LeaveCause) => void) => {
          if (!entry.left) return this._register((entry.leaveCbs ??= new Listeners()), cb, entry)
          assert(entry.leaveCause)
          invokeChannelListener(cb, [entry.leaveCause], this._onCallbackError)
          return makeDisposer()
        },
      }
      entry.remote = remote
      remoteBackings.set(remote, { state: this, entry })
    }
    return remote
  }
  /** A departed member's listeners were released at its leave, so one added after it is never held. */
  private _registerLive<T>(entry: MemberEntry, list: Listeners<T>, cb: T, filter?: CountedTrack): () => void {
    if (entry.left) return makeDisposer()
    return this._register(list, cb, entry, filter)
  }
  /** `entry` owns the list (`null`: the room); a binary listener's `filter` is counted in its list's counts. */
  private _register<T>(list: Listeners<T>, cb: T, entry: MemberEntry | null = null, filter?: CountedTrack): () => void {
    assertUsage(
      filter?.track === undefined ||
        this._trackWants < this._trackWantsMax ||
        filter.counts.named.has(laneTrack(filter.track)),
      `Too many binary tracks subscribed on one room (${this._trackWantsMax} max): subscribe without \`{ track }\` to receive every track`,
    )
    const remove = list.add(cb)
    this._bumpListenerCount(
      1,
      entry,
      filter && countTrack(filter.counts, filter.track, 1) ? wantOf(entry, filter, true) : null,
    )
    let cleanups = this._listenerCleanups.get(list)
    if (!cleanups) this._listenerCleanups.set(list, (cleanups = new Set()))
    const unlisten = makeDisposer(() => {
      if (remove())
        this._bumpListenerCount(
          -1,
          entry,
          filter && countTrack(filter.counts, filter.track, -1) ? wantOf(entry, filter, false) : null,
        )
    }, cleanups)
    return unlisten
  }
  /** A discarded entry's listeners die with it, so the counters let owners drop what it held open. */
  private _releaseEntryListeners(entry: MemberEntry): void {
    for (const list of [entry.dataCbs, entry.binary?.cbs, entry.updateCbs, entry.leaveCbs]) {
      if (!list) continue
      for (const unlisten of [...(this._listenerCleanups.get(list) ?? [])]) unlisten()
      this._listenerCleanups.delete(list)
    }
  }
  private _releaseAllListeners(): void {
    for (const cleanups of [...this._listenerCleanups.values()]) for (const unlisten of [...cleanups]) unlisten()
  }
  private _bumpListenerCount(delta: number, entry: MemberEntry | null, binary: BinaryWantChange | null): void {
    this._listenerCount += delta
    if (binary !== null && binary.track !== null) this._trackWants += binary.on ? 1 : -1
    if (entry !== null) {
      if ((entry.dataCbs?.size ?? 0) > 0) this._textWanted.add(entry.id)
      else this._textWanted.delete(entry.id)
    }
    this._onListenersChanged(entry?.id ?? null, binary)
  }
  private _fireAll<Args extends unknown[]>(cbs: Listeners<(...args: Args) => unknown>, ...args: Args): void {
    for (const cb of cbs.list()) invokeChannelListener(cb, args, this._onCallbackError)
  }
}
function replayMembershipEvent(
  members: Map<string, MemberSnapshot>,
  departing: Set<string>,
  event: MembershipEvent,
): void {
  if ('join' in event) {
    if (!members.has(event.join.id)) members.set(event.join.id, event.join)
  } else if ('leave' in event) {
    members.delete(event.leave)
    departing.delete(event.leave)
  } else {
    const { id, update } = event.meta
    const member = members.get(id)
    if (member === undefined) return
    const meta = new MemberMeta(member.meta, member.metaSeq)
    if (meta.apply(update) === 'applied') members.set(id, { ...member, meta: meta.object, metaSeq: meta.seq })
  }
}
/** Validate a `subscribeBinary` track option: `undefined` = every track, `null` = the default lane, a non-empty name = that track. */
function normalizeTrackFilter(opts: { track?: string | null } | undefined): TrackFilter {
  assertUsage(
    opts === undefined || (typeof opts === 'object' && opts !== null && !Array.isArray(opts)),
    'subscribeBinary() options should be an object',
  )
  assertKnownOptions(opts, ['track'], 'subscribeBinary()')
  const track = opts?.track
  if (track === undefined || track === null) return track
  assertUsage(isNamedTrack(track), 'subscribeBinary() track should be a valid non-empty string')
  return track
}
function binaryListener<CB>(cb: CB, opts: { track?: string | null } | undefined): { cb: CB; track: TrackFilter } {
  return { cb, track: normalizeTrackFilter(opts) }
}
function newTrackCounts(): TrackCounts {
  return { all: 0, named: new Map() }
}
/** Whether the count crossed zero, turning a want on or off. */
function countTrack(counts: TrackCounts, track: TrackFilter, delta: 1 | -1): boolean {
  if (track === undefined) {
    counts.all += delta
    return counts.all === (delta === 1 ? 1 : 0)
  }
  const key = laneTrack(track)
  const count = (counts.named.get(key) ?? 0) + delta
  if (count === 0) counts.named.delete(key)
  else counts.named.set(key, count)
  return count === (delta === 1 ? 1 : 0)
}
function wantOf(entry: MemberEntry | null, { track }: CountedTrack, on: boolean): BinaryWantChange {
  return { member: entry?.id ?? null, track: track === undefined ? null : laneTrack(track), on }
}
