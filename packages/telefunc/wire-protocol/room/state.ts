export { RoomState, RoomStateView, remoteBacking }

import { assertUsage } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { invokeChannelListener, type ChannelPublishInfo } from '../channel.js'
import { makeDisposer, untether } from '../wrapProxy.js'
import {
  emptyTrackWants,
  isNamedTrack,
  laneTrack,
  type BinaryFrame,
  type BinaryWants,
  type TrackWants,
} from './binary.js'
import { ROOM_WANTED_TRACKS_MAX } from './constants.js'
import { assertKnownOptions, ownLeaveCause, ownMetadata, senderOf, stampNewer } from './model.js'
import type { AcceptedMeta, MemberSnapshot, MemberWants, RoomDataEnvelope } from './protocol.js'
import type {
  BinaryFrameInfo,
  LeaveCause,
  ParticipantMeta,
  RemoteParticipant,
  RoomMeta,
  RoomSnapshotView,
  Sender,
} from './types.js'
// RoomState: the local view of a room, driven by the event stream
/** A binary listener's track filter: `undefined` = every track, `null` = the default lane only, a name = that track only. */
type TrackFilter = string | null | undefined
type MemberEntry = {
  id: string
  meta: ParticipantMeta
  joinedAt: number
  identity: string | null
  /** Latest applied meta revision. Stale and echoed `p-meta` events are absorbed. */
  metaSeq: number
  /** Named tracks the member is known to publish, grown by `track` events and rosters, never shrunk (tracks live as long as the member). Drives all-track key subscriptions. */
  tracks: Set<string>
  /** An off-presence participant: a member for routing/discovery, excluded from every presence read (`count`, `snapshot`, `onJoin`/`onLeave`/`onEmpty`). Any number per room. */
  hidden: boolean
  remote: RemoteParticipant | null
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
  /** Either the authoritative roster, or just its member count. A lazy view seeds with `{ count }` and learns the members from its first `reconcile()` (KV read / streamed roster). */
  seed: { members: MemberSnapshot[] } | { count: number }
  /** The LWW stamp of the config `meta` was read from (see `applyRoomUpdate`). */
  updateStamp: { at: number; by: string }
  closed?: boolean
  /** Fired whenever the number of attached listeners changes. Lets the owner (de)activate its event source (adapter subscription, wire subscription). */
  onListenersChanged: () => void
  /** A user callback threw. The owner decides how to report it. */
  onCallbackError: (err: unknown) => void
  /** Every leave, from an event or a reconciled roster, after this view's callbacks; `hidden` is null for a member
   *  this view didn't know. A leave with no cause had no event. */
  onLeave: (id: string, cause: LeaveCause | undefined, hidden: boolean | null) => void
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
  onChange(callback: () => void): () => void {
    return this._state.onChange(callback)
  }
}
/** A room's local view and callbacks, shared by server and client. A `join` for a known member or a `leave` for an
 *  unknown one is a no-op, so a snapshot and a concurrent event stream compose without double-firing. */
class RoomState {
  /** @internal The owning `ServerRoom`/`ClientRoom`, for serialization backing. */
  _owner: RoomStateView | null = null
  readonly roomId: string
  meta: RoomMeta
  closed: boolean
  /** Bumped on every membership change. Guards async KV reconciles against going stale. */
  membershipVersion = 0
  /** Bumped on every observable change (membership, participant meta, room config, closure). Drives `onChange`/`snapshot()` cache invalidation. */
  private _stateVersion = 0
  private _snapshotCache: { version: number; value: WeakRef<RoomSnapshotView> } | null = null
  private readonly _listenerCleanups = new Map<object, Set<() => void>>()
  private readonly _changeCbs: Array<() => void> = []
  private readonly _members = new Map<string, MemberEntry>()
  private readonly _onListenersChanged: () => void
  private readonly _onCallbackError: (err: unknown) => void
  private readonly _onLeave: RoomStateOptions['onLeave']
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
  private _closedCause: LeaveCause = ownLeaveCause({ type: 'closed' })
  private _seedCount = 0
  constructor(opts: RoomStateOptions) {
    this.roomId = opts.roomId
    this.meta = ownMetadata(opts.meta)
    this.closed = opts.closed === true
    this._updateStamp = opts.updateStamp
    this._onListenersChanged = opts.onListenersChanged
    this._onCallbackError = opts.onCallbackError
    this._onLeave = opts.onLeave
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
    return this._members.size - this._hiddenCount()
  }
  /** `join({ hidden: true })` members: routable, excluded from every presence read. */
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
  /** Which (member, track) binary streams this holder needs delivered. Drives the wire/adapter subscriptions on both sides (client declares it, server aggregates it per stub). */
  binaryWants(): BinaryWants {
    const members: Record<string, TrackWants> = Object.create(null)
    for (const entry of this._members.values()) {
      if (entry.binaryCbs.length > 0) members[entry.id] = trackWantsOf(entry.binaryCbs)
    }
    return { everyMember: trackWantsOf(this._roomBinaryCbs), members }
  }
  /** A member's meta with the revision it was accepted at. */
  acceptedMeta(id: string): AcceptedMeta | null {
    const entry = this._members.get(id)
    return entry ? { meta: entry.meta, seq: entry.metaSeq } : null
  }
  /** Named tracks the member is known to publish (`[]` for unknown members). */
  memberTracks(id: string): string[] {
    const entry = this._members.get(id)
    return entry ? [...entry.tracks] : []
  }
  /** The text-lane twin of `binaryWants()`: `all` while room-level `subscribe()`rs exist, and the members with participant-scoped listeners either way. */
  textWants(): MemberWants {
    const members: string[] = []
    for (const entry of this._members.values()) {
      if (entry.dataCbs.length > 0) members.push(entry.id)
    }
    return { all: this._roomDataCbs.length > 0, members }
  }
  getRemote(id: string): RemoteParticipant | null {
    const entry = this._members.get(id)
    return entry ? this._remote(entry) : null
  }
  listVisible(): RemoteParticipant[] {
    return [...this._members.values()].filter((entry) => !entry.hidden).map((entry) => this._remote(entry))
  }
  /** Member IDs currently known. Drives the per-member binary key subscriptions. */
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
    return this._register(this._roomBinaryCbs, binaryListener(this._roomBinaryCbs, cb, opts))
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
  /** A member published its first frame on a new named track (idempotent: echoes, rosters, and the owner's local apply all land here). */
  applyTrack(id: string, track: string): void {
    const entry = this._members.get(id)
    if (entry) entry.tracks.add(track)
    else this._markUnknownMember()
  }
  /** Immutable view of the whole room, cached by state version, so the reference is stable until something actually changes (the `useSyncExternalStore` contract). */
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
    untether(value) // plain data: holding a snapshot must not keep the room's wrapper alive
    this._snapshotCache = { version: this._stateVersion, value: new WeakRef(value) }
    return value
  }
  /** State changed observably: invalidate the snapshot and tell `onChange` subscribers. */
  private _bumpState(): void {
    this._stateVersion++
    this._fireAll(this._changeCbs)
  }
  /** Membership changed: guard async KV reconciles against going stale, and narrate the change. */
  private _bumpMembership(): void {
    this.membershipVersion++
    this._bumpState()
  }
  /** An event for a member this view doesn't know means the roster drifted, so an in-flight roster read is stale. */
  private _markUnknownMember(): void {
    this.membershipVersion++
  }
  // ── Event application ──
  applyJoin(member: MemberSnapshot): void {
    if (this.closed) return
    // A known member's join echo has nothing new: its meta is the admission meta, frozen before any guard.
    if (this._members.has(member.id)) return
    const entry = this._createEntry(member)
    // A hidden participant is no presence event (no count, no `onJoin`), but the roster changed, so `onChange` fires.
    if (entry.hidden) {
      this._bumpMembership()
      return
    }
    if (!this._rosterKnown) this._seedCount++ // pre-roster, `count` is the seed adjusted by applied events
    this._bumpMembership()
    this._fireAll(this._joinCbs, this._remote(entry))
  }
  applyLeave(id: string, cause?: LeaveCause): void {
    const entry = this._members.get(id)
    if (entry) this._removeEntry(entry, cause && ownLeaveCause(cause))
    else this._markUnknownMember()
    this._onLeave(id, cause, entry ? entry.hidden : null)
  }
  private _removeEntry(entry: MemberEntry, cause: LeaveCause | undefined): void {
    entry.left = true
    entry.leaveCause = cause
    const remote = this._remote(entry)
    this._members.delete(entry.id)
    // A hidden participant's leave is no presence event either; its own handlers and listener release still run.
    if (!entry.hidden && !this._rosterKnown) this._seedCount = Math.max(0, this._seedCount - 1)
    this._bumpMembership()
    this._fireAll(entry.leaveCbs, cause)
    if (!entry.hidden) this._fireAll(this._leaveCbs, remote, cause)
    this._releaseEntryListeners(entry)
    if (entry.hidden) return
    if (this.count === 0) this._fireAll(this._emptyCbs)
  }
  /** Applies only revisions newer than the entry's: the origin's echo (same seq) and events arriving behind a fresher reconcile are absorbed. */
  applyParticipantMeta(id: string, meta: ParticipantMeta, seq: number): void {
    const entry = this._members.get(id)
    if (!entry) {
      // Before the first roster every member is unknown: its meta change is no drift, and the heartbeat heals its meta.
      if (this._rosterKnown) this._markUnknownMember()
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
  /** Last-writer-wins by `(at, by)`, so every instance converges; `prev` is this view's own previous meta, which can
   *  differ per instance. `true` when the update applied. */
  applyRoomUpdate(meta: RoomMeta, at: number, by: string): boolean {
    if (!stampNewer({ at, by }, this._updateStamp)) return false
    const prev = this.meta
    this._updateStamp = { at, by }
    const next = ownMetadata(meta)
    this.meta = next
    this._bumpState()
    this._fireAll(this._updateCbs, next, prev)
    return true
  }
  /** The stamp of the config this view currently reflects (serialized into room snapshots). */
  get updateStamp(): { at: number; by: string } {
    return this._updateStamp
  }
  /** Room closed: member-level cleanup callbacks run (decoders etc.), then `onClose`. Room-level `onLeave`/`onEmpty` intentionally don't fire: `onClose` is the signal. */
  applyClosed(cause: LeaveCause = { type: 'closed' }): void {
    if (this.closed) return
    cause = ownLeaveCause(cause)
    this._closedCause = cause
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
    }
    this._fireAll(this._closeCbs)
    this._releaseAllListeners()
  }
  applyAnnounce(data: unknown, info: ChannelPublishInfo): void {
    this._fireAll(this._announceCbs, data, info)
  }
  /** Never waits on the roster: an unknown sender (its join may be behind on the control lane) is the snapshot its instance stamped. */
  applyData(event: RoomDataEnvelope, info: ChannelPublishInfo): void {
    const entry = this._members.get(event.from)
    const sender = entry ? this._remote(entry) : senderOf(event.from, event.fromMeta, event.fromIdentity ?? null)
    this._fireAll(this._roomDataCbs, event.data, info, sender)
    if (entry) this._fireAll(entry.dataCbs, event.data, info)
  }
  /** A binary frame names only its sender's id: one from a member not yet known surfaces as `{ id, meta: {} }`. */
  applyBinary({ from, payload, track, meta }: BinaryFrame, info: ChannelPublishInfo): void {
    const frameInfo: ChannelPublishInfo & BinaryFrameInfo = { ...info, track, meta }
    const entry = this._members.get(from)
    const sender = entry ? this._remote(entry) : senderOf(from, {}, null)
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
      invokeChannelListener(invoke, [cb], this._onCallbackError)
    }
  }
  /** The first roster loads silently; later ones narrate the drift they correct as events. A departing member stays
   *  until its leave event, which carries the cause. */
  reconcileRoster(roster: MemberSnapshot[], departing: ReadonlySet<string> = new Set()): boolean {
    const narrate = this._rosterKnown
    this._rosterKnown = true
    let narratedDrift = false
    let viewChanged = false
    for (const member of roster) {
      const outcome = this._mergeRosterMember(member, narrate)
      narratedDrift ||= outcome.narrated
      viewChanged ||= outcome.viewChanged
    }
    const listed = new Set([...roster.map((member) => member.id), ...departing])
    narratedDrift = this._removeMissingMembers(listed) || narratedDrift
    if (!narrate) {
      this._bumpMembership()
      return false
    }
    if (viewChanged && !narratedDrift) this._bumpMembership()
    return narratedDrift
  }
  private _mergeRosterMember(member: MemberSnapshot, narrate: boolean): { narrated: boolean; viewChanged: boolean } {
    const entry = this._members.get(member.id)
    if (!entry) {
      if (!narrate) {
        this._createEntry(member)
        return { narrated: false, viewChanged: true }
      }
      this.applyJoin(member)
      return { narrated: true, viewChanged: false }
    }
    let narrated = false
    if (member.metaSeq > entry.metaSeq) {
      if (narrate) {
        this.applyParticipantMeta(member.id, member.meta, member.metaSeq)
        narrated = true
      } else {
        entry.meta = ownMetadata(member.meta)
        entry.metaSeq = member.metaSeq
      }
    }
    entry.joinedAt = member.joinedAt
    return { narrated, viewChanged: this._mergeKnownTracks(entry, member.tracks) }
  }
  private _mergeKnownTracks(entry: MemberEntry, tracks: string[] | undefined): boolean {
    const before = entry.tracks.size
    for (const track of tracks ?? []) entry.tracks.add(track)
    return entry.tracks.size !== before
  }
  private _removeMissingMembers(seen: Set<string>): boolean {
    let removed = false
    for (const id of [...this._members.keys()]) {
      if (seen.has(id)) continue
      this.applyLeave(id)
      removed = true
    }
    return removed
  }
  // ── Private ──
  private _createEntry(entrySeed: MemberSnapshot): MemberEntry {
    const entry = this._newEntry(entrySeed)
    this._members.set(entry.id, entry)
    return entry
  }
  private _newEntry(entrySeed: MemberSnapshot): MemberEntry {
    const { id, meta, joinedAt } = entrySeed
    const entry: MemberEntry = {
      id,
      meta: ownMetadata(meta),
      joinedAt,
      identity: entrySeed.identity ?? null,
      metaSeq: entrySeed.metaSeq,
      tracks: new Set(entrySeed.tracks),
      hidden: entrySeed.hidden === true,
      remote: null,
      left: false,
      dataCbs: [],
      binaryCbs: [],
      updateCbs: [],
      leaveCbs: [],
    }
    return entry
  }
  private _remote(entry: MemberEntry): RemoteParticipant {
    let remote = entry.remote
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
        subscribe: (cb) => this._registerLive(entry, entry.dataCbs, cb),
        subscribeBinary: (cb, opts) =>
          this._registerLive(entry, entry.binaryCbs, binaryListener(entry.binaryCbs, cb, opts)),
        onUpdate: (cb) => this._registerLive(entry, entry.updateCbs, cb),
        onLeave: (cb: (cause?: LeaveCause) => void) => {
          if (!entry.left) return this._register(entry.leaveCbs, cb)
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
  private _registerLive<T>(entry: MemberEntry, list: T[], cb: T): () => void {
    return entry.left ? makeDisposer() : this._register(list, cb)
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
    return unlisten
  }
  /** A discarded entry's listeners die with it, so the counters let owners drop what it held open. */
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
    for (const cb of [...cbs]) invokeChannelListener(cb, args, this._onCallbackError)
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
function binaryListener<CB>(
  cbs: ReadonlyArray<{ track: TrackFilter }>,
  cb: CB,
  opts: { track?: string | null } | undefined,
): { cb: CB; track: TrackFilter } {
  const listener = { cb, track: normalizeTrackFilter(opts) }
  // Named tracks count even beside an all-track listener, whose removal would declare them all.
  const named = new Set([...cbs, listener].flatMap(({ track }) => (track === undefined ? [] : [laneTrack(track)])))
  assertUsage(
    named.size <= ROOM_WANTED_TRACKS_MAX,
    `subscribeBinary() can name at most ${ROOM_WANTED_TRACKS_MAX} tracks per participant, the default track included, and as many room-wide; subscribe without a track to receive every track`,
  )
  return listener
}
/** Fold a listener list's track filters into the `TrackWants` they add up to. */
function trackWantsOf(cbs: ReadonlyArray<{ track: TrackFilter }>): TrackWants {
  const wants = emptyTrackWants()
  for (const { track } of cbs) {
    if (track === undefined) return { all: true, tracks: [] }
    const asTrack = laneTrack(track)
    if (!wants.tracks.includes(asTrack)) wants.tracks.push(asTrack)
  }
  return wants
}
