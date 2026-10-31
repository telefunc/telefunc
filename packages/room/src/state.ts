export { RoomModel, RoomState, RoomStateView, remoteBacking }
export type { MembershipEvent, RoomModelOwner, RoomStateOwner }

import { assert, assertUsage } from './utils/assert.js'
import { getGlobalObject } from './utils/getGlobalObject.js'
import { invokeChannelListener } from './channel.js'
import { type ChannelPublishInfo, untether } from 'telefunc/__internal'
import { makeDisposer } from './disposer.js'
import { Slice } from './Slice.js'
import { Listeners } from './utils/Listeners.js'
import { getOrCreate } from './utils/getOrCreate.js'
import {
  EVERY_MEMBER,
  EVERY_TRACK,
  DEFAULT_TRACK,
  isNamedTrack,
  publicTrack,
  type EveryTrack,
  type BinaryFrame,
  type BinaryWantChange,
  type TrackKey,
} from './binary.js'
import { assertKnownOptions, ownLeaveCause, removedCause, senderOf } from './model.js'
import { RoomMetaState } from './metaState.js'
import { MemberMeta, type MetaRewinds } from './MemberMeta.js'
import type {
  MemberMetaUpdate,
  MemberSnapshot,
  MemberWants,
  RoomDataEvent,
  RoomMetaEntry,
  RoomMetaUpdate,
  SenderStamp,
  WholeMemberUpdate,
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

/** A binary listener's track filter: one track, or `EVERY_TRACK`. */
type TrackFilter = TrackKey | EveryTrack
/** A membership event as this model got it, whether it applied it or not. */
type MembershipEvent = { join: MemberSnapshot } | { leave: string } | { meta: { id: string; update: MemberMetaUpdate } }
/** A binary listener list's filters as counts: every-track listeners, and listeners per lane track. */
type TrackCounts = { all: number; named: Map<TrackKey, number> }
type CountedTrack = { counts: TrackCounts; track: TrackFilter }

type MemberEntry = {
  id: string
  /** Stale and echoed `p-meta` events are absorbed. */
  meta: MemberMeta
  joinedAt: number
  identity: string | null
  /** An off-presence participant: a member for routing/discovery, excluded from every presence read (`count`,
   *  `snapshot`, `onJoin`/`onLeave`/`onEmpty`). Any number per room. */
  hidden: boolean
  left: boolean
  leaveCause?: LeaveCause
}

/** One view's remote of a member and its listeners on it; each list is made when its first listener is added: most
 *  members never get one. */
type MemberListeners = {
  remote: RemoteParticipant | null
  dataCbs: Listeners<(data: unknown, info: ChannelPublishInfo) => unknown> | null
  binary: {
    cbs: Listeners<{
      cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo) => unknown
      track: TrackFilter
    }>
    tracks: TrackCounts
  } | null
  updateCbs: Listeners<(meta: ParticipantMeta, prev: ParticipantMeta) => void> | null
  disconnectCbs: Listeners<() => void> | null
  reconnectCbs: Listeners<() => void> | null
  leaveCbs: Listeners<(cause: LeaveCause) => void> | null
}

type RoomModelOptions = {
  roomId: string
  /** In key order. */
  meta: readonly RoomMetaEntry[]
  /** Either the authoritative roster, or just its member count. A lazy model seeds with `{ count }` and learns the
   *  members from its first roster (read on the server, streamed to the client). */
  seed: { members: MemberSnapshot[] } | { count: number }
  /** How many meta writes `meta` reflects (see `applyRoomUpdate`). */
  metaSeq: number
  closed?: boolean
  owner: RoomModelOwner
}

/** What a model tells the side that holds it, as each happens. */
interface RoomModelEvents {
  /** Every leave, from an event or a reconciled roster, after every view's callbacks; `hidden` is null for a member
   *  this model didn't know. */
  onMemberLeft(id: string, cause: LeaveCause, hidden: boolean | null): void
  /** A known member's meta update applied, from an event, a stamp or a roster read. */
  onMemberMeta(id: string, update: MemberMetaUpdate): void
  /** A room meta change arrived ahead of one before it, and waits for it. */
  onMetaWaiting(): void
  /** A member's meta change arrived ahead of one before it, which never arrived: the next roster read repairs it. */
  onMemberMetaGap(): void
}

/** What a model asks the side that holds it. */
interface RoomModelQueries {
  /** A member of a roster still streaming in or applying, which an unstamped message may name. */
  pendingMember(id: string): MemberSnapshot | undefined
  /** This side's own connection to a member it holds, shown in place of the room's record of it; `null` for one it
   *  doesn't hold. */
  heldConnection(id: string): boolean | null
}

interface RoomModelOwner extends RoomModelEvents, RoomModelQueries {}

type RoomStateOptions = {
  view: RoomStateView
  model: RoomModel
  owner: RoomStateOwner
  /** How many tracks its binary listeners may name, each `(member or room, track)` pair once; unbounded if omitted. */
  trackWantsMax?: number
}

/** What a view's state tells the side that holds it. */
interface RoomStateOwner {
  /** The number of attached listeners changed, with the member whose own listeners changed (`null` for a room-level
   *  one), and the binary want it turned on or off, if any: the side (de)activates its event source (adapter
   *  subscription, wire subscription). */
  onListenersChanged(member: string | null, binary: BinaryWantChange | null): void
  /** A user callback threw: the side reports it. Called on its own, as a function, so the state hands it to each
   *  listener's call as it is. */
  readonly onCallbackError: (error: unknown) => void
}

/** Exact-keyed backing lets the serializer recover (room, member) without exposing a public brand. */
type RemoteBacking = { state: RoomState; entry: MemberEntry }

const { remoteBackings } = getGlobalObject('state.ts', () => ({
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
    return this._state.model.roomId
  }

  get meta(): RoomMeta {
    return this._state.model.meta
  }

  get count(): number {
    return this._state.model.count
  }

  get isEmpty(): boolean {
    return this._state.model.count === 0
  }

  get isClosed(): boolean {
    return this._state.model.closed
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

/** A room's members, meta and closure as one side knows them, shared by every view over it. A `join` for a known
 *  member or a `leave` for an unknown one changes nothing, so a snapshot and a concurrent event stream compose without
 *  double-firing; each change it applies reaches every view with listeners. */
class RoomModel {
  readonly roomId: string
  closed: boolean
  /** Bumped on every observable change (membership, participant meta, room config, closure). Drives
   *  `onChange`/`snapshot()` cache invalidation. */
  private stateVersion = 0
  private snapshotCache: { version: number; value: WeakRef<RoomSnapshotView> } | null = null
  /** The membership events the in-flight roster read has seen so far. */
  private recording: MembershipEvent[] | null = null
  private readonly members = new Map<string, MemberEntry>()
  private hiddenMembers = 0
  /** The views with a listener: one without has nothing to fire. */
  private readonly views = new Set<RoomState>()
  private readonly owner: RoomModelOwner
  private readonly metaState: RoomMetaState
  /** Shared by the members' metas here and by the local participants of this view. */
  readonly metaRewinds: MetaRewinds = { count: 0 }
  private _rosterKnown: boolean
  private _closedCause: LeaveCause = ownLeaveCause({ type: 'closed' })
  private seedCount = 0
  /** The meta changes per member that reached this model before its first roster, in order. */
  private readonly preRosterMeta = new Map<string, MemberMetaUpdate[]>()

  constructor(opts: RoomModelOptions) {
    this.roomId = opts.roomId
    this.metaState = new RoomMetaState(opts.meta, opts.metaSeq)
    this.closed = opts.closed === true
    this.owner = opts.owner
    if ('members' in opts.seed) {
      this._rosterKnown = true
      for (const member of opts.seed.members) this.createEntry(member)
    } else {
      this._rosterKnown = false
      this.seedCount = opts.seed.count
    }
  }

  // ── Reads ──
  /** Before the roster is known: the seed count (which excludes hidden members) adjusted by the events since. */
  get count(): number {
    if (!this._rosterKnown) return this.seedCount
    return this.members.size - this.hiddenMembers
  }

  /** How the room closed for this model; `null` while it is open. */
  get closedCause(): LeaveCause | null {
    return this.closed ? this._closedCause : null
  }

  /** Whether this model holds the authoritative member list (vs just a count). */
  get rosterKnown(): boolean {
    return this._rosterKnown
  }

  /** A member's meta and connection, whole, with the revision they were accepted at. */
  acceptedMeta(id: string): WholeMemberUpdate | null {
    const entry = this.members.get(id)
    return entry ? wholeUpdate(entry.meta.seq, entry.meta.object, !entry.meta.connected) : null
  }

  /** Whether a sender's stamp at `seq` is newer than the member's meta here. */
  newerStamp(id: string, seq: number): boolean {
    return this.members.get(id)?.meta.newerStamp(seq) ?? false
  }

  /** A failover rewound the room's and its members' meta seqs: the next whole meta of each applies, whatever its
   *  seq. */
  forgetMetaSeqs(): void {
    this.metaState.forget()
    this.metaRewinds.count++
    this.bumpState()
  }

  hasMember(id: string): boolean {
    return this.members.has(id)
  }

  /** Null for a member this model doesn't know. */
  memberHidden(id: string): boolean | null {
    return this.members.get(id)?.hidden ?? null
  }

  /** Members currently known, hidden ones included. */
  get memberCount(): number {
    return this.members.size
  }

  /** Member IDs currently known, live. Drives the per-member binary key subscriptions. */
  memberIds(): IterableIterator<string> {
    return this.members.keys()
  }

  /** @internal The member's entry, which its remotes read. */
  _entry(id: string): MemberEntry | undefined {
    return this.members.get(id)
  }

  /** @internal Every member's entry, live. */
  _entries(): IterableIterator<MemberEntry> {
    return this.members.values()
  }

  /** A member `memberIds()` listed, as a roster carries it. */
  memberSnapshot(id: string): MemberSnapshot {
    const { meta, joinedAt, identity, hidden } = this.members.get(id)!
    const member: MemberSnapshot = { id, meta: meta.object, joinedAt, metaSeq: meta.seq, identity }
    if (hidden) member.hidden = true
    if (!meta.connected) member.disconnected = true
    return member
  }

  /** Bumped on every observable change: a roster read at one version lists the same members as any other read at it. */
  get version(): number {
    return this.stateVersion
  }

  /** @internal The entry of a revived `RemoteParticipant`: the live one if known, else seeded silently from the
   *  snapshot (its seed count already includes it). */
  _entryFromSnapshot(snap: MemberSnapshot): MemberEntry {
    const existing = this.members.get(snap.id)
    if (existing) return existing
    if (this.closed) {
      // A closed room has no members: one it hands out left with it.
      const entry = newEntry(snap, this.metaRewinds)
      entry.left = true
      entry.leaveCause = this._closedCause
      return entry
    }
    const entry = this.createEntry(snap)
    this.bumpState()
    return entry
  }

  /** @internal A view's listeners start or stop: only a view with one hears the changes. */
  _listen(view: RoomState, on: boolean): void {
    if (on) this.views.add(view)
    else this.views.delete(view)
  }

  /** Immutable view of the whole room, cached by state version, so the reference is stable until something actually
   *  changes (the `useSyncExternalStore` contract). */
  snapshot(): RoomSnapshotView {
    const cached = this.snapshotCache?.version === this.stateVersion ? this.snapshotCache.value.deref() : undefined
    if (cached) return cached
    const participants = Object.freeze(
      [...this.members.values()]
        .filter((entry) => !entry.hidden)
        .map((entry) => {
          const { id, identity, meta, joinedAt } = entry
          return Object.freeze({ id, identity, meta: meta.object, joinedAt, isConnected: this.isConnected(entry) })
        }),
    )
    const value = Object.freeze({
      id: this.roomId,
      meta: this.meta,
      count: this.count,
      isClosed: this.closed,
      participants,
    })
    untether(value) // plain data: holding a snapshot must not keep the room's wrapper alive
    this.snapshotCache = { version: this.stateVersion, value: new WeakRef(value) }
    return value
  }

  /** State changed observably: invalidate the snapshot and tell `onChange` subscribers. */
  private bumpState(): void {
    this.stateVersion++
    for (const view of [...this.views]) view._changed()
  }

  /** As this side shows it: its own connection to a member it holds, the room's record for any other. */
  isConnected(entry: MemberEntry): boolean {
    return this.owner.heldConnection(entry.id) ?? entry.meta.connected
  }

  /** This side's own connection to a member it holds changed from `wasConnected`, or `null` for one it didn't hold. */
  heldConnectionChanged(id: string, wasConnected: boolean | null): void {
    const entry = this.members.get(id)
    if (!entry || this.isConnected(entry) === (wasConnected ?? entry.meta.connected)) return
    this.bumpState()
    this.connectionChanged(entry)
  }

  private connectionChanged(entry: MemberEntry): void {
    for (const view of [...this.views]) view._connectionChanged(entry)
  }

  /** Runs `read`, then `settle` with what it read, recording the membership events this model gets until `settle` ends;
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

  /** Records the membership events this model gets into the returned list, until `stopRecordingMembership()`. */
  recordMembership(): readonly MembershipEvent[] {
    assert(this.recording === null)
    this.recording = []
    return this.recording
  }

  stopRecordingMembership(): void {
    this.recording = null
  }

  private record(event: MembershipEvent): void {
    this.recording?.push(event)
  }

  // ── Event application ──
  applyJoin(member: MemberSnapshot): boolean {
    this.record({ join: member })
    if (this.closed) return false
    // A known member's join echo has nothing new: its meta is the admission meta, frozen before any guard.
    if (this.members.has(member.id)) return false
    const entry = this.createEntry(member)
    // A hidden participant is no presence event (no count, no `onJoin`), but the roster changed, so `onChange` fires.
    if (entry.hidden) {
      this.bumpState()
      return true
    }
    if (!this._rosterKnown) this.seedCount++ // pre-roster, `count` is the seed adjusted by applied events
    this.bumpState()
    for (const view of [...this.views]) view._joined(entry)
    return true
  }

  /** A leave a server relayed, which it does once for each member it knew: before the first roster, a visible member
   *  this model has not seen is one its seed count holds. */
  applyRelayedLeave(id: string, cause: LeaveCause, hidden: boolean): void {
    if (!this._rosterKnown && !hidden && !this.closed && !this.members.has(id)) {
      this.seedCount = Math.max(0, this.seedCount - 1)
      this.bumpState()
    }
    this.applyLeave(id, cause)
  }

  applyLeave(id: string, cause: LeaveCause): void {
    this.record({ leave: id })
    const entry = this.members.get(id)
    if (entry) this.removeEntry(entry, ownLeaveCause(cause))
    this.owner.onMemberLeft(id, cause, entry ? entry.hidden : null)
  }

  private removeEntry(entry: MemberEntry, cause: LeaveCause): void {
    entry.left = true
    entry.leaveCause = cause
    this.members.delete(entry.id)
    if (entry.hidden) this.hiddenMembers--
    // A hidden participant's leave is no presence event either; its own handlers and listener release still run.
    if (!entry.hidden && !this._rosterKnown) this.seedCount = Math.max(0, this.seedCount - 1)
    this.bumpState()
    for (const view of [...this.views]) view._left(entry, cause)
  }

  /** Applies updates in seq order (see `MemberMeta.apply`): the origin's echo (same seq) and events arriving behind a
   *  fresher reconcile are absorbed. Only a listener builds the meta object per update. */
  applyParticipantMeta(id: string, update: MemberMetaUpdate): boolean {
    this.record({ meta: { id, update } })
    const entry = this.members.get(id)
    // Before the first roster every member is unknown, and the first roster applies the changes after its own.
    if (!entry && !this._rosterKnown) getOrCreate(this.preRosterMeta, id, () => []).push(update)
    if (!entry) return false
    const listening = 'connected' in update ? [] : [...this.views].filter((view) => view._listensToMeta(entry))
    const prev = listening.length > 0 ? entry.meta.object : null
    const wasConnected = this.isConnected(entry)
    const applied = entry.meta.apply(update)
    if (applied === 'gap') this.owner.onMemberMetaGap()
    if (applied !== 'applied') return false
    this.owner.onMemberMeta(id, update)
    this.bumpState()
    if (this.isConnected(entry) !== wasConnected) this.connectionChanged(entry)
    if (prev === null) return true
    const next = entry.meta.object
    for (const view of listening) view._metaChanged(entry, next, prev)
    return true
  }

  /** Applies updates in `seq` order (see `RoomMetaState.apply`), so every model converges; returns the ones applied.
   *  `prev` is this model's own previous meta, which can differ per model. */
  applyRoomUpdate(update: RoomMetaUpdate): RoomMetaUpdate[] {
    const listening = [...this.views].filter((view) => view._listensToRoomMeta())
    const prev = listening.length > 0 ? this.metaState.object : null
    const applied = this.metaState.apply(update)
    if (this.metaState.waiting) this.owner.onMetaWaiting()
    if (applied.length === 0) return applied
    this.bumpState()
    if (prev !== null) for (const view of listening) view._roomMetaChanged(this.metaState.object, prev)
    return applied
  }

  /** Built when read, and kept until the next change. */
  get meta(): RoomMeta {
    return this.metaState.object
  }

  /** In key order, as a snapshot or a whole `update` carries it. */
  metaEntries(): RoomMetaEntry[] {
    return this.metaState.entries()
  }

  /** How many meta writes this model's meta reflects (serialized into room snapshots). */
  get metaSeq(): number {
    return this.metaState.seq
  }

  get metaWaiting(): boolean {
    return this.metaState.waiting
  }

  /** Room closed: member-level cleanup callbacks run (decoders etc.), then `onClose`. Room-level `onLeave`/`onEmpty`
   *  intentionally don't fire: `onClose` is the signal. */
  applyClosed(cause: LeaveCause = { type: 'closed' }): boolean {
    if (this.closed) return false
    cause = ownLeaveCause(cause)
    this._closedCause = cause
    const departed = [...this.members.values()]
    this.closed = true
    this._rosterKnown = true // authoritatively empty
    this.members.clear()
    this.hiddenMembers = 0
    this.bumpState()
    // State and snapshot are already closed-and-empty when cleanup callbacks run.
    for (const entry of departed) {
      entry.left = true
      entry.leaveCause = cause
    }
    for (const view of [...this.views]) view._closed(departed, cause)
    return true
  }

  /** @internal A message's sender as of its send: a stamp newer than the member's meta here applies first, as the meta
   *  update it is. Unstamped, the sender is one this model was sent, maybe in a roster still applying. */
  _resolveSender({
    from,
    fromMeta,
    fromMetaSeq,
    fromIdentity,
    fromDisconnected,
  }: { from: string } & Partial<SenderStamp>): { entry: MemberEntry } | { sender: Sender } {
    const entry = this.members.get(from)
    if (fromMeta !== undefined && fromMetaSeq !== undefined) {
      if (!entry) return { sender: senderOf(from, fromMeta, fromIdentity ?? null) }
      if (entry.meta.newerStamp(fromMetaSeq)) {
        this.applyParticipantMeta(from, wholeUpdate(fromMetaSeq, fromMeta, fromDisconnected === true))
      }
      return { entry }
    }
    const pending = this.owner.pendingMember(from)
    if (entry) {
      if (pending !== undefined && pending.metaSeq > entry.meta.seq) {
        this.applyParticipantMeta(from, wholeUpdate(pending.metaSeq, pending.meta, pending.disconnected === true))
      }
      return { entry }
    }
    assert(pending !== undefined)
    // With the meta changes relayed after the member's roster part.
    const meta = new MemberMeta({ meta: pending.meta, seq: pending.metaSeq })
    for (const update of this.preRosterMeta.get(from) ?? []) meta.apply(update)
    return { sender: senderOf(from, meta.object, pending.identity ?? null) }
  }

  /** The first roster loads silently; later ones narrate the drift they correct as events. A departing member stays
   *  until its leave event, which carries the cause. Runs `reconcilingRoster` a `ROOM_PLAN_SLICE_MS` slice per turn;
   *  null if the room closed between slices. */
  async reconcileRoster(
    roster: { members: MemberSnapshot[]; departing: ReadonlySet<string> },
    events: readonly MembershipEvent[],
  ): Promise<boolean | null> {
    const steps = this.reconcilingRoster(roster, events)
    const slice = new Slice()
    let step = steps.next()
    for (; step.done !== true; step = steps.next()) {
      if (!slice.over) continue
      await slice.next()
      if (this.closed) return null
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
      narratedDrift = this.mergeRosterMember(member, narrate) || narratedDrift
      yield
      replay()
    }
    if (!narrate) {
      this._rosterKnown = true
      this.preRosterMeta.clear()
      this.bumpState()
    }
    for (const id of [...this.members.keys()]) {
      if (this.members.has(id) && !listed.has(id) && !departing.has(id)) {
        // No leave event reached this model, so its cause is unknown: it is reported as removed.
        this.applyLeave(id, removedCause(undefined))
        narratedDrift = true
      }
      yield
      replay()
    }
    return narrate && narratedDrift
  }

  /** Whether the member's drift was narrated as events. */
  private mergeRosterMember(member: MemberSnapshot, narrate: boolean): boolean {
    const entry = this.members.get(member.id)
    if (!entry) {
      if (!narrate) {
        this.createEntry(member)
        return false
      }
      this.applyJoin(member)
      return true
    }
    if (member.metaSeq <= entry.meta.seq) return false
    const update = wholeUpdate(member.metaSeq, member.meta, member.disconnected === true)
    if (narrate) {
      this.applyParticipantMeta(member.id, update)
      return true
    }
    if (entry.meta.apply(update) === 'applied') this.owner.onMemberMeta(member.id, update)
    return false
  }

  private createEntry(entrySeed: MemberSnapshot): MemberEntry {
    const entry = newEntry(entrySeed, this.metaRewinds)
    this.members.set(entry.id, entry)
    if (entry.hidden) this.hiddenMembers++
    return entry
  }
}

/** One view of a room model: its listeners and the remotes it hands out, each backed by its owner. A room-level
 *  `onLeave` or `onEmpty` never fires for a hidden member. */
class RoomState {
  /** @internal The `ServerRoom`/`ClientRoom` this is the state of, for serialization backing. */
  readonly _view: RoomStateView
  readonly model: RoomModel
  private readonly listenerCleanups = new Map<object, Set<() => void>>()
  /** By the model's entry, so a remote still held keeps its own, and a member's leave forgets it. */
  private readonly memberListeners = new WeakMap<MemberEntry, MemberListeners>()
  private readonly changeCbs: Listeners<() => void> = new Listeners()
  private readonly owner: RoomStateOwner

  private readonly roomDataCbs: Listeners<(data: unknown, info: ChannelPublishInfo, from: Sender) => unknown> =
    new Listeners()

  private readonly roomBinaryCbs: Listeners<{
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown
    track: TrackFilter
  }> = new Listeners()

  private readonly roomBinaryTracks: TrackCounts = newTrackCounts()
  /** The members with a `subscribe()` listener. */
  private readonly textWanted = new Set<string>()
  private readonly joinCbs: Listeners<(member: RemoteParticipant) => void> = new Listeners()
  private readonly leaveCbs: Listeners<(member: RemoteParticipant, cause: LeaveCause) => void> = new Listeners()

  private readonly participantUpdateCbs: Listeners<
    (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void
  > = new Listeners()

  private readonly updateCbs: Listeners<(meta: RoomMeta, prev: RoomMeta) => void> = new Listeners()
  private readonly emptyCbs: Listeners<() => void> = new Listeners()
  private readonly closeCbs: Listeners<() => void> = new Listeners()
  private readonly announceCbs: Listeners<(data: unknown, info: ChannelPublishInfo) => void> = new Listeners()
  private _listenerCount = 0
  /** The tracks its binary listeners name, each `(member or room, track)` pair once. */
  private trackWants = 0
  private readonly trackWantsMax: number

  constructor(opts: RoomStateOptions) {
    this._view = opts.view
    this.model = opts.model
    this.owner = opts.owner
    this.trackWantsMax = opts.trackWantsMax ?? Number.POSITIVE_INFINITY
  }

  // ── Reads ──
  get roomId(): string {
    return this.model.roomId
  }

  /** `join({ hidden: true })` members: routable, excluded from every presence read. */
  listHidden(): RemoteParticipant[] {
    return [...this.model._entries()].filter((entry) => entry.hidden).map((entry) => this.remote(entry))
  }

  /** Total attached listeners; owners derive lane-specific wants from the callback lists. */
  get listenerCount(): number {
    return this._listenerCount
  }

  /** Whether this holder consumes room-authored messages on the semantic lane. */
  get wantsAnnounce(): boolean {
    return this.announceCbs.size > 0
  }

  /** `all` while room-level `subscribe()`rs exist, and the members with participant-scoped listeners either way. */
  textWants(): MemberWants {
    return { all: this.roomDataCbs.size > 0, members: this.textWanted }
  }

  getRemote(id: string): RemoteParticipant | null {
    const entry = this.model._entry(id)
    return entry ? this.remote(entry) : null
  }

  listVisible(): RemoteParticipant[] {
    return [...this.model._entries()].filter((entry) => !entry.hidden).map((entry) => this.remote(entry))
  }

  /** A revived `RemoteParticipant`: the live member if known, else seeded silently from the snapshot. */
  ensureRemoteFromSnapshot(snap: MemberSnapshot): RemoteParticipant {
    return this.remote(this.model._entryFromSnapshot(snap))
  }

  // ── Listener registration (all return an unlisten function) ──
  subscribe(cb: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this.register(this.roomDataCbs, cb)
  }

  subscribeBinary(
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown,
    opts?: { track?: string | null },
  ): () => void {
    const listener = binaryListener(cb, opts)
    return this.register(this.roomBinaryCbs, listener, null, {
      counts: this.roomBinaryTracks,
      track: listener.track,
    })
  }

  onJoin(cb: (member: RemoteParticipant) => void): () => void {
    return this.register(this.joinCbs, cb)
  }

  onLeave(cb: (member: RemoteParticipant, cause: LeaveCause) => void): () => void {
    return this.register(this.leaveCbs, cb)
  }

  onParticipantUpdate(
    cb: (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void,
  ): () => void {
    return this.register(this.participantUpdateCbs, cb)
  }

  onUpdate(cb: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this.register(this.updateCbs, cb)
  }

  onEmpty(cb: () => void): () => void {
    return this.register(this.emptyCbs, cb)
  }

  onClose(cb: () => void): () => void {
    if (!this.model.closed) return this.register(this.closeCbs, cb)
    // Terminal, like a departed member's onLeave: a late listener hears it at once.
    invokeChannelListener(cb, [], this.owner.onCallbackError)
    return makeDisposer()
  }

  onChange(cb: () => void): () => void {
    return this.register(this.changeCbs, cb)
  }

  onAnnounce(cb: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this.register(this.announceCbs, cb)
  }

  // ── The model's changes, told while this view has a listener ──
  /** @internal */
  _changed(): void {
    this.fireAll(this.changeCbs)
  }

  /** @internal A visible member joined. */
  _joined(entry: MemberEntry): void {
    if (this.joinCbs.size > 0) this.fireAll(this.joinCbs, this.remote(entry))
  }

  /** @internal */
  _left(entry: MemberEntry, cause: LeaveCause): void {
    const leaveCbs = this.memberListeners.get(entry)?.leaveCbs
    if (leaveCbs) this.fireAll(leaveCbs, cause)
    if (!entry.hidden && this.leaveCbs.size > 0) this.fireAll(this.leaveCbs, this.remote(entry), cause)
    this.releaseEntryListeners(entry)
    if (entry.hidden) return
    if (this.model.count === 0) this.fireAll(this.emptyCbs)
  }

  /** @internal */
  _connectionChanged(entry: MemberEntry): void {
    const listeners = this.memberListeners.get(entry)
    const cbs = this.model.isConnected(entry) ? listeners?.reconnectCbs : listeners?.disconnectCbs
    if (cbs) this.fireAll(cbs)
  }

  /** @internal Whether a member meta change needs its previous meta built for this view. */
  _listensToMeta(entry: MemberEntry): boolean {
    return (this.memberListeners.get(entry)?.updateCbs ?? null) !== null || this.participantUpdateCbs.size > 0
  }

  /** @internal */
  _metaChanged(entry: MemberEntry, next: ParticipantMeta, prev: ParticipantMeta): void {
    const updateCbs = this.memberListeners.get(entry)?.updateCbs
    if (updateCbs) this.fireAll(updateCbs, next, prev)
    this.fireAll(this.participantUpdateCbs, this.remote(entry), next, prev)
  }

  /** @internal */
  _listensToRoomMeta(): boolean {
    return this.updateCbs.size > 0
  }

  /** @internal */
  _roomMetaChanged(next: RoomMeta, prev: RoomMeta): void {
    this.fireAll(this.updateCbs, next, prev)
  }

  /** @internal */
  _closed(departed: MemberEntry[], cause: LeaveCause): void {
    for (const entry of departed) {
      const leaveCbs = this.memberListeners.get(entry)?.leaveCbs
      if (leaveCbs) this.fireAll(leaveCbs, cause)
      this.releaseEntryListeners(entry)
    }
    this.fireAll(this.closeCbs)
    this.releaseAllListeners()
  }

  // ── Delivery ──
  applyAnnounce(data: unknown, info: ChannelPublishInfo): void {
    this.fireAll(this.announceCbs, data, info)
  }

  /** Never waits on the roster: an unknown sender (its join may be behind on the control lane) is the snapshot its
   *  instance stamped. */
  applyData(event: RoomDataEvent, info: ChannelPublishInfo): void {
    const sender = this.resolveSender(event)
    this.fireAll(this.roomDataCbs, event.data, info, sender)
    const entry = this.model._entry(event.from)
    const dataCbs = entry && this.memberListeners.get(entry)?.dataCbs
    if (dataCbs) this.fireAll(dataCbs, event.data, info)
  }

  /** A message's sender as of its send (see `RoomModel._resolveSender`). */
  resolveSender(stamp: { from: string } & Partial<SenderStamp>): Sender {
    const resolved = this.model._resolveSender(stamp)
    return 'entry' in resolved ? this.remote(resolved.entry) : resolved.sender
  }

  /** A binary frame names only its sender's id: one from a member not yet known surfaces as `{ id, meta: {} }`. */
  applyBinary({ from, payload, track, meta }: BinaryFrame, info: ChannelPublishInfo): void {
    const frameInfo: ChannelPublishInfo & BinaryFrameInfo = {
      key: info.key,
      seq: info.seq,
      timestamp: info.timestamp,
      track: publicTrack(track),
      meta,
    }
    const entry = this.model._entry(from)
    const sender = entry ? this.remote(entry) : senderOf(from, {}, null)
    // Each listener gets bytes of its own: one may write into them or transfer their buffer.
    this.fireTrackFiltered(this.roomBinaryCbs, track, (cb) => cb(new Uint8Array(payload), frameInfo, sender))
    const binary = entry && this.memberListeners.get(entry)?.binary
    if (binary) this.fireTrackFiltered(binary.cbs, track, (cb) => cb(new Uint8Array(payload), frameInfo))
  }

  /** Fire the listeners whose track filter admits `track`. */
  private fireTrackFiltered<CB>(
    cbs: Listeners<{ cb: CB; track: TrackFilter }>,
    track: TrackKey,
    invoke: (cb: CB) => unknown,
  ): void {
    for (const { cb, track: want } of cbs.list()) {
      if (want !== EVERY_TRACK && want !== track) continue
      invokeChannelListener(invoke, [cb], this.owner.onCallbackError)
    }
  }

  // ── Private ──
  private listenersOf(entry: MemberEntry): MemberListeners {
    const existing = this.memberListeners.get(entry)
    if (existing !== undefined) return existing
    const listeners: MemberListeners = {
      remote: null,
      dataCbs: null,
      binary: null,
      updateCbs: null,
      disconnectCbs: null,
      reconnectCbs: null,
      leaveCbs: null,
    }
    this.memberListeners.set(entry, listeners)
    return listeners
  }

  private remote(entry: MemberEntry): RemoteParticipant {
    const listeners = this.listenersOf(entry)
    if (listeners.remote) return listeners.remote
    const model = this.model
    const remote: RemoteParticipant = {
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
      get isConnected() {
        return model.isConnected(entry)
      },
      subscribe: (cb) => this.registerLive(entry, (listeners.dataCbs ??= new Listeners()), cb),
      subscribeBinary: (cb, opts) => {
        const listener = binaryListener(cb, opts)
        const binary = (listeners.binary ??= { cbs: new Listeners(), tracks: newTrackCounts() })
        return this.registerLive(entry, binary.cbs, listener, { counts: binary.tracks, track: listener.track })
      },
      onUpdate: (cb) => this.registerLive(entry, (listeners.updateCbs ??= new Listeners()), cb),
      onDisconnect: (cb) => this.registerLive(entry, (listeners.disconnectCbs ??= new Listeners()), cb),
      onReconnect: (cb) => this.registerLive(entry, (listeners.reconnectCbs ??= new Listeners()), cb),
      onLeave: (cb: (cause: LeaveCause) => void) => {
        if (!entry.left) return this.register((listeners.leaveCbs ??= new Listeners()), cb, entry)
        assert(entry.leaveCause)
        invokeChannelListener(cb, [entry.leaveCause], this.owner.onCallbackError)
        return makeDisposer()
      },
    }
    listeners.remote = remote
    remoteBackings.set(remote, { state: this, entry })
    return remote
  }

  /** A departed member's listeners were released at its leave, so one added after it is never held. */
  private registerLive<T>(entry: MemberEntry, list: Listeners<T>, cb: T, filter?: CountedTrack): () => void {
    if (entry.left) return makeDisposer()
    return this.register(list, cb, entry, filter)
  }

  /** `entry` owns the list (`null`: the room); a binary listener's `filter` is counted in its list's counts. */
  private register<T>(list: Listeners<T>, cb: T, entry: MemberEntry | null = null, filter?: CountedTrack): () => void {
    assertUsage(
      filter === undefined ||
        filter.track === EVERY_TRACK ||
        this.trackWants < this.trackWantsMax ||
        filter.counts.named.has(filter.track),
      `Too many binary tracks subscribed on one room (${this.trackWantsMax} max): subscribe without \`{ track }\` to receive every track`,
    )
    const remove = list.add(cb)
    this.bumpListenerCount(
      1,
      entry,
      filter && countTrack(filter.counts, filter.track, 1) ? wantOf(entry, filter, true) : null,
    )
    const cleanups = getOrCreate(this.listenerCleanups, list, () => new Set())
    return makeDisposer(() => {
      if (remove()) {
        this.bumpListenerCount(
          -1,
          entry,
          filter && countTrack(filter.counts, filter.track, -1) ? wantOf(entry, filter, false) : null,
        )
      }
    }, cleanups)
  }

  /** A discarded entry's listeners die with it, so the counters let owners drop what it held open. */
  private releaseEntryListeners(entry: MemberEntry): void {
    const listeners = this.memberListeners.get(entry)
    if (!listeners) return
    const lists = [
      listeners.dataCbs,
      listeners.binary?.cbs,
      listeners.updateCbs,
      listeners.disconnectCbs,
      listeners.reconnectCbs,
      listeners.leaveCbs,
    ]
    for (const list of lists) {
      if (!list) continue
      for (const unlisten of this.listenerCleanups.get(list) ?? []) unlisten()
      this.listenerCleanups.delete(list)
    }
  }

  private releaseAllListeners(): void {
    for (const cleanups of this.listenerCleanups.values()) for (const unlisten of cleanups) unlisten()
  }

  private bumpListenerCount(delta: number, entry: MemberEntry | null, binary: BinaryWantChange | null): void {
    this._listenerCount += delta
    if (this._listenerCount === (delta > 0 ? 1 : 0)) this.model._listen(this, delta > 0)
    if (binary !== null && binary.track !== EVERY_TRACK) this.trackWants += binary.on ? 1 : -1
    if (entry !== null) {
      if ((this.memberListeners.get(entry)?.dataCbs?.size ?? 0) > 0) this.textWanted.add(entry.id)
      else this.textWanted.delete(entry.id)
    }
    this.owner.onListenersChanged(entry?.id ?? null, binary)
  }

  private fireAll<Args extends unknown[]>(cbs: Listeners<(...args: Args) => unknown>, ...args: Args): void {
    for (const cb of cbs.list()) invokeChannelListener(cb, args, this.owner.onCallbackError)
  }
}

function newEntry(
  { id, meta, joinedAt, metaSeq, identity, hidden, disconnected }: MemberSnapshot,
  rewinds: MetaRewinds,
): MemberEntry {
  return {
    id,
    meta: new MemberMeta({ meta, seq: metaSeq, rewinds, connected: disconnected !== true }),
    joinedAt,
    identity: identity ?? null,
    hidden: hidden === true,
    left: false,
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
    const meta = new MemberMeta({ meta: member.meta, seq: member.metaSeq, connected: member.disconnected !== true })
    if (meta.apply(update) !== 'applied') return
    const { disconnected, ...rest } = member
    const next: MemberSnapshot = { ...rest, meta: meta.object, metaSeq: meta.seq }
    if (!meta.connected) next.disconnected = true
    members.set(id, next)
  }
}

/** A member's whole meta and connection at `seq`, as a roster or a stamp carries them. */
function wholeUpdate(seq: number, meta: ParticipantMeta, disconnected: boolean): WholeMemberUpdate {
  const update: WholeMemberUpdate = { seq, meta }
  if (disconnected) update.disconnected = true
  return update
}

/** Validate a `subscribeBinary` track option: `undefined` = every track, `null` = the default track, a non-empty name =
 *  that track. */
function normalizeTrackFilter(opts: { track?: string | null } | undefined): TrackFilter {
  assertUsage(
    opts === undefined || (typeof opts === 'object' && opts !== null && !Array.isArray(opts)),
    'subscribeBinary() options should be an object',
  )
  assertKnownOptions(opts, ['track'], 'subscribeBinary()')
  const track = opts?.track
  if (track === undefined) return EVERY_TRACK
  if (track === null) return DEFAULT_TRACK
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
  if (track === EVERY_TRACK) {
    counts.all += delta
    return counts.all === (delta === 1 ? 1 : 0)
  }
  const count = (counts.named.get(track) ?? 0) + delta
  if (count === 0) counts.named.delete(track)
  else counts.named.set(track, count)
  return count === (delta === 1 ? 1 : 0)
}

function wantOf(entry: MemberEntry | null, { track }: CountedTrack, on: boolean): BinaryWantChange {
  return { member: entry?.id ?? EVERY_MEMBER, track, on }
}
