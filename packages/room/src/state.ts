export { RoomState, RoomStateView, remoteBacking }
export type { RoomStateOwner }

import { assert, assertUsage } from './utils/assert.js'
import { getGlobalObject } from './utils/getGlobalObject.js'
import { invokeChannelListener } from './channel.js'
import type { ChannelPublishInfo } from 'telefunc/__internal'
import { makeDisposer } from './disposer.js'
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
import { assertKnownOptions, senderOf, type MemberEntry, type RoomModel } from './model.js'
import type { MemberSnapshot, MemberWants, RoomDataEvent, SenderStamp } from './protocol.js'
import type { BinaryFrameInfo, LeaveCause, ParticipantMeta, RemoteParticipant, RoomMeta, Sender } from './types.js'

/** A binary listener's track filter: one track, or `EVERY_TRACK`. */
type TrackFilter = TrackKey | EveryTrack
/** A binary listener list's filters as counts: every-track listeners, and listeners per lane track. */
type TrackCounts = { all: number; named: Map<TrackKey, number> }
type CountedTrack = { counts: TrackCounts; track: TrackFilter }

/** One view's remote of a member and its listeners on it; each list is made when its first listener is added: most
 *  members never get one. */
type MemberListeners = {
  remote: RemoteParticipant | null
  dataListeners: Listeners<(data: unknown, info: ChannelPublishInfo) => unknown> | null
  binary: {
    listeners: Listeners<{
      cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo) => unknown
      track: TrackFilter
    }>
    tracks: TrackCounts
  } | null
  updateListeners: Listeners<(meta: ParticipantMeta, prev: ParticipantMeta) => void> | null
  disconnectListeners: Listeners<() => void> | null
  reconnectListeners: Listeners<() => void> | null
  leaveListeners: Listeners<(cause: LeaveCause) => void> | null
}

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
  /** A user callback threw: the side reports it. */
  reportCallbackError(error: unknown): void
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

/** One view of a room model: its listeners and the remotes it hands out, each backed by its owner. A room-level
 *  `onLeave` or `onEmpty` never fires for a hidden member. */
class RoomState {
  /** The `ServerRoom`/`ClientRoom` this is the state of, for serialization backing. */
  readonly view: RoomStateView
  readonly model: RoomModel
  private readonly listenerCleanups = new Map<object, Set<() => void>>()
  /** By the model's entry, so a remote still held keeps its own, and a member's leave forgets it. */
  private readonly memberListeners = new WeakMap<MemberEntry, MemberListeners>()
  private readonly changeListeners: Listeners<() => void> = new Listeners()
  private readonly owner: RoomStateOwner
  /** The owner's `reportCallbackError`, bound once for every listener call. */
  private readonly reportError: (error: unknown) => void

  private readonly roomDataListeners: Listeners<(data: unknown, info: ChannelPublishInfo, from: Sender) => unknown> =
    new Listeners()

  private readonly roomBinaryListeners: Listeners<{
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown
    track: TrackFilter
  }> = new Listeners()

  private readonly roomBinaryTracks: TrackCounts = newTrackCounts()
  /** The members with a `subscribe()` listener. */
  private readonly textWanted = new Set<string>()
  private readonly joinListeners: Listeners<(member: RemoteParticipant) => void> = new Listeners()
  private readonly leaveListeners: Listeners<(member: RemoteParticipant, cause: LeaveCause) => void> = new Listeners()

  private readonly participantUpdateListeners: Listeners<
    (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void
  > = new Listeners()

  private readonly updateListeners: Listeners<(meta: RoomMeta, prev: RoomMeta) => void> = new Listeners()
  private readonly emptyListeners: Listeners<() => void> = new Listeners()
  private readonly closeListeners: Listeners<() => void> = new Listeners()
  private readonly announceListeners: Listeners<(data: unknown, info: ChannelPublishInfo) => void> = new Listeners()
  private _listenerCount = 0
  /** The tracks its binary listeners name, each `(member or room, track)` pair once. */
  private trackWants = 0
  private readonly trackWantsMax: number

  constructor(opts: RoomStateOptions) {
    this.view = opts.view
    this.model = opts.model
    const owner = opts.owner
    this.owner = owner
    this.reportError = (error) => owner.reportCallbackError(error)
    this.trackWantsMax = opts.trackWantsMax ?? Number.POSITIVE_INFINITY
  }

  get roomId(): string {
    return this.model.roomId
  }

  /** `join({ hidden: true })` members: routable, excluded from every presence read. */
  listHidden(): RemoteParticipant[] {
    return [...this.model.entries()].filter((entry) => entry.hidden).map((entry) => this.remote(entry))
  }

  /** Total attached listeners; owners derive lane-specific wants from the callback lists. */
  get listenerCount(): number {
    return this._listenerCount
  }

  /** Whether this holder consumes room-authored messages on the semantic lane. */
  get wantsAnnounce(): boolean {
    return this.announceListeners.size > 0
  }

  /** `all` while room-level `subscribe()`rs exist, and the members with participant-scoped listeners either way. */
  textWants(): MemberWants {
    return { all: this.roomDataListeners.size > 0, members: this.textWanted }
  }

  getRemote(id: string): RemoteParticipant | null {
    const entry = this.model.entry(id)
    return entry ? this.remote(entry) : null
  }

  listVisible(): RemoteParticipant[] {
    return [...this.model.entries()].filter((entry) => !entry.hidden).map((entry) => this.remote(entry))
  }

  /** A revived `RemoteParticipant`: the live member if known, else seeded silently from the snapshot. */
  ensureRemoteFromSnapshot(snap: MemberSnapshot): RemoteParticipant {
    return this.remote(this.model.entryFromSnapshot(snap))
  }

  // Each registration returns its unlisten function.
  subscribe(cb: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this.register(this.roomDataListeners, cb)
  }

  subscribeBinary(
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown,
    opts?: { track?: string | null },
  ): () => void {
    const listener = binaryListener(cb, opts)
    return this.register(this.roomBinaryListeners, listener, null, {
      counts: this.roomBinaryTracks,
      track: listener.track,
    })
  }

  onJoin(cb: (member: RemoteParticipant) => void): () => void {
    return this.register(this.joinListeners, cb)
  }

  onLeave(cb: (member: RemoteParticipant, cause: LeaveCause) => void): () => void {
    return this.register(this.leaveListeners, cb)
  }

  onParticipantUpdate(
    cb: (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void,
  ): () => void {
    return this.register(this.participantUpdateListeners, cb)
  }

  onUpdate(cb: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this.register(this.updateListeners, cb)
  }

  onEmpty(cb: () => void): () => void {
    return this.register(this.emptyListeners, cb)
  }

  onClose(cb: () => void): () => void {
    if (!this.model.closed) return this.register(this.closeListeners, cb)
    // Terminal, like a departed member's onLeave: a late listener hears it at once.
    invokeChannelListener(cb, [], this.reportError)
    return makeDisposer()
  }

  onChange(cb: () => void): () => void {
    return this.register(this.changeListeners, cb)
  }

  onAnnounce(cb: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this.register(this.announceListeners, cb)
  }

  // Each of these tells one change of the model, while this view has a listener.
  changed(): void {
    this.fireAll(this.changeListeners)
  }

  /** A visible member joined. */
  joined(entry: MemberEntry): void {
    if (this.joinListeners.size > 0) this.fireAll(this.joinListeners, this.remote(entry))
  }

  left(entry: MemberEntry, cause: LeaveCause): void {
    const leaveListeners = this.memberListeners.get(entry)?.leaveListeners
    if (leaveListeners) this.fireAll(leaveListeners, cause)
    if (!entry.hidden && this.leaveListeners.size > 0) this.fireAll(this.leaveListeners, this.remote(entry), cause)
    this.releaseEntryListeners(entry)
    if (entry.hidden) return
    if (this.model.count === 0) this.fireAll(this.emptyListeners)
  }

  connectionChanged(entry: MemberEntry): void {
    const listeners = this.memberListeners.get(entry)
    const connectionListeners = this.model.isConnected(entry)
      ? listeners?.reconnectListeners
      : listeners?.disconnectListeners
    if (connectionListeners) this.fireAll(connectionListeners)
  }

  /** Whether a member meta change needs its previous meta built for this view. */
  listensToMeta(entry: MemberEntry): boolean {
    return (
      (this.memberListeners.get(entry)?.updateListeners ?? null) !== null || this.participantUpdateListeners.size > 0
    )
  }

  metaChanged(entry: MemberEntry, next: ParticipantMeta, prev: ParticipantMeta): void {
    const updateListeners = this.memberListeners.get(entry)?.updateListeners
    if (updateListeners) this.fireAll(updateListeners, next, prev)
    this.fireAll(this.participantUpdateListeners, this.remote(entry), next, prev)
  }

  listensToRoomMeta(): boolean {
    return this.updateListeners.size > 0
  }

  roomMetaChanged(next: RoomMeta, prev: RoomMeta): void {
    this.fireAll(this.updateListeners, next, prev)
  }

  closed(departed: MemberEntry[], cause: LeaveCause): void {
    for (const entry of departed) {
      const leaveListeners = this.memberListeners.get(entry)?.leaveListeners
      if (leaveListeners) this.fireAll(leaveListeners, cause)
      this.releaseEntryListeners(entry)
    }
    this.fireAll(this.closeListeners)
    this.releaseAllListeners()
  }

  applyAnnounce(data: unknown, info: ChannelPublishInfo): void {
    this.fireAll(this.announceListeners, data, info)
  }

  /** Never waits on the roster: an unknown sender (its join may be behind on the control lane) is the snapshot its
   *  instance stamped. */
  applyData(event: RoomDataEvent, info: ChannelPublishInfo): void {
    const sender = this.resolveSender(event)
    this.fireAll(this.roomDataListeners, event.data, info, sender)
    const entry = this.model.entry(event.from)
    const dataListeners = entry && this.memberListeners.get(entry)?.dataListeners
    if (dataListeners) this.fireAll(dataListeners, event.data, info)
  }

  /** A message's sender as of its send (see `RoomModel.resolveSender`). */
  resolveSender(stamp: { from: string } & Partial<SenderStamp>): Sender {
    const resolved = this.model.resolveSender(stamp)
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
    const entry = this.model.entry(from)
    const sender = entry ? this.remote(entry) : senderOf(from, {}, null)
    // Each listener gets bytes of its own: one may write into them or transfer their buffer.
    for (const { cb, track: want } of this.roomBinaryListeners.list()) {
      if (want === EVERY_TRACK || want === track) {
        invokeChannelListener(cb, [new Uint8Array(payload), frameInfo, sender], this.reportError)
      }
    }
    const binary = entry && this.memberListeners.get(entry)?.binary
    if (!binary) return
    for (const { cb, track: want } of binary.listeners.list()) {
      if (want === EVERY_TRACK || want === track)
        invokeChannelListener(cb, [new Uint8Array(payload), frameInfo], this.reportError)
    }
  }

  private listenersOf(entry: MemberEntry): MemberListeners {
    const existing = this.memberListeners.get(entry)
    if (existing !== undefined) return existing
    const listeners: MemberListeners = {
      remote: null,
      dataListeners: null,
      binary: null,
      updateListeners: null,
      disconnectListeners: null,
      reconnectListeners: null,
      leaveListeners: null,
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
      subscribe: (cb) => this.registerLive(entry, (listeners.dataListeners ??= new Listeners()), cb),
      subscribeBinary: (cb, opts) => {
        const listener = binaryListener(cb, opts)
        const binary = (listeners.binary ??= { listeners: new Listeners(), tracks: newTrackCounts() })
        return this.registerLive(entry, binary.listeners, listener, { counts: binary.tracks, track: listener.track })
      },
      onUpdate: (cb) => this.registerLive(entry, (listeners.updateListeners ??= new Listeners()), cb),
      onDisconnect: (cb) => this.registerLive(entry, (listeners.disconnectListeners ??= new Listeners()), cb),
      onReconnect: (cb) => this.registerLive(entry, (listeners.reconnectListeners ??= new Listeners()), cb),
      onLeave: (cb: (cause: LeaveCause) => void) => {
        if (!entry.left) return this.register((listeners.leaveListeners ??= new Listeners()), cb, entry)
        assert(entry.leaveCause)
        invokeChannelListener(cb, [entry.leaveCause], this.reportError)
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
      listeners.dataListeners,
      listeners.binary?.listeners,
      listeners.updateListeners,
      listeners.disconnectListeners,
      listeners.reconnectListeners,
      listeners.leaveListeners,
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
    if (this._listenerCount === (delta > 0 ? 1 : 0)) this.model.listen(this, delta > 0)
    if (binary !== null && binary.track !== EVERY_TRACK) this.trackWants += binary.on ? 1 : -1
    if (entry !== null) {
      if ((this.memberListeners.get(entry)?.dataListeners?.size ?? 0) > 0) this.textWanted.add(entry.id)
      else this.textWanted.delete(entry.id)
    }
    this.owner.onListenersChanged(entry?.id ?? null, binary)
  }

  private fireAll<Args extends unknown[]>(listeners: Listeners<(...args: Args) => unknown>, ...args: Args): void {
    for (const cb of listeners.list()) invokeChannelListener(cb, args, this.reportError)
  }
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
