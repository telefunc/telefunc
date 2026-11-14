export { RoomState, RoomStateView, remoteBacking }
export type { RoomStateOwner }

import { assert, assertUsage } from '../utils/assert.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import {
  invokeChannelListener,
  type ChannelPublishInfo,
  getOrCreate,
  Listeners,
  untether,
  type TELEFUNC_SHIELDS,
} from 'telefunc/__internal'
import { makeDisposer, type ListenerOptions } from './disposer.js'
import {
  EVERY_MEMBER,
  EVERY_TRACK,
  isNamedTrack,
  decodeFrameMessage,
  publicTrack,
  trackKey,
  type EveryTrack,
  type BinaryFrame,
  type BinaryWantChange,
  type TrackKey,
} from './binary.js'
import { type MemberEntry, type RoomModel } from './RoomModel.js'
import { fieldValue, participantFields } from './participantFields.js'
import { assertKnownOptions, senderSnapshot } from './arguments.js'
import { roomMessages } from './messages.js'
import type { MemberSnapshot, MemberWants, RoomDataEvent, SenderStamp } from './protocol.js'
import type {
  BinaryFrameInfo,
  LeaveCause,
  ParticipantMeta,
  RemoteParticipant,
  RoomMessage,
  RoomMeta,
  RoomSnapshotView,
  Sender,
} from '../types.js'

type TrackListenerOptions = ListenerOptions & { track?: string }
/** A `subscribe()` listener: its `info` has the frame's fields where it names a track. */
type MessageListener<From extends unknown[]> = (data: unknown, info: any, ...from: From) => unknown
/** A binary listener's track filter: one track, or `EVERY_TRACK`. */
type TrackFilter = TrackKey | EveryTrack
/** A binary listener list's filters as counts: every-track listeners, and listeners per lane track. */
type TrackCounts = { all: number; named: Map<TrackKey, number> }
type CountedTrack = { counts: TrackCounts; track: TrackFilter }
/** A listener on tracks: `subscribeBinary()`'s gets their binary frames, `subscribe({ track })`'s their messages. */
type TrackListener<From extends unknown[]> = {
  cb: (data: any, info: ChannelPublishInfo & BinaryFrameInfo, ...from: From) => unknown
  track: TrackFilter
  text: boolean
}

/** Who adds a listener to a view: the app, or a package's field, through what `attach` hands it. Only the app's
 *  listeners make a server's view observed. */
type Adder = 'app' | 'field'

/** The participant `snapshot()` lists, with the meta, connection and package fields it shows. */
type ShownParticipant = {
  meta: ParticipantMeta
  isConnected: boolean
  fields: Readonly<Record<string, unknown>>
  participant: RemoteParticipant
}

/** One view's remotes of a member, one per adder, and its listeners on it; each list is made when its first listener
 *  is added: most members never get one. */
type MemberListeners = {
  remotes: Partial<Record<Adder, RemoteParticipant>>
  shown: Partial<Record<Adder, ShownParticipant>>
  dataListeners: Listeners<(data: unknown, info: ChannelPublishInfo) => unknown> | null
  binary: { listeners: Listeners<TrackListener<[]>>; tracks: TrackCounts } | null
  changeListeners: Listeners<() => void> | null
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

/** Exact-keyed backing lets the serializer recover (room, member) without exposing a public brand. A remote and the
 *  participants `snapshot()` lists of it share one. */
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
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  protected abstract readonly _state: RoomState
  /** Who adds the listeners added through this view, and through the participants it hands out. */
  protected readonly _adder: Adder = 'app'

  /** The state a read answers from; a server's room checks first how old its view is. */
  protected get _read(): RoomState {
    return this._state
  }

  get id(): string {
    return this._state.model.roomId
  }

  get meta(): RoomMeta {
    return this._read.model.meta
  }

  get count(): number {
    return this._read.model.count
  }

  get isEmpty(): boolean {
    return this._read.model.count === 0
  }

  get isClosed(): boolean {
    return this._read.model.closed
  }

  getParticipant(id: string): RemoteParticipant | null {
    return this._read.getRemote(id, this._adder)
  }

  snapshot(): RoomSnapshotView {
    return this._read.snapshot(this._adder)
  }

  subscribe(callback: MessageListener<[Sender]>, options?: TrackListenerOptions): () => void {
    return this._state.subscribe(callback, options, this._adder)
  }

  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown,
    options?: TrackListenerOptions,
  ): () => void {
    return this._state.subscribeBinary(callback, options, this._adder)
  }

  messages(options?: ListenerOptions): AsyncIterableIterator<RoomMessage> {
    return roomMessages(this, options?.signal)
  }

  onJoin(callback: (member: RemoteParticipant) => void, options?: ListenerOptions): () => void {
    return this._state.onJoin(callback, options, this._adder)
  }

  onLeave(callback: (member: RemoteParticipant, cause: LeaveCause) => void, options?: ListenerOptions): () => void {
    return this._state.onLeave(callback, options, this._adder)
  }

  onEmpty(callback: () => void, options?: ListenerOptions): () => void {
    return this._state.onEmpty(callback, options, this._adder)
  }

  onClose(callback: (cause: LeaveCause) => void, options?: ListenerOptions): () => void {
    return this._state.onClose(callback, options, this._adder)
  }

  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void, options?: ListenerOptions): () => void {
    return this._state.onAnnounce(callback, options, this._adder)
  }

  onChange(callback: () => void, options?: ListenerOptions): () => void {
    return this._state.onChange(callback, options, this._adder)
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
  private readonly snapshots: Partial<Record<Adder, { version: number; value: WeakRef<RoomSnapshotView> }>> = {}
  private readonly changeListeners: Listeners<() => void> = new Listeners()
  /** A change waits for the end of its task, so the changes of one task call `onChange` once. */
  private changeDue = false
  private readonly owner: RoomStateOwner
  /** The owner's `reportCallbackError`, bound once for every listener call. */
  private readonly reportError: (error: unknown) => void

  private readonly roomDataListeners: Listeners<(data: unknown, info: ChannelPublishInfo, from: Sender) => unknown> =
    new Listeners()

  private readonly roomBinaryListeners: Listeners<TrackListener<[Sender]>> = new Listeners()

  private readonly roomBinaryTracks: TrackCounts = newTrackCounts()
  /** The members with a `subscribe()` listener. */
  private readonly textWanted = new Set<string>()
  private readonly joinListeners: Listeners<(member: RemoteParticipant) => void> = new Listeners()
  private readonly leaveListeners: Listeners<(member: RemoteParticipant, cause: LeaveCause) => void> = new Listeners()

  private readonly emptyListeners: Listeners<() => void> = new Listeners()
  private readonly closeListeners: Listeners<(cause: LeaveCause) => void> = new Listeners()
  private readonly announceListeners: Listeners<(data: unknown, info: ChannelPublishInfo) => void> = new Listeners()
  /** The package fields' `onPrivate()` callbacks, by field. */
  private readonly privateListeners = new Map<string, Listeners<(from: string, data: unknown) => void>>()
  private _listenerCount = 0
  /** Of them, the listeners the package fields added. */
  private fieldListenerCount = 0
  /** What stops the package fields on this view, while they run. */
  private stopFields: (() => void) | null = null
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

  /** `join({ hidden: true })` members: routable, excluded from every presence read. */
  listHidden(): RemoteParticipant[] {
    return [...this.model.entries()].filter((entry) => entry.hidden).map((entry) => this.remote(entry))
  }

  /** Every listener on this view, the package fields' included. */
  get listenerCount(): number {
    return this._listenerCount
  }

  /** The listeners besides the package fields': only these make a server's view observed. */
  get ownListenerCount(): number {
    return this._listenerCount - this.fieldListenerCount
  }

  /** Runs each package's field on this view until `detachFields()`; a closed room runs none. */
  attachFields(): void {
    if (this.stopFields !== null || this.model.closed) return
    // Attaching adds listeners, which can bring the owner back here: the fields run already.
    this.stopFields = () => {}
    const room = new FieldRoom({ state: this })
    const detaches = Array.from(participantFields(), ([name, { attach }]) =>
      attach({
        room,
        set: (id, value) => this.model.setField(id, name, value),
        onPrivate: (cb) =>
          this.register(
            getOrCreate(this.privateListeners, name, () => new Listeners()),
            cb,
            undefined,
            'field',
          ),
      }),
    )
    this.stopFields = () => detaches.forEach((detach) => detach())
  }

  /** Stops the package fields on this view, where they run. */
  detachFields(): void {
    const stop = this.stopFields
    this.stopFields = null
    stop?.()
  }

  /** Whether this holder consumes room-authored messages on the semantic lane. */
  get wantsAnnounce(): boolean {
    return this.announceListeners.size > 0
  }

  /** `all` while room-level `subscribe()`rs exist, and the members with participant-scoped listeners either way. */
  textWants(): MemberWants {
    return { all: this.roomDataListeners.size > 0, members: this.textWanted }
  }

  getRemote(id: string, adder: Adder = 'app'): RemoteParticipant | null {
    const entry = this.model.entry(id)
    return entry ? this.remote(entry, adder) : null
  }

  /** The participant `adder` holds of the member `sender` is a remote of, or `null` if it is none. */
  participantFor(sender: Sender, adder: Adder): RemoteParticipant | null {
    const backing = remoteBacking(sender)
    return backing === null ? null : this.remote(backing.entry, adder)
  }

  listVisible(): RemoteParticipant[] {
    return [...this.model.entries()].filter((entry) => !entry.hidden).map((entry) => this.remote(entry))
  }

  /** Immutable view of the whole room, cached by the model's state version, so the reference is stable until something
   *  actually changes (the `useSyncExternalStore` contract). */
  snapshot(adder: Adder = 'app'): RoomSnapshotView {
    const model = this.model
    const cache = this.snapshots[adder]
    const cached = cache?.version === model.version ? cache.value.deref() : undefined
    if (cached) return cached
    const participants = Object.freeze(
      [...model.entries()].filter((entry) => !entry.hidden).map((entry) => this.shownParticipant(entry, adder)),
    )
    const value = Object.freeze({
      id: model.roomId,
      meta: model.meta,
      count: model.count,
      isClosed: model.closed,
      participants,
    })
    untether(value) // holding a snapshot must not keep the room's wrapper alive
    this.snapshots[adder] = { version: model.version, value: new WeakRef(value) }
    return value
  }

  /** The member as `snapshot()` lists it now, or `null` where it doesn't: a hidden member is never listed. */
  shownParticipantOf(id: string): RemoteParticipant | null {
    const entry = this.model.entry(id)
    return entry && !entry.hidden ? this.shownParticipant(entry) : null
  }

  /** The member's remote with its fields as they are now, the same object until they change. */
  private shownParticipant(entry: MemberEntry, adder: Adder = 'app'): RemoteParticipant {
    const listeners = this.listenersOf(entry)
    const meta = entry.meta.object
    const isConnected = this.model.isConnected(entry)
    const { fields } = entry
    const shown = listeners.shown[adder]
    if (shown?.meta === meta && shown.isConnected === isConnected && shown.fields === fields) return shown.participant
    const remote = this.remote(entry, adder)
    const values: PropertyDescriptorMap = {
      id: ownValue(entry.id),
      identity: ownValue(entry.identity),
      meta: ownValue(meta),
      joinedAt: ownValue(entry.joinedAt),
      isConnected: ownValue(isConnected),
    }
    for (const name of participantFields().keys()) values[name] = ownValue(fieldValue(fields, name))
    const participant: RemoteParticipant = Object.freeze(Object.create(remote, values))
    remoteBackings.set(participant, remoteBackings.get(remote)!)
    listeners.shown[adder] = { meta, isConnected, fields, participant }
    return participant
  }

  /** A revived `RemoteParticipant`: the member's remote, or one that left where this room doesn't have it. */
  remoteFromSnapshot(snap: MemberSnapshot): RemoteParticipant {
    return this.remote(this.model.entryFromSnapshot(snap))
  }

  // Each registration returns its unlisten function.
  subscribe(cb: MessageListener<[Sender]>, opts?: TrackListenerOptions, adder: Adder = 'app'): () => void {
    if (opts?.track === undefined) return this.register(this.roomDataListeners, cb, opts, adder)
    return this.registerRoomTrack(textListener(cb, opts.track), opts, adder)
  }

  subscribeBinary(
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown,
    opts?: TrackListenerOptions,
    adder: Adder = 'app',
  ): () => void {
    return this.registerRoomTrack(binaryListener(cb, opts), opts, adder)
  }

  private registerRoomTrack(
    listener: TrackListener<[Sender]>,
    opts: ListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    return this.register(this.roomBinaryListeners, listener, opts, adder, null, {
      counts: this.roomBinaryTracks,
      track: listener.track,
    })
  }

  onJoin(cb: (member: RemoteParticipant) => void, opts?: ListenerOptions, adder: Adder = 'app'): () => void {
    return this.register(this.joinListeners, cb, opts, adder)
  }

  onLeave(
    cb: (member: RemoteParticipant, cause: LeaveCause) => void,
    opts?: ListenerOptions,
    adder: Adder = 'app',
  ): () => void {
    return this.register(this.leaveListeners, cb, opts, adder)
  }

  onEmpty(cb: () => void, opts?: ListenerOptions, adder: Adder = 'app'): () => void {
    return this.register(this.emptyListeners, cb, opts, adder)
  }

  onClose(cb: (cause: LeaveCause) => void, opts?: ListenerOptions, adder: Adder = 'app'): () => void {
    const cause = this.model.closedCause
    if (cause === null) return this.register(this.closeListeners, cb, opts, adder)
    // Terminal, like a departed member's onLeave: a late listener hears it at once.
    if (!opts?.signal?.aborted) invokeChannelListener(cb, [cause], this.reportError)
    return makeDisposer()
  }

  onChange(cb: () => void, opts?: ListenerOptions, adder: Adder = 'app'): () => void {
    return this.register(this.changeListeners, cb, opts, adder)
  }

  onAnnounce(
    cb: (data: unknown, info: ChannelPublishInfo) => void,
    opts?: ListenerOptions,
    adder: Adder = 'app',
  ): () => void {
    return this.register(this.announceListeners, cb, opts, adder)
  }

  // Each of these tells one change of the model, while this view has a listener.
  changed(): void {
    if (this.changeDue) return
    this.changeDue = true
    queueMicrotask(() => {
      this.changeDue = false
      this.fireAll(this.changeListeners)
    })
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

  /** What a member shows changed: its meta, its connection, or a package's field. */
  memberChanged(entry: MemberEntry): void {
    const changeListeners = this.memberListeners.get(entry)?.changeListeners
    if (changeListeners) this.fireAll(changeListeners)
  }

  /** The room closed with `cause`, taking the `departed` members, which leave with `departure`. */
  closed(departed: MemberEntry[], cause: LeaveCause, departure: LeaveCause): void {
    for (const entry of departed) {
      const leaveListeners = this.memberListeners.get(entry)?.leaveListeners
      if (leaveListeners) this.fireAll(leaveListeners, departure)
      this.releaseEntryListeners(entry)
    }
    this.fireAll(this.closeListeners, cause)
    this.releaseAllListeners()
  }

  /** A package's message to a member this view holds, for its field's `onPrivate()` callbacks only. */
  applyPrivate(field: string, from: string, data: unknown): void {
    const listeners = this.privateListeners.get(field)
    if (listeners) this.fireAll(listeners, from, data)
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

  /** A frame names only its sender's id: one from a member not yet known surfaces as `{ id, meta: {} }`. */
  applyBinary(frame: BinaryFrame, info: ChannelPublishInfo): void {
    const { from, track, meta } = frame
    const frameInfo: ChannelPublishInfo & BinaryFrameInfo = {
      key: info.key,
      seq: info.seq,
      timestamp: info.timestamp,
      track: publicTrack(track),
      meta,
    }
    const entry = this.model.entry(from)
    const sender = entry ? this.remote(entry) : senderSnapshot(from, {}, null)
    for (const listener of this.roomBinaryListeners.list()) {
      if (hearsFrame(listener, frame))
        invokeChannelListener(listener.cb, [listenerData(frame), frameInfo, sender], this.reportError)
    }
    const binary = entry && this.memberListeners.get(entry)?.binary
    if (!binary) return
    for (const listener of binary.listeners.list()) {
      if (hearsFrame(listener, frame))
        invokeChannelListener(listener.cb, [listenerData(frame), frameInfo], this.reportError)
    }
  }

  private listenersOf(entry: MemberEntry): MemberListeners {
    const existing = this.memberListeners.get(entry)
    if (existing !== undefined) return existing
    const listeners: MemberListeners = {
      remotes: {},
      shown: {},
      dataListeners: null,
      binary: null,
      changeListeners: null,
      disconnectListeners: null,
      reconnectListeners: null,
      leaveListeners: null,
    }
    this.memberListeners.set(entry, listeners)
    return listeners
  }

  private remote(entry: MemberEntry, adder: Adder = 'app'): RemoteParticipant {
    const listeners = this.listenersOf(entry)
    const existing = listeners.remotes[adder]
    if (existing) return existing
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
      subscribe: (cb: MessageListener<[]>, opts?: TrackListenerOptions) => {
        if (opts?.track === undefined)
          return this.registerLive(entry, (listeners.dataListeners ??= new Listeners()), cb, opts, adder)
        return this.registerMemberTrack(entry, listeners, textListener(cb, opts.track), opts, adder)
      },
      subscribeBinary: (cb, opts) => this.registerMemberTrack(entry, listeners, binaryListener(cb, opts), opts, adder),
      onChange: (cb, opts) =>
        this.registerLive(entry, (listeners.changeListeners ??= new Listeners()), cb, opts, adder),
      onDisconnect: (cb, opts) =>
        this.registerLive(entry, (listeners.disconnectListeners ??= new Listeners()), cb, opts, adder),
      onReconnect: (cb, opts) =>
        this.registerLive(entry, (listeners.reconnectListeners ??= new Listeners()), cb, opts, adder),
      onLeave: (cb: (cause: LeaveCause) => void, opts?: ListenerOptions) => {
        if (!entry.left) return this.register((listeners.leaveListeners ??= new Listeners()), cb, opts, adder, entry)
        assert(entry.leaveCause)
        if (!opts?.signal?.aborted) invokeChannelListener(cb, [entry.leaveCause], this.reportError)
        return makeDisposer()
      },
    }
    for (const name of participantFields().keys()) {
      Object.defineProperty(remote, name, { get: () => fieldValue(entry.fields, name), enumerable: true })
    }
    listeners.remotes[adder] = remote
    remoteBackings.set(remote, { state: this, entry })
    // A read through the page's room wrapper tethers what it returns to it, but only the member `join()` returns holds
    // its room open.
    untether(remote)
    return remote
  }

  private registerMemberTrack(
    entry: MemberEntry,
    listeners: MemberListeners,
    listener: TrackListener<[]>,
    opts: ListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    const binary = (listeners.binary ??= { listeners: new Listeners(), tracks: newTrackCounts() })
    return this.registerLive(entry, binary.listeners, listener, opts, adder, {
      counts: binary.tracks,
      track: listener.track,
    })
  }

  /** A departed member's listeners were released at its leave, so one added after it is never held. */
  private registerLive<T>(
    entry: MemberEntry,
    list: Listeners<T>,
    cb: T,
    opts: ListenerOptions | undefined,
    adder: Adder,
    filter?: CountedTrack,
  ): () => void {
    if (entry.left) return makeDisposer()
    return this.register(list, cb, opts, adder, entry, filter)
  }

  /** `entry` owns the list (`null`: the room); a binary listener's `filter` is counted in its list's counts. */
  private register<T>(
    list: Listeners<T>,
    cb: T,
    opts: ListenerOptions | undefined,
    adder: Adder,
    entry: MemberEntry | null = null,
    filter?: CountedTrack,
  ): () => void {
    const signal = opts?.signal
    if (signal?.aborted) return makeDisposer()
    assertUsage(
      filter === undefined ||
        filter.track === EVERY_TRACK ||
        this.trackWants < this.trackWantsMax ||
        filter.counts.named.has(filter.track),
      `Too many binary tracks subscribed on one room (${this.trackWantsMax} max): subscribe without \`{ track }\` ` +
        `to receive every track`,
    )
    const remove = list.add(cb)
    // Counted for its adder by every way it goes: its stop, its signal, its member's leave or the close.
    const asField = adder === 'field'
    if (asField) this.fieldListenerCount++
    this.bumpListenerCount(
      1,
      entry,
      filter && countTrack(filter.counts, filter.track, 1) ? { ...wantedTrack(entry, filter), on: true } : null,
    )
    const cleanups = getOrCreate(this.listenerCleanups, list, () => new Set())
    return makeDisposer(
      () => {
        if (remove()) {
          if (asField) this.fieldListenerCount--
          this.bumpListenerCount(
            -1,
            entry,
            filter && countTrack(filter.counts, filter.track, -1) ? { ...wantedTrack(entry, filter), on: false } : null,
          )
        }
      },
      cleanups,
      signal,
    )
  }

  /** A discarded entry's listeners die with it, so the counters let owners drop what it held open. */
  private releaseEntryListeners(entry: MemberEntry): void {
    const listeners = this.memberListeners.get(entry)
    if (!listeners) return
    const lists = [
      listeners.dataListeners,
      listeners.binary?.listeners,
      listeners.changeListeners,
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

  /** The room closed: every listener goes, the package fields' too. */
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

type FieldRoomOptions = { state: RoomState }

/** A view's room as a package's field sees it (`attachFields()`): what the field adds through it, or through a
 *  participant it hands out, is the field's. */
class FieldRoom extends RoomStateView {
  protected readonly _state: RoomState
  protected override readonly _adder = 'field'

  constructor({ state }: FieldRoomOptions) {
    super()
    this._state = state
  }

  override subscribe(callback: MessageListener<[Sender]>, options?: TrackListenerOptions): () => void {
    return super.subscribe((data, info, from) => callback(data, info, this.own(from) ?? from), options)
  }

  override subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown,
    options?: TrackListenerOptions,
  ): () => void {
    return super.subscribeBinary((data, info, from) => callback(data, info, this.own(from) ?? from), options)
  }

  override onJoin(callback: (member: RemoteParticipant) => void, options?: ListenerOptions): () => void {
    return super.onJoin((member) => callback(this.own(member) ?? member), options)
  }

  override onLeave(
    callback: (member: RemoteParticipant, cause: LeaveCause) => void,
    options?: ListenerOptions,
  ): () => void {
    return super.onLeave((member, cause) => callback(this.own(member) ?? member, cause), options)
  }

  /** The field's own participant of the member `sender` is, if it is one. */
  private own(sender: Sender): RemoteParticipant | null {
    return this._state.participantFor(sender, 'field')
  }
}

/** A snapshot participant's value: its own, so it shadows the remote's getter. */
function ownValue(value: unknown): PropertyDescriptor {
  return { value, enumerable: true }
}

/** Validate a `subscribeBinary` track option: `undefined` = every track, a non-empty name = that track, `'main'` the
 *  default one. */
function normalizeTrackFilter(opts: TrackListenerOptions | undefined): TrackFilter {
  assertUsage(
    opts === undefined || (typeof opts === 'object' && opts !== null && !Array.isArray(opts)),
    'subscribeBinary() options should be an object',
  )
  assertKnownOptions(opts, ['track', 'signal'], 'subscribeBinary()')
  const track = opts?.track
  if (track === undefined) return EVERY_TRACK
  assertUsage(isNamedTrack(track), 'subscribeBinary() track should be a valid non-empty string')
  return trackKey(track)
}

function binaryListener<From extends unknown[]>(
  cb: TrackListener<From>['cb'],
  opts: TrackListenerOptions | undefined,
): TrackListener<From> {
  return { cb, track: normalizeTrackFilter(opts), text: false }
}

function textListener<From extends unknown[]>(cb: TrackListener<From>['cb'], track: unknown): TrackListener<From> {
  assertUsage(isNamedTrack(track), 'subscribe() track should be a valid non-empty string')
  return { cb, track: trackKey(track), text: true }
}

function hearsFrame({ track, text }: Omit<TrackListener<[]>, 'cb'>, frame: BinaryFrame): boolean {
  return text === frame.text && (track === EVERY_TRACK || track === frame.track)
}

/** Of the listener's own: one may write into its bytes or transfer their buffer. */
function listenerData(frame: BinaryFrame): unknown {
  return frame.text ? decodeFrameMessage(frame) : new Uint8Array(frame.payload)
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

/** The member and track a listener's filter names. */
function wantedTrack(entry: MemberEntry | null, { track }: CountedTrack): Omit<BinaryWantChange, 'on'> {
  return { member: entry?.id ?? EVERY_MEMBER, track }
}
