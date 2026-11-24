export { RoomState }
export type { Adder, MessageListener, RoomStateOwner, TrackListenerOptions, WantChange }

import { assert, assertUsage } from '../utils/assert.js'
import { invokeChannelListener, type ChannelPublishInfo, getOrCreate, Listeners } from 'telefunc/__internal'
import { makeDisposer, replayToLateListener, type ListenerOptions } from './disposer.js'
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
import type { MemberEntry, ResolvedSender, RoomModel } from './RoomModel.js'
import { participantFields } from './participantFields.js'
import { assertKnownOptions, senderSnapshot } from './arguments.js'
import { toggle } from '../utils/sets.js'
import { RoomView, RoomRemotes } from './RoomView.js'
import type { MemberWants, RoomDataEvent } from './protocol.js'
import type { BinaryFrameInfo, LeaveCause } from '../types.js'

type TrackListenerOptions = ListenerOptions & { track?: string }
/** A `subscribe()` listener: its `info` has the frame's fields where it names a track. `info` is `any` because the
 *  public overloads type it by `options.track`, which no implementation signature ties to the listener: a narrower type
 *  refuses one overload's listener. */
type MessageListener<From extends unknown[]> = (data: unknown, info: any, ...from: From) => unknown
/** A track listener's filter: one track, or `EVERY_TRACK`. */
type TrackFilter = TrackKey | EveryTrack
/** A track listener list's filters as counts: every-track listeners, and listeners per lane track. */
type TrackCounts = { all: number; named: Map<TrackKey, number> }
type CountedTrack = { counts: TrackCounts; track: TrackFilter }
/** A track listener's call with what a frame carries, its info and its sender. */
type TrackCallback<Data> = (data: Data, info: ChannelPublishInfo & BinaryFrameInfo, from: ResolvedSender) => unknown
/** A listener on tracks: `subscribeBinary()`'s gets their binary frames, `subscribe({ track })`'s their messages. */
type TrackListener = { track: TrackFilter } & (
  | { text: false; cb: TrackCallback<Uint8Array> }
  | { text: true; cb: TrackCallback<unknown> }
)

/** Who adds a listener: the app, or a package's field, through the view `attachFields()` hands it. Only the app's
 *  listeners make a server's view observed. */
type Adder = 'app' | 'field'

/** The listeners on one member, each list made when its first listener is added: most members never get one. */
type MemberListeners = {
  data: Listeners<(data: unknown, info: ChannelPublishInfo, from: ResolvedSender) => unknown> | null
  tracks: { listeners: Listeners<TrackListener>; counts: TrackCounts } | null
  change: Listeners<() => void> | null
  disconnect: Listeners<() => void> | null
  reconnect: Listeners<() => void> | null
  leave: Listeners<(cause: LeaveCause) => void> | null
  /** What removes each of them, run when the member leaves. */
  disposers: Set<() => void>
}

/** One listener's registration: who adds it, its options, the member whose list it joins (none: the room's), and a
 *  track listener's filter, counted in its list's counts. */
type Registration = {
  adder: Adder
  options: ListenerOptions | undefined
  entry?: MemberEntry
  filter?: CountedTrack
}

type RoomStateOptions = {
  model: RoomModel
  owner: RoomStateOwner
  /** How many tracks its track listeners may name, each `(member or room, track)` pair once; unbounded if omitted. */
  trackWantsMax?: number
}

/** What its listeners want crossed: the first listener of a kind came, or the last one went. `listeners`: its
 *  listeners, or those besides the package fields'; `text`: the room-wide messages (`member` null) or one member's;
 *  `track`: a track. */
type WantChange =
  | { kind: 'listeners' }
  | { kind: 'text'; member: string | null; on: boolean }
  | { kind: 'track'; change: BinaryWantChange }

/** What a room's state tells the side that holds it. */
interface RoomStateOwner {
  /** What its listeners want crossed: the side (de)activates its event source (adapter subscription, wire
   *  subscription). */
  onWantChanged(change: WantChange): void
  /** A user callback threw: the side reports it. */
  reportCallbackError(error: unknown): void
}

/** One side's listeners on a room model, the app's and the package fields', and the wants they amount to. Each
 *  listener hears the model's entries; a view hands its own participants of them. A room-level `onLeave` or `onEmpty`
 *  never fires for a hidden member. */
class RoomState {
  readonly model: RoomModel
  /** What removes each listener on the room, run when it closes. */
  private readonly roomDisposers = new Set<() => void>()
  /** By the model's entry, so a remote still held keeps its own, and a member's leave forgets it. */
  private readonly memberListeners = new WeakMap<MemberEntry, MemberListeners>()
  private readonly changeListeners: Listeners<() => void> = new Listeners()
  /** A change waits for the end of its task, so the changes of one task call `onChange` once. */
  private changeDue = false
  private readonly owner: RoomStateOwner
  /** The owner's `reportCallbackError`, bound once for every listener call. */
  private readonly reportError: (error: unknown) => void

  /** A listener on the room's messages; `from` is `null` for the server's. */
  private readonly roomDataListeners: Listeners<
    (data: unknown, info: ChannelPublishInfo, from: ResolvedSender | null) => unknown
  > = new Listeners()

  private readonly roomTrackListeners: Listeners<TrackListener> = new Listeners()

  private readonly roomTracks: TrackCounts = newTrackCounts()
  /** The members with a `subscribe()` listener. */
  private readonly textWanted = new Set<string>()
  private readonly joinListeners: Listeners<(entry: MemberEntry) => void> = new Listeners()
  private readonly leaveListeners: Listeners<(entry: MemberEntry, cause: LeaveCause) => void> = new Listeners()

  private readonly emptyListeners: Listeners<() => void> = new Listeners()
  private readonly closeListeners: Listeners<(cause: LeaveCause) => void> = new Listeners()
  /** The package fields' `onPrivate()` callbacks, by field. */
  private readonly privateListeners = new Map<string, Listeners<(from: string, data: unknown) => void>>()
  private _listenerCount = 0
  /** Of them, the listeners the package fields added. */
  private fieldListenerCount = 0
  /** The room the package fields see, made at their first attach. */
  private fieldView: RoomView | null = null
  /** What stops the package fields, while they run. */
  private stopFields: (() => void) | null = null
  /** The tracks its track listeners name, each `(member or room, track)` pair once. */
  private trackWants = 0
  private readonly trackWantsMax: number

  constructor(opts: RoomStateOptions) {
    this.model = opts.model
    const owner = opts.owner
    this.owner = owner
    this.reportError = (error) => owner.reportCallbackError(error)
    this.trackWantsMax = opts.trackWantsMax ?? Number.POSITIVE_INFINITY
  }

  /** Every listener on this state, the package fields' included. */
  get listenerCount(): number {
    return this._listenerCount
  }

  /** The listeners besides the package fields': only these make a server's view observed. */
  get ownListenerCount(): number {
    return this._listenerCount - this.fieldListenerCount
  }

  /** Runs each package's field on this state until `detachFields()`; a closed room runs none. */
  attachFields(): void {
    if (this.stopFields !== null || this.model.closed) return
    // Attaching adds listeners, which can bring the owner back here: the fields run already.
    this.stopFields = () => {}
    const room = (this.fieldView ??= new RoomView(new RoomRemotes({ state: this, adder: 'field' })))
    const detaches = Array.from(participantFields(), ([name, { attach }]) =>
      attach({
        room,
        set: (id, value) => this.model.setField(id, name, value),
        onPrivate: (cb) =>
          this.register(
            getOrCreate(this.privateListeners, name, () => new Listeners()),
            cb,
            { adder: 'field', options: undefined },
          ),
      }),
    )
    this.stopFields = () => detaches.forEach((detach) => detach())
  }

  /** Stops the package fields on this state, where they run. */
  detachFields(): void {
    const stop = this.stopFields
    this.stopFields = null
    stop?.()
  }

  /** `all` while room-level `subscribe()`rs exist, and the members with participant-scoped listeners either way. */
  textWants(): MemberWants {
    return { all: this.roomDataListeners.size > 0, members: this.textWanted }
  }

  /** Whether a listener takes `member`'s messages: a room-level `subscribe()`r, or one of `member`'s own. */
  wantsTextFrom(member: string): boolean {
    return this.roomDataListeners.size > 0 || this.textWanted.has(member)
  }

  // Each registration returns its unlisten function, and counts its listener as `adder`'s.
  subscribe(
    cb: MessageListener<[ResolvedSender | null]>,
    opts: TrackListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    if (opts?.track === undefined) return this.register(this.roomDataListeners, cb, { adder, options: opts })
    return this.registerRoomTrack(textListener(cb, opts.track), opts, adder)
  }

  subscribeBinary(
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: ResolvedSender) => unknown,
    opts: TrackListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    return this.registerRoomTrack(binaryListener(cb, opts), opts, adder)
  }

  onJoin(cb: (entry: MemberEntry) => void, opts: ListenerOptions | undefined, adder: Adder): () => void {
    return this.register(this.joinListeners, cb, { adder, options: opts })
  }

  onLeave(
    cb: (entry: MemberEntry, cause: LeaveCause) => void,
    opts: ListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    return this.register(this.leaveListeners, cb, { adder, options: opts })
  }

  onEmpty(cb: () => void, opts: ListenerOptions | undefined, adder: Adder): () => void {
    return this.register(this.emptyListeners, cb, { adder, options: opts })
  }

  onClose(cb: (cause: LeaveCause) => void, opts: ListenerOptions | undefined, adder: Adder): () => void {
    const cause = this.model.closedCause
    if (cause === null) return this.register(this.closeListeners, cb, { adder, options: opts })
    return replayToLateListener(cb, [cause], opts, this.reportError)
  }

  onChange(cb: () => void, opts: ListenerOptions | undefined, adder: Adder): () => void {
    return this.register(this.changeListeners, cb, { adder, options: opts })
  }

  // A member's own listeners: one added after the member left is never held, as its listeners went at its leave.
  subscribeMember(
    entry: MemberEntry,
    cb: MessageListener<[ResolvedSender]>,
    opts: TrackListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    if (entry.left) return makeDisposer()
    const listeners = this.listenersOf(entry)
    if (opts?.track === undefined) {
      return this.register((listeners.data ??= new Listeners()), cb, { adder, options: opts, entry })
    }
    return this.registerMemberTrack(entry, listeners, textListener(cb, opts.track), opts, adder)
  }

  subscribeMemberBinary(
    entry: MemberEntry,
    cb: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: ResolvedSender) => unknown,
    opts: TrackListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    if (entry.left) return makeDisposer()
    return this.registerMemberTrack(entry, this.listenersOf(entry), binaryListener(cb, opts), opts, adder)
  }

  onMember(
    entry: MemberEntry,
    event: 'change' | 'disconnect' | 'reconnect',
    cb: () => void,
    opts: ListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    if (entry.left) return makeDisposer()
    const listeners = this.listenersOf(entry)
    return this.register((listeners[event] ??= new Listeners()), cb, { adder, options: opts, entry })
  }

  /** A member that left already tells a late listener its leave at once. */
  onMemberLeave(
    entry: MemberEntry,
    cb: (cause: LeaveCause) => void,
    opts: ListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    if (!entry.left) {
      const listeners = this.listenersOf(entry)
      return this.register((listeners.leave ??= new Listeners()), cb, { adder, options: opts, entry })
    }
    assert(entry.leaveCause)
    return replayToLateListener(cb, [entry.leaveCause], opts, this.reportError)
  }

  // Each of these tells one change of the model, while this state has a listener.
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
    this.fireAll(this.joinListeners, entry)
  }

  left(entry: MemberEntry, cause: LeaveCause): void {
    const leaveListeners = this.memberListeners.get(entry)?.leave
    if (leaveListeners) this.fireAll(leaveListeners, cause)
    if (!entry.hidden) this.fireAll(this.leaveListeners, entry, cause)
    this.releaseMemberListeners(entry)
    if (entry.hidden) return
    if (this.model.count === 0) this.fireAll(this.emptyListeners)
  }

  connectionChanged(entry: MemberEntry): void {
    const listeners = this.memberListeners.get(entry)
    const connectionListeners = this.model.isConnected(entry) ? listeners?.reconnect : listeners?.disconnect
    if (connectionListeners) this.fireAll(connectionListeners)
  }

  /** What a member shows changed: its meta, its connection, or a package's field. */
  memberChanged(entry: MemberEntry): void {
    const changeListeners = this.memberListeners.get(entry)?.change
    if (changeListeners) this.fireAll(changeListeners)
  }

  /** The room closed with `cause`, taking the `departed` members, which leave with `departure`. Every listener goes,
   *  the package fields' too: the departed members' with them, then the room's. */
  closed(departed: MemberEntry[], cause: LeaveCause, departure: LeaveCause): void {
    for (const entry of departed) {
      const leaveListeners = this.memberListeners.get(entry)?.leave
      if (leaveListeners) this.fireAll(leaveListeners, departure)
      this.releaseMemberListeners(entry)
    }
    this.fireAll(this.closeListeners, cause)
    for (const dispose of this.roomDisposers) dispose()
  }

  /** A package's message to a member this state's side holds, for its field's `onPrivate()` callbacks only. */
  applyPrivate(field: string, from: string, data: unknown): void {
    const listeners = this.privateListeners.get(field)
    if (listeners) this.fireAll(listeners, from, data)
  }

  /** Never waits on the roster: an unknown sender (its join may be behind on the control lane) is the snapshot its
   *  instance stamped (see `RoomModel.resolveSender`). */
  applyData(event: RoomDataEvent, info: ChannelPublishInfo): void {
    const from = event.from === '' ? null : this.model.resolveSender(event)
    this.fireAll(this.roomDataListeners, event.data, info, from)
    if (from === null || !('entry' in from)) return
    const dataListeners = this.memberListeners.get(from.entry)?.data
    if (dataListeners) this.fireAll(dataListeners, event.data, info, from)
  }

  /** A frame names only its sender's id: one from a member not yet known surfaces as `{ id, meta: {} }`. */
  applyBinary(frame: BinaryFrame, info: ChannelPublishInfo): void {
    const { track, meta } = frame
    const frameInfo: ChannelPublishInfo & BinaryFrameInfo = {
      key: info.key,
      seq: info.seq,
      timestamp: info.timestamp,
      track: publicTrack(track),
      meta,
    }
    const entry = this.model.entry(frame.from)
    const from: ResolvedSender = entry ? { entry } : { sender: senderSnapshot(frame.from, {}, null) }
    for (const { listener } of this.roomTrackListeners.list()) {
      if (listener && hearsFrame(listener, frame)) this.fireTrack(listener, frame, frameInfo, from)
    }
    const tracks = entry && this.memberListeners.get(entry)?.tracks
    if (!tracks) return
    for (const { listener } of tracks.listeners.list()) {
      if (listener && hearsFrame(listener, frame)) this.fireTrack(listener, frame, frameInfo, from)
    }
  }

  /** `frame` to a listener that hears it: its message, or bytes of the listener's own, which it may write into or
   *  transfer. */
  private fireTrack(
    listener: TrackListener,
    frame: BinaryFrame,
    info: ChannelPublishInfo & BinaryFrameInfo,
    from: ResolvedSender,
  ): void {
    if (listener.text) invokeChannelListener(listener.cb, [decodeFrameMessage(frame), info, from], this.reportError)
    else invokeChannelListener(listener.cb, [new Uint8Array(frame.payload), info, from], this.reportError)
  }

  private listenersOf(entry: MemberEntry): MemberListeners {
    const existing = this.memberListeners.get(entry)
    if (existing !== undefined) return existing
    const listeners: MemberListeners = {
      data: null,
      tracks: null,
      change: null,
      disconnect: null,
      reconnect: null,
      leave: null,
      disposers: new Set(),
    }
    this.memberListeners.set(entry, listeners)
    return listeners
  }

  private registerRoomTrack(listener: TrackListener, opts: ListenerOptions | undefined, adder: Adder): () => void {
    const filter: CountedTrack = { counts: this.roomTracks, track: listener.track }
    return this.register(this.roomTrackListeners, listener, { adder, options: opts, filter })
  }

  private registerMemberTrack(
    entry: MemberEntry,
    listeners: MemberListeners,
    listener: TrackListener,
    opts: ListenerOptions | undefined,
    adder: Adder,
  ): () => void {
    const tracks = (listeners.tracks ??= { listeners: new Listeners(), counts: newTrackCounts() })
    const filter: CountedTrack = { counts: tracks.counts, track: listener.track }
    return this.register(tracks.listeners, listener, { adder, options: opts, entry, filter })
  }

  private register<T>(list: Listeners<T>, cb: T, { adder, options, entry, filter }: Registration): () => void {
    const signal = options?.signal
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
    const counted = { list, entry, asField }
    const trackOn = filter && countTrack(filter, 1) ? { ...wantedTrack(entry, filter), on: true } : null
    this.bumpListenerCount(1, counted, trackOn)
    return makeDisposer(
      () => {
        if (!remove()) return
        if (asField) this.fieldListenerCount--
        const trackOff = filter && countTrack(filter, -1) ? { ...wantedTrack(entry, filter), on: false } : null
        this.bumpListenerCount(-1, counted, trackOff)
      },
      entry ? this.listenersOf(entry).disposers : this.roomDisposers,
      signal,
    )
  }

  /** A departed member's listeners die with it, so the counters let owners drop what it held open. */
  private releaseMemberListeners(entry: MemberEntry): void {
    const disposers = this.memberListeners.get(entry)?.disposers
    if (disposers) for (const dispose of disposers) dispose()
  }

  /** A listener of `list` came (`delta` 1) or went (-1); `track` is the track want it turned on or off, if any. The
   *  owner hears each want that crossed. */
  private bumpListenerCount(
    delta: number,
    { list, entry, asField }: { list: { readonly size: number }; entry: MemberEntry | undefined; asField: boolean },
    track: BinaryWantChange | null,
  ): void {
    const on = delta > 0
    const crossed = (count: number) => count === (on ? 1 : 0)
    this._listenerCount += delta
    if (crossed(this._listenerCount)) this.model.listen(this, on)
    if (track !== null && track.track !== EVERY_TRACK) this.trackWants += on ? 1 : -1
    if (crossed(this._listenerCount) || (!asField && crossed(this.ownListenerCount))) {
      this.owner.onWantChanged({ kind: 'listeners' })
    }
    if (crossed(list.size)) {
      if (list === this.roomDataListeners) this.owner.onWantChanged({ kind: 'text', member: null, on })
      else if (entry !== undefined && list === this.memberListeners.get(entry)?.data) {
        toggle(this.textWanted, entry.id, on)
        this.owner.onWantChanged({ kind: 'text', member: entry.id, on })
      }
    }
    if (track !== null) this.owner.onWantChanged({ kind: 'track', change: track })
  }

  private fireAll<Args extends unknown[]>(listeners: Listeners<(...args: Args) => unknown>, ...args: Args): void {
    for (const { listener } of listeners.list()) if (listener) invokeChannelListener(listener, args, this.reportError)
  }
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

function binaryListener(cb: TrackCallback<Uint8Array>, opts: TrackListenerOptions | undefined): TrackListener {
  return { cb, track: normalizeTrackFilter(opts), text: false }
}

function textListener(cb: TrackCallback<unknown>, track: unknown): TrackListener {
  assertUsage(isNamedTrack(track), 'subscribe() track should be a valid non-empty string')
  return { cb, track: trackKey(track), text: true }
}

function hearsFrame({ track, text }: Omit<TrackListener, 'cb'>, frame: BinaryFrame): boolean {
  return text === frame.text && (track === EVERY_TRACK || track === frame.track)
}

function newTrackCounts(): TrackCounts {
  return { all: 0, named: new Map() }
}

/** Whether the count crossed zero, turning a want on or off. */
function countTrack({ counts, track }: CountedTrack, delta: 1 | -1): boolean {
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
function wantedTrack(entry: MemberEntry | undefined, { track }: CountedTrack): Omit<BinaryWantChange, 'on'> {
  return { member: entry?.id ?? EVERY_MEMBER, track }
}
