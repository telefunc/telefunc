export { ParticipantBase, watchersOf }
export type { ParticipantBaseOptions, MemberLink }

import { stringify } from '@brillout/json-serializer/stringify'
import {
  invokeChannelListener,
  markHandled,
  type ChannelPublishAck,
  type TELEFUNC_SHIELDS,
  Listeners,
  Fifo,
  PUBLISH_HOLD_BYTES,
  soleAnswer,
  TWO_ANSWERS,
  utf8ByteLength,
} from 'telefunc/__internal'
import { Abort } from 'telefunc/client'
import { assert, assertUsage } from '../utils/assert.js'
import { makeDisposer, replayToLateListener, type ListenerOptions } from './disposer.js'
import { DM_FAILURE, participantLeftError } from './errors.js'
import {
  assertKnownOptions,
  assertPublishOptions,
  ownLeaveCause,
  ownMetaArgument,
  ownMetadata,
  recipientId,
  senderSnapshot,
} from './arguments.js'
import { MemberMeta, type MetaRewinds, type MetaWrite } from './MemberMeta.js'
import { DEFAULT_TRACK, encodeBinaryFrame, encodeMessageFrame, publicTrack, trackKey, type TrackKey } from './binary.js'
import type {
  MemberMetaUpdate,
  MemberRequest,
  WholeMemberUpdate,
  RoomResult,
  InboxMessage,
  TrackWatch,
} from './protocol.js'
import type {
  BinaryPublishOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  Sender,
  TrackDemand,
} from '../types.js'

/** What holding a DM before the first `listen()` retains beyond its serialized bytes (about 55 measured), so many small
 *  ones are bounded by memory too. The inbox is the one lane no want gates, so the only one to bridge. */
const PENDING_INBOX_OVERHEAD_BYTES = 64
type HeldDm = { msg: InboxMessage; bytes: number; ackResolve?: (reply: RoomResult) => void }
/** Where the inbox delivers once it attaches: the local listeners, or the client holding the participant. */
type InboxTarget = { deliver(msg: InboxMessage): void; deliverAck(msg: InboxMessage): Promise<RoomResult> }

/** What `onChange` tells a change of; `demand` counts the changes of its entries. */
type Shown = { meta: ParticipantMeta; connected: boolean; demand: number }
/** Who receives some tracks, counted as `demand` counts one. */
type TracksWatchers = { watchers: RemoteParticipant[]; watcherCount: number }
const NO_DEMAND: readonly TrackDemand[] = Object.freeze([])
const NO_WATCHES: ReadonlyMap<TrackKey, TrackWatch> = new Map()
/** Marks a participant of any copy of the package, which a package's helper is handed (`watchersOf()`,
 *  `sendPrivate()`). */
const PARTICIPANT_BRAND: unique symbol = Symbol.for('telefunc.RoomParticipant')

/** Where a participant's acts go: on a page, up the channel of the room it joined through or its own; on a server, to
 *  its room's writes. */
interface MemberLink {
  publishText(from: string, data: unknown, retain: boolean | undefined): Promise<ChannelPublishAck>
  /** A frame of `from` on one of its tracks; `stream` and `lossy` pace a page's uplink. */
  publishFrame(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck>
  /** Resolves once the room performed it and this side applied what it changed for the member: its accepted meta, or
   *  its leave, which ends the participant. */
  request(from: string, req: MemberRequest): Promise<unknown>
}

type ParticipantBaseOptions<Link extends MemberLink = MemberLink> = {
  participant: { id: string; meta: ParticipantMeta; selfDelivery: boolean; identity: string | null }
  /** Shared with its room view's members; a participant a client holds without a room view has its own. */
  rewinds: MetaRewinds
  link: Link
}

/** A `LocalParticipant`'s acts, private-message inbox and leave lifecycle, identical on server and page; each side
 *  supplies its link and what it reads its room through. */
abstract class ParticipantBase<Link extends MemberLink = MemberLink> implements LocalParticipant {
  readonly [PARTICIPANT_BRAND] = true

  /** A participant of any copy of the package: the brand, not `instanceof`, which a second copy's class fails. */
  static isParticipantBase(value: unknown): value is ParticipantBase {
    return value !== null && typeof value === 'object' && PARTICIPANT_BRAND in value
  }

  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  readonly id: string
  readonly identity: string | null
  readonly selfDelivery: boolean
  protected readonly memberMeta: MemberMeta
  private leftCause: LeaveCause | null = null
  // Each list is made when its first listener is added.
  private leaveListeners: Listeners<(cause: LeaveCause) => unknown> | null = null
  private messageListeners: Listeners<(data: unknown, from: Sender | null) => unknown> | null = null
  private changeListeners: Listeners<() => unknown> | null = null
  /** What `onChange`'s listeners last heard of, while they listen. */
  private seen: Shown | null = null
  private changeDue = false
  private shownDemand: readonly TrackDemand[] = NO_DEMAND
  /** `demand`'s entries by track, as last read. */
  private readonly demandEntries = new Map<TrackKey, TrackDemand>()
  /** The tracks whose entry may have changed since it was read. */
  private readonly staleTracks = new Set<TrackKey>()
  /** Whether every track's entry may have changed, as before the first read. */
  private allTracksStale = true
  /** The version of its room's roster the entries were read at. */
  private demandRosterVersion = -1
  /** The entries that changed since `shownDemand` was made: each its new entry, `null` for one that went, and whether
   *  `shownDemand` lists its track. */
  private readonly listChanges = new Map<TrackKey, { entry: TrackDemand | null; listed: boolean }>()
  private demandChanges = 0
  private disconnectListeners: Listeners<() => unknown> | null = null
  private reconnectListeners: Listeners<() => unknown> | null = null
  private listenerCleanups: Set<() => void> | null = null
  private inboxAttached = false
  /** DMs held until the first `listen()` (`null` once flushed); an ack DM carries the resolver of its reply. */
  private pendingInbox: Fifo<HeldDm> | null = null
  private pendingInboxBytes = 0
  /** Its meta writes the room hasn't answered yet. */
  private metaWrites = 0
  protected readonly link: Link

  constructor({ participant, rewinds, link }: ParticipantBaseOptions<Link>) {
    this.id = participant.id
    this.memberMeta = new MemberMeta({ meta: participant.meta, seq: 0, rewinds })
    this.selfDelivery = participant.selfDelivery
    this.identity = participant.identity
    this.link = link
  }

  get meta(): ParticipantMeta {
    return this.memberMeta.object
  }

  /** @internal The member's whole meta and its seq, which a client that attaches again is sent. */
  get _acceptedMeta(): WholeMemberUpdate {
    const update: WholeMemberUpdate = {
      meta: this.memberMeta.object,
      seq: this.memberMeta.seq,
    }
    if (!this.memberMeta.connected) update.disconnected = true
    return update
  }

  /** The connection of the page holding this member, as the room accepted it. */
  protected get _acceptedConnected(): boolean {
    return this.memberMeta.connected
  }

  /** @internal A meta update the room accepted, applied in seq order; whether it applied. One past a gap waits for the
   *  whole meta its holder's next attach or roster sends. */
  _acceptMeta(update: MemberMetaUpdate): boolean {
    const applied = this.memberMeta.apply(update)
    if (applied === 'changed') this._changed()
    return applied !== 'stale' && applied !== 'gap'
  }

  protected get _left(): boolean {
    return this.leftCause !== null
  }

  /** A package's message to this member, for its field's `onPrivate()` on the views of its room here. */
  protected abstract _deliverPrivate(field: string, from: string, data: unknown): void
  abstract get isConnected(): boolean
  /** A user callback threw. Each side reports through its own pipeline. */
  protected abstract _reportError(err: unknown): void

  /** What a message it sends carries of `data`: the value itself, which a page's wire copies on its way. */
  protected _ownMessage(data: unknown): unknown {
    return data
  }

  // Messaging is often fire-and-forget: a usage error throws, and a failure is a rejection left handled.
  publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    assertPublishOptions(options)
    if (options?.track === undefined) return markHandled(this.publishText(this._ownMessage(data), options?.retain))
    return this.publishOnTrack(encodeMessageFrame(this.id, data, options), options)
  }

  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    return this.publishOnTrack(encodeBinaryFrame(this.id, data, options), options)
  }

  private async publishText(data: unknown, retain: boolean | undefined): Promise<ChannelPublishAck> {
    this.assertActive()
    return await this.link.publishText(this.id, data, retain)
  }

  private publishOnTrack(framed: Uint8Array, options: BinaryPublishOptions | undefined): Promise<ChannelPublishAck> {
    return markHandled(this.publishFrame(framed, this.trackStreamOf(options?.track), options?.lossy === true))
  }

  private async publishFrame(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    this.assertActive()
    return await this.link.publishFrame(framed, stream, lossy)
  }

  /** The stream this member's publishes on `track` go up in, in order: a newer lossy one replaces an older one still
   *  waiting, and a plain one goes after it. */
  private trackStreamOf(track: string | undefined): string {
    return `${this.id}:${track === undefined ? DEFAULT_TRACK : trackKey(track)}`
  }

  // The overloads live on `LocalParticipant`; this is their implementation signature.
  send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    assertKnownOptions(options, ['ack'], 'send()')
    assertUsage(
      options?.ack === undefined || typeof options.ack === 'boolean',
      'send() options.ack should be a boolean',
    )
    return markHandled(this.sendDm(recipientId(to), this._ownMessage(data), options?.ack === true))
  }

  private async sendDm(to: string, data: unknown, ack: boolean): Promise<unknown> {
    this.assertActive()
    const request: MemberRequest = { __r: 'req-dm', to, data }
    if (ack) request.ack = true
    return await this.link.request(this.id, request)
  }

  /** @internal A package's message to `to`, for its field `field` there (`sendPrivate()`). */
  async _sendPrivate(to: string, field: string, data: unknown): Promise<void> {
    this.assertActive()
    await this.link.request(this.id, { __r: 'req-private', to, field, data: this._ownMessage(data) })
  }

  // Concurrent writes can commit out of request order: the participant adopts the sequence-accepted value, as
  // observers do.
  async setMeta(meta: ParticipantMeta): Promise<void> {
    const owned = ownMetaArgument(meta, 'setMeta() meta')
    await this.writeMeta({ meta: owned }, { __r: 'req-set-meta', meta: owned })
  }

  async updateMeta(meta: ParticipantMeta): Promise<void> {
    const owned = ownMetaArgument(meta, 'updateMeta() meta')
    await this.writeMeta({ attrs: owned }, { __r: 'req-update-meta', attrs: owned })
  }

  /** Sends a meta write, unless it leaves the meta as it is and no write of its own is on its way, whose value it
   *  must still replace. */
  private async writeMeta(write: MetaWrite, req: MemberRequest): Promise<void> {
    this.assertActive()
    if (this.metaWrites === 0 && !this.memberMeta.changedBy(write)) return
    this.metaWrites++
    try {
      await this.link.request(this.id, req)
    } finally {
      this.metaWrites--
    }
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this.link.request(this.id, { __r: 'req-leave' })
  }

  listen(callback: (data: unknown, info: { from: Sender | null }) => unknown, options?: ListenerOptions): () => void {
    if (options?.signal?.aborted) return makeDisposer()
    // Each call gets an `info` of its own, so a listener writing to it changes no other listener's.
    const taking = (data: unknown, from: Sender | null) => callback(data, { from })
    const unlisten = this.register((this.messageListeners ??= new Listeners()), taking, options)
    this._flushHeld({ deliver: (msg) => this.fireInbox(msg), deliverAck: (msg) => this.fireInboxAck(msg) })
    return unlisten
  }

  /** @internal A DM for this member: to its listeners, held until the first `listen()`; a package's to the package. */
  _deliverMessage(msg: InboxMessage): void {
    if (msg.field !== undefined) return this._deliverPrivate(msg.field, msg.from, msg.data)
    if (!this.messageListeners?.size) {
      if (this._left || this.inboxAttached) return
      this.hold(msg)
      return
    }
    this.fireInbox(msg)
  }

  /** @internal An `{ ack: true }` DM, resolved with the recipient's reply (or an error if it leaves first); never
   *  rejects. */
  _deliverMessageAck(msg: InboxMessage): Promise<RoomResult> {
    if (this.messageListeners?.size) return this.fireInboxAck(msg)
    if (this._left) return Promise.resolve(DM_FAILURE.left)
    if (this.inboxAttached) return Promise.resolve(DM_FAILURE.noListener)
    return new Promise<RoomResult>((resolve) => this.hold(msg, resolve))
  }

  /** The inbox attached: DMs held until now go out in order, and nothing is held again. */
  protected _flushHeld(to: InboxTarget): void {
    this.inboxAttached = true
    const held = this.pendingInbox
    this.pendingInbox = null
    this.pendingInboxBytes = 0
    for (const { msg, ackResolve } of held ?? []) {
      if (ackResolve) void to.deliverAck(msg).then(ackResolve)
      else to.deliver(msg)
    }
  }

  private hold(msg: InboxMessage, ackResolve?: (reply: RoomResult) => void): void {
    const pending = (this.pendingInbox ??= new Fifo())
    const bytes = utf8ByteLength(stringify(msg)) + PENDING_INBOX_OVERHEAD_BYTES
    pending.push({ msg, bytes, ackResolve })
    this.pendingInboxBytes += bytes
    while (this.pendingInboxBytes > PUBLISH_HOLD_BYTES) {
      const dropped = pending.shift()
      assert(dropped !== undefined)
      this.pendingInboxBytes -= dropped.bytes
      dropped.ackResolve?.(DM_FAILURE.overflow)
    }
  }

  /** `from`/`fromMeta` come from the wire envelope; upgrades to the live `RemoteParticipant` when a room view
   *  exists. An empty `from` is the wire encoding of a room-authored message → `null`. */
  private inboxSender(msg: InboxMessage): Sender | null {
    const { from, fromMeta, fromIdentity: identity } = msg
    return from === ''
      ? null
      : (this._resolveSender(from) ?? senderSnapshot(from, ownMetadata(fromMeta ?? {}), identity))
  }

  private fireInbox(msg: InboxMessage): void {
    this.fireAll(this.messageListeners, msg.data, this.inboxSender(msg))
  }

  /** Channel's rule (`soleAnswer`) picks the reply. An `Abort()` the handler throws is its sender's rejection; any other
   *  throw is reported here, and its sender gets only that the handler threw. */
  private async fireInboxAck(msg: InboxMessage): Promise<RoomResult> {
    let answer: unknown
    try {
      answer = await soleAnswer(this.messageListeners?.list() ?? [], msg.data, this.inboxSender(msg))
    } catch (err) {
      if (err instanceof Abort) return { ok: false, kind: 'abort', abortValue: err.abortValue }
      this._reportError(err)
      return DM_FAILURE.handlerThrew
    }
    return answer === TWO_ANSWERS ? DM_FAILURE.twoReplies : { ok: true, value: answer }
  }

  /** The live room-backed sender, when this flavor has a room view. */
  protected _resolveSender(_id: string): Sender | null {
    return null
  }

  /** Who receives each of its tracks, by track name: each entry the same object until its watchers or count change, and
   *  the list the same array until an entry does. */
  get demand(): readonly TrackDemand[] {
    this.refreshDemand()
    if (this.listChanges.size > 0) {
      this.shownDemand = Object.freeze(this.demandList())
      this.listChanges.clear()
    }
    return this.shownDemand
  }

  /** @internal Who receives `track`, or any of its tracks where none is named, may have changed. Past as many tracks
   *  as it has entries and watches, it reads them all again instead. */
  protected _demandStale(track?: TrackKey): void {
    if (this.allTracksStale) return
    if (track !== undefined && this.staleTracks.size < this.demandEntries.size + this.watchesNow.size) {
      this.staleTracks.add(track)
    } else {
      this.allTracksStale = true
      this.staleTracks.clear()
    }
  }

  /** Reads again the entries of the tracks that may have changed, and only those: every one where its room's roster
   *  changed since. */
  private refreshDemand(): void {
    const rosterVersion = this._rosterVersion
    if (rosterVersion !== this.demandRosterVersion) this.allTracksStale = true
    this.demandRosterVersion = rosterVersion
    const watches = this.watchesNow
    const tracks = this.allTracksStale ? new Set([...this.demandEntries.keys(), ...watches.keys()]) : this.staleTracks
    for (const key of tracks) {
      const watch = watches.get(key)
      const was = this.demandEntries.get(key)
      const entry = watch === undefined ? undefined : this.demandEntry(publicTrack(key), watch, was)
      if (entry === was) continue
      if (entry === undefined) this.demandEntries.delete(key)
      else this.demandEntries.set(key, entry)
      // A track the list doesn't show that went again changes nothing in it.
      const listed = this.listChanges.get(key)?.listed ?? was !== undefined
      if (entry === undefined && !listed) this.listChanges.delete(key)
      else this.listChanges.set(key, { entry: entry ?? null, listed })
      this.demandChanges++
    }
    this.allTracksStale = false
    this.staleTracks.clear()
  }

  /** Lets go of what `demand` kept: once it left, it receives nothing. */
  private releaseDemand(): void {
    this.demandEntries.clear()
    this.staleTracks.clear()
    this.listChanges.clear()
    this.allTracksStale = false
    if (this.shownDemand.length === 0) return
    this.shownDemand = NO_DEMAND
    this.demandChanges++
  }

  /** `shownDemand` with `listChanges` in it, in track order: the changes, sorted, merged into it in one pass. */
  private demandList(): TrackDemand[] {
    const changes = Array.from(this.listChanges, ([key, { entry }]) => ({ track: publicTrack(key), entry })).sort(
      byTrack,
    )
    const list: TrackDemand[] = []
    let next = 0
    const addChange = ({ entry }: (typeof changes)[number]) => {
      if (entry !== null) list.push(entry)
      next++
    }
    for (const shown of this.shownDemand) {
      let change = changes[next]
      while (change !== undefined && change.track < shown.track) {
        addChange(change)
        change = changes[next]
      }
      if (change !== undefined && change.track === shown.track) addChange(change)
      else list.push(shown)
    }
    for (const change of changes.slice(next)) addChange(change)
    return list
  }

  /** The entry of `track`, which is `was` while nothing in it changed. */
  private demandEntry(track: string, watch: TrackWatch, was: TrackDemand | undefined) {
    const { watchers, watcherCount } = this.watchersIn(watch)
    const same =
      was?.watcherCount === watcherCount &&
      was.watchers.length === watchers.length &&
      watchers.every((watcher, i) => watcher === was.watchers[i])
    if (same) return was
    return Object.freeze({ track, watchers: Object.freeze(watchers), watcherCount })
  }

  /** @internal Who receives any of `tracks`: each participant, and each page with nobody to name, once. */
  _watchersOf(tracks: readonly string[]): TracksWatchers {
    const union = { watchers: new Set<string>(), pages: new Set<string>() }
    for (const track of tracks) {
      const watch = this.watchesNow.get(trackKey(track))
      for (const id of watch?.watchers ?? []) union.watchers.add(id)
      for (const page of watch?.pages ?? []) union.pages.add(page)
    }
    return this.watchersIn(union)
  }

  /** The participants `watch` names as its room lists them, and how many receive the track. */
  private watchersIn({ watchers, pages }: TrackWatch): TracksWatchers {
    return { watchers: this._shownParticipants(watchers), watcherCount: watchers.size + pages.size }
  }

  /** @internal Who receives each of its tracks, as the room told it. */
  abstract get _watches(): ReadonlyMap<TrackKey, TrackWatch>

  /** Who receives each of its tracks: nobody once it left, whatever the room last told it. */
  private get watchesNow(): ReadonlyMap<TrackKey, TrackWatch> {
    return this._left ? NO_WATCHES : this._watches
  }
  /** The participants of `ids` as its room lists them now, in its order: none that left, is hidden, or whose join this
   *  side hasn't seen yet. */
  protected abstract _shownParticipants(ids: ReadonlySet<string>): RemoteParticipant[]
  /** The version of the roster `_shownParticipants` reads: the same while it shows the same participants. */
  protected abstract get _rosterVersion(): number
  /** Calls `onChange` at each change of the room it lists participants by; returns what stops it. */
  protected abstract _followRoom(onChange: () => void): () => void

  onChange(callback: () => void, options?: ListenerOptions): () => void {
    if (options?.signal?.aborted) return makeDisposer()
    if (!this.changeListeners?.size) this.seen = this.shown()
    if (this.changeListeners === null) {
      this.changeListeners = new Listeners()
      ;(this.listenerCleanups ??= new Set()).add(this._followRoom(() => this._changed()))
    }
    return this.register(this.changeListeners, callback, options)
  }

  /** @internal What it shows may have changed: its `onChange` listeners hear it once the current task's changes are
   *  in, if it did. */
  _changed(): void {
    if (this.changeDue || !this.changeListeners?.size) return
    this.changeDue = true
    queueMicrotask(() => this.tellChanged())
  }

  private tellChanged(): void {
    this.changeDue = false
    const now = this.shown()
    const seen = this.seen
    this.seen = now
    if (seen?.meta === now.meta && seen.connected === now.connected && seen.demand === now.demand) return
    this.fireAll(this.changeListeners)
  }

  private shown(): Shown {
    this.refreshDemand()
    return { meta: this.meta, connected: this.isConnected, demand: this.demandChanges }
  }

  onDisconnect(callback: () => void, options?: ListenerOptions): () => void {
    return this.register((this.disconnectListeners ??= new Listeners()), callback, options)
  }

  onReconnect(callback: () => void, options?: ListenerOptions): () => void {
    return this.register((this.reconnectListeners ??= new Listeners()), callback, options)
  }

  /** @internal */
  _onDisconnected(): void {
    this._changed()
    this.fireAll(this.disconnectListeners)
  }

  /** @internal */
  _onReconnected(): void {
    this._changed()
    this.fireAll(this.reconnectListeners)
  }

  onLeave(callback: (cause: LeaveCause) => void, options?: ListenerOptions): () => void {
    if (this.leftCause)
      return replayToLateListener(callback, [this.leftCause], options, (err) => this._reportError(err))
    return this.register((this.leaveListeners ??= new Listeners()), callback, options)
  }

  /** @internal */
  _onLeft(cause: LeaveCause): void {
    if (this.leftCause) return
    const ownedCause = ownLeaveCause(cause)
    this.leftCause = ownedCause
    this.releaseDemand()
    // Held ack DMs will never be handled now, so fail their senders instead of hanging them.
    const held = this.pendingInbox
    this.pendingInbox = null
    this.pendingInboxBytes = 0
    if (held) for (const entry of held) entry.ackResolve?.(DM_FAILURE.left)
    this.fireAll(this.leaveListeners, ownedCause)
    for (const unlisten of this.listenerCleanups ?? []) unlisten()
  }

  private assertActive(): void {
    if (this._left) throw participantLeftError()
  }

  private register<T>(list: Listeners<T>, cb: T, options: ListenerOptions | undefined): () => void {
    if (options?.signal?.aborted) return makeDisposer()
    const remove = list.add(cb)
    return makeDisposer(() => void remove(), (this.listenerCleanups ??= new Set()), options?.signal)
  }

  private fireAll<Args extends unknown[]>(
    listeners: Listeners<(...args: Args) => unknown> | null,
    ...args: Args
  ): void {
    for (const { listener } of listeners?.list() ?? []) {
      if (listener) invokeChannelListener(listener, args, (err) => this._reportError(err))
    }
  }
}

/** Who receives any of your `tracks`, such as the tracks of one media source: each person, and each page or server-side
 *  room with nobody to name, once. */
function watchersOf(me: LocalParticipant, tracks: readonly string[]): TracksWatchers {
  assert(ParticipantBase.isParticipantBase(me))
  return me._watchersOf(tracks)
}

/** In code-unit order, as `sort()` orders strings; no two entries share a track. */
function byTrack(a: { track: string }, b: { track: string }): number {
  return a.track < b.track ? -1 : 1
}
