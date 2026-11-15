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
import { assertUsage } from '../utils/assert.js'
import { makeDisposer, replayToLateListener, type ListenerOptions } from './disposer.js'
import { DM_FAILURE, participantLeftError, toRoomFailure } from './errors.js'
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

/** What `onChange` tells a change of. */
type Shown = { meta: ParticipantMeta; connected: boolean; demand: readonly TrackDemand[] }
/** Who receives some tracks, counted as `demand` counts one. */
type TracksWatchers = { watchers: RemoteParticipant[]; watcherCount: number }
const NO_DEMAND: readonly TrackDemand[] = Object.freeze([])

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
  /** An ack DM's reply to a bug in a `listen()` handler, naming where `_reportError` logged it. */
  protected abstract readonly _bugMessage: string

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

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    const owned = ownMetaArgument(attrs, 'setAttributes() attributes')
    await this.writeMeta({ attrs: owned }, { __r: 'req-set-attrs', attrs: owned })
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

  listen(callback: (data: unknown, from: Sender | null) => unknown, options?: ListenerOptions): () => void {
    if (options?.signal?.aborted) return makeDisposer()
    const unlisten = this.register((this.messageListeners ??= new Listeners()), callback, options)
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
      const dropped = pending.shift()!
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
    const sender = this.inboxSender(msg)
    for (const cb of this.messageListeners?.list() ?? []) this.invoke(cb, msg.data, sender)
  }

  /** Channel's rule (`soleAnswer`) picks the reply; a throw is the failure reply (see `toRoomFailure`: a bug is
   *  reported here, the sender gets only that it failed). */
  private async fireInboxAck(msg: InboxMessage): Promise<RoomResult> {
    let answer: unknown
    try {
      answer = await soleAnswer(this.messageListeners?.list() ?? [], msg.data, this.inboxSender(msg))
    } catch (err) {
      return toRoomFailure(err, (e) => this._reportError(e), this._bugMessage)
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
    if (this._left) return (this.shownDemand = NO_DEMAND)
    const shown = this.shownDemand
    const next: TrackDemand[] = []
    for (const [key, watch] of this._watches) {
      const track = publicTrack(key)
      next.push(
        this.demandEntry(
          track,
          watch,
          shown.find((entry) => entry.track === track),
        ),
      )
    }
    next.sort(byTrack)
    const same = next.length === shown.length && next.every((entry, i) => entry === shown[i])
    if (!same) this.shownDemand = Object.freeze(next)
    return this.shownDemand
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
      const watch = this._watches.get(trackKey(track))
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
  /** The participants of `ids` as its room lists them now, in its order: none that left, is hidden, or whose join this
   *  side hasn't seen yet. */
  protected abstract _shownParticipants(ids: ReadonlySet<string>): RemoteParticipant[]
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
    for (const cb of this.changeListeners?.list() ?? []) this.invoke(cb)
  }

  private shown(): Shown {
    return { meta: this.meta, connected: this.isConnected, demand: this.demand }
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
    for (const cb of this.disconnectListeners?.list() ?? []) this.invoke(cb)
  }

  /** @internal */
  _onReconnected(): void {
    this._changed()
    for (const cb of this.reconnectListeners?.list() ?? []) this.invoke(cb)
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
    // Held ack DMs will never be handled now, so fail their senders instead of hanging them.
    const held = this.pendingInbox
    this.pendingInbox = null
    this.pendingInboxBytes = 0
    if (held) for (const entry of held) entry.ackResolve?.(DM_FAILURE.left)
    const leaveListeners = this.leaveListeners?.list() ?? []
    for (const unlisten of this.listenerCleanups ?? []) unlisten()
    for (const cb of leaveListeners) this.invoke(cb, ownedCause)
  }

  private assertActive(): void {
    if (this._left) throw participantLeftError()
  }

  private register<T>(list: Listeners<T>, cb: T, options: ListenerOptions | undefined): () => void {
    if (options?.signal?.aborted) return makeDisposer()
    const remove = list.add(cb)
    return makeDisposer(() => void remove(), (this.listenerCleanups ??= new Set()), options?.signal)
  }

  private invoke<Args extends unknown[]>(cb: (...args: Args) => unknown, ...args: Args): void {
    invokeChannelListener(cb, args, (err) => this._reportError(err))
  }
}

/** Who receives any of your `tracks`, such as the tracks of one media source: each person, and each page or server-side
 *  room with nobody to name, once. */
function watchersOf(me: LocalParticipant, tracks: readonly string[]): TracksWatchers {
  return (me as ParticipantBase)._watchersOf(tracks)
}

/** In code-unit order, as `sort()` orders strings; no two entries share a track. */
function byTrack(a: TrackDemand, b: TrackDemand): number {
  return a.track < b.track ? -1 : 1
}
