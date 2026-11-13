export { ParticipantBase }
export type { ParticipantBaseOptions }

import { stringify } from '@brillout/json-serializer/stringify'
import {
  invokeChannelListener,
  type ChannelPublishAck,
  type TELEFUNC_SHIELDS,
  Listeners,
  Fifo,
  PUBLISH_HOLD_BYTES,
  soleAnswer,
  TWO_ANSWERS,
  utf8ByteLength,
} from 'telefunc/__internal'
import { makeDisposer, type ListenerOptions } from './disposer.js'
import { DM_FAILURE, participantLeftError, toRoomFailure } from './errors.js'
import { ownLeaveCause, ownMetadata, senderSnapshot } from './arguments.js'
import { MemberMeta, type MetaRewinds } from './MemberMeta.js'
import { DEFAULT_TRACK, encodeBinaryFrame, encodeMessageFrame, publicTrack, type TrackKey } from './binary.js'
import type { MemberMetaUpdate, WholeMemberUpdate, RoomResult, InboxMessage, TrackWatch } from './protocol.js'
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
const NO_DEMAND: readonly TrackDemand[] = Object.freeze([])

type ParticipantBaseOptions = {
  participant: { id: string; meta: ParticipantMeta; selfDelivery: boolean; identity: string | null }
  /** Shared with its room view's members; a participant a client holds without a room view has its own. */
  rewinds: MetaRewinds
}

/** The private-message inbox and the leave lifecycle, identical on server and client; flavors supply the transport. */
abstract class ParticipantBase implements LocalParticipant {
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

  constructor({ participant, rewinds }: ParticipantBaseOptions) {
    this.id = participant.id
    this.memberMeta = new MemberMeta({ meta: participant.meta, seq: 0, rewinds })
    this.selfDelivery = participant.selfDelivery
    this.identity = participant.identity
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
    if (this.memberMeta.apply(update) !== 'applied') return false
    this._changed()
    return true
  }

  protected get _left(): boolean {
    return this.leftCause !== null
  }

  abstract publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck>
  abstract publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck>
  // The overloads live on `LocalParticipant`; this is their implementation signature.
  abstract send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any>
  /** @internal A package's message to `to`, for its field `field` there (`sendPrivate()`). */
  abstract _sendPrivate(to: string, field: string, data: unknown): Promise<void>
  abstract setMeta(meta: ParticipantMeta): Promise<void>
  abstract setAttributes(attributes: ParticipantMeta): Promise<void>
  abstract leave(): Promise<void>
  abstract get isConnected(): boolean
  /** A user callback threw. Each side reports through its own pipeline. */
  protected abstract _reportError(err: unknown): void
  /** An ack DM's reply to a bug in a `listen()` handler, naming where `_reportError` logged it. */
  protected abstract readonly _bugMessage: string

  protected encodeBinaryFrame(data: Uint8Array, options?: BinaryPublishOptions): Uint8Array {
    return encodeBinaryFrame(this.id, data, options)
  }

  /** The frame of a `publish()` on a track. */
  protected encodeTextFrame(data: unknown, options: PublishOptions): Uint8Array {
    return encodeMessageFrame(this.id, data, options)
  }

  /** The stream this member's publishes on `track` go up in, in order: a newer lossy one replaces an older one still
   *  waiting, and a plain one goes after it. */
  protected trackStreamOf(track: string | undefined): string {
    return `${this.id}:${track ?? DEFAULT_TRACK}`
  }

  listen(callback: (data: unknown, from: Sender | null) => unknown, options?: ListenerOptions): () => void {
    if (options?.signal?.aborted) return makeDisposer()
    const unlisten = this.register((this.messageListeners ??= new Listeners()), callback, options)
    this._flushHeld({ deliver: (msg) => this.fireInbox(msg), deliverAck: (msg) => this.fireInboxAck(msg) })
    return unlisten
  }

  /** @internal A DM for this member: to its listeners, held until the first `listen()`. */
  _deliverMessage(msg: InboxMessage): void {
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

  /** Who receives each of its tracks, in the order they started: each entry the same object until its watchers or
   *  count change, and the list the same array until an entry does. */
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
    const same = next.length === shown.length && next.every((entry, i) => entry === shown[i])
    if (!same) this.shownDemand = Object.freeze(next)
    return this.shownDemand
  }

  /** The entry of `track`, which is `was` while nothing in it changed. */
  private demandEntry(track: string | null, { watchers: ids, count }: TrackWatch, was: TrackDemand | undefined) {
    const watchers: RemoteParticipant[] = []
    for (const id of ids) {
      const watcher = this._shownParticipant(id)
      if (watcher) watchers.push(watcher)
    }
    watchers.sort((a, b) => a.joinedAt - b.joinedAt || (a.id < b.id ? -1 : 1))
    const same =
      was?.watcherCount === count &&
      was.watchers.length === watchers.length &&
      watchers.every((watcher, i) => watcher === was.watchers[i])
    if (same) return was
    return Object.freeze({ track, watchers: Object.freeze(watchers), watcherCount: count })
  }

  /** @internal Who receives each of its tracks, as the room told it. */
  abstract get _watches(): ReadonlyMap<TrackKey, TrackWatch>
  /** The participant as its room lists it now, or `null` where it doesn't: one that left, is hidden, or whose join this
   *  side hasn't seen yet. */
  protected abstract _shownParticipant(id: string): RemoteParticipant | null
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
    if (options?.signal?.aborted) return makeDisposer()
    if (this.leftCause) {
      this.invoke(callback, this.leftCause)
      return makeDisposer()
    }
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

  protected _assertActive(): void {
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
