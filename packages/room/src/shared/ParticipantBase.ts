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
import {
  DEFAULT_TRACK,
  encodeBinaryFrame,
  encodeMessageFrame,
  publicTrack,
  trackStreamKey,
  type TrackKey,
} from './binary.js'
import { assertUsage } from '../utils/assert.js'
import type { MemberMetaUpdate, WholeMemberUpdate, RoomResult, InboxMessage } from './protocol.js'
import type {
  BinaryPublishOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  Sender,
} from '../types.js'

/** What holding a DM before the first `listen()` retains beyond its serialized bytes (about 55 measured), so many small
 *  ones are bounded by memory too. The inbox is the one lane no want gates, so the only one to bridge. */
const PENDING_INBOX_OVERHEAD_BYTES = 64
type HeldDm = { msg: InboxMessage; bytes: number; ackResolve?: (reply: RoomResult) => void }
/** Where the inbox delivers once it attaches: the local listeners, or the client holding the participant. */
type InboxTarget = { deliver(msg: InboxMessage): void; deliverAck(msg: InboxMessage): Promise<RoomResult> }

type ParticipantBaseOptions = {
  participant: { id: string; meta: ParticipantMeta; selfDelivery: boolean; identity: string | null }
  /** Shared with its room view's members; a participant a client holds without a room view has its own. */
  rewinds: MetaRewinds
}

/** The private-message inbox and the leave lifecycle, identical on server and client; flavors supply the transport. */
/** A track of a member's on its way up: its stream's name, and whether a newer frame replaces an older one waiting. */
type TrackStream = { name: string; lossy: boolean }

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
  private demandListeners: Listeners<(track: string | null, wanted: boolean) => unknown> | null = null
  private watchListeners: Listeners<(track: string | null) => unknown> | null = null
  private disconnectListeners: Listeners<() => unknown> | null = null
  private reconnectListeners: Listeners<() => unknown> | null = null
  private listenerCleanups: Set<() => void> | null = null
  private inboxAttached = false
  /** DMs held until the first `listen()` (`null` once flushed); an ack DM carries the resolver of its reply. */
  private pendingInbox: Fifo<HeldDm> | null = null
  private pendingInboxBytes = 0
  /** The `lossy` each track's first publish gave it. */
  private readonly trackLossy = new Map<TrackKey, boolean>()

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
    return this.memberMeta.apply(update) === 'applied'
  }

  protected get _left(): boolean {
    return this.leftCause !== null
  }

  abstract publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck>
  abstract publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck>
  // The overloads live on `LocalParticipant`; this is their implementation signature.
  abstract send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any>
  abstract setMeta(meta: ParticipantMeta): Promise<void>
  abstract setAttributes(attributes: ParticipantMeta): Promise<void>
  abstract leave(): Promise<void>
  abstract get isConnected(): boolean
  abstract get uplinkRate(): number
  /** A user callback threw. Each side reports through its own pipeline. */
  protected abstract _reportError(err: unknown): void
  /** An ack DM's reply to a bug in a `listen()` handler, naming where `_reportError` logged it. */
  protected abstract readonly _bugMessage: string

  protected encodeBinaryFrame(data: Uint8Array, options?: BinaryPublishOptions): Uint8Array {
    return this.encodeOnTrack(encodeBinaryFrame, data, options)
  }

  /** The frame of a `publish()` on a track. */
  protected encodeTextFrame(data: unknown, options: PublishOptions): Uint8Array {
    return this.encodeOnTrack(encodeMessageFrame, data, options)
  }

  /** A track keeps the `lossy` its first publish gave it, which a later one may leave out. */
  private encodeOnTrack<Payload>(
    encode: (memberId: string, payload: Payload, opts: BinaryPublishOptions) => Uint8Array,
    payload: Payload,
    options: BinaryPublishOptions | undefined,
  ): Uint8Array {
    const track = options?.track ?? DEFAULT_TRACK
    const declared = this.trackLossy.get(track)
    const lossy = options?.lossy ?? declared ?? false
    assertUsage(
      declared === undefined || lossy === declared,
      `The ${track === DEFAULT_TRACK ? 'default' : `"${track}"`} track's first publish gave it lossy: ${declared}, which a track keeps`,
    )
    const framed = encode(this.id, payload, { ...options, lossy })
    if (declared === undefined) this.trackLossy.set(track, lossy)
    return framed
  }

  /** The stream of this member's `track` on its way up: its frames go on a domain of their own (see telefunc's
   *  `TAG.DOMAIN`), so no other track's loss delays them, and a newer frame of a lossy one replaces an older one. */
  protected trackStreamOf(track: string | undefined): TrackStream {
    const key = track ?? DEFAULT_TRACK
    return { name: trackStreamKey(this.id, key), lossy: this.trackLossy.get(key) === true }
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

  onDemand(callback: (track: string | null, wanted: boolean) => void, options?: ListenerOptions): () => void {
    if (options?.signal?.aborted) return makeDisposer()
    const unlisten = this.register((this.demandListeners ??= new Listeners()), callback, options)
    for (const track of this._demandedTracks) this.invoke(callback, publicTrack(track), true)
    return unlisten
  }

  /** @internal */
  abstract get _demandedTracks(): TrackKey[]

  /** @internal Room-wide demand for one of this member's tracks changed. */
  _onDemand(track: TrackKey, wanted: boolean): void {
    for (const cb of this.demandListeners?.list() ?? []) this.invoke(cb, publicTrack(track), wanted)
  }

  onWatch(callback: (track: string | null) => void, options?: ListenerOptions): () => void {
    return this.register((this.watchListeners ??= new Listeners()), callback, options)
  }

  /** @internal A page started watching one of this member's binary tracks. */
  _onWatch(track: TrackKey): void {
    for (const cb of this.watchListeners?.list() ?? []) this.invoke(cb, publicTrack(track))
  }

  onDisconnect(callback: () => void, options?: ListenerOptions): () => void {
    return this.register((this.disconnectListeners ??= new Listeners()), callback, options)
  }

  onReconnect(callback: () => void, options?: ListenerOptions): () => void {
    return this.register((this.reconnectListeners ??= new Listeners()), callback, options)
  }

  /** @internal */
  _onDisconnected(): void {
    for (const cb of this.disconnectListeners?.list() ?? []) this.invoke(cb)
  }

  /** @internal */
  _onReconnected(): void {
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
