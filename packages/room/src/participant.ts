export { ParticipantBase }

import { stringify } from '@brillout/json-serializer/stringify'
import { invokeChannelListener } from './channel.js'
import type { ChannelPublishAck, TELEFUNC_SHIELDS } from 'telefunc/__internal'
import { makeDisposer } from './disposer.js'
import { Listeners } from './utils/Listeners.js'
import { Fifo } from './utils/Fifo.js'
import { utf8ByteLength } from './utils/utf8ByteLength.js'
import { DM_FAILURE, participantLeftError, toRoomFailure } from './errors.js'
import { ownLeaveCause, ownMetadata, senderOf } from './model.js'
import { MemberMeta, type MetaRewinds } from './MemberMeta.js'
import { publicTrack, type TrackKey } from './binary.js'
import type { MemberMetaUpdate, WholeMemberUpdate, RoomResult, InboxMessage } from './protocol.js'
import type {
  BinaryPublishOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  Sender,
} from './types.js'

/** What holding a DM before the first `listen()` retains beyond its serialized bytes (about 55 measured), so many small
 *  ones are bounded by memory too. The inbox is the one lane no want gates, so the only one to bridge. */
const PENDING_INBOX_OVERHEAD_BYTES = 64
type HeldDm = { msg: InboxMessage; bytes: number; ackResolve?: (reply: RoomResult) => void }
/** Where the inbox delivers once it attaches: the local listeners, or the client holding the participant. */
type InboxTarget = { deliver(msg: InboxMessage): void; deliverAck(msg: InboxMessage): Promise<RoomResult> }

/** The private-message inbox and the leave lifecycle, identical on server and client; flavors supply the transport. */
abstract class ParticipantBase implements LocalParticipant {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  readonly id: string
  readonly identity: string | null
  readonly selfDelivery: boolean
  private readonly _meta: MemberMeta
  private leftCause: LeaveCause | null = null
  // Each list is made when its first listener is added.
  private leaveCbs: Listeners<(cause: LeaveCause) => unknown> | null = null
  private messageCbs: Listeners<(data: unknown, from: Sender | null) => unknown> | null = null
  private demandCbs: Listeners<(track: string | null, wanted: boolean) => unknown> | null = null
  private disconnectCbs: Listeners<() => unknown> | null = null
  private reconnectCbs: Listeners<() => unknown> | null = null
  private listenerCleanups: Set<() => void> | null = null
  private inboxAttached = false
  /** DMs held until the first `listen()` (`null` once flushed); an ack DM carries the resolver of its reply. */
  private pendingInbox: Fifo<HeldDm> | null = null
  private pendingInboxBytes = 0

  /** `rewinds` is its room view's, or its own for a participant a client holds alone. */
  constructor(
    participant: { id: string; meta: ParticipantMeta; selfDelivery: boolean; identity: string | null },
    rewinds: MetaRewinds,
  ) {
    this.id = participant.id
    this._meta = new MemberMeta({ meta: participant.meta, seq: 0, rewinds })
    this.selfDelivery = participant.selfDelivery
    this.identity = participant.identity
  }

  get meta(): ParticipantMeta {
    return this._meta.object
  }

  /** @internal Whole, as an attach resends it. */
  get _acceptedMeta(): WholeMemberUpdate {
    const update: WholeMemberUpdate = {
      meta: this._meta.object,
      seq: this._meta.seq,
    }
    if (!this._meta.connected) update.disconnected = true
    return update
  }

  /** The connection of the page holding this member, as the room accepted it. */
  protected get _acceptedConnected(): boolean {
    return this._meta.connected
  }

  /** @internal A meta update the room accepted, applied in seq order; whether it applied. One past a gap waits for the
   *  whole meta its holder's next attach or roster sends. */
  _acceptMeta(update: MemberMetaUpdate): boolean {
    return this._meta.apply(update) === 'applied'
  }

  /** @internal A failover rewound the seq: the next whole meta applies, whatever its seq. */
  _forgetMetaSeq(): void {
    this._meta.forget()
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
  /** A user callback threw. Each side reports through its own pipeline. */
  protected abstract _reportError(err: unknown): void
  /** An ack DM's reply to a bug in a `listen()` handler, naming where `_reportError` logged it. */
  protected abstract readonly _bugMessage: string
  /** The bytes of DMs held before the first `listen()`, past which the oldest is dropped. */
  protected abstract _inboxBufferLimit(): number

  listen(callback: (data: unknown, from: Sender | null) => unknown): () => void {
    const unlisten = this.register((this.messageCbs ??= new Listeners()), callback)
    this._flushHeld({ deliver: (msg) => this.fireInbox(msg), deliverAck: (msg) => this.fireInboxAck(msg) })
    return unlisten
  }

  /** @internal A DM for this member: to its listeners, held until the first `listen()`. */
  _deliverMessage(msg: InboxMessage): void {
    if (!this.messageCbs?.size) {
      if (this._left || this.inboxAttached) return
      this.hold(msg)
      return
    }
    this.fireInbox(msg)
  }

  /** @internal An `{ ack: true }` DM, resolved with the recipient's reply (or an error if it leaves first); never
   *  rejects. */
  _deliverMessageAck(msg: InboxMessage): Promise<RoomResult> {
    if (this.messageCbs?.size) return this.fireInboxAck(msg)
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
    const limit = this._inboxBufferLimit()
    while (this.pendingInboxBytes > limit) {
      const dropped = pending.shift()!
      this.pendingInboxBytes -= dropped.bytes
      dropped.ackResolve?.(DM_FAILURE.overflow)
    }
  }

  /** `from`/`fromMeta` come from the wire envelope; upgrades to the live `RemoteParticipant` when a room view
   *  exists. An empty `from` is the wire encoding of a room-authored message → `null`. */
  private inboxSender(msg: InboxMessage): Sender | null {
    const { from, fromMeta, fromIdentity: identity } = msg
    return from === '' ? null : (this._resolveSender(from) ?? senderOf(from, ownMetadata(fromMeta ?? {}), identity))
  }

  private fireInbox(msg: InboxMessage): void {
    const sender = this.inboxSender(msg)
    for (const cb of this.messageCbs?.list() ?? []) this.invoke(cb, msg.data, sender)
  }

  /** The last listener's return is the reply; a throw is the failure reply (see `toRoomFailure`: a bug is reported
   *  here, the sender gets only that it failed). */
  private async fireInboxAck(msg: InboxMessage): Promise<RoomResult> {
    const sender = this.inboxSender(msg)
    let result: unknown
    for (const cb of this.messageCbs?.list() ?? []) {
      try {
        result = await cb(msg.data, sender)
      } catch (err) {
        return toRoomFailure(err, (e) => this._reportError(e), this._bugMessage)
      }
    }
    return { ok: true, value: result }
  }

  /** The live room-backed sender, when this flavor has a room view. */
  protected _resolveSender(_id: string): Sender | null {
    return null
  }

  onDemand(callback: (track: string | null, wanted: boolean) => void): () => void {
    const unlisten = this.register((this.demandCbs ??= new Listeners()), callback)
    for (const track of this._demandedTracks) this.invoke(callback, publicTrack(track), true)
    return unlisten
  }

  /** @internal The wanted tracks, whole. */
  abstract get _demandedTracks(): TrackKey[]

  /** @internal Room-wide demand for one of this member's tracks changed. */
  _onDemand(track: TrackKey, wanted: boolean): void {
    for (const cb of this.demandCbs?.list() ?? []) this.invoke(cb, publicTrack(track), wanted)
  }

  onDisconnect(callback: () => void): () => void {
    return this.register((this.disconnectCbs ??= new Listeners()), callback)
  }

  onReconnect(callback: () => void): () => void {
    return this.register((this.reconnectCbs ??= new Listeners()), callback)
  }

  /** @internal */
  _onDisconnected(): void {
    for (const cb of this.disconnectCbs?.list() ?? []) this.invoke(cb)
  }

  /** @internal */
  _onReconnected(): void {
    for (const cb of this.reconnectCbs?.list() ?? []) this.invoke(cb)
  }

  onLeave(callback: (cause: LeaveCause) => void): () => void {
    if (this.leftCause) {
      this.invoke(callback, this.leftCause)
      return makeDisposer()
    }
    return this.register((this.leaveCbs ??= new Listeners()), callback)
  }

  /** @internal The member is gone; `cause` says how. */
  _onLeft(cause: LeaveCause): void {
    if (this.leftCause) return
    const ownedCause = ownLeaveCause(cause)
    this.leftCause = ownedCause
    // Held ack DMs will never be handled now, so fail their senders instead of hanging them.
    const held = this.pendingInbox
    this.pendingInbox = null
    this.pendingInboxBytes = 0
    if (held) for (const entry of held) entry.ackResolve?.(DM_FAILURE.left)
    const cbs = this.leaveCbs?.list() ?? []
    for (const unlisten of this.listenerCleanups ?? []) unlisten()
    for (const cb of cbs) this.invoke(cb, ownedCause)
  }

  protected _assertActive(): void {
    if (this._left) throw participantLeftError()
  }

  private register<T>(list: Listeners<T>, cb: T): () => void {
    const remove = list.add(cb)
    return makeDisposer(() => void remove(), (this.listenerCleanups ??= new Set()))
  }

  private invoke<Args extends unknown[]>(cb: (...args: Args) => unknown, ...args: Args): void {
    invokeChannelListener(cb, args, (err) => this._reportError(err))
  }
}
