export { ParticipantBase }

import { stringify } from '@brillout/json-serializer/stringify'
import { invokeChannelListener, type ChannelPublishAck } from '../channel/channel.js'
import { makeDisposer } from './disposer.js'
import { Listeners } from '../../utils/Listeners.js'
import { Fifo } from '../../utils/Fifo.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shieldKey.js'
import { DM_FAILURE, participantLeftError, toRoomFailure } from './errors.js'
import { ownLeaveCause, ownMetadata, senderOf } from './model.js'
import { MemberMeta, type MetaRewinds } from './MemberMeta.js'
import { publicTrack, type TrackKey } from './binary.js'
import type { MemberMetaUpdate, WholeMemberUpdate, DmReply, InboxMessage } from './protocol.js'
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
type HeldDm = { msg: InboxMessage; bytes: number; ackResolve?: (reply: DmReply) => void }
type InboxForwarder = { deliver(msg: InboxMessage): void; deliverAck(msg: InboxMessage): Promise<DmReply> }

/** The private-message inbox and the leave lifecycle, identical on server and client; flavors supply the transport. */
abstract class ParticipantBase implements LocalParticipant {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  readonly id: string
  readonly identity: string | null
  readonly selfDelivery: boolean
  private readonly _meta: MemberMeta
  private _leftCause: LeaveCause | null = null
  // Each list is made when its first listener is added.
  private _leaveCbs: Listeners<(cause: LeaveCause) => unknown> | null = null
  private _messageCbs: Listeners<(data: unknown, from: Sender | null) => unknown> | null = null
  private _demandCbs: Listeners<(track: string | null, wanted: boolean) => unknown> | null = null
  private _disconnectCbs: Listeners<() => unknown> | null = null
  private _reconnectCbs: Listeners<() => unknown> | null = null
  private _listenerCleanups: Set<() => void> | null = null
  private _inboxAttached = false
  /** DMs held until the first `listen()` (`null` once flushed); an ack DM carries the resolver of its reply. */
  private _pendingInbox: Fifo<HeldDm> | null = null
  private _pendingInboxBytes = 0
  /** When a client holds this participant, its inbox forwards there instead of to local listeners (see
   *  `RoomParticipantStubChannel`). */
  private _forwarder: InboxForwarder | null = null

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

  /** @internal Route this participant's inbox to a remote holder instead of local listeners. */
  _setForwarder(forwarder: InboxForwarder): void {
    this._forwarder = forwarder
    this._flushHeld(forwarder)
  }

  /** @internal Already bound to a client holder (serialized once, via `RoomParticipantStubChannel`)? */
  get _isBound(): boolean {
    return this._forwarder !== null
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
    return this._leftCause !== null
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
    const unlisten = this._register((this._messageCbs ??= new Listeners()), callback)
    this._flushHeld({ deliver: (msg) => this._fireInbox(msg), deliverAck: (msg) => this._fireInboxAck(msg) })
    return unlisten
  }

  /** @internal A DM for this member: to its remote holder if bound, else its listeners (held until the first
   *  `listen()`). */
  _deliverMessage(msg: InboxMessage): void {
    if (this._forwarder) return this._forwarder.deliver(msg)
    if (!this._messageCbs?.size) {
      if (this._left || this._inboxAttached) return
      this._hold(msg)
      return
    }
    this._fireInbox(msg)
  }

  /** @internal An `{ ack: true }` DM, resolved with the recipient's reply (or an error if it leaves first); never
   *  rejects. */
  _deliverMessageAck(msg: InboxMessage): Promise<DmReply> {
    if (this._forwarder) return this._forwarder.deliverAck(msg)
    if (this._messageCbs?.size) return this._fireInboxAck(msg)
    if (this._left) return Promise.resolve(DM_FAILURE.left)
    if (this._inboxAttached) return Promise.resolve(DM_FAILURE.noListener)
    return new Promise<DmReply>((resolve) => this._hold(msg, resolve))
  }

  /** The inbox attached: DMs held until now go out in order, and nothing is held again. */
  private _flushHeld(to: InboxForwarder): void {
    this._inboxAttached = true
    const held = this._pendingInbox
    this._pendingInbox = null
    this._pendingInboxBytes = 0
    for (const { msg, ackResolve } of held ?? []) {
      if (ackResolve) void to.deliverAck(msg).then(ackResolve)
      else to.deliver(msg)
    }
  }

  private _hold(msg: InboxMessage, ackResolve?: (reply: DmReply) => void): void {
    const pending = (this._pendingInbox ??= new Fifo())
    const bytes = utf8ByteLength(stringify(msg)) + PENDING_INBOX_OVERHEAD_BYTES
    pending.push({ msg, bytes, ackResolve })
    this._pendingInboxBytes += bytes
    const limit = this._inboxBufferLimit()
    while (this._pendingInboxBytes > limit) {
      const dropped = pending.shift()!
      this._pendingInboxBytes -= dropped.bytes
      dropped.ackResolve?.(DM_FAILURE.overflow)
    }
  }

  /** `from`/`fromMeta` come from the wire envelope; upgrades to the live `RemoteParticipant` when a room view
   *  exists. An empty `from` is the wire encoding of a room-authored message → `null`. */
  private _senderOf(msg: InboxMessage): Sender | null {
    const { from, fromMeta, fromIdentity: identity } = msg
    return from === '' ? null : (this._resolveSender(from) ?? senderOf(from, ownMetadata(fromMeta ?? {}), identity))
  }

  private _fireInbox(msg: InboxMessage): void {
    const sender = this._senderOf(msg)
    for (const cb of this._messageCbs?.list() ?? []) this._invoke(cb, msg.data, sender)
  }

  /** The last listener's return is the reply; a throw is the failure reply (`Abort(value)` reaches the sender, anything
   *  else is a bug reported here). */
  private async _fireInboxAck(msg: InboxMessage): Promise<DmReply> {
    const sender = this._senderOf(msg)
    let result: unknown
    for (const cb of this._messageCbs?.list() ?? []) {
      try {
        result = await cb(msg.data, sender)
      } catch (err) {
        return toRoomFailure(err, (e) => this._reportError(e), this._bugMessage)
      }
    }
    return { ok: true, result }
  }

  /** The live room-backed sender, when this flavor has a room view. */
  protected _resolveSender(_id: string): Sender | null {
    return null
  }

  onDemand(callback: (track: string | null, wanted: boolean) => void): () => void {
    const unlisten = this._register((this._demandCbs ??= new Listeners()), callback)
    for (const track of this._demandedTracks) this._invoke(callback, publicTrack(track), true)
    return unlisten
  }

  /** @internal The wanted tracks, whole. */
  abstract get _demandedTracks(): TrackKey[]

  /** @internal Room-wide demand for one of this member's tracks changed. */
  _onDemand(track: TrackKey, wanted: boolean): void {
    for (const cb of this._demandCbs?.list() ?? []) this._invoke(cb, publicTrack(track), wanted)
  }

  onDisconnect(callback: () => void): () => void {
    return this._register((this._disconnectCbs ??= new Listeners()), callback)
  }

  onReconnect(callback: () => void): () => void {
    return this._register((this._reconnectCbs ??= new Listeners()), callback)
  }

  /** @internal */
  _onDisconnected(): void {
    for (const cb of this._disconnectCbs?.list() ?? []) this._invoke(cb)
  }

  /** @internal */
  _onReconnected(): void {
    for (const cb of this._reconnectCbs?.list() ?? []) this._invoke(cb)
  }

  onLeave(callback: (cause: LeaveCause) => void): () => void {
    if (this._leftCause) {
      this._invoke(callback, this._leftCause)
      return makeDisposer()
    }
    return this._register((this._leaveCbs ??= new Listeners()), callback)
  }

  /** @internal The member is gone; `cause` says how. */
  _onLeft(cause: LeaveCause): void {
    if (this._leftCause) return
    const ownedCause = ownLeaveCause(cause)
    this._leftCause = ownedCause
    // Held ack DMs will never be handled now, so fail their senders instead of hanging them.
    const held = this._pendingInbox
    this._pendingInbox = null
    this._pendingInboxBytes = 0
    if (held) for (const entry of held) entry.ackResolve?.(DM_FAILURE.left)
    const cbs = this._leaveCbs?.list() ?? []
    for (const unlisten of this._listenerCleanups ?? []) unlisten()
    for (const cb of cbs) this._invoke(cb, ownedCause)
  }

  protected _assertActive(): void {
    if (this._left) throw participantLeftError()
  }

  private _register<T>(list: Listeners<T>, cb: T): () => void {
    const remove = list.add(cb)
    return makeDisposer(() => void remove(), (this._listenerCleanups ??= new Set()))
  }

  private _invoke<Args extends unknown[]>(cb: (...args: Args) => unknown, ...args: Args): void {
    invokeChannelListener(cb, args, (err) => this._reportError(err))
  }
}
