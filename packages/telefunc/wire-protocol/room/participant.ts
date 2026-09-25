export { ParticipantBase }

import { invokeChannelListener, type ChannelPublishAck } from '../channel.js'
import { makeDisposer } from '../wrapProxy.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import { assert } from '../../utils/assert.js'
import { DM_FAILURE, participantLeftError, toRoomFailure } from './errors.js'
import { ownLeaveCause, ownMetadata, senderOf } from './model.js'
import type { AcceptedMeta, DmReply, InboxMessage } from './protocol.js'
import type {
  BinaryPublishOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  Sender,
} from './types.js'
/** DMs held before the first `listen()`, dropping the oldest. The inbox is the one lane no want gates, so the only one to bridge. */
const PENDING_INBOX_MAX_COUNT = 64
/** The private-message inbox and the leave lifecycle, identical on server and client; flavors supply the transport. */
abstract class ParticipantBase implements LocalParticipant {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  readonly id: string
  readonly identity: string | null
  readonly selfDelivery: boolean
  private _meta: ParticipantMeta
  private _metaSeq = 0
  private _leftCause: LeaveCause | null = null
  private _leaveCbs: Array<(cause: LeaveCause) => unknown> = []
  private readonly _messageCbs: Array<(data: unknown, from: Sender | null) => unknown> = []
  private readonly _demandCbs: Array<(track: string | null, wanted: boolean) => unknown> = []
  private readonly _wantedTracks = new Set<string | null>()
  private readonly _listenerCleanups = new Set<() => void>()
  private _inboxAttached = false
  /** DMs held until the first `listen()` (`null` once flushed); an ack DM carries the resolver of its reply. */
  private _pendingInbox: Array<{ msg: InboxMessage; ackResolve?: (reply: DmReply) => void }> | null = null
  /** When a client holds this participant, its inbox forwards there instead of to local listeners; the forwarder returns the client's reply for an ack DM (see `RoomParticipantStubChannel`). */
  private _forwarder: ((msg: InboxMessage) => Promise<DmReply> | void) | null = null
  /** @internal Route this participant's inbox to a remote holder instead of local listeners. */
  _setForwarder(forwarder: (msg: InboxMessage) => Promise<DmReply> | void): void {
    this._forwarder = forwarder
    this._flushHeld((msg, ackResolve) => {
      const reply = forwarder(msg)
      if (!ackResolve) return
      assert(reply) // an ack DM's forwarder answers
      void reply.then(ackResolve)
    })
  }
  /** @internal Already bound to a client holder (serialized once, via `RoomParticipantStubChannel`)? */
  get _isBound(): boolean {
    return this._forwarder !== null
  }
  constructor(id: string, meta: ParticipantMeta, selfDelivery: boolean, identity: string | null) {
    this.id = id
    this._meta = ownMetadata(meta)
    this.selfDelivery = selfDelivery
    this.identity = identity
  }
  get meta(): ParticipantMeta {
    return this._meta
  }
  /** @internal A meta the room accepted at `seq`; an older one never replaces a newer. */
  _acceptMeta({ meta, seq }: AcceptedMeta): void {
    if (seq <= this._metaSeq) return
    this._metaSeq = seq
    this._meta = ownMetadata(meta)
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
  /** A user callback threw. Each side reports through its own pipeline. */
  protected abstract _reportError(err: unknown): void
  listen(callback: (data: unknown, from: Sender | null) => unknown): () => void {
    const unlisten = this._register(this._messageCbs, callback)
    this._flushHeld((msg, ackResolve) => {
      if (ackResolve) void this._fireInboxAck(msg).then(ackResolve)
      else this._fireInbox(msg)
    })
    return unlisten
  }
  /** @internal A DM for this member: to its remote holder if bound, else its listeners (held until the first `listen()`). */
  _deliverMessage(msg: InboxMessage): void {
    if (this._forwarder) {
      void this._forwarder(msg)
      return
    }
    if (this._messageCbs.length === 0) {
      if (this._left || this._inboxAttached) return
      this._hold(msg)
      return
    }
    this._fireInbox(msg)
  }
  /** @internal An `{ ack: true }` DM, resolved with the recipient's reply (or an error if it leaves first); never rejects. */
  _deliverMessageAck(msg: InboxMessage): Promise<DmReply> {
    if (this._forwarder) {
      const reply = this._forwarder(msg)
      assert(reply) // an ack DM's forwarder answers
      return reply
    }
    if (this._messageCbs.length === 0) {
      if (this._left) return Promise.resolve(DM_FAILURE.left)
      if (this._inboxAttached) return Promise.resolve(DM_FAILURE.noListener)
      return new Promise<DmReply>((resolve) => this._hold(msg, resolve))
    }
    return this._fireInboxAck(msg)
  }
  /** The inbox attached: DMs held until now go out in order, and nothing is held again. */
  private _flushHeld(deliver: (msg: InboxMessage, ackResolve?: (reply: DmReply) => void) => void): void {
    this._inboxAttached = true
    const held = this._pendingInbox
    this._pendingInbox = null
    for (const { msg, ackResolve } of held ?? []) deliver(msg, ackResolve)
  }
  private _hold(msg: InboxMessage, ackResolve?: (reply: DmReply) => void): void {
    const pending = (this._pendingInbox ??= [])
    pending.push({ msg, ackResolve })
    if (pending.length > PENDING_INBOX_MAX_COUNT) {
      pending.shift()?.ackResolve?.(DM_FAILURE.overflow)
    }
  }
  /** `from`/`fromMeta` come from the wire envelope; upgrades to the live `RemoteParticipant` when a room view exists. An empty `from` is the wire encoding of a room-authored message → `null`. */
  private _senderOf(msg: InboxMessage): Sender | null {
    const { from, fromMeta, fromIdentity: identity } = msg
    return from === '' ? null : (this._resolveSender(from) ?? senderOf(from, ownMetadata(fromMeta ?? {}), identity))
  }
  private _fireInbox(msg: InboxMessage): void {
    const sender = this._senderOf(msg)
    for (const cb of [...this._messageCbs]) this._invoke(cb, msg.data, sender)
  }
  /** The last listener's return is the reply; a throw is the failure reply (`Abort(value)` reaches the sender, anything else is a bug reported here). */
  private async _fireInboxAck(msg: InboxMessage): Promise<DmReply> {
    const sender = this._senderOf(msg)
    let result: unknown
    for (const cb of [...this._messageCbs]) {
      try {
        result = await cb(msg.data, sender)
      } catch (err) {
        return toRoomFailure(err, (e) => this._reportError(e))
      }
    }
    return { ok: true, result }
  }
  /** The live room-backed sender, when this flavor has a room view. */
  protected _resolveSender(_id: string): Sender | null {
    return null
  }
  onDemand(callback: (track: string | null, wanted: boolean) => void): () => void {
    const unlisten = this._register(this._demandCbs, callback)
    for (const track of this._wantedTracks) this._invoke(callback, track, true)
    return unlisten
  }
  /** @internal Room-wide demand for one of this member's tracks changed; `null` is the default track. */
  _onDemand(track: string | null, wanted: boolean): void {
    if (wanted) this._wantedTracks.add(track)
    else this._wantedTracks.delete(track)
    for (const cb of [...this._demandCbs]) this._invoke(cb, track, wanted)
  }
  onLeave(callback: (cause: LeaveCause) => void): () => void {
    if (this._leftCause) {
      this._invoke(callback, this._leftCause)
      return makeDisposer()
    }
    return this._register(this._leaveCbs, callback)
  }
  /** @internal The member is gone; `cause` says how. */
  _onLeft(cause: LeaveCause): void {
    if (this._leftCause) return
    const ownedCause = (this._leftCause = ownLeaveCause(cause))
    // Held ack DMs will never be handled now, so fail their senders instead of hanging them.
    const held = this._pendingInbox
    this._pendingInbox = null
    if (held) for (const entry of held) entry.ackResolve?.(DM_FAILURE.left)
    const cbs = [...this._leaveCbs]
    for (const unlisten of [...this._listenerCleanups]) unlisten()
    for (const cb of cbs) this._invoke(cb, ownedCause)
    this._wantedTracks.clear()
  }
  protected _assertActive(): void {
    if (this._left) throw participantLeftError()
  }
  private _register<T>(list: T[], cb: T): () => void {
    list.push(cb)
    return makeDisposer(() => {
      const i = list.indexOf(cb)
      if (i >= 0) list.splice(i, 1)
    }, this._listenerCleanups)
  }
  private _invoke<Args extends unknown[]>(cb: (...args: Args) => unknown, ...args: Args): void {
    invokeChannelListener(cb, args, (err) => this._reportError(err))
  }
}
