export { ParticipantBase }
export type { InboxMessage }

import type { ChannelPublishAck } from '../channel.js'
import { makeDisposer } from '../wrapProxy.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import { isPromise } from '../../utils/isPromise.js'
import { DM_PARTICIPANT_LEFT, RoomError, toRoomFailure } from './errors.js'
import { ownLeaveCause, ownMetadata } from './model.js'
import type { DmReply } from './protocol.js'
import type {
  BinaryPublishOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  Sender,
} from './types.js'
// ParticipantBase — the shared half of every LocalParticipant
/** The private-message inbox and the leave lifecycle, identical on server and client. Flavors supply the transport through the abstract operations and their own error pipeline. */
/** A delivered private message, as stamped by the sender's node. `ackId` is present when the sender awaits a reply (`send(…, { ack: true })`) — the recipient's handler return is routed back. */
type InboxMessage = {
  from: string
  fromMeta: ParticipantMeta | null
  fromIdentity: string | null
  data: unknown
  ackId?: string
}
/** Pre-listen inbox hold: count-capped, drop-oldest. The DM lane is the only unconditionally-delivered lane (addressed — there are no wants to gate it on), so it's the only lane with a client-side
 * attach window to bridge; every room lane is want-gated at the server and has nothing to hold.
 */
const PENDING_INBOX_MAX_COUNT = 64
const DM_NO_INBOX_LISTENER: DmReply = { ok: false, err: 'No participant inbox listener is attached' }
abstract class ParticipantBase implements LocalParticipant {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  readonly id: string
  readonly identity: string | null
  readonly selfDelivery: boolean
  /** @internal */ _meta: ParticipantMeta
  protected _left = false
  private _leftCause: LeaveCause | null = null
  private _leaveCbs: Array<(cause: LeaveCause) => unknown> = []
  private readonly _messageCbs: Array<(data: unknown, from: Sender | null) => unknown> = []
  private readonly _demandCbs: Array<(track: string | null, wanted: boolean) => unknown> = []
  private readonly _wantedTracks = new Set<string | null>()
  private readonly _listenerCleanups = new Set<() => void>()
  private _inboxAttached = false
  /** DMs delivered before the first `listen()` — held bounded, flushed on attach, then never
   *  allocated again (`null` = flushed or empty; zero steady-state cost). An entry carries an
   *  `ackResolve` when the sender awaits a reply — resolved when the hold flushes (or on leave). */
  private _pendingInbox: Array<{ msg: InboxMessage; ackResolve?: (reply: DmReply) => void }> | null = null
  /** When a client holds this participant, its inbox forwards there instead of to local listeners; the forwarder returns the client's reply for an ack DM (see `bindParticipantStubChannel`). */
  private _forwarder: ((msg: InboxMessage) => Promise<DmReply> | void) | null = null
  /** @internal — route this participant's inbox to a remote holder instead of local listeners. */
  _setForwarder(forwarder: (msg: InboxMessage) => Promise<DmReply> | void): void {
    this._forwarder = forwarder
    this._inboxAttached = true
    const held = this._pendingInbox
    this._pendingInbox = null
    if (!held) return
    for (const { msg, ackResolve } of held) {
      const reply: DmReply | Promise<DmReply> = forwarder(msg) ?? { ok: true, result: undefined }
      if (ackResolve) void Promise.resolve(reply).then(ackResolve)
    }
  }
  /** @internal — already bound to a client holder (serialized once, via `bindParticipantStubChannel`)? */
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
  abstract publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck>
  abstract publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck>
  // Implementation signature for the overloaded `LocalParticipant.send` (receipt, or the recipient's reply with `{ ack: true }`); callers see the precise overloads through the interface. `any` is the
  // standard overload-implementation return.
  abstract send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any>
  abstract setMeta(meta: ParticipantMeta): Promise<void>
  abstract setAttributes(attributes: ParticipantMeta): Promise<void>
  abstract leave(): Promise<void>
  /** A user callback threw — each side reports through its own pipeline. */
  protected abstract _reportError(err: unknown): void
  listen(callback: (data: unknown, from: Sender | null) => unknown): () => void {
    const unlisten = this._register(this._messageCbs, callback)
    this._inboxAttached = true
    if (this._pendingInbox) {
      const held = this._pendingInbox
      this._pendingInbox = null
      for (const entry of held) {
        if (entry.ackResolve) void this._fireInboxAck(entry.msg).then(entry.ackResolve)
        else this._fireInbox(entry.msg)
      }
    }
    return unlisten
  }
  /** @internal — a direct message arrived on this member's inbox. Forwarded to a remote holder if
   *  one is bound (client-held), else delivered to local listeners — held bounded until the first
   *  `listen()` if none is registered yet (a reactive send can beat `listen()` by a tick). */
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
  /** @internal — deliver an `{ ack: true }` DM and resolve with the recipient's reply: the last listener's return, its thrown error, or — when a client holds this participant — the client's reply.
   * Held (like any DM) until the first `listen()` if none is registered; resolves with an error if the participant leaves first (see `_onLeft`). Never rejects.
   */
  _deliverMessageAck(msg: InboxMessage): Promise<DmReply> {
    if (this._forwarder) return Promise.resolve(this._forwarder(msg) ?? { ok: true, result: undefined })
    if (this._messageCbs.length === 0) {
      if (this._left) return Promise.resolve(DM_PARTICIPANT_LEFT)
      if (this._inboxAttached) return Promise.resolve(DM_NO_INBOX_LISTENER)
      return new Promise<DmReply>((resolve) => this._hold(msg, resolve))
    }
    return this._fireInboxAck(msg)
  }
  private _hold(msg: InboxMessage, ackResolve?: (reply: DmReply) => void): void {
    const pending = (this._pendingInbox ??= [])
    pending.push({ msg, ackResolve })
    if (pending.length > PENDING_INBOX_MAX_COUNT) {
      pending.shift()?.ackResolve?.({ ok: false, err: 'Inbox overflowed before the message was handled' })
    }
  }
  /** `from`/`fromMeta` come from the wire envelope; upgrades to the live `RemoteParticipant` when a room view exists. An empty `from` is the wire encoding of a room-authored message → `null`. */
  private _senderOf(msg: InboxMessage): Sender | null {
    const { from, fromMeta, fromIdentity: identity } = msg
    return from === ''
      ? null
      : (this._resolveSender(from) ?? Object.freeze({ id: from, meta: ownMetadata(fromMeta ?? {}), identity }))
  }
  private _fireInbox(msg: InboxMessage): void {
    const sender = this._senderOf(msg)
    for (const cb of [...this._messageCbs]) {
      this._invoke(cb, msg.data, sender)
    }
  }
  /** Run every listener (channel semantics: the last non-throwing return is the reply); a throw short-circuits to a failure reply. The handler is user code, so it obeys telefunc's contract: `throw
   * Abort(value)` sends the value to the sender, any other throw is a hidden bug (reported on this — the recipient's — side).
   */
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
  /** @internal — whether any node wants one of this member's tracks flipped (see the room's demand aggregation). `track` is `null` for the default `publishBinary()` lane. */
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
  /** @internal — the member is gone; `cause` says how. A local participant always knows its cause: its holder either initiated the leave or witnessed the event/closure that caused it. */
  _onLeft(cause: LeaveCause): void {
    this._left = true
    if (this._leftCause) return
    const ownedCause = (this._leftCause = ownLeaveCause(cause))
    // Held ack DMs will never be handled now — fail their senders instead of hanging them.
    const held = this._pendingInbox
    this._pendingInbox = null
    if (held) for (const entry of held) entry.ackResolve?.(DM_PARTICIPANT_LEFT)
    const cbs = [...this._leaveCbs]
    for (const unlisten of [...this._listenerCleanups]) unlisten()
    for (const cb of cbs) this._invoke(cb, ownedCause)
    this._wantedTracks.clear()
  }
  protected _assertActive(): void {
    if (this._left) throw new RoomError('Participant has left the room')
  }
  private _register<T>(list: T[], cb: T): () => void {
    list.push(cb)
    return makeDisposer(() => {
      void this // the active listener owns its participant; `makeDisposer` severs this edge
      const i = list.indexOf(cb)
      if (i >= 0) list.splice(i, 1)
    }, this._listenerCleanups)
  }
  private _invoke<Args extends unknown[]>(cb: (...args: Args) => unknown, ...args: Args): void {
    try {
      const result = cb(...args)
      if (isPromise(result)) void Promise.resolve(result).catch((err) => this._reportError(err))
    } catch (err) {
      this._reportError(err)
    }
  }
}
