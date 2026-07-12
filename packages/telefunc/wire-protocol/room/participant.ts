export { ParticipantBase }
export type { InboxMessage }

import type { ChannelPublishAck } from '../channel.js'
import type {
  BinaryPublishOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RoomSendReceipt,
  Sender,
} from './types.js'

// ---------------------------------------------------------------------------
// ParticipantBase — the shared half of every LocalParticipant
// ---------------------------------------------------------------------------

/**
 * The private-message inbox and the leave lifecycle, identical on server and client.
 * Flavors supply the transport through the abstract operations and their own error pipeline.
 */
/** A delivered private message, as stamped by the sender's node. */
type InboxMessage = {
  from: string
  fromMeta: ParticipantMeta | null
  fromIdentity: string | null
  data: unknown
}

/** Pre-listen inbox hold: count-capped, drop-oldest. The DM lane is the only
 *  unconditionally-delivered lane (addressed — there are no wants to gate it on), so it's the
 *  only lane with a client-side attach window to bridge; every room lane is want-gated at the
 *  server and has nothing to hold. Message size is bounded upstream by the wire-protocol
 *  `messageLimit`, which bounds the hold's memory too. */
const PENDING_INBOX_MAX_COUNT = 64

abstract class ParticipantBase implements LocalParticipant {
  readonly id: string
  readonly identity: string | null
  readonly selfDelivery: boolean
  /** @internal */ _meta: ParticipantMeta
  protected _left = false
  private _leftCause: LeaveCause | null = null
  private _leaveCbs: Array<(cause: LeaveCause) => void> = []
  private readonly _messageCbs: Array<(data: unknown, from: Sender | null) => void> = []
  private readonly _demandCbs: Array<(track: string | null, count: number) => void> = []
  /** DMs delivered before the first `listen()` — held bounded, flushed on attach, then never
   *  allocated again (`null` = flushed or empty; zero steady-state cost). */
  private _pendingInbox: InboxMessage[] | null = null

  constructor(id: string, meta: ParticipantMeta, selfDelivery: boolean, identity: string | null) {
    this.id = id
    this._meta = meta
    this.selfDelivery = selfDelivery
    this.identity = identity
  }

  get meta(): ParticipantMeta {
    return this._meta
  }

  abstract publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck>
  abstract publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck>
  abstract send(to: string | Sender, data: unknown): Promise<RoomSendReceipt>
  abstract setMeta(meta: ParticipantMeta): Promise<void>
  abstract setAttributes(attributes: ParticipantMeta): Promise<void>
  abstract leave(): Promise<void>
  /** A user callback threw — each side reports through its own pipeline. */
  protected abstract _reportError(err: unknown): void

  listen(callback: (data: unknown, from: Sender | null) => void): () => void {
    this._messageCbs.push(callback)
    if (this._pendingInbox) {
      const held = this._pendingInbox
      this._pendingInbox = null
      for (const msg of held) this._deliverMessage(msg)
    }
    return () => {
      const i = this._messageCbs.indexOf(callback)
      if (i >= 0) this._messageCbs.splice(i, 1)
    }
  }

  /** @internal — a direct message arrived on this member's inbox. `from`/`fromMeta` come from
   *  the wire envelope; `resolve` upgrades to the live `RemoteParticipant` when a room view
   *  exists. An empty `from` is the wire encoding of a room-authored message → `null`. */
  _deliverMessage(msg: InboxMessage): void {
    if (this._messageCbs.length === 0) {
      // A reactive send can beat the app's `listen()` by a tick — hold it (bounded) instead of
      // dropping it. A departed participant will never flush: drop.
      if (this._left) return
      const pending = (this._pendingInbox ??= [])
      pending.push(msg)
      if (pending.length > PENDING_INBOX_MAX_COUNT) pending.shift()
      return
    }
    const { from, fromMeta, fromIdentity, data } = msg
    const sender =
      from === '' ? null : (this._resolveSender(from) ?? { id: from, meta: fromMeta ?? {}, identity: fromIdentity })
    for (const cb of [...this._messageCbs]) {
      try {
        cb(data, sender)
      } catch (err) {
        this._reportError(err)
      }
    }
  }

  /** The live room-backed sender, when this flavor has a room view. */
  protected _resolveSender(_id: string): Sender | null {
    return null
  }

  onDemand(callback: (track: string | null, count: number) => void): () => void {
    this._demandCbs.push(callback)
    return () => {
      const i = this._demandCbs.indexOf(callback)
      if (i >= 0) this._demandCbs.splice(i, 1)
    }
  }

  /** @internal — the global demand for one of this member's tracks changed (see the room's
   *  demand aggregation). `track` is `null` for the default `publishBinary()` lane. */
  _onDemand(track: string | null, count: number): void {
    for (const cb of [...this._demandCbs]) {
      try {
        cb(track, count)
      } catch (err) {
        this._reportError(err)
      }
    }
  }

  onLeave(callback: (cause: LeaveCause) => void): () => void {
    if (this._leftCause) {
      this._invoke(callback, this._leftCause)
      return () => {}
    }
    this._leaveCbs.push(callback)
    return () => {
      const i = this._leaveCbs.indexOf(callback)
      if (i >= 0) this._leaveCbs.splice(i, 1)
    }
  }

  /** @internal — the member is gone; `cause` says how. A local participant always knows its
   *  cause: its holder either initiated the leave or witnessed the event/closure that caused it. */
  _onLeft(cause: LeaveCause): void {
    this._left = true
    if (this._leftCause) return
    this._leftCause = cause
    this._pendingInbox = null
    const cbs = this._leaveCbs
    this._leaveCbs = []
    for (const cb of cbs) this._invoke(cb, cause)
  }

  protected _assertActive(): void {
    if (this._left) throw new Error('Participant has left the room')
  }

  private _invoke(cb: (cause: LeaveCause) => void, cause: LeaveCause): void {
    try {
      cb(cause)
    } catch (err) {
      this._reportError(err)
    }
  }
}
