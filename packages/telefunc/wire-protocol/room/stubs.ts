export { RoomStubChannel, RoomParticipantStubChannel, bindParticipantStubChannel }

import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import { ROOM_DM_ACK_TIMEOUT_MS, ROOM_TAIL_ATTACH_TIMEOUT_MS } from '../constants.js'
import type { ChannelPublishAck } from '../channel.js'
import { ServerChannel } from '../server/channel.js'
import type { ShieldValidator } from '../../node/server/shield.js'
import { ServerBroadcast } from '../server/server-broadcast.js'
import { encodePublishText, type WirePublishInfo } from '../shared-ws.js'
import { type ServerLocalParticipant, type ServerRoom } from './server.js'
import { reportRoomError } from './server/lanes.js'
import type { ParticipantMeta, RoomSendReceipt } from './types.js'
import {
  binaryWantsCovers,
  pushBoundedTail,
  DM_PARTICIPANT_LEFT,
  emptyTrackWants,
  RoomError,
  roomAckError,
  roomFailureError,
  hasRoomTag,
  roomCtrlKey,
  unframeMemberId,
  type BinaryWants,
  type RoomOrder,
  type DmReply,
  type MemberSnapshot,
  type ParticipantStubRequest,
  type RoomDemandEvent,
  type RoomRosterEvent,
} from './protocol.js'
assertIsNotBrowser()

// The wire adapters of the room domain: how a `Room` and a `LocalParticipant` cross a
// response. Room-wide authority (membership, admission, validation, ordering) stays in
// `ServerRoom`; each stub owns one holder's view of that room — the client's declared wants
// and the per-holder buffering, watermarks and correlations that deliver the room to that
// one peer.

/**
 * The channel registered with a response when a `Room` crosses the wire.
 * - server→client: room events & data relayed as PUBLISH frames (pre-peer buffered,
 *   replayed on reconnect). Control always flows; text is gated by the client's broadcast
 *   subscription (all) or its member-selective `sub-text` set; binary is member-selective
 *   (`sub-binary`).
 * - client→server: join/leave/set-meta as ack-bearing channel messages; publishes as
 *   PUBLISH(_BINARY)_ACK_REQ frames, validated against the members joined through this stub.
 */
class RoomStubChannel extends ServerBroadcast {
  private readonly _room: ServerRoom
  /** @internal — members the remote client joined through this stub (membership & lifecycle). */
  readonly _stubMembers = new Set<string>()
  /** Live ack-DM correlations, stored in their constant-offset deadline order. */
  private readonly _pendingAckDms = new Map<string, { sender: string; recipient: string; expiresAt: number }>()

  /** @internal — record a relayed ack DM and sweep correlations whose sender already timed out. */
  _recordAckDm(ackId: string, sender: string, recipient: string): void {
    const now = Date.now()
    for (const [id, entry] of this._pendingAckDms) {
      if (entry.expiresAt > now) break // constant offset ⇒ insertion order is deadline order; the rest are younger
      this._pendingAckDms.delete(id)
    }
    this._pendingAckDms.set(ackId, { sender, recipient, expiresAt: now + ROOM_DM_ACK_TIMEOUT_MS })
  }

  /** @internal — consume a live correlation only for the recipient it was relayed to. */
  _takeAckDm(ackId: string, replier: unknown): string | undefined {
    const entry = this._pendingAckDms.get(ackId)
    if (!entry) return undefined
    if (entry.expiresAt <= Date.now()) {
      this._pendingAckDms.delete(ackId)
      return undefined
    }
    if (entry.recipient !== replier) return undefined
    this._pendingAckDms.delete(ackId)
    return entry.sender
  }
  /** @internal — members whose own echo this client's room view must not receive (`selfDelivery`
   *  off). The one relay-gate input, fed by both ways a member arises: a client `req-join` adds
   *  here directly, and a co-returned server-side participant is bound here at serialization time
   *  (`roomReplacer` adopts the pass's drop-set by reference, so `{room, me}` and `{me, room}`
   *  converge on the same set). A suppressed frame is never written to this client's wire. */
  _selfSuppressed = new Set<string>()
  /** @internal — adopt the serialization pass's drop-set for this room (see `roomReplacer`). */
  _adoptSelfSuppressed(set: Set<string>): void {
    this._selfSuppressed = set
  }
  /** @internal — the publish shield auto-generated from the room's declared message type (`Pub`),
   *  installed by `roomReplacer` at serialization time and consulted at the publish ingress
   *  (`ServerRoom._publishFromStub` → `_shieldPublishData`). Undefined when `Pub` is `unknown`. Kept off
   *  `_validators` on purpose: that map drives the base channel's request-envelope validation, which a
   *  room stub must not shield (it multiplexes join/leave/dm through the same `_dispatchAckReq` path). */
  _publishShield?: ShieldValidator
  /** @internal — the client's declared binary wants, per member and track (`sub-binary`). */
  _binaryWants: BinaryWants = { everyMember: emptyTrackWants(), members: {} }
  /** @internal — whether the client subscribes to the whole text lane (the broadcast-sub ctrl). */
  _wantsText = false
  /** @internal — which members' text the client wants without a room-level subscription (`sub-text`). */
  _textMemberWants: Set<string> = new Set()

  /** @internal — tail mode (`Room.get(id, { tail: true })`): the room's recent text, held on this stub
   *  (bounded, drop-oldest) from the moment it attaches until the client's first text want declares the
   *  selector, then flushed once, in order, ahead of the live stream (see `_flushTail`). `null` outside
   *  tail mode or once flushed. Held server-side, so the client buffers nothing and the flush is
   *  member-selective rather than a fire-hose of everything. */
  _tailPending: Array<{ serialized: string; ord: RoomOrder; from: string }> | null = null
  private _tailTimer: ReturnType<typeof setTimeout> | null = null

  /** @internal — retained replay is exactly-once and in-order against a racing live publish. Text's
   *  watermark is room-global, matching the semantic lane; `_textPendingRetained` is a retained order
   *  just emitted that is still awaiting its own live echo. A retained frame is
   *  skipped when the watermark already covers it (a same-or-newer live frame won the race); the live
   *  echo of an emitted retained is dropped (the retained won). Binary is the twin, per (member,
   *  track) lane — its own order domain (the per-key transport seq). Pruned on leave (`_forgetMember`). */
  private _textHigh: RoomOrder | null = null
  private _textPendingRetained: RoomOrder | null = null
  // Binary lanes keyed `${member}\0${track}` — one flat map, so a member leaving prunes by prefix.
  private readonly _binaryHigh = new Map<string, WirePublishInfo>()
  private readonly _binaryPendingRetained = new Map<string, WirePublishInfo>()

  /** @internal — relay gate: does this client want the (member, track) the frame belongs to? */
  _wantsBinary(memberId: string, track: string): boolean {
    return binaryWantsCovers(this._binaryWants, memberId, track)
  }

  /** @internal */
  _wantsTextFrom(memberId: string): boolean {
    return this._wantsText || this._textMemberWants.has(memberId)
  }

  constructor(serverRoom: ServerRoom) {
    super({ key: roomCtrlKey(serverRoom.id) })
    this._room = serverRoom
    // Stub requests (join/leave/set-meta/dm/sub-binary/sub-text) arrive as channel messages.
  }

  override _onPeerMessage(text: string, bytes: number): void {
    const started = performance.now()
    try {
      this._flow.onReceived(bytes)
      Promise.resolve(this._room._handleStubRequest(this, parse(text)))
        .catch((error: unknown) => this._handleCallbackError(error))
        .finally(() => this._flow.onConsumed(bytes))
    } finally {
      this._flow._recordSelfTime(performance.now() - started)
    }
  }

  override _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(
      (async () => {
        try {
          const result = await this._room._handleStubRequest(this, parse(text))
          this._sendAckRes(seq, stringify(result))
        } catch (error) {
          const failure = roomAckError(error, reportRoomError)
          this._sendAckRes(seq, failure.text, failure.status)
        }
      })(),
    )
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._ackPublish(this._room._publishFromStub(this, { text }), seq)
  }

  override _onPeerPublishBinaryAckReqMessage(binary: Uint8Array, seq: number): Promise<void> {
    return this._ackPublish(this._room._publishFromStub(this, { binary }), seq)
  }

  private _ackPublish(publishing: Promise<ChannelPublishAck>, seq: number): Promise<void> {
    // A publish rides its own ack frame (not the request dispatch), so it maps the room error onto
    // the channel's native status here — the same `roomAckError` classification every stub request uses.
    return this._trackAck(
      publishing.then(
        (ack) => this._sendAckRes(seq, stringify(ack)),
        (err: unknown) => {
          const { text, status } = roomAckError(err, reportRoomError)
          this._sendAckRes(seq, text, status)
        },
      ),
    )
  }

  // The standard broadcast-subscription ctrl is the text-lane gate: control events always flow
  // (a client's live view is only correct if it sees every one), text data flows only while the
  // client holds a subscription — presence-only holders never receive the room's chatter. The
  // binary flavor is ignored: binary wants ride the richer member-selective `sub-binary`.
  override _onPeerBroadcastSubscribe(binary: boolean): void {
    if (binary) return
    const prevWantsText = this._wantsText
    this._wantsText = true
    // Tail mode: this room-level want covers the whole held tail — flush it before the retained
    // back-fill, so the flush advances the causal watermark and the retained replay dedupes against it.
    this._flushTail()
    this._room._syncSubs()
    // MQTT-retained delivery: back-fill the last `publish(…, { retain: true })` now that this
    // client wants the text lane (see `_replayRetainedText` for the newly-covered semantics).
    void this._room._replayRetainedText(this, prevWantsText, this._textMemberWants).catch(reportRoomError)
  }
  override _onPeerBroadcastUnsubscribe(binary: boolean): void {
    if (binary) return
    this._wantsText = false
    this._room._syncSubs()
  }

  /** @internal — push the authoritative roster to this client (see `RoomRosterEvent`). */
  _relayRoster(members: MemberSnapshot[]): void {
    const event: RoomRosterEvent = { __r: 'roster', members }
    this._relayPublishText(encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  /** @internal — explicitly settle a client's failed initial roster read. */
  _relayRosterError(): void {
    const event: RoomRosterEvent = { __r: 'roster-error' }
    this._relayPublishText(encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  /** @internal — push a demand update for one of this client's members (see `onDemand`). */
  _relayDemand(event: RoomDemandEvent): void {
    this._relayPublishText(encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  /** @internal */
  _relayPublishText(wireText: string): void {
    if (this._peer) this._peer.sendPublish(wireText)
    else this._prePeerBuffer.pushPublish(wireText)
  }

  /** @internal */
  _relayPublishBinary(wireData: Uint8Array): void {
    if (this._peer) this._peer.sendPublishBinary(wireData)
    else this._prePeerBuffer.pushPublishBinary(wireData)
  }

  /** @internal — relay a live text frame, advancing the sender's watermark and dropping the live echo
   *  of a retained frame this stub was just handed, so a subscribe-then-publish race delivers the
   *  message exactly once. */
  _relayTextLive(wireText: string, _from: string, ord: RoomOrder): void {
    const pending = this._textPendingRetained
    if (pending && pending.seq === ord.seq && pending.timestamp === ord.timestamp) {
      this._textPendingRetained = null // this live frame is the echo of the retained we emitted
      return
    }
    this._relayPublishText(wireText)
    if (!this._textHigh || this._textHigh.seq < ord.seq) this._textHigh = ord
  }

  /** @internal — replay a retained text frame unless the sender's watermark already covers it (a
   *  same-or-newer live frame reached this stub first), then record it so its own live echo is
   *  dropped. The MQTT-retained backfill, made causal. */
  _emitRetainedText(wireText: string, _from: string, ord: RoomOrder): void {
    const high = this._textHigh
    if (high && high.seq >= ord.seq) return // superseded by a same-or-newer live frame
    this._relayPublishText(wireText)
    this._textHigh = ord
    this._textPendingRetained = ord
  }

  /** @internal — the binary twin of `_relayTextLive`, per (member, track) lane; order is the per-key
   *  transport seq (binary's own domain). */
  _relayBinaryLive(wireData: Uint8Array, from: string, track: string, info: WirePublishInfo): void {
    const lane = `${from}\0${track}`
    if (this._binaryPendingRetained.get(lane)?.seq === info.seq) {
      this._binaryPendingRetained.delete(lane) // this live frame is the echo of the retained we emitted
      return
    }
    this._relayPublishBinary(wireData)
    const seen = this._binaryHigh.get(lane)
    if (!seen || seen.seq < info.seq) this._binaryHigh.set(lane, info)
  }

  /** @internal — the binary twin of `_emitRetainedText`, per (member, track) lane. */
  _emitRetainedBinary(wireData: Uint8Array, from: string, track: string, info: WirePublishInfo): void {
    const lane = `${from}\0${track}`
    if ((this._binaryHigh.get(lane)?.seq ?? -1) >= info.seq) return // superseded by a same-or-newer live frame
    this._relayPublishBinary(wireData)
    this._binaryHigh.set(lane, info)
    this._binaryPendingRetained.set(lane, info)
  }

  /** @internal — a member left: drop its retained-replay bookkeeping so the maps stay bounded by the
   *  live roster, not the room's lifetime churn. */
  _forgetMember(from: string): void {
    const prefix = `${from}\0`
    for (const lane of [this._binaryHigh, this._binaryPendingRetained])
      for (const key of lane.keys()) if (key.startsWith(prefix)) lane.delete(key)
  }

  /** @internal — begin holding the tail on this stub, seeded from the room's pre-attach hold. A
   *  safety timer drops the hold and releases the room's text ingestion (via `onExpire`) if the
   *  client never subscribes — the hold stays bounded in size, this bounds its lifetime too. */
  _beginTail(seed: Array<{ serialized: string; ord: RoomOrder; from: string }>, onExpire: () => void): void {
    this._tailPending = seed
    this._tailTimer = setTimeout(() => {
      this._tailPending = null
      this._tailTimer = null
      onExpire()
    }, ROOM_TAIL_ATTACH_TIMEOUT_MS)
  }

  /** @internal — append a live message to the pending tail, bounded drop-oldest (the freshest tail is
   *  what a late subscriber wants). Called only while `_tailPending` is non-null. */
  _holdTail(serialized: string, ord: RoomOrder, from: string): void {
    const hold = this._tailPending
    if (!hold) return
    pushBoundedTail(hold, { serialized, ord, from })
  }

  /** @internal — the client's first text want declares the selector: flush the held tail it now
   *  covers, in order, through the live relay — so it advances the causal watermark and a retained
   *  replay racing the same subscribe dedupes against it (see `_relayTextLive`/`_emitRetainedText`).
   *  One-shot; a no-op if no tail is pending or the want still selects nothing (an empty `sub-text`). */
  _flushTail(): void {
    const hold = this._tailPending
    if (!hold) return
    if (!this._wantsText && this._textMemberWants.size === 0) return // keep holding until a real want
    this._endTail()
    for (const { serialized, ord, from } of hold) {
      if (!this._wantsTextFrom(from)) continue
      if (this._selfSuppressed.has(from)) continue
      this._relayTextLive(encodePublishText(serialized, ord), from, ord)
    }
  }

  /** @internal — drop the pending tail and its safety timer without flushing (the stub closed, or the
   *  timer fired because the client never subscribed). */
  _endTail(): void {
    this._tailPending = null
    if (this._tailTimer !== null) {
      clearTimeout(this._tailTimer)
      this._tailTimer = null
    }
  }
}

/**
 * Wire a fresh channel to a `ServerLocalParticipant` for serialization (see
 * `roomParticipantReplacer`). The client sends publish/set-meta/leave requests;
 * the server pushes metadata updates and the leave notice.
 */
class RoomParticipantStubChannel extends ServerChannel<unknown, unknown> {
  private _requestHandler: ((message: unknown) => Promise<unknown>) | null = null

  _listenRoomRequests(handler: (message: unknown) => Promise<unknown>): void {
    this._requestHandler = handler
  }

  override _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(
      (async () => {
        try {
          const result = await this._requestHandler?.(parse(text))
          this._sendAckRes(seq, stringify(result))
        } catch (error) {
          const failure = roomAckError(error, reportRoomError)
          this._sendAckRes(seq, failure.text, failure.status)
        }
      })(),
    )
  }
}

function bindParticipantStubChannel(
  channel: RoomParticipantStubChannel,
  participant: ServerLocalParticipant,
  publishShield?: ShieldValidator,
): void {
  // A LocalParticipant is a single member's live seat: it forwards its inbox to exactly one holder
  // (`_setForwarder`) and its holder's close ends the membership (`onClose → leave`). Binding a second
  // client would silently overwrite the first's forwarder and let either close drop the shared seat.
  // A member belongs to one holder: return a fresh `join()` per client, or a read-only view.
  assertUsage(
    !participant._isBound,
    'This LocalParticipant is already bound to a client and cannot be handed to another. A LocalParticipant is a single member: give each client its own join(), or share a getParticipants() view (which is read-only) instead.',
  )
  channel._listenRoomRequests(async (msg: unknown) => {
    if (!hasRoomTag(msg)) return undefined
    const req = msg as ParticipantStubRequest
    switch (req.__r) {
      case 'req-publish':
        participant._room._shieldPublishData(publishShield, req.data) // reject malformed data before publish
        return await participant.publish(req.data, req.retain ? { retain: true } : undefined)
      case 'req-set-meta':
        await participant.setMeta(isObject(req.meta) ? req.meta : {})
        return undefined
      case 'req-set-attrs':
        await participant.setAttributes(isObject(req.attrs) ? req.attrs : {})
        return undefined
      case 'req-dm': {
        if (!req.ack) return (await participant.send(req.to, req.data)) as RoomSendReceipt
        const { receipt, reply } = await participant._room._sendDmAck(participant.id, req.to, req.data)
        if (!reply.ok) throw roomFailureError(reply)
        return { ...receipt, response: reply.result }
      }
      case 'req-leave':
        await participant.leave()
        return undefined
      default:
        return undefined
    }
  })

  channel.listenBinary(async (framed: Uint8Array) => {
    if (unframeMemberId(framed)?.from !== participant.id) throw new RoomError('Malformed room binary publish')
    return await participant._publishFramed(framed)
  })

  // Keep the client-side `participant.meta` fresh. Serializing a participant that already left
  // is possible (leave raced the response) — then there's no remote view left to observe.
  const remote = participant._room._state.getRemote(participant.id)
  const unlistenMeta = remote?.onUpdate(
    (meta: ParticipantMeta) => void channel.send({ __r: 'p-meta', meta }).catch(() => {}),
  )

  // The participant's holder is the client — forward inbox deliveries to it. An ack DM rides the
  // channel's own ack: the client's `listen` reply comes back as the channel ack, which becomes
  // the DM reply routed to the sender (see server `_onDm`).
  participant._setForwarder((msg) => {
    const notice = {
      __r: 'dm' as const,
      from: msg.from,
      fromMeta: msg.fromMeta,
      ...(msg.fromIdentity == null ? {} : { fromIdentity: msg.fromIdentity }),
      data: msg.data,
      ...(msg.ackId ? { ackId: msg.ackId } : {}),
    }
    if (!msg.ackId) {
      void channel.send(notice).catch(() => {})
      return
    }
    return channel.send(notice, { ack: true }).then(
      (reply) => reply as DmReply,
      // The channel ack only rejects on transport failure (a handler throw comes back encoded in
      // the resolved `DmReply`), so a rejection means the holder's stub died — it has left.
      () => DM_PARTICIPANT_LEFT,
    )
  })

  // Demand updates for this member's own tracks (onDemand) — forwarded to the client holder.
  const unlistenDemand = participant.onDemand((track, wanted) => {
    void channel.send({ __r: 'demand', track, wanted }).catch(() => {})
  })

  const unlistenLeave = participant.onLeave((cause) => {
    const notice =
      cause.type === 'left'
        ? { __r: 'left' as const }
        : { __r: 'left' as const, cause: cause.type, ...(cause.reason === undefined ? {} : { reason: cause.reason }) }
    void channel.send(notice).catch(() => {})
    void channel.close().catch(() => {})
  })

  channel.onClose(() => {
    unlistenMeta?.()
    unlistenDemand()
    unlistenLeave()
    // The client is gone (page closed, GC, network death) — presence says the member leaves.
    void participant.leave().catch(reportRoomError)
  })
}
