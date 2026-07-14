export { RoomStubChannel, bindParticipantStubChannel }

import { stringify } from '@brillout/json-serializer/stringify'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { isObject } from '../../utils/isObject.js'
import type { ChannelPublishAck } from '../channel.js'
import type { ServerChannel } from '../server/channel.js'
import { ServerBroadcast } from '../server/server-broadcast.js'
import { encodePublishText } from '../shared-ws.js'
import { reportRoomError, type ServerLocalParticipant, type ServerRoom } from './server.js'
import type { ParticipantMeta, RoomSendReceipt } from './types.js'
import {
  binaryWantsCovers,
  emptyTrackWants,
  RoomError,
  roomAckError,
  roomFailureError,
  hasRoomTag,
  roomCtrlKey,
  unframeMemberId,
  type BinaryWants,
  type DmReply,
  type MemberSnapshot,
  type ParticipantStubRequest,
  type RoomDemandEvent,
  type RoomRosterEvent,
} from './protocol.js'
assertIsNotBrowser()

// The wire adapters of the room domain: how a `Room` and a `LocalParticipant` cross a
// response. All room semantics (membership, validation, relay decisions) stay in
// `ServerRoom` — these classes only move frames between the peer and the room.

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
  /** @internal — ack DMs relayed to this client awaiting its `dm-reply`: `ackId → sender`. Only an
   *  `ackId` recorded here is honored, so a forged reply routes nowhere. Dropped with the stub. */
  readonly _pendingAckDms = new Map<string, string>()
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
  /** @internal — the client's declared binary wants, per member and track (`sub-binary`). */
  _binaryWants: BinaryWants = { everyMember: emptyTrackWants(), members: {} }
  /** @internal — whether the client subscribes to the whole text lane (the broadcast-sub ctrl). */
  _wantsText = false
  /** @internal — which members' text the client wants without a room-level subscription (`sub-text`). */
  _textMemberWants: Set<string> = new Set()

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
    // A throwing stub request maps onto the channel's native ack status (see `roomAckError`), so a
    // rejected join/dm/etc. rejects the client's request without an envelope or tearing the channel.
    this.setAckErrorEncoder((err) => roomAckError(err, reportRoomError))
    // Stub requests (join/leave/set-meta/dm/sub-binary/sub-text) arrive as channel messages.
    this._listen((msg: unknown) => this._room._handleStubRequest(this, msg))
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._ackPublish(this._room._publishFromStub(this, { text }), seq)
  }

  override _onPeerPublishBinaryAckReqMessage(binary: Uint8Array, seq: number): Promise<void> {
    return this._ackPublish(this._room._publishFromStub(this, { binary }), seq)
  }

  private _ackPublish(publishing: Promise<ChannelPublishAck>, seq: number): Promise<void> {
    // A publish rides its own ack frame (not the request dispatch), so it maps the room error onto
    // the channel's native status here — the same `roomAckError` classification every stub request
    // gets through `setAckErrorEncoder`.
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
}

/**
 * Wire a fresh channel to a `ServerLocalParticipant` for serialization (see
 * `roomParticipantReplacer`). The client sends publish/set-meta/leave requests;
 * the server pushes metadata updates and the leave notice.
 */
function bindParticipantStubChannel(
  channel: ServerChannel<unknown, unknown>,
  participant: ServerLocalParticipant,
): void {
  // A throwing request maps onto the channel's native ack status (see `roomAckError`) — identical to
  // the shared room stub. Every request here is ack-bearing, so a throw rejects the client's request.
  channel.setAckErrorEncoder((err) => roomAckError(err, reportRoomError))
  channel.listen(async (msg: unknown) => {
    if (!hasRoomTag(msg)) return undefined
    const req = msg as ParticipantStubRequest
    switch (req.__r) {
      case 'req-publish':
        return await participant.publish(req.data, req.retain ? { retain: true } : undefined)
      case 'req-set-meta':
        await participant.setMeta(isObject(req.meta) ? req.meta : {})
        return undefined
      case 'req-set-attrs':
        await participant.setAttributes(isObject(req.attrs) ? req.attrs : {})
        return undefined
      case 'req-dm': {
        if (!req.ack) return (await participant.send(req.to, req.data)) as RoomSendReceipt
        // The recipient's failure rides home too — throw it so the ack encoder emits the native
        // ABORT/ERROR (a re-catch would reclassify a carried Abort/RoomError as an opaque bug).
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
      () => ({ ok: false, err: 'Participant left the room' }) as DmReply,
    )
  })

  // Demand updates for this member's own tracks (onDemand) — forwarded to the client holder.
  const unlistenDemand = participant.onDemand((track, count) => {
    void channel.send({ __r: 'demand', track, count }).catch(() => {})
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
