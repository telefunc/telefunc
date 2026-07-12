export { RoomStubChannel, bindParticipantStubChannel }

import { stringify } from '@brillout/json-serializer/stringify'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { isObject } from '../../utils/isObject.js'
import type { ChannelPublishAck } from '../channel.js'
import type { ServerChannel } from '../server/channel.js'
import { ServerBroadcast } from '../server/server-broadcast.js'
import { ACK_STATUS, encodePublishText } from '../shared-ws.js'
import { reportRoomError, type ServerLocalParticipant, type ServerRoom } from './server.js'
import type { ParticipantMeta } from './types.js'
import {
  emptyTrackWants,
  errorMessage,
  hasRoomTag,
  roomCtrlKey,
  unframeMemberId,
  wantsTrack,
  type BinaryWants,
  type MemberSnapshot,
  type ParticipantStubRequest,
  type ReqOkAck,
  type ReqPublishAck,
  type ReqDmAck,
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
    if (wantsTrack(this._binaryWants.everyMember, track)) return true
    const memberWants = this._binaryWants.members[memberId]
    return memberWants !== undefined && wantsTrack(memberWants, track)
  }

  /** @internal */
  _wantsTextFrom(memberId: string): boolean {
    return this._wantsText || this._textMemberWants.has(memberId)
  }

  constructor(serverRoom: ServerRoom) {
    super({ key: roomCtrlKey(serverRoom.id) })
    this._room = serverRoom
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
    return this._trackAck(
      publishing.then(
        (ack) => this._sendAckRes(seq, stringify(ack)),
        (err: unknown) => this._sendAckRes(seq, errorMessage(err), ACK_STATUS.ERROR),
      ),
    )
  }

  // The standard broadcast-subscription ctrl is the text-lane gate: control events always flow
  // (a client's live view is only correct if it sees every one), text data flows only while the
  // client holds a subscription — presence-only holders never receive the room's chatter. The
  // binary flavor is ignored: binary wants ride the richer member-selective `sub-binary`.
  override _onPeerBroadcastSubscribe(binary: boolean): void {
    if (binary) return
    this._wantsText = true
    this._room._syncSubs()
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
  channel.listen(async (msg: unknown) => {
    if (!hasRoomTag(msg)) return undefined
    const req = msg as ParticipantStubRequest
    try {
      switch (req.__r) {
        case 'req-publish':
          return { ok: true, ack: await participant.publish(req.data) } satisfies ReqPublishAck
        case 'req-set-meta':
          await participant.setMeta(isObject(req.meta) ? req.meta : {})
          return { ok: true } satisfies ReqOkAck
        case 'req-set-attrs':
          await participant.setAttributes(isObject(req.attrs) ? req.attrs : {})
          return { ok: true } satisfies ReqOkAck
        case 'req-dm':
          return { ok: true, ack: await participant.send(req.to, req.data) } satisfies ReqDmAck
        case 'req-leave':
          await participant.leave()
          return { ok: true } satisfies ReqOkAck
        default:
          return undefined
      }
    } catch (err) {
      return { ok: false, err: errorMessage(err) } satisfies ReqOkAck
    }
  })

  channel.listenBinary(async (framed: Uint8Array) => {
    try {
      if (unframeMemberId(framed)?.from !== participant.id) throw new Error('Malformed room binary publish')
      return { ok: true, ack: await participant._publishFramed(framed) }
    } catch (err) {
      return { ok: false, err: errorMessage(err) } satisfies ReqOkAck
    }
  })

  // Keep the client-side `participant.meta` fresh. Serializing a participant that already left
  // is possible (leave raced the response) — then there's no remote view left to observe.
  const remote = participant._room._state.getRemote(participant.id)
  const unlistenMeta = remote?.onUpdate(
    (meta: ParticipantMeta) => void channel.send({ __r: 'p-meta', meta }).catch(() => {}),
  )

  // The participant's holder is the client — forward inbox deliveries to it.
  const unlistenDm = participant.listen((data, from) => {
    void channel
      .send({
        __r: 'dm',
        from: from?.id ?? '',
        fromMeta: from?.meta ?? null,
        ...(from?.identity == null ? {} : { fromIdentity: from.identity }),
        data,
      })
      .catch(() => {})
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
    unlistenDm()
    unlistenDemand()
    unlistenLeave()
    // The client is gone (page closed, GC, network death) — presence says the member leaves.
    void participant.leave().catch(reportRoomError)
  })
}
