export { RoomStubChannel, RoomParticipantStubChannel }
export type { ResponseRoomGrants }

import { stringify } from '@brillout/json-serializer/stringify'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { ROOM_DM_ACK_TIMEOUT_MS, ROOM_TAIL_ATTACH_TIMEOUT_MS } from '../constants.js'
import { ServerChannel, parsePeerText } from '../../server/channel.js'
import type { ShieldValidator } from '../../../node/server/shield.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../../shared-ws.js'
import { type ServerLocalParticipant, type ServerRoom } from './room.js'
import { reportRoomError, roomAckError } from './errors.js'
import {
  decodeParticipantFrame,
  decodeParticipantRequest,
  decodeRoomDeclaration,
  decodeRoomPublish,
  decodeRoomRequest,
  decodeStubBinaryFrame,
} from './requests.js'
import { ReplayGate, TEXT_LANE_KEY, binaryLaneKey, type LaneHolder } from './replay.js'
import type { ParticipantMeta, RoomSendReceipt } from '../types.js'
import { DEFAULT_TRACK, binaryWantsCovers, emptyTrackWants, type BinaryFrame, type BinaryWants } from '../binary.js'
import { DM_PARTICIPANT_LEFT, roomFailureError } from '../errors.js'
import { leaveCauseToWire } from '../model.js'
import {
  pushBoundedTail,
  decodeDmReply,
  type RoomOrder,
  type ParticipantStubRequest,
  type RoomCtrlEnvelope,
  type RoomDataEnvelope,
  type RoomDemandEvent,
  type RoomRosterEvent,
} from '../protocol.js'
assertIsNotBrowser()

// Room authority stays server-side; each wire stub owns one holder's wants, buffering, watermarks, and correlations.

/** What one response's Room values grant the client on a room: echo drops for its own members, and hidden members it returned. */
type ResponseRoomGrants = { selfSuppressed: Set<string>; hidden: Set<string> }

/** A Room stub answers each client request through its channel ack, under the Room error contract. */
abstract class RoomRequestChannel extends ServerChannel {
  protected _ackRoomResult(seq: number, work: Promise<unknown>): Promise<void> {
    return this._trackAck(
      work.then(
        (result) => this._sendAckRes(seq, stringify(result)),
        (error: unknown) => {
          const { text, status } = roomAckError(error, reportRoomError)
          this._sendAckRes(seq, text, status)
        },
      ),
    )
  }
}

/** Server→client control/data obey wants; client→server membership/control and validated publishes use native channel acks. */
class RoomStubChannel extends RoomRequestChannel implements LaneHolder {
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

  /** @internal — consume a correlation only for the recipient it was relayed to; the sender drops a reply after its timeout. */
  _takeAckDm(ackId: string, replier: unknown): string | undefined {
    const entry = this._pendingAckDms.get(ackId)
    if (!entry || entry.recipient !== replier) return undefined
    this._pendingAckDms.delete(ackId)
    return entry.sender
  }
  /** One self-delivery gate combines direct client joins and co-returned server joins before wire emission. */
  readonly _selfSuppressed: Set<string>
  /** Hidden members this response handed the client: their events are relayed to it alone. */
  readonly _grantedHidden: Set<string>
  /** The generated publish shield validates Room data ingress only; base validators own multiplexed request envelopes. */
  readonly _publishShield: ShieldValidator | undefined
  /** @internal — the client's declared binary wants, per member and track (`sub-binary`). */
  _binaryWants: BinaryWants = { everyMember: emptyTrackWants(), members: {} }
  /** @internal — whether the client subscribes to the whole text lane (the broadcast-sub ctrl). */
  _wantsText = false
  /** @internal — which members' text the client wants without a room-level subscription (`sub-text`). */
  _textMemberWants: Set<string> = new Set()
  /** @internal — whether the client wants room-authored messages on the shared semantic lane. */
  _wantsAnnounce = false

  /** A bounded server-side tail waits for the first text selector, then flushes once in order. */
  _tailPending: Array<{ serialized: string; ord: RoomOrder; from: string }> | null = null
  private _tailTimer: ReturnType<typeof setTimeout> | null = null

  private readonly _replay = new ReplayGate()

  /** @internal — relay gate: does this client want the (member, track) the frame belongs to? */
  _wantsBinary(memberId: string, track: string): boolean {
    return !this._selfSuppressed.has(memberId) && binaryWantsCovers(this._binaryWants, memberId, track)
  }

  _wantsTextFrom(memberId: string): boolean {
    return !this._selfSuppressed.has(memberId) && (this._wantsText || this._textMemberWants.has(memberId))
  }

  constructor(
    serverRoom: ServerRoom,
    { publishShield, grants }: { publishShield?: ShieldValidator; grants: ResponseRoomGrants },
  ) {
    super()
    this._room = serverRoom
    this._publishShield = publishShield
    this._selfSuppressed = grants.selfSuppressed
    this._grantedHidden = grants.hidden
  }

  override _onPeerMessage(text: string, bytes: number): void {
    const started = performance.now()
    try {
      this._flow.onReceived(bytes)
      const declaration = decodeRoomDeclaration(parsePeerText(text))
      try {
        this._room._applyStubDeclaration(this, declaration)
      } catch (error) {
        this._handleCallbackError(error)
      }
      this._flow.onConsumed(bytes)
    } finally {
      this._flow._recordSelfTime(performance.now() - started)
    }
  }

  override _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    const request = decodeRoomRequest(parsePeerText(text))
    return this._ackRoomResult(seq, this._room._handleStubRequest(this, request))
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    const publish = decodeRoomPublish(parsePeerText(text))
    return this._ackRoomResult(seq, this._room._publishTextFromStub(this, publish))
  }

  override _onPeerPublishBinaryAckReqMessage(framed: Uint8Array, seq: number): Promise<void> {
    const { from } = decodeStubBinaryFrame(framed)
    return this._ackRoomResult(seq, this._room._publishBinaryFromStub(this, from, framed))
  }

  // Control always flows; text follows broadcast/member wants, while binary uses `sub-binary`.
  override _onPeerBroadcastSubscribe(binary: boolean): void {
    if (binary || this._wantsText) return
    const prevMembers = this._textMemberWants
    this._wantsText = true
    // Tail mode: this room-level want covers the whole held tail — flush it before the retained back-fill, so the flush advances the causal watermark and the retained replay dedupes against it.
    this._flushTail()
    this._room._syncSubs()
    void this._room._replayRetainedText(this, (member) => prevMembers.has(member)).catch(reportRoomError)
  }
  override _onPeerBroadcastUnsubscribe(binary: boolean): void {
    if (binary || !this._wantsText) return
    this._wantsText = false
    this._room._syncSubs()
  }

  /** An event this instance originates for this client alone, outside any lane's order. */
  _relayEvent(event: RoomRosterEvent | RoomDemandEvent | Extract<RoomCtrlEnvelope, { __r: 'closed' }>): void {
    this._sendPublish(encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  _relayTextLive(wireText: string, ord: RoomOrder): void {
    if (this._replay.admitLive(TEXT_LANE_KEY, ord.seq)) this._sendPublish(wireText)
  }

  _emitRetainedText(serialized: string, _event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._replay.admitRetained(TEXT_LANE_KEY, info.seq)) this._sendPublish(encodePublishText(serialized, info))
  }

  _relayBinaryLive(wireData: Uint8Array, from: string, track: string, info: WirePublishInfo): void {
    if (this._replay.admitLive(binaryLaneKey(from, track), info.seq)) this._sendPublishBinary(wireData)
  }

  _emitRetainedBinary(framed: Uint8Array, frame: BinaryFrame, info: WirePublishInfo): void {
    if (this._replay.admitRetained(binaryLaneKey(frame.from, frame.track ?? DEFAULT_TRACK), info.seq))
      this._sendPublishBinary(encodePublishBinary(framed, info))
  }

  _forgetMember(from: string): void {
    this._replay.forgetMember(from)
  }

  /** @internal — begin holding the bounded tail, seeded from the room's pre-attach hold. The client gets a fresh lease after attach; expiry drops the hold and lets the room release ingestion. */
  _beginTail(seed: Array<{ serialized: string; ord: RoomOrder; from: string }>, onExpire: () => void): void {
    this._tailPending = seed
    this._tailTimer = unrefTimer(
      setTimeout(() => {
        this._tailPending = null
        this._tailTimer = null
        onExpire()
      }, ROOM_TAIL_ATTACH_TIMEOUT_MS),
    )
  }

  /** @internal — append a live message to the pending tail, bounded drop-oldest (the freshest tail is what a late subscriber wants). Called only while `_tailPending` is non-null. */
  _holdTail(serialized: string, ord: RoomOrder, from: string): void {
    const hold = this._tailPending
    assert(hold)
    pushBoundedTail(hold, { serialized, ord, from })
  }

  /** The first real text want flushes the bounded tail in order through retained/live dedup. */
  _flushTail(): void {
    const hold = this._tailPending
    if (!hold) return
    if (!this._wantsText && this._textMemberWants.size === 0) return // keep holding until a real want
    this._endTail()
    for (const { serialized, ord, from } of hold) {
      if (!this._wantsTextFrom(from)) continue
      this._relayTextLive(encodePublishText(serialized, ord), ord)
    }
  }

  _endTail(): void {
    this._tailPending = null
    if (this._tailTimer !== null) {
      clearTimeout(this._tailTimer)
      this._tailTimer = null
    }
  }
}

async function sendParticipantDm(
  participant: ServerLocalParticipant,
  req: Extract<ParticipantStubRequest, { __r: 'req-dm' }>,
) {
  if (!req.ack) return (await participant.send(req.to, req.data)) as RoomSendReceipt
  const { receipt, reply } = await participant._room._sendDmAck(participant.id, req.to, req.data)
  if (!reply.ok) throw roomFailureError(reply)
  return { ...receipt, response: reply.result }
}

/** One client's hold on a server participant: its requests act as that participant, whose inbox, demand, meta and leave flow back to the client. */
class RoomParticipantStubChannel extends RoomRequestChannel {
  private readonly _participant: ServerLocalParticipant
  private readonly _publishShield: ShieldValidator | undefined

  constructor(participant: ServerLocalParticipant, publishShield?: ShieldValidator) {
    super()
    // A LocalParticipant has one holder; rebinding would overwrite its inbox forwarder and let either close drop it.
    assertUsage(
      !participant._isBound,
      'This LocalParticipant is already bound to a client and cannot be handed to another. A LocalParticipant is a single member: give each client its own join(), or share a getParticipants() view (which is read-only) instead.',
    )
    this._participant = participant
    this._publishShield = publishShield
    this._mirrorParticipant()
  }

  override _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    const request = decodeParticipantRequest(parsePeerText(text))
    return this._ackRoomResult(seq, this._handleRequest(request))
  }

  override _onPeerBinaryAckReqMessage(framed: Uint8Array, seq: number): Promise<void> {
    decodeParticipantFrame(framed, this._participant.id)
    return this._ackRoomResult(seq, this._publishBinary(framed))
  }

  private async _handleRequest(req: ParticipantStubRequest): Promise<unknown> {
    const participant = this._participant
    switch (req.__r) {
      case 'req-publish':
        participant._room._shieldPublishData(this._publishShield, req.data)
        return await participant.publish(req.data, req.retain ? { retain: true } : undefined)
      case 'req-set-meta':
        return await participant.setMeta(req.meta)
      case 'req-set-attrs':
        return await participant.setAttributes(req.attrs)
      case 'req-dm':
        return await sendParticipantDm(participant, req)
      case 'req-leave':
        return await participant.leave()
    }
  }

  private async _publishBinary(framed: Uint8Array): Promise<unknown> {
    return await this._participant._publishFramed(framed)
  }

  private _mirrorParticipant(): void {
    const participant = this._participant
    const remote = participant._room._state.getRemote(participant.id)
    const unlistenMeta = remote?.onUpdate(
      (meta: ParticipantMeta) => void this.send({ __r: 'p-meta', meta }).catch(() => {}),
    )

    // The ack carries the client's reply; only a transport rejection means the holder left.
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
        void this.send(notice).catch(() => {})
        return
      }
      return this.send(notice, { ack: true }).then(
        (reply) => decodeDmReply(reply) ?? { ok: false, err: 'Malformed DM reply' },
        () => DM_PARTICIPANT_LEFT,
      )
    })

    const unlistenDemand = participant.onDemand((track, wanted) => {
      void this.send({ __r: 'demand', track, wanted }).catch(() => {})
    })

    let left = false
    const unlistenLeave = participant.onLeave((cause) => {
      left = true
      const notice = { __r: 'left' as const, ...leaveCauseToWire(cause) }
      void this.send(notice).catch(() => {})
      void this.close().catch(() => {})
    })

    this.onClose(() => {
      unlistenMeta?.()
      unlistenDemand()
      unlistenLeave()
      if (!left) void participant._room._removeDepartedMember(participant.id).catch(reportRoomError)
    })
  }
}
