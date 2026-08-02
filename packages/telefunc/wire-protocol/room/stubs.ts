export { RoomStubChannel, RoomParticipantStubChannel, bindParticipantStubChannel }

import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { ROOM_DM_ACK_TIMEOUT_MS, ROOM_TAIL_ATTACH_TIMEOUT_MS } from './constants.js'
import type { ChannelPublishAck } from '../channel.js'
import { ServerChannel } from '../server/channel.js'
import type { ShieldValidator } from '../../node/server/shield.js'
import { ServerBroadcast } from '../server/server-broadcast.js'
import { encodePublishText, type WirePublishInfo } from '../shared-ws.js'
import { type ServerLocalParticipant, type ServerRoom } from './server.js'
import { reportRoomError, roomAckError } from './server/errors.js'
import type { ParticipantMeta, RoomSendReceipt } from './types.js'
import { binaryWantsCovers, emptyTrackWants, unframeMemberId, type BinaryWants } from './binary.js'
import { DM_PARTICIPANT_LEFT, RoomError, roomFailureError } from './errors.js'
import { roomCtrlKey } from './keys.js'
import { leaveCauseToWire } from './model.js'
import {
  pushBoundedTail,
  hasRoomTag,
  type RoomOrder,
  type DmReply,
  type MemberSnapshot,
  type ParticipantStubRequest,
  type RoomDemandEvent,
  type RoomRosterEvent,
} from './protocol.js'
assertIsNotBrowser()

// Room authority stays server-side; each wire stub owns one holder's wants, buffering, watermarks, and correlations.

/** Server→client control/data obey wants; client→server membership/control and validated publishes use native channel acks. */
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
  /** One self-delivery gate combines direct client joins and co-returned server joins before wire emission. */
  _selfSuppressed = new Set<string>()
  /** Adopts the serialization pass's shared self-delivery gate. */
  _adoptSelfSuppressed(set: Set<string>): void {
    this._selfSuppressed = set
  }
  /** The generated publish shield validates Room data ingress only; base validators own multiplexed request envelopes. */
  _publishShield?: ShieldValidator
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

  /** Retained/live dedup uses one text watermark and flat `${member}\0${track}` binary watermarks, pruned on leave. */
  private _textHigh: RoomOrder | null = null
  private _textPendingRetained: RoomOrder | null = null
  private readonly _binaryHigh = new Map<string, WirePublishInfo>()
  private readonly _binaryPendingRetained = new Map<string, WirePublishInfo>()

  /** @internal — relay gate: does this client want the (member, track) the frame belongs to? */
  _wantsBinary(memberId: string, track: string): boolean {
    return binaryWantsCovers(this._binaryWants, memberId, track)
  }

  _wantsTextFrom(memberId: string): boolean {
    return this._wantsText || this._textMemberWants.has(memberId)
  }

  constructor(serverRoom: ServerRoom) {
    super({ key: roomCtrlKey(serverRoom.id) })
    this._room = serverRoom
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

  // Control always flows; text follows broadcast/member wants, while binary uses `sub-binary`.
  override _onPeerBroadcastSubscribe(binary: boolean): void {
    if (binary) return
    const prevWantsText = this._wantsText
    this._wantsText = true
    // Tail mode: this room-level want covers the whole held tail — flush it before the retained back-fill, so the flush advances the causal watermark and the retained replay dedupes against it.
    this._flushTail()
    this._room._syncSubs()
    void this._room._replayRetainedText(this, prevWantsText, this._textMemberWants).catch(reportRoomError)
  }
  override _onPeerBroadcastUnsubscribe(binary: boolean): void {
    if (binary) return
    this._wantsText = false
    this._room._syncSubs()
  }

  _relayRoster(members: MemberSnapshot[]): void {
    const event: RoomRosterEvent = { __r: 'roster', members }
    this._relayPublishText(encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  _relayRosterError(): void {
    const event: RoomRosterEvent = { __r: 'roster-error' }
    this._relayPublishText(encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  _relayDemand(event: RoomDemandEvent): void {
    this._relayPublishText(encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  _relayPublishText(wireText: string): void {
    if (this._peer) this._peer.sendPublish(wireText)
    else this._prePeerBuffer.pushPublish(wireText)
  }

  _relayPublishBinary(wireData: Uint8Array): void {
    if (this._peer) this._peer.sendPublishBinary(wireData)
    else this._prePeerBuffer.pushPublishBinary(wireData)
  }

  /** Live text advances its watermark and drops the echo of a retained frame just emitted. */
  _relayTextLive(wireText: string, _from: string, ord: RoomOrder): void {
    const pending = this._textPendingRetained
    if (pending && pending.seq === ord.seq && pending.timestamp === ord.timestamp) {
      this._textPendingRetained = null // this live frame is the echo of the retained we emitted
      return
    }
    this._relayPublishText(wireText)
    if (!this._textHigh || this._textHigh.seq < ord.seq) this._textHigh = ord
  }

  /** Retained text replays only above the live watermark, then suppresses its matching live echo. */
  _emitRetainedText(wireText: string, _from: string, ord: RoomOrder): void {
    const high = this._textHigh
    if (high && high.seq >= ord.seq) return // superseded by a same-or-newer live frame
    this._relayPublishText(wireText)
    this._textHigh = ord
    this._textPendingRetained = ord
  }

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

  _emitRetainedBinary(wireData: Uint8Array, from: string, track: string, info: WirePublishInfo): void {
    const lane = `${from}\0${track}`
    if ((this._binaryHigh.get(lane)?.seq ?? -1) >= info.seq) return // superseded by a same-or-newer live frame
    this._relayPublishBinary(wireData)
    this._binaryHigh.set(lane, info)
    this._binaryPendingRetained.set(lane, info)
  }

  _forgetMember(from: string): void {
    const prefix = `${from}\0`
    for (const lane of [this._binaryHigh, this._binaryPendingRetained])
      for (const key of lane.keys()) if (key.startsWith(prefix)) lane.delete(key)
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
    if (!hold) return
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
      if (this._selfSuppressed.has(from)) continue
      this._relayTextLive(encodePublishText(serialized, ord), from, ord)
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

/** Serializes one server participant; metadata observation may be absent after a leave race. */
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

function asRoomRecord(value: unknown): ParticipantMeta { return isObject(value) ? value : {} }
async function sendParticipantDm(participant: ServerLocalParticipant, req: Extract<ParticipantStubRequest, { __r: 'req-dm' }>) {
  if (!req.ack) return (await participant.send(req.to, req.data)) as RoomSendReceipt
  const { receipt, reply } = await participant._room._sendDmAck(participant.id, req.to, req.data)
  if (!reply.ok) throw roomFailureError(reply)
  return { ...receipt, response: reply.result }
}
async function handleParticipantStubRequest(
  participant: ServerLocalParticipant, publishShield: ShieldValidator | undefined, msg: unknown,
): Promise<unknown> {
  if (!hasRoomTag(msg)) return undefined
  const req = msg as ParticipantStubRequest
  if (req.__r === 'req-publish') {
    participant._room._shieldPublishData(publishShield, req.data)
    return await participant.publish(req.data, req.retain ? { retain: true } : undefined)
  }
  if (req.__r === 'req-set-meta') return await participant.setMeta(asRoomRecord(req.meta))
  if (req.__r === 'req-set-attrs') return await participant.setAttributes(asRoomRecord(req.attrs))
  if (req.__r === 'req-dm') return await sendParticipantDm(participant, req)
  if (req.__r === 'req-leave') return await participant.leave()
  return undefined
}

function bindParticipantStubChannel(
  channel: RoomParticipantStubChannel,
  participant: ServerLocalParticipant,
  publishShield?: ShieldValidator,
): void {
  // A LocalParticipant has one holder; rebinding would overwrite its inbox forwarder and let either close drop it.
  assertUsage(
    !participant._isBound,
    'This LocalParticipant is already bound to a client and cannot be handed to another. A LocalParticipant is a single member: give each client its own join(), or share a getParticipants() view (which is read-only) instead.',
  )
  channel._listenRoomRequests((msg) => handleParticipantStubRequest(participant, publishShield, msg))

  channel.listenBinary(async (framed: Uint8Array) => {
    if (unframeMemberId(framed)?.from !== participant.id) throw new RoomError('Malformed room binary publish')
    return await participant._publishFramed(framed)
  })

  const remote = participant._room._state.getRemote(participant.id)
  const unlistenMeta = remote?.onUpdate(
    (meta: ParticipantMeta) => void channel.send({ __r: 'p-meta', meta }).catch(() => {}),
  )

  // Channel ack returns resolved DmReply outcomes; only transport rejection means the holder left.
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
      () => DM_PARTICIPANT_LEFT,
    )
  })

  const unlistenDemand = participant.onDemand((track, wanted) => {
    void channel.send({ __r: 'demand', track, wanted }).catch(() => {})
  })

  const unlistenLeave = participant.onLeave((cause) => {
    const notice = { __r: 'left' as const, ...leaveCauseToWire(cause) }
    void channel.send(notice).catch(() => {})
    void channel.close().catch(() => {})
  })

  channel.onClose(() => {
    unlistenMeta?.()
    unlistenDemand()
    unlistenLeave()
    void participant.leave().catch(reportRoomError)
  })
}
