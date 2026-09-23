export { RoomStubChannel, RoomParticipantStubChannel }
export type { ResponseRoomGrants }

import { stringify } from '@brillout/json-serializer/stringify'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { assertUsage } from '../../../utils/assert.js'
import { ROOM_DM_ACK_TIMEOUT_MS } from '../constants.js'
import { ServerChannel, parsePeerText } from '../../server/channel.js'
import type { ShieldValidator } from '../../../node/server/shield.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../../shared-ws.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import type { ChannelPublishAck } from '../../channel.js'
import type { ServerLocalParticipant, ServerRoom } from './room.js'
import { reportRoomError, roomAckError } from './errors.js'
import {
  decodeParticipantFrame,
  decodeParticipantRequest,
  decodeRoomDeclaration,
  decodeRoomPublish,
  decodeRoomRequest,
  decodeStubBinaryFrame,
  type RoomDeclaration,
} from './requests.js'
import { ReplayGate, TEXT_LANE_KEY, binaryLaneKey, type LaneHolder } from './replay.js'
import { TailHold, type TailEntry } from './tail.js'
import type { ParticipantMeta } from '../types.js'
import { binaryWantsCovers, emptyBinaryWants, laneTrack, type BinaryFrame, type BinaryWants } from '../binary.js'
import { DM_PARTICIPANT_LEFT, RoomError } from '../errors.js'
import { leaveCauseToWire } from '../model.js'
import {
  decodeDmReply,
  wireDmFromInbox,
  type DmReply,
  type RoomOrder,
  type ParticipantStubRequest,
  type RoomCtrlEnvelope,
  type RoomDataEnvelope,
  type RoomDataPublish,
  type RoomDemandEvent,
  type RoomDmEnvelope,
  type RoomRosterEvent,
} from '../protocol.js'
assertIsNotBrowser()

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

/** The publish shield validates Room data at ingress only; the base channel's validators see every request envelope. */
function assertPublishShield(validate: ShieldValidator | undefined, data: unknown): void {
  if (!validate) return
  const result = validate(data)
  if (result !== true) throw new ShieldValidationError(result)
}

/** One client's view of a server room: it relays what the client wants and acts for the members the client joined. */
class RoomStubChannel extends RoomRequestChannel implements LaneHolder {
  private readonly _room: ServerRoom
  private readonly _publishShield: ShieldValidator | undefined
  private readonly _members = new Set<string>()
  /** Members whose own messages this client doesn't get back: its selfDelivery: false joins and co-returned server joins. */
  private readonly _selfSuppressed: Set<string>
  /** Hidden members this response handed the client: their events are relayed to it alone. */
  private readonly _grantedHidden: Set<string>
  /** Live ack-DM correlations, stored in their constant-offset deadline order. */
  private readonly _pendingAckDms = new Map<string, { sender: string; recipient: string; expiresAt: number }>()
  private readonly _replay = new ReplayGate()
  private _wantsText = false
  private _textMemberWants: ReadonlySet<string> = new Set()
  private _announce = false
  private _binary: BinaryWants = emptyBinaryWants()
  /** A tail waits for the client's first text selector, then flushes once in order. */
  private _tail: TailHold | null = null

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

  // Client requests

  override _onPeerMessage(text: string, bytes: number): void {
    const started = performance.now()
    try {
      this._flow.onReceived(bytes)
      const declaration = decodeRoomDeclaration(parsePeerText(text))
      try {
        this._applyDeclaration(declaration)
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
    return this._ackRoomResult(seq, this._publishText(publish))
  }

  override _onPeerPublishBinaryAckReqMessage(framed: Uint8Array, seq: number): Promise<void> {
    const { from } = decodeStubBinaryFrame(framed)
    return this._ackRoomResult(seq, this._publishBinary(from, framed))
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

  private _applyDeclaration(declaration: RoomDeclaration): void {
    switch (declaration.__r) {
      case 'sub-binary':
        return this._declareBinaryWants(declaration.wants)
      case 'sub-text':
        return this._declareTextWants(declaration.members, declaration.announce)
      case 'dm-reply':
        return this._replyDm(declaration.id, declaration.ackId, declaration.reply)
    }
  }

  private _declareBinaryWants(wants: BinaryWants): void {
    const prev = this._binary
    this._binary = wants
    this._room._syncSubs()
    void this._room._replayRetainedBinary(this, prev).catch(reportRoomError)
  }

  private _declareTextWants(members: string[], announce: boolean): void {
    const prevMembers = this._textMemberWants
    const prevWantsText = this._wantsText
    this._textMemberWants = new Set(members)
    this._announce = announce
    this._flushTail()
    this._room._syncSubs()
    void this._room
      ._replayRetainedText(this, (member) => prevWantsText || prevMembers.has(member))
      .catch(reportRoomError)
  }

  private async _publishText(publish: RoomDataPublish): Promise<ChannelPublishAck> {
    this._requireMember(publish.from)
    assertPublishShield(this._publishShield, publish.data)
    return await this._room._publishText(publish.from, publish.data, publish.retain)
  }

  private async _publishBinary(from: string, framed: Uint8Array): Promise<ChannelPublishAck> {
    return await this._room._publishBinaryFramed(this._requireMember(from), framed)
  }

  // Membership

  _holds(id: string): boolean {
    return this._members.has(id)
  }

  _heldMembers(): IterableIterator<string> {
    return this._members.values()
  }

  _requireMember(id: string): string {
    if (!this._members.has(id)) throw new RoomError('Not a participant of this room (joined through this connection)')
    return id
  }

  _addMember(id: string, selfDelivery: boolean): void {
    this._members.add(id)
    if (!selfDelivery) this._selfSuppressed.add(id)
  }

  _forgetMember(id: string): void {
    this._members.delete(id)
    this._selfSuppressed.delete(id)
    this._replay.forgetMember(id)
  }

  // Wants, as the room's subscription planner reads them

  get _binaryWants(): BinaryWants {
    return this._binary
  }

  get _wantsAnnounce(): boolean {
    return this._announce
  }

  /** A tail-pending stub ingests all text so its hold captures the whole recent tail; its selector applies at flush. */
  _textDemand(): 'all' | ReadonlySet<string> {
    return this._wantsText || this._tail !== null ? 'all' : this._textMemberWants
  }

  _wantsTextFrom(memberId: string): boolean {
    return !this._selfSuppressed.has(memberId) && (this._wantsText || this._textMemberWants.has(memberId))
  }

  _wantsBinary(memberId: string, track: string): boolean {
    return !this._selfSuppressed.has(memberId) && binaryWantsCovers(this._binary, memberId, track)
  }

  // Relays

  /** An event this instance originates for this client alone, outside any lane's order. */
  _relayEvent(event: RoomRosterEvent | RoomDemandEvent | Extract<RoomCtrlEnvelope, { __r: 'closed' }>): void {
    this._sendPublish(encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  /** A hidden member's events reach only the clients that were handed it. */
  _relayControl(wireText: string, hiddenMember: string | null): void {
    if (hiddenMember === null || this._grantedHidden.has(hiddenMember)) this._sendPublish(wireText)
  }

  _relayAnnouncement(wireText: string, ord: RoomOrder): void {
    if (this._announce) this._relayTextLive(wireText, ord)
  }

  _relayText(serialized: string, wireText: string, from: string, ord: RoomOrder): void {
    if (this._tail !== null) this._tail.push({ serialized, ord, from })
    else if (this._wantsTextFrom(from)) this._relayTextLive(wireText, ord)
  }

  _relayBinary(wireData: Uint8Array, from: string, track: string, info: WirePublishInfo): void {
    if (this._wantsBinary(from, track) && this._replay.admitLive(binaryLaneKey(from, track), info.seq))
      this._sendPublishBinary(wireData)
  }

  /** The client replies to an ack DM with `dm-reply`, which only the recipient it was relayed to may send. */
  _relayDm(wireText: string, { from, to, ackId }: RoomDmEnvelope): void {
    if (ackId) this._recordAckDm(ackId, from, to)
    this._sendPublish(wireText)
  }

  _emitRetainedText(serialized: string, _event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._replay.admitRetained(TEXT_LANE_KEY, info.seq)) this._sendPublish(encodePublishText(serialized, info))
  }

  _emitRetainedBinary(framed: Uint8Array, frame: BinaryFrame, info: WirePublishInfo): void {
    if (this._replay.admitRetained(binaryLaneKey(frame.from, laneTrack(frame.track)), info.seq))
      this._sendPublishBinary(encodePublishBinary(framed, info))
  }

  private _relayTextLive(wireText: string, ord: RoomOrder): void {
    if (this._replay.admitLive(TEXT_LANE_KEY, ord.seq)) this._sendPublish(wireText)
  }

  // Ack-DM correlations

  /** Sweeps correlations whose sender already timed out. */
  private _recordAckDm(ackId: string, sender: string, recipient: string): void {
    const now = Date.now()
    for (const [id, entry] of this._pendingAckDms) {
      if (entry.expiresAt > now) break // constant offset ⇒ insertion order is deadline order; the rest are younger
      this._pendingAckDms.delete(id)
    }
    this._pendingAckDms.set(ackId, { sender, recipient, expiresAt: now + ROOM_DM_ACK_TIMEOUT_MS })
  }

  private _replyDm(replier: string, ackId: string, reply: DmReply): void {
    const entry = this._pendingAckDms.get(ackId)
    // The sender drops a reply after its timeout.
    if (!entry || entry.recipient !== replier) return
    this._pendingAckDms.delete(ackId)
    void this._room._publishDmAck(entry.sender, ackId, reply).catch(reportRoomError)
  }

  // Tail

  /** Seeded from the room's pre-attach hold, with a fresh lease; expiry drops the hold and lets the room release ingestion. */
  _beginTail(seed: TailEntry[], onExpire: () => void): void {
    this._tail = new TailHold(() => {
      this._tail = null
      onExpire()
    }, seed)
  }

  /** The first real text want flushes the tail in order through the replay gate. */
  private _flushTail(): void {
    if (this._tail === null) return
    if (!this._wantsText && this._textMemberWants.size === 0) return // keep holding until a real want
    const held = this._tail.take()
    this._tail = null
    for (const { serialized, ord, from } of held) {
      if (!this._wantsTextFrom(from)) continue
      this._relayTextLive(encodePublishText(serialized, ord), ord)
    }
  }

  _endTail(): void {
    this._tail?.end()
    this._tail = null
  }
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
        assertPublishShield(this._publishShield, req.data)
        return await participant.publish(req.data, req.retain ? { retain: true } : undefined)
      case 'req-set-meta':
        return await participant.setMeta(req.meta)
      case 'req-set-attrs':
        return await participant.setAttributes(req.attrs)
      case 'req-dm':
        return await participant.send(req.to, req.data, req.ack ? { ack: true } : undefined)
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
      const notice = { __r: 'dm' as const, ...wireDmFromInbox(msg) }
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
