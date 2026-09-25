export { RoomStubChannel, RoomParticipantStubChannel }
export type { ResponseRoomGrants }

import { stringify } from '@brillout/json-serializer/stringify'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { assertUsage } from '../../../utils/assert.js'
import { ROOM_DM_ACK_TIMEOUT_MS } from '../constants.js'
import { ServerChannel, parsePeerText } from '../../server/channel.js'
import type { ShieldValidator } from '../../../node/server/shield.js'
import { encodePublishBinary, encodePublishText, type ReattachState, type WirePublishInfo } from '../../shared-ws.js'
import type { IndexedPeer } from '../../server/IndexedPeer.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import type { ChannelPublishAck } from '../../channel.js'
import type { ServerLocalParticipant, ServerRoom } from './room.js'
import { reportRoomError } from './errors.js'
import {
  decodeParticipantFrame,
  decodeParticipantRequest,
  decodeRoomDeclaration,
  decodeRoomPublish,
  decodeRoomRequest,
  type RoomRequest,
  decodeStubBinaryFrame,
  type RoomDeclaration,
} from './requests.js'
import { ANNOUNCE_KEY, ReplayGate, binaryLaneKey, type LaneHolder } from './replay.js'
import { TailHold, type TailEntry } from './tail.js'
import { binaryWantsCovers, emptyBinaryWants, laneTrack, type BinaryFrame, type BinaryWants } from '../binary.js'
import { DM_FAILURE, RoomError, roomAckError } from '../errors.js'
import { leaveCauseToWire } from '../model.js'
import type { OrderingInfo } from '../../ordering-frame.js'
import {
  decodeDmReply,
  wireDmFromInbox,
  type DmReply,
  type MemberSnapshot,
  type MemberWants,
  type ParticipantStubNotice,
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
  private _attached = false

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

  /** Each attach gets the room's state: a reattached client may have missed events its offline buffer dropped. */
  override _attachPeer(peer: IndexedPeer, state?: ReattachState): void {
    const reattach = this._attached
    this._attached = true
    super._attachPeer(peer, state)
    this._room._onStubAttached(this, reattach)
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
    return this._ackRoomResult(seq, this._handleRequest(request))
  }

  async _handleRequest(req: RoomRequest): Promise<unknown> {
    const room = this._room
    switch (req.__r) {
      case 'req-join':
        return await room._joinStubMember(this, req)
      case 'req-leave':
        await room._removeMember(this._requireMember(req.id), { type: 'left' })
        return undefined
      case 'req-set-meta':
        return await room._setMemberMeta(this._requireMember(req.id), req.meta)
      case 'req-set-attrs':
        return await room._mergeMemberMeta(this._requireMember(req.id), req.attrs)
      case 'req-dm':
        return await room._sendDm(this._requireMember(req.id), req.to, req.data, req.ack === true)
    }
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    const publish = decodeRoomPublish(parsePeerText(text))
    return this._ackRoomResult(seq, this._publishText(publish))
  }

  override _onPeerPublishBinaryAckReqMessage(framed: Uint8Array, seq: number): Promise<void> {
    return this._ackRoomResult(seq, this._publishBinary(decodeStubBinaryFrame(framed), framed))
  }

  // Control always flows; text follows broadcast/member wants, while binary uses `sub-binary`.
  override _onPeerBroadcastSubscribe(binary: boolean): void {
    if (binary || this._wantsText) return
    const text = this._memberWants()
    this._wantsText = true
    // The tail flush precedes the retained back-fill, so the replay dedupes against what the flush relayed.
    this._flushTail()
    this._room._onHolderWantsChanged(this, { text })
  }

  override _onPeerBroadcastUnsubscribe(binary: boolean): void {
    if (binary || !this._wantsText) return
    this._wantsText = false
    this._room._onHolderWantsChanged(this, {})
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
    const binary = this._binary
    this._binary = wants
    this._room._onHolderWantsChanged(this, { binary })
  }

  private _declareTextWants(members: string[], announce: boolean): void {
    const text = this._memberWants()
    this._textMemberWants = new Set(members)
    this._announce = announce
    this._flushTail()
    this._room._onHolderWantsChanged(this, { text })
  }

  private _memberWants(): MemberWants {
    return { all: this._wantsText, members: [...this._textMemberWants] }
  }

  private async _publishText(publish: RoomDataPublish): Promise<ChannelPublishAck> {
    this._requireMember(publish.from)
    assertPublishShield(this._publishShield, publish.data)
    return await this._room._publishText(publish.from, publish.data, publish.retain)
  }

  private async _publishBinary(frame: BinaryFrame, framed: Uint8Array): Promise<ChannelPublishAck> {
    this._requireMember(frame.from)
    return await this._room._publishBinaryFrame(frame, framed)
  }

  // Membership

  _holds(id: string): boolean {
    return this._members.has(id)
  }

  _heldMembers(): IterableIterator<string> {
    return this._members.values()
  }

  private _requireMember(id: string): string {
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
  _relayEvent(
    event: RoomRosterEvent | RoomDemandEvent | Extract<RoomCtrlEnvelope, { __r: 'closed' | 'update' }>,
  ): void {
    this._sendPublish(encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  /** The roster holds the hidden members this client was handed, whose meta it heals too. */
  _relayRoster(members: MemberSnapshot[]): void {
    this._relayEvent({
      __r: 'roster',
      members: members.filter(({ id, hidden }) => !hidden || this._grantedHidden.has(id)),
    })
  }

  /** A hidden member's events reach only the clients that were handed it. */
  _relayControl(wireText: string, hiddenMember: string | null): void {
    if (hiddenMember === null || this._grantedHidden.has(hiddenMember)) this._sendPublish(wireText)
  }

  _relayAnnouncement(wireText: string, ord: OrderingInfo): void {
    if (this._announce) this._relayTextLive(ANNOUNCE_KEY, wireText, ord)
  }

  _relayText(serialized: string, wireText: string, from: string, ord: OrderingInfo): void {
    if (this._tail !== null) this._tail.push({ serialized, ord, from })
    else if (this._wantsTextFrom(from)) this._relayTextLive(from, wireText, ord)
  }

  _relayBinary(wireData: Uint8Array, from: string, track: string, info: WirePublishInfo): void {
    if (this._wantsBinary(from, track) && this._replay.admit(binaryLaneKey(from, track), info.seq))
      this._sendPublishBinary(wireData)
  }

  /** The client replies to an ack DM with `dm-reply`, which only the recipient it was relayed to may send. */
  _relayDm(wireText: string, { from, to, ackId }: RoomDmEnvelope): void {
    if (ackId) this._recordAckDm(ackId, from, to)
    this._sendPublish(wireText)
  }

  _emitRetainedText(serialized: string, event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._replay.admit(event.from, info.seq)) this._sendPublish(encodePublishText(serialized, info))
  }

  _emitRetainedBinary(framed: Uint8Array, frame: BinaryFrame, info: WirePublishInfo): void {
    if (this._replay.admit(binaryLaneKey(frame.from, laneTrack(frame.track)), info.seq))
      this._sendPublishBinary(encodePublishBinary(framed, info))
  }

  private _relayTextLive(key: string, wireText: string, ord: OrderingInfo): void {
    if (this._replay.admit(key, ord.seq)) this._sendPublish(wireText)
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
      this._relayTextLive(from, encodePublishText(serialized, ord), ord)
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
    const frame = decodeParticipantFrame(framed, this._participant.id)
    return this._ackRoomResult(seq, this._participant._publishFrame(frame, framed))
  }

  private async _handleRequest(req: ParticipantStubRequest): Promise<unknown> {
    const participant = this._participant
    switch (req.__r) {
      case 'req-publish':
        assertPublishShield(this._publishShield, req.data)
        return await participant.publish(req.data, req.retain ? { retain: true } : undefined)
      case 'req-set-meta':
        return await participant._setMeta(req.meta)
      case 'req-set-attrs':
        return await participant._setAttributes(req.attrs)
      case 'req-dm':
        return await participant.send(req.to, req.data, req.ack ? { ack: true } : undefined)
      case 'req-leave':
        return await participant.leave()
    }
  }

  private _mirrorParticipant(): void {
    const participant = this._participant
    const unlistenMeta = participant._onAcceptedMeta((accepted) => this._notify({ __r: 'p-meta', ...accepted }))

    // The ack carries the client's reply; a closed stub or a transport rejection means the holder left.
    participant._setForwarder((msg) => {
      const notice = { __r: 'dm' as const, ...wireDmFromInbox(msg) }
      if (!msg.ackId) return this._notify(notice)
      if (this.isClosed) return Promise.resolve(DM_FAILURE.left)
      return this.send(notice, { ack: true }).then(
        (reply) => decodeDmReply(reply) ?? DM_FAILURE.malformedReply,
        () => DM_FAILURE.left,
      )
    })

    const unlistenDemand = participant.onDemand((track, wanted) => this._notify({ __r: 'demand', track, wanted }))

    let left = false
    const unlistenLeave = participant.onLeave((cause) => {
      left = true
      this._notify({ __r: 'left', ...leaveCauseToWire(cause) })
      void this.close().catch(() => {})
    })

    this.onClose(() => {
      unlistenMeta()
      unlistenDemand()
      unlistenLeave()
      if (!left) void participant._releaseHolder().catch(reportRoomError)
    })
  }

  /** The client of a closed stub is gone, and so is a notice to it. */
  private _notify(notice: ParticipantStubNotice): void {
    if (!this.isClosed) void this.send(notice).catch(() => {})
  }
}
