export { RoomStub, RoomParticipantStub, ResponseRoomGrants, MemberMessageTexts, unorderedEventText }

import { stringify } from '@brillout/json-serializer/stringify'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { assertUsage } from '../../../utils/assert.js'
import { ROOM_TRACK_WANTS_MAX, ROOM_VIEW_MEMBERS_MAX } from '../constants.js'
import { ROOM_DM_ACK_TIMEOUT_MS } from './constants.js'
import { parsePeerText } from '../../channel/server/channel.js'
import type { ServerBroadcastEnd } from '../../channel/server/ServerBroadcastEnd.js'
import type { ServerReplacerContext } from '../../values/types.js'
import { reconnectWindow } from '../../../node/server/serverConfig.js'
import type { ShieldValidator } from '../../../node/server/shield.js'
import {
  assertProtocol,
  encodePublishBinary,
  encodePublishText,
  ProtocolViolationError,
  type BroadcastKind,
  type WirePublishInfo,
} from '../../wire/connectionFrame.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import type { ChannelPublishAck } from '../../channel/channel.js'
import type { ServerLocalParticipant, ServerRoom } from './room.js'
import { reportRoomError } from './errors.js'
import {
  decodeParticipantDeclaration,
  decodeParticipantFrame,
  decodeParticipantRequest,
  decodeRoomDeclaration,
  decodeRoomPublish,
  decodeRoomRequest,
  type RoomRequest,
  decodeStubBinaryFrame,
  isRoomRequest,
  type RoomDeclaration,
} from './requests.js'
import { ANNOUNCE_KEY, ReplayGate, type LaneHolder } from './replay.js'
import { TailHold, type TailEntry } from './tail.js'
import { ALL, DEFAULT_TRACK, type BinaryFrame, type BinaryWantChange } from '../binary.js'
import { applyBinaryWantChange, binaryWantsCovers, emptyBinaryWants, type BinaryWants } from './binaryWants.js'
import { DM_FAILURE, RoomError, toRoomResult } from '../errors.js'
import {
  leaveCauseToWire,
  dmNotice,
  type DmReply,
  type MemberSnapshot,
  type MemberWants,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type RoomCtrlEnvelope,
  type RoomDataPublish,
  type RoomDemandEvent,
  type RoomDmEnvelope,
  type RoomJoinedEvent,
  type RoomResult,
  type RoomRosterEvent,
} from '../protocol.js'
import type { RoomDataEnvelope } from './protocol.js'
import { toggle } from './sets.js'
assertIsNotBrowser()

/** What one response's Room values grant the client on a room: echo drops for its own members, and hidden members it
 *  returned. A streamed response can serialize a participant after its room's stub attached, so the stubs holding the
 *  grants hear each new echo drop. */
class ResponseRoomGrants {
  readonly selfSuppressed = new Set<string>()
  readonly hidden = new Set<string>()
  private readonly _onSuppressed: Array<(member: string) => void> = []

  suppress(member: string): void {
    if (this.selfSuppressed.has(member)) return
    this.selfSuppressed.add(member)
    for (const listener of this._onSuppressed) listener(member)
  }

  onSuppressed(listener: (member: string) => void): void {
    this._onSuppressed.push(listener)
  }
}

/** An event an instance originates, outside any lane's order. */
function unorderedEventText(
  event: RoomCtrlEnvelope | RoomRosterEvent | RoomJoinedEvent | RoomDemandEvent | ParticipantStubNotice,
): string {
  return encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() })
}

/** A member's message as its lane carried it, stamped, and without the stamp, each made once for every client. */
class MemberMessageTexts {
  private _stamped: string | null = null
  private _unstamped: string | null = null

  private readonly _serialized: string
  private readonly _info: WirePublishInfo
  private readonly _unstampedEvent: () => object

  constructor({
    serialized,
    info,
    unstampedEvent,
  }: {
    serialized: string
    info: WirePublishInfo
    unstampedEvent: () => object
  }) {
    this._serialized = serialized
    this._info = info
    this._unstampedEvent = unstampedEvent
  }

  stamped(): string {
    return (this._stamped ??= encodePublishText(this._serialized, this._info))
  }

  unstamped(): string {
    return (this._unstamped ??= encodePublishText(stringify(this._unstampedEvent()), this._info))
  }
}

function roomResult(work: Promise<unknown>): Promise<RoomResult> {
  return toRoomResult(work, reportRoomError)
}

/** The publish shield validates Room data at ingress only. */
function assertPublishShield(validate: ShieldValidator | undefined, data: unknown): void {
  if (!validate) return
  const result = validate(data)
  if (result !== true) throw new ShieldValidationError(result)
}

/** One client's view of a server room: it relays what the client wants and acts for the members the client joined. */
class RoomStub implements LaneHolder {
  /** Its channel to its client. */
  readonly end: ServerBroadcastEnd
  private readonly _room: ServerRoom
  private readonly _publishShield: ShieldValidator | undefined
  private readonly _members = new Set<string>()
  /** Members whose own messages this client doesn't get back:
   *  its selfDelivery: false joins and co-returned server joins. */
  private readonly _selfSuppressed: Set<string>
  /** Hidden members this response handed the client: their events are relayed to it alone. */
  private readonly _grantedHidden: Set<string>
  /** Live ack-DM correlations, stored in their constant-offset deadline order. */
  private readonly _pendingAckDms = new Map<string, { sender: string; expiresAt: number }>()
  private readonly _replay = new ReplayGate()
  private _wantsText = false
  private readonly _textMemberWants = new Set<string>()
  private _announce = false
  private readonly _binary: BinaryWants = emptyBinaryWants()
  /** The tracks `_binary` names, each `(member or room, track)` pair once. */
  private _trackWants = 0
  /** A tail waits for the client's first text selector, then flushes once in order. */
  private _tail: TailHold | null = null
  /** @internal Whether this client was sent the whole roster since it attached. */
  _rosterSent = false

  constructor(
    serverRoom: ServerRoom,
    {
      publishShield,
      grants,
      createBroadcastChannel,
    }: {
      publishShield?: ShieldValidator
      grants: ResponseRoomGrants
      createBroadcastChannel: ServerReplacerContext['createBroadcastChannel']
    },
  ) {
    this._room = serverRoom
    this._publishShield = publishShield
    this._selfSuppressed = grants.selfSuppressed
    this._grantedHidden = grants.hidden
    this.end = createBroadcastChannel({
      key: serverRoom.id,
      onPublish: (text) => roomResult(this._publishText(decodeRoomPublish(parsePeerText(text)))),
      onPublishBinary: (framed) => roomResult(this._publishBinary(decodeStubBinaryFrame(framed), framed)),
      onSubscription: (kind, on) => this._onSubscription(kind, on),
      onAttach: () => {
        this._rosterSent = false
        this._room._onStubAttached(this)
      },
    })
    this.end.listen((message) => this._receive(message))
    grants.onSuppressed((member) => this._room._onStubSuppressed(this, member))
  }

  // Client requests

  /** A request, sent with an ack, answers with its result; a declaration, sent without one, applies at once. */
  private _receive(message: unknown): Promise<RoomResult> | undefined {
    if (isRoomRequest(message)) return roomResult(this._handleRequest(decodeRoomRequest(message)))
    const declaration = decodeRoomDeclaration(message)
    if (declaration.__r === 'sub-binary') this._assertTrackWantFits(declaration)
    this._applyDeclaration(declaration)
    return undefined
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
        return await room._writes.setMemberMeta(this._requireMember(req.id), req.meta)
      case 'req-set-attrs':
        return await room._writes.mergeMemberMeta(this._requireMember(req.id), req.attrs)
      case 'req-dm':
        return await room._writes.sendDm(this._requireMember(req.id), req.to, req.data, req.ack === true)
    }
  }

  /** Control always flows; text follows broadcast/member wants, while binary uses `sub-binary`. */
  private _onSubscription(kind: BroadcastKind, on: boolean): void {
    if (kind === 'binary' || on === this._wantsText) return
    const before = on ? { all: this._wantsText, members: new Map<string, boolean>() } : null
    this._wantsText = on
    // The tail flush precedes the retained back-fill, so the replay dedupes against what the flush relayed.
    if (on) this._flushTail()
    this._room._onHolderTextChanged(this, before)
  }

  private _applyDeclaration(declaration: RoomDeclaration): void {
    switch (declaration.__r) {
      case 'sub-binary':
        return this._declareBinaryWant(declaration)
      case 'sub-text':
        return this._declareTextWants(declaration.members, declaration.announce)
      case 'dm-reply':
        return this._replyDm(declaration.ackId, declaration.reply)
    }
  }

  /** The client library names at most `ROOM_TRACK_WANTS_MAX` tracks, so only a peer bypassing it names more. */
  private _assertTrackWantFits({ member, track, on }: BinaryWantChange): void {
    if (!on || track === ALL || this._trackWants < ROOM_TRACK_WANTS_MAX) return
    const scope = member === ALL ? this._binary.everyMember : this._binary.members.get(member)
    assertProtocol(scope?.tracks.has(track), 'Room binary wants over ROOM_TRACK_WANTS_MAX')
  }

  /** A client learns its members from this view alone, so a want naming any other id names one that
   *  left or never was. */
  private _declareBinaryWant(change: BinaryWantChange): void {
    const { member, track, on } = change
    if (on && member !== ALL && !this._room._model.hasMember(member)) return
    if (!applyBinaryWantChange(this._binary, change)) return
    if (track !== ALL) this._trackWants += on ? 1 : -1
    this._room._onHolderBinaryChanged(this, change)
  }

  private _declareTextWants(changes: Record<string, boolean>, announce: boolean): void {
    const members = new Map<string, boolean>()
    for (const [id, wanted] of Object.entries(changes)) {
      if (wanted === this._textMemberWants.has(id) || (wanted && !this._room._model.hasMember(id))) continue
      members.set(id, !wanted)
      toggle(this._textMemberWants, id, wanted)
    }
    this._announce = announce
    this._flushTail()
    this._room._onHolderTextChanged(this, { all: this._wantsText, members })
  }

  private async _publishText(publish: RoomDataPublish): Promise<ChannelPublishAck> {
    this._requireMember(publish.from)
    assertPublishShield(this._publishShield, publish.data)
    return await this._room._writes.publishText(publish.from, publish.data, publish.retain)
  }

  private async _publishBinary(frame: BinaryFrame, framed: Uint8Array): Promise<ChannelPublishAck> {
    this._requireMember(frame.from)
    return await this._room._writes.publishBinaryFrame(frame, framed)
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

  /** Takes the member its client's `req-join` (`reqId`) admitted, and tells the client before any event naming it; past
   *  `ROOM_VIEW_MEMBERS_MAX`, throws instead, before the join commits. */
  _addMember(reqId: number, id: string, selfDelivery: boolean): void {
    if (this._members.size === ROOM_VIEW_MEMBERS_MAX) {
      throw new RoomError(`A room view joins at most ${ROOM_VIEW_MEMBERS_MAX} participants`)
    }
    this._members.add(id)
    if (!selfDelivery) this._selfSuppressed.add(id)
    this.end.deliver(unorderedEventText({ __r: 'joined', reqId, id }))
  }

  _forgetMember(id: string): void {
    this._members.delete(id)
    this._selfSuppressed.delete(id)
    this._replay.forgetMember(id)
  }

  /** Drops its wants naming `id`; returns whether it held any. */
  _forgetWantsOf(id: string): boolean {
    const text = this._textMemberWants.delete(id)
    this._trackWants -= this._binary.members.get(id)?.tracks.size ?? 0
    return this._binary.members.delete(id) || text
  }

  // Wants, as the room's subscription planner reads them

  get _binaryWants(): BinaryWants {
    return this._binary
  }

  get _wantsAnnounce(): boolean {
    return this._announce
  }

  /** A tail-pending stub ingests all text so its hold captures the whole recent tail; its selector applies at flush. */
  _textDemand(): MemberWants {
    return { all: this._wantsText || this._tail !== null, members: this._textMemberWants }
  }

  _wantsTextFrom(memberId: string): boolean {
    return !this._selfSuppressed.has(memberId) && (this._wantsText || this._textMemberWants.has(memberId))
  }

  _wantsBinary(memberId: string, track: string): boolean {
    return !this._selfSuppressed.has(memberId) && binaryWantsCovers(this._binary, memberId, track)
  }

  _suppressedMembers(): ReadonlySet<string> {
    return this._selfSuppressed
  }

  // Relays

  /** An event this instance originates for this client alone: state each attach sends again. */
  _relayEvent(event: RoomRosterEvent | RoomDemandEvent | Extract<RoomCtrlEnvelope, { __r: 'update' }>): void {
    this.end.deliver(unorderedEventText(event), { resentOnAttach: true })
  }

  /** `_relayEvent` with the event's text already made, so one text can go to many clients. */
  _relayEventText(text: string): void {
    this.end.deliver(text, { resentOnAttach: true })
  }

  /** A roster part's members this client gets: `visible` itself unless the part holds a hidden member this client was
   *  handed, whose meta it heals too. */
  _rosterMembers(part: MemberSnapshot[], visible: MemberSnapshot[]): MemberSnapshot[] {
    if (visible.length === part.length || this._grantedHidden.size === 0) return visible
    const members = part.filter(({ id, hidden }) => !hidden || this._grantedHidden.has(id))
    return members.length === visible.length ? visible : members
  }

  /** A hidden member's events reach only the clients that were handed it. */
  _relayControl(wireText: string, hiddenMember: string | null, opts: { resentOnAttach: boolean }): void {
    if (hiddenMember === null || this._grantedHidden.has(hiddenMember)) this.end.deliver(wireText, opts)
  }

  _relayAnnouncement(wireText: string, ord: WirePublishInfo): void {
    if (this._announce) this._relayTextLive(ANNOUNCE_KEY, wireText, ord)
  }

  _relayText(serialized: string, texts: MemberMessageTexts, from: string, ord: WirePublishInfo): void {
    if (this._tail !== null) {
      this._tail.push({ serialized, ord, from })
    } else if (this._wantsTextFrom(from)) {
      this._relayTextLive(from, this._namesSender(from) ? texts.unstamped() : texts.stamped(), ord)
    }
  }

  _relayBinary(wireData: Uint8Array, from: string, track: string, info: WirePublishInfo): void {
    if (this._wantsBinary(from, track) && this._replay.admitBinary(from, track, info)) this.end.deliverBinary(wireData)
  }

  _relayDm(texts: MemberMessageTexts, { from, fromMetaSeq, ackId }: RoomDmEnvelope): void {
    if (ackId) this._recordAckDm(ackId, from)
    this.end.deliver(fromMetaSeq !== undefined && this._namesSender(from) ? texts.unstamped() : texts.stamped())
  }

  /** Whether this client names `member` as this view does: it got the whole roster since it attached, and every event
   *  after it, none dropped while it was away. A hidden member's grant may reach the client after a message does. */
  private _namesSender(member: string): boolean {
    return (
      this.end.isAttached &&
      this._rosterSent &&
      this._room._replica.roster.settled &&
      this._room._model.memberHidden(member) === false
    )
  }

  _emitRetainedText(event: RoomDataEnvelope, info: WirePublishInfo, serialized: string): void {
    if (this._replay.admit(event.from, info)) this.end.deliver(encodePublishText(serialized, info))
  }

  _emitRetainedBinary(frame: BinaryFrame, info: WirePublishInfo, framed: Uint8Array): void {
    if (this._replay.admitBinary(frame.from, frame.track, info)) {
      this.end.deliverBinary(encodePublishBinary(framed, info))
    }
  }

  private _relayTextLive(key: string, wireText: string, ord: WirePublishInfo): void {
    if (this._replay.admit(key, ord)) this.end.deliver(wireText)
  }

  // Ack-DM correlations

  /** Sweeps correlations whose sender already timed out. */
  private _recordAckDm(ackId: string, sender: string): void {
    const now = Date.now()
    for (const [id, entry] of this._pendingAckDms) {
      if (entry.expiresAt > now) break // constant offset ⇒ insertion order is deadline order; the rest are younger
      this._pendingAckDms.delete(id)
    }
    this._pendingAckDms.set(ackId, { sender, expiresAt: now + ROOM_DM_ACK_TIMEOUT_MS })
  }

  private _replyDm(ackId: string, reply: DmReply): void {
    const entry = this._pendingAckDms.get(ackId)
    // The sender drops a reply after its timeout.
    if (!entry) return
    this._pendingAckDms.delete(ackId)
    void this._room._writes.publishDmAck(entry.sender, ackId, reply).catch(reportRoomError)
  }

  // Tail

  /** Seeded from the room's pre-attach hold, with a fresh lease; expiry drops the hold and lets
   *  the room release ingestion. */
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

/** One client's hold on a server participant: its requests act as that participant, whose inbox, demand, meta and leave
 *  flow back to the client. */
class RoomParticipantStub {
  /** Its channel to its client. */
  readonly end: ServerBroadcastEnd
  private readonly _participant: ServerLocalParticipant
  private readonly _publishShield: ShieldValidator | undefined
  /** The ack DMs its client was sent, each awaiting the client's `dm-reply`. */
  private readonly _pendingAckDms = new Map<string, (reply: DmReply) => void>()

  constructor(
    participant: ServerLocalParticipant,
    {
      publishShield,
      createBroadcastChannel,
    }: {
      publishShield?: ShieldValidator
      createBroadcastChannel: ServerReplacerContext['createBroadcastChannel']
    },
  ) {
    // A LocalParticipant has one holder; rebinding would overwrite its inbox forwarder and let either close drop it.
    assertUsage(
      !participant._isBound,
      'This LocalParticipant is already bound to a client and cannot be handed to another. A LocalParticipant is a single member: give each client its own join(), or share a getParticipants() view (which is read-only) instead.',
    )
    this._participant = participant
    this._publishShield = publishShield
    this.end = createBroadcastChannel({
      key: participant._room.id,
      // Its client publishes text as a `req-publish` request.
      onPublish: () => {
        throw new ProtocolViolationError('text publish on a Room participant stub')
      },
      onPublishBinary: (framed) =>
        roomResult(participant._publishFrame(decodeParticipantFrame(framed, participant.id), framed)),
      onAttach: () => {
        this._notify({ __r: 'p-meta', ...participant._acceptedMeta })
        this._notify({ __r: 'demand-state', tracks: participant._demandedTracks })
      },
    })
    this.end.listen((message) => this._receive(message))
    this._mirrorParticipant()
  }

  /** A request, sent with an ack, answers with its result; an ack DM's reply, sent without one, settles that DM. */
  private _receive(message: unknown): Promise<RoomResult> | undefined {
    if (isRoomRequest(message)) return roomResult(this._handleRequest(decodeParticipantRequest(message)))
    const { ackId, reply } = decodeParticipantDeclaration(message)
    // The sender drops a reply after its timeout.
    this._pendingAckDms.get(ackId)?.(reply)
    this._pendingAckDms.delete(ackId)
    return undefined
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
    const unlistenMeta = participant._onMeta({
      accepted: (accepted) => this._notify({ __r: 'p-meta', ...accepted }),
      rewound: () => this._notify({ __r: 'rewound' }),
    })

    participant._setForwarder({
      deliver: (msg) => this._notify(dmNotice(msg)),
      // A closed stub's client left with the participant; a closing stub's close waits for the replies to DMs it forwarded.
      deliverAck: (msg) => {
        if (this.end.isClosed) return Promise.resolve(DM_FAILURE.left)
        return this.end._trackAck(
          new Promise<DmReply>((resolve) => {
            this._pendingAckDms.set(msg.ackId!, resolve)
            this._notify(dmNotice(msg))
          }),
        )
      },
    })

    const unlistenDemand = participant.onDemand((track, wanted) =>
      this._notify({ __r: 'demand', track: track ?? DEFAULT_TRACK, wanted }),
    )

    let left = false
    const unlistenLeave = participant.onLeave((cause) => {
      left = true
      this._notify({ __r: 'left', ...leaveCauseToWire(cause) })
      // A client back while its channel still holds it gets its leave.
      void this.end.close({ timeout: reconnectWindow() }).catch(() => {})
    })

    this.end.onClose(() => {
      unlistenMeta()
      unlistenDemand()
      unlistenLeave()
      for (const resolve of this._pendingAckDms.values()) resolve(DM_FAILURE.left)
      this._pendingAckDms.clear()
      if (!left) void participant._releaseHolder().catch(reportRoomError)
    })
  }

  private _notify(notice: ParticipantStubNotice): void {
    // Each attach sends the meta and the demand again.
    const resentOnAttach = notice.__r === 'p-meta' || notice.__r === 'demand' || notice.__r === 'demand-state'
    this.end.deliver(unorderedEventText(notice), { resentOnAttach })
  }
}
