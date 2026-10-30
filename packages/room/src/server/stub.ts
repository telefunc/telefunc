export { RoomStub, RoomParticipantStub, ResponseRoomGrants, MemberMessageTexts, unorderedEventText }
export type { RoomStubOptions, RoomParticipantStubOptions }

import { stringify } from '@brillout/json-serializer/stringify'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { ROOM_TRACK_WANTS_MAX, ROOM_VIEW_MEMBERS_MAX } from '../constants.js'
import { ROOM_DM_ACK_TIMEOUT_MS } from './constants.js'
import { assertProtocol, isProtocolViolationError, parsePeerText, ProtocolViolationError } from './protocolViolation.js'
import {
  encodePublishBinary,
  encodePublishText,
  getChannelLimits,
  type BroadcastEndOwner,
  type BroadcastKind,
  type PageEnd,
  type OrderingInfo,
  type ChannelPublishAck,
} from 'telefunc/__internal'
import { ShieldValidationError, type ShieldValidator } from 'telefunc'
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
import { ANNOUNCE_KEY, ReplayGate, heldTextWeight, heldWeight, type LaneHolder, type ReplayHold } from './replay.js'
import { TailHold, type TailEntry } from './tail.js'
import { ALL, DEFAULT_TRACK, type BinaryFrame, type BinaryWantChange } from '../binary.js'
import { applyBinaryWantChange, binaryWantsCovers, emptyBinaryWants, type BinaryWants } from './binaryWants.js'
import { DM_FAILURE, RoomError, ROOM_BUG_MESSAGE, toRoomFailure } from '../errors.js'
import {
  leaveCauseToWire,
  dmNotice,
  type MemberSnapshot,
  type MemberWants,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type RoomCtrlEnvelope,
  type RoomDataPublish,
  type RoomDemandEvent,
  type InboxMessage,
  type RoomDmEnvelope,
  type RoomFailure,
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
  private readonly _info: OrderingInfo
  private readonly _unstampedEvent: () => object

  constructor({
    serialized,
    info,
    unstampedEvent,
  }: {
    serialized: string
    info: OrderingInfo
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

/** A stub's ack DMs awaiting their `dm-reply`: null at the next `await()` once its sender timed out, or on `drop()`. */
class PendingDmReplies {
  private readonly _pending = new Map<string, { expiresAt: number; resolve: (reply: RoomResult | null) => void }>()

  /** Drops the replies whose sender already timed out. */
  await(ackId: string): Promise<RoomResult | null> {
    const now = performance.now()
    for (const [id, entry] of this._pending) {
      if (entry.expiresAt > now) break // constant offset ⇒ insertion order is deadline order; the rest are younger
      this._pending.delete(id)
      entry.resolve(null)
    }
    return new Promise((resolve) => this._pending.set(ackId, { expiresAt: now + ROOM_DM_ACK_TIMEOUT_MS, resolve }))
  }

  settle(ackId: string, reply: RoomResult): void {
    const entry = this._pending.get(ackId)
    // The sender drops a reply after its timeout.
    if (!entry) return
    this._pending.delete(ackId)
    entry.resolve(reply)
  }

  drop(): void {
    for (const entry of this._pending.values()) entry.resolve(null)
    this._pending.clear()
  }
}

function roomResult(work: Promise<unknown>): Promise<RoomResult> {
  return work.then(
    (value): RoomResult => ({ ok: true, value }),
    (err: unknown) => toRoomFailure(err, reportRoomError, ROOM_BUG_MESSAGE),
  )
}

/** Runs a stub's handling of what its page sent: a malformed message ends the page's wire and fails its request. */
function receiveFromPage<T>(end: PageEnd, handle: () => T): T | Promise<RoomFailure> {
  try {
    return handle()
  } catch (err) {
    if (!isProtocolViolationError(err)) throw err
    end._terminateWire()
    return Promise.resolve({ ok: false, kind: 'error', message: err.message })
  }
}

type RoomStubOptions = {
  room: ServerRoom
  end: PageEnd
  publishShield?: ShieldValidator
  grants: ResponseRoomGrants
}

/** One client's view of a server room: it relays what the client wants and acts for the members the client joined. */
class RoomStub implements LaneHolder, BroadcastEndOwner {
  /** Its channel to its client. */
  readonly _end: PageEnd
  private readonly _room: ServerRoom
  private readonly _publishShield: ShieldValidator | undefined
  private readonly _members = new Set<string>()
  /** Members whose own messages this client doesn't get back:
   *  its selfDelivery: false joins and co-returned server joins. */
  private readonly _selfSuppressed: Set<string>
  /** Hidden members this response handed the client: their events are relayed to it alone. */
  private readonly _grantedHidden: Set<string>
  private readonly _dmReplies = new PendingDmReplies()
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

  constructor({ room, end, publishShield, grants }: RoomStubOptions) {
    this._room = room
    this._publishShield = publishShield
    this._selfSuppressed = grants.selfSuppressed
    this._grantedHidden = grants.hidden
    this._end = end
    end._setOwner(this)
    end.listen((message) => receiveFromPage(end, () => this._receive(message)))
    end.onClose(() => room._detachStub(this))
    end.onDisconnect(() => room._setHeldConnected(this, false))
    end.onReconnect(() => room._setHeldConnected(this, true))
    grants.onSuppressed((member) => room._onStubSuppressed(this, member))
  }

  // Client publishes, subscriptions and attaches

  _onPagePublish(text: string): Promise<RoomResult> {
    return receiveFromPage(this._end, () => roomResult(this._publishText(decodeRoomPublish(parsePeerText(text)))))
  }

  _onPagePublishBinary(framed: Uint8Array): Promise<RoomResult> {
    return receiveFromPage(this._end, () => roomResult(this._publishBinary(decodeStubBinaryFrame(framed), framed)))
  }

  /** Control always flows; text follows broadcast/member wants, while binary uses `sub-binary`. */
  _onPageSubscription(kind: BroadcastKind, on: boolean): void {
    if (kind === 'binary' || on === this._wantsText) return
    const before = on ? { all: this._wantsText, members: new Map<string, boolean>() } : null
    this._wantsText = on
    if (!on) this._replay.forgetUnwantedText((member) => this._wantsTextFrom(member))
    // The tail flush precedes the retained back-fill, so the replay dedupes against what the flush relayed.
    if (on) this._flushTail()
    this._room._onHolderTextChanged(this, before)
  }

  _onPageAttach(): void {
    this._rosterSent = false
    this._room._onStubAttached(this)
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

  private _applyDeclaration(declaration: RoomDeclaration): void {
    switch (declaration.__r) {
      case 'sub-binary':
        return this._declareBinaryWant(declaration)
      case 'sub-text':
        return this._declareTextWants(declaration.members, declaration.announce)
      case 'dm-reply':
        return this._dmReplies.settle(declaration.ackId, declaration.reply)
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
    if (!on) this._replay.forgetUnwantedBinary((member, track) => this._wantsBinary(member, track))
    this._room._onHolderBinaryChanged(this, change)
  }

  private _declareTextWants(changes: Record<string, boolean>, announce: boolean): void {
    const members = new Map<string, boolean>()
    for (const [id, wanted] of Object.entries(changes)) {
      if (wanted === this._textMemberWants.has(id) || (wanted && !this._room._model.hasMember(id))) continue
      members.set(id, !wanted)
      toggle(this._textMemberWants, id, wanted)
    }
    this._replay.forgetUnwantedText((member) => this._wantsTextFrom(member))
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
    this._end.deliver(unorderedEventText({ __r: 'joined', reqId, id }))
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
    this._end.deliver(unorderedEventText(event), { resentOnAttach: true })
  }

  /** `_relayEvent` with the event's text already made, so one text can go to many clients. */
  _relayEventText(text: string): void {
    this._end.deliver(text, { resentOnAttach: true })
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
    if (hiddenMember === null || this._grantedHidden.has(hiddenMember)) this._end.deliver(wireText, opts)
  }

  _relayAnnouncement(wireText: string, ord: OrderingInfo): void {
    if (this._announce) this._relayTextLive(ANNOUNCE_KEY, wireText, ord)
  }

  _relayText(serialized: string, texts: MemberMessageTexts, from: string, ord: OrderingInfo): void {
    if (this._tail !== null) {
      this._tail.push({ serialized, ord, from })
    } else if (this._wantsTextFrom(from)) {
      const replay = this._replay
      if (replay.holding) {
        const relay = () => this._relayText(serialized, texts, from, ord)
        if (replay.held(from, null, heldTextWeight(serialized), relay)) return
      }
      this._relayTextLive(from, this._namesSender(from) ? texts.unstamped() : texts.stamped(), ord)
    }
  }

  _relayBinary(wireData: Uint8Array, from: string, track: string, info: OrderingInfo): void {
    if (!this._wantsBinary(from, track)) return
    const replay = this._replay
    if (
      replay.holding &&
      replay.held(from, track, heldWeight(wireData), () => this._relayBinary(wireData, from, track, info))
    ) {
      return
    }
    if (replay.admitBinary(from, track, info)) this._end.deliverBinary(wireData)
  }

  _hold(change: BinaryWantChange | null): ReplayHold {
    return this._replay.hold(change)
  }

  _relayDm(texts: MemberMessageTexts, { from, fromMetaSeq, ackId }: RoomDmEnvelope): void {
    if (ackId) this._awaitDmReply(ackId, from)
    this._end.deliver(fromMetaSeq !== undefined && this._namesSender(from) ? texts.unstamped() : texts.stamped())
  }

  /** Whether this client names `member` as this view does: it got the whole roster since it attached, and every event
   *  after it, none dropped while it was away. A hidden member's grant may reach the client after a message does. */
  private _namesSender(member: string): boolean {
    return (
      this._end.isAttached &&
      this._rosterSent &&
      this._room._replica.roster.settled &&
      this._room._model.memberHidden(member) === false
    )
  }

  _emitRetainedText(event: RoomDataEnvelope, info: OrderingInfo, serialized: string, hold: ReplayHold): void {
    if (this._replay.admitRetained(hold, event.from, null, info)) this._end.deliver(encodePublishText(serialized, info))
  }

  _emitRetainedBinary(frame: BinaryFrame, info: OrderingInfo, framed: Uint8Array, hold: ReplayHold): void {
    if (this._replay.admitRetained(hold, frame.from, frame.track, info)) {
      this._end.deliverBinary(encodePublishBinary(framed, info))
    }
  }

  private _relayTextLive(key: string, wireText: string, ord: OrderingInfo): void {
    if (this._replay.admit(key, ord)) this._end.deliver(wireText)
  }

  // Ack-DM correlations

  /** A closing stub's close waits for the reply, as for the replies a participant stub forwards. */
  private _awaitDmReply(ackId: string, sender: string): void {
    void this._end
      ._trackAck(
        this._dmReplies.await(ackId).then((reply) => reply && this._room._writes.publishDmAck(sender, ackId, reply)),
      )
      .catch(reportRoomError)
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

  _endReplayHolds(): void {
    this._replay.dropHolds()
  }
}

type RoomParticipantStubOptions = {
  participant: ServerLocalParticipant
  end: PageEnd
  publishShield?: ShieldValidator
}

/** One client's hold on a server participant: its requests act as that participant, whose inbox, demand, meta and leave
 *  flow back to the client. */
class RoomParticipantStub implements BroadcastEndOwner {
  /** Its channel to its client. */
  private readonly _end: PageEnd
  private readonly _participant: ServerLocalParticipant
  private readonly _publishShield: ShieldValidator | undefined
  private readonly _dmReplies = new PendingDmReplies()

  constructor({ participant, end, publishShield }: RoomParticipantStubOptions) {
    this._participant = participant
    this._publishShield = publishShield
    this._end = end
    end._setOwner(this)
    end.listen((message) => receiveFromPage(end, () => this._receive(message)))
    this._mirrorParticipant()
  }

  /** Its client publishes text as a `req-publish` request. */
  _onPagePublish(): Promise<RoomResult> {
    return receiveFromPage(this._end, () => {
      throw new ProtocolViolationError('text publish on a Room participant stub')
    })
  }

  _onPagePublishBinary(framed: Uint8Array): Promise<RoomResult> {
    const participant = this._participant
    return receiveFromPage(this._end, () =>
      roomResult(participant._publishFrame(decodeParticipantFrame(framed, participant.id), framed)),
    )
  }

  _onPageAttach(): void {
    this._notify({ __r: 'p-meta', ...this._participant._acceptedMeta })
    this._notify({ __r: 'demand-state', tracks: this._participant._demandedTracks })
  }

  /** A request, sent with an ack, answers with its result; an ack DM's reply, sent without one, settles that DM. */
  private _receive(message: unknown): Promise<RoomResult> | undefined {
    if (isRoomRequest(message)) return roomResult(this._handleRequest(decodeParticipantRequest(message)))
    const { ackId, reply } = decodeParticipantDeclaration(message)
    this._dmReplies.settle(ackId, reply)
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

    const unlistenDemand = participant.onDemand((track, wanted) =>
      this._notify({ __r: 'demand', track: track ?? DEFAULT_TRACK, wanted }),
    )

    let left = false
    const unlistenLeave = participant.onLeave((cause) => {
      left = true
      this._notify({ __r: 'left', ...leaveCauseToWire(cause) })
      // A client back while its channel still holds it gets its leave.
      void this._end.close({ timeout: getChannelLimits().reconnectWindow }).catch(() => {})
    })

    this._end.onDisconnect(() => participant._setHolderConnected(false))
    this._end.onReconnect(() => participant._setHolderConnected(true))
    this._end.onClose(() => {
      unlistenMeta()
      unlistenDemand()
      unlistenLeave()
      this._dmReplies.drop()
      if (!left) void participant._releaseHolder().catch(reportRoomError)
    })
  }

  /** Its participant's inbox forwards here. */
  deliver(msg: InboxMessage): void {
    this._notify(dmNotice(msg))
  }

  /** A closed stub's client left with the participant; a closing stub's close waits for the replies to DMs it forwarded. */
  deliverAck(msg: InboxMessage): Promise<RoomResult> {
    if (this._end.isClosed) return Promise.resolve(DM_FAILURE.left)
    const reply = this._dmReplies.await(msg.ackId!)
    this._notify(dmNotice(msg))
    return this._end._trackAck(reply.then((answer) => answer ?? DM_FAILURE.left))
  }

  /** A closed stub's client gets no notice: one sent after an overflow ended it would arrive after a gap. */
  private _notify(notice: ParticipantStubNotice): void {
    if (this._end.isClosed) return
    // Each attach sends the meta and the demand again.
    const resentOnAttach = notice.__r === 'p-meta' || notice.__r === 'demand' || notice.__r === 'demand-state'
    this._end.deliver(unorderedEventText(notice), { resentOnAttach })
  }
}

/** The publish shield validates a page's Room data at ingress only. */
function assertPublishShield(validate: ShieldValidator | undefined, data: unknown): void {
  if (!validate) return
  const result = validate(data)
  if (result !== true) throw new ShieldValidationError(result)
}
