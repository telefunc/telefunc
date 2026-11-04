export { RoomStub, RoomParticipantStub, ResponseRoomGrants, MemberMessageTexts, unorderedEventText }
export type { RoomStubOptions, RoomParticipantStubOptions }

import { stringify } from '@brillout/json-serializer/stringify'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { ROOM_TRACK_WANTS_MAX, ROOM_VIEW_MEMBERS_MAX } from '../shared/constants.js'
import { ROOM_DM_ACK_TIMEOUT_MS } from './constants.js'
import {
  assertProtocol,
  isProtocolViolationError,
  parsePeerText,
  ProtocolViolationError,
  encodePublish,
  getChannelLimits,
  type BroadcastEndOwner,
  type BroadcastKind,
  type ServerBroadcastEnd,
  type OrderingInfo,
  type ChannelPublishAck,
} from 'telefunc/__internal'
import { ShieldValidationError, type ShieldValidator } from 'telefunc'
import type { RoomHandle, ServerLocalParticipant } from './room.js'
import type { RosterStream } from './lifecycle/RosterStream.js'
import { reportRoomError } from './reportRoomError.js'
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
import {
  ANNOUNCE_KEY,
  ReplayGate,
  heldTextWeight,
  heldWeight,
  type LaneHolder,
  type ReplayHold,
} from './lifecycle/replay.js'
import { TailHold, type TailEntry, type TailHoldOwner } from './lifecycle/TailHold.js'
import { EVERY_MEMBER, EVERY_TRACK, DEFAULT_TRACK, type BinaryFrame, type BinaryWantChange } from '../shared/binary.js'
import { applyBinaryWantChange, binaryWantsCovers, emptyBinaryWants, type BinaryWants } from './lanes/binaryWants.js'
import { DM_FAILURE, RoomError, ROOM_BUG_MESSAGE, toRoomFailure } from '../shared/errors.js'
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
} from '../shared/protocol.js'
import type { RoomDataEnvelope } from './protocol.js'
import { toggle } from '../utils/sets.js'
assertIsNotBrowser()

/** What one response's Room values grant the client on a room: echo drops for its own members, and hidden members it
 *  returned. A streamed response can serialize a participant after its room's stub attached, so the stubs holding the
 *  grants hear each new echo drop. */
class ResponseRoomGrants {
  readonly selfSuppressed = new Set<string>()
  readonly hidden = new Set<string>()
  private readonly suppressedListeners: Array<(member: string) => void> = []

  suppress(member: string): void {
    if (this.selfSuppressed.has(member)) return
    this.selfSuppressed.add(member)
    for (const listener of this.suppressedListeners) listener(member)
  }

  onSuppressed(listener: (member: string) => void): void {
    this.suppressedListeners.push(listener)
  }
}

/** An event an instance originates, outside any lane's order. */
function unorderedEventText(
  event: RoomCtrlEnvelope | RoomRosterEvent | RoomJoinedEvent | RoomDemandEvent | ParticipantStubNotice,
): string {
  return encodePublish(stringify(event), null)
}

/** A member's message as its lane carried it, stamped, and without the stamp, each made once for every client. */
class MemberMessageTexts {
  private _stamped: string | null = null
  private _unstamped: string | null = null

  private readonly serialized: string
  private readonly info: OrderingInfo
  /** What `serialized` encodes. */
  private readonly envelope: RoomDataEnvelope | RoomDmEnvelope

  // Positional, as one is built per message.
  constructor(serialized: string, info: OrderingInfo, envelope: RoomDataEnvelope | RoomDmEnvelope) {
    this.serialized = serialized
    this.info = info
    this.envelope = envelope
  }

  stamped(): string {
    return (this._stamped ??= encodePublish(this.serialized, this.info))
  }

  unstamped(): string {
    if (this._unstamped !== null) return this._unstamped
    const { fromMeta, fromMetaSeq, fromIdentity, fromDisconnected, ...unstamped } = this.envelope
    return (this._unstamped = encodePublish(stringify(unstamped), this.info))
  }
}

/** A stub's ack DMs awaiting their `dm-reply`: null at the next `await()` once its sender timed out, or on `drop()`. */
class PendingDmReplies {
  private readonly pending = new Map<string, { expiresAt: number; resolve: (reply: RoomResult | null) => void }>()

  /** Drops the replies whose sender already timed out. */
  await(ackId: string): Promise<RoomResult | null> {
    const now = performance.now()
    for (const [id, entry] of this.pending) {
      if (entry.expiresAt > now) break // constant offset ⇒ insertion order is deadline order; the rest are younger
      this.pending.delete(id)
      entry.resolve(null)
    }
    return new Promise((resolve) => this.pending.set(ackId, { expiresAt: now + ROOM_DM_ACK_TIMEOUT_MS, resolve }))
  }

  settle(ackId: string, reply: RoomResult): void {
    const entry = this.pending.get(ackId)
    // The sender drops a reply after its timeout.
    if (!entry) return
    this.pending.delete(ackId)
    entry.resolve(reply)
  }

  drop(): void {
    for (const entry of this.pending.values()) entry.resolve(null)
    this.pending.clear()
  }
}

function roomResult(work: Promise<unknown>): Promise<RoomResult> {
  return work.then(
    (value): RoomResult => ({ ok: true, value }),
    (err: unknown) => toRoomFailure(err, reportRoomError, ROOM_BUG_MESSAGE),
  )
}

/** Runs a stub's handling of what its page sent: a malformed message ends the page's wire and fails its request. */
function receiveFromPage<T>(end: ServerBroadcastEnd, handle: () => T): T | Promise<RoomFailure> {
  try {
    return handle()
  } catch (err) {
    if (!isProtocolViolationError(err)) throw err
    end.terminateWire()
    return Promise.resolve({ ok: false, kind: 'error', message: err.message })
  }
}

type RoomStubOptions = {
  room: RoomHandle
  /** Whether every client of the view was sent every member it holds. */
  roster: Pick<RosterStream, 'settled'>
  end: ServerBroadcastEnd
  publishShield?: ShieldValidator
  grants: ResponseRoomGrants
}

/** One client's view of a server room: it relays what the client wants and acts for the members the client joined. */
class RoomStub implements LaneHolder, BroadcastEndOwner, TailHoldOwner {
  /** Its channel to its client. */
  readonly end: ServerBroadcastEnd
  private readonly room: RoomHandle
  private readonly roster: RoomStubOptions['roster']
  private readonly publishShield: ShieldValidator | undefined
  private readonly members = new Set<string>()
  /** Members whose own messages this client doesn't get back:
   *  its selfDelivery: false joins and co-returned server joins. */
  private readonly selfSuppressed: Set<string>
  /** Hidden members this response handed the client: their events are relayed to it alone. */
  private readonly grantedHidden: Set<string>
  private readonly dmReplies = new PendingDmReplies()
  readonly replay = new ReplayGate()
  private wantsText = false
  private readonly textMemberWants = new Set<string>()
  private announce = false
  private readonly binary: BinaryWants = emptyBinaryWants()
  /** The tracks `binary` names, each `(member or room, track)` pair once. */
  private trackWants = 0
  /** A tail waits for the client's first text selector, then flushes once in order. */
  private tail: TailHold | null = null
  /** Whether this client was sent the whole roster since it attached. */
  rosterSent = false

  constructor({ room, roster, end, publishShield, grants }: RoomStubOptions) {
    this.room = room
    this.roster = roster
    this.publishShield = publishShield
    this.selfSuppressed = grants.selfSuppressed
    this.grantedHidden = grants.hidden
    this.end = end
    end.setOwner(this)
    end.listen((message) => receiveFromPage(end, () => this.receive(message)))
    end.onClose(() => {
      this.dmReplies.drop()
      room.detachStub(this)
    })
    end.onDisconnect(() => room.setHeldConnected(this, false))
    end.onReconnect(() => room.setHeldConnected(this, true))
    grants.onSuppressed((member) => room.onStubSuppressed(this, member))
  }

  // Client publishes, subscriptions and attaches

  _onPagePublish(text: string): Promise<RoomResult> {
    return receiveFromPage(this.end, () => roomResult(this.publishText(decodeRoomPublish(parsePeerText(text)))))
  }

  _onPagePublishBinary(framed: Uint8Array): Promise<RoomResult> {
    return receiveFromPage(this.end, () => roomResult(this.publishBinary(decodeStubBinaryFrame(framed), framed)))
  }

  /** Control always flows; text follows broadcast/member wants, while binary uses `sub-binary`. */
  _onPageSubscription(kind: BroadcastKind, on: boolean): void {
    if (kind === 'binary' || on === this.wantsText) return
    const before = on ? { all: this.wantsText, members: new Map<string, boolean>() } : null
    this.wantsText = on
    if (!on) this.replay.forgetUnwantedText((member) => this.wantsTextFrom(member))
    // The tail flush precedes the retained back-fill, so the replay dedupes against what the flush relayed.
    if (on) this.flushTail()
    this.room.onHolderTextChanged(this, before)
  }

  _onPageAttach(): void {
    this.rosterSent = false
    this.room.replica.onStubAttached(this)
  }

  // Client requests

  /** A request, sent with an ack, answers with its result; a declaration, sent without one, applies at once. */
  private receive(message: unknown): Promise<RoomResult> | undefined {
    if (isRoomRequest(message)) return roomResult(this.handleRequest(decodeRoomRequest(message)))
    const declaration = decodeRoomDeclaration(message)
    if (declaration.__r === 'sub-binary') this.assertTrackWantFits(declaration)
    this.applyDeclaration(declaration)
    return undefined
  }

  async handleRequest(req: RoomRequest): Promise<unknown> {
    const room = this.room
    switch (req.__r) {
      case 'req-join':
        return await room.joinStubMember(this, req)
      case 'req-leave':
        await room.removeMember(this.requireMember(req.id), { type: 'left' })
        return undefined
      case 'req-set-meta':
        return await room.writes.setMemberMeta(this.requireMember(req.id), req.meta)
      case 'req-set-attrs':
        return await room.writes.mergeMemberMeta(this.requireMember(req.id), req.attrs)
      case 'req-dm':
        return await room.writes.sendDm(this.requireMember(req.id), req.to, req.data, req.ack === true)
    }
  }

  private applyDeclaration(declaration: RoomDeclaration): void {
    switch (declaration.__r) {
      case 'sub-binary':
        return this.declareBinaryWant(declaration)
      case 'sub-text':
        return this.declareTextWants(declaration.members, declaration.announce)
      case 'dm-reply':
        return this.dmReplies.settle(declaration.ackId, declaration.reply)
    }
  }

  /** The client library names at most `ROOM_TRACK_WANTS_MAX` tracks, so only a peer bypassing it names more. */
  private assertTrackWantFits({ member, track, on }: BinaryWantChange): void {
    if (!on || track === EVERY_TRACK || this.trackWants < ROOM_TRACK_WANTS_MAX) return
    const scope = member === EVERY_MEMBER ? this.binary.everyMember : this.binary.members.get(member)
    assertProtocol(scope?.tracks.has(track), 'Room binary wants over ROOM_TRACK_WANTS_MAX')
  }

  /** A client learns its members from this view alone, so a want naming any other id names one that
   *  left or never was. */
  private declareBinaryWant(change: BinaryWantChange): void {
    const { member, track, on } = change
    if (on && member !== EVERY_MEMBER && !this.room.model.hasMember(member)) return
    if (!applyBinaryWantChange(this.binary, change)) return
    if (track !== EVERY_TRACK) this.trackWants += on ? 1 : -1
    if (!on) this.replay.forgetUnwantedBinary((member, track) => this.wantsBinary(member, track))
    this.room.onHolderBinaryChanged(this, change)
  }

  private declareTextWants(changes: Record<string, boolean>, announce: boolean): void {
    const members = new Map<string, boolean>()
    for (const [id, wanted] of Object.entries(changes)) {
      if (wanted === this.textMemberWants.has(id) || (wanted && !this.room.model.hasMember(id))) continue
      members.set(id, !wanted)
      toggle(this.textMemberWants, id, wanted)
    }
    this.replay.forgetUnwantedText((member) => this.wantsTextFrom(member))
    this.announce = announce
    this.flushTail()
    this.room.onHolderTextChanged(this, { all: this.wantsText, members })
  }

  private async publishText(publish: RoomDataPublish): Promise<ChannelPublishAck> {
    this.requireMember(publish.from)
    assertPublishShield(this.publishShield, publish.data)
    return await this.room.writes.publishText(publish.from, publish.data, publish.retain)
  }

  private async publishBinary(frame: BinaryFrame, framed: Uint8Array): Promise<ChannelPublishAck> {
    this.requireMember(frame.from)
    return await this.room.writes.publishBinaryFrame(frame, framed)
  }

  // Membership

  holds(id: string): boolean {
    return this.members.has(id)
  }

  heldMembers(): IterableIterator<string> {
    return this.members.values()
  }

  private requireMember(id: string): string {
    if (!this.members.has(id)) throw new RoomError('Not a participant of this room (joined through this connection)')
    return id
  }

  /** Takes the member its client's `req-join` (`reqId`) admitted, and tells the client before any event naming it; past
   *  `ROOM_VIEW_MEMBERS_MAX`, throws instead, before the join commits. */
  addMember(reqId: number, id: string, selfDelivery: boolean): void {
    if (this.members.size === ROOM_VIEW_MEMBERS_MAX) {
      throw new RoomError(`A room view joins at most ${ROOM_VIEW_MEMBERS_MAX} participants`)
    }
    this.members.add(id)
    if (!selfDelivery) this.selfSuppressed.add(id)
    this.end.deliver(unorderedEventText({ __r: 'joined', reqId, id }))
  }

  forgetMember(id: string): void {
    this.members.delete(id)
    this.selfSuppressed.delete(id)
    this.replay.forgetMember(id)
  }

  /** Drops its wants naming `id`; returns whether it held any. */
  forgetWantsOf(id: string): boolean {
    const text = this.textMemberWants.delete(id)
    this.trackWants -= this.binary.members.get(id)?.tracks.size ?? 0
    return this.binary.members.delete(id) || text
  }

  // Wants, as the room's subscription planner reads them

  get binaryWants(): BinaryWants {
    return this.binary
  }

  get wantsAnnounce(): boolean {
    return this.announce
  }

  /** A tail-pending stub ingests all text so its hold captures the whole recent tail; its selector applies at flush. */
  textDemand(): MemberWants {
    return { all: this.wantsText || this.tail !== null, members: this.textMemberWants }
  }

  wantsTextFrom(memberId: string): boolean {
    return !this.selfSuppressed.has(memberId) && (this.wantsText || this.textMemberWants.has(memberId))
  }

  wantsBinary(memberId: string, track: string): boolean {
    return !this.selfSuppressed.has(memberId) && binaryWantsCovers(this.binary, memberId, track)
  }

  suppressedMembers(): ReadonlySet<string> {
    return this.selfSuppressed
  }

  // Relays

  /** An event this instance originates for this client alone: state each attach sends again. */
  relayEvent(event: RoomRosterEvent | RoomDemandEvent | Extract<RoomCtrlEnvelope, { __r: 'update' }>): void {
    this.end.deliver(unorderedEventText(event), { resentOnAttach: true })
  }

  /** `relayEvent` with the event's text already made, so one text can go to many clients. */
  relayEventText(text: string): void {
    this.end.deliver(text, { resentOnAttach: true })
  }

  /** A roster part's members this client gets: `visible` itself unless the part holds a hidden member this client was
   *  handed, whose meta it heals too. */
  rosterMembers(part: MemberSnapshot[], visible: MemberSnapshot[]): MemberSnapshot[] {
    if (visible.length === part.length || this.grantedHidden.size === 0) return visible
    const members = part.filter(({ id, hidden }) => !hidden || this.grantedHidden.has(id))
    return members.length === visible.length ? visible : members
  }

  /** A hidden member's events reach only the clients that were handed it. */
  relayControl(wireText: string, hiddenMember: string | null, opts: { resentOnAttach: boolean }): void {
    if (hiddenMember === null || this.grantedHidden.has(hiddenMember)) this.end.deliver(wireText, opts)
  }

  relayAnnouncement(wireText: string, ord: OrderingInfo): void {
    if (this.announce) this.relayTextLive(ANNOUNCE_KEY, wireText, ord)
  }

  relayText(serialized: string, texts: MemberMessageTexts, from: string, ord: OrderingInfo): void {
    if (this.tail !== null) {
      this.tail.push({ serialized, ord, from })
    } else if (this.wantsTextFrom(from)) {
      const replay = this.replay
      if (replay.holding) {
        const relay = () => this.relayText(serialized, texts, from, ord)
        if (replay.held(from, null, heldTextWeight(serialized), relay)) return
      }
      this.relayTextLive(from, this.namesSender(from) ? texts.unstamped() : texts.stamped(), ord)
    }
  }

  relayBinary(wireData: Uint8Array, from: string, track: string, info: OrderingInfo): void {
    if (!this.wantsBinary(from, track)) return
    const replay = this.replay
    if (
      replay.holding &&
      replay.held(from, track, heldWeight(wireData), () => this.relayBinary(wireData, from, track, info))
    ) {
      return
    }
    if (replay.admitBinary(from, track, info)) this.end.deliverBinary(wireData)
  }

  hold(change: BinaryWantChange | null): ReplayHold {
    return this.replay.hold(change)
  }

  relayDm(texts: MemberMessageTexts, { from, fromMetaSeq, ackId }: RoomDmEnvelope): void {
    if (ackId) this.awaitDmReply(ackId, from)
    this.end.deliver(fromMetaSeq !== undefined && this.namesSender(from) ? texts.unstamped() : texts.stamped())
  }

  /** Whether this client names `member` as this view does: it got the whole roster since it attached, and every event
   *  after it, none dropped while it was away. A hidden member's grant may reach the client after a message does. */
  private namesSender(member: string): boolean {
    return (
      this.end.isAttached && this.rosterSent && this.roster.settled && this.room.model.memberHidden(member) === false
    )
  }

  emitRetainedText(event: RoomDataEnvelope, info: OrderingInfo, serialized: string, hold: ReplayHold): void {
    if (this.replay.admitRetained(hold, event.from, null, info)) this.end.deliver(encodePublish(serialized, info))
  }

  emitRetainedBinary(frame: BinaryFrame, info: OrderingInfo, framed: Uint8Array, hold: ReplayHold): void {
    if (this.replay.admitRetained(hold, frame.from, frame.track, info)) {
      this.end.deliverBinary(encodePublish(framed, info))
    }
  }

  private relayTextLive(key: string, wireText: string, ord: OrderingInfo): void {
    if (this.replay.admit(key, ord)) this.end.deliver(wireText)
  }

  // Ack-DM correlations

  /** A closing stub's close waits for the reply, as for the replies a participant stub forwards. */
  private awaitDmReply(ackId: string, sender: string): void {
    void this.end
      .trackAck(
        this.dmReplies.await(ackId).then((reply) => reply && this.room.writes.publishDmAck(sender, ackId, reply)),
      )
      .catch(reportRoomError)
  }

  // Tail

  /** Seeded from the room's pre-attach hold, with a fresh lease; expiry drops the hold and lets
   *  the room release ingestion. */
  beginTail(seed: TailEntry[]): void {
    this.tail = new TailHold({ owner: this, seed })
  }

  /** Its expiry ends only this stub's want of all text. */
  tailExpired(): void {
    this.tail = null
    this.room.onHolderTextChanged(this, null)
  }

  /** The first real text want flushes the tail in order through the replay gate. */
  private flushTail(): void {
    if (this.tail === null) return
    if (!this.wantsText && this.textMemberWants.size === 0) return // keep holding until a real want
    const held = this.tail.take()
    this.tail = null
    for (const { serialized, ord, from } of held) {
      if (!this.wantsTextFrom(from)) continue
      this.relayTextLive(from, encodePublish(serialized, ord), ord)
    }
  }

  endTail(): void {
    this.tail?.end()
    this.tail = null
  }
}

type RoomParticipantStubOptions = {
  participant: ServerLocalParticipant
  end: ServerBroadcastEnd
  publishShield?: ShieldValidator
}

/** One client's hold on a server participant: its requests act as that participant, whose inbox, demand, meta and leave
 *  flow back to the client. */
class RoomParticipantStub implements BroadcastEndOwner {
  /** Its channel to its client. */
  private readonly end: ServerBroadcastEnd
  private readonly participant: ServerLocalParticipant
  private readonly publishShield: ShieldValidator | undefined
  private readonly dmReplies = new PendingDmReplies()

  constructor({ participant, end, publishShield }: RoomParticipantStubOptions) {
    this.participant = participant
    this.publishShield = publishShield
    this.end = end
    end.setOwner(this)
    end.listen((message) => receiveFromPage(end, () => this.receive(message)))
    this.mirrorParticipant()
  }

  /** Its client publishes text as a `req-publish` request. */
  _onPagePublish(): Promise<RoomResult> {
    return receiveFromPage(this.end, () => {
      throw new ProtocolViolationError('text publish on a Room participant stub')
    })
  }

  _onPagePublishBinary(framed: Uint8Array): Promise<RoomResult> {
    const participant = this.participant
    return receiveFromPage(this.end, () =>
      roomResult(participant._publishFrame(decodeParticipantFrame(framed, participant.id), framed)),
    )
  }

  _onPageAttach(): void {
    this.notify({ __r: 'p-meta', ...this.participant._acceptedMeta })
    this.notify({ __r: 'demand-state', tracks: this.participant._demandedTracks })
  }

  /** A request, sent with an ack, answers with its result; an ack DM's reply, sent without one, settles that DM. */
  private receive(message: unknown): Promise<RoomResult> | undefined {
    if (isRoomRequest(message)) return roomResult(this.handleRequest(decodeParticipantRequest(message)))
    const { ackId, reply } = decodeParticipantDeclaration(message)
    this.dmReplies.settle(ackId, reply)
    return undefined
  }

  private async handleRequest(req: ParticipantStubRequest): Promise<unknown> {
    const participant = this.participant
    switch (req.__r) {
      case 'req-publish':
        assertPublishShield(this.publishShield, req.data)
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

  private mirrorParticipant(): void {
    const participant = this.participant
    const unlistenMeta = participant._onMeta({
      accepted: (accepted) => this.notify({ __r: 'p-meta', ...accepted }),
      rewound: () => this.notify({ __r: 'rewound' }),
    })

    const unlistenDemand = participant.onDemand((track, wanted) =>
      this.notify({ __r: 'demand', track: track ?? DEFAULT_TRACK, wanted }),
    )

    let left = false
    const unlistenLeave = participant.onLeave((cause) => {
      left = true
      this.notify({ __r: 'left', ...leaveCauseToWire(cause) })
      // A client back while its channel still holds it gets its leave.
      void this.end.close({ timeout: getChannelLimits().reconnectWindow }).catch(() => {})
    })

    this.end.onDisconnect(() => participant._setHolderConnected(false))
    this.end.onReconnect(() => participant._setHolderConnected(true))
    this.end.onClose(() => {
      unlistenMeta()
      unlistenDemand()
      unlistenLeave()
      this.dmReplies.drop()
      if (!left) void participant._releaseHolder()
    })
  }

  /** Its participant's inbox forwards here. */
  deliver(msg: InboxMessage): void {
    this.notify(dmNotice(msg))
  }

  /** A closed stub's client left with the participant; a closing stub's close waits for the replies to DMs it
   *  forwarded. */
  deliverAck(msg: InboxMessage): Promise<RoomResult> {
    if (this.end.isClosed) return Promise.resolve(DM_FAILURE.left)
    const reply = this.dmReplies.await(msg.ackId!)
    this.notify(dmNotice(msg))
    return this.end.trackAck(reply.then((answer) => answer ?? DM_FAILURE.left))
  }

  /** Its close waits for the reply's commit, as a room stub's does (`awaitDmReply`). */
  trackReply(replied: Promise<void>): void {
    if (!this.end.isClosed) void this.end.trackAck(replied).catch(() => {})
  }

  /** A closed stub's client gets no notice: one sent after an overflow ended it would arrive after a gap. */
  private notify(notice: ParticipantStubNotice): void {
    if (this.end.isClosed) return
    // Each attach sends the meta and the demand again.
    const resentOnAttach = notice.__r === 'p-meta' || notice.__r === 'demand' || notice.__r === 'demand-state'
    this.end.deliver(unorderedEventText(notice), { resentOnAttach })
  }
}

/** The publish shield validates a page's Room data at ingress only. */
function assertPublishShield(validate: ShieldValidator | undefined, data: unknown): void {
  if (!validate) return
  const result = validate(data)
  if (result !== true) throw new ShieldValidationError(result)
}
