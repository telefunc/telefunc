export { RoomStub }
export type { RoomStubOptions }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { ROOM_TRACK_WANTS_MAX, ROOM_VIEW_MEMBERS_MAX } from '../shared/constants.js'
import {
  assertProtocol,
  parsePeerText,
  type PublishBody,
  type BroadcastEndOwner,
  type BroadcastKind,
  type ServerBroadcastEnd,
  type OrderingInfo,
  type ChannelPublishAck,
} from 'telefunc/__internal'
import type { ShieldValidator } from 'telefunc'
import type { RoomHandle } from './RoomHandle.js'
import type { RosterStream } from './lifecycle/RosterStream.js'
import { reportRoomError } from './reportRoomError.js'
import {
  decodeRoomDeclaration,
  decodeRoomWantParts,
  decodeRoomPublish,
  decodeRoomRequest,
  type RoomRequest,
  decodeStubBinaryFrame,
  pageFrameMessage,
  isRoomRequest,
} from './requests.js'
import { ReplayGate, heldTextWeight, heldWeight, type LaneHolder, type ReplayHold } from './lifecycle/replay.js'
import { TailHold, type TailEntry, type TailHoldOwner } from './lifecycle/TailHold.js'
import { EVERY_MEMBER, EVERY_TRACK, type BinaryFrame, type BinaryWantChange } from '../shared/binary.js'
import {
  applyBinaryWantChange,
  binaryWantChangesTo,
  binaryWantsCovers,
  emptyBinaryWants,
  forgetMemberWants,
  type BinaryWants,
} from './lanes/binaryWants.js'
import { RoomError } from '../shared/errors.js'
import {
  leaveCauseToWire,
  type MemberSnapshot,
  type MemberWants,
  type RoomCtrlEnvelope,
  type RoomDataPublish,
  type RoomDemandEvent,
  type RoomDmEnvelope,
  type RoomResult,
  type RoomRosterEvent,
} from '../shared/protocol.js'
import type { RoomDataEnvelope } from './protocol.js'
import type { LeaveCause } from '../types.js'
import { toggle } from '../utils/sets.js'
import { MemberMessageBodies, orderedBody, trackFrameBody, unorderedEventBody } from './publishBodies.js'
import {
  PendingDmReplies,
  assertPublishShield,
  closeAfterReconnectWindow,
  receiveFromPage,
  roomResult,
} from './pageRequests.js'
assertIsNotBrowser()

type RoomStubOptions = {
  room: RoomHandle
  /** Whether every client of the view was sent every member it holds. */
  roster: RosterStream
  end: ServerBroadcastEnd
  publishShield?: ShieldValidator
  /** The hidden members its response handed the client, which a streamed response may add to after the stub
   *  attached. */
  grantedHidden: ReadonlySet<string>
}

/** One client's view of a server room: it relays what the client wants and acts for the members the client joined. */
class RoomStub implements LaneHolder, BroadcastEndOwner, TailHoldOwner {
  /** Its channel to its client. */
  readonly end: ServerBroadcastEnd
  private readonly room: RoomHandle
  private readonly roster: RosterStream
  private readonly publishShield: ShieldValidator | undefined
  /** The members its client joined, each with its `selfDelivery`. */
  private readonly members = new Map<string, boolean>()
  /** The members whose own messages its page doesn't get back (`PageIndex`). */
  private suppressed: ReadonlySet<string> = new Set()
  /** Hidden members its response handed the client: their events are relayed to it alone. */
  private readonly grantedHidden: ReadonlySet<string>
  private readonly dmReplies = new PendingDmReplies()
  /** The replies to DMs its page got, until each is out. */
  private readonly replying = new Set<Promise<void>>()
  readonly replay = new ReplayGate()
  private wantsText = false
  private readonly textMemberWants = new Set<string>()
  private readonly binary: BinaryWants = emptyBinaryWants()
  /** A tail waits for the client's first text selector, then flushes once in order. */
  private tail: TailHold | null = null
  /** Whether its client attached, before which its tail holds. */
  private attached = false

  constructor({ room, roster, end, publishShield, grantedHidden }: RoomStubOptions) {
    this.room = room
    this.roster = roster
    this.publishShield = publishShield
    this.grantedHidden = grantedHidden
    this.end = end
    end.setOwner(this)
    end.listen((message) => receiveFromPage(end, () => this.receive(message)))
    end.onClose(() => {
      this.dmReplies.drop()
      // Its members leave once the replies are out, so each sender learns that its recipient went away, not that it left.
      if (this.replying.size === 0) return room.detachStub(this)
      void Promise.allSettled(this.replying).then(() => room.detachStub(this))
    })
    end.onDisconnect(() => room.setHeldConnected(this, false))
    end.onReconnect(() => room.setHeldConnected(this, true))
  }

  // Client publishes, subscriptions and attaches

  _onPagePublish(text: string): Promise<RoomResult> {
    return receiveFromPage(this.end, () => roomResult(this.publishText(decodeRoomPublish(parsePeerText(text)))))
  }

  _onPagePublishBinary(framed: Uint8Array): Promise<RoomResult> {
    return receiveFromPage(this.end, () => {
      const frame = decodeStubBinaryFrame(framed)
      return roomResult(this.publishBinary(frame, framed, pageFrameMessage(frame)))
    })
  }

  /** Control always flows; text follows the room-wide want and the members' (`_onPageWantParts`), binary the pairs'. */
  _onPageSubscription(kind: BroadcastKind, on: boolean): void {
    if (kind === 'binary' || on === this.wantsText) return
    const before = on ? { all: this.wantsText, members: new Map<string, boolean>() } : null
    this.wantsText = on
    if (!on) this.replay.forgetUnwantedText((member) => this.wantsTextFrom(member))
    // The tail flush precedes the retained back-fill, so the replay dedupes against what the flush relayed.
    if (on) this.flushTail()
    this.room.onHolderTextChanged(this, before)
  }

  /** The members and pairs the page wants, as its want declares them whole, with the room-wide text before them: applied
   *  where they differ from what it holds. */
  _onPageWantParts(parts: unknown): void {
    const { members, tracks } = decodeRoomWantParts(parts)
    const texts = new Map<string, boolean>()
    for (const id of this.textMemberWants) if (!members.has(id)) texts.set(id, false)
    for (const id of members) if (!this.textMemberWants.has(id)) texts.set(id, true)
    if (texts.size > 0) this.declareTextWants(texts)
    for (const change of binaryWantChangesTo(this.binary, tracks)) this.declareBinaryWant(change)
  }

  _onPageAttach(): void {
    this.room.replica.onStubAttached(this)
    this.attached = true
    this.flushTail()
  }

  // Client requests

  /** A request, sent with an ack, answers with its result; a declaration, sent without one, applies at once. */
  private receive(message: unknown): Promise<RoomResult> | undefined {
    if (isRoomRequest(message)) return roomResult(this.handleRequest(decodeRoomRequest(message)))
    const { ackId, reply } = decodeRoomDeclaration(message)
    this.dmReplies.settle(ackId, reply)
    return undefined
  }

  async handleRequest(req: RoomRequest): Promise<unknown> {
    if (req.__r === 'req-join') return await this.room.joinStubMember(this, req)
    return await this.room.request(this.requireMember(req.id), req)
  }

  /** The client library names at most `ROOM_TRACK_WANTS_MAX` tracks, so only a peer bypassing it names more. */
  private assertTrackWantFits({ member, track, on }: BinaryWantChange): void {
    if (!on || track === EVERY_TRACK || this.binary.named < ROOM_TRACK_WANTS_MAX) return
    const scope = member === EVERY_MEMBER ? this.binary.everyMember : this.binary.members.get(member)
    assertProtocol(scope?.tracks.has(track), 'Room binary wants over ROOM_TRACK_WANTS_MAX')
  }

  /** A client learns its members from this view alone, so a want naming any other id names one that
   *  left or never was. */
  private declareBinaryWant(change: BinaryWantChange): void {
    this.assertTrackWantFits(change)
    const { member, on } = change
    if (on && member !== EVERY_MEMBER && !this.room.model.hasMember(member)) return
    if (!applyBinaryWantChange(this.binary, change)) return
    if (!on) this.replay.forgetUnwantedBinary((member, track) => this.wantsBinary(member, track))
    this.room.onHolderBinaryChanged(this, change)
  }

  private declareTextWants(changes: Map<string, boolean>): void {
    const members = new Map<string, boolean>()
    for (const [id, wanted] of changes) {
      if (wanted === this.textMemberWants.has(id) || (wanted && !this.room.model.hasMember(id))) continue
      members.set(id, !wanted)
      toggle(this.textMemberWants, id, wanted)
    }
    this.replay.forgetUnwantedText((member) => this.wantsTextFrom(member))
    this.flushTail()
    this.room.onHolderTextChanged(this, { all: this.wantsText, members })
  }

  private async publishText(publish: RoomDataPublish): Promise<ChannelPublishAck> {
    this.requireMember(publish.from)
    assertPublishShield(this.publishShield, publish.data)
    return await this.room.writes.publishText(publish.from, publish.data, publish.retain)
  }

  private async publishBinary(frame: BinaryFrame, framed: Uint8Array, message: unknown): Promise<ChannelPublishAck> {
    this.requireMember(frame.from)
    if (frame.text) assertPublishShield(this.publishShield, message)
    return await this.room.writes.publishBinaryFrame(frame, framed, message)
  }

  // Membership

  holds(id: string): boolean {
    return this.members.has(id)
  }

  heldMembers(): IterableIterator<string> {
    return this.members.keys()
  }

  *selfSuppressed(): Generator<string> {
    for (const [id, selfDelivery] of this.members) if (!selfDelivery) yield id
  }

  suppresses(member: string): boolean {
    return this.members.get(member) === false
  }

  atPage(suppressed: ReadonlySet<string>): void {
    this.suppressed = suppressed
  }

  /** The view closes with the removal's cause, and relays nothing from now on. */
  pageRemoved(cause: LeaveCause): void {
    this.end.deliver(unorderedEventBody({ __r: 'closed', ...leaveCauseToWire(cause) }))
    this.room.detachStub(this)
    closeAfterReconnectWindow(this.end)
  }

  private requireMember(id: string): string {
    if (!this.members.has(id))
      throw new RoomError('NOT_A_PARTICIPANT', 'Not a participant of this room (joined through this connection)')
    return id
  }

  /** Takes the member its client's `req-join` (`reqId`) admitted, and tells the client before any event naming it; past
   *  `ROOM_VIEW_MEMBERS_MAX`, throws instead, before the join commits. */
  addMember(reqId: number, id: string, selfDelivery: boolean): void {
    if (this.members.size === ROOM_VIEW_MEMBERS_MAX) {
      throw new RoomError('LIMIT_REACHED', `A room view joins at most ${ROOM_VIEW_MEMBERS_MAX} participants`)
    }
    this.members.set(id, selfDelivery)
    this.end.deliver(unorderedEventBody({ __r: 'joined', reqId, id }))
  }

  /** Returns whether it held the member. */
  forgetMember(id: string): boolean {
    this.replay.forgetMember(id)
    return this.members.delete(id)
  }

  /** Drops its wants naming `id`; returns whether it held any. */
  forgetWantsOf(id: string): boolean {
    const text = this.textMemberWants.delete(id)
    return forgetMemberWants(this.binary, id) || text
  }

  // Wants, as the room's subscription planner reads them

  get binaryWants(): BinaryWants {
    return this.binary
  }

  /** A tail-pending stub ingests all text so its hold captures the whole recent tail; its selector applies at flush. */
  textDemand(): MemberWants {
    return { all: this.wantsText || this.tail !== null, members: this.textMemberWants }
  }

  wantsTextFrom(memberId: string): boolean {
    return !this.suppressed.has(memberId) && (this.wantsText || this.textMemberWants.has(memberId))
  }

  wantsBinary(memberId: string, track: string): boolean {
    return !this.suppressed.has(memberId) && binaryWantsCovers(this.binary, memberId, track)
  }

  suppressedMembers(): ReadonlySet<string> {
    return this.suppressed
  }

  // Relays

  /** An event this instance originates for this client alone. */
  relayEvent(event: RoomRosterEvent | RoomDemandEvent | Extract<RoomCtrlEnvelope, { __r: 'update' }>): void {
    this.end.deliver(unorderedEventBody(event))
  }

  /** `relayEvent` with the event's body already made, so one body can go to many clients. */
  relayEventBody(body: PublishBody): void {
    this.end.deliver(body)
  }

  /** A roster part's members this client gets: `visible` itself unless the part holds a hidden member this client was
   *  handed, whose meta it heals too. */
  rosterMembers(part: MemberSnapshot[], visible: MemberSnapshot[]): MemberSnapshot[] {
    if (visible.length === part.length || this.grantedHidden.size === 0) return visible
    const members = part.filter(({ id, hidden }) => !hidden || this.grantedHidden.has(id))
    return members.length === visible.length ? visible : members
  }

  /** A hidden member's events reach only the clients that were handed it. */
  relayControl(body: PublishBody, hiddenMember: string | null): void {
    if (hiddenMember === null || this.grantedHidden.has(hiddenMember)) this.end.deliver(body)
  }

  relayText(serialized: string, bodies: MemberMessageBodies, from: string, ord: OrderingInfo): void {
    if (this.tail !== null) return this.tail.push({ serialized, ord, from })
    if (ord.rewound) this.replay.rewound(null, '', ord.seq)
    if (this.wantsTextFrom(from)) {
      const replay = this.replay
      if (replay.holding) {
        const relay = () => this.relayText(serialized, bodies, from, ord)
        if (replay.held(from, null, heldTextWeight(serialized), relay)) return
      }
      this.relayTextLive(from, this.namesSender(from) ? bodies.unstamped() : bodies.stamped(), ord)
    }
  }

  relayBinary(body: PublishBody, from: string, track: string, info: OrderingInfo): void {
    if (info.rewound) this.replay.rewound(from, track, info.seq)
    if (!this.wantsBinary(from, track)) return
    const replay = this.replay
    if (
      replay.holding &&
      replay.held(from, track, heldWeight(body.frame), () => this.relayBinary(body, from, track, info))
    ) {
      return
    }
    if (replay.admitBinary(from, track, info)) this.end.deliver(body)
  }

  hold(change: BinaryWantChange | null): ReplayHold {
    return this.replay.hold(change)
  }

  relayDm(bodies: MemberMessageBodies, { from, ackId }: RoomDmEnvelope): void {
    if (ackId) this.awaitDmReply(ackId, from)
    this.end.deliver(this.namesSender(from) ? bodies.unstamped() : bodies.stamped())
  }

  /** Whether this client names `member` as this view does: it got the whole roster since it attached, and every event
   *  after it, none dropped while it was away. A hidden member's grant may reach the client after a message does. */
  private namesSender(member: string): boolean {
    return (
      this.end.isAttached &&
      this.roster.sentTo(this) &&
      this.roster.settled &&
      this.room.model.memberHidden(member) === false
    )
  }

  emitRetainedText(event: RoomDataEnvelope, info: OrderingInfo, serialized: string, hold: ReplayHold): void {
    if (this.replay.admitRetained(hold, event.from, null, info)) {
      this.end.deliver(orderedBody(serialized, info))
    }
  }

  emitRetainedBinary(frame: BinaryFrame, info: OrderingInfo, framed: Uint8Array, hold: ReplayHold): void {
    if (this.replay.admitRetained(hold, frame.from, frame.track, info)) {
      this.end.deliver(trackFrameBody(frame, framed, info))
    }
  }

  private relayTextLive(key: string, body: PublishBody, ord: OrderingInfo): void {
    if (this.replay.admit(key, ord)) this.end.deliver(body)
  }

  // Ack-DM correlations

  /** A closing stub's close waits for the reply, as for the replies a participant stub forwards. */
  private awaitDmReply(ackId: string, sender: string): void {
    const replied = this.publishDmReply(ackId, sender)
    this.replying.add(replied)
    void replied.finally(() => this.replying.delete(replied)).catch(() => {})
    void this.end.trackAck(replied).catch(reportRoomError)
  }

  private async publishDmReply(ackId: string, sender: string): Promise<void> {
    const reply = await this.dmReplies.await(ackId)
    if (reply) await this.room.writes.publishDmAck(sender, ackId, reply)
  }

  // Tail

  /** Seeded from the room's pre-attach hold, with a fresh lease; expiry drops the hold and lets
   *  the room release ingestion. */
  beginTail(seed: TailEntry[]): void {
    this.tail = new TailHold({ owner: this, seed })
  }

  get roomId(): string {
    return this.room.id
  }

  /** Its expiry ends only this stub's want of all text. */
  tailExpired(): void {
    this.tail = null
    this.room.onHolderTextChanged(this, null)
  }

  /** The first real text want flushes the tail in order through the replay gate, once its client attached. */
  private flushTail(): void {
    if (this.tail === null || !this.attached) return
    if (!this.wantsText && this.textMemberWants.size === 0) return // keep holding until a real want
    const held = this.tail.take()
    this.tail = null
    for (const { serialized, ord, from } of held) {
      if (ord.rewound) this.replay.rewound(null, '', ord.seq)
      if (!this.wantsTextFrom(from)) continue
      this.relayTextLive(from, orderedBody(serialized, ord), ord)
    }
  }

  endTail(): void {
    this.tail?.end()
    this.tail = null
  }
}
