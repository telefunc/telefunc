export { RoomStub, ResponseRoomGrants }
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
import type { RoomHandle } from './room.js'
import type { RosterStream } from './lifecycle/RosterStream.js'
import { reportRoomError } from './reportRoomError.js'
import {
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
import { EVERY_MEMBER, EVERY_TRACK, type BinaryFrame, type BinaryWantChange } from '../shared/binary.js'
import { applyBinaryWantChange, binaryWantsCovers, emptyBinaryWants, type BinaryWants } from './lanes/binaryWants.js'
import { RoomError } from '../shared/errors.js'
import type {
  MemberSnapshot,
  MemberWants,
  RoomCtrlEnvelope,
  RoomDataPublish,
  RoomDemandEvent,
  RoomDmEnvelope,
  RoomResult,
  RoomRosterEvent,
} from '../shared/protocol.js'
import type { RoomDataEnvelope } from './protocol.js'
import { toggle } from '../utils/sets.js'
import { MemberMessageBodies, orderedBody, trackFrameBody, unorderedEventBody } from './publishBodies.js'
import { PendingDmReplies, assertPublishShield, receiveFromPage, roomResult } from './pageRequests.js'
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
  readonly _whenBehind: 'missOldest' | 'close'
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
  private missedWarned = false

  constructor({ room, roster, end, publishShield, grants }: RoomStubOptions) {
    this.room = room
    this._whenBehind = room.whenBehind
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

  /** Its client sees each gap in a track's `info.seq`, and the server's log this once. */
  _onPublishesMissed(): void {
    if (this.missedWarned) return
    this.missedWarned = true
    console.warn(
      `[@telefunc/room][Warning] A client of room "${this.room.model.roomId}" fell behind what the server holds for it, and misses the oldest frames of its tracks: \`info.seq\` of each track shows its gap.`,
    )
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
    this.end.deliver(unorderedEventBody({ __r: 'joined', reqId, id }))
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

  relayAnnouncement(body: PublishBody, ord: OrderingInfo): void {
    if (this.announce) this.relayTextLive(ANNOUNCE_KEY, body, ord)
  }

  relayText(serialized: string, bodies: MemberMessageBodies, from: string, ord: OrderingInfo): void {
    if (this.tail !== null) {
      this.tail.push({ serialized, ord, from })
    } else if (this.wantsTextFrom(from)) {
      const replay = this.replay
      if (replay.holding) {
        const relay = () => this.relayText(serialized, bodies, from, ord)
        if (replay.held(from, null, heldTextWeight(serialized), relay)) return
      }
      this.relayTextLive(from, this.namesSender(from) ? bodies.unstamped() : bodies.stamped(), ord)
    }
  }

  relayBinary(body: PublishBody, from: string, track: string, info: OrderingInfo): void {
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

  relayDm(bodies: MemberMessageBodies, { from, fromMetaSeq, ackId }: RoomDmEnvelope): void {
    if (ackId) this.awaitDmReply(ackId, from)
    this.end.deliver(fromMetaSeq !== undefined && this.namesSender(from) ? bodies.unstamped() : bodies.stamped())
  }

  /** Whether this client names `member` as this view does: it got the whole roster since it attached, and every event
   *  after it, none dropped while it was away. A hidden member's grant may reach the client after a message does. */
  private namesSender(member: string): boolean {
    return (
      this.end.isAttached && this.rosterSent && this.roster.settled && this.room.model.memberHidden(member) === false
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
    void this.end.trackAck(this.publishDmReply(ackId, sender)).catch(reportRoomError)
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
      this.relayTextLive(from, orderedBody(serialized, ord), ord)
    }
  }

  endTail(): void {
    this.tail?.end()
    this.tail = null
  }
}
