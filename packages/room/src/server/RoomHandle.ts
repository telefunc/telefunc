export { RoomHandle }
export type { RoomHandleOptions }

import { type ChannelPublishAck, handleCallbackBug, type OrderingInfo } from 'telefunc/__internal'
import { assert, assertUsage } from '../utils/assert.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { decodeBinaryFrame, decodeFrameMessage, type BinaryWantChange } from '../shared/binary.js'
import { RoomError, roomClosedError } from '../shared/errors.js'
import { normalizeJoinOptions, ownMetadata } from '../shared/arguments.js'
import {
  inboxMessageFromWire,
  type SenderStamp,
  type RoomDmEnvelope,
  type MemberMetaUpdate,
  type MemberRequest,
  type RoomSnapshotMetadata,
  type DemandChange,
} from '../shared/protocol.js'
import type { RoomDataEnvelope, RoomIncarnation } from './protocol.js'
import { departureCause, type RoomModel } from '../shared/RoomModel.js'
import { RoomState, type RoomStateOwner, type WantChange } from '../shared/RoomState.js'
import { RoomRemotes } from '../shared/RoomView.js'
import { type MemberLink } from '../shared/ParticipantBase.js'
import { RoomStub, type RoomStubOptions } from './RoomStub.js'
import { MemberMessageBodies } from './publishBodies.js'
import type { RoomRequest } from './requests.js'
import { LocalHolder, type LaneHolder, type TextWantsChange } from './lifecycle/replay.js'
import { TailHold, type TailHoldOwner } from './lifecycle/TailHold.js'
import type { RoomReplica } from './RoomReplica.js'
import type { LanePlan } from './lanes/LanePlan.js'
import type { RoomDemand } from './lanes/RoomDemand.js'
import type { RosterStream } from './lifecycle/RosterStream.js'
import type { PendingDmAcks } from './PendingDmAcks.js'
import type { RoomHosting } from './lifecycle/RoomHosting.js'
import { RoomWrites, type RoomWritesOwner } from './RoomWrites.js'
import { withinRoomHorizon } from './lanes/lanes.js'
import { closeAfterReconnectWindow } from './pageRequests.js'
import { reportRoomError } from './reportRoomError.js'
import { Admission } from './Admission.js'
import { Admissions, type AdmissionsOwner } from './lifecycle/admit.js'
import { evictMember } from './store/members.js'
import type { JoinOptions, LeaveCause, LocalParticipant } from '../types.js'
import type { RoomGuards } from './roomStatics.js'
import { ServerLocalParticipant } from './ServerLocalParticipant.js'
assertIsNotBrowser()

type RoomHandleOptions = {
  replica: RoomReplica
  /** Where the handle's holders' wants replan. */
  lanePlan: Pick<LanePlan, 'textChanged' | 'binaryChanged' | 'observing'>
  /** For the handle's stubs. */
  roster: Pick<RosterStream, 'settled' | 'sentTo'>
  /** For the handle's participants. */
  incomingDemand: Pick<RoomDemand, 'watchesOf'>
  /** For the handle's writes. */
  acks: PendingDmAcks
  /** Hosts the members the handle admits, and evicts again one whose eviction failed. */
  hosting: Pick<RoomHosting, 'lease' | 'evictLater'>
}

/** One `Room.get()`'s handle on a room: its guards, its tail, the stubs it opened, its participants and its listeners,
 *  over this process's one view of the room, which every handle to it shares. Each serialization opens a fresh stub. */
class RoomHandle implements RoomWritesOwner, TailHoldOwner, MemberLink, RoomStateOwner, AdmissionsOwner {
  /** Every authority check and member write carries this incarnation,
   *  rejecting stale handles after recreate. */
  readonly incarnation: RoomIncarnation
  /** What this process shares of the incarnation. */
  readonly replica: RoomReplica
  // What the handle is to the room: its tail, its stubs, its listeners and the members it owns (`localParticipants`,
  // `stubOf`). The replica derives the lanes' roles from them (`RoomReplica.reconsider()`), so each change is one
  // transition: the handle changes the collection, then tells the replica in the same turn, through `own()`,
  // `release()`, `attachStub()`, `detachStub()`, `tailChanged()` or `listenersChanged()`. A leave or a close the
  // replica applies runs the other way: the replica disowns the member, the handle forgets it (`memberLeft()`,
  // `teardown()`), and the replica reconsiders the handle.
  /** `Room.get({ tail: true })`'s hold until a stub attaches and takes it. */
  private tail: TailHold | null = null
  private declaredGuards: RoomGuards | null = null
  /** This handle's listeners over the replica's model. */
  readonly state: RoomState
  /** The app's participants of the room, which the user's `Room` and the handle's participants hand out. */
  readonly remotes: RoomRemotes
  readonly local: LocalHolder
  private readonly stubs = new Set<RoomStub>()
  /** The attached stub holding each member a client joined through. */
  private readonly stubOf = new Map<string, RoomStub>()
  readonly localParticipants = new Map<string, ServerLocalParticipant>()
  private readonly admissions: Admissions
  readonly writes: RoomWrites

  private readonly lanePlan: RoomHandleOptions['lanePlan']
  private readonly roster: RoomHandleOptions['roster']
  private readonly incomingDemand: RoomHandleOptions['incomingDemand']

  constructor({ replica, lanePlan, roster, incomingDemand, acks, hosting }: RoomHandleOptions) {
    this.replica = replica
    this.lanePlan = lanePlan
    this.roster = roster
    this.incomingDemand = incomingDemand
    this.incarnation = replica.incarnation
    this.state = new RoomState({ model: replica.model, owner: this })
    this.remotes = new RoomRemotes({ state: this.state, adder: 'app' })
    this.local = new LocalHolder({ state: this.state, participants: this.localParticipants })
    this.admissions = new Admissions({ owner: this, replica, lanePlan, hosting })
    this.writes = new RoomWrites({
      owner: this,
      incarnation: this.incarnation,
      model: replica.model,
      replica,
      acks,
      localParticipants: this.localParticipants,
    })
  }

  get model(): RoomModel {
    return this.replica.model
  }

  get id(): string {
    return this.incarnation.id
  }

  get guards(): RoomGuards | null {
    return this.declaredGuards
  }

  /** See `Room.guard()`. One declaration per instance keeps the grant declarative. */
  setGuards(guards: RoomGuards): void {
    assertUsage(
      this.declaredGuards === null,
      'Room.guard() was already called for this room instance: declare all guards in one call',
    )
    this.declaredGuards = guards
  }

  async join(options?: JoinOptions): Promise<LocalParticipant> {
    this.assertNotKnownClosed()
    const { meta, selfDelivery, identity, hidden } = normalizeJoinOptions(options)
    const admission = new Admission({ meta, identity, hidden })
    const participant = new ServerLocalParticipant({
      room: this,
      participant: { id: admission.id, meta, selfDelivery, identity },
      incomingDemand: this.incomingDemand,
    })
    await this.admissions.admit(admission, () => {
      this.localParticipants.set(admission.id, participant)
      this.replica.own(admission.id, this)
    })
    return participant
  }

  /** A member's request: from its participant here, from a page that joined through a stub, or from a page holding
   *  its participant. */
  async request(id: string, req: MemberRequest): Promise<unknown> {
    switch (req.__r) {
      case 'req-leave':
        return await this.removeMember(id, { type: 'left' })
      case 'req-set-meta':
        return await this.writes.setMemberMeta(id, req.meta)
      case 'req-set-attrs':
        return await this.writes.mergeMemberMeta(id, req.attrs)
      case 'req-dm':
        return await this.writes.sendDm(id, req.to, req.data, req.ack === true)
      case 'req-private':
        return await this.writes.sendPrivate(id, req.to, req.field, req.data)
    }
  }

  publishText(from: string, data: unknown, retain: boolean | undefined): Promise<ChannelPublishAck> {
    return this.writes.publishText(from, data, retain)
  }

  /** A frame of a participant here, decoded as a holder elsewhere decodes it. */
  publishFrame(framed: Uint8Array): Promise<ChannelPublishAck> {
    const frame = decodeBinaryFrame(framed)
    assert(frame !== null)
    return this.writes.publishBinaryFrame(frame, framed, frame.text ? decodeFrameMessage(frame) : frame.payload)
  }

  async removeMember(id: string, cause: LeaveCause): Promise<void> {
    if (this.model.closed) return // close() already removed everyone
    await evictMember(this.incarnation, id, cause)
    this.model.applyLeave(id, cause)
  }

  /** The member's holder is gone: ownership ends even if eviction fails, which the heartbeat then retries. */
  async removeDepartedMember(id: string): Promise<void> {
    if (this.model.closed) return // close() already removed everyone
    const cause = { type: 'disconnected' } as const
    await this.admissions.evictOrLater(id, cause)
    this.model.applyLeave(id, cause)
  }

  /** A room whose close hasn't reached this process fails at its join operation, with the same error. */
  private assertNotKnownClosed(): void {
    if (this.model.closed) throw roomClosedError(this.id)
  }

  /** What the lane carried for this handle's own listeners, or for its tail while no stub took it. */
  relayData(serialized: string, event: RoomDataEnvelope, info: OrderingInfo): void {
    this.local.relayText(event, info, serialized)
    if (this.stubs.size === 0) this.tail?.push({ serialized, ord: info, from: event.from })
  }

  /** A meta update of a member this handle owns, which its participant mirrors. */
  acceptMeta(id: string, accepted: MemberMetaUpdate): void {
    this.localParticipants.get(id)?._acceptMeta(accepted)
  }

  /** A failover rewound the meta seq of a member this handle owns. */
  metaRewound(id: string): void {
    this.localParticipants.get(id)?._metaRewound()
  }

  /** A member this handle owns left: its participant ends, and its admission, if under way, is abandoned. */
  memberLeft(id: string, cause: LeaveCause): void {
    const local = this.localParticipants.get(id)
    if (local) {
      this.localParticipants.delete(id)
      local._onLeft(cause)
    }
    this.stubOf.delete(id)
    this.admissions.abandon(id)
  }

  /** The room closed: the replica already applied and relayed `closed`. Its members leave as the page's do. */
  teardown(): void {
    const cause = this.model.closedCause
    assert(cause)
    this.teardownTail()
    for (const local of this.localParticipants.values()) local._onLeft(departureCause(cause))
    this.localParticipants.clear()
    this.admissions.abandonAll()
    for (const stub of this.stubs) closeAfterReconnectWindow(stub.end)
  }

  /** A DM for a member this handle owns, routed to its holder: a server participant or its client stub. */
  onDm(envelope: RoomDmEnvelope, serialized: string, info: OrderingInfo): void {
    const holder = this.holderOf(envelope.to)
    // A client that joined through a stub gets the DM relayed (its `ackId` rides along) and answers with `dm-reply`.
    if (holder instanceof RoomStub) {
      if (envelope.fromMetaSeq !== undefined) {
        this.replica.applyStamp(envelope.from, envelope as RoomDmEnvelope & SenderStamp)
      }
      const bodies = new MemberMessageBodies({ serialized, info, envelope })
      return holder.relayDm(bodies, envelope)
    }
    if (holder === undefined) return
    const msg = inboxMessageFromWire(envelope)
    const { ackId } = envelope
    if (ackId === undefined) return holder._deliverMessage(msg)
    // A server participant, or one a client holds (its forwarder answers): the reply goes to the sender's inbox, and a
    // client holding the participant closes once the reply is out, so its leave comes after it.
    const replied = holder
      ._deliverMessageAck(msg)
      .then((reply) => this.writes.publishDmAck(envelope.from, ackId, reply))
    holder._trackReply(replied)
    void replied.catch(reportRoomError)
  }

  /** Resolves once the tail receives, so it holds everything committed after it returns. */
  async startTail(): Promise<void> {
    this.tail = new TailHold({ owner: this })
    // Text ingestion comes up before any stub exists.
    await withinRoomHorizon(this.replica.tailChanged(this))
  }

  tailExpired(): void {
    this.teardownTail()
  }

  private teardownTail(): void {
    if (this.tail === null) return // already handed off to a stub
    this.tail.end()
    this.tail = null
    // The text ingestion nothing is consuming stops.
    void this.replica.tailChanged(this)
  }

  /** A client's view of this room, with its members, at the page `page` names (`PageIndex.place`). It attaches before
   *  the snapshot, so every later event relays and every earlier one is in the snapshot. */
  openStub({
    end,
    publishShield,
    grantedHidden,
    page,
  }: Omit<RoomStubOptions, 'room' | 'roster'> & { page: string }): RoomSnapshotMetadata {
    this.attachStub(new RoomStub({ room: this, roster: this.roster, end, publishShield, grantedHidden }), page)
    const model = this.model
    const members = model.visibleEntries().map((entry) => model.memberSnapshot(entry.id))
    return {
      channelId: end.id,
      roomId: this.id,
      meta: model.metaEntries(),
      closed: model.closed,
      metaSeq: model.metaSeq,
      members,
    }
  }

  private attachStub(stub: RoomStub, page: string): void {
    this.stubs.add(stub)
    if (this.tail !== null) {
      stub.beginTail(this.tail.take())
      this.tail = null
    }
    this.replica.attachStub(this, stub, page)
  }

  setHeldConnected(stub: RoomStub, connected: boolean): void {
    // A member whose join is under way is skipped: its record takes the wire's state when the join applies.
    for (const id of stub.heldMembers()) if (!this.admissions.has(id)) this.setMemberConnected(id, connected)
  }

  private setMemberConnected(id: string, connected: boolean): void {
    void this.writes.setMemberConnected(id, connected).catch(reportRoomError)
  }

  /** The stub's view ended, or its page's last participant of the room was removed: it relays nothing more. */
  detachStub(stub: RoomStub): void {
    if (!this.stubs.delete(stub)) return
    stub.endTail()
    stub.replay.dropHolds()
    this.replica.detachStub(this, stub)
    for (const id of stub.heldMembers()) {
      this.stubOf.delete(id)
      this.replica.release(id, this)
      if (this.admissions.abandon(id)) continue // the admission rolls itself back
      void this.removeDepartedMember(id)
    }
  }

  async joinStubMember(
    stub: RoomStub,
    req: Extract<RoomRequest, { __r: 'req-join' }>,
  ): Promise<{ id: string; joinedAt: number }> {
    // A page joins only where the telefunction that gave it the room decided who may.
    if (this.guards?.onBeforeJoin === undefined && this.guards?.anyoneCanJoin !== true) {
      throw new RoomError(
        `A page called join() on room ${this.id}, but the telefunction that returned the room doesn't say who may ` +
          'join: call Room.guard(room, { onBeforeJoin }) there to admit whom you choose, or ' +
          'Room.guard(room, { anyoneCanJoin: true }) to admit anyone',
      )
    }
    this.assertNotKnownClosed()
    const admission = new Admission({ meta: ownMetadata(req.meta), identity: null, hidden: false })
    await this.admissions.admit(admission, () => {
      stub.addMember(req.reqId, admission.id, req.selfDelivery)
      this.replica.pageEndChanged(stub)
      if (this.stubs.has(stub)) {
        this.stubOf.set(admission.id, stub)
        this.replica.own(admission.id, this)
      }
    })
    return { id: admission.id, joinedAt: admission.joinedAt }
  }

  /** What its state's listeners want crossed, which is what its local holder wants. */
  onWantChanged(change: WantChange): void {
    switch (change.kind) {
      case 'listeners':
        this.replica.listenersChanged(this)
        return this.onHolderTextChanged(this.local, null)
      case 'text':
        return this.onHolderTextChanged(this.local, this.local.textChanged(change.member, change.on))
      case 'announce':
        return this.onHolderTextChanged(this.local, null)
      case 'track':
        if (this.local.applyBinary(change.change)) this.onHolderBinaryChanged(this.local, change.change)
    }
  }

  reportCallbackError(error: unknown): void {
    handleCallbackBug(error)
  }

  /** A holder's text wants changed (`before` is `null` when none did): replan, then replay the retained
   *  frame it now wants. */
  onHolderTextChanged(holder: LaneHolder, before: TextWantsChange | null): void {
    if (this.holdsLanes(holder)) this.lanePlan.textChanged(holder, before?.members.keys() ?? [])
    if (before) void this.replica.replayText(holder, before).catch(reportRoomError)
  }

  /** One of a holder's binary wants changed: replan the pairs it covers, then replay what it now wants. */
  onHolderBinaryChanged(holder: LaneHolder, change: BinaryWantChange): void {
    if (this.holdsLanes(holder)) this.lanePlan.binaryChanged(holder, change)
    if (change.on) void this.replica.replayBinary(holder, change).catch(reportRoomError)
  }

  /** A stub that is not attached holds no lanes. */
  private holdsLanes(holder: LaneHolder): boolean {
    return holder === this.local || this.stubs.has(holder as RoomStub)
  }

  /** Whether this handle observes the room: a client stub, a participant, or a listener besides the package fields'. */
  isObserved(): boolean {
    return this.stubs.size > 0 || this.localParticipants.size > 0 || this.state.ownListenerCount > 0
  }

  /** Runs the package fields on the handle's view while it is observed, and stops them when it no longer is. */
  syncFields(): void {
    if (this.isObserved()) this.state.attachFields()
    else this.state.detachFields()
  }

  holdsTail(): boolean {
    return this.tail !== null
  }

  private holderOf(id: string): ServerLocalParticipant | RoomStub | undefined {
    return this.localParticipants.get(id) ?? this.stubOf.get(id)
  }

  /** A holder that detached during the guard took nothing. */
  holds(id: string): boolean {
    return this.holderOf(id) !== undefined
  }

  /** A drop of the page's wire during the admission skipped this member: it starts disconnected. */
  joined(id: string): void {
    if (this.stubOf.get(id)?.end.isConnected === false) this.setMemberConnected(id, false)
  }

  deliverDemand(member: string, track: string, change: DemandChange): void {
    const holder = this.holderOf(member)
    if (holder instanceof ServerLocalParticipant) holder._demandChanged(track, change)
    else holder?.relayEvent({ __r: 'demand', member, track, ...change })
  }
}
