export { ServerRoom, ServerLocalParticipant, RoomHandle, handleOf }
export type { MetaMirror }

import {
  type TELEFUNC_SHIELDS,
  type ChannelPublishAck,
  getChannelLimits,
  handleCallbackBug,
  type OrderingInfo,
  markHandled,
} from 'telefunc/__internal'
import { assert, assertUsage } from '../utils/assert.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import {
  encodeBinaryFrame,
  decodeBinaryFrame,
  type BinaryFrame,
  type BinaryWantChange,
  type TrackKey,
} from '../shared/binary.js'
import { RoomError, roomClosedError, ROOM_BUG_MESSAGE } from '../shared/errors.js'
import { assertKnownOptions, normalizeJoinOptions, ownMessage, ownMetadata, recipientId } from '../shared/arguments.js'
import {
  inboxMessageFromWire,
  type SenderStamp,
  type RoomDmEnvelope,
  type RoomCtrlEnvelope,
  type MemberMetaUpdate,
  type ParticipantStubMetadata,
  type RoomSnapshotMetadata,
  type InboxMessage,
  type RoomResult,
} from '../shared/protocol.js'
import type { RoomDataEnvelope, RoomMemberRecord, RoomIncarnation } from './protocol.js'
import type { RoomModel } from '../shared/RoomModel.js'
import { RoomState, RoomStateView, type RoomStateOwner } from '../shared/state.js'
import { ParticipantBase, type ParticipantBaseOptions } from '../shared/ParticipantBase.js'
import {
  MemberMessageTexts,
  RoomParticipantStub,
  RoomStub,
  type RoomParticipantStubOptions,
  type RoomStubOptions,
} from './RoomStub.js'
import type { RoomRequest } from './requests.js'
import { LocalHolder, type LaneHolder, type TextWantsChange } from './lifecycle/replay.js'
import { TailHold, type TailHoldOwner } from './lifecycle/TailHold.js'
import type { RoomReplica } from './RoomReplica.js'
import type { LanePlan } from './lanes/LanePlan.js'
import type { RoomDemand } from './lanes/RoomDemand.js'
import type { AuthoritySync } from './lifecycle/AuthoritySync.js'
import type { RosterStream } from './lifecycle/RosterStream.js'
import type { PendingDmAcks } from './PendingDmAcks.js'
import type { RoomHosting } from './lifecycle/RoomHosting.js'
import { RoomWrites, runAfterHook, type RoomWritesOwner } from './RoomWrites.js'
import { withinRoomHorizon } from './lanes/lanes.js'
import { reportRoomError } from './reportRoomError.js'
import { Admission } from './Admission.js'
import { evictMember, joinMember } from './store/members.js'
import type {
  BinaryPublishOptions,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  Room,
  RoomSnapshotView,
  Sender,
} from '../types.js'
import type { RoomGuards } from './roomStatics.js'
assertIsNotBrowser()

const SERVER_ROOM_BRAND: unique symbol = Symbol.for('telefunc.ServerRoom')
const SERVER_PARTICIPANT_BRAND: unique symbol = Symbol.for('telefunc.ServerRoomParticipant')

type MetaMirror = { accepted(update: MemberMetaUpdate): void; rewound(): void }

type RoomHandleOptions = {
  /** The user's `Room` over the handle, which the room's state is the state of. */
  view: ServerRoom
  replica: RoomReplica
  /** Where the handle's holders' wants replan. */
  lanePlan: Pick<LanePlan, 'textChanged' | 'binaryChanged' | 'memberChanged'>
  authority: Pick<AuthoritySync, 'ensureRoster'>
  /** For the handle's stubs. */
  roster: Pick<RosterStream, 'settled'>
  /** For the handle's participants. */
  incomingDemand: Pick<RoomDemand, 'wanted'>
  /** For the handle's writes. */
  acks: PendingDmAcks
  /** Hosts the members the handle admits, and evicts again one whose eviction failed. */
  hosting: Pick<RoomHosting, 'lease' | 'evictLater'>
  /** The meta of each participant a client holds, which the client mirrors. */
  metaMirrors: Set<MetaMirror>
}

/** One `Room.get()`'s handle on a room: its guards, its tail, the stubs it opened, its participants and its listeners,
 *  over this process's one view of the room, which every handle to it shares. Each serialization opens a fresh stub. */
class RoomHandle implements RoomWritesOwner, TailHoldOwner {
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
  guards: RoomGuards | null = null
  /** This handle's listeners over the replica's model. */
  readonly state: RoomState
  readonly local: LocalHolder
  private readonly stubs = new Set<RoomStub>()
  /** The attached stub holding each member a client joined through. */
  private readonly stubOf = new Map<string, RoomStub>()
  readonly localParticipants = new Map<string, ServerLocalParticipant>()
  /** The local participants that joined with `selfDelivery: false`. */
  private readonly localSuppressed = new Set<string>()
  /** The admissions under way; an `owned` one the heartbeat leaves alone, and its holder's detach doesn't evict. */
  private readonly admissions = new Map<string, Admission>()
  readonly writes: RoomWrites

  private readonly lanePlan: RoomHandleOptions['lanePlan']
  private readonly authority: RoomHandleOptions['authority']
  private readonly roster: RoomHandleOptions['roster']
  private readonly incomingDemand: RoomHandleOptions['incomingDemand']
  private readonly hosting: RoomHandleOptions['hosting']
  private readonly metaMirrors: Set<MetaMirror>

  constructor({
    view,
    replica,
    lanePlan,
    authority,
    roster,
    incomingDemand,
    acks,
    hosting,
    metaMirrors,
  }: RoomHandleOptions) {
    this.replica = replica
    this.lanePlan = lanePlan
    this.authority = authority
    this.roster = roster
    this.incomingDemand = incomingDemand
    this.hosting = hosting
    this.metaMirrors = metaMirrors
    this.incarnation = replica.incarnation
    this.state = new RoomState({ view, model: replica.model, owner: new RoomHandleStateOwner({ room: this }) })
    this.local = new LocalHolder({ state: this.state, suppressed: this.localSuppressed })
    this.writes = new RoomWrites({
      owner: this,
      incarnation: this.incarnation,
      model: replica.model,
      state: this.state,
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

  /** See `Room.guard()`. One declaration per instance keeps the grant declarative. */
  setGuards(guards: RoomGuards): void {
    assertUsage(
      this.guards === null,
      'Room.guard() was already called for this room instance: declare all guards in one call',
    )
    this.guards = guards
  }

  async join(options?: JoinOptions): Promise<LocalParticipant> {
    this.assertNotKnownClosed()
    const { meta, selfDelivery, identity, hidden } = normalizeJoinOptions(options)
    const admission = new Admission({ meta, identity, hidden })
    const participant = new ServerLocalParticipant({
      room: this,
      participant: { id: admission.id, meta, selfDelivery, identity },
      incomingDemand: this.incomingDemand,
      metaMirrors: this.metaMirrors,
    })
    await this.admit(admission, () => {
      this.localParticipants.set(admission.id, participant)
      this.replica.own(admission.id, this)
      if (!selfDelivery) this.localSuppressed.add(admission.id)
    })
    return participant
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    await this.authority.ensureRoster()
    return options?.hidden ? this.state.listHidden() : this.state.listVisible()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    await this.authority.ensureRoster()
    return this.state.getRemote(id)
  }

  snapshot(): RoomSnapshotView {
    // Loads the members for the next snapshot; their arrival fires onChange.
    if (!this.model.rosterKnown) void this.authority.ensureRoster().catch(reportRoomError)
    return this.model.snapshot()
  }

  /** Both join paths: the guard passes, the holder takes the member, then the member commits. */
  private async admit(admission: Admission, hold: () => void): Promise<void> {
    const { id, meta, identity, joinedAt, hidden } = admission
    const onBeforeJoin = this.guards?.onBeforeJoin
    if (!hidden && onBeforeJoin) await onBeforeJoin({ id, meta, identity })
    hold()
    await this.commitAdmission(admission)
    if (hidden) return // a hidden participant has no post-join hook
    const onAfterJoin = this.guards?.onAfterJoin
    if (onAfterJoin) await runAfterHook(() => onAfterJoin({ id, meta, identity }, { joinedAt }))
  }

  /** The member's inbox is ready before its record is durable, and the join is announced after,
   *  so no DM or event is lost. */
  private async commitAdmission(admission: Admission): Promise<void> {
    const { id, meta, identity, joinedAt, hidden } = admission
    const record: Omit<RoomMemberRecord, 'session'> = { joinedAt, metaSeq: 0, metaGen: 0 }
    const join: Extract<RoomCtrlEnvelope, { __r: 'join' }> = { __r: 'join', id, meta, joinedAt }
    if (identity !== null) record.identity = join.identity = identity
    if (hidden) record.hidden = join.hidden = true
    this.admissions.set(id, admission)
    // A holder that detached during the guard took nothing, and a room that closed meanwhile takes no one.
    if (this.holderOf(id) === undefined || this.model.closed) admission.abandon()
    try {
      // The member's inbox delivers (within the horizon) before its join is visible, and the admission still holds.
      await this.replica.openInbox(id)
      admission.assertLive(this.model)
      // The member's record and its join event are one write, so its leave always follows its join.
      await joinMember(this.incarnation, this.hosting.lease, { id, record, meta, join })
      admission.assertLive(this.model)
      admission.commit()
      this.admissions.delete(id)
      this.replica.applyControl(join)
      // A drop of the page's wire during the admission skipped this member: it starts disconnected.
      if (this.stubOf.get(id)?.end.isConnected === false) this.setMemberConnected(id, false)
    } catch (error) {
      // A member write that rejected may still have committed, its reply lost; evicting an absent member only reads.
      // In an open room only a detach of the holder, or a removal that already evicted the member, abandons it.
      const lost = admission.abandoned && !this.model.closed
      const cause: LeaveCause = { type: lost ? 'disconnected' : 'left' }
      await evictMember(this.incarnation, id, cause).catch((error: unknown) => {
        reportRoomError(error)
        this.hosting.evictLater(id)
      })
      this.abandonAdmission(id, cause)
      throw error
    }
  }

  private abandonAdmission(id: string, cause: LeaveCause): void {
    this.admissions.delete(id)
    this.model.applyLeave(id, cause)
  }

  async removeMember(id: string, cause: LeaveCause): Promise<void> {
    if (this.model.closed) return // close() already removed everyone
    await evictMember(this.incarnation, id, cause)
    this.model.applyLeave(id, cause)
  }

  /** The member's holder is gone: ownership ends even if eviction fails, which the heartbeat then retries. */
  async removeDepartedMember(id: string): Promise<void> {
    const cause = { type: 'disconnected' } as const
    try {
      await this.removeMember(id, cause)
    } catch {
      this.hosting.evictLater(id)
      this.model.applyLeave(id, cause)
    }
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

  /** A member this handle owns left: its participant ends, and its admission, if under way, is abandoned. */
  memberLeft(id: string, cause: LeaveCause): void {
    const local = this.localParticipants.get(id)
    if (local) {
      this.localParticipants.delete(id)
      this.localSuppressed.delete(id)
      local._onLeft(cause)
    }
    this.stubOf.delete(id)
    this.admissions.get(id)?.abandon()
  }

  /** The room closed: the replica already applied and relayed `closed`. */
  teardown(): void {
    this.teardownTail()
    for (const local of this.localParticipants.values()) local._onLeft({ type: 'closed' })
    this.localParticipants.clear()
    this.localSuppressed.clear()
    for (const admission of this.admissions.values()) admission.abandon()
    // A client back while its channel still holds it gets `closed`.
    const timeout = getChannelLimits().reconnectWindow
    for (const stub of this.stubs) void stub.end.close({ timeout }).catch(() => {})
  }

  /** A DM for a member this handle owns, routed to its holder: a server participant or its client stub. */
  onDm(envelope: RoomDmEnvelope, serialized: string, info: OrderingInfo): void {
    const holder = this.holderOf(envelope.to)
    // A client that joined through a stub gets the DM relayed (its `ackId` rides along) and answers with `dm-reply`.
    if (holder instanceof RoomStub) {
      if (envelope.fromMetaSeq !== undefined) {
        this.replica.applyStamp(envelope.from, envelope as RoomDmEnvelope & SenderStamp)
      }
      const texts = new MemberMessageTexts(serialized, info, envelope)
      return holder.relayDm(texts, envelope)
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

  /** A client's view of this room. It attaches before the snapshot, so every later event relays and every
   *  earlier one is in the snapshot. */
  openStub({ end, publishShield, grants }: Omit<RoomStubOptions, 'room' | 'roster'>): RoomSnapshotMetadata {
    this.attachStub(new RoomStub({ room: this, roster: this.roster, end, publishShield, grants }))
    return {
      channelId: end.id,
      roomId: this.id,
      meta: this.model.metaEntries(),
      closed: this.model.closed,
      metaSeq: this.model.metaSeq,
      bufferLimit: getChannelLimits().bufferLimit,
      count: this.model.count,
    }
  }

  attachStub(stub: RoomStub): void {
    this.stubs.add(stub)
    if (this.tail !== null) {
      stub.beginTail(this.tail.take())
      this.tail = null
    }
    this.replica.attachStub(this, stub)
  }

  setHeldConnected(stub: RoomStub, connected: boolean): void {
    // A member whose join is under way is skipped: its record takes the wire's state when the join applies.
    for (const id of stub.heldMembers()) if (!this.admissions.has(id)) this.setMemberConnected(id, connected)
  }

  private setMemberConnected(id: string, connected: boolean): void {
    void this.writes.setMemberConnected(id, connected).catch(reportRoomError)
  }

  detachStub(stub: RoomStub): void {
    this.stubs.delete(stub)
    stub.endTail()
    stub.replay.dropHolds()
    this.replica.detachStub(this, stub)
    for (const id of stub.heldMembers()) {
      this.stubOf.delete(id)
      this.replica.release(id, this)
      const admission = this.admissions.get(id)
      const owned = admission?.owned === true
      admission?.abandon()
      if (owned) continue // the admission rolls itself back
      void this.removeDepartedMember(id)
    }
  }

  /** A response serialized a participant whose own frames the stub's client doesn't get back. */
  onStubSuppressed(stub: RoomStub, member: string): void {
    if (this.stubs.has(stub)) this.lanePlan.memberChanged(member)
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
    const admission = new Admission({ meta: ownMetadata(req.meta), identity: null, hidden: false })
    this.assertNotKnownClosed()
    await this.admit(admission, () => {
      stub.addMember(req.reqId, admission.id, req.selfDelivery)
      if (this.stubs.has(stub)) {
        this.stubOf.set(admission.id, stub)
        this.replica.own(admission.id, this)
      }
    })
    return { id: admission.id, joinedAt: admission.joinedAt }
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

  /** Whether this handle observes the room: a client stub, a participant, or a listener. */
  isObserved(): boolean {
    return this.stubs.size > 0 || this.localParticipants.size > 0 || this.state.listenerCount > 0
  }

  holdsTail(): boolean {
    return this.tail !== null
  }

  private holderOf(id: string): ServerLocalParticipant | RoomStub | undefined {
    return this.localParticipants.get(id) ?? this.stubOf.get(id)
  }

  deliverDemand(member: string, track: string, wanted: boolean): void {
    const holder = this.holderOf(member)
    if (holder instanceof ServerLocalParticipant) holder._onDemand(track, wanted)
    else holder?.relayEvent({ __r: 'demand', member, track, wanted })
  }
}

/** The user's `Room` for one `Room.get()`: the public methods, over the handle that does the work, which Room's own
 *  code is given instead. */
class ServerRoom extends RoomStateView implements Room {
  readonly [SERVER_ROOM_BRAND] = true
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  protected readonly _state: RoomState
  private readonly handle: RoomHandle

  constructor(options: Omit<RoomHandleOptions, 'view'>) {
    super()
    this.handle = new RoomHandle({ ...options, view: this })
    this._state = this.handle.state
    roomHandles.set(this, this.handle)
  }

  static isServerRoom(value: unknown): value is ServerRoom {
    return value !== null && typeof value === 'object' && SERVER_ROOM_BRAND in value
  }

  join(options?: JoinOptions): Promise<LocalParticipant> {
    return this.handle.join(options)
  }

  getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    return this.handle.getParticipants(options)
  }

  getParticipant(id: string): Promise<RemoteParticipant | null> {
    return this.handle.getParticipant(id)
  }

  snapshot(): RoomSnapshotView {
    return this.handle.snapshot()
  }
}

// One map for every module graph of the server, as a Room from one graph is a Room in another.
const { roomHandles } = getGlobalObject('server/room.ts', () => ({
  roomHandles: new WeakMap<ServerRoom, RoomHandle>(),
}))

/** The handle a user's `Room` is over, for Room's own code. */
function handleOf(room: ServerRoom): RoomHandle {
  const handle = roomHandles.get(room)
  assert(handle)
  return handle
}

type ServerLocalParticipantOptions = {
  room: RoomHandle
  participant: ParticipantBaseOptions['participant']
  /** Holds the participant's demand once for the whole room. */
  incomingDemand: Pick<RoomDemand, 'wanted'>
  /** Where a client holding the participant mirrors its meta. */
  metaMirrors: Set<MetaMirror>
}

/** Server-side `LocalParticipant`, returned by `ServerRoom.join()`. */
class ServerLocalParticipant extends ParticipantBase {
  readonly [SERVER_PARTICIPANT_BRAND] = true
  /** @internal */
  readonly room: RoomHandle
  private metaMirror: MetaMirror | null = null
  /** The client holding this participant, which its inbox forwards to instead of local listeners. */
  private holder: RoomParticipantStub | null = null
  protected readonly _bugMessage = ROOM_BUG_MESSAGE

  private readonly incomingDemand: ServerLocalParticipantOptions['incomingDemand']
  private readonly metaMirrors: Set<MetaMirror>

  constructor({ room, participant, incomingDemand, metaMirrors }: ServerLocalParticipantOptions) {
    super({ participant, rewinds: room.model.metaRewinds })
    this.room = room
    this.incomingDemand = incomingDemand
    this.metaMirrors = metaMirrors
  }

  static isServerLocalParticipant(value: unknown): value is ServerLocalParticipant {
    return value !== null && typeof value === 'object' && SERVER_PARTICIPANT_BRAND in value
  }

  /** @internal Read from the room's demand, which holds it once. */
  get _demandedTracks(): TrackKey[] {
    return this._left ? [] : this.incomingDemand.wanted(this.id)
  }

  publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    assertKnownOptions(options, ['coalesce', 'retain'], 'publish()')
    assertUsage(
      options?.retain === undefined || typeof options.retain === 'boolean',
      'publish() options.retain should be a boolean',
    )
    assertUsage(
      options?.coalesce === undefined || typeof options.coalesce === 'string',
      'publish() options.coalesce should be a string',
    )
    return markHandled(this.publishText(ownMessage(data), options?.retain))
  }

  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    const framed = encodeBinaryFrame(this.id, data, options)
    // Local holders get exactly what remote ones decode, not the caller's objects.
    const frame = decodeBinaryFrame(framed)
    assert(frame !== null)
    return markHandled(this._publishFrame(frame, framed))
  }

  private async publishText(data: unknown, retain: boolean | undefined): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this.room.writes.publishText(this.id, data, retain)
  }

  /** @internal */
  async _publishFrame(frame: BinaryFrame, framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this.room.writes.publishBinaryFrame(frame, framed)
  }

  /** @internal A DM for this member: to the client holding it, else its listeners. */
  override _deliverMessage(msg: InboxMessage): void {
    if (this.holder) return this.holder.deliver(msg)
    super._deliverMessage(msg)
  }

  /** @internal An `{ ack: true }` DM: to the client holding it, else its listeners; never rejects. */
  override _deliverMessageAck(msg: InboxMessage): Promise<RoomResult> {
    if (this.holder) return this.holder.deliverAck(msg)
    return super._deliverMessageAck(msg)
  }

  /** @internal A reply to an acknowledged DM, on its way to the sender. */
  _trackReply(replied: Promise<void>): void {
    this.holder?.trackReply(replied)
  }

  /** @internal A client's hold on this participant, which its inbox forwards to. */
  _openStub({ end, publishShield }: Omit<RoomParticipantStubOptions, 'participant'>): ParticipantStubMetadata {
    // A LocalParticipant has one holder; rebinding would overwrite its inbox forwarder and let either close drop it.
    assertUsage(
      this.holder === null,
      'This LocalParticipant is already bound to a client and cannot be handed to another. A LocalParticipant is a ' +
        'single member: give each client its own join(), or share a getParticipants() view (which is read-only) ' +
        'instead.',
    )
    this.holder = new RoomParticipantStub({ participant: this, end, publishShield })
    this._flushHeld(this.holder)
    return {
      channelId: end.id,
      roomId: this.room.id,
      id: this.id,
      meta: this.meta,
      selfDelivery: this.selfDelivery,
      identity: this.identity,
      bufferLimit: getChannelLimits().bufferLimit,
    }
  }

  /** @internal Each meta update this participant applies, and each failover rewind of its seq, which a client holding
   *  it mirrors. */
  _onMeta(mirror: MetaMirror): () => void {
    this.metaMirror = mirror
    this.metaMirrors.add(mirror)
    return () => {
      this.metaMirrors.delete(mirror)
      if (this.metaMirror === mirror) this.metaMirror = null
    }
  }

  /** As the room accepted it: always true for one no page holds. */
  get isConnected(): boolean {
    return this._acceptedConnected
  }

  override _acceptMeta(update: MemberMetaUpdate): boolean {
    const wasConnected = this.isConnected
    if (!super._acceptMeta(update)) return false
    this.metaMirror?.accepted(update)
    if (this.isConnected === wasConnected) return true
    if (this.isConnected) this._onReconnected()
    else this._onDisconnected()
    return true
  }

  /** @internal The page holding this participant can reach it, or can't. */
  _setHolderConnected(connected: boolean): void {
    void this.room.writes.setMemberConnected(this.id, connected).catch(reportRoomError)
  }

  /** @internal The client holding this participant went away without leaving. */
  _releaseHolder(): Promise<void> {
    return this.room.removeDepartedMember(this.id)
  }

  send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    assertKnownOptions(options, ['ack'], 'send()')
    assertUsage(
      options?.ack === undefined || typeof options.ack === 'boolean',
      'send() options.ack should be a boolean',
    )
    return markHandled(this.sendDm(recipientId(to), ownMessage(data), options?.ack === true))
  }

  private async sendDm(to: string, data: unknown, ack: boolean): Promise<unknown> {
    this._assertActive()
    return await this.room.writes.sendDm(this.id, to, data, ack)
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    await this._setMeta(meta)
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    await this._setAttributes(attrs)
  }

  /** @internal The accepted write, which a client holder mirrors in revision order. */
  async _setMeta(meta: ParticipantMeta): Promise<MemberMetaUpdate> {
    this._assertActive()
    return await this.room.writes.setMemberMeta(this.id, meta)
  }

  /** @internal */
  async _setAttributes(attrs: ParticipantMeta): Promise<MemberMetaUpdate> {
    this._assertActive()
    return await this.room.writes.mergeMemberMeta(this.id, attrs)
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this.room.removeMember(this.id, { type: 'left' })
  }

  protected override _resolveSender(id: string): Sender | null {
    return this.room.state.getRemote(id) // sync view read (delivery must not wait on I/O)
  }

  protected _reportError(err: unknown): void {
    handleCallbackBug(err)
  }

  protected _inboxBufferLimit(): number {
    return getChannelLimits().bufferLimit
  }
}

type RoomHandleStateOwnerOptions = { room: RoomHandle }

/** What a handle's state asks of it. */
class RoomHandleStateOwner implements RoomStateOwner {
  private readonly room: RoomHandle

  constructor({ room }: RoomHandleStateOwnerOptions) {
    this.room = room
  }

  reportCallbackError(error: unknown): void {
    handleCallbackBug(error)
  }

  onListenersChanged(member: string | null, binary: BinaryWantChange | null): void {
    const room = this.room
    room.replica.listenersChanged(room)
    room.onHolderTextChanged(room.local, room.local.refreshText(member))
    if (binary !== null && room.local.applyBinary(binary)) room.onHolderBinaryChanged(room.local, binary)
  }
}
