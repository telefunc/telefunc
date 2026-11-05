export { RoomReplica }
export type { PresenceEvent }

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../utils/assert.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { getRoomPlane } from '../backend/plane.js'
import type { LaneId } from '../backend/contract.js'
import { encodePublish, hostLocal, type OrderingInfo } from 'telefunc/__internal'
import { EVERY_MEMBER, EVERY_TRACK, decodeBinaryFrame, type BinaryWantChange } from '../shared/binary.js'
import { ROOM_MEMBER_TRACKS_MAX } from '../shared/constants.js'
import { DemandExchange } from './lanes/DemandExchange.js'
import { RoomDemand } from './lanes/RoomDemand.js'
import { PublishedTracks } from './lanes/PublishedTracks.js'
import { DM_FAILURE, RoomError } from '../shared/errors.js'
import { joinedMember, leaveCauseFromWire, leaveCauseToWire } from '../shared/protocol.js'
import type {
  MemberMetaUpdate,
  MemberSnapshot,
  RoomCtrlEnvelope,
  RoomDmEnvelope,
  RoomEnvelope,
  SenderStamp,
} from '../shared/protocol.js'
import type { RoomConfigRecord, RoomDataEnvelope, RoomDmAckEnvelope, RoomIncarnation } from './protocol.js'
import { RoomModel, type ServerModelOwner } from '../shared/RoomModel.js'
import type { LeaveCause } from '../types.js'
import { AuthoritySync, type AuthorityChange, type AuthoritySyncOwner } from './lifecycle/AuthoritySync.js'
import { PendingDmAcks } from './PendingDmAcks.js'
import { reportRoomError } from './reportRoomError.js'
import { SEMANTIC_LANE, decodeLaneEnvelope, decodeRoomText, withinRoomHorizon } from './lanes/lanes.js'
import { eachInFlight } from './store/cellStore.js'
import { dropDmReply, takeDmReplies } from './store/dmReplies.js'
import { RoomHosting } from './lifecycle/RoomHosting.js'
import type { StoredRoomMeta } from './store/meta.js'
import type { LaneHolder, ReplayHold, TextWantsChange } from './lifecycle/replay.js'
import { type MetaMirror, type RoomHandle, ServerRoom } from './room.js'
import { RosterStream } from './lifecycle/RosterStream.js'
import { toggle } from '../utils/sets.js'
import { MemberMessageTexts, type RoomStub, unorderedEventText } from './RoomStub.js'
import { LanePlan, type LaneDelivery, type LaneEvent, type LanePlanOwner } from './lanes/LanePlan.js'
import { RoomHeartbeat } from './lifecycle/RoomHeartbeat.js'
assertIsNotBrowser()

/** What the control lane carries for the model: everything but another process's ask, which the report answers. */
type PresenceEvent = Exclude<RoomCtrlEnvelope, { __r: 'demand-ask' }>

const REPLICAS = Symbol.for('telefunc.room.replicas')

/** Per channel host: a listener may do I/O only in its own Cloudflare session DO. */
function replicasHere(): Map<string, WeakRef<RoomReplica>> {
  return hostLocal(REPLICAS, () => new Map<string, WeakRef<RoomReplica>>())
}

const { collected } = getGlobalObject('server/RoomReplica.ts', () => ({
  /** Forgets a collected replica, unless a later one of its incarnation took its place. */
  collected: new FinalizationRegistry<{ replicas: Map<string, WeakRef<RoomReplica>>; inc: string }>(
    ({ replicas, inc }) => {
      if (replicas.get(inc)?.deref() === undefined) replicas.delete(inc)
    },
  ),
}))

type RoomReplicaOptions = {
  incarnation: RoomIncarnation
  stored: StoredRoomMeta
  seed: { members: MemberSnapshot[] } | { count: number }
}

/** This process's one view of a room incarnation, however many handles share it; on Cloudflare, a session DO holds its
 *  own. The replica holds the model, the lanes planned from the wants of the handles' holders, the demand, the lease
 *  that hosts the members and pages the handles hold, and the heartbeat that renews the lease and checks the authority
 *  again. Each handle keeps its guards, its tail, the stubs it opened and its listeners. A replica that no handle
 *  observes, owns a member through or tails runs no heartbeat and holds no lane; it lives while a handle holds it. */
class RoomReplica implements ServerModelOwner, LanePlanOwner, AuthoritySyncOwner {
  /** The incarnation's replica on this channel host, while a handle or its own work holds it. */
  static of(inc: string): RoomReplica | undefined {
    return replicasHere().get(inc)?.deref()
  }

  /** The incarnation's replica: the one held, else a new one seeded with what the caller read. */
  static seeded(
    roomId: string,
    config: RoomConfigRecord,
    stored: StoredRoomMeta,
    seed: { members: MemberSnapshot[] } | { count: number },
  ): RoomReplica {
    const held = RoomReplica.of(config.inc)
    if (held !== undefined) return held
    const incarnation = { id: roomId, inc: config.inc, hold: config.hold }
    const replica = new RoomReplica({ incarnation, stored, seed })
    const replicas = replicasHere()
    replicas.set(config.inc, new WeakRef(replica))
    collected.register(replica, { replicas, inc: config.inc })
    return replica
  }

  readonly id: string
  readonly incarnation: RoomIncarnation
  readonly model: RoomModel
  private readonly incomingDemand: RoomDemand
  private readonly publishedTracks = new PublishedTracks()
  /** This process's demand to the other processes, and theirs to `incomingDemand`. */
  private readonly demandExchange: DemandExchange
  private readonly lanePlan: LanePlan
  private readonly authority: AuthoritySync
  private readonly heartbeat: RoomHeartbeat
  private readonly roster: RosterStream
  /** Hosts the members the handles join and the pages' views. */
  private readonly hosting: RoomHosting
  /** In-flight `send(…, { ack: true })`s; a leave of either end, or a close, fails the ones it strands. */
  private readonly acks = new PendingDmAcks()
  /** Every handle's attached stubs. */
  private readonly stubs = new Set<RoomStub>()
  /** The meta of each participant a client holds, which the client mirrors. */
  private readonly metaMirrors = new Set<MetaMirror>()
  /** The handle holding each member it joined. */
  private readonly owners = new Map<string, RoomHandle>()
  /** The seq of the last frame each owned member's inbox delivered, 0 before its first. */
  private readonly inboxSeqs = new Map<string, number>()
  /** The handles that observe the room (a client stub, a participant or a listener) or hold a tail. The replica keeps
   *  it in its own methods that a handle's change goes through, and holds them while they are served. */
  private readonly served = new Set<RoomHandle>()
  /** The served handles that take what a lane carries: those with a listener or a tail. A message goes to each, so it
   *  costs nothing per handle that takes none. */
  private readonly receivers = new Set<RoomHandle>()
  /** What the served handles are to the room, which the lane plan reads. */
  private readonly roles = { observed: false, listened: false, tailHeld: false, owned: this.owners }

  private constructor({ incarnation, stored, seed }: RoomReplicaOptions) {
    this.id = incarnation.id
    this.incarnation = incarnation
    this.hosting = new RoomHosting({ incarnation })
    this.model = new RoomModel({
      roomId: incarnation.id,
      meta: stored.entries,
      seed,
      metaSeq: stored.seq,
      owner: this,
    })
    this.incomingDemand = new RoomDemand({ ownedMembers: this.owners, publishedTracks: this.publishedTracks })
    this.demandExchange = new DemandExchange({ incarnation, incomingDemand: this.incomingDemand })
    const { model, demandExchange } = this
    const { outgoingDemand } = demandExchange
    this.lanePlan = new LanePlan({ owner: this, roles: this.roles, incarnation, model, outgoingDemand, demandExchange })
    const lanePlan = this.lanePlan
    this.roster = new RosterStream({ model, stubs: this.stubs })
    this.authority = new AuthoritySync({ owner: this, incarnation, roster: this.roster, model, lanePlan })
    const authority = this.authority
    this.heartbeat = new RoomHeartbeat({
      hosting: this.hosting,
      incarnation,
      model,
      lanePlan,
      outgoingDemand,
      demandExchange,
      authority,
    })
  }

  /** A new handle on this view, given the parts of it a handle and its stubs, participants and writes use. */
  handle(): ServerRoom {
    return new ServerRoom({
      replica: this,
      lanePlan: this.lanePlan,
      authority: this.authority,
      roster: this.roster,
      incomingDemand: this.incomingDemand,
      acks: this.acks,
      hosting: this.hosting,
      metaMirrors: this.metaMirrors,
    })
  }

  /** A new handle on a view other handles share, which catches up with the authority first unless it is current. */
  async sharedHandle(): Promise<ServerRoom> {
    await this.authority.catchUp()
    return this.handle()
  }

  /** A handle's listeners changed. */
  listenersChanged(handle: RoomHandle): void {
    this.reconsider(handle)
  }

  /** Keeps `served`, `receivers` and the roles the lane plan reads current with the handle. Every transition of a
   *  handle runs it after the handle changed (the rule is beside `RoomHandle`'s collections). */
  private reconsider(handle: RoomHandle): void {
    const tailHeld = handle.holdsTail()
    toggle(this.served, handle, handle.isObserved() || tailHeld)
    toggle(this.receivers, handle, handle.state.listenerCount > 0 || tailHeld)
    const roles = this.roles
    roles.observed = roles.listened = roles.tailHeld = false
    for (const served of this.served) {
      roles.observed ||= served.isObserved()
      roles.listened ||= served.state.listenerCount > 0
      roles.tailHeld ||= served.holdsTail()
    }
  }

  own(member: string, handle: RoomHandle): void {
    this.reconsider(handle)
    this.owners.set(member, handle)
    // A member's id is fresh at each join, so its inbox starts before seq 1.
    this.inboxSeqs.set(member, 0)
    this.incomingDemand.owned(member)
  }

  /** The handle let the member go: it owns it here no more, and the member's lanes follow. */
  release(member: string, handle: RoomHandle): void {
    this.reconsider(handle)
    this.disown(member, handle)
    this.lanePlan.memberReleased(member)
  }

  /** Forgets the member here: its owner, its inbox seq, its demand and its published tracks. */
  private disown(member: string, handle: RoomHandle): void {
    if (this.owners.get(member) !== handle) return
    this.owners.delete(member)
    this.inboxSeqs.delete(member)
    this.incomingDemand.forgetMember(member)
    this.publishedTracks.forget(member)
  }

  /** The handle's stub attached: its view is written and its wants planned. */
  attachStub(handle: RoomHandle, stub: RoomStub): void {
    this.reconsider(handle)
    this.stubs.add(stub)
    this.hosting.hostView(stub)
    this.lanePlan.holderChanged(stub)
  }

  /** The stub's client attached: it gets the roster, the room's meta and the demand for the members it holds. */
  onStubAttached(stub: RoomStub): void {
    this.roster.sendTo(stub, this.authority.ensureRoster())
    stub.relayEvent({ __r: 'update', seq: this.model.metaSeq, meta: this.model.metaEntries() })
    for (const member of stub.heldMembers()) {
      stub.relayEvent({ __r: 'demand-state', member, tracks: this.incomingDemand.wanted(member) })
    }
  }

  /** The handle's stub detached: its view is removed and its wants dropped. */
  detachStub(handle: RoomHandle, stub: RoomStub): void {
    this.reconsider(handle)
    this.stubs.delete(stub)
    this.hosting.dropView(stub)
    this.roster.forget(stub)
    this.lanePlan.holderGone(stub)
  }

  /** A handle's tail started or ended: text ingestion follows it, ready once the returned promise resolves. */
  tailChanged(handle: RoomHandle): Promise<void> {
    this.reconsider(handle)
    this.lanePlan.syncLanes()
    return this.lanePlan.semanticReady
  }

  /** The member a handle admits gets its inbox, which delivers (within the horizon) when this resolves. */
  async openInbox(member: string): Promise<void> {
    this.lanePlan.memberOwned(member)
    const inbox = this.lanePlan.inboxOf(member)
    if (inbox !== undefined) await withinRoomHorizon(inbox.ready)
  }

  /** The member's publish on `track` goes out after the planning pass that would carry it here, if one runs (else
   *  null), within the member's named-track limit; a new track joins what its every-track readers cover. */
  admitTrack(member: string, track: string): Promise<void> | null {
    const planned = this.lanePlan.lanePlanned(member, track)
    if (planned !== null) return planned.then(() => this.publishOnTrack(member, track))
    this.publishOnTrack(member, track)
    return null
  }

  private publishOnTrack(member: string, track: string): void {
    if (!this.publishedTracks.admits(member, track)) {
      throw new RoomError(`A participant publishes on at most ${ROOM_MEMBER_TRACKS_MAX} named tracks`)
    }
    if (this.publishedTracks.add(member, track)) this.incomingDemand.published(member, track)
  }

  /** Applies a presence event to the model and relays what it changed to the clients: as the lane carried it when
   *  `carried` is given, else as applied. A leave reaches them from `onMemberLeft`, whatever applied it, and an update
   *  as each one it let apply, in seq order. A failover's rewind of the control lane, and of the meta cells, which
   *  share its slot, makes the view forget its meta seqs. */
  applyControl(event: PresenceEvent | AuthorityChange, carried?: () => string): void {
    if (event.__r === 'rewound') {
      this.model.forgetMetaSeqs()
      for (const mirror of this.metaMirrors) mirror.rewound()
      for (const stub of this.stubs) stub.relayEvent({ __r: 'rewound' })
      return
    }
    const relay = () => this.relayControl(event, carried ?? (() => unorderedEventText(event)))
    switch (event.__r) {
      case 'join':
        if (!this.model.applyJoin(joinedMember(event))) return
        this.lanePlan.memberChanged(event.id)
        relay()
        return
      case 'leave':
        this.model.applyLeave(event.id, leaveCauseFromWire(event))
        return
      case 'p-meta':
        if (this.model.applyParticipantMeta(event.id, event)) relay()
        return
      case 'update':
        for (const applied of this.model.applyRoomUpdate(event)) {
          this.relayControl(applied, () => unorderedEventText(applied))
        }
        // An update ahead of one before it waits for it, which a read repairs if it never arrives.
        if (this.model.metaWaiting) this.authority.metaWaiting()
        return
      case 'closed':
        if (!this.model.applyClosed()) return
        relay()
        this.teardown()
    }
  }

  /** A stamp newer than the sender's meta here is the update this view missed so far: it applies and relays first, so
   *  every client here names the sender as of the send. */
  applyStamp(id: string, { fromMeta, fromMetaSeq, fromDisconnected }: SenderStamp): void {
    const hidden = this.model.memberHidden(id)
    if (hidden === null || !this.model.newerStamp(id, fromMetaSeq)) return
    const update: Extract<PresenceEvent, { __r: 'p-meta' }> = { __r: 'p-meta', id, meta: fromMeta, seq: fromMetaSeq }
    if (fromDisconnected) update.disconnected = true
    if (hidden) update.hidden = true
    this.applyControl(update)
  }

  async replayText(holder: LaneHolder, before: TextWantsChange): Promise<void> {
    if (before.all) return
    // A live message from a sender another want's subscription already delivers would otherwise beat the retained one.
    const hold = holder.hold(null)
    const reading = this.readRetainedText(holder, before, hold).finally(() => hold.end())
    try {
      await withinRoomHorizon(reading)
    } finally {
      // Past the horizon the held messages go out, and a retained one the read returns later follows them.
      hold.flush()
    }
  }

  private async readRetainedText(holder: LaneHolder, before: TextWantsChange, hold: ReplayHold): Promise<void> {
    await this.authority.ensureRoster()
    // Read retained only after subscription readiness: a racing commit is then retained or live, never lost in the gap.
    await withinRoomHorizon(this.lanePlan.semanticReady)
    const stored = await getRoomPlane().readRetained(this.id, this.incarnation.inc, SEMANTIC_LANE)
    if (stored === null) return
    const serialized = decodeRoomText(stored.payload)
    const event = parse(serialized) as RoomDataEnvelope
    // A member whose own want didn't change is wanted now only by the new room-wide want, if at all.
    const had = before.members.get(event.from) ?? holder.textDemand().members.has(event.from)
    if (had || !holder.wantsTextFrom(event.from) || !this.model.hasMember(event.from)) return
    // Replay the stored order as-is; the holder admits it only if it is newer than what it has.
    holder.emitRetainedText(event, { seq: stored.seq, timestamp: stored.timestamp }, serialized, hold)
  }

  /** Binary uses the same readiness handoff and stored receipt; its holder dedupes the live/retained race per (member,
   *  track), so a frame it already has is not delivered again. A want for one member's track reads that lane; any
   *  other lists the retained lanes it covers. As with text, a frame replays only while its member is in the roster: a
   *  leave clears the member's retained frames, and one read just before that is gone with it. */
  async replayBinary(holder: LaneHolder, change: BinaryWantChange): Promise<void> {
    // A live frame on a lane another want's subscription already delivers would otherwise beat its retained frame.
    const hold = holder.hold(change)
    const reading = this.readRetainedBinary(holder, change, hold).finally(() => hold.end())
    try {
      await withinRoomHorizon(reading)
    } finally {
      // Past the horizon the held frames go out, and a retained frame the read returns later follows them.
      hold.flush()
    }
  }

  private async readRetainedBinary(holder: LaneHolder, { member, track }: BinaryWantChange, hold: ReplayHold) {
    await this.authority.ensureRoster()
    await (member === EVERY_MEMBER ? this.lanePlan.binaryReady() : this.lanePlan.pairsReady(member, track))
    const plane = getRoomPlane()
    const lanes: LaneId[] =
      member !== EVERY_MEMBER && track !== EVERY_TRACK
        ? [{ kind: 'binary', member, track }]
        : await plane.driver.listRetained(
            this.id,
            this.incarnation.inc,
            member !== EVERY_MEMBER ? { member } : track !== EVERY_TRACK ? { track } : undefined,
          )
    // A frame is admitted by its member and track, so the reads may end in any order.
    await eachInFlight(lanes, async (lane) => {
      if (lane.kind !== 'binary' || !holder.wantsBinary(lane.member, lane.track)) return
      const stored = await plane.readRetained(this.id, this.incarnation.inc, lane)
      // An unsubscribe during the read leaves nothing to deliver to, and no receipt to keep.
      if (stored === null || !holder.wantsBinary(lane.member, lane.track)) return
      if (!this.model.hasMember(lane.member)) return
      const framed = stored.payload
      const frame = decodeBinaryFrame(framed)
      assert(frame)
      holder.emitRetainedBinary(frame, { seq: stored.seq, timestamp: stored.timestamp }, framed, hold)
    })
  }

  // What the replica's parts ask of it

  /** `LanePlanOwner`: a lane's frame, to the handler for its kind. */
  laneDelivered(delivery: LaneDelivery, info: OrderingInfo): void {
    switch (delivery.kind) {
      case 'control':
        return this.onControl(delivery.text, info)
      case 'semantic':
        return this.onTextData(delivery.text, info)
      case 'inbox':
        return this.onDm(delivery.text, info)
      case 'binary':
        return this.onBinary(delivery.framed, info)
    }
  }

  /** `LanePlanOwner`: the roster read again, a catch-up with the authority, which finds a close the lanes missed, the
   *  heartbeat, which runs while the lanes need it, or the mark a control subscription starts at. */
  laneEvent(event: LaneEvent): void {
    switch (event) {
      case 'roster-needed':
        return void this.authority.refreshMembers().catch(reportRoomError)
      case 'deliveries-lost':
        return void this.authority.reconcileAuthority().catch(reportRoomError)
      case 'lanes-changed':
        return this.heartbeat.update()
      case 'control-subscribed':
        return this.authority.controlSubscribed()
    }
  }

  private onControl(serialized: string, info: OrderingInfo): void {
    this.authority.controlDelivered(info.seq)
    const ctrl = decodeLaneEnvelope(serialized) as RoomCtrlEnvelope
    if (ctrl.__r === 'demand-ask') return this.demandExchange.onAsk(ctrl)
    this.applyControl(ctrl, () => encodePublish(serialized, info))
  }

  private onTextData(serialized: string, info: OrderingInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDataEnvelope | Extract<RoomEnvelope, { __r: 'announce' }>
    if (envelope.__r === 'announce') {
      for (const handle of this.receivers) handle.local.relayAnnouncement(envelope.data, info)
      const wireText = encodePublish(serialized, info)
      for (const stub of this.stubs) stub.relayAnnouncement(wireText, info)
      return
    }
    this.applyStamp(envelope.from, envelope)
    for (const handle of this.receivers) handle.relayData(serialized, envelope, info)
    if (this.stubs.size === 0) return
    const texts = new MemberMessageTexts(serialized, info, envelope)
    for (const stub of this.stubs) stub.relayText(serialized, texts, envelope.from, info)
  }

  private onBinary(framed: Uint8Array, info: OrderingInfo): void {
    const unframed = decodeBinaryFrame(framed)
    assert(unframed)
    for (const handle of this.receivers) handle.local.relayBinary(unframed, info)
    if (this.stubs.size === 0) return
    const wireData = encodePublish(framed, info)
    for (const stub of this.stubs) stub.relayBinary(wireData, unframed.from, unframed.track, info)
  }

  /** A DM for a member a handle here owns, routed to that handle. */
  private onDm(serialized: string, info: OrderingInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDmEnvelope | RoomDmAckEnvelope
    this.inboxFrame(envelope.to, info.seq)
    // A reply to one of our own `send(…, { ack: true })`s, riding our inbox back home.
    if (envelope.__r === 'dm-ack') return this.dmAcked(envelope)
    this.owners.get(envelope.to)?.onDm(envelope, serialized, info)
  }

  /** A seq skipped in an owned member's inbox may have been a reply, which its recipient stored before committing. */
  private inboxFrame(member: string, seq: number): void {
    const last = this.inboxSeqs.get(member)
    if (last === undefined) return
    this.inboxSeqs.set(member, seq)
    if (seq <= last + 1 || !this.acks.waitsOn(member)) return
    void takeDmReplies(this.id, this.incarnation.inc, member).then((replies) => {
      for (const [ackId, reply] of replies) this.acks.settle(ackId, reply)
    }, reportRoomError)
  }

  private dmAcked(envelope: RoomDmAckEnvelope): void {
    this.acks.settle(envelope.ackId, envelope)
    void dropDmReply(this.id, this.incarnation.inc, envelope.to, envelope.ackId).catch(reportRoomError)
  }

  private relayControl(event: PresenceEvent, wireText: () => string): void {
    if (this.stubs.size === 0) return
    const text = wireText()
    const hiddenMember = hiddenMemberOf(event)
    // Each attach sends the roster and the room's meta again, which carry every event but the close.
    const opts = { resentOnAttach: event.__r !== 'closed' }
    for (const stub of this.stubs) stub.relayControl(text, hiddenMember, opts)
  }

  /** A local participant takes each update its member's meta takes here. */
  onMemberMeta(id: string, update: MemberMetaUpdate): void {
    this.owners.get(id)?.acceptMeta(id, update)
  }

  onMemberMetaGap(): void {
    this.authority.memberMetaGap()
  }

  /** Every leave the model applies, event or reconcile, runs the member's cleanup. */
  onMemberLeft(id: string, cause: LeaveCause, hidden: boolean | null): void {
    this.acks.settleMember(id, DM_FAILURE.left) // strand no waiter on a gone sender or recipient
    for (const handle of this.receivers) handle.local.forgetMember(id)
    const owner = this.owners.get(id)
    if (owner !== undefined) {
      this.disown(id, owner)
      owner.memberLeft(id, cause)
      this.reconsider(owner)
    }
    // Every leave of a known member reaches the clients here, once, whatever applied it.
    if (hidden !== null) {
      const leave: Extract<PresenceEvent, { __r: 'leave' }> = { __r: 'leave', id, ...leaveCauseToWire(cause) }
      if (hidden) leave.hidden = true
      this.relayControl(leave, () => unorderedEventText(leave))
    }
    for (const stub of this.stubs) {
      stub.forgetMember(id)
      if (stub.forgetWantsOf(id)) this.lanePlan.holderChanged(stub, [id])
    }
    this.lanePlan.memberReleased(id)
  }

  /** The room closed. Runs once, after the `closed` event has been applied and relayed. */
  private teardown(): void {
    this.acks.settleAll(DM_FAILURE.roomClosed) // no recipient will reply now
    this.roster.clear()
    this.incomingDemand.close()
    this.publishedTracks.clear()
    for (const [id, owner] of this.owners) this.disown(id, owner)
    for (const handle of [...this.served]) {
      handle.teardown()
      this.reconsider(handle)
    }
    this.demandExchange.close()
    void this.lanePlan.replan()
  }
}

/** The owner that wrote the event flags a hidden member; room-level events always reach clients. */
function hiddenMemberOf(event: PresenceEvent): string | null {
  if (event.__r === 'join' || event.__r === 'leave' || event.__r === 'p-meta') {
    return event.hidden === true ? event.id : null
  }
  return null
}
