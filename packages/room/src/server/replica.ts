export { RoomReplica }
export type { PresenceEvent }

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../utils/assert.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { getRoomBackend } from '../backend/plane.js'
import type { LaneId } from '../backend/contract.js'
import { encodePublish, hostLocal, type OrderingInfo } from 'telefunc/__internal'
import { EVERY_MEMBER, EVERY_TRACK, decodeBinaryFrame, type BinaryWantChange } from '../binary.js'
import { DemandReport, RoomDemand, type DemandReportOwner, type RoomDemandOwner } from './demand.js'
import { PublishedTracks } from './PublishedTracks.js'
import { DM_FAILURE } from '../errors.js'
import { joinedMember, leaveCauseFromWire, leaveCauseToWire } from '../protocol.js'
import type {
  MemberMetaUpdate,
  MemberSnapshot,
  RoomCtrlEnvelope,
  RoomDmEnvelope,
  RoomEnvelope,
  SenderStamp,
} from '../protocol.js'
import type { DemandEntry, RoomConfigRecord, RoomDataEnvelope, RoomDmAckEnvelope } from './protocol.js'
import { Slice } from '../Slice.js'
import { RoomModel, type ServerModelOwner } from '../model.js'
import type { LeaveCause } from '../types.js'
import { AuthoritySync, type AuthoritySyncOwner } from './AuthoritySync.js'
import { PendingDmAcks } from './dmAcks.js'
import { reportRoomError } from './errors.js'
import { GossipOutbox } from './gossip.js'
import {
  SEMANTIC_LANE,
  commitLane,
  controlLane,
  decodeDemandPage,
  decodeLaneEnvelope,
  decodeRoomText,
  encodeRoomRecord,
  openConfig,
  withinRoomHorizon,
} from './lanes.js'
import type { RoomIncarnation } from './lifecycle.js'
import { dropDmReply, takeDmReplies } from './dmReplies.js'
import { evictMember } from './members.js'
import { finishLeaseCleanups, holdViews, removeView, renewLease } from './leases.js'
import type { StoredRoomMeta } from './meta.js'
import type { LaneHolder, ReplayHold, TextWantsChange } from './replay.js'
import type { MetaMirror, ServerRoom } from './room.js'
import { RosterStream, type RosterStreamOwner } from './RosterStream.js'
import { toggle } from './sets.js'
import { RoomLease } from './lease.js'
import { MemberMessageTexts, type RoomStub, unorderedEventText } from './stub.js'
import { LanePlan, type LanePlanOwner } from './LanePlan.js'
import { RoomHeartbeat, type RoomHeartbeatOwner } from './heartbeat.js'
assertIsNotBrowser()

/** What the control lane carries for the model: everything but another process's ask, which the report answers. */
type PresenceEvent = Exclude<RoomCtrlEnvelope, { __r: 'demand-ask' }>

const REPLICAS = Symbol.for('telefunc.room.replicas')

/** Per channel host: a listener may do I/O only in its own Cloudflare session DO. */
function replicasHere(): Map<string, WeakRef<RoomReplica>> {
  return hostLocal(REPLICAS, () => new Map<string, WeakRef<RoomReplica>>())
}

/** This process's one view of a room incarnation, a Cloudflare session DO's own, whatever the number of its handles:
 *  the model, the lanes planned from the wants of every handle's holders, the demand, the lease hosting the members
 *  and pages the handles hold, and the heartbeat that renews it and checks the authority again. Each handle keeps its
 *  guards, its tail, the stubs it opened and its listeners. A replica no handle observes, owns a member through or
 *  tails runs no heartbeat and holds no lane; it lives while a handle holds it. */
class RoomReplica
  implements
    ServerModelOwner,
    DemandReportOwner,
    RoomDemandOwner,
    LanePlanOwner,
    AuthoritySyncOwner,
    RoomHeartbeatOwner,
    RosterStreamOwner
{
  private static readonly collected = new FinalizationRegistry<{
    replicas: Map<string, WeakRef<RoomReplica>>
    inc: string
  }>(({ replicas, inc }) => {
    if (replicas.get(inc)?.deref() === undefined) replicas.delete(inc)
  })

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
    RoomReplica.collected.register(replica, { replicas, inc: config.inc })
    return replica
  }

  readonly id: string
  readonly incarnation: RoomIncarnation
  readonly model: RoomModel
  readonly incomingDemand: RoomDemand
  readonly publishedTracks = new PublishedTracks()
  /** What every holder here receives, told to the other processes and to `demand`. */
  readonly outgoingDemand = new DemandReport({ owner: this })
  readonly lanePlan: LanePlan
  readonly authority: AuthoritySync
  readonly heartbeat: RoomHeartbeat
  readonly roster: RosterStream
  /** Hosts the members the handles join and the pages' views. */
  readonly lease = new RoomLease()
  /** In-flight `send(…, { ack: true })`s; a leave of either end, or a close, fails the ones it strands. */
  readonly acks = new PendingDmAcks()
  /** Every handle's attached stubs. */
  readonly stubs = new Set<RoomStub>()
  /** The meta of each participant a client holds, which the client mirrors. */
  readonly metaMirrors = new Set<MetaMirror>()
  /** Each stub's latest view write, from its attach or a rewrite, to the key it wrote, null if it failed; its removal
   *  comes after it. */
  private readonly views = new Map<RoomStub, Promise<string | null>>()
  /** A view write failed, or the lease hosting the views was lost: the heartbeat writes them again. */
  private viewsLost = false
  /** Members whose eviction failed: the lease would host them for as long as it lives. */
  private readonly unevicted = new Set<string>()
  /** Whether this process tells its demand again, and was asked to again meanwhile. */
  private tellingAgain = false
  private askedAgain = false
  /** The cleanups of the lease's members pending at the last heartbeat. */
  private pendingCleanups: ReadonlySet<string> = new Set()
  private readonly gossip: GossipOutbox
  /** The handle holding each member it joined. */
  private readonly owners = new Map<string, ServerRoom>()
  /** The seq of the last frame each owned member's inbox delivered, 0 before its first. */
  private readonly inboxSeqs = new Map<string, number>()
  /** The handles with a client stub, a participant or a listener; those with a listener; those with a tail; those with
   *  either, which take what a lane carries. */
  private readonly observers = new Set<ServerRoom>()
  private readonly listening = new Set<ServerRoom>()
  private readonly tails = new Set<ServerRoom>()
  private readonly receivers = new Set<ServerRoom>()

  private constructor({
    incarnation,
    stored,
    seed,
  }: {
    incarnation: RoomIncarnation
    stored: StoredRoomMeta
    seed: { members: MemberSnapshot[] } | { count: number }
  }) {
    this.id = incarnation.id
    this.incarnation = incarnation
    this.model = new RoomModel({
      roomId: incarnation.id,
      meta: stored.entries,
      seed,
      metaSeq: stored.seq,
      owner: this,
    })
    this.gossip = new GossipOutbox(incarnation, this.outgoingDemand.instance)
    this.incomingDemand = new RoomDemand({ owner: this })
    this.lanePlan = new LanePlan({ owner: this })
    this.authority = new AuthoritySync({ owner: this })
    this.heartbeat = new RoomHeartbeat({ owner: this })
    this.roster = new RosterStream({ owner: this })
  }

  /** Whether the handle's holders and holds are served: its stubs, participants and listeners, and its tail. */
  track(handle: ServerRoom): void {
    toggle(this.observers, handle, handle._observed())
    toggle(this.listening, handle, handle._state.listenerCount > 0)
    toggle(this.tails, handle, handle._holdsTail())
    toggle(this.receivers, handle, this.listening.has(handle) || this.tails.has(handle))
  }

  own(member: string, handle: ServerRoom): void {
    this.owners.set(member, handle)
    // A member's id is fresh at each join, so its inbox starts before seq 1.
    this.inboxSeqs.set(member, 0)
    this.incomingDemand.owned(member)
  }

  /** The member's demand goes with it. */
  disown(member: string, handle: ServerRoom): void {
    if (this.owners.get(member) !== handle) return
    this.owners.delete(member)
    this.inboxSeqs.delete(member)
    this.incomingDemand.forgetMember(member)
    this.publishedTracks.forget(member)
  }

  attachStub(stub: RoomStub): void {
    this.stubs.add(stub)
    this.views.set(
      stub,
      this.holdViews([stub]).then(([key]) => key ?? null),
    )
  }

  detachStub(stub: RoomStub): void {
    this.stubs.delete(stub)
    const view = this.views.get(stub)
    this.views.delete(stub)
    void view
      ?.then(async (key) => {
        if (key !== null) await removeView(this.incarnation, key)
      })
      .catch(reportRoomError)
    this.roster.forget(stub)
  }

  /** The stubs' views' keys, in order; each null and the views lost if the write failed. */
  private async holdViews(stubs: RoomStub[]): Promise<(string | null)[]> {
    try {
      const keys = await holdViews(
        this.incarnation,
        this.lease,
        stubs.map((stub) => stub.end.id),
      )
      return stubs.map((_, i) => keys[i] ?? null)
    } catch (error) {
      this.viewsLost = true
      reportRoomError(error)
      return stubs.map(() => null)
    }
  }

  /** Applies a presence event to the model and relays what it changed to the clients: as the lane carried it when
   *  `carried` is given, else as applied. A leave reaches them from `onMemberLeft`, whatever applied it, and an update as
   *  each one it let apply, in seq order. */
  applyControl(event: PresenceEvent, carried?: () => string): void {
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
    const stored = await getRoomBackend().readRetained(this.id, this.incarnation.inc, SEMANTIC_LANE)
    if (stored === null) return
    const serialized = decodeRoomText(stored.payload)
    const event = parse(serialized) as RoomDataEnvelope
    // A member whose own want didn't change is wanted now only by the new room-wide want, if at all.
    const had = before.members.get(event.from) ?? holder.textDemand().members.has(event.from)
    if (had || !holder.wantsTextFrom(event.from) || !this.model.hasMember(event.from)) return
    // Replay the stored order as-is; the holder admits it only if it is newer than what it has.
    holder.emitRetainedText(event, { seq: stored.seq, timestamp: stored.timestamp }, serialized, hold)
  }

  /** Binary uses the same readiness handoff and stored receipt; its holder dedupes the live/retained race per lane, so
   *  a frame it already has is not delivered again. A want for one member's track reads that lane; any other lists the
   *  retained lanes it covers. As with text, a frame replays only while its member is in the roster: a leave clears the
   *  member's retained frames, and one read just before that is gone with it. */
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
    const backend = getRoomBackend()
    const lanes: LaneId[] =
      member !== EVERY_MEMBER && track !== EVERY_TRACK
        ? [{ kind: 'binary', member, track }]
        : await backend.listRetained(
            this.id,
            this.incarnation.inc,
            member !== EVERY_MEMBER ? { member } : track !== EVERY_TRACK ? { track } : undefined,
          )
    for (const lane of lanes) {
      if (lane.kind !== 'binary' || !holder.wantsBinary(lane.member, lane.track)) continue
      const stored = await backend.readRetained(this.id, this.incarnation.inc, lane)
      // An unsubscribe during the read leaves nothing to deliver to, and no receipt to keep.
      if (stored === null || !holder.wantsBinary(lane.member, lane.track)) continue
      if (!this.model.hasMember(lane.member)) continue
      const framed = stored.payload
      const frame = decodeBinaryFrame(framed)
      assert(frame)
      holder.emitRetainedBinary(frame, { seq: stored.seq, timestamp: stored.timestamp }, framed, hold)
    }
  }

  // What the replica's parts ask of it

  /** Whether a handle here observes the room: a client stub, a participant, or a listener. */
  observed(): boolean {
    return this.observers.size > 0
  }

  listened(): boolean {
    return this.listening.size > 0
  }

  /** A pre-attach tail ingests all text. */
  holdsTail(): boolean {
    return this.tails.size > 0
  }

  ownsMember(id: string): boolean {
    return this.owners.has(id)
  }

  ownsAnyMember(): boolean {
    return this.owners.size > 0
  }

  /** Live. A pending admission owns its inbox. */
  ownedMembers(): Iterable<string> {
    return this.owners.keys()
  }

  deliverDemand(member: string, track: string, wanted: boolean): void {
    this.owners.get(member)?._deliverDemand(member, track, wanted)
  }

  /** The heartbeat evicts the member again, as `disconnected`. */
  evictLater(id: string): void {
    this.unevicted.add(id)
  }

  /** Renews the lease, evicts again whom it failed to, and finishes the leaves its members' evicting
   *  servers left unfinished; once the lease was lost, or a view write failed, writes every stub's view again in one
   *  write, after each one's earlier write. */
  async maintainLease(): Promise<void> {
    if (!(await renewLease(this.incarnation, this.lease))) this.viewsLost = true
    for (const id of this.unevicted) {
      await evictMember(this.incarnation, id, { type: 'disconnected' })
      this.unevicted.delete(id)
    }
    this.pendingCleanups = await finishLeaseCleanups(this.incarnation, this.lease, this.pendingCleanups)
    if (!this.viewsLost) return
    this.viewsLost = false
    const stubs = [...this.views.keys()]
    if (stubs.length === 0) return
    const previous = Promise.all(this.views.values())
    const keys = previous.then(() => this.holdViews(stubs))
    stubs.forEach((stub, i) =>
      this.views.set(
        stub,
        keys.then((written) => written[i] ?? null),
      ),
    )
    await keys
  }

  /** This process's demand entry, to the other processes and to its own owners. */
  tellDemand(entry: DemandEntry, on: boolean): void {
    this.gossip.push({ on, ...entry })
    this.incomingDemand.take(this.outgoingDemand.instance, entry, on)
  }

  /** Renews this process's demand elsewhere with one page, and drops the reporters that lapsed,
   *  a slice at a time. */
  async renewDemand(): Promise<void> {
    if (this.outgoingDemand.reporting) this.gossip.renew()
    const slice = new Slice()
    for (const _ of this.incomingDemand.expire()) if (slice.over) await slice.next()
  }

  /** Tells every entry of this process's demand again as all it receives, a slice at a time; once more after it when
   *  asked again meanwhile, as an asker may not have read what it told before the ask. */
  private async tellDemandAgain(): Promise<void> {
    if (this.tellingAgain) {
      this.askedAgain = true
      return
    }
    this.tellingAgain = true
    try {
      do {
        this.askedAgain = false
        if (!this.outgoingDemand.reporting) continue
        this.gossip.push({ again: true })
        const slice = new Slice()
        for (const _ of this.outgoingDemand.all()) if (slice.over) await slice.next()
        this.gossip.push({ again: false })
      } while (this.askedAgain)
    } finally {
      this.tellingAgain = false
    }
  }

  onDemand(serialized: string): void {
    const page = decodeDemandPage(serialized)
    if (page.instance !== this.outgoingDemand.instance) this.incomingDemand.applyPage(page)
  }

  /** Asks `of`, or every other process, to tell its demand again. */
  askDemand(of?: string): void {
    const ask: Extract<RoomCtrlEnvelope, { __r: 'demand-ask' }> = {
      __r: 'demand-ask',
      instance: this.outgoingDemand.instance,
    }
    if (of) ask.of = of
    // A stale commit is a closed room's, which has no demand.
    void commitLane(controlLane(this.id, this.incarnation.inc), encodeRoomRecord(ask)).catch(reportRoomError)
  }

  forgetDemand(): void {
    this.incomingDemand.forgetLeased()
  }

  async readOpenConfig(): Promise<RoomConfigRecord | null> {
    return openConfig(await getRoomBackend().readHead(this.id), this.incarnation.inc)
  }

  /** A newer meta reaching this view through the authority never reached its clients as an event. */
  applyAuthorityMeta({ entries, seq }: StoredRoomMeta): void {
    this.applyControl({ __r: 'update', seq, meta: entries })
  }

  /** A failover rewound the room's control lane, and its meta cells with it, which share its slot. */
  rewound(): void {
    this.model.forgetMetaSeqs()
    for (const mirror of this.metaMirrors) mirror.rewound()
    for (const stub of this.stubs) stub.relayEvent({ __r: 'rewound' })
  }

  /** The authority says the room closed; the lane that would have carried `closed` failed. */
  closeFromAuthority(): void {
    this.applyControl({ __r: 'closed' })
  }

  onControl(serialized: string, info: OrderingInfo): void {
    this.authority.controlDelivered(info.seq)
    const ctrl = decodeLaneEnvelope(serialized) as RoomCtrlEnvelope
    if (ctrl.__r === 'demand-ask') {
      const self = this.outgoingDemand.instance
      if (ctrl.instance !== self && (ctrl.of ?? self) === self) void this.tellDemandAgain().catch(reportRoomError)
      return
    }
    this.applyControl(ctrl, () => encodePublish(serialized, info))
  }

  onTextData(serialized: string, info: OrderingInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDataEnvelope | Extract<RoomEnvelope, { __r: 'announce' }>
    if (envelope.__r === 'announce') {
      for (const handle of this.receivers) handle._local.relayAnnouncement(envelope.data, info)
      const wireText = encodePublish(serialized, info)
      for (const stub of this.stubs) stub.relayAnnouncement(wireText, info)
      return
    }
    this.applyStamp(envelope.from, envelope)
    for (const handle of this.receivers) handle._relayData(serialized, envelope, info)
    if (this.stubs.size === 0) return
    const texts = new MemberMessageTexts(serialized, info, envelope)
    for (const stub of this.stubs) stub.relayText(serialized, texts, envelope.from, info)
  }

  onBinary(framed: Uint8Array, info: OrderingInfo): void {
    const unframed = decodeBinaryFrame(framed)
    assert(unframed)
    for (const handle of this.receivers) handle._local.relayBinary(unframed, info)
    if (this.stubs.size === 0) return
    const wireData = encodePublish(framed, info)
    for (const stub of this.stubs) stub.relayBinary(wireData, unframed.from, unframed.track, info)
  }

  /** A DM for a member a handle here owns, routed to that handle. */
  onDm(serialized: string, info: OrderingInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDmEnvelope | RoomDmAckEnvelope
    this.inboxFrame(envelope.to, info.seq)
    // A reply to one of our own `send(…, { ack: true })`s, riding our inbox back home.
    if (envelope.__r === 'dm-ack') return this.dmAcked(envelope)
    this.owners.get(envelope.to)?._onDm(envelope, serialized, info)
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
    this.owners.get(id)?._acceptMeta(id, update)
  }

  onMetaWaiting(): void {
    this.authority.metaWaiting()
  }

  onMemberMetaGap(): void {
    this.authority.memberMetaGap()
  }

  // A server reads its rosters whole, and shows each member as the room records it.
  pendingMember(): undefined {
    return undefined
  }

  heldConnection(): null {
    return null
  }

  /** Every leave the model applies, event or reconcile, runs the member's cleanup. */
  onMemberLeft(id: string, cause: LeaveCause, hidden: boolean | null): void {
    this.acks.settleMember(id, DM_FAILURE.left) // strand no waiter on a gone sender or recipient
    for (const handle of this.receivers) handle._local.forgetMember(id)
    const owner = this.owners.get(id)
    if (owner !== undefined) {
      this.disown(id, owner)
      owner._memberLeft(id, cause)
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
    const handles = new Set(this.observers)
    for (const tail of this.tails) handles.add(tail)
    for (const handle of handles) handle._teardown()
    this.gossip.close()
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
