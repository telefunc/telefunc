export { RoomReplica }
export type { PresenceEvent }

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../../../utils/assert.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { getRoomBackend } from '../../backend/install.js'
import type { LaneId } from '../../backend/room/contract.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../../shared-ws.js'
import { decodeBinaryFrame, laneTrack, publicTrack, type BinaryWantChange } from '../binary.js'
import { DemandReport, RoomDemand } from '../demand.js'
import { DM_FAILURE } from '../errors.js'
import { joinedMember, leaveCauseFromWire, leaveCauseToWire } from '../protocol.js'
import type {
  DemandEntry,
  MemberSnapshot,
  RoomConfigRecord,
  RoomCtrlEnvelope,
  RoomDataEnvelope,
  RoomDmAckEnvelope,
  RoomDmEnvelope,
  RoomEnvelope,
  SenderStamp,
} from '../protocol.js'
import { Slice } from '../slice.js'
import { RoomModel } from '../state.js'
import type { LeaveCause } from '../types.js'
import { AuthoritySync } from './authority-sync.js'
import { PendingDmAcks } from './dm-acks.js'
import { reportRoomError } from './errors.js'
import { GossipOutbox } from './gossip.js'
import {
  CONTROL_LANE,
  SEMANTIC_LANE,
  commitRoomLane,
  decodeDemandPage,
  decodeLaneEnvelope,
  decodeRoomText,
  encodeRoomRecord,
  openConfig,
  withinRoomHorizon,
} from './lanes.js'
import type { RoomIncarnation } from './lifecycle.js'
import { evictMember, finishSessionCleanups, holdViews, removeView, renewSession } from './membership.js'
import type { StoredRoomMeta } from './meta.js'
import type { LaneHolder, TextWantsChange } from './replay.js'
import type { ServerRoom } from './room.js'
import { RosterStream } from './roster-stream.js'
import { RoomSession } from './session.js'
import { MemberMessageTexts, type RoomStubChannel, unorderedEventText } from './stub.js'
import { RoomSubscriptions } from './subscriptions.js'
assertIsNotBrowser()

/** What the control lane carries for the model: everything but another process's ask, which the report answers. */
type PresenceEvent = Exclude<RoomCtrlEnvelope, { __r: 'demand-ask' }>

/** This process's one view of a room incarnation, whatever the number of its handles: the model, the lanes planned
 *  from the wants of every handle's holders, the demand, the session hosting the members and pages the handles hold,
 *  and the heartbeat that renews it and checks the authority again. Each handle keeps its guards, its tail, the stubs it opened and its
 *  listeners. A replica no handle observes, owns a member through or tails runs no heartbeat and holds no lane; it
 *  lives while a handle holds it. */
class RoomReplica {
  private static readonly _byInc = new Map<string, WeakRef<RoomReplica>>()
  private static readonly _collected = new FinalizationRegistry<string>((inc) => {
    if (RoomReplica._byInc.get(inc)?.deref() === undefined) RoomReplica._byInc.delete(inc)
  })

  /** The incarnation's replica, while a handle or its own work holds it. */
  static of(inc: string): RoomReplica | undefined {
    return RoomReplica._byInc.get(inc)?.deref()
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
    const replica = new RoomReplica({ id: roomId, inc: config.inc, hold: config.hold }, stored, seed)
    RoomReplica._byInc.set(config.inc, new WeakRef(replica))
    RoomReplica._collected.register(replica, config.inc)
    return replica
  }

  readonly id: string
  readonly _inc: string
  readonly _incarnation: RoomIncarnation
  readonly model: RoomModel
  readonly demand: RoomDemand
  /** What every holder here receives, told to the other processes and to `demand`. */
  readonly report = new DemandReport((entry, on) => this._tellDemand(entry, on))
  readonly plan: RoomSubscriptions
  readonly sync: AuthoritySync
  readonly roster: RosterStream
  /** Hosts the members the handles join and the pages' views. */
  readonly session = new RoomSession()
  /** In-flight `send(…, { ack: true })`s; a leave of either end, or a close, fails the ones it strands. */
  readonly acks = new PendingDmAcks()
  /** Every handle's attached stubs. */
  readonly stubs = new Set<RoomStubChannel>()
  /** Each stub's latest view write, from its attach or a rewrite, to the key it wrote, null if it failed; its removal
   *  comes after it. */
  private readonly _views = new Map<RoomStubChannel, Promise<string | null>>()
  /** A view write failed, or the session hosting the views was lost: the heartbeat writes them again. */
  private _viewsLost = false
  /** Members whose eviction failed, by id, with their identity: the session would host them for as long as it lives. */
  private readonly _unevicted = new Map<string, string | null>()
  /** Whether this process tells its demand again, and was asked to again meanwhile. */
  private _tellingAgain = false
  private _askedAgain = false
  /** The cleanups of the session's members pending at the last heartbeat. */
  private _pendingCleanups: ReadonlySet<string> = new Set()
  private readonly _gossip: GossipOutbox
  /** The handle holding each member it joined. */
  private readonly _owners = new Map<string, ServerRoom>()
  /** The handles with a client stub, a participant or a listener; those with a listener; those with a tail; those with
   *  either, which take what a lane carries. */
  private readonly _observers = new Set<ServerRoom>()
  private readonly _listening = new Set<ServerRoom>()
  private readonly _tails = new Set<ServerRoom>()
  private readonly _receivers = new Set<ServerRoom>()

  private constructor(
    incarnation: RoomIncarnation,
    stored: StoredRoomMeta,
    seed: { members: MemberSnapshot[] } | { count: number },
  ) {
    this.id = incarnation.id
    this._inc = incarnation.inc
    this._incarnation = incarnation
    this.model = new RoomModel({
      roomId: incarnation.id,
      meta: stored.entries,
      seed,
      metaSeq: stored.seq,
      onMetaWaiting: () => this.sync.metaWaiting(),
      onMemberMetaGap: () => this.sync.memberMetaGap(),
      onLeave: (id, cause, hidden) => this._onLeave(id, cause, hidden),
    })
    this._gossip = new GossipOutbox(incarnation.id, incarnation.inc, this.report.instance)
    this.demand = new RoomDemand(
      (id) => this._owners.has(id),
      () => this._owners.keys(),
      (member, track, wanted) => this._owners.get(member)?._deliverDemand(member, track, wanted),
      (instance) => this._askDemand(instance),
    )
    this.plan = new RoomSubscriptions(this, this.report)
    this.sync = new AuthoritySync(this, this.plan)
    this.roster = new RosterStream(this.model, this.stubs, () => this.sync.ensureRoster())
  }

  /** Whether the handle's holders and holds are served: its stubs, participants and listeners, and its tail. */
  track(handle: ServerRoom): void {
    toggle(this._observers, handle, handle._observed())
    toggle(this._listening, handle, handle._state.listenerCount > 0)
    toggle(this._tails, handle, handle._holdsTail())
    toggle(this._receivers, handle, this._listening.has(handle) || this._tails.has(handle))
  }

  own(member: string, handle: ServerRoom): void {
    this._owners.set(member, handle)
    this.demand.owned(member)
  }

  /** The member's demand goes with it. */
  disown(member: string, handle: ServerRoom): void {
    if (this._owners.get(member) !== handle) return
    this._owners.delete(member)
    this.demand.forgetMember(member)
  }

  attachStub(stub: RoomStubChannel): void {
    this.stubs.add(stub)
    this._views.set(
      stub,
      this._holdViews([stub]).then(([key]) => key!),
    )
  }

  detachStub(stub: RoomStubChannel): void {
    this.stubs.delete(stub)
    const view = this._views.get(stub)
    this._views.delete(stub)
    void view
      ?.then(async (key) => {
        if (key !== null) await removeView(this._incarnation, key)
      })
      .catch(reportRoomError)
    this.roster.forget(stub)
  }

  /** The stubs' views' keys, in order; each null and the views lost if the write failed. */
  private async _holdViews(stubs: RoomStubChannel[]): Promise<(string | null)[]> {
    try {
      const keys = await holdViews(
        this._incarnation,
        this.session,
        stubs.map((stub) => stub.id),
      )
      return stubs.map((_, i) => keys[i] ?? null)
    } catch (error) {
      this._viewsLost = true
      reportRoomError(error)
      return stubs.map(() => null)
    }
  }

  /** The owned member's tracks someone wants. */
  demandOf(member: string): Array<string | null> {
    return this.demand.wanted(member).map(publicTrack)
  }

  /** Applies a presence event to the model and relays what it changed to the clients: as the lane carried it when
   *  `carried` is given, else as applied. A leave reaches them from `_onLeave`, whatever applied it, and an update as
   *  each one it let apply, in seq order. */
  applyControl(event: PresenceEvent, carried?: () => string): void {
    const relay = () => this._relayControl(event, carried ?? (() => unorderedEventText(event)))
    switch (event.__r) {
      case 'join':
        if (!this.model.applyJoin(joinedMember(event))) return
        this.plan.memberChanged(event.id)
        relay()
        return
      case 'leave':
        this.model.applyLeave(event.id, leaveCauseFromWire(event))
        return
      case 'p-meta': {
        const applied = this.model.applyParticipantMeta(event.id, event)
        this._owners.get(event.id)?._acceptMeta(event.id, event)
        if (applied) relay()
        return
      }
      case 'update':
        for (const applied of this.model.applyRoomUpdate(event))
          this._relayControl(applied, () => unorderedEventText(applied))
        return
      case 'closed':
        if (!this.model.applyClosed()) return
        relay()
        this._teardown()
    }
  }

  /** A stamp newer than the sender's meta here is the update this view missed so far: it applies and relays first, so
   *  every client here names the sender as of the send. */
  applyStamp(id: string, { fromMeta, fromMetaSeq }: SenderStamp): void {
    const hidden = this.model.memberHidden(id)
    if (hidden === null || this.model.memberMetaSeq(id)! >= fromMetaSeq) return
    this.applyControl({ __r: 'p-meta', id, meta: fromMeta, seq: fromMetaSeq, ...(hidden ? { hidden: true } : {}) })
  }

  async replayText(holder: LaneHolder, before: TextWantsChange): Promise<void> {
    if (before.all) return
    await this.sync.ensureRoster()
    // Read retained only after subscription readiness: a racing commit is then retained or live, never lost in the gap.
    await withinRoomHorizon(this.plan.semanticReady)
    const stored = await getRoomBackend().readRetained(this.id, this._inc, SEMANTIC_LANE)
    if (stored === null) return
    const serialized = decodeRoomText(stored.payload)
    const event = parse(serialized) as RoomDataEnvelope
    // A member whose own want didn't change is wanted now only by the new room-wide want, if at all.
    const had = before.members.get(event.from) ?? holder._textDemand().members.has(event.from)
    if (had || !holder._wantsTextFrom(event.from) || !this.model.hasMember(event.from)) return
    // Replay the stored order as-is; the holder admits it only if it is newer than what it has.
    holder._emitRetainedText(event, { seq: stored.seq, timestamp: stored.timestamp }, serialized)
  }

  /** Binary uses the same readiness handoff and stored receipt; its holder dedupes the live/retained race per lane, so a
   *  frame it already has is not delivered again. A want for one member's track reads that lane; any other lists the
   *  retained lanes it covers. As with text, a frame replays only while its member is in the roster: a leave clears the
   *  member's retained frames, and one read just before that is gone with it. */
  async replayBinary(holder: LaneHolder, { member, track }: BinaryWantChange): Promise<void> {
    await this.sync.ensureRoster()
    await (member === null ? this.plan.binaryReady() : this.plan.pairsReady(member, track))
    const backend = getRoomBackend()
    const lanes: LaneId[] =
      member !== null && track !== null
        ? [{ kind: 'binary', member, track }]
        : await backend.listRetained(
            this.id,
            this._inc,
            member !== null ? { member } : track !== null ? { track } : undefined,
          )
    for (const lane of lanes) {
      if (lane.kind !== 'binary' || !holder._wantsBinary(lane.member, lane.track)) continue
      const stored = await backend.readRetained(this.id, this._inc, lane)
      if (stored === null || !this.model.hasMember(lane.member)) continue
      const framed = stored.payload
      const frame = decodeBinaryFrame(framed)
      assert(frame)
      holder._emitRetainedBinary(frame, { seq: stored.seq, timestamp: stored.timestamp }, framed)
    }
  }

  // The planner's and the sync's host

  /** @internal Whether a handle here observes the room: a client stub, a participant, or a listener. */
  _observed(): boolean {
    return this._observers.size > 0
  }
  /** @internal */
  _listened(): boolean {
    return this._listening.size > 0
  }
  /** @internal A pre-attach tail ingests all text. */
  _holdsTail(): boolean {
    return this._tails.size > 0
  }
  /** @internal */
  _ownsMember(id: string): boolean {
    return this._owners.has(id)
  }
  /** @internal Live. A pending admission owns its inbox. */
  _ownedMembers(): Iterable<string> {
    return this._owners.keys()
  }
  /** The heartbeat evicts the member again, as `disconnected`. */
  evictLater(id: string, identity: string | null): void {
    this._unevicted.set(id, identity)
  }

  /** @internal Renews the session, evicts again whom it failed to, and finishes the leaves its members' evicting
   *  servers left unfinished; once the session was lost, or a view write failed, writes every stub's view again in one
   *  write, after each one's earlier write. */
  async _renewSession(): Promise<void> {
    if (!(await renewSession(this._incarnation, this.session))) this._viewsLost = true
    for (const [id, identity] of this._unevicted) {
      await evictMember(this._incarnation, id, identity, { type: 'disconnected' })
      this._unevicted.delete(id)
    }
    this._pendingCleanups = await finishSessionCleanups(this._incarnation, this.session, this._pendingCleanups)
    if (!this._viewsLost) return
    this._viewsLost = false
    const stubs = [...this._views.keys()]
    if (stubs.length === 0) return
    const previous = Promise.all(this._views.values())
    const keys = previous.then(() => this._holdViews(stubs))
    stubs.forEach((stub, i) =>
      this._views.set(
        stub,
        keys.then((written) => written[i]!),
      ),
    )
    await keys
  }
  /** This process's demand entry, to the other processes and to its own owners. */
  private _tellDemand(entry: DemandEntry, on: boolean): void {
    this._gossip.push({ on, ...entry })
    this.demand.take(this.report.instance, entry, on)
  }
  /** @internal Renews this process's demand elsewhere with one page, and drops the reporters that lapsed, a slice at a
   *  time. */
  async _renewDemand(): Promise<void> {
    if (this.report.reporting) this._gossip.renew()
    const slice = new Slice()
    for (const _ of this.demand.expire()) if (slice.over) await slice.next()
  }
  /** Tells every entry of this process's demand again as all it receives, a slice at a time; once more after it when
   *  asked again meanwhile, as an asker may not have read what it told before the ask. */
  private async _tellDemandAgain(): Promise<void> {
    if (this._tellingAgain) return void (this._askedAgain = true)
    this._tellingAgain = true
    try {
      do {
        this._askedAgain = false
        if (!this.report.reporting) continue
        this._gossip.push({ again: true })
        const slice = new Slice()
        for (const _ of this.report.all()) if (slice.over) await slice.next()
        this._gossip.push({ again: false })
      } while (this._askedAgain)
    } finally {
      this._tellingAgain = false
    }
  }
  /** @internal */
  _onDemand(serialized: string): void {
    const page = decodeDemandPage(serialized)
    if (page.instance !== this.report.instance) this.demand.applyPage(page)
  }
  /** @internal Asks `of`, or every other process, to tell its demand again. */
  _askDemand(of?: string): void {
    const ask = { __r: 'demand-ask', instance: this.report.instance, ...(of && { of }) } satisfies RoomCtrlEnvelope
    // A stale commit is a closed room's, which has no demand.
    void commitRoomLane(this.id, this._inc, CONTROL_LANE, encodeRoomRecord(ask)).catch(reportRoomError)
  }
  /** @internal */
  _forgetDemand(): void {
    this.demand.forgetLeased()
  }
  /** @internal */
  async _readOpenConfig(): Promise<RoomConfigRecord | null> {
    return openConfig(await getRoomBackend().readHead(this.id), this._inc)
  }
  /** @internal A newer meta reaching this view through the authority never reached its clients as an event. */
  _applyAuthorityMeta({ entries, seq }: StoredRoomMeta): void {
    this.applyControl({ __r: 'update', seq, meta: entries })
  }
  /** @internal The authority says the room closed; the lane that would have carried `closed` failed. */
  _closeFromAuthority(): void {
    this.applyControl({ __r: 'closed' })
  }
  /** @internal */
  _onControl(serialized: string, info: WirePublishInfo): void {
    this.sync.controlDelivered(info.seq)
    const ctrl = decodeLaneEnvelope(serialized) as RoomCtrlEnvelope
    if (ctrl.__r === 'demand-ask') {
      const self = this.report.instance
      if (ctrl.instance !== self && (ctrl.of ?? self) === self) void this._tellDemandAgain().catch(reportRoomError)
      return
    }
    this.applyControl(ctrl, () => encodePublishText(serialized, info))
  }
  /** @internal */
  _onTextData(serialized: string, info: WirePublishInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDataEnvelope | Extract<RoomEnvelope, { __r: 'announce' }>
    if (envelope.__r === 'announce') {
      for (const handle of this._receivers) handle._local.relayAnnouncement(envelope.data, info)
      const wireText = encodePublishText(serialized, info)
      for (const stub of this.stubs) stub._relayAnnouncement(wireText, info)
      return
    }
    this.applyStamp(envelope.from, envelope)
    for (const handle of this._receivers) handle._relayData(serialized, envelope, info)
    if (this.stubs.size === 0) return
    const texts = new MemberMessageTexts(serialized, info, () => ({
      __r: 'data',
      from: envelope.from,
      data: envelope.data,
    }))
    for (const stub of this.stubs) stub._relayText(serialized, texts, envelope.from, info)
  }
  /** @internal */
  _onBinary(framed: Uint8Array, info: WirePublishInfo): void {
    const unframed = decodeBinaryFrame(framed)
    assert(unframed)
    for (const handle of this._receivers) handle._local.relayBinary(unframed, info)
    if (this.stubs.size === 0) return
    const wireData = encodePublishBinary(framed, info)
    const track = laneTrack(unframed.track)
    for (const stub of this.stubs) stub._relayBinary(wireData, unframed.from, track, info)
  }
  /** @internal A DM for a member a handle here owns, routed to that handle. */
  _onDm(serialized: string, info: WirePublishInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDmEnvelope | RoomDmAckEnvelope
    // A reply to one of our own `send(…, { ack: true })`s, riding our inbox back home.
    if (envelope.__r === 'dm-ack') return this.acks.settle(envelope.ackId, envelope)
    this._owners.get(envelope.to)?._onDm(envelope, serialized, info)
  }

  private _relayControl(event: PresenceEvent, wireText: () => string): void {
    if (this.stubs.size === 0) return
    const text = wireText()
    const hiddenMember = hiddenMemberOf(event)
    // Each attach sends the roster and the room's meta again, which carry every event but the close.
    const resentOnAttach = event.__r !== 'closed'
    for (const stub of this.stubs) stub._relayControl(text, hiddenMember, resentOnAttach)
  }

  /** Every leave the model applies, event or reconcile, runs the member's cleanup. */
  private _onLeave(id: string, cause: LeaveCause, hidden: boolean | null): void {
    this.acks.settleMember(id, DM_FAILURE.left) // strand no waiter on a gone sender or recipient
    for (const handle of this._receivers) handle._local.forgetMember(id)
    const owner = this._owners.get(id)
    if (owner !== undefined) {
      this.disown(id, owner)
      owner._memberLeft(id, cause)
    }
    // Every leave of a known member reaches the clients here, once, whatever applied it.
    if (hidden !== null) {
      const leave = { __r: 'leave', id, ...leaveCauseToWire(cause), ...(hidden ? { hidden: true } : {}) } as const
      this._relayControl(leave, () => unorderedEventText(leave))
    }
    for (const stub of this.stubs) {
      stub._forgetMember(id)
      if (stub._forgetWantsOf(id)) this.plan.holderChanged(stub, [id])
    }
    this.plan.memberReleased(id)
  }

  /** The room closed. Runs once, after the `closed` event has been applied and relayed. */
  private _teardown(): void {
    this.acks.settleAll(DM_FAILURE.roomClosed) // no recipient will reply now
    this.roster.clear()
    this.demand.close()
    for (const [id, owner] of this._owners) this.disown(id, owner)
    for (const handle of new Set([...this._observers, ...this._tails])) handle._teardown()
    this._gossip.close()
    void this.plan.replan()
  }
}

/** The owner that wrote the event flags a hidden member; room-level events always reach clients. */
function hiddenMemberOf(event: PresenceEvent): string | null {
  if (event.__r === 'join' || event.__r === 'leave' || event.__r === 'p-meta')
    return event.hidden === true ? event.id : null
  return null
}

function toggle<T>(set: Set<T>, value: T, on: boolean): void {
  if (on) set.add(value)
  else set.delete(value)
}
