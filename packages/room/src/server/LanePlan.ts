export { LanePlan }
export type { LanePlanOwner, LanePlanOptions }

import { assert } from '../utils/assert.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { getOrCreate } from '../utils/getOrCreate.js'
import { getRoomBackend } from '../backend/plane.js'
import type { RoomSubscriptionSource } from '../backend/contract.js'
import { encodeLaneKey } from '../backend/laneKey.js'
import type { BackendReceiver, BackendSubscription, OrderingInfo } from 'telefunc/__internal'
import { ROOM_MEMBER_NAMED_LANES_MAX } from './constants.js'
import { EVERY_TRACK, type BinaryWantChange, type EveryTrack, type TrackKey } from '../binary.js'
import type { DemandReport } from './demand.js'
import type { RoomConfigRecord } from './protocol.js'
import type { RoomModel } from '../state.js'
import { reportRoomError } from './errors.js'
import { LaneSubscription, type LaneSubscriptionOwner } from './LaneSubscription.js'
import { Slice } from '../Slice.js'
import type { LaneHolder } from './replay.js'
import type { RoomIncarnation } from './lifecycle.js'
import type { AuthoritySync } from './AuthoritySync.js'
import type { RoomHeartbeat } from './heartbeat.js'
import { WantsIndex, type WantsIndexOwner } from './WantsIndex.js'
import { runAll, toggle, toggleEntry } from './sets.js'
import { CONTROL_LANE, DEMAND_LANE, SEMANTIC_LANE, decodeRoomText, withinRoomHorizon } from './lanes.js'
assertIsNotBrowser()

/** Where the lanes deliver; the demand lane also tells when it delivers and when it stops. */
interface LaneReceiver {
  onControl(serialized: string, info: OrderingInfo): void
  onTextData(serialized: string, info: OrderingInfo): void
  onBinary(framed: Uint8Array, info: OrderingInfo): void
  onDm(serialized: string, info: OrderingInfo): void
  onDemand(serialized: string): void
  /** The demand lane delivers, first or again: the other processes tell their demand, which it may have missed. */
  askDemand(): void
  /** The demand lane stopped: what it renewed lapses unread. */
  forgetDemand(): void
}

/** What this process's handles are to the room, which decides the lanes it subscribes. */
interface LaneRoles {
  /** Read-only here: every change goes through the owner's methods. */
  readonly model: Pick<RoomModel, 'closed' | 'rosterKnown' | 'memberIds' | 'memberCount' | 'hasMember'>
  /** Whether a handle observes the room: a client stub, a participant, or a listener. */
  observed(): boolean
  /** Whether a handle has a listener. */
  listened(): boolean
  /** A pre-attach tail ingests all text. */
  holdsTail(): boolean
  ownsMember(id: string): boolean
  ownsAnyMember(): boolean
  /** Live. A pending admission owns its inbox. */
  ownedMembers(): Iterable<string>
  /** What this process tells of its demand, which the plan sets as it plans. */
  readonly outgoingDemand: DemandReport
}

/** The incarnation the lanes are subscribed at, and what keeps the view in step with it. */
interface LaneAuthority {
  readonly id: string
  readonly incarnation: RoomIncarnation
  /** Recovers from a gap, and refreshes the members; the heartbeat runs while the plan keeps the room. */
  readonly authority: Pick<AuthoritySync, 'reconcileAuthority' | 'refreshMembers'>
  readonly heartbeat: Pick<RoomHeartbeat, 'update'>
  readOpenConfig(): Promise<RoomConfigRecord | null>
  closeFromAuthority(): void
}

/** The replica the subscriptions serve: it aggregates every handle's holders and applies what the lanes carry. */
type LanePlanOwner = LaneReceiver & LaneRoles & LaneAuthority

type LanePlanOptions = { owner: LanePlanOwner }

/** Fan-in lanes of one kind not delivering yet, and the lanes of the other kind, by member or track, that wait for
 *  none to be; the last one's first delivery or stop replans those, as the plan's `replans`. */
type Establishing = { lanes: Set<LaneSubscription>; waiting: Set<string>; replans: 'members' | 'tracks' }

/** A lane of binary frames: a track of a member, every track of a member, or a track of every member. */
type BinaryLane = Extract<RoomSubscriptionSource['lane'], { kind: 'binary' | 'binary-all' | 'binary-track' }>

/** What a planning pass replans: each member's binary lanes, or every member's and every track's when `all`, the
 *  lanes of each of `tracks` named of every member, and some members' named pairs. */
type BinaryPlan = { all: boolean; members: Set<string>; tracks: Set<string>; pairs: Map<string, Set<string>> }

/** What this process subscribes of a room: a function of its holders' wants, ownership and membership, kept current
 *  per change, recovered within Room's horizon, and subscribed again by the heartbeat once a recovery gave up. */
class LanePlan implements LaneSubscriptionOwner, WantsIndexOwner {
  private readonly wants = new WantsIndex({ owner: this })
  /** Whether some handle observes the room, which the control lane serves. */
  private _observing = false
  private readonly control = new LaneSubscription({ owner: this, lane: CONTROL_LANE })
  private readonly semantic = new LaneSubscription({ owner: this, lane: SEMANTIC_LANE })
  /** While this process owns a member: only an owner reads demand. */
  private readonly demandLane = new LaneSubscription({ owner: this, lane: DEMAND_LANE })

  /** Named lanes, keyed by (member, track); `binaryTracks` lists each member's, `trackMembers` each track's. */
  private readonly binary = new Map<string, LaneSubscription>()
  private readonly binaryTracks = new Map<string, Set<string>>()
  private readonly trackMembers = new Map<string, Set<string>>()
  /** Each member's every-track lane, while some holder here receives all of its tracks, more than
   *  `ROOM_MEMBER_NAMED_LANES_MAX` named of it alone, or the tracks named of every member fetched by member. */
  private readonly everyTrack = new Map<string, LaneSubscription>()
  /** Each track's every-member lane, while some holder here names it of every member and the tracks are
   *  not fetched by member. */
  private readonly everyMember = new Map<string, LaneSubscription>()
  /** Whether the tracks named of every member are fetched on each member's every-track lane, as they outnumber
   *  `ROOM_MEMBER_NAMED_LANES_MAX` and the members, so a lane per track would cost more: from more tracks than members
   *  until twice as many members as tracks. */
  private byMember = false
  /** The tracks named of every member that are told as demand while every-track lanes deliver them. */
  private readonly byMemberTracks = new Set<string>()

  /** The every-track lanes not delivering yet, and the tracks whose every-member lanes wait for none to be. */
  private readonly establishingEveryTrack: Establishing = { lanes: new Set(), waiting: new Set(), replans: 'tracks' }

  /** The every-member lanes not delivering yet, and the members whose every-track lanes wait for none to be. */
  private readonly establishingEveryMember: Establishing = { lanes: new Set(), waiting: new Set(), replans: 'members' }

  /** The members some track named of every member is told without. */
  private readonly excepted = new Set<string>()
  /** Per present member, how many tracks named of it alone some holder here receives. */
  private readonly counts = new Map<string, number>()
  /** The member a planning pass counts a slice at a time, and whether a change of its wants since
   *  made the count stale. */
  private counting: { member: string; stale: boolean } | null = null
  /** The members whose named tracks exceeded `ROOM_MEMBER_NAMED_LANES_MAX` when last planned. */
  private readonly overflowing = new Set<string>()
  /** The binary lanes not ready yet, so a readiness wait never visits the ready ones. */
  private readonly binaryPending = new Set<LaneSubscription>()
  /** The wait for `binaryPending`, until another lane starts pending. */
  private _binaryReady: Promise<void> | null = null
  private readonly inbox = new Map<string, LaneSubscription>()
  private readonly recovering = new Set<LaneSubscription>()
  /** The lanes whose recovery gave up, which the heartbeat subscribes again while they are wanted. */
  private readonly dropped = new Set<LaneSubscription>()
  /** What the next planning pass replans: binary want changes and lane readiness wait for it, so a burst of them is
   *  planned from the whole want set before any lane opens. */
  private queued = newBinaryPlan()
  /** The holders gone whose wants the next planning pass drops. */
  private readonly gone = new Set<LaneHolder>()
  private planning: Promise<void> | null = null

  private readonly owner: LanePlanOwner

  constructor({ owner }: LanePlanOptions) {
    this.owner = owner
  }

  /** Whether some handle observes the room through the control lane. */
  get observing(): boolean {
    return this._observing
  }

  /** Whether this process owns an inbox, which a member it owns reads. */
  get ownsInbox(): boolean {
    return this.inbox.size > 0
  }

  get controlEstablished(): boolean {
    return this.control.established
  }

  get semanticReady(): Promise<void> {
    return this.semantic.ready
  }

  inboxOf(member: string): LaneSubscription | undefined {
    return this.inbox.get(member)
  }

  /** Plans everything in the next planning pass, resolving once it ends; it also retries every still-wanted lane whose
   *  recovery failed. */
  replan(): Promise<void> {
    this.queued.all = true
    this.schedulePlan()
    this.syncLanes()
    return this.planning!
  }

  /** Subscribes again each lane whose recovery gave up and that is still wanted, as a planning pass would. */
  resumeDropped(): void {
    for (const slot of drain(this.dropped)) slot.resume()
  }

  /** A holder attached, or its wants changed wholesale: its room-wide ones, and those of `members` when given. The next
   *  planning pass recounts each member it may have changed. */
  holderChanged(holder: LaneHolder, members?: Iterable<string>): void {
    runAll(this.indexHolder(holder, { members }))
    this.schedulePlan()
    this.syncLanes()
  }

  /** A holder is gone. A planning pass in a later turn drops its wants, so the holders gone in one turn (the clients of
   *  a mass expiry) are dropped together, a slice at a time. */
  holderGone(holder: LaneHolder): void {
    this.gone.add(holder)
    this.schedulePlan()
    this.syncLanes()
  }

  /** A holder's text wants changed: its room-wide ones, and those of `members`. */
  textChanged(holder: LaneHolder, members: Iterable<string>): void {
    runAll(this.wants.updateText(holder, { members }))
    this.syncLanes()
  }

  /** One of a holder's binary wants changed: a want of one track replans only that track's pairs, in the next
   *  planning pass. */
  binaryChanged(holder: LaneHolder, change: BinaryWantChange): void {
    const { track } = change
    const wasEveryMember = track !== EVERY_TRACK && this.wants.namesEveryMember(track)
    const affected = this.wants.applyBinary(holder, change)
    const queued = this.queued
    if (track === EVERY_TRACK) {
      if (affected === 'all') queued.all = true
      else for (const member of affected) queued.members.add(member)
    } else {
      // Only a want of every track of every member concerns every member.
      assert(affected !== 'all')
      const everyMember = this.wants.namesEveryMember(track)
      const pair: BinaryWantChange & { track: TrackKey } = { member: change.member, track, on: change.on }
      // A track counts for a member while it is named of the member alone; one named of every member counts for none.
      for (const member of affected) {
        const count = this.counts.get(member)
        if (count === undefined) {
          if (this.counting?.member === member) this.counting.stale = true
          continue
        }
        const [before, after] = this.wants.namedPairChange(holder, pair, member)
        this.counts.set(member, count + Number(!everyMember && after) - Number(!wasEveryMember && before))
      }
      for (const member of affected) this.queuePair(member, track)
      // Whom the track's every-member lane serves here may have changed.
      if (everyMember || wasEveryMember) queued.tracks.add(track)
    }
    this.schedulePlan()
    this.syncLanes()
  }

  /** A member joined, or which holders suppress it changed: the next planning pass recounts it. */
  memberChanged(member: string): void {
    this.dropCounts(member)
    this.queued.members.add(member)
    this.schedulePlan()
    this.syncLanes()
  }

  /** A member this instance now owns gets its inbox. */
  memberOwned(member: string): void {
    this.openInbox(member)
    this.syncLanes()
  }

  /** A member left, or this instance no longer owns it. */
  memberReleased(member: string): void {
    this.queued.members.add(member)
    this.closeInbox(member)
    this.schedulePlan()
    this.syncLanes()
  }

  /** The planning pass under way while a holder here receives the member's `track` and no lane here carries it yet:
   *  once it ends, the lane establishes, and holds a commit on it until it delivers. */
  lanePlanned(member: string, track: string): Promise<void> | null {
    if (this.planning === null || !this.wants.wantsBinary || this.everyTrack.has(member)) return null
    if (this.everyMember.has(track)) return null
    if (this.binary.has(encodeLaneKey({ kind: 'binary', member, track }))) return null
    if (!this.wants.receives(member, track)) return null
    return this.planning
  }

  /** The lanes delivering the member's `track` (or `EVERY_TRACK`) are ready, so a retained read after this
   *  misses no live frame. */
  async pairsReady(member: string, track: TrackKey | EveryTrack): Promise<void> {
    await this.planning
    const lanes = [this.everyTrack.get(member)]
    if (track !== EVERY_TRACK) {
      lanes.push(this.binary.get(encodeLaneKey({ kind: 'binary', member, track })))
      lanes.push(this.everyMember.get(track))
    }
    const pending: Promise<void>[] = []
    for (const lane of lanes) if (lane !== undefined && this.binaryPending.has(lane)) pending.push(lane.ready)
    if (pending.length > 0) await withinRoomHorizon(Promise.all(pending))
  }

  /** Every binary lane planned so far is ready; concurrent callers share one wait. */
  async binaryReady(): Promise<void> {
    await this.planning
    if (this.binaryPending.size === 0) return
    if (this._binaryReady === null) {
      const ready = withinRoomHorizon(Promise.all(Array.from(this.binaryPending, (lane) => lane.ready))).then(() => {})
      const clear = () => {
        if (this._binaryReady === ready) this._binaryReady = null
      }
      ready.then(clear, clear)
      this._binaryReady = ready
    }
    await this._binaryReady
  }

  /** What the room-wide lanes depend on changed (observers, listeners, the tail, the text wants): they follow it, and
   *  the demand lane and the heartbeat after them. */
  syncLanes(): void {
    const owner = this.owner
    const model = owner.model
    const open = !model.closed
    const observing = open && owner.observed()
    const becomesObserved = observing && !this._observing
    this.control.sync(
      observing,
      this.subscriber(CONTROL_LANE, (payload, info) => owner.onControl(decodeRoomText(payload), info)),
    )
    this._observing = observing
    this.semantic.sync(
      open && (owner.holdsTail() || this.wants.wantsText),
      this.subscriber(SEMANTIC_LANE, (payload, info) => owner.onTextData(decodeRoomText(payload), info)),
    )
    const needsRoster = owner.listened() || (open && this.wants.wantsBinary)
    if ((becomesObserved && model.rosterKnown) || (open && !model.rosterKnown && needsRoster)) {
      void owner.authority.refreshMembers().catch(reportRoomError)
    }
    this.syncDemandLane()
    if (this.planning === null) this.owner.outgoingDemand.flush()
    this.owner.heartbeat.update()
  }

  /** Indexes the holder's wants a step at a time, each step queueing the members it affected. */
  private *indexHolder(holder: LaneHolder, opts: { gone?: boolean; members?: Iterable<string> }): Generator<void> {
    for (const affected of this.wants.update(holder, opts)) {
      if (affected === 'all') {
        this.counts.clear()
        if (this.counting) this.counting.stale = true
        this.queued.all = true
      } else {
        for (const member of affected) {
          this.dropCounts(member)
          this.queued.members.add(member)
        }
      }
      yield
    }
  }

  private dropCounts(member: string): void {
    this.counts.delete(member)
    if (this.counting?.member === member) this.counting.stale = true
  }

  private queuePair(member: string, track: string): void {
    toggleEntry(this.queued.pairs, member, track, true)
  }

  private schedulePlan(): void {
    this.planning ??= this.runPlan().catch(reportRoomError)
  }

  /** Plans what is queued once the current turn's changes are in, then tells the demand it changed, each round sharing
   *  one `ROOM_PLAN_SLICE_MS` slice at a time so timers and I/O run between slices; what is queued meanwhile gets the
   *  next round. No other demand is told until it ends, so no owner sees the demand between two of its slices. */
  private async runPlan(): Promise<void> {
    try {
      await Promise.resolve()
      const slice = new Slice()
      for (;;) {
        const plan = this.queued
        if (this.gone.size > 0) {
          await slice.next()
          await this.replanEach(slice, drain(this.gone), (holder) => this.indexHolder(holder, { gone: true }))
        } else if (!isEmpty(plan)) {
          await this.planRound(slice, plan)
        } else if (this.owner.outgoingDemand.changed) {
          while (!this.owner.outgoingDemand.tellUntil(slice.end)) await slice.next()
        } else {
          return
        }
      }
    } finally {
      this.planning = null
      this.owner.heartbeat.update()
    }
  }

  /** One round of what was queued: the every-member lanes kept, each member planned, the every-member lanes released,
   *  and with `all`, the inboxes. */
  private async planRound(slice: Slice, plan: BinaryPlan): Promise<void> {
    this.queued = newBinaryPlan()
    if (this.syncByMember()) plan.all = true
    const owner = this.owner
    const members = chain(
      plan.members,
      plan.pairs.keys(),
      plan.all ? owner.model.memberIds() : [],
      // Lanes of members no longer present close too.
      plan.all ? this.binaryTracks.keys() : [],
      plan.all ? this.everyTrack.keys() : [],
      plan.all ? this.excepted : [],
    )
    const tracks = () =>
      plan.all ? chain(this.everyMember.keys(), this.byMemberTracks, this.wants.everyMemberTracks()) : plan.tracks
    // Every lane opens before any lane it replaces is released.
    await this.replanEach(
      slice,
      tracks(),
      once((track) => this.keepEveryMember(track)),
    )
    await this.replanEach(
      slice,
      members,
      once((member) => {
        this.syncExcept(member)
        if (!this.unconcerned(member)) return this.planMember(member, plan)
        this.counts.delete(member)
      }),
    )
    await this.replanEach(
      slice,
      tracks(),
      once((track) => this.releaseEveryMember(track)),
    )
    if (plan.all) {
      await this.replanEach(
        slice,
        chain(this.inbox.keys(), owner.ownedMembers()),
        once((member) => {
          if (!owner.model.closed && owner.ownsMember(member)) this.openInbox(member)
          else this.closeInbox(member)
        }),
      )
    }
  }

  /** Runs `step` on each item within `slice`, the room-wide lanes synced after each slice; a step that yields
   *  can end a slice at each yield. */
  private async replanEach<T>(
    slice: Slice,
    items: Iterable<T>,
    step: (item: T) => Iterable<void> | void,
  ): Promise<void> {
    const pending = stepsOf(items, step)
    for (let done = false; !done; ) {
      if (slice.over) await slice.next()
      do {
        done = pending.next().done === true
      } while (!done && !slice.over)
      this.syncLanes()
    }
  }

  /** Whether `byMember` changed. */
  private syncByMember(): boolean {
    const model = this.owner.model
    const tracks = this.wants.everyMemberTrackCount
    const members = model.memberCount
    const byMember =
      model.rosterKnown &&
      tracks > ROOM_MEMBER_NAMED_LANES_MAX &&
      (this.byMember ? tracks * 2 > members : tracks > members)
    if (byMember === this.byMember) return false
    this.byMember = byMember
    return true
  }

  /** A member's binary lanes: its every-track lane while some holder here receives all of its tracks, more than
   *  `ROOM_MEMBER_NAMED_LANES_MAX` named of it alone, or the tracks named of every member fetched by member, else a
   *  lane per track named of it alone. A switch between them keeps the lanes it leaves until the lanes it takes
   *  deliver. It yields between tracks, so a member's plan is resumed a slice at a time: it plans from the counts and
   *  lanes it read first, and every change meanwhile queues the member again. */
  private *syncMemberBinary(member: string): Generator<void> {
    const present = this.present(member)
    if (!present) this.counts.delete(member)
    const count = present ? (this.counts.get(member) ?? (yield* this.countNamed(member))) : undefined
    const wantsAll = present && this.wants.wantsEveryTrack(member)
    const overflows = this.overflows(count)
    toggle(this.overflowing, member, overflows)
    const servedByAll = wantsAll || overflows || (present && this.byMember)
    if (servedByAll) {
      this.establishingEveryMember.waiting.delete(member)
      const lane = this.everyTrack.get(member) ?? this.openEveryTrack(member)
      lane.resume()
    }
    const all = this.everyTrack.get(member)
    const allDelivers = all?.delivering === true
    this.owner.outgoingDemand.setMember(member, wantsAll && allDelivers)
    this.owner.outgoingDemand.setFetched(
      member,
      present && allDelivers && (this.byMember || this.byMemberTracks.size > 0),
    )
    for (const track of chain(this.binaryTracks.get(member) ?? [], this.owner.outgoingDemand.localTracks(member))) {
      this.syncLane(member, track, servedByAll)
      yield
    }
    // An establishing every-track lane keeps the named lanes as they are.
    for (const track of !servedByAll || allDelivers ? this.wants.memberTracksOf(member) : []) {
      this.syncLane(member, track, servedByAll)
      yield
    }
    // Stopping a lane a change meanwhile wants again would only open it again.
    const stillServed = present && (this.wants.wantsEveryTrack(member) || this.overflows(count) || this.byMember)
    if (!servedByAll && !stillServed && all !== undefined && this.everyTrack.get(member) === all) {
      this.releaseEveryTrack(member, all)
    }
  }

  /** No want, lane or demand concerns the member, so its plan has nothing to do; the change that concerns it next
   *  counts it again. */
  private unconcerned(member: string): boolean {
    return (
      !(this.byMember && this.present(member)) &&
      !this.wants.concerns(member) &&
      !this.binaryTracks.has(member) &&
      !this.everyTrack.has(member) &&
      !this.overflowing.has(member) &&
      !this.owner.outgoingDemand.receivesFrom(member)
    )
  }

  /** The member's queued pairs; a change that moved it across `ROOM_MEMBER_NAMED_LANES_MAX` replans
   *  all of its lanes. */
  private *planMember(member: string, plan: BinaryPlan): Generator<void> {
    const count = this.counts.get(member)
    if (plan.all || plan.members.has(member) || (count === undefined && this.present(member))) {
      return yield* this.syncMemberBinary(member)
    }
    const overflows = this.overflows(count)
    if (overflows !== this.overflowing.has(member)) return yield* this.syncMemberBinary(member)
    const servedByAll = count !== undefined && (this.wants.wantsEveryTrack(member) || overflows || this.byMember)
    for (const track of plan.pairs.get(member) ?? []) {
      this.syncLane(member, track, servedByAll)
      yield
    }
  }

  /** A named lane while some holder names the track of the member alone and neither the member's every-track lane nor
   *  the track's every-member lane serves it; beside either, a lane stays until it delivers. The pair is demand while
   *  some lane here delivers it to a holder that wants it, told as a pair unless the every-member lane delivers it. */
  private syncLane(member: string, track: string, servedByAll: boolean): void {
    const present = this.present(member)
    const named = present && this.wants.wantsNamedPair(member, track)
    const everyMember = this.wants.namesEveryMember(track)
    const allDelivers = this.everyTrack.get(member)?.delivering === true
    const trackDelivers = everyMember && this.everyMember.get(track)?.delivering === true
    const key = encodeLaneKey({ kind: 'binary', member, track })
    let lane = this.binary.get(key)
    let keep = named
    if (servedByAll) keep = lane !== undefined && !allDelivers
    else if (everyMember) keep = named && lane !== undefined && !trackDelivers
    if (!keep && lane !== undefined) {
      lane.stop()
      this.binary.delete(key)
      toggleEntry(this.binaryTracks, member, track, false)
      toggleEntry(this.trackMembers, track, member, false)
      lane = undefined
      if (this.everyMember.has(track)) this.queued.tracks.add(track)
    } else if (keep) {
      lane ??= this.openNamedLane(member, track)
      lane.resume()
    }
    if (everyMember) this.setExcept(track, member, present && !named)
    const delivers = allDelivers || trackDelivers || lane?.delivering === true
    this.owner.outgoingDemand.setPair(member, track, (named || lane !== undefined) && delivers && !trackDelivers)
  }

  /** The every-member lane of `track` while some holder here names it of every member and the tracks are not fetched
   *  by member. Its demand is one entry, told once it delivers, or while the every-track lanes it replaces still do. */
  private keepEveryMember(track: string): void {
    if (this.byMember || !this.namesEveryMember(track)) return
    this.establishingEveryTrack.waiting.delete(track)
    const lane = this.everyMember.get(track) ?? this.openEveryMember(track)
    lane.resume()
    if (lane.delivering) this.byMemberTracks.delete(track)
    const byMember = this.byMemberTracks.has(track)
    this.owner.outgoingDemand.setTrack(track, lane.delivering || byMember, { byMember })
  }

  /** An every-member lane that serves no holder here stops once the lanes replacing it deliver: the named lanes and
   *  every-track lanes once no holder names the track, each member's every-track lane while the tracks are fetched by
   *  member, which then tells the track's demand. Until then, each one's readiness replans the track. */
  private releaseEveryMember(track: string): void {
    const named = this.namesEveryMember(track)
    if (named && !this.byMember) return
    const lane = this.everyMember.get(track)
    if (lane === undefined && named === this.byMemberTracks.has(track)) return
    for (const member of this.trackMembers.get(track) ?? []) {
      if (!this.binary.get(encodeLaneKey({ kind: 'binary', member, track }))!.delivering) return
    }
    if (this.waitsFor(this.establishingEveryTrack, track)) return
    if (lane !== undefined) {
      this.everyMember.delete(track)
      this.stopFanIn(lane, this.establishingEveryMember)
    }
    toggle(this.byMemberTracks, track, named)
    if (named) this.owner.outgoingDemand.setTrack(track, true, { byMember: true })
    else this.owner.outgoingDemand.forgetTrack(track)
  }

  private namesEveryMember(track: string): boolean {
    return !this.owner.model.closed && this.wants.namesEveryMember(track)
  }

  /** Whether the member is one no holder here receives a track named of every member from: only a holder's own member
   *  can be, as one suppresses it, so only an owned member is read while a track has its every-member lane or is told
   *  by member, or one told so before. */
  private syncExcept(member: string): void {
    const served = this.everyMember.size > 0 || this.byMemberTracks.size > 0
    if (!this.excepted.has(member) && (!served || !this.owner.ownsMember(member))) return
    const present = this.present(member)
    let excepted = false
    for (const track of this.wants.everyMemberTracks()) {
      const unreached = present && !this.wants.wantsNamedPair(member, track)
      this.setExcept(track, member, unreached)
      excepted ||= unreached
    }
    toggle(this.excepted, member, excepted)
  }

  private setExcept(track: string, member: string, on: boolean): void {
    this.owner.outgoingDemand.setExcept(track, member, on)
    if (on) this.excepted.add(member)
  }

  private present(member: string): boolean {
    const model = this.owner.model
    return !model.closed && model.hasMember(member)
  }

  /** Counted a slice at a time, and kept unless a change of the member's wants meanwhile made it stale: the change
   *  queued the member, so the next pass counts it again. */
  private *countNamed(member: string): Generator<void, number> {
    let count = 0
    const counting = (this.counting = { member, stale: false })
    try {
      for (const track of this.wants.memberTracksOf(member)) {
        count += Number(this.wants.wantsNamedPair(member, track))
        yield
      }
    } finally {
      this.counting = null
    }
    if (!counting.stale) this.counts.set(member, count)
    return count
  }

  private overflows(count: number | undefined): boolean {
    return count !== undefined && count > ROOM_MEMBER_NAMED_LANES_MAX
  }

  private openEveryTrack(member: string): LaneSubscription {
    const lane = new LaneSubscription({ owner: this, lane: { kind: 'binary-all', member } })
    this.establishingEveryTrack.lanes.add(lane)
    this.everyTrack.set(member, lane)
    return this.subscribeBinary(lane)
  }

  private openEveryMember(track: string): LaneSubscription {
    const lane = new LaneSubscription({ owner: this, lane: { kind: 'binary-track', track } })
    this.establishingEveryMember.lanes.add(lane)
    this.everyMember.set(track, lane)
    return this.subscribeBinary(lane)
  }

  private openNamedLane(member: string, track: string): LaneSubscription {
    const binary = { kind: 'binary', member, track } as const
    const lane = new LaneSubscription({ owner: this, lane: binary })
    this.binary.set(encodeLaneKey(binary), lane)
    toggleEntry(this.binaryTracks, member, track, true)
    toggleEntry(this.trackMembers, track, member, true)
    return this.subscribeBinary(lane)
  }

  /** Subscribes a binary lane the plan already holds, which its readiness replans by. */
  private subscribeBinary(lane: LaneSubscription): LaneSubscription {
    const owner = this.owner
    lane.sync(
      true,
      this.subscriber(lane.lane, (framed, info) => owner.onBinary(framed, info)),
    )
    return lane
  }

  /** An every-track lane nothing here wants any more stops once the named lanes and every-member lanes replacing it
   *  deliver; until then, each one's readiness replans the member. */
  private releaseEveryTrack(member: string, all: LaneSubscription): void {
    for (const track of this.binaryTracks.get(member) ?? []) {
      if (!this.binary.get(encodeLaneKey({ kind: 'binary', member, track }))!.delivering) return
    }
    if (this.waitsFor(this.establishingEveryMember, member)) return
    this.everyTrack.delete(member)
    this.stopFanIn(all, this.establishingEveryTrack)
    this.owner.outgoingDemand.setFetched(member, false)
  }

  /** Whether `item`'s lane stays while a lane of the other kind is not delivering yet: a closed room waits for none. */
  private waitsFor(other: Establishing, item: string): boolean {
    const waits = !this.owner.model.closed && other.lanes.size > 0
    toggle(other.waiting, item, waits)
    return waits
  }

  private stopFanIn(lane: LaneSubscription, establishing: Establishing): void {
    lane.stop()
    this.established(lane, establishing)
  }

  private established(lane: LaneSubscription, establishing: Establishing): void {
    if (!establishing.lanes.delete(lane) || establishing.lanes.size > 0) return
    const replans = this.queued[establishing.replans]
    for (const item of establishing.waiting) replans.add(item)
  }

  private openInbox(member: string): void {
    const owner = this.owner
    if (owner.model.closed || !owner.ownsMember(member)) return
    const inbox = { kind: 'inbox', member } as const
    const slot = getOrCreate(this.inbox, member, () => new LaneSubscription({ owner: this, lane: inbox }))
    slot.sync(
      true,
      this.subscriber(inbox, (payload, info) => owner.onDm(decodeRoomText(payload), info)),
    )
  }

  private closeInbox(member: string): void {
    this.inbox.get(member)?.stop()
    this.inbox.delete(member)
  }

  private syncDemandLane(): void {
    const owner = this.owner
    const lane = this.demandLane
    if (!owner.model.closed && owner.ownsAnyMember()) {
      return lane.sync(
        true,
        this.subscriber(DEMAND_LANE, (payload) => owner.onDemand(decodeRoomText(payload))),
      )
    }
    if (!lane.wanted) return
    lane.stop()
    owner.forgetDemand()
  }

  /** What a lane's `sync` subscribes: `lane` of this incarnation, delivering to `receiver`. */
  private subscriber(lane: RoomSubscriptionSource['lane'], receiver: BackendReceiver): () => BackendSubscription {
    const owner = this.owner
    return () => getRoomBackend().subscribeLane(owner.id, owner.incarnation.inc, lane, receiver)
  }

  onEveryMemberCoverChanged(track: string): void {
    this.queued.tracks.add(track)
  }

  onLaneRecovered(): void {
    void this.owner.authority.reconcileAuthority().catch(reportRoomError)
  }

  /** The demand lane delivering asks for demand. What a binary lane carries is demand once it delivers, so its
   *  readiness replans what it serves. */
  onLanePending(slot: LaneSubscription, pending: boolean): void {
    const lane = slot.lane
    if (lane.kind === 'demand') {
      if (!pending && slot.wanted) this.owner.askDemand()
      return
    }
    if (lane.kind !== 'binary' && lane.kind !== 'binary-all' && lane.kind !== 'binary-track') return
    this.onBinaryPending(slot, pending)
    if (pending || !slot.wanted) return
    this.replanDelivering(slot, lane)
    this.schedulePlan()
  }

  private replanDelivering(slot: LaneSubscription, lane: BinaryLane): void {
    switch (lane.kind) {
      case 'binary-all':
        this.queued.members.add(lane.member)
        return this.established(slot, this.establishingEveryTrack)
      case 'binary-track':
        // The named lanes and every-track lanes it serves stop once it delivers.
        this.queued.tracks.add(lane.track)
        for (const member of this.trackMembers.get(lane.track) ?? []) this.queuePair(member, lane.track)
        return this.established(slot, this.establishingEveryMember)
      case 'binary':
        // An every-track lane beside it may wait for it to deliver, so it replans the whole member; an every-member
        // lane that holders no longer name waits for it too.
        if (this.everyTrack.has(lane.member)) this.queued.members.add(lane.member)
        else this.queuePair(lane.member, lane.track)
        if (this.everyMember.has(lane.track)) this.queued.tracks.add(lane.track)
    }
  }

  private onBinaryPending(slot: LaneSubscription, pending: boolean): void {
    if (!pending) {
      this.binaryPending.delete(slot)
      return
    }
    this.binaryPending.add(slot)
    this._binaryReady = null
  }

  onLaneTerminal(slot: LaneSubscription): void {
    // A replacement that ends is its recovery's failure.
    if (this.recovering.has(slot)) return
    this.recovering.add(slot)
    void this.recover(slot).catch(reportRoomError)
  }

  /** A still-wanted terminal lane gets one replacement after a head read; if that fails too, the heartbeat subscribes
   *  it again. */
  private async recover(slot: LaneSubscription): Promise<void> {
    let replaced = false
    try {
      const config = await withinRoomHorizon(this.owner.readOpenConfig())
      if (config === null) return this.owner.closeFromAuthority()
      slot.retry()
      replaced = true
      await withinRoomHorizon(slot.attemptReady)
    } catch (error) {
      if (slot.wanted) {
        slot.dropAttempt()
        this.dropped.add(slot)
      }
      // The backend reported the replacement's end; the horizon's RoomError goes unreported.
      if (replaced) return
      throw error
    } finally {
      this.recovering.delete(slot)
    }
    // Catch up on what the outage dropped; a later end of the lane gets its own recovery.
    await this.owner.authority.reconcileAuthority()
  }
}

/** Each source's items in turn, each source read live as the iteration reaches it. */
function* chain<T>(...sources: Iterable<T>[]): Generator<T> {
  for (const source of sources) yield* source
}

/** Each item's step, then what it yields one at a time; an item whose step yields nothing is still one step. */
function* stepsOf<T>(items: Iterable<T>, step: (item: T) => Iterable<void> | void): Generator<void> {
  for (const item of items) {
    const parts = step(item)
    if (parts) yield* parts
    yield
  }
}

/** `step` on each item it is called with the first time; an item seen before is still one call, so one step. */
function once<T, R>(step: (item: T) => R): (item: T) => R | void {
  const seen = new Set<T>()
  return (item) => {
    if (seen.has(item)) return
    seen.add(item)
    return step(item)
  }
}

/** Each item, removed as it is taken; one added meanwhile comes too. */
function* drain<T>(items: Set<T>): Generator<T> {
  for (const item of items) {
    items.delete(item)
    yield item
  }
}

function newBinaryPlan(): BinaryPlan {
  return { all: false, members: new Set(), tracks: new Set(), pairs: new Map() }
}

function isEmpty(plan: BinaryPlan): boolean {
  return !plan.all && plan.members.size === 0 && plan.tracks.size === 0 && plan.pairs.size === 0
}
