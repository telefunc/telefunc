export { RoomSubscriptions }

import { assert } from '../../../utils/assert.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { getRoomBackend } from '../../backend/install.js'
import { inProcessBinaryLaneKey } from '../../backend/room/lane-key.js'
import { reportSubscriptionEnd } from '../../backend/subscription-manager.js'
import { reportServerChannelError } from '../../server/channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import { ROOM_MEMBER_NAMED_LANES_MAX } from '../constants.js'
import type { BinaryWantChange } from '../binary.js'
import type { DemandReport } from '../demand.js'
import type { RoomConfigRecord } from '../protocol.js'
import type { RoomModel } from '../state.js'
import { reportRoomError } from './errors.js'
import { LaneSubscription } from './lane-subscription.js'
import { Slice } from '../slice.js'
import type { LaneHolder } from './replay.js'
import type { AuthoritySync } from './authority-sync.js'
import { WantsIndex } from './wants-index.js'
import { CONTROL_LANE, DEMAND_LANE, SEMANTIC_LANE, decodeRoomText, withinRoomHorizon } from './lanes.js'
assertIsNotBrowser()

/** The replica these subscriptions serve: it aggregates every handle's holders and applies what the lanes carry. */
type SubscriptionHost = {
  readonly id: string
  readonly _inc: string
  /** Read-only here: every change goes through the host's methods. */
  readonly model: Pick<RoomModel, 'closed' | 'rosterKnown' | 'memberIds' | 'memberCount' | 'hasMember'>
  /** What keeps the view in step with the authority, and runs the heartbeat. */
  readonly sync: Pick<AuthoritySync, 'reconcileAuthority' | 'refreshMembers' | 'heartbeat'>
  /** Whether a handle observes the room: a client stub, a participant, or a listener. */
  _observed(): boolean
  /** Whether a handle has a listener. */
  _listened(): boolean
  /** A pre-attach tail ingests all text. */
  _holdsTail(): boolean
  _ownsMember(id: string): boolean
  /** Live. A pending admission owns its inbox. */
  _ownedMembers(): Iterable<string>
  _onControl(serialized: string, info: WirePublishInfo): void
  _onTextData(serialized: string, info: WirePublishInfo): void
  _onBinary(framed: Uint8Array, info: WirePublishInfo): void
  _onDm(serialized: string, info: WirePublishInfo): void
  _onDemand(serialized: string): void
  /** The demand lane delivers, first or again: the other processes tell their demand, which it may have missed. */
  _askDemand(): void
  /** The demand lane stopped: what it renewed lapses unread. */
  _forgetDemand(): void
  _readOpenConfig(): Promise<RoomConfigRecord | null>
  _closeFromAuthority(): void
}

/** What a planning pass replans: each member's binary lanes, or every member's and every track's when `all`, the
 *  lanes of each of `tracks` named of every member, and some members' named pairs. */
/** Fan-in lanes of one kind not delivering yet, and the lanes of the other kind, by member or track, that wait for
 *  none to be; the last one's first delivery or stop replans those. */
type Establishing = { lanes: Set<LaneSubscription>; waiting: Set<string>; replan(item: string): void }
type BinaryPlan = { all: boolean; members: Set<string>; tracks: Set<string>; pairs: Map<string, Set<string>> }

/** What this process subscribes of a room: a function of its holders' wants, ownership and membership, kept current
 *  per change, recovered within Room's horizon, and replanned by the heartbeat. */
class RoomSubscriptions {
  private readonly _wants = new WantsIndex((track) => this._queued.tracks.add(track))
  // Every lane shares these, so an owned member's inbox lane makes none; declared before the lanes made here.
  private readonly _onLaneTerminal = (slot: LaneSubscription, error?: unknown) => this._onTerminal(slot, error)
  private readonly _onLaneRecovered = () => void this._host.sync.reconcileAuthority().catch(reportRoomError)
  private readonly _onBinaryPending = (slot: LaneSubscription, pending: boolean) => {
    if (!pending) return void this._binaryPending.delete(slot)
    this._binaryPending.add(slot)
    this._binaryReady = null
  }
  /** Whether some handle observes the room, which the control lane serves. */
  private _observing = false
  private readonly _control = this._newLaneSubscription()
  private readonly _semantic = this._newLaneSubscription()
  /** While this process owns a member: only an owner reads demand. */
  private readonly _demandLane = new LaneSubscription(this._onLaneTerminal, this._onLaneRecovered, (slot, pending) => {
    if (!pending && slot.wanted) this._host._askDemand()
  })
  /** Named lanes, keyed by (member, track); `_binaryTracks` lists each member's, `_trackMembers` each track's. */
  private readonly _binary = new Map<string, LaneSubscription>()
  private readonly _binaryTracks = new Map<string, Set<string>>()
  private readonly _trackMembers = new Map<string, Set<string>>()
  /** Each member's every-track lane, while some holder here receives all of its tracks, more than
   *  `ROOM_MEMBER_NAMED_LANES_MAX` named of it alone, or the tracks named of every member fetched by member. */
  private readonly _everyTrack = new Map<string, LaneSubscription>()
  /** Each track's every-member lane, while some holder here names it of every member and the tracks are not fetched by
   *  member. */
  private readonly _everyMember = new Map<string, LaneSubscription>()
  /** Whether the tracks named of every member are fetched on each member's every-track lane, as they outnumber
   *  `ROOM_MEMBER_NAMED_LANES_MAX` and the members, so a lane per track would cost more: from more tracks than members
   *  until twice as many members as tracks. */
  private _byMember = false
  /** The tracks named of every member that are told as demand while every-track lanes deliver them. */
  private readonly _byMemberTracks = new Set<string>()
  /** The every-track lanes not delivering yet, and the tracks whose every-member lanes wait for none to be. */
  private readonly _establishingEveryTrack: Establishing = {
    lanes: new Set(),
    waiting: new Set(),
    replan: (track) => this._queued.tracks.add(track),
  }
  /** The every-member lanes not delivering yet, and the members whose every-track lanes wait for none to be. */
  private readonly _establishingEveryMember: Establishing = {
    lanes: new Set(),
    waiting: new Set(),
    replan: (member) => this._queued.members.add(member),
  }
  /** The members some track named of every member is told without. */
  private readonly _excepted = new Set<string>()
  /** Per present member, how many tracks named of it alone some holder here receives. */
  private readonly _counts = new Map<string, number>()
  /** The member a planning pass counts a slice at a time, and whether a change of its wants since made the count stale. */
  private _counting: { member: string; stale: boolean } | null = null
  /** The members whose named tracks exceeded `ROOM_MEMBER_NAMED_LANES_MAX` when last planned. */
  private readonly _overflowing = new Set<string>()
  /** The binary lanes not ready yet, so a readiness wait never visits the ready ones. */
  private readonly _binaryPending = new Set<LaneSubscription>()
  /** The wait for `_binaryPending`, until another lane starts pending. */
  private _binaryReady: Promise<void> | null = null
  private readonly _inbox = new Map<string, LaneSubscription>()
  private readonly _recovering = new Set<LaneSubscription>()
  /** What the next planning pass replans: binary want changes and lane readiness wait for it, so a burst of them is
   *  planned from the whole want set before any lane opens. */
  private _queued = newBinaryPlan()
  /** The holders gone whose wants the next planning pass drops. */
  private readonly _gone = new Set<LaneHolder>()
  private _planning: Promise<void> | null = null

  constructor(
    private readonly _host: SubscriptionHost,
    private readonly _demand: DemandReport,
  ) {}

  /** Whether some handle observes the room through the control lane. */
  get observing(): boolean {
    return this._observing
  }

  get controlEstablished(): boolean {
    return this._control.established
  }

  get semanticReady(): Promise<void> {
    return this._semantic.ready
  }

  inboxOf(member: string): LaneSubscription | undefined {
    return this._inbox.get(member)
  }

  /** Plans everything in the next planning pass, resolving once it ends: the heartbeat's pass, which also retries every
   *  still-wanted lane whose recovery failed. */
  replan(): Promise<void> {
    this._queued.all = true
    this._replanAround(() => this._schedulePlan())
    return this._planning!
  }

  /** What the room-wide lanes depend on changed: observers, listeners, the tail. */
  syncLanes(): void {
    this._replanAround(() => {})
  }

  /** A holder attached, or its wants changed wholesale: its room-wide ones, and those of `members` when given. The next
   *  planning pass recounts each member it may have changed. */
  holderChanged(holder: LaneHolder, members?: Iterable<string>): void {
    runAll(this._indexHolder(holder, false, members))
    this._replanAround(() => this._schedulePlan())
  }

  /** A holder is gone. A planning pass in a later turn drops its wants, so the holders gone in one turn (the clients of
   *  a mass expiry) are dropped together, a slice at a time. */
  holderGone(holder: LaneHolder): void {
    this._gone.add(holder)
    this._replanAround(() => this._schedulePlan())
  }

  /** A holder's text wants changed: its room-wide ones, and those of `members`. */
  textChanged(holder: LaneHolder, members: Iterable<string>): void {
    runAll(this._wants.updateText(holder, false, members))
    this._replanAround(() => {})
  }

  /** One of a holder's binary wants changed: a want of one track replans only that track's pairs, in the next
   *  planning pass. */
  binaryChanged(holder: LaneHolder, change: BinaryWantChange): void {
    const { track } = change
    const wasEveryMember = track !== null && this._wants.namesEveryMember(track)
    const affected = this._wants.applyBinary(holder, change)
    const queued = this._queued
    if (track === null) {
      if (affected === 'all') queued.all = true
      else for (const member of affected) queued.members.add(member)
    } else {
      // Only a want of every track of every member concerns every member.
      assert(affected !== 'all')
      const everyMember = this._wants.namesEveryMember(track)
      // A track counts for a member while it is named of the member alone; one named of every member counts for none.
      for (const member of affected) {
        const count = this._counts.get(member)
        if (count === undefined) {
          if (this._counting?.member === member) this._counting.stale = true
          continue
        }
        const [before, after] = this._wants.namedPairChange(holder, { ...change, track }, member)
        this._counts.set(member, count + Number(!everyMember && after) - Number(!wasEveryMember && before))
      }
      for (const member of affected) this._queuePair(member, track)
      // Whom the track's every-member lane serves here may have changed.
      if (everyMember || wasEveryMember) queued.tracks.add(track)
    }
    this._replanAround(() => this._schedulePlan())
  }

  /** A member joined, or which holders suppress it changed: the next planning pass recounts it. */
  memberChanged(member: string): void {
    this._dropCounts(member)
    this._queued.members.add(member)
    this._replanAround(() => this._schedulePlan())
  }

  /** A member this instance now owns gets its inbox. */
  memberOwned(member: string): void {
    this._replanAround(() => this._openInbox(member))
  }

  /** A member left, or this instance no longer owns it. */
  memberReleased(member: string): void {
    this._queued.members.add(member)
    this._replanAround(() => {
      this._closeInbox(member)
      this._schedulePlan()
    })
  }

  /** The planning pass under way while a holder here receives the member's `track` and no lane here carries it yet:
   *  once it ends, the lane establishes, and holds a commit on it until it delivers. */
  lanePlanned(member: string, track: string): Promise<void> | null {
    if (this._planning === null || !this._wants.wantsBinary || this._everyTrack.has(member)) return null
    if (this._everyMember.has(track)) return null
    if (this._binary.has(inProcessBinaryLaneKey(member, track)) || !this._wants.receives(member, track)) return null
    return this._planning
  }

  /** The lanes delivering the member's `track` (`null`: every track) are ready, so a retained read after this misses
   *  no live frame. */
  async pairsReady(member: string, track: string | null): Promise<void> {
    await this._planning
    const pending: Promise<void>[] = []
    for (const lane of [
      this._everyTrack.get(member),
      track === null ? undefined : this._binary.get(inProcessBinaryLaneKey(member, track)),
      track === null ? undefined : this._everyMember.get(track),
    ])
      if (lane !== undefined && this._binaryPending.has(lane)) pending.push(lane.ready)
    if (pending.length > 0) await withinRoomHorizon(Promise.all(pending))
  }

  /** Every binary lane planned so far is ready; concurrent callers share one wait. */
  async binaryReady(): Promise<void> {
    await this._planning
    if (this._binaryPending.size === 0) return
    if (this._binaryReady === null) {
      const ready = withinRoomHorizon(Promise.all([...this._binaryPending].map((lane) => lane.ready))).then(() => {})
      const clear = () => {
        if (this._binaryReady === ready) this._binaryReady = null
      }
      ready.then(clear, clear)
      this._binaryReady = ready
    }
    await this._binaryReady
  }

  /** The room-wide lanes are planned around a change, and the heartbeat after it. */
  private _replanAround(change: () => void): void {
    const host = this._host
    const model = host.model
    const open = !model.closed
    const observing = open && host._observed()
    const becomesObserved = observing && !this._observing
    this._control.sync(observing, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, CONTROL_LANE, (payload, info) =>
        host._onControl(decodeRoomText(payload), info),
      ),
    )
    this._observing = observing
    this._semantic.sync(open && (host._holdsTail() || this._wants.wantsText), () =>
      getRoomBackend().subscribeLane(host.id, host._inc, SEMANTIC_LANE, (payload, info) =>
        host._onTextData(decodeRoomText(payload), info),
      ),
    )
    const needsRoster = host._listened() || (open && this._wants.wantsBinary)
    if ((becomesObserved && model.rosterKnown) || (open && !model.rosterKnown && needsRoster))
      void host.sync.refreshMembers().catch(reportRoomError)
    change()
    this._syncDemandLane()
    if (this._planning === null) this._demand.flush()
    this._syncHeartbeat()
  }

  /** Indexes the holder's wants a step at a time, each step queueing the members it affected. */
  private *_indexHolder(holder: LaneHolder, gone: boolean, members?: Iterable<string>): Generator<void> {
    for (const affected of this._wants.update(holder, gone, members)) {
      if (affected === 'all') {
        this._counts.clear()
        if (this._counting) this._counting.stale = true
        this._queued.all = true
      } else
        for (const member of affected) {
          this._dropCounts(member)
          this._queued.members.add(member)
        }
      yield
    }
  }

  private _dropCounts(member: string): void {
    this._counts.delete(member)
    if (this._counting?.member === member) this._counting.stale = true
  }

  private _queuePair(member: string, track: string): void {
    const pairs = this._queued.pairs
    let tracks = pairs.get(member)
    if (!tracks) pairs.set(member, (tracks = new Set()))
    tracks.add(track)
  }

  private _schedulePlan(): void {
    this._planning ??= this._runPlan().catch(reportRoomError)
  }

  /** Plans what is queued once the current turn's changes are in, then tells the demand it changed, each round sharing
   *  one `ROOM_PLAN_SLICE_MS` slice at a time so timers and I/O run between slices; what is queued meanwhile gets the
   *  next round. No other demand is told until it ends, so no owner sees the demand between two of its slices. */
  private async _runPlan(): Promise<void> {
    try {
      await Promise.resolve()
      const slice = new Slice()
      for (;;) {
        const plan = this._queued
        if (this._gone.size > 0) {
          await slice.next()
          await this._replanEach(slice, drain(this._gone), (holder) => this._indexHolder(holder, true))
        } else if (!isEmpty(plan)) {
          this._queued = newBinaryPlan()
          if (this._syncByMember()) plan.all = true
          const host = this._host
          const members = chain(
            plan.members,
            plan.pairs.keys(),
            plan.all ? host.model.memberIds() : [],
            // Lanes of members no longer present close too.
            plan.all ? this._binaryTracks.keys() : [],
            plan.all ? this._everyTrack.keys() : [],
            plan.all ? this._excepted : [],
          )
          const tracks = () =>
            plan.all
              ? chain(this._everyMember.keys(), this._byMemberTracks, this._wants.everyMemberTracks())
              : plan.tracks
          // Every lane opens before any lane it replaces is released.
          await this._replanEach(
            slice,
            tracks(),
            once((track) => this._keepEveryMember(track)),
          )
          await this._replanEach(
            slice,
            members,
            once((member) => {
              this._syncExcept(member)
              return this._unconcerned(member) ? void this._counts.delete(member) : this._planMember(member, plan)
            }),
          )
          await this._replanEach(
            slice,
            tracks(),
            once((track) => this._releaseEveryMember(track)),
          )
          if (plan.all) {
            await this._replanEach(
              slice,
              chain(this._inbox.keys(), host._ownedMembers()),
              once((member) =>
                !host.model.closed && host._ownsMember(member) ? this._openInbox(member) : this._closeInbox(member),
              ),
            )
          }
        } else if (this._demand.changed) {
          while (!this._demand.tellUntil(slice.end)) await slice.next()
        } else return
      }
    } finally {
      this._planning = null
      this._syncHeartbeat()
    }
  }

  /** Runs `step` on each item within `slice`, each slice planned around; a step that yields can end a slice at each
   *  yield. */
  private async _replanEach<T>(
    slice: Slice,
    items: Iterable<T>,
    step: (item: T) => Iterable<void> | void,
  ): Promise<void> {
    const pending = stepsOf(items, step)
    for (let done = false; !done; ) {
      if (slice.over) await slice.next()
      this._replanAround(() => {
        while (!(done = pending.next().done === true)) if (slice.over) return
      })
    }
  }

  /** Whether `_byMember` changed. */
  private _syncByMember(): boolean {
    const model = this._host.model
    const tracks = this._wants.everyMemberTrackCount
    const members = model.memberCount
    const byMember =
      model.rosterKnown &&
      tracks > ROOM_MEMBER_NAMED_LANES_MAX &&
      (this._byMember ? tracks * 2 > members : tracks > members)
    if (byMember === this._byMember) return false
    this._byMember = byMember
    return true
  }

  /** A member's binary lanes: its every-track lane while some holder here receives all of its tracks, more than
   *  `ROOM_MEMBER_NAMED_LANES_MAX` named of it alone, or the tracks named of every member fetched by member, else a lane
   *  per track named of it alone. A switch between them keeps the lanes it leaves until the lanes it takes deliver. It
   *  yields between tracks, so a member's plan is resumed a slice at a time: it plans from the counts and lanes it read
   *  first, and every change meanwhile queues the member again. */
  private *_syncMemberBinary(member: string): Generator<void> {
    const present = this._present(member)
    if (!present) this._counts.delete(member)
    const count = present ? (this._counts.get(member) ?? (yield* this._countNamed(member))) : undefined
    const wantsAll = present && this._wants.wantsEveryTrack(member)
    const overflows = this._overflows(count)
    if (overflows) this._overflowing.add(member)
    else this._overflowing.delete(member)
    const servedByAll = wantsAll || overflows || (present && this._byMember)
    if (servedByAll) this._openEveryTrack(member)
    const all = this._everyTrack.get(member)
    const allDelivers = all?.delivering === true
    this._demand.setMember(member, wantsAll && allDelivers)
    for (const track of chain(this._binaryTracks.get(member) ?? [], this._demand.localTracks(member))) {
      this._syncLane(member, track, servedByAll)
      yield
    }
    // An establishing every-track lane keeps the named lanes as they are.
    for (const track of !servedByAll || allDelivers ? this._wants.memberTracksOf(member) : []) {
      this._syncLane(member, track, servedByAll)
      yield
    }
    // Stopping a lane a change meanwhile wants again would only open it again.
    const stillServed = present && (this._wants.wantsEveryTrack(member) || this._overflows(count) || this._byMember)
    if (!servedByAll && !stillServed && all !== undefined && this._everyTrack.get(member) === all)
      this._releaseEveryTrack(member, all)
  }

  /** No want, lane or demand concerns the member, so its plan has nothing to do; the change that concerns it next
   *  counts it again. */
  private _unconcerned(member: string): boolean {
    return (
      !(this._byMember && this._present(member)) &&
      !this._wants.concerns(member) &&
      !this._binaryTracks.has(member) &&
      !this._everyTrack.has(member) &&
      !this._overflowing.has(member) &&
      !this._demand.receivesFrom(member)
    )
  }

  /** The member's queued pairs; a change that moved it across `ROOM_MEMBER_NAMED_LANES_MAX` replans all of its lanes. */
  private *_planMember(member: string, plan: BinaryPlan): Generator<void> {
    const count = this._counts.get(member)
    if (plan.all || plan.members.has(member) || (count === undefined && this._present(member)))
      return yield* this._syncMemberBinary(member)
    const overflows = this._overflows(count)
    if (overflows !== this._overflowing.has(member)) return yield* this._syncMemberBinary(member)
    const servedByAll = count !== undefined && (this._wants.wantsEveryTrack(member) || overflows || this._byMember)
    for (const track of plan.pairs.get(member) ?? []) {
      this._syncLane(member, track, servedByAll)
      yield
    }
  }

  /** A named lane while some holder names the track of the member alone and neither the member's every-track lane nor
   *  the track's every-member lane serves it; beside either, a lane stays until it delivers. The pair is demand while
   *  some lane here delivers it to a holder that wants it, told as a pair unless the every-member lane delivers it. */
  private _syncLane(member: string, track: string, servedByAll: boolean): void {
    const present = this._present(member)
    const named = present && this._wants.wantsNamedPair(member, track)
    const everyMember = this._wants.namesEveryMember(track)
    const allDelivers = this._everyTrack.get(member)?.delivering === true
    const trackDelivers = everyMember && this._everyMember.get(track)?.delivering === true
    const key = inProcessBinaryLaneKey(member, track)
    let lane = this._binary.get(key)
    let keep = named
    if (servedByAll) keep = lane !== undefined && !allDelivers
    else if (everyMember) keep = named && lane !== undefined && !trackDelivers
    if (!keep && lane !== undefined) {
      lane.stop()
      this._binary.delete(key)
      untrack(this._binaryTracks, member, track)
      untrack(this._trackMembers, track, member)
      lane = undefined
      if (this._everyMember.has(track)) this._queued.tracks.add(track)
    } else if (keep) {
      ;(lane ??= this._openNamedLane(member, track)).resume()
    }
    if (everyMember) this._setExcept(track, member, present && !named)
    const delivers = allDelivers || trackDelivers || lane?.delivering === true
    this._demand.setPair(member, track, (named || lane !== undefined) && delivers && !trackDelivers)
  }

  /** The every-member lane of `track` while some holder here names it of every member and the tracks are not fetched
   *  by member. Its demand is one entry, told once it delivers, or while the every-track lanes it replaces still do. */
  private _keepEveryMember(track: string): void {
    if (this._byMember || !this._namesEveryMember(track)) return
    this._establishingEveryTrack.waiting.delete(track)
    const lane = this._everyMember.get(track) ?? this._openEveryMember(track)
    lane.resume()
    if (lane.delivering) this._byMemberTracks.delete(track)
    this._demand.setTrack(track, lane.delivering || this._byMemberTracks.has(track))
  }

  /** An every-member lane that serves no holder here stops once the lanes replacing it deliver: the named lanes and
   *  every-track lanes once no holder names the track, each member's every-track lane while the tracks are fetched by
   *  member, which then tells the track's demand. Until then, each one's readiness replans the track. */
  private _releaseEveryMember(track: string): void {
    const named = this._namesEveryMember(track)
    if (named && !this._byMember) return
    const lane = this._everyMember.get(track)
    if (lane === undefined && named === this._byMemberTracks.has(track)) return
    for (const member of this._trackMembers.get(track) ?? [])
      if (!this._binary.get(inProcessBinaryLaneKey(member, track))!.delivering) return
    if (this._waitsFor(this._establishingEveryTrack, track)) return
    if (lane !== undefined) {
      this._everyMember.delete(track)
      this._stopFanIn(lane, this._establishingEveryMember)
    }
    toggle(this._byMemberTracks, track, named)
    if (named) this._demand.setTrack(track, true)
    else this._demand.forgetTrack(track)
  }

  private _namesEveryMember(track: string): boolean {
    return !this._host.model.closed && this._wants.namesEveryMember(track)
  }

  /** Whether the member is one no holder here receives a track named of every member from: only a holder's own member
   *  can be, as one suppresses it, so only an owned member is read while a track has its every-member lane or is told
   *  by member, or one told so before. */
  private _syncExcept(member: string): void {
    const served = this._everyMember.size > 0 || this._byMemberTracks.size > 0
    if (!this._excepted.has(member) && (!served || !this._host._ownsMember(member))) return
    const present = this._present(member)
    let excepted = false
    for (const track of this._wants.everyMemberTracks()) {
      const unreached = present && !this._wants.wantsNamedPair(member, track)
      this._setExcept(track, member, unreached)
      excepted ||= unreached
    }
    toggle(this._excepted, member, excepted)
  }

  private _setExcept(track: string, member: string, on: boolean): void {
    this._demand.setExcept(track, member, on)
    if (on) this._excepted.add(member)
  }

  private _present(member: string): boolean {
    const model = this._host.model
    return !model.closed && model.hasMember(member)
  }

  /** Counted a slice at a time, and kept unless a change of the member's wants meanwhile made it stale: the change
   *  queued the member, so the next pass counts it again. */
  private *_countNamed(member: string): Generator<void, number> {
    let count = 0
    const counting = (this._counting = { member, stale: false })
    try {
      for (const track of this._wants.memberTracksOf(member)) {
        count += Number(this._wants.wantsNamedPair(member, track))
        yield
      }
    } finally {
      this._counting = null
    }
    if (!counting.stale) this._counts.set(member, count)
    return count
  }

  private _overflows(count: number | undefined): boolean {
    return count !== undefined && count > ROOM_MEMBER_NAMED_LANES_MAX
  }

  private _openEveryTrack(member: string): void {
    let lane = this._everyTrack.get(member)
    if (lane === undefined) {
      const host = this._host
      const establishing = this._establishingEveryTrack
      const opened = (lane = this._newLaneSubscription(() => {
        this._queued.members.add(member)
        this._established(opened, establishing)
      }))
      establishing.lanes.add(lane)
      this._everyTrack.set(member, lane)
      lane.sync(true, () =>
        getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'binary-all', member }, (framed, info) =>
          host._onBinary(framed, info),
        ),
      )
    }
    this._establishingEveryMember.waiting.delete(member)
    lane.resume()
  }

  private _openEveryMember(track: string): LaneSubscription {
    const host = this._host
    const establishing = this._establishingEveryMember
    // The named lanes and every-track lanes it serves stop once it delivers.
    const lane = this._newLaneSubscription(() => {
      this._queued.tracks.add(track)
      for (const member of this._trackMembers.get(track) ?? []) this._queuePair(member, track)
      this._established(lane, establishing)
    })
    establishing.lanes.add(lane)
    this._everyMember.set(track, lane)
    lane.sync(true, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'binary-track', track }, (framed, info) =>
        host._onBinary(framed, info),
      ),
    )
    return lane
  }

  private _openNamedLane(member: string, track: string): LaneSubscription {
    const host = this._host
    // An every-track lane beside it may wait for it to deliver, so it replans the whole member; an every-member lane
    // that holders no longer name waits for it too.
    const lane = this._newLaneSubscription(() => {
      if (this._everyTrack.has(member)) this._queued.members.add(member)
      else this._queuePair(member, track)
      if (this._everyMember.has(track)) this._queued.tracks.add(track)
    })
    this._binary.set(inProcessBinaryLaneKey(member, track), lane)
    addTo(this._binaryTracks, member, track)
    addTo(this._trackMembers, track, member)
    lane.sync(true, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'binary', member, track }, (framed, info) =>
        host._onBinary(framed, info),
      ),
    )
    return lane
  }

  /** An every-track lane nothing here wants any more stops once the named lanes and every-member lanes replacing it
   *  deliver; until then, each one's readiness replans the member. */
  private _releaseEveryTrack(member: string, all: LaneSubscription): void {
    for (const track of this._binaryTracks.get(member) ?? [])
      if (!this._binary.get(inProcessBinaryLaneKey(member, track))!.delivering) return
    if (this._waitsFor(this._establishingEveryMember, member)) return
    this._everyTrack.delete(member)
    this._stopFanIn(all, this._establishingEveryTrack)
  }

  /** Whether `item`'s lane stays while a lane of the other kind is not delivering yet: a closed room waits for none. */
  private _waitsFor(other: Establishing, item: string): boolean {
    const waits = !this._host.model.closed && other.lanes.size > 0
    toggle(other.waiting, item, waits)
    return waits
  }

  private _stopFanIn(lane: LaneSubscription, establishing: Establishing): void {
    lane.stop()
    this._established(lane, establishing)
  }

  private _established(lane: LaneSubscription, establishing: Establishing): void {
    if (!establishing.lanes.delete(lane) || establishing.lanes.size > 0) return
    for (const item of establishing.waiting) establishing.replan(item)
  }

  private _openInbox(member: string): void {
    const host = this._host
    if (host.model.closed || !host._ownsMember(member)) return
    let slot = this._inbox.get(member)
    if (!slot) this._inbox.set(member, (slot = this._newLaneSubscription()))
    slot.sync(true, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'inbox', member }, (payload, info) =>
        host._onDm(decodeRoomText(payload), info),
      ),
    )
  }

  private _closeInbox(member: string): void {
    this._inbox.get(member)?.stop()
    this._inbox.delete(member)
  }

  private _syncDemandLane(): void {
    const host = this._host
    const lane = this._demandLane
    if (!host.model.closed && !host._ownedMembers()[Symbol.iterator]().next().done)
      return lane.sync(true, () =>
        getRoomBackend().subscribeLane(host.id, host._inc, DEMAND_LANE, (payload) =>
          host._onDemand(decodeRoomText(payload)),
        ),
      )
    if (!lane.wanted) return
    lane.stop()
    host._forgetDemand()
  }

  /** A binary lane passes `queue`: what it carries is demand once it delivers, so its readiness replans what `queue`
   *  queues. */
  private _newLaneSubscription(queue?: () => void): LaneSubscription {
    if (queue === undefined) return new LaneSubscription(this._onLaneTerminal, this._onLaneRecovered)
    return new LaneSubscription(this._onLaneTerminal, this._onLaneRecovered, (slot, pending) => {
      this._onBinaryPending(slot, pending)
      if (pending || !slot.wanted) return
      queue()
      this._schedulePlan()
    })
  }

  private _onTerminal(slot: LaneSubscription, failure?: unknown): void {
    // A replacement that ends is its recovery's failure, which the recovery reports.
    if (this._recovering.has(slot)) return
    if (failure !== undefined) reportSubscriptionEnd(failure, reportServerChannelError)
    this._recovering.add(slot)
    void this._recover(slot).catch(reportRoomError)
  }

  /** A still-wanted terminal lane gets one replacement after a head read; if that fails too, the heartbeat's replan
   *  subscribes it again. */
  private async _recover(slot: LaneSubscription): Promise<void> {
    try {
      const config = await withinRoomHorizon(this._host._readOpenConfig())
      if (config === null) return this._host._closeFromAuthority()
      slot.retry()
      await withinRoomHorizon(slot.attemptReady)
    } catch (error) {
      if (slot.wanted) slot.dropAttempt()
      throw error
    } finally {
      this._recovering.delete(slot)
    }
    // Catch up on what the outage dropped; a later end of the lane gets its own recovery.
    await this._host.sync.reconcileAuthority()
  }

  /** The heartbeat runs while a handle observes the room or owns a member, or the demand tells anything. */
  private _syncHeartbeat(): void {
    const host = this._host
    host.sync.heartbeat(!host.model.closed && (this._observing || this._inbox.size > 0 || this._demand.reporting))
  }
}

function addTo(sets: Map<string, Set<string>>, key: string, value: string): void {
  let values = sets.get(key)
  if (!values) sets.set(key, (values = new Set()))
  values.add(value)
}
function untrack(sets: Map<string, Set<string>>, key: string, value: string): void {
  const values = sets.get(key)
  if (values?.delete(value) && values.size === 0) sets.delete(key)
}
function toggle<T>(set: Set<T>, value: T, on: boolean): void {
  if (on) set.add(value)
  else set.delete(value)
}
/** Each source's items in turn, each source read live as the iteration reaches it. */
function* chain<T>(...sources: Iterable<T>[]): Generator<T> {
  for (const source of sources) yield* source
}
/** Runs every step now. */
function runAll(steps: Iterable<unknown>): void {
  for (const _ of steps);
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
