export { RoomSubscriptions }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { getRoomBackend } from '../../backend/install.js'
import { inProcessBinaryLaneKey } from '../../backend/room/lane-key.js'
import { reportSubscriptionEnd } from '../../backend/subscription-manager.js'
import { reportServerChannelError } from '../../server/channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import { ROOM_MEMBER_NAMED_LANES_MAX } from '../constants.js'
import type { BinaryWantChange } from '../binary.js'
import type { DemandReport, RoomDemand } from '../demand.js'
import type { RoomConfigRecord } from '../protocol.js'
import type { RoomModel } from '../state.js'
import { reportRoomError } from './errors.js'
import { LaneSubscription } from './lane-subscription.js'
import { Slice } from '../slice.js'
import type { LaneHolder } from './replay.js'
import type { AuthoritySync } from './authority-sync.js'
import { WantsIndex } from './wants-index.js'
import { CONTROL_LANE, SEMANTIC_LANE, decodeRoomText, withinRoomHorizon } from './lanes.js'
assertIsNotBrowser()

/** The replica these subscriptions serve: it aggregates every handle's holders and applies what the lanes carry. */
type SubscriptionHost = {
  readonly id: string
  readonly _inc: string
  /** Read-only here: every change goes through the host's methods. */
  readonly model: Pick<RoomModel, 'closed' | 'rosterKnown' | 'memberIds' | 'hasMember'>
  readonly demand: Pick<RoomDemand, 'reporting'>
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
  _readOpenConfig(): Promise<RoomConfigRecord | null>
  _closeFromAuthority(): void
}

/** Of one member: the tracks named of it alone that some holder here receives, and the tracks named of every member
 *  none does. Its named tracks received here are `everyMemberTracks - unreached + own`. */
type NamedCounts = { own: number; unreached: number }
/** Binary pairs a planning pass replans: each member's, or every pair of each of `tracks` when `all`, the pairs of
 *  each of `tracks` named of every member, and some members' named pairs. */
type BinaryPlan = { all: boolean; members: Set<string>; tracks: Set<string>; pairs: Map<string, Set<string>> }

/** What this process subscribes of a room: a function of its holders' wants, ownership and membership, kept current
 *  per change, recovered within Room's horizon, and replanned by the heartbeat. */
class RoomSubscriptions {
  private readonly _wants = new WantsIndex((track, named) => this._demand.setTrack(track, named))
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
  /** Named lanes, keyed by (member, track); `_binaryTracks` lists each member's. */
  private readonly _binary = new Map<string, LaneSubscription>()
  private readonly _binaryTracks = new Map<string, Set<string>>()
  /** Each member's every-track lane, while some holder here receives all of its tracks or more than
   *  `ROOM_MEMBER_NAMED_LANES_MAX` named ones. */
  private readonly _everyTrack = new Map<string, LaneSubscription>()
  /** Per present member, its named tracks received here, counted apart from the tracks named of every member. */
  private readonly _counts = new Map<string, NamedCounts>()
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
      const everyMember = this._wants.namesEveryMember(track)
      // A track that starts or stops being named of every member counts in `everyMemberTracks`, but apart for a member
      // the holder doesn't receive or that some holder names it of alone.
      const recounted =
        affected === 'all' ? new Set([...holder._suppressedMembers(), ...this._wants.membersNaming(track)]) : affected
      for (const member of recounted) {
        const counts = this._counts.get(member)
        if (counts === undefined) {
          if (this._counting?.member === member) this._counting.stale = true
          continue
        }
        const [before, after] = this._wants.namedPairChange(holder, { ...change, track }, member)
        counts.own += Number(!everyMember && after) - Number(!wasEveryMember && before)
        counts.unreached += Number(everyMember && !after) - Number(wasEveryMember && !before)
      }
      if (affected === 'all') queued.tracks.add(track)
      else for (const member of affected) this._queuePair(member, track)
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
          const host = this._host
          const members = chain(
            plan.members,
            plan.pairs.keys(),
            plan.all || plan.tracks.size > 0 ? host.model.memberIds() : [],
            // Lanes of members no longer present close too.
            plan.all ? this._binaryTracks.keys() : [],
            plan.all ? this._everyTrack.keys() : [],
          )
          await this._replanEach(
            slice,
            members,
            once((member) =>
              this._unconcerned(member) ? void this._counts.delete(member) : this._planMember(member, plan),
            ),
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

  /** A member's binary lanes: its every-track lane while some holder here receives all of its tracks or more than
   *  `ROOM_MEMBER_NAMED_LANES_MAX` named ones, else a lane per named track. A switch between them keeps the lanes it
   *  leaves until the lanes it takes deliver. It yields between tracks, so a member's plan is resumed a slice at a time:
   *  it plans from the counts and lanes it read first, and every change meanwhile queues the member again. */
  private *_syncMemberBinary(member: string): Generator<void> {
    const present = this._present(member)
    if (!present) this._counts.delete(member)
    const counts = present ? (this._counts.get(member) ?? (yield* this._countNamed(member))) : undefined
    const wantsAll = present && this._wants.wantsEveryTrack(member)
    const overflows = this._overflows(counts)
    if (overflows) this._overflowing.add(member)
    else this._overflowing.delete(member)
    const servedByAll = wantsAll || overflows
    if (servedByAll) this._openEveryTrack(member)
    const all = this._everyTrack.get(member)
    const allDelivers = all?.delivering === true
    this._demand.setPair(member, null, wantsAll && allDelivers)
    const covered = allDelivers && this._reachesEveryMemberTrack(counts)
    this._demand.setCovers(member, covered)
    for (const track of chain(this._binaryTracks.get(member) ?? [], this._demand.localTracks(member))) {
      this._syncLane(member, track, servedByAll)
      yield
    }
    // A covered member gets the tracks named of every member through `covers`; an establishing every-track lane none.
    let tracks: Iterable<string> = []
    if (!servedByAll || (allDelivers && !covered)) tracks = this._wants.namedTracksOf(member)
    else if (allDelivers) tracks = this._wants.memberTracksOf(member)
    for (const track of tracks) {
      this._syncLane(member, track, servedByAll)
      yield
    }
    // Stopping a lane a change meanwhile wants again would only open it again.
    const stillServed = present && (this._wants.wantsEveryTrack(member) || this._overflows(counts))
    if (!servedByAll && !stillServed && all !== undefined && this._everyTrack.get(member) === all)
      this._releaseEveryTrack(member, all)
  }

  /** No want, lane or demand concerns the member, so its plan has nothing to do; the change that concerns it next
   *  counts it again. */
  private _unconcerned(member: string): boolean {
    return (
      !this._wants.concerns(member) &&
      !this._binaryTracks.has(member) &&
      !this._everyTrack.has(member) &&
      !this._overflowing.has(member) &&
      !this._demand.receivesFrom(member)
    )
  }

  /** The member's queued pairs; a change that moved it across `ROOM_MEMBER_NAMED_LANES_MAX`, or into or out of being
   *  covered, replans all of its lanes. */
  private *_planMember(member: string, plan: BinaryPlan): Generator<void> {
    const counts = this._counts.get(member)
    if (plan.all || plan.members.has(member) || (counts === undefined && this._present(member)))
      return yield* this._syncMemberBinary(member)
    const overflows = this._overflows(counts)
    const all = this._everyTrack.get(member)
    const covered = all?.delivering === true && this._reachesEveryMemberTrack(counts)
    if (overflows !== this._overflowing.has(member) || covered !== this._demand.covers(member))
      return yield* this._syncMemberBinary(member)
    const servedByAll = counts !== undefined && (this._wants.wantsEveryTrack(member) || overflows)
    for (const track of chain(plan.tracks, plan.pairs.get(member) ?? [])) {
      this._syncLane(member, track, servedByAll)
      yield
    }
  }

  /** A named lane while some holder names the track and the every-track lane doesn't serve the member; beside that
   *  lane, one stays until it delivers. The pair is demand while some lane here delivers it to a holder that wants it,
   *  reported as a pair unless the track is named of every member and the member is covered. */
  private _syncLane(member: string, track: string, servedByAll: boolean): void {
    const named = this._present(member) && this._wants.wantsNamedPair(member, track)
    const allDelivers = this._everyTrack.get(member)?.delivering === true
    const key = inProcessBinaryLaneKey(member, track)
    let lane = this._binary.get(key)
    const keep = servedByAll ? lane !== undefined && !allDelivers : named
    if (!keep && lane !== undefined) {
      lane.stop()
      this._binary.delete(key)
      const tracks = this._binaryTracks.get(member)!
      tracks.delete(track)
      if (tracks.size === 0) this._binaryTracks.delete(member)
      lane = undefined
    } else if (keep) {
      ;(lane ??= this._openNamedLane(member, track)).resume()
    }
    const delivers = allDelivers || lane?.delivering === true
    const reached = this._demand.covers(member) && this._wants.namesEveryMember(track)
    this._demand.setPair(member, track, (named || lane !== undefined) && delivers && !reached)
  }

  private _present(member: string): boolean {
    const model = this._host.model
    return !model.closed && model.hasMember(member)
  }

  /** Counted a slice at a time, and kept unless a change of the member's wants meanwhile made it stale: the change
   *  queued the member, so the next pass counts it again. */
  private *_countNamed(member: string): Generator<void, NamedCounts> {
    const counts = { own: 0, unreached: 0 }
    const counting = (this._counting = { member, stale: false })
    try {
      for (const track of this._wants.namedTracksOf(member)) {
        const received = this._wants.wantsNamedPair(member, track)
        if (!this._wants.namesEveryMember(track)) counts.own += Number(received)
        else counts.unreached += Number(!received)
        yield
      }
    } finally {
      this._counting = null
    }
    if (!counting.stale) this._counts.set(member, counts)
    return counts
  }

  private _overflows(counts: NamedCounts | undefined): boolean {
    return (
      counts !== undefined &&
      this._wants.everyMemberTracks - counts.unreached + counts.own > ROOM_MEMBER_NAMED_LANES_MAX
    )
  }

  /** Whether some holder here receives from the member each track named of every member. */
  private _reachesEveryMemberTrack(counts: NamedCounts | undefined): boolean {
    return counts !== undefined && counts.unreached === 0 && this._wants.everyMemberTracks > 0
  }

  private _openEveryTrack(member: string): void {
    let lane = this._everyTrack.get(member)
    if (lane === undefined) {
      const host = this._host
      lane = this._newLaneSubscription(() => this._queued.members.add(member))
      this._everyTrack.set(member, lane)
      lane.sync(true, () =>
        getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'binary-all', member }, (framed, info) =>
          host._onBinary(framed, info),
        ),
      )
    }
    lane.resume()
  }

  private _openNamedLane(member: string, track: string): LaneSubscription {
    const host = this._host
    // An every-track lane beside it may wait for it to deliver, so it replans the whole member.
    const lane = this._newLaneSubscription(() =>
      this._everyTrack.has(member) ? this._queued.members.add(member) : this._queuePair(member, track),
    )
    this._binary.set(inProcessBinaryLaneKey(member, track), lane)
    let tracks = this._binaryTracks.get(member)
    if (!tracks) this._binaryTracks.set(member, (tracks = new Set()))
    tracks.add(track)
    lane.sync(true, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'binary', member, track }, (framed, info) =>
        host._onBinary(framed, info),
      ),
    )
    return lane
  }

  /** An every-track lane nothing here wants any more stops once the named lanes replacing it deliver; until then, each
   *  one's readiness replans the member. */
  private _releaseEveryTrack(member: string, all: LaneSubscription): void {
    for (const track of this._binaryTracks.get(member) ?? [])
      if (!this._binary.get(inProcessBinaryLaneKey(member, track))!.delivering) return
    all.stop()
    this._everyTrack.delete(member)
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
    host.sync.heartbeat(!host.model.closed && (this._observing || this._inbox.size > 0 || host.demand.reporting))
  }
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
