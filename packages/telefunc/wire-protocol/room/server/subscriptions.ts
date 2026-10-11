export { RoomSubscriptions }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getRoomBackend } from '../../backend/install.js'
import type { LaneId } from '../../backend/room/contract.js'
import { reportSubscriptionEnd } from '../../backend/subscription-manager.js'
import { nextTurnOf } from '../../flow-control/macrotask-yield.js'
import { reportServerChannelError } from '../../server/channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_NAMED_LANES_MAX, ROOM_PLAN_SLICE_MS } from '../constants.js'
import type { BinaryWantChange } from '../binary.js'
import type { RoomDemand } from '../demand.js'
import type { MemberSnapshot, RoomConfigRecord } from '../protocol.js'
import { replayMembership, type RoomState } from '../state.js'
import { reportRoomError } from './errors.js'
import { LaneSubscription } from './lane-subscription.js'
import { binaryLaneKey, type LaneHolder } from './replay.js'
import { WantsIndex } from './wants-index.js'
import { CONTROL_LANE, SEMANTIC_LANE, decodeRoomText, withinRoomHorizon } from './lanes.js'
import { reapAndReadRoster, renewMemberLeases } from './membership.js'
import { maintainHold, type RoomIncarnation } from './lifecycle.js'
import { readRoomMeta, type StoredRoomMeta } from './meta.js'
assertIsNotBrowser()

/** The room side these subscriptions serve: it aggregates its holders and applies what the lanes and the authority carry. */
type SubscriptionHost = {
  readonly id: string
  readonly _inc: string
  readonly _incarnation: RoomIncarnation
  /** Read-only here: every change goes through the host's methods. */
  readonly _state: Pick<
    RoomState,
    'closed' | 'rosterKnown' | 'listenerCount' | 'recordingMembership' | 'listMemberIds' | 'hasMember'
  >
  /** Whether anything here observes the room: a client stub, a participant, or a listener. */
  _observed(): boolean
  /** A pre-attach tail ingests all text. */
  _holdsTail(): boolean
  _ownsMember(id: string): boolean
  /** A pending admission owns its inbox, but its record is renewed only once it commits. */
  _ownedMembers(): { all: string[]; renewable: string[] }
  _renewViews(): Promise<void>
  _onCtrlMessage(serialized: string, info: WirePublishInfo): void
  _onTextData(serialized: string, info: WirePublishInfo): void
  _onBinary(framed: Uint8Array, info: WirePublishInfo): void
  _onDm(serialized: string, info: WirePublishInfo): void
  _readOpenConfig(): Promise<RoomConfigRecord | null>
  _applyAuthorityMeta(stored: StoredRoomMeta): void
  /** A complete roster read: applied, and sent to the clients it concerns. */
  _applyAuthorityRoster(members: MemberSnapshot[], departing: ReadonlySet<string>): void
  _closeFromAuthority(): void
}

/** Of one member: the tracks named of it alone that some holder here receives, and the tracks named of every member
 *  none does. Its named tracks received here are `everyMemberTracks - unreached + own`. */
type NamedCounts = { own: number; unreached: number }
/** Binary pairs a planning pass replans: each member's, or every pair of each of `tracks` when `all`, the pairs of
 *  each of `tracks` named of every member, and some members' named pairs. */
type BinaryPlan = { all: boolean; members: Set<string>; tracks: Set<string>; pairs: Map<string, Set<string>> }

/** What this instance subscribes: a function of its holders' wants, ownership and membership, kept current per change,
 *  recovered within Room's horizon, and renewed by the heartbeat. */
class RoomSubscriptions {
  private readonly _wants = new WantsIndex((track, named) => this._demand.setTrack(track, named))
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
  /** The members whose named tracks exceeded `ROOM_MEMBER_NAMED_LANES_MAX` when last planned. */
  private readonly _overflowing = new Set<string>()
  /** The binary lanes that were ready once: what they carry is this instance's demand. */
  private readonly _delivering = new WeakSet<LaneSubscription>()
  /** The members whose every-track lane waits for the named lanes replacing it. */
  private readonly _releasing = new Set<string>()
  /** The binary lanes not ready yet, so a readiness wait never visits the ready ones. */
  private readonly _binaryPending = new Set<LaneSubscription>()
  /** The wait for `_binaryPending`, until another lane starts pending. */
  private _binaryReady: Promise<void> | null = null
  private readonly _inbox = new Map<string, LaneSubscription>()
  private readonly _recovering = new Set<LaneSubscription>()
  /** What the next planning pass replans: binary want changes and lane readiness wait for it, so a burst of them is
   *  planned from the whole want set before any lane opens. */
  private _queued = newBinaryPlan()
  private _planning: Promise<void> | null = null
  private _pendingRefresh: Promise<void> | null = null
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _heartbeatBusy = false

  constructor(
    private readonly _host: SubscriptionHost,
    private readonly _demand: RoomDemand,
  ) {}

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

  /** A holder attached or is gone, or its wants changed wholesale: its room-wide ones, and those of `members` when
   *  given. The next planning pass recounts each member it may have changed. */
  holderChanged(holder: LaneHolder, gone = false, members?: Iterable<string>): void {
    const affected = this._wants.update(holder, gone, members)
    const queued = this._queued
    if (affected === 'all') {
      this._counts.clear()
      queued.all = true
    } else
      for (const member of affected) {
        this._counts.delete(member)
        queued.members.add(member)
      }
    this._replanAround(() => this._schedulePlan())
  }

  /** A holder's text wants changed: its room-wide ones, and those of `members`. */
  textChanged(holder: LaneHolder, members: Iterable<string>): void {
    this._wants.updateText(holder, false, members)
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
        if (counts === undefined) continue
        const [before, after] = this._wants.namedPairChange(holder, { ...change, track }, member)
        counts.own += Number(!everyMember && after) - Number(!wasEveryMember && before)
        counts.unreached += Number(everyMember && !after) - Number(wasEveryMember && !before)
      }
      if (affected === 'all') queued.tracks.add(track)
      else for (const member of affected) this._queuePair(member, track)
    }
    this._replanAround(() => this._schedulePlan())
  }

  /** A member joined, or which holders suppress it changed. */
  memberChanged(member: string): void {
    this._replanAround(() => this._syncMemberBinary(member, true))
  }

  /** A member this instance now owns gets its inbox. */
  memberOwned(member: string): void {
    this._replanAround(() => this._openInbox(member))
  }

  /** A member left, or this instance no longer owns it. */
  memberReleased(member: string): void {
    this._replanAround(() => {
      this._syncMemberBinary(member)
      this._closeInbox(member)
    })
  }

  /** The lanes delivering the member's `track` (`null`: every track) are ready, so a retained read after this misses
   *  no live frame. */
  async pairsReady(member: string, track: string | null): Promise<void> {
    await this._planning
    const pending: Promise<void>[] = []
    for (const lane of [
      this._everyTrack.get(member),
      track === null ? undefined : this._binary.get(binaryLaneKey(member, track)),
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

  ensureRoster(): Promise<void> {
    if (this._pendingRefresh !== null) return this._pendingRefresh
    const state = this._host._state
    if (state.closed || (state.rosterKnown && this._control.established)) return Promise.resolve()
    return this._refreshMembers()
  }

  async reconcileAuthority(): Promise<void> {
    const host = this._host
    if (host._state.closed) return
    const config = await host._readOpenConfig()
    const stored = config && (await readRoomMeta(host.id, config))
    if (stored === null) return host._closeFromAuthority()
    host._applyAuthorityMeta(stored)
    await this._refreshMembers()
  }

  /** The room-wide lanes are planned around a change, and the heartbeat after it. */
  private _replanAround(change: () => void): void {
    const host = this._host
    const state = host._state
    const open = !state.closed
    const observed = host._observed()
    const becomesObserved = open && observed && !this._control.active
    this._control.sync(open && observed, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, CONTROL_LANE, (payload, info) =>
        host._onCtrlMessage(decodeRoomText(payload), info),
      ),
    )
    this._semantic.sync(open && (host._holdsTail() || this._wants.wantsText), () =>
      getRoomBackend().subscribeLane(host.id, host._inc, SEMANTIC_LANE, (payload, info) =>
        host._onTextData(decodeRoomText(payload), info),
      ),
    )
    const needsRoster = state.listenerCount > 0 || (open && this._wants.wantsBinary)
    if ((becomesObserved && state.rosterKnown) || (open && !state.rosterKnown && needsRoster))
      void this._refreshMembers().catch(reportRoomError)
    change()
    if (this._planning === null) this._demand.flush()
    this._syncHeartbeat()
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

  /** Plans what is queued once the current turn's changes are in, then tells the demand it changed, each
   *  `ROOM_PLAN_SLICE_MS` at a time so timers and I/O run between slices; what is queued meanwhile gets the next round.
   *  No other demand is told until it ends, so no owner sees the demand between two of its slices. */
  private async _runPlan(): Promise<void> {
    try {
      await Promise.resolve()
      for (;;) {
        const plan = this._queued
        if (!isEmpty(plan)) {
          this._queued = newBinaryPlan()
          const members = new Set([...plan.members, ...plan.pairs.keys()])
          if (plan.all || plan.tracks.size > 0)
            for (const member of this._host._state.listMemberIds()) members.add(member)
          // Lanes of members no longer present close too.
          if (plan.all)
            for (const member of [...this._binaryTracks.keys(), ...this._everyTrack.keys()]) members.add(member)
          await this._replanEach(members, (member) => this._planMember(member, plan))
          if (plan.all) {
            const host = this._host
            const inboxes = new Set([...this._inbox.keys(), ...host._ownedMembers().all])
            await this._replanEach(inboxes, (member) =>
              !host._state.closed && host._ownsMember(member) ? this._openInbox(member) : this._closeInbox(member),
            )
          }
        } else if (this._demand.changed) {
          while (!this._demand.tellUntil(performance.now() + ROOM_PLAN_SLICE_MS)) await nextTurnOf()
        } else return
      }
    } finally {
      this._planning = null
      this._syncHeartbeat()
    }
  }

  /** Runs `step` on each item, `ROOM_PLAN_SLICE_MS` at a time, each slice planned around. */
  private async _replanEach<T>(items: Iterable<T>, step: (item: T) => void): Promise<void> {
    const pending = items[Symbol.iterator]()
    for (let done = false; !done; ) {
      this._replanAround(() => {
        const deadline = performance.now() + ROOM_PLAN_SLICE_MS
        for (let next = pending.next(); !(done = next.done === true); next = pending.next()) {
          step(next.value)
          if (performance.now() >= deadline) return
        }
      })
      if (!done) await nextTurnOf()
    }
  }

  /** A member's binary lanes: its every-track lane while some holder here receives all of its tracks or more than
   *  `ROOM_MEMBER_NAMED_LANES_MAX` named ones, else a lane per named track. A switch between them keeps the lanes it
   *  leaves until the lanes it takes deliver. `recount` when the member's named wants may have changed wholesale. */
  private _syncMemberBinary(member: string, recount = false): void {
    const present = this._present(member)
    if (!present) this._counts.delete(member)
    else if (recount || !this._counts.has(member)) this._counts.set(member, this._countNamed(member))
    const counts = this._counts.get(member)
    const wantsAll = present && this._wants.wantsEveryTrack(member)
    const overflows = this._overflows(counts)
    if (overflows) this._overflowing.add(member)
    else this._overflowing.delete(member)
    const servedByAll = wantsAll || overflows
    if (servedByAll) this._openEveryTrack(member)
    const all = this._everyTrack.get(member)
    const allDelivers = all !== undefined && this._delivering.has(all)
    this._demand.setPair(member, null, wantsAll && allDelivers)
    const covered = allDelivers && this._reachesEveryMemberTrack(counts)
    this._demand.setCovers(member, covered)
    for (const track of [...(this._binaryTracks.get(member) ?? []), ...this._demand.localTracks(member)])
      this._syncLane(member, track, servedByAll)
    // A covered member gets the tracks named of every member through `covers`; an establishing every-track lane none.
    let tracks: Iterable<string> = []
    if (!servedByAll || (allDelivers && !covered)) tracks = this._wants.namedTracksOf(member)
    else if (allDelivers) tracks = this._wants.memberTracksOf(member)
    for (const track of tracks) this._syncLane(member, track, servedByAll)
    if (!servedByAll && all !== undefined) this._releaseEveryTrack(member, all)
  }

  /** The member's queued pairs; a change that moved it across `ROOM_MEMBER_NAMED_LANES_MAX`, or into or out of being
   *  covered, replans all of its lanes. */
  private _planMember(member: string, plan: BinaryPlan): void {
    const counts = this._counts.get(member)
    if (plan.all || plan.members.has(member) || (counts === undefined && this._present(member)))
      return this._syncMemberBinary(member)
    const overflows = this._overflows(counts)
    const all = this._everyTrack.get(member)
    const covered = all !== undefined && this._delivering.has(all) && this._reachesEveryMemberTrack(counts)
    if (overflows !== this._overflowing.has(member) || covered !== this._demand.covers(member))
      return this._syncMemberBinary(member)
    const servedByAll = counts !== undefined && (this._wants.wantsEveryTrack(member) || overflows)
    for (const track of plan.tracks) this._syncLane(member, track, servedByAll)
    for (const track of plan.pairs.get(member) ?? []) this._syncLane(member, track, servedByAll)
  }

  /** A named lane while some holder names the track and the every-track lane doesn't serve the member; beside that
   *  lane, one stays until it delivers. The pair is demand while some lane here delivers it to a holder that wants it,
   *  reported as a pair unless the track is named of every member and the member is covered. */
  private _syncLane(member: string, track: string, servedByAll: boolean): void {
    const named = this._present(member) && this._wants.wantsNamedPair(member, track)
    const all = this._everyTrack.get(member)
    const allDelivers = all !== undefined && this._delivering.has(all)
    const key = binaryLaneKey(member, track)
    let lane = this._binary.get(key)
    const keep = servedByAll ? lane !== undefined && !allDelivers : named
    if (!keep && lane !== undefined) {
      lane.stop()
      this._binary.delete(key)
      const tracks = this._binaryTracks.get(member)!
      tracks.delete(track)
      if (tracks.size === 0) this._binaryTracks.delete(member)
      lane = undefined
    } else if (keep && lane === undefined) {
      lane = this._openNamedLane(member, track)
    }
    const delivers = allDelivers || (lane !== undefined && this._delivering.has(lane))
    const reached = this._demand.covers(member) && this._wants.namesEveryMember(track)
    this._demand.setPair(member, track, (named || lane !== undefined) && delivers && !reached)
  }

  private _present(member: string): boolean {
    const state = this._host._state
    return !state.closed && state.hasMember(member)
  }

  private _countNamed(member: string): NamedCounts {
    const counts = { own: 0, unreached: 0 }
    for (const track of this._wants.namedTracksOf(member)) {
      const received = this._wants.wantsNamedPair(member, track)
      if (!this._wants.namesEveryMember(track)) counts.own += Number(received)
      else counts.unreached += Number(!received)
    }
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
    if (this._everyTrack.has(member)) return
    const host = this._host
    const lane = this._newLaneSubscription(true)
    this._everyTrack.set(member, lane)
    lane.sync(true, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'binary-all', member }, (framed, info) =>
        host._onBinary(framed, info),
      ),
    )
    this._onceDelivering(
      lane,
      () => this._everyTrack.get(member) === lane,
      () => this._queued.members.add(member),
    )
  }

  private _openNamedLane(member: string, track: string): LaneSubscription {
    const host = this._host
    const key = binaryLaneKey(member, track)
    const lane = this._newLaneSubscription(true)
    this._binary.set(key, lane)
    let tracks = this._binaryTracks.get(member)
    if (!tracks) this._binaryTracks.set(member, (tracks = new Set()))
    tracks.add(track)
    lane.sync(true, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'binary', member, track }, (framed, info) =>
        host._onBinary(framed, info),
      ),
    )
    this._onceDelivering(
      lane,
      () => this._binary.get(key) === lane,
      () => this._queuePair(member, track),
    )
    return lane
  }

  /** Demand is reported once a lane is ready, so an encoder started on demand loses no frames. */
  private _onceDelivering(lane: LaneSubscription, current: () => boolean, queue: () => void): void {
    void withinRoomHorizon(lane.ready)
      .then(() => {
        if (!current()) return
        this._delivering.add(lane)
        queue()
        this._schedulePlan()
      })
      .catch(reportRoomError)
  }

  /** An every-track lane nothing here wants any more stops once the named lanes replacing it deliver. */
  private _releaseEveryTrack(member: string, all: LaneSubscription): void {
    const pending = [...(this._binaryTracks.get(member) ?? [])]
      .map((track) => this._binary.get(binaryLaneKey(member, track))!)
      .filter((lane) => !this._delivering.has(lane))
    if (pending.length === 0) {
      all.stop()
      this._everyTrack.delete(member)
      return
    }
    if (this._releasing.has(member)) return
    this._releasing.add(member)
    void withinRoomHorizon(Promise.all(pending.map((lane) => lane.ready)))
      .finally(() => this._releasing.delete(member))
      .then(() => {
        this._queued.members.add(member)
        this._schedulePlan()
      })
      .catch(reportRoomError)
  }

  private _openInbox(member: string): void {
    const host = this._host
    if (host._state.closed || !host._ownsMember(member)) return
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

  private _newLaneSubscription(binary = false): LaneSubscription {
    return new LaneSubscription(
      (slot, error) => this._onTerminal(slot, error),
      () => void this.reconcileAuthority().catch(reportRoomError),
      (slot, pending) => {
        if (!binary) return
        if (!pending) return void this._binaryPending.delete(slot)
        this._binaryPending.add(slot)
        this._binaryReady = null
      },
    )
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
    await this.reconcileAuthority()
  }

  /** One roster refresh at a time; concurrent callers join it. */
  private _refreshMembers(): Promise<void> {
    this._pendingRefresh ??= this._runMemberRefresh().finally(() => {
      this._pendingRefresh = null
    })
    return this._pendingRefresh
  }

  /** One roster read, with the membership events this view got meanwhile replayed onto it, so it undoes none. */
  private async _runMemberRefresh(): Promise<void> {
    const host = this._host
    if (host._state.closed) return
    const read = await host._state.recordingMembership(
      () => reapAndReadRoster(host._incarnation),
      (roster, events) => {
        if (host._state.closed) return false
        const { members, departing } = replayMembership(roster, events)
        host._applyAuthorityRoster(members, departing)
        return true
      },
    )
    if (read) await this.replan()
  }

  // Graceful departures use events; heartbeats renew owned members and reap records orphaned by hard crashes.
  private _syncHeartbeat(): void {
    const host = this._host
    const want = !host._state.closed && (this._control.wanted || this._inbox.size > 0 || this._demand.isActive())
    if (want && !this._heartbeatTimer) {
      this._heartbeatTimer = unrefTimer(
        setInterval(() => void this._heartbeatTick().catch(reportRoomError), ROOM_HEARTBEAT_INTERVAL_MS),
      )
    } else if (!want && this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer)
      this._heartbeatTimer = null
    }
  }

  private async _heartbeatTick(): Promise<void> {
    if (this._heartbeatBusy) return // a slow backend must not pile up overlapping ticks
    this._heartbeatBusy = true
    const host = this._host
    try {
      // No cell I/O, so member-cell latency never delays demand renewal.
      this._demand.heartbeat()
      let renewalFailure: { error: unknown } | null = null
      try {
        await renewMemberLeases(host.id, host._inc, host._ownedMembers().renewable)
      } catch (error) {
        renewalFailure = { error }
      }
      try {
        await host._renewViews()
      } catch (error) {
        renewalFailure ??= { error }
      }
      await this.replan() // bounded retry trigger for still-wanted terminal lanes
      await this.reconcileAuthority() // the roster read reaps crashed instances' expired members and views
      if (!host._state.closed) await maintainHold(host._incarnation)
      if (renewalFailure) throw renewalFailure.error
    } finally {
      this._heartbeatBusy = false
    }
  }
}

function newBinaryPlan(): BinaryPlan {
  return { all: false, members: new Set(), tracks: new Set(), pairs: new Map() }
}

function isEmpty(plan: BinaryPlan): boolean {
  return !plan.all && plan.members.size === 0 && plan.tracks.size === 0 && plan.pairs.size === 0
}
