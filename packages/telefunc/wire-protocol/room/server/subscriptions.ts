export { RoomSubscriptions }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getRoomBackend } from '../../backend/install.js'
import type { LaneId } from '../../backend/room/contract.js'
import { reportSubscriptionEnd } from '../../backend/subscription-manager.js'
import { reportServerChannelError } from '../../server/channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from '../constants.js'
import type { RoomDemand } from '../demand.js'
import type { MemberSnapshot, RoomConfigRecord } from '../protocol.js'
import { replayMembership, type RoomState } from '../state.js'
import { reportRoomError } from './errors.js'
import { LaneSubscription } from './lane-subscription.js'
import { binaryLaneKey, type LaneHolder } from './replay.js'
import { WantsIndex } from './wants-index.js'
import { CONTROL_LANE, SEMANTIC_LANE, decodeRoomText, withinRoomHorizon } from './lanes.js'
import { reapAndReadRoster, renewMemberLease } from './membership.js'
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

/** What this instance subscribes: a function of its holders' wants, ownership and membership, kept current per change,
 *  recovered within Room's horizon, and renewed by the heartbeat. */
class RoomSubscriptions {
  private readonly _wants = new WantsIndex()
  private readonly _control = this._newLaneSubscription()
  private readonly _semantic = this._newLaneSubscription()
  /** Named lanes, keyed by (member, track); `_binaryTracks` lists each member's. */
  private readonly _binary = new Map<string, LaneSubscription>()
  private readonly _binaryTracks = new Map<string, Set<string>>()
  /** Each member's every-track lane, while some holder here receives all of its tracks. */
  private readonly _everyTrack = new Map<string, LaneSubscription>()
  /** The binary lanes that were ready once: what they carry is this instance's demand. */
  private readonly _delivering = new WeakSet<LaneSubscription>()
  /** The members whose every-track lane waits for the named lanes replacing it. */
  private readonly _releasing = new Set<string>()
  /** The binary lanes not ready yet, so a readiness wait never visits the ready ones. */
  private readonly _binaryPending = new Set<LaneSubscription>()
  private readonly _inbox = new Map<string, LaneSubscription>()
  private readonly _recovering = new Set<LaneSubscription>()
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

  /** Plans everything: the heartbeat's pass, which also retries every still-wanted lane whose recovery failed. */
  replan(): void {
    this._replanAround(() => {
      const state = this._host._state
      const members = state.closed ? [] : state.listMemberIds()
      const present = new Set(members)
      for (const member of new Set([...this._binaryTracks.keys(), ...this._everyTrack.keys()]))
        if (!present.has(member)) this._syncMemberBinary(member)
      for (const member of members) this._syncMemberBinary(member)
      const owned = new Set(state.closed ? [] : this._host._ownedMembers().all)
      for (const member of [...this._inbox.keys()]) if (!owned.has(member)) this._closeInbox(member)
      for (const member of owned) this._openInbox(member)
    })
  }

  /** What the room-wide lanes depend on changed: observers, listeners, the tail. */
  syncLanes(): void {
    this._replanAround(() => {})
  }

  /** A holder attached, changed its wants (its room-wide ones, and those of `members` when given), or is gone. */
  holderChanged(holder: LaneHolder, gone = false, members?: Iterable<string>): void {
    const affected = this._wants.update(holder, gone, members)
    this._replanAround(() => {
      if (affected !== 'all') for (const member of affected) this._syncMemberBinary(member)
      else for (const member of this._host._state.listMemberIds()) this._syncMemberBinary(member)
    })
  }

  /** A member joined. */
  memberChanged(member: string): void {
    this._replanAround(() => this._syncMemberBinary(member))
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

  binaryReady(): Promise<void> {
    const pending: Promise<void>[] = []
    for (const subscription of this._binaryPending) pending.push(subscription.ready)
    return pending.length === 0 ? Promise.resolve() : withinRoomHorizon(Promise.all(pending)).then(() => undefined)
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
    this._syncHeartbeat()
  }

  /** A member's binary lanes: its every-track lane while some holder here receives all of its tracks, else a lane per
   *  track some holder names. A switch between them keeps the lanes it leaves until the lanes it takes deliver. */
  private _syncMemberBinary(member: string): void {
    const state = this._host._state
    const present = !state.closed && state.hasMember(member)
    const everyTrack = present && this._wants.wantsEveryTrack(member)
    if (everyTrack) this._openEveryTrack(member)
    const all = this._everyTrack.get(member)
    const everyTrackDemand = everyTrack && this._delivering.has(all!)
    // On before off, so a pair one lane hands to another stays wanted throughout.
    if (everyTrackDemand) this._demand.setLocal(member, null, true)
    const tracks = present ? this._wants.namedTracksOf(member) : new Set<string>()
    for (const track of [...(this._binaryTracks.get(member) ?? []), ...this._demand.localTracks(member)])
      tracks.add(track)
    for (const track of tracks) this._syncNamedLane(member, track)
    if (!everyTrackDemand) this._demand.setLocal(member, null, false)
    if (!everyTrack && all !== undefined) this._releaseEveryTrack(member, all)
  }

  /** A named lane while some holder names the track and none receives every track; beside an every-track lane, one
   *  stays until that lane delivers. The pair is demand while some lane here delivers it to a holder that wants it. */
  private _syncNamedLane(member: string, track: string): void {
    const state = this._host._state
    const present = !state.closed && state.hasMember(member)
    const named = present && this._wants.wantsNamedPair(member, track)
    const all = this._everyTrack.get(member)
    const allDelivers = all !== undefined && this._delivering.has(all)
    const key = binaryLaneKey(member, track)
    let lane = this._binary.get(key)
    const keep = present && this._wants.wantsEveryTrack(member) ? lane !== undefined && !allDelivers : named
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
    this._demand.setLocal(member, track, (named || lane !== undefined) && delivers)
  }

  private _openEveryTrack(member: string): void {
    if (this._everyTrack.has(member)) return
    const host = this._host
    const lane = this._newLaneSubscription(this._binaryPending)
    this._everyTrack.set(member, lane)
    lane.sync(true, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'binary-all', member }, (framed, info) =>
        host._onBinary(framed, info),
      ),
    )
    this._onceDelivering(
      lane,
      () => this._everyTrack.get(member) === lane,
      () => this._syncMemberBinary(member),
    )
  }

  private _openNamedLane(member: string, track: string): LaneSubscription {
    const host = this._host
    const key = binaryLaneKey(member, track)
    const lane = this._newLaneSubscription(this._binaryPending)
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
      () => this._syncNamedLane(member, track),
    )
    return lane
  }

  /** Demand is reported once a lane is ready, so an encoder started on demand loses no frames. */
  private _onceDelivering(lane: LaneSubscription, current: () => boolean, sync: () => void): void {
    void withinRoomHorizon(lane.ready)
      .then(() => {
        if (!current()) return
        this._delivering.add(lane)
        this._replanAround(sync)
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
      .then(() => this._replanAround(() => this._syncMemberBinary(member)))
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

  private _newLaneSubscription(pendingSlots?: Set<LaneSubscription>): LaneSubscription {
    return new LaneSubscription(
      (slot, error) => this._onTerminal(slot, error),
      () => void this.reconcileAuthority().catch(reportRoomError),
      (slot, pending) => {
        if (pending) pendingSlots?.add(slot)
        else pendingSlots?.delete(slot)
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
    const read = await host._state.recordingMembership(() => reapAndReadRoster(host._incarnation))
    if (host._state.closed) return
    const { members, departing } = replayMembership(read.value, read.events)
    host._applyAuthorityRoster(members, departing)
    this.replan()
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
      for (const id of host._ownedMembers().renewable) {
        try {
          await renewMemberLease(host.id, host._inc, id)
        } catch (error) {
          renewalFailure ??= { error }
        }
      }
      try {
        await host._renewViews()
      } catch (error) {
        renewalFailure ??= { error }
      }
      this.replan() // bounded retry trigger for still-wanted terminal lanes
      await this.reconcileAuthority() // the roster read reaps crashed instances' expired members and views
      if (!host._state.closed) await maintainHold(host._incarnation)
      if (renewalFailure) throw renewalFailure.error
    } finally {
      this._heartbeatBusy = false
    }
  }
}
