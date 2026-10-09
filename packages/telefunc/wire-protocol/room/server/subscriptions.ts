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
import { RoomError } from '../errors.js'
import type { MemberSnapshot, RoomConfigRecord } from '../protocol.js'
import type { RoomState } from '../state.js'
import { reportRoomError } from './errors.js'
import { LaneSubscription } from './lane-subscription.js'
import { binaryLaneKey, type LaneHolder } from './replay.js'
import { WantsIndex } from './wants-index.js'
import { CONTROL_LANE, SEMANTIC_LANE, decodeRoomText, withinRoomHorizon } from './lanes.js'
import { reapAndReadRoster, renewMemberLease } from './membership.js'
import { maintainHold } from './lifecycle.js'
assertIsNotBrowser()

const ROSTER_REFRESH_RETRY_LIMIT = 5

/** The room side these subscriptions serve: it aggregates its holders and applies what the lanes and the authority carry. */
type SubscriptionHost = {
  readonly id: string
  readonly _inc: string
  /** Read-only here: every change goes through the host's methods. */
  readonly _state: Pick<
    RoomState,
    'closed' | 'rosterKnown' | 'listenerCount' | 'membershipVersion' | 'listMemberIds' | 'hasMember' | 'memberTracks'
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
  _applyAuthorityConfig(config: RoomConfigRecord): void
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
  /** Keyed by (member, track); `_binaryTracks` lists each member's. */
  private readonly _binary = new Map<string, LaneSubscription>()
  private readonly _binaryTracks = new Map<string, Set<string>>()
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
      for (const member of [...this._binaryTracks.keys()]) if (!present.has(member)) this._syncMemberBinary(member)
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

  /** A holder attached, changed its wants, or is gone. */
  holderChanged(holder: LaneHolder, gone = false): void {
    const affected = this._wants.update(holder, gone)
    this._replanAround(() => {
      if (affected !== 'all') for (const member of affected) this._syncMemberBinary(member)
      else for (const member of this._host._state.listMemberIds()) this._syncMemberBinary(member)
    })
  }

  /** A member joined or published a new track. */
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
    for (const subscription of this._binary.values()) pending.push(subscription.ready)
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
    if (config === null) return host._closeFromAuthority()
    host._applyAuthorityConfig(config)
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

  /** Declared wants filter the room's members; a want naming anyone else takes effect on their `join`. */
  private _syncMemberBinary(member: string): void {
    const host = this._host
    const state = host._state
    const wanted = new Set<string>()
    if (!state.closed && state.hasMember(member)) {
      for (const track of this._wants.tracksOf(member, () => state.memberTracks(member)))
        if (this._wants.wantsPair(member, track)) wanted.add(track)
    }
    const had = this._binaryTracks.get(member)
    for (const track of had ?? []) {
      if (wanted.has(track)) continue
      const key = binaryLaneKey(member, track)
      this._binary.get(key)?.stop()
      this._binary.delete(key)
      this._demand.setLocal(member, track, false)
    }
    if (wanted.size === 0) this._binaryTracks.delete(member)
    else this._binaryTracks.set(member, wanted)
    for (const track of wanted) {
      const key = binaryLaneKey(member, track)
      let slot = this._binary.get(key)
      const added = slot === undefined
      if (!slot) this._binary.set(key, (slot = this._newLaneSubscription()))
      slot.sync(true, () =>
        getRoomBackend().subscribeLane(host.id, host._inc, { kind: 'binary', member, track }, (framed, info) =>
          host._onBinary(framed, info),
        ),
      )
      if (!added) continue
      // Demand is reported once the lane is ready, so an encoder started on demand loses no frames.
      const subscribed = slot
      void withinRoomHorizon(subscribed.ready)
        .then(() => {
          if (this._binary.get(key) === subscribed) this._demand.setLocal(member, track, true)
        })
        .catch(reportRoomError)
    }
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

  private _newLaneSubscription(): LaneSubscription {
    return new LaneSubscription(
      (slot, error) => this._onTerminal(slot, error),
      () => void this.reconcileAuthority().catch(reportRoomError),
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

  /** A roster read that raced a membership event is retried, so the committed snapshot never undoes a newer event. */
  private async _runMemberRefresh(): Promise<void> {
    const host = this._host
    for (let attempt = 0; !host._state.closed; attempt++) {
      const version = host._state.membershipVersion
      const { members, departing } = await reapAndReadRoster(host.id, host._inc)
      if (host._state.membershipVersion === version) {
        host._applyAuthorityRoster(members, departing)
        this.replan()
        return
      }
      if (attempt === ROSTER_REFRESH_RETRY_LIMIT) throw new RoomError(`Room roster refresh contention: ${host.id}`)
    }
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
      if (!host._state.closed) await maintainHold(host.id, host._inc)
      if (renewalFailure) throw renewalFailure.error
    } finally {
      this._heartbeatBusy = false
    }
  }
}
