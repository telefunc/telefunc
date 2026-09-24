export { RoomSubscriptions }
export type { SubscriptionHost, HolderWants }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getRoomBackend } from '../../backend/install.js'
import type { LaneId } from '../../backend/room/contract.js'
import type { BackendSubscription } from '../../backend/subscription.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import { DEFAULT_TRACK, mergeTrackWants, wantsAnyBinary, type BinaryWants } from '../binary.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS } from '../constants.js'
import type { RoomDemand } from '../demand.js'
import { RoomError } from '../errors.js'
import type { MemberSnapshot, RoomConfigRecord } from '../protocol.js'
import type { RoomState } from '../state.js'
import { reportRoomError } from './errors.js'
import { LaneSubscription } from './lane-subscription.js'
import { binaryLaneKey } from './replay.js'
import { CONTROL_LANE, SEMANTIC_LANE, decodeRoomText, withinRoomHorizon } from './lanes.js'
import { readAllMembers, renewMemberLease } from './membership.js'
assertIsNotBrowser()

const ROOM_REPLAN_LIMIT = 5

/** What the room's holders (client stubs, its own listeners, a pre-attach tail) want, aggregated. */
type HolderWants = {
  observed: boolean
  text: { all: boolean; members: ReadonlySet<string> }
  announce: boolean
  binary: BinaryWants
}

/** The room side these subscriptions serve: it aggregates its holders and applies what the lanes and the authority carry. */
type SubscriptionHost = {
  readonly id: string
  readonly _inc: string
  /** Read-only here: every change goes through the host's methods. */
  readonly _state: Pick<
    RoomState,
    'closed' | 'rosterKnown' | 'listenerCount' | 'membershipVersion' | 'getRemote' | 'listMemberIds' | 'memberTracks'
  >
  _holderWants(): HolderWants
  /** Whether some holder receives the pair: a publisher's own suppressed frames are no demand and need no lane. */
  _wantsBinary(member: string, track: string): boolean
  /** A pending admission owns its inbox, but its record is renewed only once it commits. */
  _ownedMembers(): { all: string[]; renewable: string[] }
  _onCtrlMessage(serialized: string, info: WirePublishInfo): void
  _onTextData(serialized: string, info: WirePublishInfo): void
  _onBinary(framed: Uint8Array, info: WirePublishInfo): void
  _onDm(serialized: string, info: WirePublishInfo): void
  _readOpenConfig(): Promise<RoomConfigRecord | null>
  _applyAuthorityConfig(config: RoomConfigRecord): void
  /** `true` when the complete roster corrected a drift. */
  _applyAuthorityRoster(members: MemberSnapshot[]): boolean
  _closeFromAuthority(): void
  _onRosterDrift(): void
  _applyLeave(id: string): void
}

type SubscriptionPlan = {
  open: boolean
  observed: boolean
  becomesObserved: boolean
  wantSemantic: boolean
  wantAnyBinary: boolean
  needsRoster: boolean
  binaryPairs: Array<[string, string]>
}

/** What this instance subscribes: a function of its holders' wants, ownership and membership, kept current, recovered
 *  within Room's horizon, and renewed by the heartbeat. */
class RoomSubscriptions {
  private readonly _control = this._newLaneSubscription()
  private readonly _semantic = this._newLaneSubscription()
  /** Keyed by (member, track) and by member. */
  private readonly _binary = new Map<string, LaneSubscription>()
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

  replan(): void {
    const plan = this._derivePlan()
    this._syncControlAndSemantic(plan)
    this._syncRoster(plan)
    this._syncBinary(plan)
    this._syncInbox(plan)
    this._syncHeartbeat()
  }

  binaryReady(): Promise<void> {
    const pending: Promise<void>[] = []
    for (const subscription of this._binary.values()) pending.push(subscription.ready)
    return pending.length === 0
      ? Promise.resolve()
      : withinRoomHorizon(Promise.all(pending), ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS).then(() => undefined)
  }

  ensureRoster(): Promise<void> {
    if (this._pendingRefresh !== null) return this._pendingRefresh
    const state = this._host._state
    if (state.closed || (state.rosterKnown && this._control.established)) return Promise.resolve()
    return this._refreshMembers()
  }

  /** Unknown-sender traffic heals an at-most-once roster drift through one single-flight snapshot. */
  healUnknownSender(from: string): void {
    const state = this._host._state
    if (!state.rosterKnown || state.getRemote(from) !== null) return
    void this._refreshMembers().catch(reportRoomError)
  }

  async reconcileAuthority(): Promise<void> {
    const host = this._host
    if (host._state.closed) return
    const config = await host._readOpenConfig()
    if (config === null) return host._closeFromAuthority()
    host._applyAuthorityConfig(config)
    await this._refreshMembers()
  }

  private _derivePlan(): SubscriptionPlan {
    const state = this._host._state
    const open = !state.closed
    const wants = this._host._holderWants()
    const wantAnyBinary = open && wantsAnyBinary(wants.binary)
    return {
      open,
      observed: wants.observed,
      becomesObserved: open && wants.observed && !this._control.active,
      wantSemantic: open && (wants.text.all || wants.text.members.size > 0 || wants.announce),
      wantAnyBinary,
      needsRoster: state.listenerCount > 0 || wantAnyBinary,
      binaryPairs: open ? this._binaryPairs(wants.binary) : [],
    }
  }

  private _syncControlAndSemantic(plan: SubscriptionPlan): void {
    const host = this._host
    this._control.sync(plan.open && plan.observed, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, CONTROL_LANE, (payload, info) =>
        host._onCtrlMessage(decodeRoomText(payload), info),
      ),
    )
    this._semantic.sync(plan.wantSemantic, () =>
      getRoomBackend().subscribeLane(host.id, host._inc, SEMANTIC_LANE, (payload, info) =>
        host._onTextData(decodeRoomText(payload), info),
      ),
    )
  }

  private _syncRoster(plan: SubscriptionPlan): void {
    const state = this._host._state
    if ((plan.becomesObserved && state.rosterKnown) || (plan.open && !state.rosterKnown && plan.needsRoster))
      void this._refreshMembers().catch(reportRoomError)
  }

  private _syncBinary(plan: SubscriptionPlan): void {
    const host = this._host
    this._syncKeyedSubs(
      this._binary,
      plan.binaryPairs.map(([member, track]) => ({
        key: binaryLaneKey(member, track),
        value: { kind: 'binary', member, track },
      })),
      (lane) =>
        getRoomBackend().subscribeLane(host.id, host._inc, lane, (framed, info) => host._onBinary(framed, info)),
    )
    if (plan.binaryPairs.length === 0) return this._demand.sync([])
    // Demand is reported once the lanes are ready, so an encoder started on demand loses no frames.
    void this.binaryReady()
      .then(() => {
        this._demand.sync(host._state.closed ? [] : this._binaryPairs(host._holderWants().binary))
      })
      .catch(reportRoomError)
  }

  private _syncInbox(plan: SubscriptionPlan): void {
    const host = this._host
    const owned = plan.open ? host._ownedMembers().all : []
    this._syncKeyedSubs(
      this._inbox,
      owned.map((member) => ({ key: member, value: { kind: 'inbox', member } as const })),
      (lane) =>
        getRoomBackend().subscribeLane(host.id, host._inc, lane, (payload, info) =>
          host._onDm(decodeRoomText(payload), info),
        ),
    )
  }

  /** Declared wants filter the room's members; a want naming anyone else takes effect on their `join`. */
  private _binaryPairs(wants: BinaryWants): Array<[string, string]> {
    const state = this._host._state
    const pairs: Array<[string, string]> = []
    for (const memberId of state.listMemberIds()) {
      const memberWants = wants.members[memberId]
      const eff = memberWants ? mergeTrackWants(wants.everyMember, memberWants) : wants.everyMember
      const tracks = eff.all ? [DEFAULT_TRACK, ...state.memberTracks(memberId)] : eff.tracks
      for (const track of tracks) if (this._host._wantsBinary(memberId, track)) pairs.push([memberId, track])
    }
    return pairs
  }

  private _syncKeyedSubs<T extends LaneId>(
    subs: Map<string, LaneSubscription>,
    wantedEntries: Array<{ key: string; value: T }>,
    subscribe: (value: T) => BackendSubscription,
  ): void {
    const wanted = new Map(wantedEntries.map(({ key, value }) => [key, value]))
    for (const [key, slot] of [...subs]) {
      if (!wanted.has(key)) {
        subs.delete(key)
        slot.stop()
      }
    }
    for (const [key, value] of wanted) {
      let slot = subs.get(key)
      if (!slot) subs.set(key, (slot = this._newLaneSubscription()))
      slot.sync(true, () => subscribe(value))
    }
  }

  private _newLaneSubscription(): LaneSubscription {
    return new LaneSubscription(
      (slot, error) => this._onTerminal(slot, error),
      () => void this.reconcileAuthority().catch(reportRoomError),
    )
  }

  /** Recover a still-wanted terminal lane inside Room's one policy horizon. */
  private _onTerminal(slot: LaneSubscription, failure?: unknown): void {
    if (failure !== undefined) reportRoomError(failure)
    if (this._recovering.has(slot)) return
    this._recovering.add(slot)
    void this._recover(slot)
      .catch(reportRoomError)
      .finally(() => this._recovering.delete(slot))
  }

  private async _recover(slot: LaneSubscription): Promise<void> {
    const deadline = Date.now() + ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS
    for (let attempt = 0; attempt <= ROOM_REPLAN_LIMIT && slot.wanted && Date.now() < deadline; attempt++) {
      const outcome = await this._attemptRecovery(slot, deadline).catch((error: unknown) => reportRoomError(error))
      if (outcome === 'closed') return this._host._closeFromAuthority()
      // Catch up on what the outage dropped; the lane itself is healthy.
      if (outcome === 'ready') return await this.reconcileAuthority()
    }
    if (!slot.wanted) return
    reportRoomError(new Error(`Room subscription recovery exhausted: ${this._host.id}`))
    slot.markLost()
  }

  /** One replacement attempt, within its share of the horizon. */
  private async _attemptRecovery(slot: LaneSubscription, deadline: number): Promise<'closed' | 'ready'> {
    if ((await withinRoomHorizon(this._host._readOpenConfig(), deadline - Date.now())) === null) return 'closed'
    slot.retry()
    const attemptMs = ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS / (ROOM_REPLAN_LIMIT + 1)
    await withinRoomHorizon(slot.attemptReady, Math.min(attemptMs, deadline - Date.now()))
    return 'ready'
  }

  /** Roster refresh replans on membership-version drift and re-seeds streamed views from the committed snapshot. */
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
      const members = await readAllMembers(host.id, host._inc)
      if (host._state.membershipVersion === version) {
        const drifted = host._applyAuthorityRoster(members)
        this.replan()
        if (drifted) host._onRosterDrift()
        return
      }
      if (attempt === ROOM_REPLAN_LIMIT) throw new RoomError(`Room roster refresh contention: ${host.id}`)
    }
  }

  // Graceful departures use events; heartbeats renew owned members and reap records orphaned by hard crashes.
  private _syncHeartbeat(): void {
    const host = this._host
    const want =
      !host._state.closed && (this._control.active || host._ownedMembers().all.length > 0 || this._demand.isActive())
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
          if (!(await renewMemberLease(host.id, host._inc, id))) host._applyLeave(id)
        } catch (error) {
          renewalFailure ??= { error }
        }
      }
      this.replan() // bounded retry trigger for still-wanted terminal lanes
      await this.reconcileAuthority() // the roster read reaps crashed instances' expired members
      if (renewalFailure) throw renewalFailure.error
    } finally {
      this._heartbeatBusy = false
    }
  }
}
