export { RoomReplica }
export type { ControlObserver, PresenceEvent }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getRoomBackend } from '../../backend/install.js'
import { reportSubscriptionEnd } from '../../backend/subscription-manager.js'
import { reportServerChannelError } from '../../server/channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from '../constants.js'
import { RoomDemand, type DemandGossip } from '../demand.js'
import type { RoomCtrlEnvelope } from '../protocol.js'
import { reportRoomError } from './errors.js'
import { GossipOutbox } from './gossip.js'
import { LaneSubscription } from './lane-subscription.js'
import { CONTROL_LANE, decodeLaneEnvelope, decodeRoomText, openConfig, withinRoomHorizon } from './lanes.js'
import { Slice } from '../slice.js'
assertIsNotBrowser()

/** What the control lane tells a handle: everything but demand, which the replica takes. */
type PresenceEvent = Exclude<RoomCtrlEnvelope, DemandGossip>

/** A handle observing the room through the replica's control lane. */
type ControlObserver = {
  onControlEvent(event: PresenceEvent, serialized: string, info: WirePublishInfo): void
  /** The lane ended and the authority says the room closed. */
  onControlClosed(): void
  /** The lane came back after a loss: what it dropped is read again. */
  onControlRecovered(): Promise<void>
}

/** A handle owning members: the demand for them is pushed to it. */
type DemandOwner = { _deliverDemand(member: string, track: string, wanted: boolean): void }

/** What this process shares of one room incarnation, whatever the number of its handles: the demand it tells and the
 *  demand for the members it owns, renewed once per heartbeat, and the control lane, each frame parsed once. It is
 *  dropped at the first heartbeat no handle observes the room, owns a member or tells demand through it. */
class RoomReplica {
  private static readonly _byInc = new Map<string, RoomReplica>()

  /** The replica of the incarnation, made if none is live. */
  static of(roomId: string, inc: string): RoomReplica {
    let replica = RoomReplica._byInc.get(inc)
    if (replica === undefined) RoomReplica._byInc.set(inc, (replica = new RoomReplica(roomId, inc)))
    return replica
  }

  /** False once dropped: a handle holding it takes the incarnation's live one. */
  live = true
  readonly demand: RoomDemand
  private readonly _gossip: GossipOutbox
  private readonly _owners = new Map<string, DemandOwner>()
  private readonly _observers = new Set<ControlObserver>()
  /** `_observers` as a delivery took them; one that stops observing during it still gets that frame. */
  private _delivering: ControlObserver[] | null = null
  private readonly _control = new LaneSubscription(
    (_slot, failure) => this._onTerminal(failure),
    () => void this._reconcileObservers().catch(reportRoomError),
  )
  private _recovering = false
  private readonly _heartbeat = unrefTimer(
    setInterval(() => void this._heartbeatTick().catch(reportRoomError), ROOM_HEARTBEAT_INTERVAL_MS),
  )
  private _heartbeatBusy = false

  private constructor(
    private readonly _roomId: string,
    private readonly _inc: string,
  ) {
    const gossip = (this._gossip = new GossipOutbox(_roomId, _inc))
    this.demand = new RoomDemand(
      (event) => gossip.push(event),
      (id) => this._owners.has(id),
      (member, track, wanted) => this._owners.get(member)?._deliverDemand(member, track, wanted),
    )
  }

  get controlEstablished(): boolean {
    return this._control.established
  }

  /** Whether the observer gets the control lane's events; observing again retries a lane whose recovery gave up. */
  observe(observer: ControlObserver, on: boolean): void {
    if (on !== this._observers.has(observer)) {
      if (on) this._observers.add(observer)
      else this._observers.delete(observer)
      this._delivering = null
    }
    this._control.sync(this._observers.size > 0, () =>
      getRoomBackend().subscribeLane(this._roomId, this._inc, CONTROL_LANE, (payload, info) =>
        this._onControl(decodeRoomText(payload), info),
      ),
    )
  }

  own(member: string, owner: DemandOwner): void {
    this._owners.set(member, owner)
  }

  /** The member's demand goes with it. */
  disown(member: string, owner: DemandOwner): void {
    if (this._owners.get(member) !== owner) return
    this._owners.delete(member)
    this.demand.forgetMember(member)
  }

  /** The incarnation closed: its demand has no reader. */
  closed(): void {
    this._gossip.close()
  }

  private _onControl(serialized: string, info: WirePublishInfo): void {
    const ctrl = decodeLaneEnvelope(serialized) as RoomCtrlEnvelope
    if (ctrl.__r === 'want' || ctrl.__r === 'covers') return this.demand.applyWant(ctrl)
    for (const observer of (this._delivering ??= [...this._observers])) observer.onControlEvent(ctrl, serialized, info)
  }

  private _onTerminal(failure?: unknown): void {
    // A replacement that ends is its recovery's failure, which the recovery reports.
    if (this._recovering) return
    if (failure !== undefined) reportSubscriptionEnd(failure, reportServerChannelError)
    this._recovering = true
    void this._recover().catch(reportRoomError)
  }

  /** A still-wanted terminal lane gets one replacement after a head read; if that fails too, an observer's next
   *  planning pass subscribes it again. */
  private async _recover(): Promise<void> {
    const slot = this._control
    try {
      const config = openConfig(await withinRoomHorizon(getRoomBackend().readHead(this._roomId)), this._inc)
      if (config === null) {
        for (const observer of [...this._observers]) observer.onControlClosed()
        return
      }
      slot.retry()
      await withinRoomHorizon(slot.attemptReady)
    } catch (error) {
      if (slot.wanted) slot.dropAttempt()
      throw error
    } finally {
      this._recovering = false
    }
    // Catch up on what the outage dropped; a later end of the lane gets its own recovery.
    await this._reconcileObservers()
  }

  private async _reconcileObservers(): Promise<void> {
    await Promise.all([...this._observers].map((observer) => observer.onControlRecovered()))
  }

  /** Renews the demand a slice at a time; a slow backend must not pile up overlapping ticks. */
  private async _heartbeatTick(): Promise<void> {
    if (this._observers.size === 0 && this._owners.size === 0 && !this.demand.reporting) return this._drop()
    if (this._heartbeatBusy) return
    this._heartbeatBusy = true
    try {
      const slice = new Slice()
      for (const _ of this.demand.heartbeat()) if (slice.over) await slice.next()
    } finally {
      this._heartbeatBusy = false
    }
  }

  private _drop(): void {
    clearInterval(this._heartbeat)
    this.live = false
    if (RoomReplica._byInc.get(this._inc) === this) RoomReplica._byInc.delete(this._inc)
  }
}
