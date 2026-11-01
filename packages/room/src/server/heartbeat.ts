export { RoomHeartbeat }
export type { RoomHeartbeatOptions, RoomHeartbeatOwner }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../utils/unrefTimer.js'
import type { RoomModel } from '../shared/model.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from './constants.js'
import type { DemandReport } from './demand.js'
import { sweepLapsedRoomsSoon } from './directory.js'
import { reportRoomError } from './reportRoomError.js'
import { reapLapsedLeases } from './leases.js'
import { maintainHold, type RoomIncarnation } from './lifecycle.js'
import { dropReplacedMetaGenerations } from './meta.js'
import type { AuthoritySync } from './AuthoritySync.js'
import type { LanePlan } from './LanePlan.js'
assertIsNotBrowser()

/** The replica the heartbeat keeps at the authority. */
interface RoomHeartbeatOwner {
  /** Renews what the replica told of its demand. */
  renewDemand(): Promise<void>
  maintainLease(): Promise<void>
}

type RoomHeartbeatOptions = {
  owner: RoomHeartbeatOwner
  incarnation: RoomIncarnation
  model: Pick<RoomModel, 'closed'>
  lanePlan: Pick<LanePlan, 'observing' | 'ownsInbox' | 'resumeDropped'>
  outgoingDemand: Pick<DemandReport, 'reporting'>
  authority: Pick<AuthoritySync, 'reconcileMeta' | 'checkCoverage'>
}

/** Graceful departures use events; the heartbeat renews this server's lease and its demand, reaps the lapsed leases
 *  whose servers crashed, and reads the roster again once the control lane is past what the view covers. It runs while
 *  the room is open and this process observes it, owns an inbox, or reports demand. */
class RoomHeartbeat {
  private timer: ReturnType<typeof setInterval> | null = null
  /** A slow backend must not pile up overlapping ticks. */
  private ticking = false

  private readonly owner: RoomHeartbeatOwner
  private readonly incarnation: RoomIncarnation
  private readonly model: RoomHeartbeatOptions['model']
  private readonly lanePlan: RoomHeartbeatOptions['lanePlan']
  private readonly outgoingDemand: RoomHeartbeatOptions['outgoingDemand']
  private readonly authority: RoomHeartbeatOptions['authority']

  constructor({ owner, incarnation, model, lanePlan, outgoingDemand, authority }: RoomHeartbeatOptions) {
    this.owner = owner
    this.incarnation = incarnation
    this.model = model
    this.lanePlan = lanePlan
    this.outgoingDemand = outgoingDemand
    this.authority = authority
  }

  /** Starts or stops the heartbeat by what this process is to the room now. */
  update(): void {
    const on =
      !this.model.closed && (this.lanePlan.observing || this.lanePlan.ownsInbox || this.outgoingDemand.reporting)
    if (on && !this.timer) {
      this.timer = unrefTimer(setInterval(() => void this.tick().catch(reportRoomError), ROOM_HEARTBEAT_INTERVAL_MS))
    } else if (!on && this.timer) {
      clearInterval(this.timer)
      this.timer = null
    }
  }

  private async tick(): Promise<void> {
    if (this.ticking) return
    this.ticking = true
    const owner = this.owner
    try {
      await owner.renewDemand()
      const renewalFailure = await owner.maintainLease().then(
        () => null,
        (error: unknown) => ({ error }),
      )
      this.lanePlan.resumeDropped()
      if (!this.model.closed && (await this.authority.reconcileMeta())) {
        const incarnation = this.incarnation
        await dropReplacedMetaGenerations(incarnation.id, incarnation.inc)
        await reapLapsedLeases(incarnation)
        await this.authority.checkCoverage()
      }
      if (!this.model.closed) await maintainHold(this.incarnation)
      sweepLapsedRoomsSoon()
      if (renewalFailure) throw renewalFailure.error
    } finally {
      this.ticking = false
    }
  }
}
