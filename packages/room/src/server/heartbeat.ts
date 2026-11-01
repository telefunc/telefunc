export { RoomHeartbeat }
export type { RoomHeartbeatOptions, RoomHeartbeatOwner }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../utils/unrefTimer.js'
import type { RoomModel } from '../model.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from './constants.js'
import type { DemandReport } from './demand.js'
import { sweepLapsedRoomsSoon } from './directory.js'
import { reportRoomError } from './errors.js'
import { reapLapsedLeases } from './leases.js'
import { maintainHold, type RoomIncarnation } from './lifecycle.js'
import { dropReplacedMetaGenerations } from './meta.js'
import type { AuthoritySync } from './AuthoritySync.js'
import type { LanePlan } from './LanePlan.js'
assertIsNotBrowser()

/** The replica the heartbeat keeps at the authority. */
interface RoomHeartbeatOwner {
  readonly incarnation: RoomIncarnation
  readonly model: Pick<RoomModel, 'closed'>
  readonly lanePlan: Pick<LanePlan, 'observing' | 'ownsInbox' | 'resumeDropped'>
  readonly outgoingDemand: Pick<DemandReport, 'reporting'>
  readonly authority: Pick<AuthoritySync, 'reconcileMeta' | 'checkCoverage'>
  /** Renews what the replica told of its demand. */
  renewDemand(): Promise<void>
  maintainLease(): Promise<void>
}

type RoomHeartbeatOptions = { owner: RoomHeartbeatOwner }

/** Graceful departures use events; the heartbeat renews this server's lease and its demand, reaps the lapsed leases
 *  whose servers crashed, and reads the roster again once the control lane is past what the view covers. It runs while
 *  the room is open and this process observes it, owns an inbox, or reports demand. */
class RoomHeartbeat {
  private readonly owner: RoomHeartbeatOwner
  private timer: ReturnType<typeof setInterval> | null = null
  /** A slow backend must not pile up overlapping ticks. */
  private ticking = false

  constructor({ owner }: RoomHeartbeatOptions) {
    this.owner = owner
  }

  /** Starts or stops the heartbeat by what this process is to the room now. */
  update(): void {
    const { model, lanePlan, outgoingDemand } = this.owner
    const on = !model.closed && (lanePlan.observing || lanePlan.ownsInbox || outgoingDemand.reporting)
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
      owner.lanePlan.resumeDropped()
      if (!owner.model.closed && (await owner.authority.reconcileMeta())) {
        const incarnation = owner.incarnation
        await dropReplacedMetaGenerations(incarnation.id, incarnation.inc)
        await reapLapsedLeases(incarnation)
        await owner.authority.checkCoverage()
      }
      if (!owner.model.closed) await maintainHold(owner.incarnation)
      sweepLapsedRoomsSoon()
      if (renewalFailure) throw renewalFailure.error
    } finally {
      this.ticking = false
    }
  }
}
