export { RoomHeartbeat }

import { getRoomPlane } from '../../backend/plane.js'
import { unrefTimer } from 'telefunc/__internal'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { RoomModel } from '../../shared/RoomModel.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from '../constants.js'
import type { DemandReport } from '../lanes/DemandReport.js'
import type { DemandExchange } from '../lanes/DemandExchange.js'
import { sweepLapsedRoomsSoon } from '../store/directory.js'
import { reportRoomError } from '../reportRoomError.js'
import { reapLapsedLeases } from '../store/lease.js'
import { maintainHold } from './hold.js'
import type { RoomIncarnation } from '../protocol.js'
import { dropReplacedMetaGenerations } from '../store/meta.js'
import type { AuthoritySync } from './AuthoritySync.js'
import type { RoomHosting } from './RoomHosting.js'
import type { LanePlan } from '../lanes/LanePlan.js'
assertIsNotBrowser()

type RoomHeartbeatOptions = {
  /** What it renews at the authority, and redoes where it failed. */
  hosting: RoomHosting
  incarnation: RoomIncarnation
  model: RoomModel
  lanePlan: LanePlan
  outgoingDemand: DemandReport
  /** Renews what this process told of its demand. */
  demandExchange: DemandExchange
  authority: AuthoritySync
}

/** Graceful departures use events; the heartbeat renews this server's lease and its demand, reaps the lapsed leases
 *  whose servers crashed, and reads the roster again once the control lane is past what the view covers. It runs while
 *  the room is open and this process observes it, owns an inbox, or reports demand. */
class RoomHeartbeat {
  private timer: ReturnType<typeof setInterval> | null = null
  /** A slow backend must not pile up overlapping ticks. */
  private ticking = false
  private checkingCoverage = false

  private readonly hosting: RoomHosting
  private readonly incarnation: RoomIncarnation
  private readonly model: RoomModel
  private readonly lanePlan: LanePlan
  private readonly outgoingDemand: DemandReport
  private readonly demandExchange: DemandExchange
  private readonly authority: AuthoritySync

  constructor({
    hosting,
    incarnation,
    model,
    lanePlan,
    outgoingDemand,
    demandExchange,
    authority,
  }: RoomHeartbeatOptions) {
    this.hosting = hosting
    this.incarnation = incarnation
    this.model = model
    this.lanePlan = lanePlan
    this.outgoingDemand = outgoingDemand
    this.demandExchange = demandExchange
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
    try {
      const [renewal, reconciled] = await Promise.allSettled([
        Promise.all([this.demandExchange.renew(), this.hosting.maintain()]),
        this.model.closed ? null : this.authority.reconcileMeta(getRoomPlane().direct),
      ])
      const head = reconciled.status === 'fulfilled' ? reconciled.value : null
      if (head !== null) {
        const { id, inc } = this.incarnation
        await Promise.all([
          getRoomPlane().call((store) => dropReplacedMetaGenerations(store, id, inc)),
          getRoomPlane().call((store) => reapLapsedLeases(store, this.incarnation)),
        ])
        // Each reads what the reap left.
        if (!this.checkingCoverage) void this.checkCoverage().catch(reportRoomError)
        await maintainHold(this.incarnation, head)
      }
      sweepLapsedRoomsSoon()
      for (const result of [renewal, reconciled]) if (result.status === 'rejected') throw result.reason
    } finally {
      this.ticking = false
    }
  }

  /** One at a time, beside the ticks: the roster refresh it may wait for can outlast many. */
  private async checkCoverage(): Promise<void> {
    this.checkingCoverage = true
    try {
      await this.authority.checkCoverage()
    } finally {
      this.checkingCoverage = false
    }
  }
}
