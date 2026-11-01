export { AuthoritySync }
export type { AuthoritySyncOptions, AuthoritySyncOwner }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../utils/unrefTimer.js'
import { getRoomBackend } from '../backend/plane.js'
import { ROOM_META_REPAIR_MS } from './constants.js'
import type { RoomConfigRecord } from './protocol.js'
import type { RoomModel } from '../state.js'
import { reportRoomError } from './errors.js'
import type { RoomIncarnation } from './lifecycle.js'
import { CONTROL_LANE } from './lanes.js'
import { reapAndReadRoster } from './roster.js'
import { dropReplacedMetaGenerations, readRoomMeta, readRoomMetaSeq, type StoredRoomMeta } from './meta.js'
import type { RosterStream } from './RosterStream.js'
import type { LanePlan } from './LanePlan.js'
assertIsNotBrowser()

/** The replica whose view the sync keeps: what it renews, and where what the sync reads applies. */
interface AuthoritySyncOwner {
  readonly id: string
  readonly incarnation: RoomIncarnation
  /** Read-only here: every change goes through the owner's methods. */
  readonly model: Pick<
    RoomModel,
    'closed' | 'metaWaiting' | 'metaSeq' | 'rosterKnown' | 'recordMembership' | 'stopRecordingMembership'
  >
  readOpenConfig(): Promise<RoomConfigRecord | null>
  applyAuthorityMeta(stored: StoredRoomMeta): void
  /** A failover rewound the control lane: the view forgets its meta seqs. */
  rewound(): void
  /** Applies a complete roster read, and sends it to the clients it concerns. */
  readonly roster: Pick<RosterStream, 'reconcile'>
  readonly lanePlan: Pick<LanePlan, 'replan' | 'resumeDropped' | 'observing' | 'controlEstablished'>
  closeFromAuthority(): void
}

type AuthoritySyncOptions = { owner: AuthoritySyncOwner }

/** The repairs a gap asks for, each due `ROOM_META_REPAIR_MS` after it. */
type Repair = 'control' | 'meta' | 'memberMeta'

/** Keeps a replica's view in step with the authority: its roster and meta reads, the repairs a gap asks for, and what
 *  the control lane covers, which the heartbeat checks. */
class AuthoritySync {
  private pendingRefresh: Promise<void> | null = null
  /** The repairs due `ROOM_META_REPAIR_MS` after the first gap that asked for each. */
  private readonly repairsDue = new Set<Repair>()
  /** Every control seq at or below it reached this view, as a frame or through a roster read started after it. */
  private covered = 0
  /** The highest control seq delivered, and the first of the delivered run that ends at it. */
  private received = 0
  private runStart = 0

  private readonly owner: AuthoritySyncOwner

  constructor({ owner }: AuthoritySyncOptions) {
    this.owner = owner
  }

  ensureRoster(): Promise<void> {
    if (this.pendingRefresh !== null) return this.pendingRefresh
    if (this.owner.model.closed || this.current()) return Promise.resolve()
    return this.refreshMembers()
  }

  /** What a new handle reads of a view it shares: nothing while the view is current, else the meta and the roster. */
  async catchUp(): Promise<void> {
    if (this.owner.model.closed || this.current()) return
    if (await this.reconcileMeta()) await this.refreshMembers()
  }

  async reconcileAuthority(): Promise<void> {
    const owner = this.owner
    if (owner.model.closed || !(await this.reconcileMeta())) return
    await dropReplacedMetaGenerations(owner.id, owner.incarnation.inc)
    await this.refreshMembers()
  }

  /** The heartbeat's check, after the meta and the lapsed leases: the roster read again only once the control lane
   *  committed past what this view covers, or doesn't deliver to it. */
  async checkCoverage(): Promise<void> {
    const before = this.covered
    const mark = await this.controlMark()
    // The authority's seq never goes below what a view covered, unless a failover rewound it.
    if (mark < before) {
      this.rewound()
      if (!(await this.reconcileMeta())) return
    }
    const covered = () => this.current() && mark <= this.covered
    if (covered()) return
    // A read already running may predate the mark.
    await this.pendingRefresh?.catch(() => {})
    if (!covered()) await this.refreshMembers()
  }

  /** A control frame reached this view. One past a seq that didn't opens a hole, which a whole read repairs
   *  `ROOM_META_REPAIR_MS` later unless the missing frame arrives meanwhile. */
  controlDelivered(seq: number): void {
    // A driver delivers a lane in order, so a seq at or below the highest one is a failover's rewind.
    if (seq <= this.received) this.rewound()
    if (seq !== this.received + 1) this.runStart = seq
    this.received = seq
    if (seq <= this.covered + 1) this.covered = Math.max(this.covered, seq)
    this.joinRun()
    if (this.covered < this.received) {
      this.repairSoon('control', () => {
        if (this.covered < this.received && !this.owner.model.closed) {
          void this.reconcileAuthority().catch(reportRoomError)
        }
      })
    }
  }

  /** The meta and roster cells share the control lane's slot, so a failover rewound them with it. */
  private rewound(): void {
    this.covered = 0
    this.received = 0
    this.runStart = 0
    this.owner.rewound()
  }

  private cover(mark: number): void {
    this.covered = Math.max(this.covered, mark)
    this.joinRun()
  }

  /** The delivered run joins what is covered once nothing is missing between them. */
  private joinRun(): void {
    if (this.runStart <= this.covered + 1) this.covered = Math.max(this.covered, this.received)
  }

  private repairSoon(repair: Repair, run: () => void): void {
    if (this.repairsDue.has(repair)) return
    this.repairsDue.add(repair)
    unrefTimer(
      setTimeout(() => {
        this.repairsDue.delete(repair)
        run()
      }, ROOM_META_REPAIR_MS),
    )
  }

  private async controlMark(): Promise<number> {
    const owner = this.owner
    return (await getRoomBackend().readLaneOrder(owner.id, owner.incarnation.inc, CONTROL_LANE))?.seq ?? 0
  }

  /** A meta change waits on one before it. Unless that one arrives, a whole read repairs the gap
   *  `ROOM_META_REPAIR_MS` after the first wait rather than at the next heartbeat. */
  metaWaiting(): void {
    this.repairSoon('meta', () => {
      const model = this.owner.model
      if (model.metaWaiting && !model.closed) void this.reconcileMeta().catch(reportRoomError)
    })
  }

  /** A member's meta change arrived past a gap and was dropped: a roster read `ROOM_META_REPAIR_MS` after it repairs
   *  the member rather than the next heartbeat's. */
  memberMetaGap(): void {
    this.repairSoon('memberMeta', () => {
      if (!this.owner.model.closed) void this.refreshMembers().catch(reportRoomError)
    })
  }

  /** One roster refresh at a time; concurrent callers join it. */
  refreshMembers(): Promise<void> {
    this.pendingRefresh ??= this.runMemberRefresh().finally(() => {
      this.pendingRefresh = null
    })
    return this.pendingRefresh
  }

  /** The roster is known, and the control lane delivers every change since. */
  private current(): boolean {
    return this.owner.model.rosterKnown && this.owner.lanePlan.observing && this.owner.lanePlan.controlEstablished
  }

  /** The stored meta, applied if newer, and read whole only when it holds a write the view lacks or one waits; false
   *  once the room is gone. */
  async reconcileMeta(): Promise<boolean> {
    const owner = this.owner
    const config = await owner.readOpenConfig()
    if (config !== null && !owner.model.metaWaiting) {
      const seq = await readRoomMetaSeq(owner.id, config.inc)
      if (seq !== null && seq <= owner.model.metaSeq) return true
    }
    const stored = config === null ? null : await readRoomMeta(owner.id, config)
    if (stored === null) {
      owner.closeFromAuthority()
      return false
    }
    owner.applyAuthorityMeta(stored)
    return true
  }

  /** One roster read, with the membership events this view got meanwhile replayed onto it, so it undoes none. It covers
   *  the control lane up to the order read before it, as a frame commits after the cells it changed. */
  private async runMemberRefresh(): Promise<void> {
    const owner = this.owner
    if (owner.model.closed) return
    const mark = await this.controlMark()
    // The events grow while the roster is read and reconciled.
    const events = owner.model.recordMembership()
    try {
      const roster = await reapAndReadRoster(owner.incarnation)
      if (owner.model.closed || !(await owner.roster.reconcile(roster, events))) return
    } finally {
      owner.model.stopRecordingMembership()
    }
    this.cover(mark)
    await this.owner.lanePlan.replan()
  }
}
