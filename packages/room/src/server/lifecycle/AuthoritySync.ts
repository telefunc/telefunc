export { AuthoritySync }
export type { AuthorityChange, AuthoritySyncOptions, AuthoritySyncOwner }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { raceTimeout } from '../../utils/raceTimeout.js'
import { createDeferred, unrefTimer } from 'telefunc/__internal'
import { getRoomPlane } from '../../backend/plane.js'
import type { RoomHead } from '../../backend/contract.js'
import { ROOM_HORIZON_MS, ROOM_META_REPAIR_MS } from '../constants.js'
import type { RoomIncarnation } from '../protocol.js'
import type { RoomModel } from '../../shared/RoomModel.js'
import type { RoomCtrlEnvelope } from '../../shared/protocol.js'
import { reportRoomError } from '../reportRoomError.js'
import { CONTROL_LANE, openConfig } from '../lanes/lanes.js'
import { readLeaveCauses, reapAndReadRoster } from '../store/roster.js'
import { dropReplacedMetaGenerations, readRoomMeta, readRoomMetaSeq } from '../store/meta.js'
import type { LanePlan } from '../lanes/LanePlan.js'
import type { RosterStream } from './RosterStream.js'
assertIsNotBrowser()

/** What the sync's reads found, as the control lane would carry it: newer room meta, the room closed, or a failover
 *  rewound the lane, so the view forgets its meta seqs. */
type AuthorityChange = Extract<RoomCtrlEnvelope, { __r: 'update' | 'closed' }> | { __r: 'rewound' }

/** The replica whose view the sync keeps, which applies what the sync reads. */
interface AuthoritySyncOwner {
  applyControl(change: AuthorityChange): void
}

type AuthoritySyncOptions = {
  owner: AuthoritySyncOwner
  incarnation: RoomIncarnation
  /** Reconciles a roster read into the view, and sends it to the clients it concerns. */
  roster: Pick<RosterStream, 'reconcile'>
  /** Read-only here: every change goes through the owner's methods. */
  model: Pick<
    RoomModel,
    'closed' | 'metaWaiting' | 'metaSeq' | 'rosterKnown' | 'memberIds' | 'recordMembership' | 'stopRecordingMembership'
  >
  lanePlan: Pick<LanePlan, 'replan' | 'resumeDropped' | 'observing' | 'controlEstablished'>
}

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
  /** The waits for delivery to reach a seq, each settled once it does or past Room's horizon. */
  private readonly waiters = new Set<{ seq: number; reached: () => void }>()
  /** The control lane's mark once its current subscription was ready: what it committed up to then never arrives on
   *  it, so a roster read settles it. Null until read. */
  private baseline: Promise<number> | null = null

  private readonly owner: AuthoritySyncOwner
  private readonly incarnation: RoomIncarnation
  private readonly roster: AuthoritySyncOptions['roster']
  private readonly model: AuthoritySyncOptions['model']
  private readonly lanePlan: AuthoritySyncOptions['lanePlan']

  constructor({ owner, incarnation, roster, model, lanePlan }: AuthoritySyncOptions) {
    this.owner = owner
    this.incarnation = incarnation
    this.roster = roster
    this.model = model
    this.lanePlan = lanePlan
  }

  ensureRoster(): Promise<void> {
    if (this.pendingRefresh !== null) return this.pendingRefresh
    if (this.model.closed || this.current()) return Promise.resolve()
    return this.refreshMembers()
  }

  /** What a new handle reads of a view it shares: nothing while the view is current, else the meta and the roster. */
  async catchUp(): Promise<void> {
    if (this.model.closed || this.current()) return
    if (await this.reconcileMeta()) await this.refreshMembers()
  }

  async reconcileAuthority(): Promise<void> {
    if (this.model.closed || !(await this.reconcileMeta())) return
    await dropReplacedMetaGenerations(this.incarnation.id, this.incarnation.inc)
    await this.refreshMembers()
  }

  /** The heartbeat's check, after the meta and the lapsed leases: the roster read again only once the control lane
   *  committed past what this view covers, or doesn't deliver to it. */
  async checkCoverage(): Promise<void> {
    const before = this.covered
    const mark = await this.controlMark()
    // The authority's seq never goes below what a view covered, unless a failover rewound it.
    if (mark < before) {
      this.onControlLaneRewound()
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
    if (seq <= this.received) this.onControlLaneRewound()
    if (seq !== this.received + 1) this.runStart = seq
    this.received = seq
    for (const waiter of this.waiters) if (waiter.seq <= seq) waiter.reached()
    if (seq <= this.covered + 1) this.covered = Math.max(this.covered, seq)
    this.joinRun()
    if (this.covered < this.received) {
      this.repairSoon('control', () => {
        if (this.covered < this.received && !this.model.closed) {
          void this.reconcileAuthority().catch(reportRoomError)
        }
      })
    }
  }

  /** The control lane's subscription is ready, new or again after a loss. It delivers only what commits from here on,
   *  so the mark it starts at is read now, before a roster read compares delivery with a later one. */
  controlSubscribed(): void {
    this.baseline = null
    void this.subscriptionBaseline().catch(reportRoomError)
  }

  /** The meta and roster cells share the control lane's slot, so a failover rewound them with it. */
  private onControlLaneRewound(): void {
    this.baseline = null
    this.covered = 0
    this.received = 0
    this.runStart = 0
    this.owner.applyControl({ __r: 'rewound' })
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
    return (await getRoomPlane().readLaneOrder(this.incarnation.id, this.incarnation.inc, CONTROL_LANE))?.seq ?? 0
  }

  /** A meta change waits on one before it. Unless that one arrives, a whole read repairs the gap
   *  `ROOM_META_REPAIR_MS` after the first wait rather than at the next heartbeat. */
  metaWaiting(): void {
    this.repairSoon('meta', () => {
      const model = this.model
      if (model.metaWaiting && !model.closed) void this.reconcileMeta().catch(reportRoomError)
    })
  }

  /** A member's meta change arrived past a gap and was dropped: a roster read `ROOM_META_REPAIR_MS` after it repairs
   *  the member rather than the next heartbeat's. */
  memberMetaGap(): void {
    this.repairSoon('memberMeta', () => {
      if (!this.model.closed) void this.refreshMembers().catch(reportRoomError)
    })
  }

  /** One roster refresh at a time; concurrent callers join it. */
  refreshMembers(): Promise<void> {
    this.pendingRefresh ??= this.refreshOnce()
    return this.pendingRefresh
  }

  private async refreshOnce(): Promise<void> {
    try {
      await this.runMemberRefresh()
    } finally {
      this.pendingRefresh = null
    }
  }

  /** Whether frames the current control subscription will deliver are still in flight past Room's horizon, behind
   *  what a roster read just covered. A leave the read no longer lists may then be on its way, and only its event or
   *  its departing record ends that member. Until delivery reaches the lane's order after the read, the read waits for
   *  it, and for the leave events it brings. What the lane committed before the subscription is history, which never
   *  arrives: the read settles it at once. A view the lane doesn't deliver to, or a first roster, which narrates
   *  nothing, waits for nothing. */
  private async deliveryBehind(): Promise<boolean> {
    if (!this.model.rosterKnown || !this.lanePlan.observing || !this.lanePlan.controlEstablished) return false
    const [mark, baseline] = await Promise.all([this.controlMark(), this.subscriptionBaseline()])
    if (mark <= Math.max(baseline, this.received)) return false
    const reached = createDeferred<boolean>()
    const waiter = { seq: mark, reached: () => reached.resolve(true) }
    this.waiters.add(waiter)
    try {
      return !(await raceTimeout(reached.promise, ROOM_HORIZON_MS, () => false))
    } finally {
      this.waiters.delete(waiter)
    }
  }

  /** The current subscription's baseline, read again after a failed read. */
  private subscriptionBaseline(): Promise<number> {
    const read = (this.baseline ??= this.controlMark())
    read.catch(() => {
      if (this.baseline === read) this.baseline = null
    })
    return read
  }

  /** The roster is known, and the control lane delivers every change since. */
  private current(): boolean {
    return this.model.rosterKnown && this.lanePlan.observing && this.lanePlan.controlEstablished
  }

  /** The stored meta, applied if newer, and read whole only when it holds a write the view lacks or one waits. Returns
   *  the head it read, null once the room is gone. */
  async reconcileMeta(): Promise<RoomHead | null> {
    const { id, inc } = this.incarnation
    const [head, seq] = await Promise.all([
      getRoomPlane().driver.readHead(id),
      this.model.metaWaiting ? null : readRoomMetaSeq(id, inc),
    ])
    const config = openConfig(head, inc)
    if (config !== null && seq !== null && seq <= this.model.metaSeq) return head
    const stored = config === null ? null : await readRoomMeta(id, config)
    if (stored === null) {
      this.owner.applyControl({ __r: 'closed' })
      return null
    }
    this.owner.applyControl({ __r: 'update', seq: stored.seq, meta: stored.entries })
    return head
  }

  /** One roster read, with the membership events this view got meanwhile replayed onto it, so it undoes none. It covers
   *  the control lane up to the order read before it, as a frame commits after the cells it changed. */
  private async runMemberRefresh(): Promise<void> {
    if (this.model.closed) return
    const mark = await this.controlMark()
    // The events grow while the roster is read and reconciled.
    const events = this.model.recordMembership()
    try {
      const roster = await reapAndReadRoster(this.incarnation)
      const listed = new Set(roster.members.map((member) => member.id))
      const unlisted = [...this.model.memberIds()].filter((id) => !listed.has(id))
      const [departing, behind] = await Promise.all([
        readLeaveCauses(this.incarnation, unlisted),
        this.deliveryBehind(),
      ])
      if (this.model.closed || !(await this.roster.reconcile({ ...roster, departing, behind }, events))) return
      // A view still behind is covered by the frames in flight, or else by the next heartbeat's read.
      if (behind) return
    } finally {
      this.model.stopRecordingMembership()
    }
    this.cover(mark)
    await this.lanePlan.replan()
  }
}
