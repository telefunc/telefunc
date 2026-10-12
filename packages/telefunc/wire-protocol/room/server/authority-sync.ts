export { AuthoritySync }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getRoomBackend } from '../../backend/install.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_META_REPAIR_MS } from '../constants.js'
import type { RoomConfigRecord } from '../protocol.js'
import type { RoomModel } from '../state.js'
import { sweepLapsedRoomsSoon } from './directory.js'
import { reportRoomError } from './errors.js'
import { maintainHold, type RoomIncarnation } from './lifecycle.js'
import { CONTROL_LANE } from './lanes.js'
import { reapAndReadRoster, reapLapsedSessions } from './membership.js'
import { dropReplacedMetaGenerations, readRoomMeta, readRoomMetaSeq, type StoredRoomMeta } from './meta.js'
import type { RosterStream } from './roster-stream.js'
import type { RoomSubscriptions } from './subscriptions.js'
assertIsNotBrowser()

/** The replica whose view the sync keeps: what it renews, and where what the sync reads applies. */
type SyncHost = {
  readonly id: string
  readonly _inc: string
  readonly _incarnation: RoomIncarnation
  /** Read-only here: every change goes through the host's methods. */
  readonly model: Pick<RoomModel, 'closed' | 'metaWaiting' | 'metaSeq' | 'rosterKnown' | 'recordingMembership'>
  _renewSession(): Promise<void>
  /** The highest seq the control lane delivered. */
  _controlSeen(): number
  /** Renews what the replica told of its demand. */
  _renewDemand(): Promise<void>
  _readOpenConfig(): Promise<RoomConfigRecord | null>
  _applyAuthorityMeta(stored: StoredRoomMeta): void
  /** Applies a complete roster read, and sends it to the clients it concerns. */
  readonly roster: Pick<RosterStream, 'reconcile'>
  _closeFromAuthority(): void
}

/** Keeps a replica's view in step with the authority: its roster and meta reads, the repairs a gap asks for, and the
 *  heartbeat, which renews the session and the demand, reaps lapsed sessions, and reads the roster again once the
 *  control lane is past what this view received. */
class AuthoritySync {
  private _pendingRefresh: Promise<void> | null = null
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _metaRepair: ReturnType<typeof setTimeout> | null = null
  private _memberMetaRepair: ReturnType<typeof setTimeout> | null = null
  private _heartbeatBusy = false
  /** The control lane's seq a roster read here started at or after. */
  private _rosterAt = 0

  constructor(
    private readonly _host: SyncHost,
    private readonly _lanes: Pick<RoomSubscriptions, 'replan' | 'observing' | 'controlEstablished'>,
  ) {}

  ensureRoster(): Promise<void> {
    if (this._pendingRefresh !== null) return this._pendingRefresh
    if (this._host.model.closed || this._current()) return Promise.resolve()
    return this.refreshMembers()
  }

  /** What a new handle reads of a view it shares: nothing while the view is current, else the meta and the roster. */
  async catchUp(): Promise<void> {
    if (this._host.model.closed || this._current()) return
    if (await this._reconcileMeta()) await this.refreshMembers()
  }

  async reconcileAuthority(): Promise<void> {
    const host = this._host
    if (host.model.closed || !(await this._reconcileMeta())) return
    await dropReplacedMetaGenerations(host.id, host._inc)
    await this.refreshMembers()
  }

  /** What a heartbeat reads of the authority: the meta, the lapsed sessions, and the roster only once the control lane
   *  committed past what this view received, or doesn't deliver to it. */
  private async _checkAuthority(): Promise<void> {
    const host = this._host
    if (host.model.closed || !(await this._reconcileMeta())) return
    await dropReplacedMetaGenerations(host.id, host._inc)
    await reapLapsedSessions(host._incarnation)
    const mark = (await getRoomBackend().readLaneOrder(host.id, host._inc, CONTROL_LANE))?.seq ?? 0
    if (this._current() && mark <= Math.max(host._controlSeen(), this._rosterAt)) return
    // A read already running may predate the mark.
    await this._pendingRefresh?.catch(() => {})
    await this.refreshMembers()
    this._rosterAt = Math.max(this._rosterAt, mark)
  }

  /** A meta change waits on one before it. Unless that one arrives, a whole read repairs the gap
   *  `ROOM_META_REPAIR_MS` after the first wait rather than at the next heartbeat. */
  metaWaiting(): void {
    if (this._metaRepair !== null) return
    this._metaRepair = unrefTimer(
      setTimeout(() => {
        this._metaRepair = null
        const state = this._host.model
        if (state.metaWaiting && !state.closed) void this._reconcileMeta().catch(reportRoomError)
      }, ROOM_META_REPAIR_MS),
    )
  }

  /** A member's meta change arrived past a gap and was dropped: a roster read `ROOM_META_REPAIR_MS` after it repairs the
   *  member rather than the next heartbeat's. */
  memberMetaGap(): void {
    if (this._memberMetaRepair !== null) return
    this._memberMetaRepair = unrefTimer(
      setTimeout(() => {
        this._memberMetaRepair = null
        if (!this._host.model.closed) void this.refreshMembers().catch(reportRoomError)
      }, ROOM_META_REPAIR_MS),
    )
  }

  /** One roster refresh at a time; concurrent callers join it. */
  refreshMembers(): Promise<void> {
    this._pendingRefresh ??= this._runMemberRefresh().finally(() => {
      this._pendingRefresh = null
    })
    return this._pendingRefresh
  }

  /** Runs the heartbeat while `on`. Graceful departures use events; the heartbeat renews owned members and reaps the
   *  records hard crashes orphaned. */
  heartbeat(on: boolean): void {
    if (on && !this._heartbeatTimer) {
      this._heartbeatTimer = unrefTimer(
        setInterval(() => void this._heartbeatTick().catch(reportRoomError), ROOM_HEARTBEAT_INTERVAL_MS),
      )
    } else if (!on && this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer)
      this._heartbeatTimer = null
    }
  }

  /** The roster is known, and the control lane delivers every change since. */
  private _current(): boolean {
    return this._host.model.rosterKnown && this._lanes.observing && this._lanes.controlEstablished
  }

  /** The stored meta, applied if newer, and read whole only when it holds a write the view lacks or one waits; false
   *  once the room is gone. */
  private async _reconcileMeta(): Promise<boolean> {
    const host = this._host
    const config = await host._readOpenConfig()
    if (config !== null && !host.model.metaWaiting) {
      const seq = await readRoomMetaSeq(host.id, config.inc)
      if (seq !== null && seq <= host.model.metaSeq) return true
    }
    const stored = config && (await readRoomMeta(host.id, config))
    if (stored === null) {
      host._closeFromAuthority()
      return false
    }
    host._applyAuthorityMeta(stored)
    return true
  }

  /** One roster read, with the membership events this view got meanwhile replayed onto it, so it undoes none. */
  private async _runMemberRefresh(): Promise<void> {
    const host = this._host
    if (host.model.closed) return
    const read = await host.model.recordingMembership(
      () => reapAndReadRoster(host._incarnation),
      async (roster, events) => !host.model.closed && (await host.roster.reconcile(roster, events)),
    )
    if (read) await this._lanes.replan()
  }

  private async _heartbeatTick(): Promise<void> {
    if (this._heartbeatBusy) return // a slow backend must not pile up overlapping ticks
    this._heartbeatBusy = true
    const host = this._host
    try {
      await host._renewDemand()
      let renewalFailure: { error: unknown } | null = null
      try {
        await host._renewSession()
      } catch (error) {
        renewalFailure = { error }
      }
      await this._lanes.replan() // bounded retry trigger for still-wanted terminal lanes
      await this._checkAuthority()
      if (!host.model.closed) await maintainHold(host._incarnation)
      sweepLapsedRoomsSoon()
      if (renewalFailure) throw renewalFailure.error
    } finally {
      this._heartbeatBusy = false
    }
  }
}
