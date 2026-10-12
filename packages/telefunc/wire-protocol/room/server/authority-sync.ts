export { AuthoritySync }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_META_REPAIR_MS } from '../constants.js'
import type { RoomConfigRecord } from '../protocol.js'
import type { RoomState } from '../state.js'
import { sweepLapsedRoomsSoon } from './directory.js'
import { reportRoomError } from './errors.js'
import { maintainHold, type RoomIncarnation } from './lifecycle.js'
import { reapAndReadRoster, renewMemberLeases } from './membership.js'
import { dropReplacedMetaGenerations, readRoomMeta, readRoomMetaSeq, type StoredRoomMeta } from './meta.js'
import type { RoomReplica } from './replica.js'
import type { RosterStream } from './roster-stream.js'
import type { RoomSubscriptions } from './subscriptions.js'
assertIsNotBrowser()

/** The handle whose view the sync keeps: what it renews, and where what the sync reads applies. */
type SyncHost = {
  readonly id: string
  readonly _inc: string
  readonly _incarnation: RoomIncarnation
  readonly _replica: RoomReplica
  /** Read-only here: every change goes through the host's methods. */
  readonly _state: Pick<RoomState, 'closed' | 'metaWaiting' | 'metaSeq' | 'rosterKnown' | 'recordingMembership'>
  /** Live: the owned members whose records committed; a pending admission's is renewed once it commits. */
  _renewableMembers(): Iterable<string>
  _renewViews(): Promise<void>
  _readOpenConfig(): Promise<RoomConfigRecord | null>
  _applyAuthorityMeta(stored: StoredRoomMeta): void
  /** Applies a complete roster read, and sends it to the clients it concerns. */
  readonly _roster: Pick<RosterStream, 'reconcile'>
  _closeFromAuthority(): void
}

/** Keeps a handle's view in step with the authority: its roster and meta reads, the repairs a gap asks for, and the
 *  heartbeat, which renews the members and views the handle holds and reads both again. */
class AuthoritySync {
  private _pendingRefresh: Promise<void> | null = null
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _metaRepair: ReturnType<typeof setTimeout> | null = null
  private _memberMetaRepair: ReturnType<typeof setTimeout> | null = null
  private _heartbeatBusy = false

  constructor(
    private readonly _host: SyncHost,
    private readonly _lanes: Pick<RoomSubscriptions, 'replan' | 'observing'>,
  ) {}

  ensureRoster(): Promise<void> {
    if (this._pendingRefresh !== null) return this._pendingRefresh
    const state = this._host._state
    if (state.closed || (state.rosterKnown && this._lanes.observing && this._host._replica.controlEstablished))
      return Promise.resolve()
    return this.refreshMembers()
  }

  async reconcileAuthority(): Promise<void> {
    const host = this._host
    if (host._state.closed || !(await this._reconcileMeta())) return
    await dropReplacedMetaGenerations(host.id, host._inc)
    await this.refreshMembers()
  }

  /** A meta change waits on one before it. Unless that one arrives, a whole read repairs the gap
   *  `ROOM_META_REPAIR_MS` after the first wait rather than at the next heartbeat. */
  metaWaiting(): void {
    if (this._metaRepair !== null) return
    this._metaRepair = unrefTimer(
      setTimeout(() => {
        this._metaRepair = null
        const state = this._host._state
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
        if (!this._host._state.closed) void this.refreshMembers().catch(reportRoomError)
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

  /** The stored meta, applied if newer, and read whole only when it holds a write the view lacks or one waits; false
   *  once the room is gone. */
  private async _reconcileMeta(): Promise<boolean> {
    const host = this._host
    const config = await host._readOpenConfig()
    if (config !== null && !host._state.metaWaiting) {
      const seq = await readRoomMetaSeq(host.id, config.inc)
      if (seq !== null && seq <= host._state.metaSeq) return true
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
    if (host._state.closed) return
    const read = await host._state.recordingMembership(
      () => reapAndReadRoster(host._incarnation),
      async (roster, events) => !host._state.closed && (await host._roster.reconcile(roster, events)),
    )
    if (read) await this._lanes.replan()
  }

  private async _heartbeatTick(): Promise<void> {
    if (this._heartbeatBusy) return // a slow backend must not pile up overlapping ticks
    this._heartbeatBusy = true
    const host = this._host
    try {
      let renewalFailure: { error: unknown } | null = null
      try {
        await renewMemberLeases(host.id, host._inc, host._renewableMembers())
      } catch (error) {
        renewalFailure = { error }
      }
      try {
        await host._renewViews()
      } catch (error) {
        renewalFailure ??= { error }
      }
      await this._lanes.replan() // bounded retry trigger for still-wanted terminal lanes
      await this.reconcileAuthority() // the roster read reaps crashed instances' expired members and views
      if (!host._state.closed) await maintainHold(host._incarnation)
      sweepLapsedRoomsSoon()
      if (renewalFailure) throw renewalFailure.error
    } finally {
      this._heartbeatBusy = false
    }
  }
}
