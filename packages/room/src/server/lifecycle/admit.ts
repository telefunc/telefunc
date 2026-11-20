export { Admissions }
export type { AdmissionsOptions, AdmissionsOwner }

import { BackendCall } from 'telefunc/__internal'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { roomClosedError } from '../../shared/errors.js'
import type { RoomCtrlEnvelope } from '../../shared/protocol.js'
import type { RoomMemberRecord } from '../protocol.js'
import type { Admission } from '../Admission.js'
import type { RoomModel } from '../../shared/RoomModel.js'
import type { RoomReplica } from '../RoomReplica.js'
import type { LanePlan } from '../lanes/LanePlan.js'
import type { RoomHosting } from './RoomHosting.js'
import type { RoomGuardHooks } from '../roomStatics.js'
import { runAfterHook } from '../RoomWrites.js'
import { readOpenConfig } from '../lanes/lanes.js'
import { reportRoomError } from '../reportRoomError.js'
import { evictMember, joinMember } from '../store/members.js'
import type { LeaveCause } from '../../types.js'
assertIsNotBrowser()

/** What a handle's joins ask of it. */
interface AdmissionsOwner {
  readonly guards: Partial<RoomGuardHooks> | null
  /** Whether a holder here holds member `id`. */
  holds(id: string): boolean
  /** Member `id`'s join applied. */
  joined(id: string): void
}

type AdmissionsOptions = {
  owner: AdmissionsOwner
  replica: Pick<RoomReplica, 'incarnation' | 'model' | 'openInbox' | 'applyControl'>
  lanePlan: Pick<LanePlan, 'observing'>
  /** Hosts the members the handle admits, and evicts again one whose eviction failed. */
  hosting: Pick<RoomHosting, 'lease' | 'evictLater'>
}

/** One handle's joins, from its guard to a committed member, and each one's rollback. A join is three awaits against a
 *  store that can stall or lose a reply, during which the holder can detach and the room can close. */
class Admissions {
  private readonly owner: AdmissionsOwner
  private readonly replica: AdmissionsOptions['replica']
  private readonly lanePlan: AdmissionsOptions['lanePlan']
  private readonly hosting: AdmissionsOptions['hosting']
  /** The admissions under way; an `owned` one the heartbeat leaves alone, and its holder's detach doesn't evict. */
  private readonly underWay = new Map<string, Admission>()

  constructor({ owner, replica, lanePlan, hosting }: AdmissionsOptions) {
    this.owner = owner
    this.replica = replica
    this.lanePlan = lanePlan
    this.hosting = hosting
  }

  private get model(): RoomModel {
    return this.replica.model
  }

  /** Whether member `id`'s join is under way. */
  has(id: string): boolean {
    return this.underWay.has(id)
  }

  /** Member `id` left, its holder detached or the room closed: its join, if under way, is given up. True where the
   *  admission still owned the member, which it then rolls back itself. */
  abandon(id: string): boolean {
    const admission = this.underWay.get(id)
    const owned = admission?.owned === true
    admission?.abandon()
    return owned
  }

  abandonAll(): void {
    for (const admission of this.underWay.values()) admission.abandon()
  }

  /** Both join paths: the guard passes, the holder takes the member, then the member commits. */
  async admit(admission: Admission, hold: () => void): Promise<void> {
    const { id, meta, identity, joinedAt, hidden } = admission
    const { incarnation } = this.replica
    const onBeforeJoin = this.owner.guards?.onBeforeJoin
    if (!hidden && onBeforeJoin) {
      // With no control lane, this handle hears of no close, however old it is: a closed room doesn't get its guard run.
      const closed = !this.lanePlan.observing && (await readOpenConfig(incarnation)) === null
      if (closed) throw roomClosedError(incarnation.id)
      await onBeforeJoin({ id, meta, identity })
    }
    hold()
    await this.commitAdmission(admission)
    if (hidden) return // a hidden participant has no post-join hook
    const onAfterJoin = this.owner.guards?.onAfterJoin
    if (onAfterJoin) await runAfterHook(() => onAfterJoin({ id, meta, identity }, { joinedAt }))
  }

  /** The member's inbox delivers before its record is durable, and the join is announced after, so no DM or event is
   *  lost. */
  private async commitAdmission(admission: Admission): Promise<void> {
    const { id, meta, identity, joinedAt, hidden } = admission
    const record: Omit<RoomMemberRecord, 'session'> = { joinedAt, metaSeq: 0, metaGen: 0 }
    const join: Extract<RoomCtrlEnvelope, { __r: 'join' }> = { __r: 'join', id, meta, joinedAt }
    if (identity !== null) record.identity = join.identity = identity
    if (hidden) record.hidden = join.hidden = true
    this.underWay.set(id, admission)
    // A holder that detached during the guard took nothing, and a room that closed meanwhile takes no one.
    if (!this.owner.holds(id) || this.model.closed) admission.abandon()
    try {
      // The member's inbox delivers (within the horizon) before its join applies, which the driver sees to.
      const inbox = { member: id, ready: this.replica.openInbox(id) }
      // Awaited once the join applied; where the join fails, that failure is the admission's.
      inbox.ready.catch(() => {})
      admission.assertLive(this.model)
      // The member's record and its join event are one write, so its leave always follows its join. Not sent once the
      // admission was abandoned, whose own error it then rejects with.
      const call = new BackendCall()
      await call
        .write(() =>
          joinMember(
            this.replica.incarnation,
            this.hosting.lease,
            { id, record, meta, join },
            { inbox, wanted: () => !admission.abandoned },
          ),
        )
        .catch((error: unknown) => {
          admission.assertLive(this.model)
          throw error
        })
      // An inbox that fails after the join was written tells the join may have run, though the member is then evicted.
      await call.read(() => inbox.ready)
      admission.assertLive(this.model)
      admission.commit()
      this.underWay.delete(id)
      this.replica.applyControl(join)
      this.owner.joined(id)
    } catch (error) {
      // A member write that rejected may still have committed, its reply lost; evicting an absent member only reads.
      // In an open room only a detach of the holder, or a removal that already evicted the member, abandons it.
      const lost = admission.abandoned && !this.model.closed
      const cause: LeaveCause = { type: lost ? 'disconnected' : 'left' }
      await this.evictOrLater(id, cause)
      this.abandonAdmission(id, cause)
      throw error
    }
  }

  /** Evicts member `id`; where that fails, the failure is reported and the heartbeat evicts it again. */
  async evictOrLater(id: string, cause: LeaveCause): Promise<void> {
    try {
      await evictMember(this.replica.incarnation, id, cause)
    } catch (error) {
      reportRoomError(error)
      this.hosting.evictLater(id)
    }
  }

  private abandonAdmission(id: string, cause: LeaveCause): void {
    this.underWay.delete(id)
    this.model.applyLeave(id, cause)
  }
}
