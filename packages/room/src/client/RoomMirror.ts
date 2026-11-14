export { RoomMirror }
export type { RoomMirrorOptions, MirrorEvent }

import {
  joinedMember,
  leaveCauseFromWire,
  type MemberSnapshot,
  type RoomCtrlEnvelope,
  type RoomRosterEvent,
  type RoomSnapshotMetadata,
  type SenderStamp,
} from '../shared/protocol.js'
import { RoomModel, type RoomModelOwner } from '../shared/RoomModel.js'
import { RosterIntake } from './RosterIntake.js'
import { heldConnection } from './page.js'
import type { LeaveCause, Sender } from '../types.js'

/** The events that change who is in the room and its meta, which a mirror applies as they arrive. */
type MirrorEvent = RoomRosterEvent | Extract<RoomCtrlEnvelope, { __r: 'join' | 'leave' | 'update' }>

type RoomMirrorOptions = {
  /** The room as the response carried it. */
  snapshot: RoomSnapshotMetadata
  /** Every leave the model applies, after its views' callbacks. */
  onMemberLeft: (id: string, cause: LeaveCause) => void
  /** A roster applied: it holds whole each member's meta, those of the page's own members too. */
  onRosterApplied: () => void
}

/** A room's model as its server streams it to the page: the room the response carried, then its rosters in parts and
 *  its membership and meta events, over a connection that drops. */
class RoomMirror implements RoomModelOwner {
  readonly model: RoomModel
  private readonly rosters: RosterIntake
  private readonly memberLeft: (id: string, cause: LeaveCause) => void

  constructor({ snapshot, onMemberLeft, onRosterApplied }: RoomMirrorOptions) {
    this.memberLeft = onMemberLeft
    this.model = new RoomModel({
      roomId: snapshot.roomId,
      meta: snapshot.meta,
      seed: { members: snapshot.members },
      metaSeq: snapshot.metaSeq,
      closed: snapshot.closed,
      owner: this,
    })
    this.rosters = new RosterIntake({ model: this.model, onApplied: onRosterApplied })
  }

  apply(event: MirrorEvent): void {
    switch (event.__r) {
      case 'roster':
        return this.rosters.part(event)
      case 'rewound':
        return this.model.forgetMetaSeqs()
      case 'join':
        this.model.applyJoin(joinedMember(event))
        return
      case 'leave':
        return this.model.applyLeave(event.id, leaveCauseFromWire(event))
      case 'update':
        this.model.applyRoomUpdate(event)
        return
    }
  }

  /** The meta and identity a message's sender had at its send, a newer meta it was stamped with applied first (see
   *  `RoomModel.resolveSender`). */
  senderOf(stamp: { from: string } & Partial<SenderStamp>): Pick<Sender, 'meta' | 'identity'> {
    const resolved = this.model.resolveSender(stamp)
    if (!('entry' in resolved)) return resolved.sender
    return { meta: resolved.entry.meta.object, identity: resolved.entry.identity }
  }

  onMemberLeft(id: string, cause: LeaveCause): void {
    this.memberLeft(id, cause)
  }

  pendingMember(id: string): MemberSnapshot | undefined {
    return this.rosters.pending(id)
  }

  heldConnection(id: string): boolean | null {
    return heldConnection(id)
  }
}
