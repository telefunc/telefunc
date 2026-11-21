export { anyoneCanJoin }

import { Room, type ParticipantMeta, type RoomMeta } from '@telefunc/room'

/** `room`, which any page it is returned to may join: Room refuses a page's join() on a room returned unguarded. */
function anyoneCanJoin<M extends RoomMeta, P extends ParticipantMeta, Pub>(room: Room<M, P, Pub>): Room<M, P, Pub> {
  Room.guard(room, { anyoneCanJoin: true })
  return room
}
