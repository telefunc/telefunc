export { onRoomToLoseFrom, onLoseLeave, onLoseMeta, onLoseClose }

import { Room, type LocalParticipant } from '@telefunc/room'
import { addBackendFault } from '../../failing-backend'

// The server's members, kept until their room's test ends.
const members = new Map<string, LocalParticipant[]>()

async function onRoomToLoseFrom(roomId: string) {
  await Room.create(roomId)
  members.set(roomId, [await Room.join(roomId, { meta: { name: 'Stays' } })])
  return await Room.get(roomId)
}

/** The store drops the next message on the room's lanes. */
function loseNext(roomId: string) {
  addBackendFault({ op: 'deliver', roomId })
}

async function onLoseLeave(roomId: string) {
  const [stays] = members.get(roomId)!
  loseNext(roomId)
  // A removal, unlike the member's own leave(), reaches this server's view only through the control lane.
  await Room.removeParticipant(roomId, { id: stays!.id })
  members.get(roomId)!.push(await Room.join(roomId, { meta: { name: 'Next' } }))
}

async function onLoseMeta(roomId: string) {
  loseNext(roomId)
  await Room.setAttributes(roomId, { lost: 1 })
  await Room.setAttributes(roomId, { next: 2 })
}

async function onLoseClose(roomId: string) {
  loseNext(roomId)
  await Room.close(roomId)
}
