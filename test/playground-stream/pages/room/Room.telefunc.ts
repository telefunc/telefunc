export { onCreateRoom, onGetRoom, onJoinAsServer, onAnnounce, onSystemSend, onKick, onCloseRoom }

import { Room } from 'telefunc'

/** Room IDs are generated client-side per scenario run — the in-memory KV outlives page
 *  loads, so a fixed ID would leak members and seq counters across runs. */
async function onCreateRoom(roomId: string, opts?: { size?: number; isolated?: boolean }) {
  return await Room.create(roomId, { meta: { topic: 'e2e' }, ...opts })
}

async function onGetRoom(roomId: string) {
  return await Room.get(roomId)
}

/** Server-side join returning a standalone `LocalParticipant` — exercises the participant
 *  stub (its own channel), unlike client-side `room.join()` which rides the room's stub. */
async function onJoinAsServer(roomId: string, name: string) {
  const theRoom = await Room.get(roomId)
  return await theRoom.join({ name })
}

async function onAnnounce(roomId: string, data: unknown) {
  await Room.announce(roomId, data)
}

async function onSystemSend(roomId: string, participantId: string, data: unknown) {
  await Room.send(roomId, participantId, data)
}

async function onKick(roomId: string, participantId: string) {
  await Room.removeParticipant(roomId, participantId)
}

async function onCloseRoom(roomId: string) {
  await Room.close(roomId)
}
