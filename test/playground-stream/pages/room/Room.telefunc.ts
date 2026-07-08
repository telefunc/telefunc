export { onCreateRoom, onGetRoom, onJoinAsServer, onKick, onCloseRoom }

import { room } from 'telefunc'

/** Room IDs are generated client-side per scenario run — the in-memory KV outlives page
 *  loads, so a fixed ID would leak members and seq counters across runs. */
async function onCreateRoom(roomId: string, opts?: { size?: number; isolated?: boolean }) {
  return await room.create(roomId, { meta: { topic: 'e2e' }, ...opts })
}

async function onGetRoom(roomId: string) {
  return await room(roomId)
}

/** Server-side join returning a standalone `LocalParticipant` — exercises the participant
 *  stub (its own channel), unlike client-side `room.join()` which rides the room's stub. */
async function onJoinAsServer(roomId: string, name: string) {
  const theRoom = await room(roomId)
  return await theRoom.join({ name })
}

async function onKick(roomId: string, participantId: string) {
  await room.removeParticipant(roomId, participantId)
}

async function onCloseRoom(roomId: string) {
  await room.close(roomId)
}
