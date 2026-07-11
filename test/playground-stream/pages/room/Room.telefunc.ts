export {
  onCreateRoom,
  onGetRoom,
  onGetGuardedRoom,
  onJoinAsServer,
  onGetRoomWithMember,
  onAnnounce,
  onSystemSend,
  onKick,
  onCloseRoom,
}

import { Room } from 'telefunc'

/** Room IDs are generated client-side per scenario run — the in-memory KV outlives page
 *  loads, so a fixed ID would leak members and seq counters across runs. */
async function onCreateRoom(roomId: string, opts?: { size?: number; isolated?: boolean }) {
  return await Room.create(roomId, { meta: { topic: 'e2e' }, ...opts })
}

async function onGetRoom(roomId: string) {
  return await Room.get(roomId)
}

/** Guarded grant — every membership through this instance is policed server-side. */
async function onGetGuardedRoom(roomId: string) {
  const room = await Room.get(roomId)
  Room.guard(room, {
    onBeforeJoin: (member) => {
      if (member.meta.name === 'Banned') throw new Error(`blocked join of ${member.meta.name}`)
    },
    onBeforePublish: (from, data) => {
      if (data === 'forbidden') throw new Error(`blocked publish from ${from.meta.name}`)
    },
    onBeforeSend: (from, _to, data) => {
      if (data === 'forbidden') throw new Error(`blocked send from ${from.meta.name}`)
    },
  })
  return room
}

/** Server-side join returning a standalone `LocalParticipant` — exercises the participant
 *  stub (its own channel), unlike client-side `room.join()` which rides the room's stub.
 *  Identity is stamped here, where trust lives. */
async function onJoinAsServer(roomId: string, name: string) {
  return await Room.join(roomId, { name }, { identity: `user:${name}` })
}

/** A `RemoteParticipant` view returned alongside its room — ref-identity binds them:
 *  on the client, `room.getParticipant(member.id)` is the very same object as `member`. */
async function onGetRoomWithMember(roomId: string, memberId: string) {
  const room = await Room.get(roomId)
  const member = await room.getParticipant(memberId)
  return { room, member }
}

async function onAnnounce(roomId: string, data: unknown) {
  await Room.announce(roomId, data)
}

async function onSystemSend(roomId: string, participantId: string, data: unknown) {
  await Room.send(roomId, participantId, data)
}

async function onKick(roomId: string, participantId: string) {
  await Room.removeParticipant(roomId, participantId, { reason: 'be nice' })
}

async function onCloseRoom(roomId: string) {
  await Room.close(roomId)
}
