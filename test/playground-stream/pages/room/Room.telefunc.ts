export {
  onCreateRoom,
  onGetRoom,
  onGetRoomTail,
  onGetOrCreateRoom,
  onCreateTypedRoom,
  onOpenGuardedRoom,
  onOpenAuditedRoom,
  onGetAudit,
  onJoinAsServer,
  onJoinRoomAsServerSelf,
  onGetRoomWithMember,
  onGetMember,
  onWatchRoom,
  onGetWatched,
  onAnnounce,
  onSystemSend,
  onUpdateRoom,
  onListRooms,
  onKick,
  onKickByIdentity,
  onCloseRoom,
}

import { Room, Abort } from 'telefunc'

async function onCreateRoom(roomId: string) {
  return await Room.create(roomId, { meta: { topic: 'e2e' } })
}

async function onGetRoom(roomId: string) {
  return await Room.get(roomId)
}

/** Tail starts at serialization, covering the gap before the client's first subscribe. */
async function onGetRoomTail(roomId: string) {
  return await Room.get(roomId, { tail: true })
}

async function onGetOrCreateRoom(roomId: string) {
  return await Room.getOrCreate(roomId, { meta: { topic: 'e2e' } })
}

type ChatMsg = { kind: 'chat'; text: string }
async function onCreateTypedRoom(roomId: string) {
  return await Room.create<{ topic: string }, { name: string }, ChatMsg>(roomId, { meta: { topic: 'typed' } })
}

async function onOpenGuardedRoom(roomId: string) {
  const room = await Room.get(roomId)
  Room.guard(room, {
    onBeforeJoin: (member) => {
      if (member.meta.name === 'Banned') throw Abort(`blocked join of ${member.meta.name}`)
    },
    onBeforePublish: (from, data) => {
      if (data === 'forbidden') throw Abort(`blocked publish from ${from.meta.name}`)
    },
    onBeforeSend: (from, _to, data) => {
      if (data === 'forbidden') throw Abort(`blocked send from ${from.meta.name}`)
    },
  })
  return room
}

const auditLog = new Map<string, Array<Record<string, unknown>>>()
function audit(roomId: string, entry: Record<string, unknown>) {
  const list = auditLog.get(roomId) ?? []
  list.push(entry)
  auditLog.set(roomId, list)
}
async function onOpenAuditedRoom(roomId: string) {
  const room = await Room.get(roomId)
  Room.guard(room, {
    onAfterJoin: (member, info) => {
      audit(roomId, { kind: 'join', name: member.meta.name, joinedAt: info.joinedAt })
    },
    onAfterPublish: (from, data, info) => {
      audit(roomId, { kind: 'publish', name: from.meta.name, data, seq: info.seq })
    },
    onAfterSend: (from, to, data, info) => {
      audit(roomId, { kind: 'send', name: from.meta.name, to: to.meta.name, seq: info.seq })
    },
  })
  return room
}
async function onGetAudit(roomId: string) {
  return auditLog.get(roomId) ?? []
}

async function onJoinAsServer(roomId: string, name: string) {
  return await Room.join(roomId, { meta: { name }, identity: `user:${name}` })
}

/** Co-returned `{ room, me }` suppresses `me`'s echo while preserving other-member delivery. */
async function onJoinRoomAsServerSelf(roomId: string, name: string) {
  const room = await Room.get(roomId)
  const me = await room.join({ meta: { name }, selfDelivery: false })
  return { room, me }
}

async function onGetRoomWithMember(roomId: string, memberId: string) {
  const room = await Room.get(roomId)
  const member = await room.getParticipant(memberId)
  return { room, member }
}

async function onGetMember(roomId: string, memberId: string) {
  return await (await Room.get(roomId)).getParticipant(memberId)
}

/** Keep the watched Room referenced so its independent server subscription survives. */
const watched = new Map<string, unknown[]>()
const watchedRooms = new Map<string, unknown>()
async function onWatchRoom(roomId: string) {
  const room = await Room.get(roomId)
  const list: unknown[] = []
  watched.set(roomId, list)
  watchedRooms.set(roomId, room)
  room.subscribe((data) => list.push(data))
}
async function onGetWatched(roomId: string) {
  return watched.get(roomId) ?? []
}

async function onAnnounce(roomId: string, data: unknown) {
  await Room.announce(roomId, data)
}

async function onSystemSend(roomId: string, participantId: string, data: unknown) {
  await Room.send(roomId, { id: participantId }, data)
}

async function onUpdateRoom(roomId: string, meta: Record<string, unknown>) {
  await Room.setMeta(roomId, meta)
}

async function onListRooms(prefix: string) {
  return (await Room.list({ prefix })).map((r) => r.id).sort()
}

async function onKick(roomId: string, participantId: string) {
  await Room.removeParticipant(roomId, { id: participantId, reason: 'be nice' })
}

async function onKickByIdentity(roomId: string, identity: string) {
  await Room.removeParticipant(roomId, { identity, reason: 'multi-tab' })
}

async function onCloseRoom(roomId: string) {
  await Room.close(roomId)
}
