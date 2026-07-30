export {
  onCreateRoom,
  onGetRoom,
  onGetRoomTail,
  onGetOrCreateRoom,
  onGetTypedRoom,
  onGetGuardedRoom,
  onGetAuditRoom,
  onGetAudit,
  onJoinAsServer,
  onJoinRoomAsServerSelf,
  onGetRoomWithMember,
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

/** Room IDs are generated client-side per scenario run — the in-memory KV outlives page
 *  loads, so a fixed ID would leak members and seq counters across runs. */
async function onCreateRoom(roomId: string) {
  return await Room.create(roomId, { meta: { topic: 'e2e' } })
}

async function onGetRoom(roomId: string) {
  return await Room.get(roomId)
}

/** Tail mode: relay starts the moment the room is serialized, so a message published between
 *  this call and the client's first `subscribe()` is held and flushed, never missed. */
async function onGetRoomTail(roomId: string) {
  return await Room.get(roomId, { tail: true })
}

/** Idempotent create — returns the existing room if present, creates it otherwise. */
async function onGetOrCreateRoom(roomId: string) {
  return await Room.getOrCreate(roomId, { meta: { topic: 'e2e' } })
}

type ChatMsg = { kind: 'chat'; text: string }
/** Typed room: `Room.create`'s 3rd generic types `publish()`/`subscribe()` end to end — the room
 *  arrives on the client already typed, so `data` needs no cast there. */
async function onGetTypedRoom(roomId: string) {
  return await Room.create<{ topic: string }, { name: string }, ChatMsg>(roomId, { meta: { topic: 'typed' } })
}

/** Guarded grant — every membership through this instance is policed server-side. */
async function onGetGuardedRoom(roomId: string) {
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

/** After-hooks record an audit trail with authoritative receipts (seq/joinedAt) — the documented
 *  place to persist for history. Keyed by the per-run room ID, read back via `onGetAudit`. */
const auditLog = new Map<string, Array<Record<string, unknown>>>()
function audit(roomId: string, entry: Record<string, unknown>) {
  const list = auditLog.get(roomId) ?? []
  list.push(entry)
  auditLog.set(roomId, list)
}
async function onGetAuditRoom(roomId: string) {
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

/** Server-side join returning a standalone `LocalParticipant` — exercises the participant
 *  stub (its own channel), unlike client-side `room.join()` which rides the room's stub.
 *  Identity is stamped here, where trust lives. */
async function onJoinAsServer(roomId: string, name: string) {
  return await Room.join(roomId, { meta: { name }, identity: `user:${name}` })
}

/** Server-side join with `selfDelivery: false`, returning `{ room, me }` together — the co-return
 *  case. On the client `me` is a standalone participant, `room` its live view; the server binds
 *  `me`'s echo-suppression onto that room's stub at serialization, so `me`'s own publishes never
 *  come back to `room` — no client-side drop, no wasted network. */
async function onJoinRoomAsServerSelf(roomId: string, name: string) {
  const room = await Room.get(roomId)
  const me = await room.join({ meta: { name }, selfDelivery: false })
  return { room, me }
}

/** A `RemoteParticipant` view returned alongside its room — ref-identity binds them:
 *  on the client, `room.getParticipant(member.id)` is the very same object as `member`. */
async function onGetRoomWithMember(roomId: string, memberId: string) {
  const room = await Room.get(roomId)
  const member = await room.getParticipant(memberId)
  return { room, member }
}

/** A server-side room subscriber — a genuinely *different* client than the browser, so it isn't
 *  subject to the browser's own `selfDelivery` self-suppression. Proves a `selfDelivery: false`
 *  publish still reaches everyone else. The room is kept referenced so its subscription survives. */
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

/** Replace a room's metadata — propagates to observers via `room.onUpdate()`. */
async function onUpdateRoom(roomId: string, meta: Record<string, unknown>) {
  await Room.setMeta(roomId, meta)
}

/** Enumerate rooms by ID prefix. */
async function onListRooms(prefix: string) {
  return (await Room.list({ prefix })).map((r) => r.id).sort()
}

async function onKick(roomId: string, participantId: string) {
  await Room.removeParticipant(roomId, { id: participantId, reason: 'be nice' })
}

/** Multi-tab kick: remove every membership carrying this app identity. */
async function onKickByIdentity(roomId: string, identity: string) {
  await Room.removeParticipant(roomId, { identity, reason: 'multi-tab' })
}

async function onCloseRoom(roomId: string) {
  await Room.close(roomId)
}
