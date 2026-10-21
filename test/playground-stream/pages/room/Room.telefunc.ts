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
  onWatchRoom,
  onGetWatched,
  onAnnounce,
  onSystemSend,
  onUpdateRoom,
  onListRooms,
  onKick,
  onKickByIdentity,
  onCloseRoom,
  onWatchRoomBinary,
  onGetWatchedBinary,
  onPublishBufferAsServer,
  onSendReusingObjects,
  onPublishRightAfterSubscribe,
  onOpenSlowGuardRoom,
  onConcurrentMetaWrites,
  onReplaceMetaInLoop,
  onStopReplacingMeta,
  onOpenRecordingGuardRoom,
  onGetGuardCalls,
  onOpenSlowJoinRoom,
  onGetSlowJoins,
  onGetSlowJoinRoom,
  onCloseWhenEmpty,
  onCreateRoomWith,
  onCreateRoomHeld,
  onJoinAndLeaveAsServer,
  onIsOpen,
  onListAsListed,
  onListWhileClosing,
  onRecreate,
  onJoinAnswering,
}

import { Buffer } from 'node:buffer'
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

/** Two server listeners on one room view: the first writes into the bytes it gets, the second records what it got. */
const watchedBinary = new Map<string, number[][]>()

async function onWatchRoomBinary(roomId: string) {
  const room = await Room.get(roomId)
  const seen: number[][] = []
  watchedBinary.set(roomId, seen)
  watchedRooms.set(roomId, room)
  room.subscribeBinary((data) => data.fill(0))
  room.subscribeBinary((data) => seen.push([...data]))
  return room
}

async function onGetWatchedBinary(roomId: string) {
  return watchedBinary.get(roomId) ?? []
}

async function onPublishBufferAsServer(roomId: string, bytes: number[]) {
  const me = await Room.join(roomId, { meta: { name: 'Server' } })
  await me.publishBinary(Buffer.from(bytes), { retain: true })
}

/** Each write's caller changes its object or buffer right after the call, before the write settles. */
async function onSendReusingObjects(roomId: string, memberId: string) {
  const me = await Room.join(roomId, { meta: { name: 'Reuser', tags: ['a'] } })
  const message = { n: 1 }
  const published = me.publish(message)
  message.n = 2
  const sent = me.send(memberId, message)
  message.n = 3
  const announced = Room.announce(roomId, message)
  message.n = 4
  const systemSent = Room.send(roomId, { id: memberId }, message)
  message.n = 5
  const meta = { name: 'Reuser', tags: ['b'] }
  const metaSet = me.setMeta(meta)
  meta.tags.push('later')
  const roomMeta = { topic: 'at-call', tags: ['x'] }
  const roomMetaSet = Room.setMeta(roomId, roomMeta)
  roomMeta.tags.push('later')
  const bytes = new Uint8Array([1, 2, 3])
  const binary = me.publishBinary(bytes)
  bytes.fill(0)
  await Promise.all([published, sent, announced, systemSent, metaSet, roomMetaSet, binary])
  return me.id
}

/** While another member's publishes commit, a view subscribes and a member publishes at once; a member DMs one who just
 *  joined. Each records only what the member sent. */
async function onPublishRightAfterSubscribe(roomId: string) {
  const busy = await Room.join(roomId, { meta: { name: 'Busy' } })
  const me = await Room.join(roomId, { meta: { name: 'Me' } })
  const room = await Room.get(roomId)
  const seen: unknown[] = []
  watched.set(roomId, seen)
  watchedRooms.set(roomId, room)
  for (let i = 0; i < 100; i++) void busy.publish(i)
  room.subscribe((data, _info, from) => {
    if (from.id === me.id) seen.push(data)
  })
  room.subscribeBinary((data, _info, from) => {
    if (from.id === me.id) seen.push([...data])
  })
  await Promise.all([me.publish('t1'), me.publishBinary(new Uint8Array([1])), me.publish('t2')])
  const peer = await Room.join(roomId, { meta: { name: 'Peer' } })
  peer.listen((data) => seen.push(data))
  await me.send(peer.id, 'dm')
}

/** The guard takes longer for an earlier publish: 50 ms less the number the message or frame carries. */
async function onOpenSlowGuardRoom(roomId: string) {
  const room = await Room.get(roomId)
  Room.guard(room, {
    onBeforePublish: async (_from, data) => {
      const n = data instanceof Uint8Array ? data[0]! : (data as number)
      await new Promise((resolve) => setTimeout(resolve, 50 - n))
    },
  })
  return room
}

/** 200 concurrent Room.setAttributes() calls, then 20 concurrent Room.setMeta() calls, and a member's 100 concurrent
 *  setAttributes() and setMeta() calls; returns the room meta and the member's meta as a fresh view reads them. */
async function onConcurrentMetaWrites(roomId: string) {
  const me = await Room.join(roomId, { meta: { name: 'Writer', round: -1 } })
  const keys = Array.from({ length: 200 }, (_, i) => i)
  await Promise.all(keys.map((i) => Room.setAttributes(roomId, { [`k${i}`]: i })))
  const afterAttributes = Object.keys((await Room.get(roomId)).meta)
  await Promise.all(keys.slice(0, 20).map((i) => Room.setMeta(roomId, { winner: i, [`w${i}`]: true })))
  await Promise.all(
    keys
      .slice(0, 100)
      .map((i) => (i % 10 === 0 ? me.setMeta({ name: 'Writer', round: i }) : me.setAttributes({ [`a${i}`]: i }))),
  )
  const view = await Room.get(roomId)
  const member = await view.getParticipant(me.id)
  return {
    afterAttributes,
    roomMeta: Object.entries(view.meta),
    memberId: me.id,
    memberMeta: Object.entries(member!.meta),
  }
}

/** A meta of 1,500 keys, `a0`… or `b0`…: more than one page of cells. */
const bigMeta = (prefix: string) => Object.fromEntries(Array.from({ length: 1500 }, (_, i) => [`${prefix}${i}`, i]))

const replacing = new Map<string, ReturnType<typeof setInterval>>()

/** Sets a member's and the room's meta to the `a` meta, then replaces both with the `b` and `a` metas in turn every
 *  10 ms, each replace overlapping the ones before it, until `onStopReplacingMeta()`. */
async function onReplaceMetaInLoop(roomId: string) {
  const me = await Room.join(roomId, { meta: { name: 'Big', ...bigMeta('a') } })
  await Room.setMeta(roomId, bigMeta('a'))
  let round = 0
  const timer = setInterval(() => {
    const prefix = round++ % 2 === 0 ? 'b' : 'a'
    void me.setMeta({ name: 'Big', ...bigMeta(prefix) })
    void Room.setMeta(roomId, bigMeta(prefix))
  }, 10)
  replacing.set(roomId, timer)
  return me.id
}

async function onStopReplacingMeta(roomId: string) {
  clearInterval(replacing.get(roomId))
  replacing.delete(roomId)
}

const guardCalls = new Map<string, Array<{ hook: string; name: unknown; frozen: boolean }>>()

/** Guards that record what they were handed: a `slow` publish waits 100 ms, a binary one is refused, and
 *  `onAfterPublish` throws for an `after` publish. */
async function onOpenRecordingGuardRoom(roomId: string) {
  const room = await Room.get(roomId)
  const calls: Array<{ hook: string; name: unknown; frozen: boolean }> = []
  guardCalls.set(roomId, calls)
  Room.guard(room, {
    onBeforeJoin: (member) => {
      calls.push({ hook: 'join', name: member.meta.name, frozen: Object.isFrozen(member.meta) })
    },
    onBeforePublish: async (from, data) => {
      calls.push({ hook: 'publish', name: from.meta.name, frozen: Object.isFrozen(from) && Object.isFrozen(from.meta) })
      if (data === 'slow') await new Promise((resolve) => setTimeout(resolve, 100))
      if (data instanceof Uint8Array) throw Abort('no frames')
    },
    onBeforeSend: (from, to) => {
      calls.push({ hook: 'send', name: from.meta.name, frozen: Object.isFrozen(from) && Object.isFrozen(to) })
    },
    onAfterPublish: (_from, data) => {
      if (data === 'after') throw new Error('server-listener-bug')
    },
  })
  return room
}

async function onGetGuardCalls(roomId: string) {
  return guardCalls.get(roomId) ?? []
}

const slowJoins = new Map<string, { entered: string[]; done: string[] }>()

/** A join whose meta has `slow` waits 1 s in its guard; the names that entered the wait and that ended it are kept. */
async function onOpenSlowJoinRoom(roomId: string) {
  const room = await Room.get(roomId)
  const joins = { entered: [] as string[], done: [] as string[] }
  slowJoins.set(roomId, joins)
  watchedRooms.set(roomId, room)
  Room.guard(room, {
    onBeforeJoin: async (member) => {
      if (!member.meta.slow) return
      joins.entered.push(member.meta.name as string)
      await new Promise((resolve) => setTimeout(resolve, 1000))
      joins.done.push(member.meta.name as string)
    },
  })
  return room
}

/** The view `onOpenSlowJoinRoom()` guarded: a guard applies to the joins made through it. */
async function onGetSlowJoinRoom(roomId: string) {
  return watchedRooms.get(roomId) as Awaited<ReturnType<typeof onOpenSlowJoinRoom>>
}

async function onGetSlowJoins(roomId: string) {
  return slowJoins.get(roomId)!
}

/** A server view closes the room once it is empty, the cleanup the Room docs show. */
async function onCloseWhenEmpty(roomId: string) {
  const room = await Room.get(roomId)
  watchedRooms.set(roomId, room)
  room.onEmpty(() => {
    void Room.close(roomId)
  })
}

async function onCreateRoomWith(roomId: string, options: { emptyTimeout?: number; departureTimeout?: number }) {
  await Room.create(roomId, options)
}

/** The page this returns the room to holds it open. */
async function onCreateRoomHeld(roomId: string) {
  return await Room.create(roomId, { emptyTimeout: 1000 })
}

async function onJoinAndLeaveAsServer(roomId: string) {
  const me = await Room.join(roomId, { meta: { name: 'Passing' } })
  await me.leave()
}

async function onIsOpen(roomId: string) {
  return (await Room.list({ prefix: roomId })).some((room) => room.id === roomId)
}

async function onListAsListed(prefix: string) {
  return (await Room.list({ prefix })).map((room) => room.id)
}

/** Lists the prefix while 20 rooms under it close. */
async function onListWhileClosing(prefix: string) {
  const ids = Array.from({ length: 20 }, (_, i) => `${prefix}:closing${i}`)
  await Promise.all(ids.map((id) => Room.create(id)))
  const [listed] = await Promise.all([
    Room.list({ prefix }).then(
      (rooms) => rooms.map((room) => room.id),
      (err: Error) => err.name,
    ),
    ...ids.map((id) => Room.close(id)),
  ])
  return { listed, after: await onListAsListed(prefix) }
}

/** Creates the room with a member and closes it, then creates it again, empty, closing 1 s after. */
async function onRecreate(roomId: string) {
  await Room.create(roomId, { meta: { incarnation: 1 } })
  await Room.join(roomId, { meta: { name: 'Earlier' } })
  await Room.close(roomId)
  await Room.create(roomId, { meta: { incarnation: 2 }, emptyTimeout: 1000 })
  const room = await Room.get(roomId)
  return { meta: room.meta, count: room.count }
}

/** A server member that answers each DM with `{ echo }`, and refuses `refuse` with an Abort. */
async function onJoinAnswering(roomId: string) {
  const me = await Room.join(roomId, { meta: { name: 'Answering' } })
  me.listen((data) => {
    if (data === 'refuse') throw Abort('refused')
    return { echo: data }
  })
  return me.id
}
