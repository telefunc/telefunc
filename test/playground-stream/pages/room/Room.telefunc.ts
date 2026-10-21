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
  onWatchRoomTrack,
  onGetWatchedTrack,
  onJoinHiddenAndHand,
  onHiddenSetMeta,
  onHiddenLeave,
  onHiddenAfterPlayers,
  onGetHiddenAfterPlayers,
  onCloseRoomWithMember,
  onBackgroundErrors,
  onRoomWhileAway,
  onRoomLeaveWhileJoining,
  onRoomOverflow,
  onGetRoomOverflow,
  onRoomBurst,
  onGetWatchedCount,
  onRoomRelease,
  onFailBackend,
  onEndBackendSubscriptions,
  onListParticipantNames,
  onCreateRoomClosingAfter,
  onSeedRoom,
  onSeedChurn,
  onPing,
  onAnnounce,
  onSystemSend,
  onUpdateRoom,
  onListRooms,
  onKick,
  onKickByIdentity,
  onCloseRoom,
}

import { Room, Abort, onBug, type LeaveCause, type LocalParticipant } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { onServerAction } from '../../server-actions'
import { addBackendFault, endBackendSubscriptions, type BackendFault } from '../../failing-backend'

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

const watchedTracks = new Map<string, number[]>()

/** A server-side view's subscribers, text and one binary track, kept like `onWatchRoom()`'s. */
async function onWatchRoomTrack(roomId: string, track: string) {
  const room = await Room.get(roomId)
  const list: unknown[] = []
  const frames: number[] = []
  watched.set(roomId, list)
  watchedTracks.set(roomId, frames)
  watchedRooms.set(roomId, room)
  room.subscribe((data) => list.push(data))
  room.subscribeBinary((data) => frames.push(data[0]!), { track })
}

async function onGetWatchedTrack(roomId: string) {
  return { text: watched.get(roomId) ?? [], frames: watchedTracks.get(roomId) ?? [] }
}

const hiddenBots = new Map<string, LocalParticipant>()

/** A hidden member, handed to this call's page only. */
async function onJoinHiddenAndHand(roomId: string) {
  const room = await Room.get(roomId)
  const bot = await room.join({ hidden: true, meta: { name: 'Bot' } })
  hiddenBots.set(roomId, bot)
  return { room, member: await room.getParticipant(bot.id) }
}

async function onHiddenSetMeta(roomId: string, name: string) {
  await hiddenBots.get(roomId)!.setMeta({ name })
}

async function onHiddenLeave(roomId: string) {
  await hiddenBots.get(roomId)!.leave()
}

const hiddenAfterPlayers = new Map<string, { cause: LeaveCause | null; closed: boolean; refs: unknown[] }>()

/** A hidden member stays after the room's only player left; nothing else holds the room. */
async function onHiddenAfterPlayers(roomId: string) {
  const room = await Room.create(roomId, { departureTimeout: 500 })
  const bot = await room.join({ hidden: true, meta: { name: 'Bot' } })
  const state = { cause: null as LeaveCause | null, closed: false, refs: [room, bot] as unknown[] }
  hiddenAfterPlayers.set(roomId, state)
  bot.onLeave((cause) => (state.cause = cause))
  room.onClose(() => (state.closed = true))
  const player = await room.join({ meta: { name: 'P' } })
  await player.leave()
}

async function onGetHiddenAfterPlayers(roomId: string) {
  const state = hiddenAfterPlayers.get(roomId)!
  return { cause: state.cause, closed: state.closed }
}

/** Returns a room closed under its member, as a page gets one that closed while the call ran. */
async function onCloseRoomWithMember(roomId: string) {
  const room = await Room.create(roomId)
  const member = await room.getParticipant((await room.join({ meta: { name: 'Gone' } })).id)
  const { promise: closed, resolve } = Promise.withResolvers<void>()
  room.onClose(resolve)
  await Room.close(roomId)
  await closed
  return { room, member }
}

let bugs: string[] | null = null
onBug((err) => bugs?.push((err as Error).message))

/** Listeners that fail in the background, and un-awaited writes of members a closed room let go. */
async function onBackgroundErrors(roomId: string) {
  bugs = []
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => void unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  const room = await Room.create(roomId)
  room.onJoin(async () => {
    throw new Error('room-listener-bug: async onJoin')
  })
  const left = await room.join()
  await left.leave()
  const member = await room.join()
  const { promise: escaped, resolve } = Promise.withResolvers<void>()
  member.listen(async (data) => {
    if (data !== 'escape') throw new Error('room-listener-bug: async listen')
    resolve()
    const error = await left.publish('after-leave').then(
      () => new Error('published after leave'),
      (err: Error) => err,
    )
    error.message = `room-listener-bug: ${error.message}`
    throw error
  })
  const sender = await room.join()
  await sender.send(member.id, 'reject')
  await sender.send(member.id, 'escape')
  await escaped

  const authority = await room.join({ hidden: true })
  await Room.close(roomId)
  authority.publish('after-close')
  authority.publishBinary(new Uint8Array([1]))
  authority.send(member.id, 'after-close')
  const awaited = await authority.publish('after-close').then(
    () => null,
    (err: Error) => err.message,
  )
  // Node emits 'unhandledRejection' once the microtask queue drains, before the next macrotask.
  await new Promise(setImmediate)
  process.off('unhandledRejection', onUnhandled)
  const reported = bugs.sort()
  bugs = null
  return { reported, unhandled: unhandled.length, awaited }
}

const awayMembers = new Map<string, LocalParticipant[]>()

/** What the room does while the page is away, run by the test with `runServerAction()`. */
async function onRoomWhileAway(roomId: string, memberId: string) {
  const room = await Room.get(roomId)
  const srv = await room.join({ meta: { name: 'Srv' } })
  const gone = await room.join({ meta: { name: 'Gone' } })
  awayMembers.set(roomId, [srv, gone])
  onServerAction(`roomAway_${roomId}`, async () => {
    for (const text of ['m1', 'm2', 'm3']) await srv.publish(text)
    await gone.leave()
    await Room.setMeta(roomId, { topic: 'while-away' })
    await Room.removeParticipant(roomId, { id: memberId, reason: 'while-away' })
    await Room.close(roomId)
    cleanupState[`roomAway_${roomId}`] = 'done'
  })
}

/** A member the test removes, with `runServerAction()`, while the page is away. */
async function onRoomLeaveWhileJoining(roomId: string) {
  const other = await (await Room.get(roomId)).join({ meta: { name: 'Other' } })
  awayMembers.set(roomId, [other])
  onServerAction(`roomLeaveWhileJoining_${roomId}`, async () => {
    await Room.removeParticipant(roomId, { id: other.id, reason: 'while-joining' })
    cleanupState[`roomLeaveWhileJoining_${roomId}`] = 'done'
  })
}

const overflows = new Map<string, { leaves: string[]; refs: unknown[] }>()

/** A speaker says 'before', then, run by the test while the page is offline, 1 MiB: more than the page's 512 KiB
 *  bufferLimit for text holds. */
async function onRoomOverflow(roomId: string) {
  const room = await Room.get(roomId)
  const speaker = await room.join({ meta: { name: 'Speaker' } })
  const leaves: string[] = []
  room.onLeave((member, cause) => leaves.push(`${member.meta.name}:${cause.type}`))
  overflows.set(roomId, { leaves, refs: [room, speaker] })
  onServerAction(`roomOverflow_${roomId}`, async () => {
    for (let n = 0; n < 8; n++) await speaker.publish(String(n).padEnd(128 * 1024))
    cleanupState[`roomOverflow_${roomId}`] = 'done'
  })
  await speaker.publish('before')
}

async function onGetRoomOverflow(roomId: string) {
  return overflows.get(roomId)!.leaves
}

/** A server member publishes `count` messages of `size` bytes, each awaited. */
async function onRoomBurst(roomId: string, count: number, size: number) {
  const room = await Room.get(roomId)
  const speaker = await room.join({ meta: { name: 'Speaker' } })
  for (let n = 0; n < count; n++) await speaker.publish(String(n).padEnd(size))
  await speaker.leave()
}

async function onGetWatchedCount(roomId: string) {
  return watched.get(roomId)?.length ?? 0
}

const releaseKeepers = new Map<string, LocalParticipant>()

/** Server action roomRelease_<roomId> sets cleanupState roomRelease_<roomId> to what the server still holds. */
async function onRoomRelease(roomId: string) {
  const refs = await releaseRefs(roomId)
  onServerAction(`roomRelease_${roomId}`, () => {
    const held = Object.entries(refs).map(([name, ref]) => `${name}:${ref.deref() ? 'held' : 'released'}`)
    cleanupState[`roomRelease_${roomId}`] = held.join()
  })
}

// Apart from onRoomRelease: a closure made there would share the scope that holds them.
async function releaseRefs(roomId: string) {
  const room = await Room.create(roomId)
  // Keeps the room open, so what is let go is let go of an open room.
  releaseKeepers.set(roomId, await room.join({ meta: { name: 'Keeper' } }))

  const left = await room.join({ meta: { name: 'Left' } })
  left.listen(() => {})
  left.onLeave(() => {})
  left.onDemand(() => {})
  const view = await Room.get(roomId)
  const departed = (await view.getParticipant(left.id))!
  departed.onUpdate(() => {})
  await left.leave()
  // Listeners added to a member that left, then the view's own, all gone.
  departed.subscribe(() => {})
  departed.subscribeBinary(() => {})
  departed.onUpdate(() => {})
  const offs = [view.subscribe(() => {}), view.subscribeBinary(() => {}), view.onJoin(() => {}), view.onLeave(() => {})]
  for (const off of offs) off()

  const closedId = `${roomId}:closed`
  const closed = await Room.create(closedId)
  closed.subscribe(() => {})
  closed.onJoin(() => {})
  const member = await closed.join()
  member.listen(() => {})
  await Room.close(closedId)

  return { view: new WeakRef(view), left: new WeakRef(left), closed: new WeakRef(closed), member: new WeakRef(member) }
}

/** The store fails what `fault` names (failing-backend.ts). */
async function onFailBackend(fault: BackendFault) {
  addBackendFault(fault)
}

async function onEndBackendSubscriptions(roomId: string) {
  return endBackendSubscriptions({ roomId })
}

async function onListParticipantNames(roomId: string) {
  const room = await Room.get(roomId)
  return (await room.getParticipants()).map((member) => member.meta.name).sort()
}

/** A room that closes `emptyTimeout` ms after nothing holds it, if nobody joined it. */
async function onCreateRoomClosingAfter(roomId: string, emptyTimeout: number) {
  await Room.create(roomId, { emptyTimeout })
}

const seeded = new Map<string, LocalParticipant[]>()

/** `count` server members join a new room, a thousand at a time. */
async function onSeedRoom(roomId: string, count: number) {
  const room = await Room.create(roomId)
  const members: LocalParticipant[] = []
  seeded.set(roomId, members)
  while (members.length < count) {
    const batch = Math.min(1_000, count - members.length)
    const base = members.length
    members.push(
      ...(await Promise.all(Array.from({ length: batch }, (_, i) => room.join({ meta: { name: `m${base + i}` } })))),
    )
  }
}

/** One member joins and the first seeded one leaves. */
async function onSeedChurn(roomId: string) {
  const members = seeded.get(roomId)!
  members.push(await (await Room.get(roomId)).join({ meta: { name: 'late' } }))
  await members[0]!.leave()
}

async function onPing() {
  return 'pong'
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
