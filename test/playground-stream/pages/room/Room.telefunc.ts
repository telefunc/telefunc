export {
  onCreateRoom,
  onGetRoom,
  onGetRoomTail,
  onGetOrCreateRoom,
  onGetOrCreateRoomTail,
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
  onRoomMetaWhileAway,
  onRoomLeaveWhileJoining,
  onRoomOverflow,
  onParticipantOverflow,
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
  onWatchRoomBinary,
  onGetWatchedBinary,
  onPublishBufferAsServer,
  onSendReusingObjects,
  onPublishRightAfterSubscribe,
  onOpenSlowGuardRoom,
  onConcurrentMetaWrites,
  onReplaceMetaInLoop,
  onStopReplacingMeta,
  onReadMetaAcrossWrite,
  onReadMetaAcrossDrop,
  onReadSplitMetaAcrossMerge,
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
  onSaveReply,
  onSeedAndChurn,
  onStopChurn,
  onPublishWhileEstablishing,
}

import { Buffer } from 'node:buffer'
import { Room, Abort, onBug, type LeaveCause, type LocalParticipant } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { onServerAction } from '../../server-actions'
import {
  addBackendFault,
  endBackendSubscriptions,
  holdCellPages,
  holdSubscriptions,
  type BackendFault,
} from '../../failing-backend'

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

async function onGetOrCreateRoomTail(roomId: string) {
  return await Room.getOrCreate(roomId, { meta: { topic: 'e2e' }, tail: true })
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

/** Run by the test with `runServerAction()` while the page is away, once per absence: the room's meta changes, then a
 *  member's meta changes past the page's 512 KiB bufferLimit for text, which drops the room's change from its buffer. */
async function onRoomMetaWhileAway(roomId: string) {
  const srv = await (await Room.get(roomId)).join({ meta: { name: 'Srv' } })
  awayMembers.set(roomId, [srv])
  let absence = 0
  onServerAction(`roomMetaAway_${roomId}`, async () => {
    absence++
    await Room.setMeta(roomId, { topic: `away-${absence}` })
    for (let n = 0; n < 6; n++) await srv.setMeta({ name: 'Srv', filler: `${absence}`.padEnd(128 * 1024) })
    cleanupState[`roomMetaAway_${roomId}`] = `done-${absence}`
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

/** A sender DMs 'before' to the page's server-joined participant `memberId`, then, run by the test while the page is
 *  offline, 1 MiB: more than the page's 512 KiB bufferLimit for text holds. */
async function onParticipantOverflow(roomId: string, memberId: string) {
  const sender = await (await Room.get(roomId)).join({ meta: { name: 'Sender' } })
  awayMembers.set(roomId, [sender])
  onServerAction(`participantOverflow_${roomId}`, async () => {
    // Sent at once: the DMs after the one its page's buffer drops reach the participant before it leaves.
    await Promise.all(
      Array.from({ length: 8 }, (_, n) => sender.send(memberId, String(n).padEnd(128 * 1024)).catch(() => {})),
    )
    cleanupState[`participantOverflow_${roomId}`] = 'done'
  })
  await sender.send(memberId, 'before')
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

/** A member's `a` meta, set as its write `gen` (a replace from 9 or 99 writes keys one digit longer), read by the room's
 *  roster while `write` commits between the read's first and second page: 'before', 'after', or the keys the read got. */
async function onReadMetaAcrossWrite(roomId: string, write: 'replace' | 'replace with no keys' | 'merge', gen: number) {
  const member = await Room.join(roomId, { meta: {} })
  for (let seq = 1; seq < gen; seq++) await member.setAttributes({ seq })
  await member.setMeta(bigMeta('a'))
  const held = holdCellPages(roomId, 'pm:', 1)
  const reading = Room.getParticipants(roomId)
  await held.reached
  const after: Record<string, unknown> = write === 'replace' ? bigMeta('b') : write === 'merge' ? bigMeta('a') : {}
  if (write === 'merge') {
    delete after.a0
    after.zz = 'new'
    await member.setAttributes({ a0: undefined, zz: 'new' })
  } else {
    await member.setMeta(after)
  }
  held.release()
  const [read] = await reading
  return metaName(read!.meta, { before: bigMeta('a'), after })
}

/** A member's `a` meta read by the room's roster once a replace with no keys deleted the first of the two pages it
 *  drops and before it deleted the second: 'before', 'after', or the keys the read got. */
async function onReadMetaAcrossDrop(roomId: string) {
  const member = await Room.join(roomId, { meta: bigMeta('a') })
  const reading = holdCellPages(roomId, 'pm:', 0)
  const participants = Room.getParticipants(roomId)
  await reading.reached
  const dropping = holdCellPages(roomId, `pm:${member.id}:0:`, 2)
  const replacing = member.setMeta({})
  await dropping.reached
  reading.release()
  const [read] = await participants
  dropping.release()
  await replacing
  return metaName(read!.meta, { before: bigMeta('a'), after: {} })
}

/** Member A's meta, whose last key it wrote after member B joined with the `b` meta, read by the room's roster while a
 *  merge of A's commits between the read's first and second page: 'before', 'after', or the keys the read got. */
async function onReadSplitMetaAcrossMerge(roomId: string) {
  const member = await Room.join<Record<string, string>>(roomId, { meta: { k0: 'a', k1: 'a' } })
  await Room.join(roomId, { meta: bigMeta('b') })
  await member.setAttributes({ k2: 'a' })
  const held = holdCellPages(roomId, 'pm:', 1)
  const reading = Room.getParticipants(roomId)
  await held.reached
  await member.setAttributes({ k0: undefined, k2: undefined, zz: 'new' })
  held.release()
  const read = (await reading).find((participant) => participant.id === member.id)!
  return metaName(read.meta, { before: { k0: 'a', k1: 'a', k2: 'a' }, after: { k1: 'a', zz: 'new' } })
}

/** The name of the meta in `named` that `meta` is, keys in order, or the keys it holds. */
function metaName(meta: Record<string, unknown>, named: Record<string, Record<string, unknown>>): string {
  const entries = JSON.stringify(Object.entries(meta))
  for (const name in named) if (JSON.stringify(Object.entries(named[name]!)) === entries) return name
  const keys = Object.keys(meta)
  return `${keys.length} keys from ${keys[0]} to ${keys.at(-1)}`
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

/** A store's save of a reply, finished by the test with `runServerAction()`. */
async function onSaveReply(roomId: string) {
  const { promise, resolve } = Promise.withResolvers<void>()
  onServerAction(`saveReply_${roomId}`, resolve)
  await promise
}

const churning = new Map<string, () => Promise<number>>()

/** Joins 2,000 members and a hidden one, then every 5 ms changes a member's meta and has a member join or leave, until
 *  `onStopChurn()`, which answers with the room's visible member count. */
async function onSeedAndChurn(roomId: string) {
  const members = await Promise.all(
    Array.from({ length: 2000 }, (_, i) => Room.join(roomId, { meta: { name: `m${i}`, n: 0 } })),
  )
  await Room.join(roomId, { meta: { name: 'Hidden' }, hidden: true })
  let tick = 0
  const passing: Awaited<ReturnType<typeof Room.join>>[] = []
  const timer = setInterval(() => {
    tick++
    void members[tick % members.length]!.setAttributes({ n: tick })
    if (tick % 2 === 0) void Room.join(roomId, { meta: { name: `p${tick}` } }).then((member) => passing.push(member))
    else void passing.shift()?.leave()
  }, 5)
  churning.set(roomId, async () => {
    clearInterval(timer)
    await Promise.all(passing.map((member) => member.leave()))
    return (await Room.get(roomId)).count
  })
}

async function onStopChurn(roomId: string) {
  return await churning.get(roomId)!()
}

/** While the store holds a view's new subscription short of ready, a member publishes three 1,500-byte messages at once,
 *  past the 4,096-byte bufferLimit of the lane that runs it. */
async function onPublishWhileEstablishing(roomId: string) {
  const me = await Room.join(roomId, { meta: { name: 'Me' } })
  const room = await Room.get(roomId)
  const seen: string[] = []
  watched.set(roomId, seen)
  watchedRooms.set(roomId, room)
  const release = holdSubscriptions(roomId)
  room.subscribe((data, _info, from) => {
    if (from.id === me.id) seen.push((data as string)[0]!)
  })
  const publishes = ['1', '2', '3'].map((n) =>
    me.publish(n.padEnd(1500, '.')).then(
      () => 'sent',
      (err: Error) => err.name,
    ),
  )
  // The one past bufferLimit settles while the others are held.
  await publishes[2]
  release()
  return await Promise.all(publishes)
}
