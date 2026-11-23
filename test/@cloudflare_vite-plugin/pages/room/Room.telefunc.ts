export {
  onRoomRoundTrip,
  onOrderChannels,
  onJoinRoom,
  onSubscribeThenPublish,
  onListen,
  onPublishFrom,
  onReceived,
  onListInOrder,
  onCreateRoom,
  onListRooms,
  onRetainLarge,
  onCloseWhenLeft,
  onHoldView,
  onCloseHeld,
  onSubscribeHeld,
  onMetaKeyOrder,
  onThrowingListener,
  onJoinClosing,
  onJoinRefused,
  onReplyOutcome,
  onRemovalOutcome,
  onJoinOutcome,
  onMetaUpdateOutcome,
  onSendOutcome,
  onBesideRead,
}

import { Broadcast, BroadcastChannel, onBug } from 'telefunc'
import { Room, type LeaveCause } from '@telefunc/room'
import { getRoomPlane, type CellsBeside, type CellsRead } from '@telefunc/room/backend'
import { inboxRegistrations } from './inboxRegistrations'

async function onRoomRoundTrip() {
  const id = `telefunction-${crypto.randomUUID()}`
  const room = await Room.getOrCreate(id)
  const received: unknown[] = []
  room.subscribe((data) => received.push(data))
  const me = await room.join()
  await me.publish('hello')
  const joined = room.count
  await Room.close(id)
  return { joined, received }
}

type Numbered = { from: string; n: number }

/** A Broadcast key and a room each page publishes on, from its own session. */
async function onOrderChannels(id: string) {
  const room = await Room.getOrCreate<Record<string, never>, { name: string }, Numbered>(`order:${id}`)
  Room.guard(room, { anyoneCanJoin: true })
  return { channel: new BroadcastChannel<Numbered>({ key: `order:${id}` }), room }
}

async function onJoinRoom(id: string) {
  const room = await Room.getOrCreate(`join:${id}`)
  await room.join()
  return 'joined'
}

/** Subscribes, then publishes at once. */
async function onSubscribeThenPublish() {
  const key = `at-once:${crypto.randomUUID()}`
  const received: string[] = []
  Broadcast.subscribe<string>(key, (message) => received.push(message))
  await Broadcast.publish(key, 'at-once')
  for (let i = 0; i < 50 && received.length === 0; i++) await new Promise((resolve) => setTimeout(resolve, 100))
  return received
}

// What each page's own listeners got, and the room view they listen on, by `<id>:<page>`.
const listeners = new Map<string, { got: string[]; room: Room }>()

/** The page's session listens to a room and a Broadcast key, each listener recording under the page's name. */
async function onListen(id: string, page: string) {
  const got: string[] = []
  const room = await Room.getOrCreate(`sessions:${id}`)
  listeners.set(`${id}:${page}`, { got, room })
  room.subscribe((data) => got.push(`room:${data}`))
  Broadcast.subscribe<string>(`sessions:${id}`, (message) => got.push(`broadcast:${message}`))
  // Each subscription is live once its session's own publish reaches it.
  await Broadcast.publish(`sessions:${id}`, `ready:${page}`)
  await (await room.join()).publish(`ready:${page}`)
}

async function onPublishFrom(id: string, message: string) {
  const room = await Room.get(`sessions:${id}`)
  await (await room.join()).publish(message)
  await Broadcast.publish(`sessions:${id}`, message)
}

async function onReceived(id: string, page: string) {
  return listeners.get(`${id}:${page}`)!.got.filter((message) => !message.includes('ready:'))
}

/** Rooms whose ids differ in their last character, listed by prefix. */
async function onListInOrder() {
  const prefix = `list:${crypto.randomUUID()}:`
  // '！' (U+FF01) sorts after '😀' (U+1F600) by UTF-16 code unit, before it by code point.
  const ids = ['b', '😀', 'a', '！', 'B', 'é', 'z'].map((suffix) => prefix + suffix)
  for (const id of ids) await Room.create(id)
  const listed = (await Room.list({ prefix })).map((room) => room.id.slice(prefix.length))
  for (const id of ids) await Room.close(id)
  return listed
}

/** A room the page's session creates, under the regions test's prefix. */
async function onCreateRoom(id: string, page: string) {
  await Room.create(`regions:${id}:${page}`)
  return 'created'
}

async function onListRooms(id: string) {
  return (await Room.list({ prefix: `regions:${id}:` })).map((room) => room.id)
}

/** A member retains a frame larger than a SQLite row; a view that subscribes afterwards gets it. */
async function onRetainLarge() {
  const id = `retain:${crypto.randomUUID()}`
  const room = await Room.create(id)
  const member = await room.join()
  const frame = new Uint8Array(3 * 1024 * 1024).map((_, i) => i % 251)
  await member.publishBinary(frame, { retain: true })
  const late = await Room.get(id)
  const { promise, resolve } = Promise.withResolvers<Uint8Array>()
  late.subscribeBinary((data) => resolve(data))
  const replayed = await promise
  await Room.close(id)
  return { size: replayed.byteLength, intact: replayed.every((byte, i) => byte === i % 251) }
}

/** A room with a short departureTimeout, whose only member leaves: it closes by itself. */
async function onCloseWhenLeft() {
  const id = `auto-close:${crypto.randomUUID()}`
  const room = await Room.create(id, { departureTimeout: 1000 })
  const { promise: closed, resolve } = Promise.withResolvers<void>()
  room.onClose(() => resolve())
  await (await room.join()).leave()
  await closed
  return { listed: (await Room.list({ prefix: id })).length }
}

let bugs: string[] | null = null
onBug((err) => bugs?.push((err as Error).message))

// The views of a room each page holds, by the id the test gave it.
const heldViews = new Map<string, Room>()

/** A view of a new room, which the page subscribes once another session closed the room. */
async function onHoldView(id: string) {
  await Room.create(`held:${id}`)
  heldViews.set(id, await Room.get(`held:${id}`))
}

async function onCloseHeld(id: string) {
  await Room.close(`held:${id}`)
}

/** Subscribes the held view, waits for it to close, and returns the bugs reported meanwhile. */
async function onSubscribeHeld(id: string) {
  bugs = []
  const room = heldViews.get(id)!
  const { promise, resolve } = Promise.withResolvers<LeaveCause>()
  room.onClose(resolve)
  room.subscribe(() => {})
  room.subscribeBinary(() => {}, { track: 'cam' })
  await promise
  const reported = bugs
  bugs = null
  return reported
}

async function onMetaKeyOrder() {
  const id = `meta-order:${crypto.randomUUID()}`
  await Room.create(id, { meta: { b: 1, a: 2 } })
  await Room.updateMeta(id, { c: 3, a: 4 })
  const merged = (await Room.get(id)).meta
  await Room.setMeta(id, { z: 1, y: 2 })
  const replaced = (await Room.get(id)).meta
  await Room.close(id)
  return { merged: Object.entries(merged), replaced: Object.entries(replaced) }
}

/** A server listener that throws, ahead of a healthy one on the same room: both hear every message. */
async function onThrowingListener() {
  const id = `throwing-listener:${crypto.randomUUID()}`
  const room = await Room.getOrCreate(id)
  let thrower = 0
  let healthy = 0
  room.subscribe(() => {
    thrower++
    throw new Error('Test: a listener throws')
  })
  room.subscribe(() => void healthy++)
  const me = await room.join()
  await me.publish('first')
  await me.publish('second')
  for (let i = 0; i < 50 && (thrower < 2 || healthy < 2); i++) await new Promise((resolve) => setTimeout(resolve, 100))
  await Room.close(id)
  return { thrower, healthy }
}

/** The name of a room's authority, as `telefuncDurableObjectCall` sees it. */
const authorityOf = (roomId: string) => `__telefunc_room__:${roomId}`

const failureOf = (error: unknown) => ({
  message: error instanceof Error ? error.message : String(error),
  cause: error instanceof Error && error.cause instanceof Error ? error.cause.message : null,
})

/** Joins a room that closes while its authority holds the join's inbox registration: what the join gets, and when. */
async function onJoinClosing() {
  const roomId = `join-closing:${crypto.randomUUID()}`
  const room = await Room.create(roomId)
  const reached = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  inboxRegistrations.set(authorityOf(roomId), { kind: 'hold', reached, release: release.promise })
  const startedAt = Date.now()
  const joining = room.join().then(
    () => ({ joined: true, failure: null }),
    (error: unknown) => ({ joined: false, failure: failureOf(error) }),
  )
  await reached.promise
  await Room.close(roomId)
  release.resolve()
  const outcome = await joining
  return { ...outcome, seconds: Math.round((Date.now() - startedAt) / 100) / 10 }
}

/** Joins a room whose authority refuses the join's inbox registration: what the join gets, and who is in the room. */
async function onJoinRefused() {
  const roomId = `join-refused:${crypto.randomUUID()}`
  const room = await Room.create(roomId)
  inboxRegistrations.set(authorityOf(roomId), { kind: 'refuse' })
  const outcome = await room.join().then(
    () => ({ joined: true, failure: null }),
    (error: unknown) => ({ joined: false, failure: failureOf(error) }),
  )
  const members = (await Room.get(roomId)).count
  await Room.close(roomId)
  return { ...outcome, members }
}

// The calls the size tests make, each with one value of `length` characters in a new room. A Durable Object's SQLite
// storage refuses a row over 8 MiB and 34 bytes; the tests find each call's longest value and run it one longer.

/** What `call` told its caller: resolved, or the name of the error it rejected with. */
const outcomeOf = (call: Promise<unknown>) =>
  call.then(
    () => 'resolved',
    (error: unknown) => (error instanceof Error ? error.name : 'not an Error'),
  )

async function inNewRoom<T>(run: (room: Room, id: string) => Promise<T>) {
  const id = `row-limit:${crypto.randomUUID()}`
  const room = await Room.create(id)
  try {
    return await run(room, id)
  } finally {
    await Room.close(id)
  }
}

/** What the sender of an acknowledged DM whose answer has `length` characters was told by the time a later small DM to
 *  the same member had its answer. */
async function onReplyOutcome(length: number) {
  return inNewRoom(async (room) => {
    const sender = await room.join()
    const recipient = await room.join()
    recipient.listen((data) => (data === 'large' ? 'v'.repeat(length) : 'small'))
    let told = 'not told'
    void outcomeOf(sender.send(recipient.id, 'large', { ack: true })).then((outcome) => (told = outcome))
    await sender.send(recipient.id, 'small', { ack: true })
    return { told }
  })
}

/** A removal by identity, with a reason of `length` characters, of a visible tab and a hidden one: what it told, and
 *  the visible tabs the store holds after it. */
async function onRemovalOutcome(length: number) {
  return inNewRoom(async (room, id) => {
    await room.join({ identity: 'u1' })
    await room.join({ identity: 'u1', hidden: true })
    const told = await outcomeOf(Room.removeParticipant(id, { identity: 'u1', reason: 'r'.repeat(length) }))
    return { told, visible: (await Room.getParticipants(id)).length }
  })
}

/** A join with an identity of 1,000 characters and a meta value of `length`: what it told, the participants it left,
 *  and what the next join is told. */
async function onJoinOutcome(length: number) {
  return inNewRoom(async (room, id) => {
    const told = await outcomeOf(room.join({ identity: 'i'.repeat(1000), meta: { m: 'm'.repeat(length) } }))
    const participants = (await Room.getParticipants(id)).length
    return { told, participants, nextJoin: await outcomeOf(room.join({ identity: 'small' })) }
  })
}

/** A `Room.updateMeta()` of a key of 1,000 characters and a value of `length`: what it told, the meta's keys after it,
 *  and what the next update is told. */
async function onMetaUpdateOutcome(length: number) {
  return inNewRoom(async (_room, id) => {
    await Room.setMeta(id, { seed: 's' })
    const told = await outcomeOf(Room.updateMeta(id, { ['k'.repeat(1000)]: 'v'.repeat(length) }))
    const keys = Object.keys((await Room.get(id)).meta)
    return { told, keys, nextUpdate: await outcomeOf(Room.updateMeta(id, { n: '1' })) }
  })
}

/** What a `Room.send()` by id with `length` characters of data told: its commit hands the message on and keeps no
 *  row of it. */
async function onSendOutcome(length: number) {
  return inNewRoom(async (room, id) => {
    const recipient = await room.join()
    return { told: await outcomeOf(Room.send(id, { id: recipient.id }, 'v'.repeat(length))) }
  })
}

const BESIDE: CellsBeside = { of: 'tm:', reference: 'tml:', target: 'tl:' }

/** A cell per filler member, each referencing lease `L1`: with the three below, a prefix read takes two pages. */
const FILLERS = Array.from({ length: 1024 }, (_, i) => `tm:f${String(i).padStart(4, '0')}`)

/** `tm:a` references a lease that is there, `tm:b` one that isn't, `tm:c` none; `tx:a` isn't under `of`. */
const BESIDE_CELLS: Record<string, string> = {
  'tm:a': 'A',
  'tml:a': 'L1',
  'tl:L1': 'lease one',
  'tm:b': 'B',
  'tml:b': 'L2',
  'tm:c': 'C',
  'tx:a': 'X',
  ...Object.fromEntries(
    FILLERS.flatMap((key) => [
      [key, 'F'],
      [`tml:${key.slice(3)}`, 'L1'],
    ]),
  ),
}

function besideText(read: CellsRead): Record<string, string | null> | 'none' {
  if (read.kind !== 'read' || read.beside === undefined) return 'none'
  const decoder = new TextDecoder()
  return Object.fromEntries([...read.beside].map(([key, bytes]) => [key, bytes && decoder.decode(bytes)]))
}

/** What the store reads beside a room's cells: by key, on each page of a prefix read, and without `beside`. */
async function onBesideRead() {
  return inNewRoom(async (_room, id) => {
    const { direct } = getRoomPlane()
    const inc = (await direct.reads.readHead(id))?.currentInc
    if (typeof inc !== 'string') throw new Error(`room ${id} has no incarnation`)
    const encoder = new TextEncoder()
    const mutations = Object.entries(BESIDE_CELLS).map(([key, text]) => ({ key, bytes: encoder.encode(text) }))
    const written = (await direct.compareExchangeCells(id, inc, { expect: [], mutations })).kind
    const keys = ['tm:a', 'tm:b', 'tm:c', 'tx:a', 'tm:absent']
    const byKey = besideText(await direct.reads.readCells(id, inc, { keys, beside: BESIDE }))
    const pages: (Record<string, string | null> | 'none')[] = []
    for await (const page of direct.reads.readCellPages(id, inc, 'tm:', BESIDE)) pages.push(besideText(page))
    const read: Record<string, string | null> = Object.assign({}, ...pages)
    const fillers = FILLERS.filter((key) => read[key] === 'lease one').length
    const others = Object.fromEntries(Object.entries(read).filter(([key]) => !FILLERS.includes(key)))
    const plain = besideText(await direct.reads.readCells(id, inc, { keys }))
    return {
      written,
      byKey,
      pages: pages.length > 1,
      without: pages.filter((page) => page === 'none').length,
      fillers,
      others,
      plain,
    }
  })
}
