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
  onThrowingReceiver,
}

import { Broadcast, BroadcastChannel, onBug } from 'telefunc'
import { Room } from '@telefunc/room'
import { getRoomPlane } from '@telefunc/room/backend'

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
  const { promise, resolve } = Promise.withResolvers<void>()
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
  await Room.setAttributes(id, { c: 3, a: 4 })
  const merged = (await Room.get(id)).meta
  await Room.setMeta(id, { z: 1, y: 2 })
  const replaced = (await Room.get(id)).meta
  await Room.close(id)
  return { merged: Object.entries(merged), replaced: Object.entries(replaced) }
}

/** A store handing a control frame to a receiver that throws: no write sees its delivery lost, and the next frame still
 *  reaches it. */
async function onThrowingReceiver() {
  const id = `throwing-receiver:${crypto.randomUUID()}`
  await Room.create(id)
  const { driver } = getRoomPlane()
  const inc = (await driver.readHead(id))!.currentInc!
  let calls = 0
  const attempt = driver.subscribe(
    { roomId: id, inc, lane: { kind: 'control' } },
    {
      deliver() {
        calls++
        throw new Error('Test: a receiver throws')
      },
    },
  )
  while (attempt.state() !== 'ready') await new Promise((resolve) => setTimeout(resolve, 10))
  const write = (n: number) =>
    Room.setMeta(id, { n }).then(
      () => 'written',
      (error: Error) => error.message,
    )
  const writes = [await write(1), await write(2)]
  await attempt.unsubscribe()
  return { writes, calls }
}
