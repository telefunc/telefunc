export {
  onTextBroadcast,
  onBinaryBroadcastPair,
  onBinaryBroadcast,
  onBroadcastShieldClient,
  onSubscribeThrows,
  onChatLeaveOnClose,
  onGetChatWatched,
  onServerBroadcastChecks,
  onBroadcastErrors,
}

import { Broadcast, BroadcastChannel, onBug } from 'telefunc'
import { addBackendFault, pendingBackendFaults } from '../../failing-backend'

type TextMsg = { text: string; from: string }

/** Two broadcast instances sharing a key — publish from one, subscribe on the other.
 *  The key is unique per call: the in-memory adapter's seq counter is per-key, so a
 *  fixed key would leak seq state across page loads and break the [1,2,3] assertion. */
async function onTextBroadcast() {
  const key = `room:text-test:${crypto.randomUUID()}`
  const publisher = new BroadcastChannel<TextMsg>({ key })
  const subscriber = new BroadcastChannel<TextMsg>({ key })

  const received: Array<{ text: string; from: string; seq: number }> = []
  subscriber.subscribe((msg, info) => {
    received.push({ text: msg.text, from: msg.from, seq: info.seq })
  })

  return {
    publisher,
    subscriber,
    getReceived: () => received,
  }
}

/** Two broadcast instances sharing a key — publishBinary from one, subscribeBinary on the other.
 *  Unique key per call, same reason as onTextBroadcast(). */
async function onBinaryBroadcastPair() {
  const key = `room:binary-test:${crypto.randomUUID()}`
  const publisher = new BroadcastChannel({ key })
  const subscriber = new BroadcastChannel({ key })

  const received: Array<{ size: number; firstByte: number; seq: number }> = []
  subscriber.subscribeBinary((data, info) => {
    received.push({ size: data.byteLength, firstByte: data[0]!, seq: info.seq })
  })

  return {
    publisher,
    subscriber,
    getReceived: () => received,
  }
}

/** Shield validates client-published data on a `BroadcastChannel<{ text: string }>`:
 *  - valid `{ text: string }` reaches subscribers and resolves with a publish receipt.
 *  - invalid payload rejects the client's `publish()` with a shield error and is not delivered. */
async function onBroadcastShieldClient() {
  const room = new BroadcastChannel<{ text: string }>({ key: `shield-test:${crypto.randomUUID()}` })
  const received: Array<{ text: string }> = []
  room.subscribe((msg) => {
    received.push(msg)
  })
  return { room, getReceived: async () => received }
}

/** Client subscribes to binary broadcast, server publishes frames. Tests client-side subscribeBinary.
 *  The client sends a `{ start: true }` text publish on the same wire as its BROADCAST_SUB (binary),
 *  so by the time this `start` callback runs, the peer is wire-subscribed for binary. */
async function onBinaryBroadcast() {
  const room = new BroadcastChannel<{ start: true }>({ key: `room:broadcast-test:${crypto.randomUUID()}` })
  room.subscribe(async () => {
    for (let i = 0; i < 5; i++) {
      await room.publishBinary(new Uint8Array(64).fill(i + 1))
    }
  })
  return room
}

/** The store refuses the first subscribe on this key (a test asks it to, see `failBackend()`). */
async function onSubscribeThrows() {
  const channel = new BroadcastChannel<{ n: number }>({ key: `room:subscribe-throws:${crypto.randomUUID()}` })
  const seen: string[] = []
  let thrown: string | null = null
  try {
    channel.subscribe((msg) => seen.push(`first:${msg.n}`))
  } catch (err) {
    thrown = (err as Error).message
  }
  const { promise: delivered, resolve } = Promise.withResolvers<void>()
  channel.subscribe((msg) => {
    seen.push(`second:${msg.n}`)
    resolve()
  })
  await channel.publish({ n: 1 })
  await delivered
  return { thrown, seen }
}

const chatWatched = new Map<string, { received: string[]; unsubscribe(): void }>()

/** The documented chat pattern: a page's channel says it left from its onClose. */
async function onChatLeaveOnClose() {
  const key = `room:chat-leave:${crypto.randomUUID()}`
  const received: string[] = []
  chatWatched.set(key, { received, unsubscribe: Broadcast.subscribe<string>(key, (msg) => received.push(msg)) })
  const chat = new BroadcastChannel<string>({ key })
  chat.onClose(() => void chat.publish('left'))
  return { key, chat }
}

async function onGetChatWatched(key: string) {
  const watched = chatWatched.get(key)!
  if (watched.received.length > 0) watched.unsubscribe()
  return watched.received
}

async function onServerBroadcastChecks() {
  const key = `room:server-checks:${crypto.randomUUID()}`

  const aborted = new BroadcastChannel({ key })
  aborted.abort()
  aborted.subscribe(() => {})
  aborted.subscribeBinary(() => {})
  const afterAbort = [
    (await Broadcast.publish(key, 'after-abort')).receivers,
    (await Broadcast.publishBinary(key, new Uint8Array([1]))).receivers,
  ]

  const received: string[] = []
  const unsubscribeLeaving = Broadcast.subscribe<string>(`${key}:leaving`, (msg) => {
    received.push(msg)
    unsubscribeLeaving()
  })
  const unsubscribeJoining = Broadcast.subscribe<string>(`${key}:joining`, (msg) => {
    received.push(msg)
    unsubscribeJoining()
    Broadcast.subscribe(`${key}:joining`, () => {})
  })
  const duringDelivery = [
    (await Broadcast.publish(`${key}:leaving`, 'leaving')).receivers,
    (await Broadcast.publish(`${key}:joining`, 'joining')).receivers,
  ]

  const statically: string[] = []
  const unsubscribe = Broadcast.subscribe<string>(`${key}:static`, (msg) => statically.push(msg))
  await Broadcast.publish(`${key}:static`, 'first')
  unsubscribe()
  await Broadcast.publish(`${key}:static`, 'second')

  return { afterAbort, received, duringDelivery, statically }
}

let bugs: string[] | null = null
onBug((err) => bugs?.push((err as Error).message))

/** Subscribers that throw or reject, and un-awaited publishes the store fails. */
async function onBroadcastErrors() {
  bugs = []
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => void unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  const key = `room:listener-errors:${crypto.randomUUID()}`
  const channel = new BroadcastChannel<string>({ key })
  channel.subscribe(() => {
    throw new Error('broadcast-listener-bug: thrown')
  })
  channel.subscribe(async () => {
    throw new Error('broadcast-listener-bug: rejected')
  })
  const seen: string[] = []
  const { promise: delivered, resolve } = Promise.withResolvers<void>()
  channel.subscribe((msg) => {
    seen.push(msg)
    if (msg === 'b') resolve()
  })
  await channel.publish('a')
  await channel.publish('b')
  await delivered

  addBackendFault({ op: 'publish', key, times: 2 })
  channel.publish('lost')
  Broadcast.publish(key, 'lost')
  // Node emits 'unhandledRejection' once the microtask queue drains, before the next macrotask.
  await new Promise(setImmediate)
  process.off('unhandledRejection', onUnhandled)
  const reported = bugs.sort()
  bugs = null
  return { seen, reported, unhandled: unhandled.length, storeFailedBoth: pendingBackendFaults() === 0 }
}
