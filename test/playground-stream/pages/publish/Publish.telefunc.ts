export {
  onTextBroadcast,
  onBinaryBroadcastPair,
  onBinaryBroadcast,
  onBroadcastShieldClient,
  onBinaryBroadcastOwnBytes,
  onBinaryBroadcastReusedBuffer,
  onBroadcastRightAfterSubscribe,
  onBroadcastAnswers,
  onBroadcastAnsweringLoop,
  onBroadcastWhileEstablishing,
  onSubscribeThrows,
  onChatLeaveOnClose,
  onGetChatWatched,
  onServerBroadcastChecks,
  onBroadcastErrors,
  onBroadcastServerListener,
  onBroadcastOwnBytes,
  onBroadcastOnOpenPublish,
  onBroadcastAnswerOrder,
  onBroadcastSelfAnswer,
  onStaticPublishReceipts,
  onBroadcastSelfUnsubscribe,
  onBroadcastReconnect,
  onBroadcastWhileOffline,
}

import { Buffer } from 'node:buffer'
import { Broadcast, BroadcastChannel, onBug } from 'telefunc'
import { addBackendFault, pendingBackendFaults } from '../../failing-backend'
import { cleanupState } from '../../cleanup-state'
import { onServerAction } from '../../server-actions'

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

/** On the page's `start`, the server publishes a Node Buffer. On the channel and on the key, the first server listener
 *  writes into the bytes it gets and the second records them. */
async function onBinaryBroadcastOwnBytes() {
  const key = `room:own-bytes-test:${crypto.randomUUID()}`
  const room = new BroadcastChannel<{ start: true }>({ key })
  const seen: number[][] = []
  room.subscribeBinary((data) => data.fill(0))
  room.subscribeBinary((data) => seen.push([...data]))
  const keySeen: number[][] = []
  const unsubscribes = [
    Broadcast.subscribeBinary(key, (data) => data.fill(0)),
    Broadcast.subscribeBinary(key, (data) => keySeen.push([...data])),
  ]
  room.subscribe(async () => {
    await room.publishBinary(Buffer.from([1, 2, 3, 4]))
  })
  room.onClose(() => {
    for (const unsubscribe of unsubscribes) unsubscribe()
  })
  return { room, getSeen: async () => ({ seen, keySeen }) }
}

/** On the page's `start`, a second instance subscribes on the key, and the server publishes 3 binary frames from one
 *  reused Node Buffer at once, so they wait for that subscription. */
async function onBinaryBroadcastReusedBuffer() {
  const key = `room:reused-buffer-test:${crypto.randomUUID()}`
  const room = new BroadcastChannel<{ start: true }>({ key })
  const sibling = new BroadcastChannel({ key })
  const seen: number[][] = []
  room.subscribe(() => {
    sibling.subscribeBinary((data) => seen.push([...data]))
    const bytes = Buffer.alloc(4)
    for (let i = 1; i <= 3; i++) void room.publishBinary(bytes.fill(i))
  })
  room.onClose(() => {
    void sibling.close()
  })
  return { room, getSeen: async () => seen }
}

/** While another key's publishes are delivered, an instance subscribes text and binary and publishes text, binary and
 *  text at once. */
async function onBroadcastRightAfterSubscribe() {
  const busy = new BroadcastChannel<number>({ key: `room:busy-test:${crypto.randomUUID()}` })
  busy.subscribe(() => {})
  for (let i = 0; i < 2000; i++) void busy.publish(i)
  const room = new BroadcastChannel<string>({ key: `room:right-after-test:${crypto.randomUUID()}` })
  const seen: unknown[] = []
  // publish() resolves once the message is accepted: a store like Redis delivers it to local subscribers after that.
  const sawAll = Promise.withResolvers<void>()
  const see = (entry: unknown) => {
    seen.push(entry)
    if (seen.length === 3) sawAll.resolve()
  }
  room.subscribe(see)
  room.subscribeBinary((data) => see([...data]))
  await Promise.all([room.publish('t1'), room.publishBinary(new Uint8Array([1])), room.publish('t2')])
  const deadline = Promise.withResolvers<void>()
  const timer = setTimeout(deadline.resolve, 10_000)
  await Promise.race([sawAll.promise, deadline.promise])
  clearTimeout(timer)
  await Promise.all([busy.close(), room.close()])
  return seen
}

/** One listener answers `ping` at once, another after an await. */
async function onBroadcastAnswers() {
  const key = `room:answers-test:${crypto.randomUUID()}`
  const room = new BroadcastChannel<string>({ key })
  const answerer = new BroadcastChannel<string>({ key })
  answerer.subscribe((data) => {
    if (data === 'ping') void answerer.publish('pong')
  })
  answerer.subscribe(async (data) => {
    await Promise.resolve()
    if (data === 'ping') await answerer.publish('async-pong')
  })
  room.onClose(() => {
    void answerer.close()
  })
  return room
}

/** A listener answers every message on its key while the server waits for a 0 ms timer; then it awaits 3,000 publishes. */
async function onBroadcastAnsweringLoop() {
  const loop = new BroadcastChannel<number>({ key: `room:loop-test:${crypto.randomUUID()}` })
  let answering = true
  loop.subscribe(async (n) => {
    await Promise.resolve()
    if (answering) void loop.publish(n + 1)
  })
  void loop.publish(0)
  await new Promise((resolve) => setTimeout(resolve, 0))
  answering = false
  const counter = new BroadcastChannel<number>({ key: `room:awaited-test:${crypto.randomUUID()}` })
  let delivered = 0
  counter.subscribe(() => delivered++)
  for (let i = 0; i < 3000; i++) await counter.publish(i)
  const deliveredAtLastReceipt = delivered
  // On Redis a local delivery comes back through the store after the publish's receipt.
  for (let poll = 0; poll < 50 && delivered < 3000; poll++) await new Promise((resolve) => setTimeout(resolve, 200))
  await Promise.all([loop.close(), counter.close()])
  return { timerFired: true, deliveredAtLastReceipt, delivered }
}

/** While another key's publishes are delivered, an instance subscribes and publishes three 1,500-byte messages at once,
 *  past the 4,096-byte bufferLimit of the lane that runs it. */
async function onBroadcastWhileEstablishing() {
  const busy = new BroadcastChannel<number>({ key: `room:busy-test:${crypto.randomUUID()}` })
  busy.subscribe(() => {})
  for (let i = 0; i < 2000; i++) void busy.publish(i)
  const room = new BroadcastChannel<string>({ key: `room:held-test:${crypto.randomUUID()}` })
  const seen: string[] = []
  room.subscribe((data) => seen.push(data[0]!))
  const outcomes = await Promise.all(
    ['1', '2', '3'].map((n) =>
      room.publish(n.padEnd(1500, '.')).then(
        () => 'sent',
        (err: Error) => err.name,
      ),
    ),
  )
  await new Promise((resolve) => setTimeout(resolve, 0))
  await Promise.all([busy.close(), room.close()])
  return { outcomes, seen }
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

  // publish() resolves once the message is accepted: a store like Redis delivers it to local subscribers after that.
  const leavingSeen = Promise.withResolvers<void>()
  const joiningSeen = Promise.withResolvers<void>()
  const firstSeen = Promise.withResolvers<void>()

  const received: string[] = []
  const unsubscribeLeaving = Broadcast.subscribe<string>(`${key}:leaving`, (msg) => {
    received.push(msg)
    unsubscribeLeaving()
    leavingSeen.resolve()
  })
  const unsubscribeJoining = Broadcast.subscribe<string>(`${key}:joining`, (msg) => {
    received.push(msg)
    unsubscribeJoining()
    Broadcast.subscribe(`${key}:joining`, () => {})
    joiningSeen.resolve()
  })
  const duringDelivery = [
    (await Broadcast.publish(`${key}:leaving`, 'leaving')).receivers,
    (await Broadcast.publish(`${key}:joining`, 'joining')).receivers,
  ]

  const statically: string[] = []
  const unsubscribe = Broadcast.subscribe<string>(`${key}:static`, (msg) => {
    statically.push(msg)
    firstSeen.resolve()
  })
  await Broadcast.publish(`${key}:static`, 'first')
  const deadline = Promise.withResolvers<void>()
  const timer = setTimeout(deadline.resolve, 10_000)
  await Promise.race([Promise.all([leavingSeen.promise, joiningSeen.promise, firstSeen.promise]), deadline.promise])
  clearTimeout(timer)
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

/** The server listens on the broadcast it returns, on both kinds, while the page subscribes and unsubscribes. */
async function onBroadcastServerListener() {
  const room = new BroadcastChannel<string>({ key: `room:server-listener:${crypto.randomUUID()}` })
  const received: Array<string | number> = []
  room.subscribe((msg) => {
    received.push(msg)
  })
  room.subscribeBinary((data) => {
    received.push(data.byteLength)
  })
  return { room, getReceived: async () => received }
}

/** Two binary listeners of a broadcast and two of its key's static subscribers; the first of each transfers its bytes. */
async function onBroadcastOwnBytes() {
  const key = `room:own-bytes:${crypto.randomUUID()}`
  const room = new BroadcastChannel({ key })
  const received: Array<number[] | 'detached'> = []
  const transfer = (data: Uint8Array) => void structuredClone(data.buffer, { transfer: [data.buffer] })
  const record = (data: Uint8Array) => void received.push(data.byteLength === 0 ? 'detached' : [...data])
  room.subscribeBinary(transfer)
  room.subscribeBinary(record)
  const unsubscribe = [Broadcast.subscribeBinary(key, transfer), Broadcast.subscribeBinary(key, record)]
  room.onClose(() => {
    for (const off of unsubscribe) off()
  })
  return { room, getReceived: async () => received }
}

async function onBroadcastOnOpenPublish() {
  const room = new BroadcastChannel<string>({ key: `room:on-open:${crypto.randomUUID()}` })
  room.onOpen(() => {
    void room.publish('from-onOpen')
  })
  return room
}

/** The first of two subscribers answers message 1 with message 2; both listen until the second sees it, or 10 s pass. */
async function onBroadcastAnswerOrder() {
  const key = `room:answer-order:${crypto.randomUUID()}`
  const seen: string[] = []
  // publish() resolves once the message is accepted: a store like Redis delivers it to local subscribers after that.
  const sawAnswer = Promise.withResolvers<void>()
  const offFirst = Broadcast.subscribe<number>(key, (n) => {
    seen.push(`first:${n}`)
    if (n === 1) void Broadcast.publish(key, 2)
  })
  const offSecond = Broadcast.subscribe<number>(key, (n) => {
    seen.push(`second:${n}`)
    if (n === 2) sawAnswer.resolve()
  })
  await Broadcast.publish(key, 1)
  const deadline = Promise.withResolvers<void>()
  const timer = setTimeout(deadline.resolve, 10_000)
  await Promise.race([sawAnswer.promise, deadline.promise])
  clearTimeout(timer)
  offFirst()
  offSecond()
  return seen
}

/** Listeners that answer every message on their own key, one at once and one after an await, until stop(); it returns
 *  once each answered, or 10 s pass. */
async function onBroadcastSelfAnswer() {
  const key = `room:self-answer:${crypto.randomUUID()}`
  let answers = 0
  // A store like Redis delivers a message to local subscribers once it comes back through the store.
  const syncAnswered = Promise.withResolvers<void>()
  const asyncAnswered = Promise.withResolvers<void>()
  const offSync = Broadcast.subscribe<number>(`${key}:sync`, (n) => {
    answers++
    syncAnswered.resolve()
    void Broadcast.publish(`${key}:sync`, n + 1)
  })
  const offAsync = Broadcast.subscribe<number>(`${key}:async`, async (n) => {
    await Promise.resolve()
    answers++
    asyncAnswered.resolve()
    void Broadcast.publish(`${key}:async`, n + 1)
  })
  void Broadcast.publish(`${key}:sync`, 0)
  void Broadcast.publish(`${key}:async`, 0)
  const deadline = Promise.withResolvers<void>()
  const timer = setTimeout(deadline.resolve, 10_000)
  await Promise.race([Promise.all([syncAnswered.promise, asyncAnswered.promise]), deadline.promise])
  clearTimeout(timer)
  return {
    stop: async () => {
      offSync()
      offAsync()
      return answers
    },
  }
}

async function onStaticPublishReceipts() {
  const key = `room:receipt:${crypto.randomUUID()}`
  const text = await Broadcast.publish(key, 'hi')
  const binary = await Broadcast.publishBinary(key, new Uint8Array([1]))
  return { key, receipts: [text, binary].map((receipt) => ({ key: receipt.key, seq: receipt.seq })) }
}

/** On the server and on the page, the first of two subscribers unsubscribes itself on its first message. */
async function onBroadcastSelfUnsubscribe() {
  const room = new BroadcastChannel<string>({ key: `room:self-unsubscribe:${crypto.randomUUID()}` })
  const serverSeen: string[] = []
  const off = room.subscribe((msg) => {
    serverSeen.push(`once:${msg}`)
    off()
  })
  room.subscribe((msg) => {
    serverSeen.push(`other:${msg}`)
  })
  return {
    room,
    publish: async () => {
      await room.publish('one')
      await room.publish('two')
    },
    getServerSeen: async () => serverSeen,
  }
}

async function onBroadcastReconnect() {
  const room = new BroadcastChannel<string>({ key: `room:reconnect:${crypto.randomUUID()}` })
  return {
    room,
    publish: async (text: string) => {
      await room.publish(text)
    },
  }
}

/** At server action `publish_<key>`, publishes `count` messages of 128 KiB to the key. */
async function onBroadcastWhileOffline(count: number) {
  const key = `room:while-offline:${crypto.randomUUID()}`
  const room = new BroadcastChannel<string>({ key })
  room.onClose((err) => {
    cleanupState[`whileOffline_${key}_serverClose`] = err?.name ?? 'none'
  })
  onServerAction(`publish_${key}`, () => {
    for (let n = 0; n < count; n++) void Broadcast.publish(key, String(n).padEnd(128 * 1024))
  })
  return { key, room }
}
