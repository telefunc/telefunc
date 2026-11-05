export {
  onTextBroadcast,
  onBroadcastApi,
  onBroadcastBacklog,
  onInMemoryPublishes,
  onBroadcastBurstThenClose,
  onBinaryBroadcastPair,
  onBinaryBroadcast,
  onBroadcastShieldClient,
  onBinaryBroadcastReusedBuffer,
  onBroadcastRightAfterSubscribe,
  onBroadcastAnswers,
  onBroadcastAnsweringLoop,
  onBroadcastWhileEstablishing,
  onSubscribeRefused,
  onDeliveredBeforeReady,
  onJoinedBeforeReady,
  onDeliveredWhileLost,
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
  onBroadcastSeqAcrossSubscribers,
  onBroadcastJoinWhileQueued,
  onBroadcastManyKeys,
  onBroadcastThrowingListener,
  onJoinChat,
}

import { Buffer } from 'node:buffer'
import { Abort, Broadcast, BroadcastChannel, onBug } from 'telefunc'
import { addBackendFault, pendingBackendFaults } from '../../failing-backend'
import { prepareBroadcastApi } from './broadcastApiProbe'
import { cleanupState } from '../../cleanup-state'
import { onServerAction } from '../../server-actions'

type TextMsg = { text: string; from: string }

/** Two broadcast instances sharing a key — publish from one, subscribe on the other.
 *  The key is unique per call, so no other page load publishes on it. */
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
  await within10s(sawAll.promise)
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
  loop.subscribe((n) => {
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

/** An instance subscribes and publishes three 1,500-byte messages in the same turn, past the 4,096-byte bufferLimit of
 *  the lane that runs it: Redis's SUBSCRIBE round trip keeps the new subscription short of ready, so the Redis driver
 *  holds the three publishes until it is. It returns once its listener saw two, or 10 s pass. */
async function onBroadcastWhileEstablishing() {
  const room = new BroadcastChannel<string>({ key: `room:held-test:${crypto.randomUUID()}` })
  const seen: string[] = []
  // publish() resolves once the message is accepted: a store like Redis delivers it to local subscribers after that.
  const sawBoth = Promise.withResolvers<void>()
  room.subscribe((data) => {
    seen.push(data[0]!)
    if (seen.length === 2) sawBoth.resolve()
  })
  const outcomes = await Promise.all(
    ['1', '2', '3'].map((n) =>
      room.publish(n.padEnd(1500, '.')).then(
        () => 'sent',
        (err: Error) => err.name,
      ),
    ),
  )
  await within10s(sawBoth.promise)
  await room.close()
  return { outcomes, seen }
}

/** The store refuses the first subscribe on this key (a test asks it to, see `failBackend()`). */
/** The store refuses this key's first subscribe (a test asks it to, see `failBackend()`). */
async function onSubscribeRefused() {
  const channel = new BroadcastChannel<{ n: number }>({ key: `room:subscribe-refused:${crypto.randomUUID()}` })
  const seen: string[] = []
  const { promise: delivered, resolve } = Promise.withResolvers<void>()
  channel.subscribe((msg) => {
    seen.push(`first:${msg.n}`)
    resolve()
  })
  // The refused subscription is replaced a few microtasks later.
  await new Promise((resolve) => setTimeout(resolve, 0))
  await channel.publish({ n: 1 })
  await delivered
  return { seen }
}

/** The store delivers the first message on this key before it acknowledges the subscription (a test asks it to, see
 *  `failBackend()`). */
async function onDeliveredBeforeReady() {
  const channel = new BroadcastChannel<{ n: number }>({ key: `room:delivered-before-ready:${crypto.randomUUID()}` })
  const seen: number[] = []
  const { promise: delivered, resolve } = Promise.withResolvers<void>()
  channel.subscribe((msg) => {
    seen.push(msg.n)
    if (msg.n === 2) resolve()
  })
  await channel.publish({ n: 1 })
  await channel.publish({ n: 2 })
  await within10s(delivered)
  await channel.close()
  return seen
}

/** The store delivers two messages on this key before it acknowledges the subscription, and a second listener joins
 *  between them; like Redis, the store leaves it to core to keep the first message from that listener. */
async function onJoinedBeforeReady() {
  const key = `room:joined-before-ready:${crypto.randomUUID()}`
  addBackendFault({ op: 'early', key, times: 2 })
  const first = new BroadcastChannel<{ n: number }>({ key })
  const second = new BroadcastChannel<{ n: number }>({ key })
  const seen = { first: [] as number[], second: [] as number[] }
  const { promise: delivered, resolve } = Promise.withResolvers<void>()
  first.subscribe((msg) => seen.first.push(msg.n))
  await first.publish({ n: 1 })
  addBackendFault({ op: 'published', key })
  second.subscribe((msg) => {
    seen.second.push(msg.n)
    if (msg.n === 2) resolve()
  })
  await first.publish({ n: 2 })
  await within10s(delivered)
  await Promise.all([first.close(), second.close()])
  return seen
}

/** The store loses the subscription on this key and goes on delivering (a test asks it to, see `failBackend()`). */
async function onDeliveredWhileLost() {
  const channel = new BroadcastChannel<{ n: number }>({ key: `room:delivered-while-lost:${crypto.randomUUID()}` })
  const seen: number[] = []
  const { promise: delivered, resolve } = Promise.withResolvers<void>()
  channel.subscribe((msg) => {
    seen.push(msg.n)
    if (msg.n === 2) resolve()
  })
  await channel.publish({ n: 1 })
  await channel.publish({ n: 2 })
  await within10s(delivered)
  await channel.close()
  return seen
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
  const afterAbort: unknown[] = []
  aborted.subscribe((msg) => afterAbort.push(msg))
  aborted.subscribeBinary((bytes) => afterAbort.push(bytes))
  // A live subscriber of the key sees both publishes, so the aborted channel had its chance to.
  const liveSeen = Promise.withResolvers<void>()
  const unsubscribeLive = Broadcast.subscribe<string>(key, () => liveSeen.resolve())
  await Broadcast.publishBinary(key, new Uint8Array([1]))
  await Broadcast.publish(key, 'after-abort')

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
  await Broadcast.publish(`${key}:leaving`, 'leaving')
  await Broadcast.publish(`${key}:joining`, 'joining')

  const statically: string[] = []
  const unsubscribe = Broadcast.subscribe<string>(`${key}:static`, (msg) => {
    statically.push(msg)
    firstSeen.resolve()
  })
  await Broadcast.publish(`${key}:static`, 'first')
  await within10s(Promise.all([liveSeen.promise, leavingSeen.promise, joiningSeen.promise, firstSeen.promise]))
  unsubscribeLive()
  unsubscribe()
  await Broadcast.publish(`${key}:static`, 'second')

  return { afterAbort, received, statically }
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
  await within10s(sawAnswer.promise)
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
  await within10s(Promise.all([syncAnswered.promise, asyncAnswered.promise]))
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

/** A key's receipts: two publishes while it has a subscriber, two after it left and another key published, one with a new subscriber. */
async function onBroadcastSeqAcrossSubscribers() {
  const key = `room:seq:${crypto.randomUUID()}`
  const seqs: number[] = []
  const publish = async () => seqs.push((await Broadcast.publish(key, seqs.length)).seq)
  const off = Broadcast.subscribe(key, () => {})
  await publish()
  await publish()
  off()
  await Broadcast.publish(`room:seq-other:${crypto.randomUUID()}`, 0)
  await publish()
  await publish()
  const offAgain = Broadcast.subscribe(key, () => {})
  await publish()
  offAgain()
  return seqs
}

/** A text subscriber holds a key. A listener's in-memory publishes queue: it gives the key text, binary and text, a second
 *  subscriber then subscribes to both kinds, and the key gets text once more. What it got. */
async function onBroadcastJoinWhileQueued() {
  const key = `room:join-queued:${crypto.randomUUID()}`
  const trigger = `room:join-trigger:${crypto.randomUUID()}`
  const offs = [Broadcast.subscribe(key, () => {})]
  // Resolves once the subscription is established, so the readiness gate holds none of the publishes below.
  await Broadcast.publish(key, 't0')
  const received: Array<string | number> = []
  const queued: Array<ReturnType<typeof Broadcast.publish>> = []
  offs.push(
    Broadcast.subscribe(trigger, () => {
      queued.push(
        Broadcast.publish(key, 't1'),
        Broadcast.publishBinary(key, new Uint8Array([2])),
        Broadcast.publish(key, 't3'),
      )
      offs.push(Broadcast.subscribe<string>(key, (message) => received.push(message)))
      offs.push(Broadcast.subscribeBinary(key, (data) => received.push(data[0]!)))
    }),
  )
  await Broadcast.publish(trigger, 'go')
  await Promise.all(queued)
  await Broadcast.publish(key, 't4')
  for (const off of offs) off()
  return received
}

/** Publishes once on each of `count` distinct keys, each without a subscriber, or each while one subscribes. */
async function onBroadcastManyKeys(count: number, subscribed: boolean) {
  const run = crypto.randomUUID()
  const published: Promise<unknown>[] = []
  for (let i = 0; i < count; i++) {
    const key = `room:many:${run}:${i}`
    const off = subscribed ? Broadcast.subscribe(key, () => {}) : null
    published.push(Promise.resolve(Broadcast.publish(key, i)).finally(() => off?.()))
  }
  await Promise.all(published)
}

/** A key's static listeners, the first throwing, the second rejecting and the third throwing `Abort`, then one that
 *  records, then a `BroadcastChannel`'s. */
async function onBroadcastThrowingListener() {
  const key = `room:throwing:${crypto.randomUUID()}`
  const received: string[] = []
  // A store like Redis delivers a message to local subscribers once it comes back through the store.
  const delivered = Promise.withResolvers<void>()
  const record = (message: string) => {
    received.push(message)
    if (received.length === 2) delivered.resolve()
  }
  const unsubscribe = [
    Broadcast.subscribe(key, () => {
      throw new Error('broadcast-listener-throws')
    }),
    Broadcast.subscribe(key, async () => {
      throw new Error('broadcast-listener-rejects')
    }),
    Broadcast.subscribe(key, () => {
      throw Abort('broadcast-listener-aborts')
    }),
    Broadcast.subscribe<string>(key, (message) => record(`static:${message}`)),
  ]
  const room = new BroadcastChannel<string>({ key })
  room.subscribe((message) => record(`channel:${message}`))
  const receipt = await Broadcast.publish(key, 'hello')
  await within10s(delivered.promise)
  for (const off of unsubscribe) off()
  await room.close()
  return { received, receiptKey: receipt.key === key }
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

/** Resolves once `seen` does, or after 10 s, so the page gets whatever arrived by then. */
async function within10s(seen: Promise<unknown>): Promise<void> {
  const deadline = Promise.withResolvers<void>()
  const timer = setTimeout(deadline.resolve, 10_000)
  await Promise.race([seen, deadline.promise])
  clearTimeout(timer)
}

/** A chat room whose subscriber saves each message, and which sends its history to a page that joins. */
async function onJoinChat() {
  const room = new BroadcastChannel<string>({ key: `chat:${crypto.randomUUID()}` })
  const database = connectToDatabase()
  room.subscribe(async (message) => {
    await database.saveMessage(message)
  })
  room.onOpen(async () => {
    for (const message of await loadHistory()) await room.publish(message)
  })
  return room
}

/** The chat history store is down. */
async function loadHistory(): Promise<string[]> {
  throw new Error('Could not load the chat history')
}

/** A database that is unavailable for the first write. */
function connectToDatabase() {
  let connected = false
  const saved: string[] = []
  return {
    async saveMessage(message: string) {
      if (!connected) {
        connected = true
        throw new Error('Database unavailable')
      }
      saved.push(message)
      cleanupState.chatSaved = JSON.stringify(saved)
    },
  }
}

async function onBroadcastApi() {
  const room = new BroadcastChannel<string>({ key: `room:api:${crypto.randomUUID()}` })
  const probe = prepareBroadcastApi(room)
  const controller = new AbortController()
  let staticCalls = 0
  Broadcast.subscribe(room.key, () => staticCalls++, { signal: controller.signal })
  Broadcast.subscribeBinary(room.key, () => staticCalls++, { signal: controller.signal })
  Broadcast.subscribe(room.key, () => (staticCalls += 100), { signal: AbortSignal.abort() })
  Broadcast.subscribeBinary(room.key, () => (staticCalls += 100), { signal: AbortSignal.abort() })
  room.onClose(() => controller.abort())
  return {
    room,
    stop: async () => {
      const result = await probe.stop()
      controller.abort()
      return result
    },
    snapshot: () => ({ callbacks: probe.callbacks(), staticCalls }),
  }
}

/** A busy key: `flood()` publishes 2 KiB messages without awaiting them. A `'slow'` server loop over the key's
 *  messages takes 20 ms per message, as the page's slow loop does; a `'held'` one holds its first message until the
 *  key's channel closes. */
async function onBroadcastBacklog(serverLoopKind: 'slow' | 'held' | null) {
  const room = new BroadcastChannel<string>({ key: `room:backlog:${crypto.randomUUID()}` })
  const payload = 'x'.repeat(2048)
  const serverLoop = serverLoopKind === null ? null : readSlowly(room, serverLoopKind)
  return {
    room,
    flood: async (count: number) => {
      for (let i = 0; i < count; i++) void room.publish(`${i}:${payload}`)
    },
    publishAwaited: async (count: number) => {
      for (let i = 0; i < count; i++) await room.publish(`${i}:${payload}`)
    },
    serverLoop: () => serverLoop,
    close: async () => {
      await room.close()
    },
  }
}

/** A job publishes a burst and closes its broadcast in one task, while a server loop over it takes 20 ms per message. */
async function onBroadcastBurstThenClose() {
  const room = new BroadcastChannel<string>({ key: `room:burst:${crypto.randomUUID()}` })
  const serverLoop = readSlowly(room, 'slow')
  await room.publish('start')
  await new Promise((resolve) => setTimeout(resolve, 0))
  for (let i = 0; i < 1000; i++) await room.publish(`${i}:${'x'.repeat(2048)}`)
  await room.close()
  return serverLoop
}

/** The in-memory adapter: a loop of publishes, a listener publishing a batch, then one big message, producers awaiting
 *  each publish to one key, and a listener answering its own key. */
async function onInMemoryPublishes() {
  const id = crypto.randomUUID()
  const seen: number[] = []
  const stopLoop = Broadcast.subscribe<number>(`loop:${id}`, (n) => void seen.push(n))
  const stopAnswer = Broadcast.subscribe(`loop:${id}`, () => void Broadcast.publish(`answers:${id}`, 'seen'))
  for (let i = 0; i < 3000; i++) void Broadcast.publish(`loop:${id}`, i)
  const loop = { deliveredInLoop: seen.length, inOrder: seen.every((n, i) => n === i) }
  stopLoop()
  stopAnswer()

  const listenerPublishes = async (count: number) => {
    const batch: number[] = []
    const stopBatch = Broadcast.subscribeBinary(`batch:${id}`, (data) => void batch.push(data[0]! * 256 + data[1]!))
    const sends: unknown[] = []
    const stopStart = Broadcast.subscribe(`start:${id}`, () => {
      const bytes = new Uint8Array(64 * 1024)
      for (let i = 0; i < count; i++) {
        bytes[0] = i >> 8
        bytes[1] = i & 255
        sends.push(Broadcast.publishBinary(`batch:${id}`, bytes))
      }
    })
    await Broadcast.publish(`start:${id}`, 'go')
    const results = await Promise.allSettled(sends)
    await new Promise((resolve) => setTimeout(resolve, 50))
    const rejected = results.filter((r) => r.status === 'rejected')
    const firstRejected = results.findIndex((r) => r.status === 'rejected')
    stopBatch()
    stopStart()
    return {
      delivered: batch.length,
      rejected: rejected.length,
      errors: [...new Set(rejected.map((r) => (r as PromiseRejectedResult).reason.name))],
      admittedInOrder: (firstRejected === -1 || firstRejected === batch.length) && batch.every((n, i) => n === i),
    }
  }
  const listenerBatch = await listenerPublishes(2000)
  const pastTheBound = await listenerPublishes(5000)

  let aloneBytes = 0
  let alone: unknown
  const stopAlone = Broadcast.subscribeBinary(`alone:${id}`, (data) => void (aloneBytes = data.byteLength))
  const stopAloneStart = Broadcast.subscribe(`alone-start:${id}`, () => {
    alone = Broadcast.publishBinary(`alone:${id}`, new Uint8Array(8 * 1024 * 1024))
  })
  await Broadcast.publish(`alone-start:${id}`, 'go')
  await alone
  stopAlone()
  stopAloneStart()

  let producerMessages = 0
  const stopProducers = Broadcast.subscribe(`producers:${id}`, () => void producerMessages++)
  const payload = 'x'.repeat(400 * 1024)
  const producerRuns = await Promise.allSettled(
    Array.from({ length: 4 }, async () => {
      for (let i = 0; i < 2000; i++) await Broadcast.publish(`producers:${id}`, payload)
    }),
  )
  stopProducers()
  const producers = {
    received: producerMessages,
    rejected: producerRuns.filter((r) => r.status === 'rejected').length,
  }

  // Each tick publishes 4,096 messages, unawaited, each in a run of its own: all of it arrives by the next tick.
  let timerDelivered = 0
  let timerSent = 0
  let peakWaiting = 0
  const stopTimerProducer = Broadcast.subscribe(`timer:${id}`, () => void timerDelivered++)
  for (let tick = 0; tick < 30; tick++) {
    await new Promise((resolve) => setTimeout(resolve, 1))
    peakWaiting = Math.max(peakWaiting, timerSent - timerDelivered)
    for (let i = 0; i < 4096; i++) {
      await null
      void Broadcast.publish(`timer:${id}`, i)
      timerSent++
    }
  }
  await new Promise((resolve) => setTimeout(resolve, 1))
  stopTimerProducer()
  const timerProducer = { sent: timerSent, peakWaiting, delivered: timerDelivered }

  // Two turns, each with 20,000 of 64 KiB in one run of code, all delivering in it on a reused buffer nothing has to copy.
  const reused = new Uint8Array(64 * 1024)
  const longLoopsInLoop: number[] = []
  let longLoopDelivered = 0
  const stopLongLoop = Broadcast.subscribeBinary(`long-loop:${id}`, () => void longLoopDelivered++)
  for (let turn = 0; turn < 2; turn++) {
    await new Promise((resolve) => setImmediate(resolve))
    longLoopDelivered = 0
    for (let i = 0; i < 20_000; i++) void Broadcast.publishBinary(`long-loop:${id}`, reused)
    longLoopsInLoop.push(longLoopDelivered)
  }
  stopLongLoop()

  // A listener's answer waits for the next turn, as a loop publishes 5,000 of 64 KiB: past 256 MiB, what waits goes.
  await new Promise((resolve) => setTimeout(resolve, 0))
  let behindDelivered = 0
  const stopBehind = Broadcast.subscribeBinary(`behind:${id}`, () => void behindDelivered++)
  const stopFirst = Broadcast.subscribe(`answer-1:${id}`, () => void Broadcast.publish(`answer-2:${id}`, 'seen'))
  const stopSecond = Broadcast.subscribe(`answer-2:${id}`, () => void Broadcast.publish(`answer-3:${id}`, 'seen'))
  void Broadcast.publish(`answer-1:${id}`, 'go')
  for (let i = 0; i < 5000; i++) void Broadcast.publishBinary(`behind:${id}`, reused)
  const behindInLoop = behindDelivered
  await new Promise((resolve) => setTimeout(resolve, 50))
  const behindAnAnswer = { inLoop: behindInLoop, delivered: behindDelivered }
  stopBehind()
  stopFirst()
  stopSecond()

  return {
    longLoopsInLoop,
    behindAnAnswer,
    loop,
    listenerBatch,
    pastTheBound,
    aloneBytes,
    producers,
    timerProducer,
    timerFiredMidChain: await timerFiresMidChain(`chain:${id}`),
    awaitedLoopEnded: await timerEndsAwaitedLoop(`awaited-loop:${id}`),
    answeringListenerEnded: await timerEndsAnsweringListener(`answering:${id}`),
  }
}

/** A loop that only awaits publishes: true once the timer that stops it has fired. */
async function timerEndsAwaitedLoop(key: string): Promise<boolean> {
  let stop = false
  setTimeout(() => (stop = true), 50)
  while (!stop) await Broadcast.publish(key, 'again')
  return true
}

/** A listener that answers itself after an `await`: true once the timer that stops it has fired. */
function timerEndsAnsweringListener(key: string): Promise<boolean> {
  let stop = false
  setTimeout(() => (stop = true), 50)
  return new Promise((resolve) => {
    const unsubscribe = Broadcast.subscribe(key, async () => {
      await null
      if (stop) {
        unsubscribe()
        resolve(true)
      } else void Broadcast.publish(key, 'again')
    })
    void Broadcast.publish(key, 'start')
  })
}

/** A listener answers itself 20,000 times: whether a timer fired before it was done. */
async function timerFiresMidChain(key: string): Promise<boolean> {
  let answers = 0
  let answersWhenTimerFired = -1
  setTimeout(() => (answersWhenTimerFired = answers), 0)
  const finished = new Promise<void>((resolve) => {
    Broadcast.subscribe(key, () => {
      if (++answers === 20_000) resolve()
      else void Broadcast.publish(key, 'again')
    })
  })
  void Broadcast.publish(key, 'start')
  await finished
  return answersWhenTimerFired >= 0 && answersWhenTimerFired < 20_000
}

async function readSlowly(room: BroadcastChannel<string>, kind: 'slow' | 'held') {
  let received = 0
  try {
    for await (const _ of room.messages()) {
      received++
      if (kind === 'slow') await new Promise((resolve) => setTimeout(resolve, 20))
      else if (received === 1) await room.closed.catch(() => {})
    }
    return { received, error: null }
  } catch (err) {
    return { received, error: (err as Error).name }
  }
}
