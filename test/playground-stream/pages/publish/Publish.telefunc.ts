export {
  onTextBroadcast,
  onBroadcastApi,
  onBroadcastBacklog,
  onInMemoryPublishes,
  onBroadcastBurstThenClose,
  onBinaryBroadcastPair,
  onBinaryBroadcast,
  onBroadcastShieldClient,
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
  onBroadcastSeqWhileQueued,
  onBroadcastManyKeys,
  onBroadcastThrowingListener,
  onJoinChat,
}

import { Broadcast, BroadcastChannel } from 'telefunc'
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

/** A listener's in-memory publishes queue. It publishes a key nobody subscribes to, then another key, then a subscriber
 *  comes before the queue drains, and the key is published again. */
async function onBroadcastSeqWhileQueued() {
  const key = `room:seq-queued:${crypto.randomUUID()}`
  const trigger = `room:seq-trigger:${crypto.randomUUID()}`
  const received: Array<[string, number]> = []
  const sends: ReturnType<typeof Broadcast.publish>[] = []
  let off = () => {}
  const offTrigger = Broadcast.subscribe(trigger, () => {
    sends.push(Broadcast.publish(key, 'first'))
    void Broadcast.publish(`room:seq-other:${crypto.randomUUID()}`, 0)
    off = Broadcast.subscribe<string>(key, (message, info) => received.push([message, info.seq]))
    sends.push(Broadcast.publish(key, 'second'))
  })
  await Broadcast.publish(trigger, 'go')
  const receipts = [(await sends[0]!).seq, (await sends[1]!).seq]
  off()
  offTrigger()
  return { receipts, received }
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

/** A key's static listeners, the first throwing and the second rejecting, then one that records, then a `BroadcastChannel`'s. */
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
  for (let i = 0; i < 2000; i++) await room.publish(`${i}:${'x'.repeat(2048)}`)
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
  const pastTheBound = await listenerPublishes(6000)

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
