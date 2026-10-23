export {
  onTextBroadcast,
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

import { Broadcast, BroadcastChannel, onBug } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { onServerAction } from '../../server-actions'

type TextMsg = { text: string; from: string }

onBug((err) => {
  cleanupState[`bug:${err instanceof Error ? err.message : String(err)}`] = 'reported'
})

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

/** Past the 1,024 in-memory deliveries of one turn, publishes queue. A key published while nobody subscribes, then
 *  another key, then a subscriber comes before the queue drains, and the key is published again. */
async function onBroadcastSeqWhileQueued() {
  const key = `room:seq-queued:${crypto.randomUUID()}`
  for (let i = 0; i < 2_000; i++) void Broadcast.publish(`room:busy:${crypto.randomUUID()}`, i)
  const first = Broadcast.publish(key, 'first')
  void Broadcast.publish(`room:seq-other:${crypto.randomUUID()}`, 0)
  const received: Array<[string, number]> = []
  const off = Broadcast.subscribe<string>(key, (message, info) => received.push([message, info.seq]))
  const second = Broadcast.publish(key, 'second')
  const receipts = [(await first).seq, (await second).seq]
  off()
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

/** A chat room whose subscriber saves each message. */
async function onJoinChat() {
  const room = new BroadcastChannel<string>({ key: `chat:${crypto.randomUUID()}` })
  const database = connectToDatabase()
  room.subscribe(async (message) => {
    await database.saveMessage(message)
  })
  return room
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
