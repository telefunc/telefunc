export {
  onTextBroadcast,
  onBinaryBroadcastPair,
  onBinaryBroadcast,
  onBroadcastShieldClient,
  onBinaryBroadcastOwnBytes,
  onBinaryBroadcastReusedBuffer,
}

import { Buffer } from 'node:buffer'
import { Broadcast, BroadcastChannel } from 'telefunc'

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
