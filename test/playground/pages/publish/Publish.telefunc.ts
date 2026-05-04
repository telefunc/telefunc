export { onTextBroadcast, onBinaryBroadcastPair, onBinaryBroadcast, onBroadcastShieldClient }

import { Broadcast } from 'telefunc'

type TextMsg = { text: string; from: string }

/** Two broadcast instances sharing a key — publish from one, subscribe on the other. */
async function onTextBroadcast() {
  const publisher = new Broadcast<TextMsg>({ key: 'room:text-test' })
  const subscriber = new Broadcast<TextMsg>({ key: 'room:text-test' })

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

/** Two broadcast instances sharing a key — publishBinary from one, subscribeBinary on the other. */
async function onBinaryBroadcastPair() {
  const publisher = new Broadcast({ key: 'room:binary-test' })
  const subscriber = new Broadcast({ key: 'room:binary-test' })

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

/** Shield validates client-published data on a `Broadcast<{ text: string }>`:
 *  - valid `{ text: string }` reaches subscribers and resolves with a publish receipt.
 *  - invalid payload rejects the client's `publish()` with a shield error and is not delivered. */
async function onBroadcastShieldClient() {
  const room = new Broadcast<{ text: string }>({ key: `shield-test:${crypto.randomUUID()}` })
  const received: Array<{ text: string }> = []
  room.subscribe((msg) => {
    received.push(msg)
  })
  return { room, getReceived: async () => received }
}

/** Client subscribes to binary broadcast, server publishes frames. Tests client-side subscribeBinary. */
async function onBinaryBroadcast() {
  const room = new Broadcast({ key: 'room:broadcast-test' })

  // Server publishes 5 binary frames after a short delay
  room.onOpen(() => {
    let i = 0
    const interval = setInterval(() => {
      if (i >= 5) {
        clearInterval(interval)
        return
      }
      const data = new Uint8Array(64).fill(i + 1)
      room.publishBinary(data)
      i++
    }, 100)
  })

  return room
}
