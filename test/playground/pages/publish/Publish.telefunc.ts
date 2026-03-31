export { onTextPubSub, onBinaryPubSub, onBinaryBroadcast }

import { pubsub } from 'telefunc'

type TextMsg = { text: string; from: string }

/** Two pubsub instances sharing a key — publish from one, subscribe on the other. */
async function onTextPubSub() {
  const publisher = pubsub<TextMsg>('room:text-test')
  const subscriber = pubsub<TextMsg>('room:text-test')

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

/** Two pubsub instances sharing a key — publishBinary from one, subscribeBinary on the other. */
async function onBinaryPubSub() {
  const publisher = pubsub('room:binary-test')
  const subscriber = pubsub('room:binary-test')

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

/** Client subscribes to binary pubsub, server publishes frames. Tests client-side subscribeBinary. */
async function onBinaryBroadcast() {
  const ps = pubsub('room:broadcast-test')

  // Server publishes 5 binary frames after a short delay
  ps.onOpen(() => {
    let i = 0
    const interval = setInterval(() => {
      if (i >= 5) {
        clearInterval(interval)
        return
      }
      const data = new Uint8Array(64).fill(i + 1)
      ps.publishBinary(data)
      i++
    }, 100)
  })

  return ps
}
