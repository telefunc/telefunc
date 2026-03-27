export { onTextPubSub, onBinaryPubSub, onBinaryBroadcast }

import { createChannel } from 'telefunc'

type TextMsg = { text: string; from: string }

/** Two channels sharing a key — publish from one, subscribe on the other. */
async function onTextPubSub() {
  const publisher = createChannel<(msg: TextMsg) => void, (msg: TextMsg) => void>({ key: 'room:text-test' })
  const subscriber = createChannel<(msg: TextMsg) => void, (msg: TextMsg) => void>({ key: 'room:text-test' })

  const received: Array<{ text: string; from: string; seq: number }> = []
  subscriber.subscribe((msg, info) => {
    received.push({ text: msg.text, from: msg.from, seq: info.seq })
  })

  return {
    publisherChannel: publisher.client,
    subscriberChannel: subscriber.client,
    getReceived: () => received,
  }
}

/** Two channels sharing a key — publishBinary from one, subscribeBinary on the other. */
async function onBinaryPubSub() {
  const publisher = createChannel({ key: 'room:binary-test' })
  const subscriber = createChannel({ key: 'room:binary-test' })

  const received: Array<{ size: number; firstByte: number; seq: number }> = []
  subscriber.subscribeBinary((data, info) => {
    received.push({ size: data.byteLength, firstByte: data[0]!, seq: info.seq })
  })

  return {
    publisherChannel: publisher.client,
    subscriberChannel: subscriber.client,
    getReceived: () => received,
  }
}

/** Client subscribes to binary pubsub, server publishes frames. Tests client-side subscribeBinary. */
async function onBinaryBroadcast() {
  const channel = createChannel({ key: 'room:broadcast-test' })

  // Server publishes 5 binary frames after a short delay
  channel.onOpen(() => {
    let i = 0
    const interval = setInterval(() => {
      if (i >= 5) {
        clearInterval(interval)
        return
      }
      const data = new Uint8Array(64).fill(i + 1)
      channel.publishBinary(data)
      i++
    }, 100)
  })

  return { channel: channel.client }
}
