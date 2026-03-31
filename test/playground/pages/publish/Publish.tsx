export { Publish }

import React, { useEffect, useState } from 'react'
import { onTextPubSub, onBinaryPubSub, onBinaryBroadcast } from './Publish.telefunc'

function Publish() {
  const [result, setResult] = useState<string>('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="publish-result">{result}</pre>

      <h2>Text Pub/Sub</h2>

      <button
        id="test-text-pubsub"
        onClick={async () => {
          setResult('')
          const { publisher, getReceived } = await onTextPubSub()
          // Publish 3 messages from the publisher
          const acks = []
          for (let i = 0; i < 3; i++) {
            const ack = await publisher.publish({ text: `msg-${i}`, from: 'client' })
            acks.push({ seq: ack.seq, key: ack.key })
          }
          // Small delay for delivery
          await new Promise((r) => setTimeout(r, 200))
          const received = await getReceived()
          setResult(JSON.stringify({ acks, received }))
        }}
      >
        Text publish (3 messages)
      </button>

      <h2>Binary Pub/Sub</h2>

      <button
        id="test-binary-pubsub"
        onClick={async () => {
          setResult('')
          const { publisher, getReceived } = await onBinaryPubSub()
          // Publish 3 binary frames
          const acks = []
          for (let i = 0; i < 3; i++) {
            const data = new Uint8Array(128).fill(i + 10)
            const ack = await publisher.publishBinary(data)
            acks.push({ seq: ack.seq, key: ack.key })
          }
          await new Promise((r) => setTimeout(r, 200))
          const received = await getReceived()
          setResult(JSON.stringify({ acks, received }))
        }}
      >
        Binary publish (3 frames)
      </button>

      <h2>Client Subscribe Binary</h2>

      <button
        id="test-binary-broadcast"
        onClick={async () => {
          setResult('')
          const ps = await onBinaryBroadcast()
          const received: Array<{ size: number; firstByte: number }> = []
          ps.subscribeBinary((data, info) => {
            received.push({ size: data.byteLength, firstByte: data[0]! })
            setResult(JSON.stringify({ received, done: received.length >= 5 }))
          })
        }}
      >
        Server broadcasts 5 binary frames
      </button>
    </div>
  )
}
