export { Publish }

import React, { useEffect, useState } from 'react'
import { onTextBroadcast, onBinaryBroadcastPair, onBinaryBroadcast, onBroadcastShieldClient } from './Publish.telefunc'

type ShieldState = {
  validReceiptKey: string | null
  validReceived: Array<{ text: string }> | null
  invalidThrew: boolean | null
  invalidErrorMessage: string | null
  receivedAfterInvalid: Array<{ text: string }> | null
}

function Publish() {
  const [result, setResult] = useState<string>('')
  const [shieldState, setShieldState] = useState<ShieldState>({
    validReceiptKey: null,
    validReceived: null,
    invalidThrew: null,
    invalidErrorMessage: null,
    receivedAfterInvalid: null,
  })
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="publish-result">{result}</pre>
      <pre id="publish-shield-state">{JSON.stringify(shieldState)}</pre>

      <h2>Text Pub/Sub</h2>

      <button
        id="test-text-broadcast"
        onClick={async () => {
          setResult('')
          const { publisher, subscriber, getReceived } = await onTextBroadcast()
          // Publish 3 messages from the publisher
          const acks = []
          for (let i = 0; i < 3; i++) {
            const ack = await publisher.publish({ text: `msg-${i}`, from: 'client' })
            acks.push({ seq: ack.seq, key: ack.key })
          }
          // Render every poll so the e2e autoRetry sees fresh data on each iteration.
          // A one-shot snapshot freezes the DOM and turns slow delivery into a
          // guaranteed test failure.
          for (let poll = 0; poll < 50; poll++) {
            const received = await getReceived()
            setResult(JSON.stringify({ acks, received }))
            if (received.length >= 3) break
            await new Promise((r) => setTimeout(r, 200))
          }
          // `subscriber` is unused but must stay reachable until the scenario is over:
          // it crosses the wire as a remote handle, and a GC pass would close it,
          // tearing down the server-side subscription mid-test.
          void subscriber
        }}
      >
        Text publish (3 messages)
      </button>

      <h2>Binary Pub/Sub</h2>

      <button
        id="test-binary-broadcast-pair"
        onClick={async () => {
          setResult('')
          const { publisher, subscriber, getReceived } = await onBinaryBroadcastPair()
          // Publish 3 binary frames
          const acks = []
          for (let i = 0; i < 3; i++) {
            const data = new Uint8Array(128).fill(i + 10)
            const ack = await publisher.publishBinary(data)
            acks.push({ seq: ack.seq, key: ack.key })
          }
          // Same poll-and-render pattern as the text broadcast above.
          for (let poll = 0; poll < 50; poll++) {
            const received = await getReceived()
            setResult(JSON.stringify({ acks, received }))
            if (received.length >= 3) break
            await new Promise((r) => setTimeout(r, 200))
          }
          // Keep the unused remote handle reachable, see the text broadcast above.
          void subscriber
        }}
      >
        Binary publish (3 frames)
      </button>

      <h2>Client Subscribe Binary</h2>

      <button
        id="test-binary-broadcast"
        onClick={async () => {
          setResult('')
          const room = await onBinaryBroadcast()
          const received: Array<{ size: number; firstByte: number }> = []
          room.subscribeBinary((data, info) => {
            received.push({ size: data.byteLength, firstByte: data[0]! })
            setResult(JSON.stringify({ received, done: received.length >= 5 }))
          })
          await room.publish({ start: true })
        }}
      >
        Server broadcasts 5 binary frames
      </button>

      <h2>Broadcast Shield Validation</h2>

      <button
        id="test-broadcast-shield"
        onClick={async () => {
          const { room, getReceived } = await onBroadcastShieldClient()
          await new Promise<void>((resolve) => room.onOpen(resolve))

          // Valid publish: matches `{ text: string }` — server validates, fans out, resolves with receipt.
          const validAck = await room.publish({ text: 'hi' })
          // Brief delay so the subscribe callback runs.
          await new Promise((r) => setTimeout(r, 100))
          const validReceived = await getReceived()
          setShieldState((s) => ({
            ...s,
            validReceiptKey: validAck.key,
            validReceived,
          }))

          // Invalid publish: wrong type — server shield rejects, publish() rejects with ShieldValidationError.
          let threw = false
          let errorMessage: string | null = null
          try {
            await (room.publish as any)({ text: 42 })
          } catch (err: any) {
            threw = true
            errorMessage = err?.message ?? 'unknown'
          }
          await new Promise((r) => setTimeout(r, 100))
          const receivedAfterInvalid = await getReceived()
          setShieldState((s) => ({
            ...s,
            invalidThrew: threw,
            invalidErrorMessage: errorMessage,
            receivedAfterInvalid,
          }))
        }}
      >
        Broadcast Shield
      </button>
    </div>
  )
}
