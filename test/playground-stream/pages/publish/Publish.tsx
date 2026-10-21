export { Publish }

import React, { useEffect, useState } from 'react'
import { keepReachable } from '../keepReachable'
import {
  onTextBroadcast,
  onBinaryBroadcastPair,
  onBinaryBroadcast,
  onBroadcastShieldClient,
  onBinaryBroadcastOwnBytes,
  onBinaryBroadcastReusedBuffer,
  onBroadcastRightAfterSubscribe,
  onBroadcastAnswers,
  onBroadcastAnsweringLoop,
} from './Publish.telefunc'

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
          // `subscriber` is a remote handle a GC pass would close, tearing down the server-side subscription mid-test.
          const releaseSubscriber = keepReachable(subscriber)
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
          releaseSubscriber()
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
          // Keep the unused remote handle, see the text broadcast above.
          const releaseSubscriber = keepReachable(subscriber)
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
          releaseSubscriber()
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
          // A GC pass would close `room` and its subscription before the server's frames arrive.
          const releaseRoom = keepReachable(room)
          const received: Array<{ size: number; firstByte: number }> = []
          room.subscribeBinary((data, info) => {
            received.push({ size: data.byteLength, firstByte: data[0]! })
            setResult(JSON.stringify({ received, done: received.length >= 5 }))
          })
          await room.publish({ start: true })
          for (let poll = 0; poll < 50 && received.length < 5; poll++) await new Promise((r) => setTimeout(r, 200))
          releaseRoom()
        }}
      >
        Server broadcasts 5 binary frames
      </button>

      <h2>Bytes of Its Own</h2>

      <button
        id="test-binary-broadcast-own-bytes"
        onClick={async () => {
          setResult('')
          const { room, getSeen } = await onBinaryBroadcastOwnBytes()
          const releaseRoom = keepReachable(room)
          // The first listener detaches its buffer, as handing it to a worker does.
          room.subscribeBinary((data) => structuredClone(data.buffer, { transfer: [data.buffer] }))
          const received: number[][] = []
          room.subscribeBinary((data) => received.push([...data]))
          await room.publish({ start: true })
          for (let poll = 0; poll < 50; poll++) {
            const { seen, keySeen } = await getSeen()
            setResult(JSON.stringify({ received, seen, keySeen }))
            if (received.length >= 1 && seen.length >= 1 && keySeen.length >= 1) break
            await new Promise((r) => setTimeout(r, 200))
          }
          releaseRoom()
        }}
      >
        Listeners that write into or transfer their bytes
      </button>

      <h2>Reused Buffer</h2>

      <button
        id="test-binary-broadcast-reused-buffer"
        onClick={async () => {
          setResult('')
          const { room, getSeen } = await onBinaryBroadcastReusedBuffer()
          const releaseRoom = keepReachable(room)
          const received: number[][] = []
          room.subscribeBinary((data) => received.push([...data]))
          await room.publish({ start: true })
          for (let poll = 0; poll < 50; poll++) {
            const seen = await getSeen()
            setResult(JSON.stringify({ received, seen }))
            if (received.length >= 3 && seen.length >= 3) break
            await new Promise((r) => setTimeout(r, 200))
          }
          releaseRoom()
        }}
      >
        Server publishes from one reused buffer
      </button>

      <h2>Publish Right After Subscribe</h2>

      <button
        id="test-broadcast-right-after"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify({ seen: await onBroadcastRightAfterSubscribe() }))
        }}
      >
        Subscribe, then publish at once
      </button>

      <h2>Answers</h2>

      <button
        id="test-broadcast-answers"
        onClick={async () => {
          setResult('')
          const room = await onBroadcastAnswers()
          const releaseRoom = keepReachable(room)
          const received: string[] = []
          room.subscribe((data) => received.push(data))
          await room.publish('ping')
          const loop = await onBroadcastAnsweringLoop()
          for (let poll = 0; poll < 50 && received.length < 3; poll++) await new Promise((r) => setTimeout(r, 200))
          setResult(JSON.stringify({ received, ...loop }))
          releaseRoom()
        }}
      >
        Listeners that answer
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
