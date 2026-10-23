export { Publish }

import React, { useEffect, useRef, useState } from 'react'
import { keepReachable } from '../keepReachable'
import {
  onTextBroadcast,
  onBinaryBroadcastPair,
  onBinaryBroadcast,
  onBroadcastShieldClient,
  onBinaryBroadcastReusedBuffer,
  onBroadcastRightAfterSubscribe,
  onBroadcastAnswers,
  onBroadcastAnsweringLoop,
  onBroadcastWhileEstablishing,
  onSubscribeThrows,
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
  onBroadcastManyKeys,
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
  const reconnectRef = useRef<Awaited<ReturnType<typeof onBroadcastReconnect>> | null>(null)
  // A broadcast the page drops closes once garbage-collected.
  const listenedRef = useRef<unknown>(null)

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

      <h2>Held Past bufferLimit</h2>

      <button
        id="test-broadcast-hold-overflow"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onBroadcastWhileEstablishing()))
        }}
      >
        Publishes held past bufferLimit
      </button>

      <h2>Broadcast Shield Validation</h2>

      <button
        id="test-broadcast-shield"
        onClick={async () => {
          const { room, getReceived } = await onBroadcastShieldClient()
          await new Promise<void>((resolve) => room.onOpen(resolve))

          // Valid publish: matches `{ text: string }` — server validates, fans out, resolves with receipt.
          const validAck = await room.publish({ text: 'hi' })
          // A store like Redis delivers to the server's subscriber after publish() resolves.
          let validReceived = await getReceived()
          for (let poll = 0; poll < 50 && validReceived.length === 0; poll++) {
            await new Promise((r) => setTimeout(r, 200))
            validReceived = await getReceived()
          }
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

      <h2>Subscribe that throws</h2>

      <button
        id="test-subscribe-throws"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onSubscribeThrows()))
        }}
      >
        Subscribe refused by the store
      </button>

      <h2>Leave message from onClose</h2>

      <button
        id="test-chat-leave-on-close"
        onClick={async () => {
          setResult('')
          const { key, chat } = await onChatLeaveOnClose()
          await new Promise<void>((resolve) => chat.onOpen(resolve))
          await chat.close()
          for (let poll = 0; poll < 50; poll++) {
            const received = await onGetChatWatched(key)
            setResult(JSON.stringify({ received }))
            if (received.length > 0) break
            await new Promise((r) => setTimeout(r, 200))
          }
        }}
      >
        Close a chat channel
      </button>

      <h2>Server-side subscriptions</h2>

      <button
        id="test-server-broadcast-checks"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onServerBroadcastChecks()))
        }}
      >
        Abort, receivers, unsubscribe
      </button>

      <h2>Failing subscribers and publishes</h2>

      <button
        id="test-broadcast-errors"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onBroadcastErrors()))
        }}
      >
        Subscribers throw, the store fails
      </button>

      <h2>Binary Listeners Get Bytes of Their Own</h2>

      <button
        id="test-broadcast-own-bytes"
        onClick={async () => {
          setResult('')
          const { room, getReceived } = await onBroadcastOwnBytes()
          const received: Array<number[] | 'detached'> = []
          room.subscribeBinary((data) => void structuredClone(data.buffer, { transfer: [data.buffer] }))
          room.subscribeBinary((data) => void received.push(data.byteLength === 0 ? 'detached' : [...data]))
          try {
            await room.publishBinary(new Uint8Array([1, 2, 3]))
            for (let poll = 0; poll < 50; poll++) {
              const server = await getReceived()
              setResult(JSON.stringify({ page: received, server }))
              if (received.length >= 1 && server.length >= 2) break
              await new Promise((r) => setTimeout(r, 200))
            }
          } catch (err) {
            setResult(JSON.stringify({ error: String(err) }))
          }
          room.close()
        }}
      >
        Binary listeners get bytes of their own
      </button>
      <h2>Server Listener After Page Unsubscribe</h2>

      <button
        id="test-broadcast-server-listener"
        onClick={async () => {
          setResult('')
          const { room, getReceived } = await onBroadcastServerListener()
          const off = room.subscribe(() => {})
          const offBinary = room.subscribeBinary(() => {})
          await new Promise<void>((resolve) => room.onOpen(resolve))
          off()
          offBinary()
          await room.publish('after')
          await room.publishBinary(new Uint8Array(3))
          for (let poll = 0; poll < 50; poll++) {
            const received = await getReceived()
            setResult(JSON.stringify({ received }))
            if (received.length >= 2) break
            await new Promise((r) => setTimeout(r, 200))
          }
        }}
      >
        Server listener after page unsubscribe
      </button>

      <h2>onOpen Publish</h2>

      <button
        id="test-broadcast-on-open-publish"
        onClick={async () => {
          setResult('')
          const room = await onBroadcastOnOpenPublish()
          listenedRef.current = room
          const received: string[] = []
          room.subscribe((msg) => {
            received.push(msg)
            setResult(JSON.stringify({ received }))
          })
        }}
      >
        onOpen publishes
      </button>

      <h2>Listener Answers</h2>

      <button
        id="test-broadcast-answer-order"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify({ seen: await onBroadcastAnswerOrder() }))
        }}
      >
        Listener answers a message
      </button>

      <button
        id="test-broadcast-self-answer"
        onClick={async () => {
          setResult('')
          const { stop } = await onBroadcastSelfAnswer()
          setResult(JSON.stringify({ answers: await stop() }))
        }}
      >
        Listeners answer their own key
      </button>

      <h2>Static Publish Receipts</h2>

      <button
        id="test-broadcast-static-receipts"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onStaticPublishReceipts()))
        }}
      >
        Broadcast.publish() receipts
      </button>

      <h2>Seqs</h2>

      <button
        id="test-broadcast-seq-across-subscribers"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify({ seqs: await onBroadcastSeqAcrossSubscribers() }))
        }}
      >
        Seqs across subscribers
      </button>
      {([false, true] as const).map((subscribed) => (
        <button
          key={String(subscribed)}
          id={`test-broadcast-many-keys${subscribed ? '-subscribed' : ''}`}
          onClick={async () => {
            setResult('')
            await onBroadcastManyKeys(100_000, subscribed)
            setResult(JSON.stringify({ done: true }))
          }}
        >
          100,000 keys{subscribed ? ', each subscribed' : ''}
        </button>
      ))}

      <h2>Subscriber Unsubscribes Itself</h2>

      <button
        id="test-broadcast-self-unsubscribe"
        onClick={async () => {
          setResult('')
          const { room, publish, getServerSeen } = await onBroadcastSelfUnsubscribe()
          listenedRef.current = room
          const pageSeen: string[] = []
          const off = room.subscribe((msg) => {
            pageSeen.push(`once:${msg}`)
            off()
          })
          room.subscribe((msg) => {
            pageSeen.push(`other:${msg}`)
          })
          await new Promise<void>((resolve) => room.onOpen(resolve))
          await publish()
          for (let poll = 0; poll < 50; poll++) {
            const serverSeen = await getServerSeen()
            setResult(JSON.stringify({ pageSeen, serverSeen }))
            if (pageSeen.length >= 3 && serverSeen.length >= 3) break
            await new Promise((r) => setTimeout(r, 200))
          }
        }}
      >
        Subscriber unsubscribes itself
      </button>

      <h2>Subscribe Across a Reconnect</h2>

      <button
        id="test-broadcast-reconnect-open"
        onClick={async () => {
          setResult('')
          const opened = await onBroadcastReconnect()
          reconnectRef.current = opened
          opened.room.onOpen(() => setResult(JSON.stringify({ open: true, received: [] })))
        }}
      >
        Open broadcast
      </button>
      <button
        id="test-broadcast-reconnect-subscribe"
        onClick={() => {
          const received: string[] = []
          reconnectRef.current!.room.subscribe((msg) => {
            received.push(msg)
            setResult(JSON.stringify({ open: true, received }))
          })
        }}
      >
        Subscribe
      </button>
      <button id="test-broadcast-reconnect-publish" onClick={() => reconnectRef.current!.publish('after-reconnect')}>
        Server publishes
      </button>

      <h2>Published While Offline</h2>

      {[2, 8].map((count) => (
        <button
          key={count}
          id={`test-broadcast-while-offline-${count}`}
          onClick={async () => {
            setResult('')
            const { key, room } = await onBroadcastWhileOffline(count)
            listenedRef.current = room
            const received: string[] = []
            let closed: string | null = null
            const render = () => setResult(JSON.stringify({ key, received, closed }))
            room.subscribe((msg) => {
              received.push(msg.trim())
              render()
            })
            room.onClose((err) => {
              closed = err?.name ?? 'none'
              render()
            })
            await room.publish('before')
          }}
        >
          {count} × 128 KiB published while offline
        </button>
      ))}
    </div>
  )
}
