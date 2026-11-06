export { Publish }

import { prepareBroadcastApi } from './broadcastApiProbe'
import { close } from 'telefunc/client'
import React, { useEffect, useRef, useState } from 'react'
import { keepReachable } from '../keepReachable'
import {
  onTextBroadcast,
  onBroadcastApi,
  onBroadcastBacklog,
  onInMemoryPublishes,
  onBroadcastBurstThenClose,
  onBinaryBroadcastPair,
  onBinaryBroadcast,
  onBroadcastShieldClient,
  onBinaryBroadcastReusedBuffer,
  onBroadcastRightAfterSubscribe,
  onBroadcastAnswers,
  onBroadcastAnsweringLoop,
  onBroadcastWhileEstablishing,
  onSubscribeRefused,
  onDeliveredBeforeReady,
  onJoinedBeforeReady,
  onDeliveredWhileLost,
  onChatLeaveOnClose,
  onGetChatWatched,
  onServerBroadcastChecks,
  onBroadcastErrors,
  onBroadcastServerListener,
  onBroadcastOwnBytes,
  onBroadcastOnOpenPublish,
  onBroadcastSubscriberAborts,
  onBroadcastLoopLeftBehind,
  onBroadcastAnswerOrder,
  onBroadcastSelfAnswer,
  onStaticPublishReceipts,
  onBroadcastSelfUnsubscribe,
  onBroadcastReconnect,
  onBroadcastWhileOffline,
  onBroadcastSeqAcrossSubscribers,
  onBroadcastJoinWhileQueued,
  onBroadcastManyKeys,
  onBroadcastThrowingListener,
  onJoinChat,
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
  const backlogRef = useRef<Awaited<ReturnType<typeof onBroadcastBacklog>> | null>(null)

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="publish-result">{result}</pre>
      <pre id="publish-shield-state">{JSON.stringify(shieldState)}</pre>

      <button
        id="test-broadcast-api"
        onClick={async () => {
          try {
            const remote = await onBroadcastApi()
            const { room } = remote
            await room.opened
            const local = prepareBroadcastApi(room)
            const receipts = [await room.publish('first'), await room.publishBinary(new Uint8Array([2]))]
            const client = await local.stop()
            const server = await remote.stop()
            await room.publish('after')
            const snapshot = await remote.snapshot()
            const open = !room.isClosed
            const ending = room.messages().next()
            await room.close()
            const ended = (await ending).done
            await room.closed
            close(remote)
            setResult(JSON.stringify({ client, server, snapshot, receipts, open, ended }))
          } catch (err) {
            setResult(String(err))
          }
        }}
      >
        Broadcast iteration and signals
      </button>

      <button
        id="test-broadcast-slow-loop"
        onClick={async () => {
          try {
            const remote = await onBroadcastBacklog('slow')
            backlogRef.current = remote
            const { room } = remote
            await room.opened
            let received = 0
            const loop = (async () => {
              try {
                for await (const _ of room.messages()) {
                  received++
                  await new Promise((resolve) => setTimeout(resolve, 20))
                }
                return null
              } catch (err) {
                return (err as Error).name
              }
            })()
            // On the wire after the loop's subscribe, so the server has the subscription before the flood.
            await room.publish('ready')
            await remote.flood(2000)
            const error = await loop
            setResult(JSON.stringify({ error, received, server: await remote.serverLoop() }))
          } catch (err) {
            setResult(String(err))
          }
        }}
      >
        A slow loop over a busy broadcast
      </button>
      <button
        id="test-broadcast-overflow-then-close"
        onClick={async () => {
          try {
            const remote = await onBroadcastBacklog('held')
            const { room } = remote
            await room.opened
            const closed = room.closed.then(
              () => 'clean',
              (err) => (err as Error).name,
            )
            let received = 0
            const loop = (async () => {
              try {
                for await (const _ of room.messages()) {
                  received++
                  // Holds its first message until the server closed the broadcast, after the loop fell behind.
                  if (received === 1) await closed
                }
                return null
              } catch (err) {
                return (err as Error).name
              }
            })()
            await room.publish('ready')
            await remote.flood(2000)
            await new Promise((resolve) => setTimeout(resolve, 1000))
            await remote.close()
            const error = await loop
            setResult(JSON.stringify({ error, received, closed: await closed, server: await remote.serverLoop() }))
          } catch (err) {
            setResult(String(err))
          }
        }}
      >
        A held loop over a busy broadcast that then closes
      </button>
      <button
        id="test-broadcast-burst-then-close"
        onClick={async () => {
          try {
            setResult(JSON.stringify(await onBroadcastBurstThenClose()))
          } catch (err) {
            setResult(String(err))
          }
        }}
      >
        A server loop over a burst published and closed in one task
      </button>
      <button
        id="test-in-memory-publishes"
        onClick={async () => {
          try {
            setResult(JSON.stringify(await onInMemoryPublishes()))
          } catch (err) {
            setResult(String(err))
          }
        }}
      >
        In-memory publishes: a loop, a listener's batch, a chain of answers
      </button>
      <button
        id="test-broadcast-after-overflow"
        onClick={async () => {
          const remote = backlogRef.current!
          // On the wire after the ended loop's unsubscribe, so the server has that before this flood.
          await remote.room.publish('after')
          await remote.flood(400)
          await new Promise((resolve) => setTimeout(resolve, 1000))
          close(remote)
          setResult('flooded')
        }}
      >
        Publish after the slow loop ended
      </button>
      <button
        id="test-broadcast-keeps-up"
        onClick={async () => {
          try {
            const remote = await onBroadcastBacklog(null)
            const { room } = remote
            await room.opened
            const seen: number[] = []
            const loop = (async () => {
              for await (const message of room.messages()) {
                if (message.type !== 'text' || message.data === 'ready') continue
                seen.push(Number(message.data.split(':')[0]))
                if (seen.length === 600) break
              }
            })()
            await room.publish('ready')
            await remote.publishAwaited(600)
            await loop
            close(remote)
            setResult(JSON.stringify({ received: seen.length, inOrder: seen.every((n, i) => n === i) }))
          } catch (err) {
            setResult(String(err))
          }
        }}
      >
        A loop that keeps up with a broadcast
      </button>

      <button
        id="test-broadcast-publish-failures"
        onClick={async () => {
          const outcomes = []
          for (const binary of [false, true]) {
            for (const caught of [false, true]) {
              const remote = await onBroadcastServerListener()
              const { room } = remote
              const pending = binary ? room.publishBinary(new Uint8Array([1])) : room.publish('failure')
              room.abort('stop')
              if (caught) outcomes.push(await pending.catch((err) => err.abortValue))
              else outcomes.push('ignored')
              await room.closed.catch(() => {})
              close(remote)
            }
          }
          setResult(JSON.stringify(outcomes))
        }}
      >
        Observe or ignore publish failures
      </button>

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

      <h2>Held Past 2 MiB</h2>

      <button
        id="test-broadcast-hold-overflow"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onBroadcastWhileEstablishing(3, 900_000)))
        }}
      >
        Publishes held past 2 MiB
      </button>
      <button
        id="test-broadcast-hold-alone"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onBroadcastWhileEstablishing(2, 2_300_000)))
        }}
      >
        One publish held, itself past 2 MiB
      </button>

      <h2>Broadcast Shield Validation</h2>

      <button
        id="test-broadcast-shield"
        onClick={async () => {
          const { room, getReceived } = await onBroadcastShieldClient()
          await room.opened

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

      <h2>Subscribe the store refuses</h2>

      <button
        id="test-subscribe-refused"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onSubscribeRefused()))
        }}
      >
        Subscribe refused by the store
      </button>

      <button
        id="test-delivered-before-ready"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify({ seen: await onDeliveredBeforeReady() }))
        }}
      >
        Message delivered before the store acknowledges the subscription
      </button>

      <button
        id="test-joined-before-ready"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onJoinedBeforeReady()))
        }}
      >
        Listener joining between messages delivered before the store acknowledges the subscription
      </button>

      <button
        id="test-delivered-while-lost"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify({ seen: await onDeliveredWhileLost() }))
        }}
      >
        Messages delivered while the store restores a lost subscription
      </button>

      <h2>Leave message from onClose</h2>

      <button
        id="test-chat-leave-on-close"
        onClick={async () => {
          setResult('')
          const { key, chat } = await onChatLeaveOnClose()
          await chat.opened
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
        Abort, unsubscribe
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
          await room.opened
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

      <h2>Subscriber Ends</h2>

      <button
        id="test-broadcast-subscriber-aborts"
        onClick={async () => {
          setResult('')
          const { aborting, room } = await onBroadcastSubscriberAborts()
          listenedRef.current = room
          aborting.subscribe(() => {})
          const received: string[] = []
          room.subscribe((msg) => received.push(msg))
          const { seq } = await room.publish('abort')
          setResult(JSON.stringify({ seq, received }))
        }}
      >
        A subscriber aborts its channel
      </button>

      <button
        id="test-broadcast-loop-left-behind"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onBroadcastLoopLeftBehind()))
        }}
      >
        A loop stops reading before its channel closes
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

      <h2>Throwing Listener</h2>

      <button
        id="test-broadcast-throwing-listener"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify(await onBroadcastThrowingListener()))
        }}
      >
        A listener throws, the others get the message
      </button>

      <h2>Chat</h2>

      <button
        id="test-chat"
        onClick={async () => {
          setResult('')
          const room = await onJoinChat()
          listenedRef.current = room
          const messages: string[] = []
          let avatarLoaded = false
          const loadAvatar = async () => {
            if (!avatarLoaded) {
              avatarLoaded = true
              throw new Error('Could not load the avatar')
            }
          }
          const markChatAsRead = async () => {
            throw new Error('Could not mark the chat as read')
          }
          room.onOpen(async () => {
            await markChatAsRead()
          })
          room.subscribe(async (message) => {
            await loadAvatar()
            messages.push(message)
            setResult(JSON.stringify({ messages }))
          })
          await room.opened
          await room.publish('hello')
          await room.publish('how are you?')
        }}
      >
        Join the chat
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
      <button
        id="test-broadcast-join-while-queued"
        onClick={async () => {
          setResult('')
          setResult(JSON.stringify({ received: await onBroadcastJoinWhileQueued() }))
        }}
      >
        Subscribe while publishes queue
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
          await room.opened
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

      {[2, 32].map((count) => (
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
