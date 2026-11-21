export { RedisReconnect }

import React, { useRef, useState } from 'react'
import {
  onBinaryBroadcastAcrossReconnect,
  onRedisSubscribeFailure,
  onPublishBesideRefusedSubscribe,
  onPublishAcrossSeqBoundary,
} from './RedisReconnect.telefunc'

function RedisReconnect() {
  const [readiness, setReadiness] = useState<unknown>(null)
  const [beside, setBeside] = useState<unknown>(null)
  const [firstBytes, setFirstBytes] = useState<number[]>([])
  const [seqBoundary, setSeqBoundary] = useState<unknown>(null)
  // A broadcast the page drops closes once garbage-collected.
  const roomRef = useRef<Awaited<ReturnType<typeof onBinaryBroadcastAcrossReconnect>> | null>(null)

  return (
    <div>
      <pre id="redis-readiness-result">{JSON.stringify(readiness)}</pre>
      <button
        type="button"
        id="test-redis-readiness"
        onClick={async () => setReadiness(await onRedisSubscribeFailure())}
      >
        Fail and restore Redis subscriptions
      </button>
      <pre id="redis-beside-refused-result">{JSON.stringify(beside)}</pre>
      <button
        type="button"
        id="test-redis-beside-refused"
        onClick={async () => setBeside(await onPublishBesideRefusedSubscribe())}
      >
        Publish on one key while Redis refuses another key's subscription
      </button>
      <pre id="redis-seq-boundary-result">{JSON.stringify(seqBoundary)}</pre>
      <button
        type="button"
        id="test-redis-seq-boundary"
        onClick={async () => setSeqBoundary(await onPublishAcrossSeqBoundary())}
      >
        Publish across a key's 2^32nd sequence number in Redis
      </button>
      <h2>Binary Publish Across a Redis Reconnect</h2>
      <pre id="redis-reconnect-result">{JSON.stringify(firstBytes)}</pre>
      <button
        type="button"
        id="test-redis-reconnect"
        onClick={async () => {
          setFirstBytes([])
          const room = await onBinaryBroadcastAcrossReconnect()
          roomRef.current = room
          room.subscribeBinary((data) => setFirstBytes((bytes) => [...bytes, data[0]!]))
          await room.publish({ start: true })
        }}
      >
        Server publishes 3 binary frames while Redis reconnects
      </button>
    </div>
  )
}
