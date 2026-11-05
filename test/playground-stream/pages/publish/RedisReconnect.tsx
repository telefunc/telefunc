export { RedisReconnect }

import React, { useRef, useState } from 'react'
import {
  onBinaryBroadcastAcrossReconnect,
  onBroadcastSubscriberDropped,
  onRedisSubscribeFailure,
} from './RedisReconnect.telefunc'

function RedisReconnect() {
  const [readiness, setReadiness] = useState<unknown>(null)
  const [firstBytes, setFirstBytes] = useState<number[]>([])
  const [dropped, setDropped] = useState<unknown>(null)
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
      <h2>Publishes Across a Dropped Subscriber</h2>
      <pre id="redis-subscriber-dropped-result">{JSON.stringify(dropped)}</pre>
      <button
        type="button"
        id="test-redis-subscriber-dropped"
        onClick={async () => {
          const result = onBroadcastSubscriberDropped()
          await new Promise((resolve) => setTimeout(resolve, 200))
          await fetch('/api/redis/drop-connections', { method: 'POST' })
          setDropped(await result)
        }}
      >
        Drop the server's Redis connections during publishes
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
