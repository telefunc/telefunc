export { RedisReconnect }

import React, { useRef, useState } from 'react'
import {
  onBinaryBroadcastAcrossReconnect,
  onBroadcastPublishedDuringResume,
  onBroadcastSubscriberDropped,
  onRedisChannelDenied,
  onRedisSubscribeFailure,
} from './RedisReconnect.telefunc'

function RedisReconnect() {
  const [readiness, setReadiness] = useState<unknown>(null)
  const [channelDenied, setChannelDenied] = useState<unknown>(null)
  const [firstBytes, setFirstBytes] = useState<number[]>([])
  const [dropped, setDropped] = useState<unknown>(null)
  const [duringResume, setDuringResume] = useState<unknown>(null)
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
      <pre id="redis-channel-denied-result">{JSON.stringify(channelDenied)}</pre>
      <button
        type="button"
        id="test-redis-channel-denied"
        onClick={async () => setChannelDenied(await onRedisChannelDenied())}
      >
        Redis denies one channel next to allowed ones
      </button>
      <h2>Publishes Across a Dropped Subscriber</h2>
      <pre id="redis-subscriber-dropped-result">{JSON.stringify(dropped)}</pre>
      <button
        type="button"
        id="test-redis-subscriber-dropped"
        onClick={async () => setDropped(await onBroadcastSubscriberDropped())}
      >
        Drop the server's Redis subscriber connection during publishes
      </button>
      <h2>A Publish While a Reconnected Subscription Reads Its Start</h2>
      <pre id="redis-during-resume-result">{JSON.stringify(duringResume)}</pre>
      <button
        type="button"
        id="test-redis-during-resume"
        onClick={async () => setDuringResume(await onBroadcastPublishedDuringResume())}
      >
        Publish while the start is read back
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
