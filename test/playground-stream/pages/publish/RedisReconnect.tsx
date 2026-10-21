export { RedisReconnect }

import React, { useState } from 'react'
import { onBinaryBroadcastAcrossReconnect } from './RedisReconnect.telefunc'

function RedisReconnect() {
  const [firstBytes, setFirstBytes] = useState<number[]>([])

  return (
    <div>
      <h2>Binary Publish Across a Redis Reconnect</h2>
      <pre id="redis-reconnect-result">{JSON.stringify(firstBytes)}</pre>
      <button
        type="button"
        id="test-redis-reconnect"
        onClick={async () => {
          setFirstBytes([])
          const room = await onBinaryBroadcastAcrossReconnect()
          room.subscribeBinary((data) => setFirstBytes((bytes) => [...bytes, data[0]!]))
          await room.publish({ start: true })
        }}
      >
        Server publishes 3 binary frames while Redis reconnects
      </button>
    </div>
  )
}
