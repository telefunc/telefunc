export { onBinaryBroadcastAcrossReconnect }

import { BroadcastChannel } from 'telefunc'
import { redis } from '../../redis'

/** On the page's `start`, the server's Redis connection drops, and it publishes 3 binary frames from one reused buffer
 *  while ioredis reconnects. */
async function onBinaryBroadcastAcrossReconnect() {
  const room = new BroadcastChannel<{ start: true }>({ key: `room:reconnect-test:${crypto.randomUUID()}` })
  room.subscribe(() => {
    redis!.disconnect(true)
    const bytes = new Uint8Array(64)
    for (let i = 1; i <= 3; i++) void room.publishBinary(bytes.fill(i))
  })
  return room
}
