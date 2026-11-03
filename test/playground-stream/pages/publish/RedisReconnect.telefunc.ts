export { onBinaryBroadcastAcrossReconnect, onRedisSubscribeFailure }

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

/** Redis refuses the server's SUBSCRIBEs (its user loses the command) while it publishes on a new subscription; then
 *  the command is granted again, and a new subscription gets the next publish. */
async function onRedisSubscribeFailure() {
  const room = new BroadcastChannel<string>({ key: `room:subscribe-failure:${crypto.randomUUID()}` })
  const received: string[] = []
  await redis!.acl('SETUSER', 'default', '-subscribe')
  try {
    let unsubscribe = room.subscribe((value) => received.push(value))
    const pending = await Promise.allSettled([room.publish('text'), room.publishBinary(new Uint8Array(1))])
    const later = await Promise.allSettled([room.publish('later')])
    await redis!.acl('SETUSER', 'default', '+subscribe')
    unsubscribe()
    const delivered = new Promise<void>((resolve) => {
      unsubscribe = room.subscribe((value) => {
        received.push(value)
        resolve()
      })
    })
    await room.publish('recovered')
    await delivered
    unsubscribe()
    return { rejected: [...pending, ...later].every((result) => result.status === 'rejected'), received }
  } finally {
    await redis!.acl('SETUSER', 'default', '+subscribe')
  }
}
