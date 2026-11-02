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

async function onRedisSubscribeFailure() {
  const { RedisTransport } = await import('@telefunc/redis')
  const user = `subscribe-test-${crypto.randomUUID()}`
  await redis!.acl('SETUSER', user, 'on', '>test', '~*', '&*', '+@all', '-subscribe')
  const publisher = redis!.duplicate({ username: user, password: 'test', retryStrategy: () => null })
  const transport = new RedisTransport({ redis: publisher })
  await publisher.ping()
  const received: string[] = []
  let stop = transport.listen(user, (value) => received.push(value))
  try {
    const pending = await Promise.allSettled([
      transport.send(user, 'text'),
      transport.sendBinary(user, new Uint8Array(1)),
    ])
    const later = await Promise.allSettled([transport.send(user, 'later')])
    await redis!.acl('SETUSER', user, '+subscribe')
    stop()
    const delivered = new Promise<void>((resolve) => {
      stop = transport.listen(user, (value) => {
        received.push(value)
        resolve()
      })
    })
    await transport.send(user, 'recovered')
    await delivered
    return { rejected: [...pending, ...later].every((result) => result.status === 'rejected'), received }
  } finally {
    stop()
    publisher.disconnect()
    await redis!.client('KILL', 'USER', user)
    await redis!.acl('DELUSER', user)
  }
}
