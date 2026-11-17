export { onBinaryBroadcastAcrossReconnect, onRedisSubscribeFailure, onPublishBesideRefusedSubscribe }

import { Broadcast, BroadcastChannel } from 'telefunc'
import { redis } from '../../redis'
import { refuseChannels } from './refuseChannels'

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
  let stop = transport.listen(user, (value) => received.push(value)).unsubscribe
  try {
    const [text, binary] = await Promise.allSettled([
      transport.send(user, 'text'),
      transport.sendBinary(user, new Uint8Array(1)),
    ])
    const later = await Promise.allSettled([transport.send(user, 'later')])
    await redis!.acl('SETUSER', user, '+subscribe')
    // Dragonfly applies an ACL change to an open connection a moment after it answers, so a new subscription may still
    // be refused.
    let delivered: Promise<void>
    for (let attempt = 1; ; attempt++) {
      stop()
      delivered = new Promise<void>((resolve) => {
        stop = transport.listen(user, (value) => {
          received.push(value)
          resolve()
        }).unsubscribe
      })
      try {
        await transport.send(user, 'recovered')
        break
      } catch (err) {
        if (attempt === 50) throw err
        await new Promise((resolve) => setTimeout(resolve, 100))
      }
    }
    await delivered
    return {
      rejected: [text, ...later].every((result) => result.status === 'rejected'),
      binary: binary.status,
      received,
    }
  } finally {
    stop()
    publisher.disconnect()
    // Also closes the transport's subscriber connection, which runs as `user` too.
    await redis!.acl('DELUSER', user)
  }
}

/** While Redis refuses the SUBSCRIBE of one key, which a subscriber the app hasn't removed keeps, this server publishes on
 *  another key. */
async function onPublishBesideRefusedSubscribe() {
  if (!redis) throw new Error('The playground runs without Redis')
  if (!(await refuseChannels())) {
    await redis.call('ACL', 'SETUSER', 'default', 'allchannels')
    return { refusesChannels: false }
  }
  const refused = Broadcast.subscribe(`!refused:${crypto.randomUUID()}`, () => {})
  const key = `room:beside-refused:${crypto.randomUUID()}`
  const received: string[] = []
  const subscribed = Broadcast.subscribe<string>(key, (text) => received.push(text))
  try {
    const refusedReady = await refused.ready.then(
      () => 'resolved',
      () => 'rejected',
    )
    await subscribed.ready
    const published = await Promise.resolve(Broadcast.publish(key, 'beside')).then(
      () => 'sent',
      (err: unknown) => String(err),
    )
    for (let i = 0; i < 25 && received.length === 0; i++) await new Promise((resolve) => setTimeout(resolve, 100))
    return { refusedReady, published, received }
  } finally {
    refused()
    subscribed()
    await redis.call('ACL', 'SETUSER', 'default', 'allchannels')
  }
}
