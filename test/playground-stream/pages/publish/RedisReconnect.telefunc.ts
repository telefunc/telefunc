export {
  onBinaryBroadcastAcrossReconnect,
  onRedisSubscribeFailure,
  onPublishBesideRefusedSubscribe,
  onPublishAcrossSeqBoundary,
}

import { Broadcast, BroadcastChannel } from 'telefunc'
import { redis } from '../../redis'
import { allowEveryChannel, refuseChannels } from './refuseChannels'

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
  if (!redis) throw new Error('Expected the server to run with Redis')
  await redis.acl('SETUSER', user, 'on', '>test', '~*', '&*', '+@all', '-subscribe')
  // From here on, a failure still removes the user and its connections.
  let publisher: ReturnType<NonNullable<typeof redis>['duplicate']> | undefined
  let stop = () => {}
  try {
    publisher = redis.duplicate({ username: user, password: 'test', retryStrategy: () => null })
    const transport = new RedisTransport({ redis: publisher })
    await publisher.ping()
    const received: string[] = []
    stop = transport.listen(user, (value) => received.push(value)).unsubscribe
    const [text, binary] = await Promise.allSettled([
      transport.send(user, 'text'),
      transport.sendBinary(user, new Uint8Array(1)),
    ])
    const later = await Promise.allSettled([transport.send(user, 'later')])
    await redis.acl('SETUSER', user, '+subscribe')
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
    publisher?.disconnect()
    // Also closes the transport's subscriber connection, which runs as `user` too.
    await redis.acl('DELUSER', user)
  }
}

/** Starts a key's sequence in Redis 2 short of 2^32, then publishes 3 texts on it: the seq each publish returned, and
 *  what this server's subscriber received. */
async function onPublishAcrossSeqBoundary() {
  if (!redis) throw new Error('The playground runs without Redis')
  const key = `room:seq-boundary:${crypto.randomUUID()}`
  // The transport's counter for the key, under its default prefix.
  const seqKey = `tf:seq:{${key}}`
  await redis.set(seqKey, 2 ** 32 - 2)
  const received: { text: string; seq: number }[] = []
  const unsubscribe = Broadcast.subscribe<string>(key, (text, { seq }) => {
    received.push({ text, seq })
  })
  try {
    await unsubscribe.opened
    const published: number[] = []
    for (const text of ['a', 'b', 'c']) published.push((await Broadcast.publish(key, text)).seq)
    for (let i = 0; i < 25 && received.length < 3; i++) await new Promise((resolve) => setTimeout(resolve, 100))
    return { published, received }
  } finally {
    unsubscribe()
    await redis.del(seqKey)
  }
}

/** While Redis refuses the SUBSCRIBE of one key, which a subscriber the app hasn't removed keeps, this server publishes on
 *  another key. */
async function onPublishBesideRefusedSubscribe() {
  if (!redis) throw new Error('The playground runs without Redis')
  const unsubscribe: Array<() => void> = []
  try {
    if (!(await refuseChannels())) return { refusesChannels: false }
    const refused = Broadcast.subscribe(`!refused:${crypto.randomUUID()}`, () => {})
    unsubscribe.push(refused)
    const key = `room:beside-refused:${crypto.randomUUID()}`
    const received: string[] = []
    const subscribed = Broadcast.subscribe<string>(key, (text) => received.push(text))
    unsubscribe.push(subscribed)
    const refusedOpened = await refused.opened.then(
      () => 'resolved',
      () => 'rejected',
    )
    await subscribed.opened
    const published = await Promise.resolve(Broadcast.publish(key, 'beside')).then(
      () => 'sent',
      (err: unknown) => String(err),
    )
    for (let i = 0; i < 25 && received.length === 0; i++) await new Promise((resolve) => setTimeout(resolve, 100))
    return { refusedOpened, published, received }
  } finally {
    for (const stop of unsubscribe) stop()
    await allowEveryChannel()
  }
}
