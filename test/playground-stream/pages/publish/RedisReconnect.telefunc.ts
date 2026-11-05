export { onBinaryBroadcastAcrossReconnect, onBroadcastSubscriberDropped, onRedisSubscribeFailure }

import type { Redis } from 'ioredis'
import { Broadcast, BroadcastChannel, config } from 'telefunc'
import type { BroadcastPayload, BroadcastRoute, DriverAttempt } from 'telefunc/__internal'
// Only `installRedis()` is public, and it takes one backend per process.
import { RedisBackend } from '../../../../packages/redis/dist/RedisBackend.js'
import { RedisConnection } from '../../../../packages/redis/dist/RedisConnection.js'
import { redis } from '../../redis'
import { sleep } from '../../sleep'

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

/** The page drops this server's connections to Redis during these publishes of both kinds to one key: each publish of a
 *  kind arrives, is reported or rejects. */
async function onBroadcastSubscriberDropped() {
  const key = `room:subscriber-dropped:${crypto.randomUUID()}`
  const text = { published: 0, received: 0, reported: 0, rejected: 0 }
  const binary = { published: 0, received: 0, reported: 0, rejected: 0 }
  const unsubscribeText = Broadcast.subscribe(key, () => text.received++)
  const unsubscribeBinary = Broadcast.subscribeBinary(key, () => binary.received++)
  config.broadcast.onDeliveryLost = ({ reason }) => {
    const kind = reason.includes('binary') ? binary : text
    kind.reported += Number.parseInt(reason)
  }
  const published: Promise<unknown>[] = []
  for (let i = 0; i < 600; i++) {
    const kind = i % 2 === 0 ? text : binary
    kind.published++
    const publish = kind === text ? Broadcast.publish(key, i) : Broadcast.publishBinary(key, new Uint8Array(1))
    published.push(Promise.resolve(publish).catch(() => kind.rejected++))
    await sleep(1)
  }
  await Promise.all(published)
  // Long enough to reconnect and read where the key starts.
  await sleep(1_000)
  config.broadcast.onDeliveryLost = ({ key, reason }) => console.error(`Broadcast delivery of '${key}' lost: ${reason}`)
  unsubscribeText()
  unsubscribeBinary()
  return { text, binary }
}

/** Redis refuses a user's SUBSCRIBEs while its backend publishes on a new subscription; then the command is granted
 *  again, and a new subscription gets the next publish. The user is this test's own, as the rest of the lane runs as
 *  `default`, so it gets its own backend: `installRedis()` takes one per process. */
async function onRedisSubscribeFailure() {
  const user = `subscribe-test-${crypto.randomUUID()}`
  await redis!.acl('SETUSER', user, 'on', '>test', '~*', '&*', '+@all', '-subscribe')
  const client = redis!.duplicate({ username: user, password: 'test', retryStrategy: () => null })
  client.on('error', () => {})
  const backend = new RedisBackend({ connection: new RedisConnection({ redis: client, prefix: 'tf:' }) })
  const text = { key: user, kind: 'text' } as const
  // A refusal known already throws at the call.
  const publish = async (route: BroadcastRoute, payload: BroadcastPayload) => backend.publish(route, payload)
  const received: string[] = []
  const subscribe = (receive: (value: string) => void) =>
    backend.subscribe(text, { deliver: (value) => receive(value as string) })
  let attempt: DriverAttempt | undefined
  try {
    await subscribeRefused(client, user)
    attempt = subscribe((value) => received.push(value))
    const pending = await Promise.allSettled([
      publish(text, 'text'),
      publish({ key: user, kind: 'binary' }, new Uint8Array(1)),
    ])
    const later = await Promise.allSettled([publish(text, 'later')])
    await redis!.acl('SETUSER', user, '+subscribe')
    // Dragonfly applies an ACL change to an open connection a moment after it answers, so a new subscription may still
    // be refused.
    let delivered: Promise<void>
    for (let tries = 1; ; tries++) {
      await attempt.unsubscribe()
      delivered = new Promise<void>((resolve) => {
        attempt = subscribe((value) => {
          received.push(value)
          resolve()
        })
      })
      try {
        await publish(text, 'recovered')
        break
      } catch (err) {
        if (tries === 50) throw err
        await sleep(100)
      }
    }
    await delivered
    return { rejected: [...pending, ...later].every((result) => result.status === 'rejected'), received }
  } finally {
    await attempt?.unsubscribe()
    client.disconnect()
    // Also closes the backend's command connection, which runs as `user` too.
    await redis!.acl('DELUSER', user)
  }
}

/** Settles once Redis refuses a SUBSCRIBE on `client`, as Dragonfly's answer to an ACL change can come before the
 *  change reaches an open connection. */
async function subscribeRefused(client: Redis, channel: string) {
  for (let tries = 1; tries <= 50; tries++) {
    try {
      await client.subscribe(channel)
      await client.unsubscribe(channel)
    } catch (err) {
      if (String(err).includes('NOPERM')) return
      throw err
    }
    await sleep(100)
  }
  throw new Error(`Redis never refused a SUBSCRIBE of ${channel}`)
}
