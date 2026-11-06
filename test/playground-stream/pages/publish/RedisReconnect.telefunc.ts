export {
  onBinaryBroadcastAcrossReconnect,
  onBroadcastOverRedisLimit,
  onBroadcastPublishedDuringResume,
  onBroadcastSubscriberDropped,
  onBroadcastSubscriberSilent,
  onRedisChannelDenied,
  onRedisSubscribeFailure,
}

import type { Socket } from 'node:net'
import type { Redis } from 'ioredis'
import { Broadcast, BroadcastChannel, config } from 'telefunc'
import type { DriverAttempt } from 'telefunc/__internal'
// Only `installRedis()` is public, and it takes one backend per process.
import { REDIS_COMMANDS } from '../../../../packages/redis/dist/commands.js'
import type { SubscriberSocket } from '../../../../packages/redis/dist/ioredis.js'
import { RedisBackend } from '../../../../packages/redis/dist/RedisBackend.js'
import { RedisConnection } from '../../../../packages/redis/dist/RedisConnection.js'
import { dropRedisSubscribers, redis } from '../../redis'
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

/** This server's subscriber connection drops during these publishes of both kinds to one key: each publish of a kind
 *  arrives or is reported. */
async function onBroadcastSubscriberDropped() {
  const key = `room:subscriber-dropped:${crypto.randomUUID()}`
  const text = { published: 0, received: 0, reported: 0 }
  const binary = { published: 0, received: 0, reported: 0 }
  const unsubscribeText = Broadcast.subscribe(key, () => text.received++)
  const unsubscribeBinary = Broadcast.subscribeBinary(key, () => binary.received++)
  config.broadcast.onDeliveryLost = ({ reason }) => {
    const kind = reason.includes('binary') ? binary : text
    kind.reported += Number.parseInt(reason)
  }
  const published: unknown[] = []
  for (let i = 0; i < 600; i++) {
    if (i === 100) {
      await Promise.all(published)
      await dropRedisSubscribers()
    }
    const kind = i % 2 === 0 ? text : binary
    kind.published++
    published.push(kind === text ? Broadcast.publish(key, i) : Broadcast.publishBinary(key, new Uint8Array(1)))
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

/** Redis's replies stop reaching a subscriber connection, as in a partition, which closes no socket: the server notices,
 *  reports what passed it by, and delivers again. */
async function onBroadcastSubscriberSilent() {
  const route = { key: `room:subscriber-silent:${crypto.randomUUID()}`, kind: 'text' } as const
  const connection = new RedisConnection({ redis: redis!.duplicate(), prefix: 'tf:' })
  const backend = new RedisBackend({ connection })
  const sockets: SubscriberSocket[] = []
  const open = connection.openSubscriberSocket.bind(connection)
  connection.openSubscriberSocket = async () => {
    const socket = await open()
    sockets.push(socket)
    return socket
  }
  const reported: string[] = []
  config.broadcast.onDeliveryLost = ({ key, reason }) => {
    if (key === route.key) reported.push(reason)
  }
  const received: string[] = []
  const attempt = backend.subscribe(route, { deliver: (value) => received.push(value as string) })
  try {
    await backend.publish(route, 'before')
    await receivedCount(received, 1)
    // Two pings at rest: an answered one keeps the connection.
    await sleep(21_000)
    ;(sockets[0] as unknown as { stream: Socket }).stream.pause()
    await backend.publish(route, 'missed')
    for (let tries = 1; reported.length === 0 && tries <= 300; tries++) await sleep(100)
    await backend.publish(route, 'after')
    await receivedCount(received, 2)
    return { received, reported, connections: sockets.length }
  } finally {
    config.broadcast.onDeliveryLost = ({ key, reason }) =>
      console.error(`Broadcast delivery of '${key}' lost: ${reason}`)
    await attempt.unsubscribe()
    connection.client.disconnect()
  }
}

/** This server's subscriber connection drops, and once it subscribed again a publish lands just before the key's start
 *  is read back, so the start counts its frame while that frame is on its way. It arrives, and nothing is reported. */
async function onBroadcastPublishedDuringResume() {
  const route = { key: `room:during-resume:${crypto.randomUUID()}`, kind: 'text' } as const
  const connection = new RedisConnection({ redis: redis!.duplicate(), prefix: 'tf:' })
  const backend = new RedisBackend({ connection })
  const sockets: SubscriberSocket[] = []
  const open = connection.openSubscriberSocket.bind(connection)
  connection.openSubscriberSocket = async () => {
    const socket = await open()
    sockets.push(socket)
    return socket
  }
  let resuming = false
  const run = connection.run.bind(connection)
  connection.run = (async (command: never, input: never) => {
    if (resuming && command === REDIS_COMMANDS.broadcastStart) {
      resuming = false
      await backend.publish(route, 'during the read')
    }
    return run(command, input)
  }) as typeof connection.run
  const reported: string[] = []
  config.broadcast.onDeliveryLost = ({ key, reason }) => {
    if (key === route.key) reported.push(reason)
  }
  const received: string[] = []
  const attempt = backend.subscribe(route, { deliver: (value) => received.push(value as string) })
  try {
    await backend.publish(route, 'before')
    await receivedCount(received, 1)
    resuming = true
    sockets[0]!.disconnect()
    await receivedCount(received, 2)
    await backend.publish(route, 'after')
    await receivedCount(received, 3)
    return { received, reported }
  } finally {
    config.broadcast.onDeliveryLost = ({ key, reason }) =>
      console.error(`Broadcast delivery of '${key}' lost: ${reason}`)
    await attempt.unsubscribe()
    connection.client.disconnect()
  }
}

/** The largest message Redis delivers, and past it by a byte, in bytes and in the bytes of a shorter text. */
async function onBroadcastOverRedisLimit() {
  const key = `room:over-limit:${crypto.randomUUID()}`
  const largest = 28 * 1024 * 1024
  const received: number[] = []
  const unsubscribe = Broadcast.subscribeBinary(key, (bytes) => received.push(bytes.byteLength))
  const refusal = (error: Error) => error.name
  await Broadcast.publishBinary(key, new Uint8Array(largest))
  const refused = await Promise.all([
    Promise.resolve(Broadcast.publishBinary(key, new Uint8Array(largest + 1))).catch(refusal),
    Promise.resolve(Broadcast.publish(key, 'é'.repeat(largest / 2))).catch(refusal),
  ])
  await Broadcast.publishBinary(key, new Uint8Array(1))
  await receivedCount(received, 2)
  unsubscribe()
  return { refused, received }
}

/** Settles once `received` holds `count`, or a moment after it plainly won't. */
async function receivedCount(received: unknown[], count: number) {
  for (let tries = 1; received.length < count && tries <= 40; tries++) await sleep(50)
}

/** Redis refuses a user's SUBSCRIBEs while its backend publishes on a new subscription; a publish made once that
 *  subscription failed goes out; then the command is granted again, and a new subscription gets the next publish. The user is this test's own, as the rest of the lane runs as
 *  `default`, so it gets its own backend: `installRedis()` takes one per process. */
async function onRedisSubscribeFailure() {
  const user = `subscribe-test-${crypto.randomUUID()}`
  await redis!.acl('SETUSER', user, 'on', '>test', '~*', '&*', '+@all', '-subscribe')
  const client = redis!.duplicate({ username: user, password: 'test', retryStrategy: () => null })
  client.on('error', () => {})
  const backend = new RedisBackend({ connection: new RedisConnection({ redis: client, prefix: 'tf:' }) })
  const text = { key: user, kind: 'text' } as const
  const received: string[] = []
  const subscribe = (receive: (value: string) => void) =>
    backend.subscribe(text, { deliver: (value) => receive(value as string) })
  let attempt: DriverAttempt | undefined
  try {
    await subscribeRefused(client, user)
    attempt = subscribe((value) => received.push(value))
    const pending = await Promise.allSettled([
      backend.publish(text, 'text'),
      backend.publish({ key: user, kind: 'binary' }, new Uint8Array(1)),
    ])
    await backend.publish(text, 'later')
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
        await backend.publish(text, 'recovered')
        break
      } catch (err) {
        if (tries === 50) throw err
        await sleep(100)
      }
    }
    await delivered
    return { rejected: pending.every((result) => result.status === 'rejected'), received }
  } finally {
    await attempt?.unsubscribe()
    client.disconnect()
    // Also closes the backend's command connection, which runs as `user` too.
    await redis!.acl('DELUSER', user)
  }
}

/** Redis denies a user one channel, subscribed in one turn with a new allowed one, beside another already up: the denied
 *  subscription fails with Redis's reason, its publish is rejected, and the other two keep delivering uninterrupted. The
 *  user is this test's own, as in `onRedisSubscribeFailure`. */
async function onRedisChannelDenied() {
  const user = `channel-test-${crypto.randomUUID()}`
  await redis!.acl('SETUSER', user, 'on', '>test', '~*', 'resetchannels', '&*allowed*', '+@all')
  const client = redis!.duplicate({ username: user, password: 'test', retryStrategy: () => null })
  client.on('error', () => {})
  const backend = new RedisBackend({ connection: new RedisConnection({ redis: client, prefix: 'tf:' }) })
  const route = (name: string) => ({ key: `${user}:${name}`, kind: 'text' }) as const
  const received: string[] = []
  const subscribe = (name: string) =>
    backend.subscribe(route(name), { deliver: (value) => received.push(`${name}: ${value}`) })
  const up = subscribe('allowed-up')
  const upChanges: string[] = []
  let denied: DriverAttempt | undefined
  let fresh: DriverAttempt | undefined
  try {
    await backend.publish(route('allowed-up'), 'first')
    up.onStateChange((state) => upChanges.push(state))
    denied = subscribe('denied')
    const deniedReasons: unknown[] = []
    denied.onStateChange((_state, reason) => deniedReasons.push(reason))
    fresh = subscribe('allowed-fresh')
    const [deniedPublish] = await Promise.allSettled([
      backend.publish(route('denied'), 'never'),
      backend.publish(route('allowed-fresh'), 'next to it'),
    ])
    await backend.publish(route('allowed-up'), 'after')
    for (let tries = 1; received.length < 3 && tries <= 100; tries++) await sleep(50)
    return {
      deniedListenerTold: String(deniedReasons).includes('NOPERM'),
      deniedPublishTold: deniedPublish.status === 'rejected' && String(deniedPublish.reason).includes('NOPERM'),
      upChanges: [...upChanges],
      received: received.sort(),
    }
  } finally {
    await Promise.all([up.unsubscribe(), denied?.unsubscribe(), fresh?.unsubscribe()])
    client.disconnect()
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
