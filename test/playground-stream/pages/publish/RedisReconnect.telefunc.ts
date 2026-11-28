export {
  onBinaryBroadcastAcrossReconnect,
  onBroadcastOverRedisLimit,
  onBroadcastPublishedDuringResume,
  onBroadcastRenewalRefused,
  onBroadcastStartReadFailed,
  onBroadcastSubscriberDropped,
  onBroadcastSubscriberSilent,
  onPublishBesideRefusedSubscribe,
  onRedisChannelDenied,
  onRedisSubscribeFailure,
  onPublishAcrossSeqBoundary,
}

import { Socket } from 'node:net'
import type { Redis } from 'ioredis'
import { Broadcast, BroadcastChannel, config, onBug } from 'telefunc'
import type { BroadcastRoute, DriverAttempt } from 'telefunc/__internal'
// Only `installRedis()` is public, and it takes one backend per process.
import { REDIS_COMMANDS, type RedisCommand } from '../../../../packages/redis/dist/commands.js'
import type { SubscriberSocket } from '../../../../packages/redis/dist/ioredis.js'
import { RedisBackend } from '../../../../packages/redis/dist/RedisBackend.js'
import { RedisConnection } from '../../../../packages/redis/dist/RedisConnection.js'
import { dropRedisSubscribers, redis, whileRedisSubscribersDown } from '../../redis'
import { sleep } from '../../sleep'
import { allowEveryChannel, refuseChannels } from './refuseChannels'
import { present } from '../../../checked'

/** On the page's `start`, the server's Redis connection drops, and it publishes 3 binary frames from one reused buffer
 *  while ioredis reconnects. */
async function onBinaryBroadcastAcrossReconnect() {
  const room = new BroadcastChannel<{ start: true }>({ key: `room:reconnect-test:${crypto.randomUUID()}` })
  room.subscribe(() => {
    present(redis, 'a Redis: REDIS_URL is set').disconnect(true)
    const bytes = new Uint8Array(64)
    for (let i = 1; i <= 3; i++) void room.publishBinary(bytes.fill(i))
  })
  return room
}

/** Publishes of both kinds to one key, while this server's subscriber connection is up, then while it is dropped and
 *  kept down, then once it may come back: each publish of a kind arrives or is reported, and each one made while the
 *  connection was down is reported. */
async function onBroadcastSubscriberDropped() {
  const key = `room:subscriber-dropped:${crypto.randomUUID()}`
  const text = { published: 0, publishedWhileDown: 0, received: 0, reported: 0 }
  const binary = { published: 0, publishedWhileDown: 0, received: 0, reported: 0 }
  const unsubscribeText = Broadcast.subscribe(key, () => text.received++)
  const unsubscribeBinary = Broadcast.subscribeBinary(key, () => binary.received++)
  const before = reported.length
  const monitorBug = `broadcast-listener-bug: delivery loss monitor ${key}`
  config.broadcast.onDeliveryLost = ({ key: lostKey, reason }) => {
    if (lostKey !== key) return
    const kind = reason.includes('binary') ? binary : text
    kind.reported += Number.parseInt(reason)
    const error = new Error(`${monitorBug}:${kind === text ? 'text' : 'binary'}`)
    if (kind === text) throw error
    return Promise.reject(error)
  }
  const publish = async (count: number, whileDown: boolean) => {
    const published: unknown[] = []
    for (let i = 0; i < count; i++) {
      const kind = i % 2 === 0 ? text : binary
      kind.published++
      if (whileDown) kind.publishedWhileDown++
      published.push(kind === text ? Broadcast.publish(key, i) : Broadcast.publishBinary(key, new Uint8Array(1)))
    }
    await Promise.all(published)
  }
  try {
    await publish(200, false)
    await whileRedisSubscribersDown(() => publish(200, true))
    await publish(200, false)
    // The subscription has read its new start once every publish has arrived or been reported.
    const settled = () => [text, binary].every(({ published, received, reported }) => received + reported >= published)
    for (let tries = 1; !settled() && tries <= 200; tries++) await sleep(50)
    const bugs = [
      ...new Set(
        reported
          .slice(before)
          .filter((msg) => msg.startsWith(monitorBug))
          .map((msg) => msg.slice(monitorBug.length + 1)),
      ),
    ].sort()
    return { text, binary, bugs }
  } finally {
    config.broadcast = {}
    unsubscribeText()
    unsubscribeBinary()
  }
}

/** Redis's replies stop reaching a subscriber connection, as in a partition, which closes no socket: the server notices,
 *  reports what passed it by, and delivers again. */
async function onBroadcastSubscriberSilent() {
  const route = { key: `room:subscriber-silent:${crypto.randomUUID()}`, kind: 'text' } as const
  const { backend, sockets, reported, received, stop } = ownRedisBackend(route)
  try {
    await backend.publish(route, 'before')
    await receivedCount(received, 1)
    // Two pings at rest: an answered one keeps the connection.
    await sleep(21_000)
    tcpSocketOf(present(sockets[0], 'a subscriber socket')).pause()
    await backend.publish(route, 'missed')
    for (let tries = 1; reported.length === 0 && tries <= 300; tries++) await sleep(100)
    await backend.publish(route, 'after')
    await receivedCount(received, 2)
    return { received, reported, connections: sockets.length }
  } finally {
    await stop()
  }
}

/** This server's subscriber connection drops, and once it subscribed again a publish lands just before the key's start
 *  is read back, so the start counts its frame while that frame is on its way. It arrives, and nothing is reported. */
async function onBroadcastPublishedDuringResume() {
  const route = { key: `room:during-resume:${crypto.randomUUID()}`, kind: 'text' } as const
  const { connection, backend, sockets, reported, received, stop } = ownRedisBackend(route)
  let resuming = false
  const run = connection.run.bind(connection)
  connection.run = async <Input, Output>(command: RedisCommand<Input, Output>, input: Input) => {
    if (resuming && command === REDIS_COMMANDS.broadcastStart) {
      resuming = false
      await backend.publish(route, 'during the read')
    }
    return run(command, input)
  }
  try {
    await backend.publish(route, 'before')
    await receivedCount(received, 1)
    resuming = true
    present(sockets[0], 'a subscriber socket').disconnect()
    await receivedCount(received, 2)
    await backend.publish(route, 'after')
    await receivedCount(received, 3)
    return { received, reported }
  } finally {
    await stop()
  }
}

/** A key's text subscription is up, and the first read of where a new binary one starts fails while a text publish
 *  waits for both. That failure ends neither: the binary subscription reads again and is ready, and the publish then
 *  goes out and arrives. */
async function onBroadcastStartReadFailed() {
  const route = { key: `room:start-read-failed:${crypto.randomUUID()}`, kind: 'text' } as const
  const { connection, backend, reported, received, stop } = ownRedisBackend(route)
  try {
    await backend.publish(route, 'before')
    await receivedCount(received, 1)
    const run = connection.run.bind(connection)
    let startReads = 0
    connection.run = async <Input, Output>(command: RedisCommand<Input, Output>, input: Input) => {
      if (command === REDIS_COMMANDS.broadcastStart && startReads++ === 0) throw new Error('the start read failed')
      return run(command, input)
    }
    const binary = backend.subscribe({ key: route.key, kind: 'binary' }, { deliver() {} }, 'binary')
    await backend.publish(route, 'beside the failed read')
    await receivedCount(received, 2)
    return { received, reported, binary: binary.state(), startReads }
  } finally {
    await stop()
  }
}

/** The TCP socket under an ioredis connection. */
function tcpSocketOf(connection: object): Socket {
  const stream = 'stream' in connection ? connection.stream : undefined
  if (!(stream instanceof Socket)) throw new Error(`Expected an ioredis connection's socket, read ${String(stream)}`)
  return stream
}

/** A backend of the test's own, as `installRedis()` takes one per process, subscribed to `route`: what arrives, what the
 *  key's `onDeliveryLost` reports and the subscriber sockets it opens. `stop()` ends all of it. */
function ownRedisBackend(route: BroadcastRoute) {
  const connection = new RedisConnection({
    redis: present(redis, 'a Redis: REDIS_URL is set').duplicate(),
    prefix: 'tf:',
  })
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
  const attempt = backend.subscribe(route, { deliver: (value) => received.push(String(value)) }, 'text')
  const stop = async () => {
    config.broadcast.onDeliveryLost = ({ key, reason }) =>
      console.error(`Broadcast delivery of '${key}' lost: ${reason}`)
    await attempt.unsubscribe()
    connection.client.disconnect()
  }
  return { connection, backend, sockets, reported, received, stop }
}

/** The largest message Telefunc's Redis backend takes, and past it by a byte, in bytes and in the bytes of a shorter text. */
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
  const store = present(redis, 'a Redis: REDIS_URL is set')
  await store.acl('SETUSER', user, 'on', '>test', '~*', '&*', '+@all', '-subscribe')
  const client = store.duplicate({ username: user, password: 'test', retryStrategy: () => null })
  client.on('error', () => {})
  const backend = new RedisBackend({ connection: new RedisConnection({ redis: client, prefix: 'tf:' }) })
  const text = { key: user, kind: 'text' } as const
  const received: string[] = []
  const subscribe = (receive: (value: string) => void) =>
    backend.subscribe(text, { deliver: (value) => receive(String(value)) }, 'text')
  let attempt: DriverAttempt | undefined
  try {
    await subscribeRefused(client, user)
    attempt = subscribe((value) => received.push(value))
    const [textPublish, binaryPublish] = await Promise.allSettled([
      backend.publish(text, 'text'),
      backend.publish({ key: user, kind: 'binary' }, new Uint8Array(1)),
    ])
    await backend.publish(text, 'later')
    await store.acl('SETUSER', user, '+subscribe')
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
    return { rejected: textPublish.status === 'rejected', binary: binaryPublish.status, received }
  } finally {
    await attempt?.unsubscribe()
    client.disconnect()
    // Also closes the backend's command connection, which runs as `user` too.
    await store.acl('DELUSER', user)
  }
}

/** Redis denies a user one channel, subscribed in one turn with a new allowed one, beside another already up: the denied
 *  subscription fails with Redis's reason, its publish is rejected, and the other two keep delivering uninterrupted. The
 *  user is this test's own, as in `onRedisSubscribeFailure`. */
async function onRedisChannelDenied() {
  const user = `channel-test-${crypto.randomUUID()}`
  const store = present(redis, 'a Redis: REDIS_URL is set')
  await store.acl('SETUSER', user, 'on', '>test', '~*', 'resetchannels', '&*allowed*', '+@all')
  const client = store.duplicate({ username: user, password: 'test', retryStrategy: () => null })
  client.on('error', () => {})
  const backend = new RedisBackend({ connection: new RedisConnection({ redis: client, prefix: 'tf:' }) })
  const route = (name: string) => ({ key: `${user}:${name}`, kind: 'text' }) as const
  const received: string[] = []
  const subscribe = (name: string) =>
    backend.subscribe(route(name), { deliver: (value) => received.push(`${name}: ${value}`) }, name)
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
    await store.acl('DELUSER', user)
  }
}

/** Starts a key's sequence in Redis 2 short of 2^32, then publishes 3 texts on it: the seq each publish returned, and
 *  what this server's subscriber received. */
async function onPublishAcrossSeqBoundary() {
  if (!redis) throw new Error('The playground runs without Redis')
  const key = `room:seq-boundary:${crypto.randomUUID()}`
  // The transport's order record for the key, under its default prefix and the client's database: its seq, then each
  // kind's count and last publish's id, none yet (zero).
  const orderKey = `tf:${redis.options.db ?? 0}:seq:{${encodeURIComponent(key)}}`
  const record = Buffer.alloc(40)
  record.writeDoubleLE(2 ** 32 - 2, 0)
  await redis.set(orderKey, record)
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
    await redis.del(orderKey)
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

/** What this server reported through `onBug()`, by message. */
const reported: string[] = []
onBug((err) => reported.push(err instanceof Error ? err.message : String(err)))

/** A key's subscription is in effect when Redis starts refusing the key and this server's subscriber connection drops:
 *  Redis refuses its SUBSCRIBE again, which ends the subscriber. Once Redis allows the key again, a publish reaches only
 *  a fresh subscriber. */
async function onBroadcastRenewalRefused() {
  if (!redis) throw new Error('The playground runs without Redis')
  const id = crypto.randomUUID()
  const key = `!renewal-refused:${id}`
  const heard: string[] = []
  const fresh: string[] = []
  const unsubscribe: Array<() => void> = []
  const reports = () => reported.filter((message) => message.includes(id)).length
  try {
    const subscribed = Broadcast.subscribe<string>(key, (text) => heard.push(text))
    unsubscribe.push(subscribed)
    await subscribed.opened
    await Broadcast.publish(key, 'before')
    await receivedCount(heard, 1)
    if (!(await refuseChannels())) return { refusesChannels: false }
    await dropRedisSubscribers()
    for (let tries = 1; reports() === 0 && tries <= 100; tries++) await sleep(100)
    // A second report would come right after the first.
    await sleep(500)
    const reportsWhileRefused = reports()
    await redis.call('ACL', 'SETUSER', 'default', 'allchannels')
    // Dragonfly applies an ACL change to an open connection a moment after it answers, so a fresh subscription may still
    // be refused.
    for (let tries = 1; ; tries++) {
      const again = Broadcast.subscribe<string>(key, (text) => fresh.push(text))
      unsubscribe.push(again)
      if (
        await again.opened.then(
          () => true,
          () => tries === 50,
        )
      )
        break
      await sleep(100)
    }
    await Broadcast.publish(key, 'after')
    await receivedCount(fresh, 1)
    const opened = await subscribed.opened.then(
      () => 'resolved',
      () => 'rejected',
    )
    return { opened, reports: reportsWhileRefused, heard, fresh }
  } finally {
    for (const stop of unsubscribe) stop()
    await redis.call('ACL', 'SETUSER', 'default', 'allchannels')
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
