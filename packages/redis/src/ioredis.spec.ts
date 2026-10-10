import { createServer, type AddressInfo } from 'node:net'
import { Cluster, Redis } from 'ioredis'
import { expect, onTestFinished, test, vi } from 'vitest'
import { RedisBackend } from './backend.js'
import { callDefinedCommand, createSubscriberSocket } from './ioredis.js'
import { REDIS_COMMANDS } from './commands.js'

/** The backend on `redis`, and the clone of `redis` it runs its commands on. */
function backendOn<Client extends Redis | Cluster>(redis: Client): { backend: RedisBackend; commands: Client } {
  const duplicate = vi.spyOn(redis as Redis, 'duplicate')
  const backend = new RedisBackend({ redis, prefix: 'tf:' })
  return { backend, commands: duplicate.mock.results[0]?.value as Client }
}

test("runs its commands on a never-resend, master-reading clone of any client, leaving the client's options as they were", () => {
  const nodes = [{ host: '127.0.0.1', port: 6379 }]
  const reconnectOnError = () => true
  const standalone = [
    new Redis('redis://127.0.0.1:6379', { lazyConnect: true }),
    new Redis('redis://127.0.0.1:6379', { lazyConnect: true, maxRetriesPerRequest: 5, reconnectOnError }),
  ]
  const clusters = [
    new Cluster(nodes, { lazyConnect: true }),
    new Cluster(nodes, {
      lazyConnect: true,
      scaleReads: 'slave',
      retryDelayOnFailover: 100,
      enableAutoPipelining: true,
      redisOptions: { maxRetriesPerRequest: 5, reconnectOnError, connectionName: 'app' },
    }),
  ]
  onTestFinished(() => [...standalone, ...clusters].forEach((redis) => redis.disconnect()))
  for (const redis of standalone) {
    const options = { ...redis.options }
    const { commands } = backendOn(redis)
    expect(redis.options).toEqual(options)
    expect(commands.options).toMatchObject({ host: '127.0.0.1', port: 6379, lazyConnect: true })
    expect(commands.options).toMatchObject({ maxRetriesPerRequest: 0, reconnectOnError: null })
  }
  for (const redis of clusters) {
    const snapshot = () => ({ ...redis.options, redisOptions: { ...redis.options.redisOptions } })
    const options = snapshot()
    const { commands } = backendOn(redis)
    expect(snapshot()).toEqual(options)
    expect(commands.options).toMatchObject({
      lazyConnect: true,
      scaleReads: 'master',
      retryDelayOnFailover: 0,
      enableAutoPipelining: false,
      redisOptions: { ...options.redisOptions, maxRetriesPerRequest: 0, reconnectOnError: null },
    })
  }
})

test("rejects an ioredis keyPrefix, which Pub/Sub channel names don't get", () => {
  const nodes = [{ host: '127.0.0.1', port: 6379 }]
  const prefixed = [
    new Redis('redis://127.0.0.1:6379', { lazyConnect: true, keyPrefix: 'app:' }),
    new Cluster(nodes, { lazyConnect: true, redisOptions: { keyPrefix: 'app:' } }),
    new Cluster(nodes, { lazyConnect: true, keyPrefix: 'app:' }),
  ]
  onTestFinished(() => prefixed.forEach((redis) => redis.disconnect()))
  for (const redis of prefixed) expect(() => new RedisBackend({ redis, prefix: 'tf:' })).toThrow('keyPrefix')
})

test("ends its clone when the app's client ends, so the process can exit", async () => {
  const redis = new Redis({ lazyConnect: true })
  const cluster = new Cluster([{ host: '127.0.0.1', port: 6379 }], { lazyConnect: true })
  const clones = [backendOn(redis).commands, backendOn(cluster).commands]
  expect(clones.map((clone) => clone.status)).toEqual(['wait', 'wait'])
  redis.disconnect()
  cluster.disconnect()
  await vi.waitFor(() => expect(clones.map((clone) => clone.status)).toEqual(['end', 'end']))
})

test("runs a script once when Redis ran it but its reply was lost, through a client with ioredis's defaults", async () => {
  // A Redis stand-in that drops the connection instead of answering the first script it runs.
  let runs = 0
  const server = createServer((socket) => {
    let pending = Buffer.alloc(0)
    socket.on('data', (data) => {
      pending = Buffer.concat([pending, data])
      for (let command = readCommand(pending); command; command = readCommand(pending)) {
        pending = pending.subarray(command.length)
        const name = command.args[0]?.toUpperCase()
        if (name === 'INFO') socket.write('$11\r\nloading:0\r\n\r\n')
        else if (name !== 'EVALSHA' && name !== 'EVAL') socket.write('+OK\r\n')
        else if (++runs === 1) return void socket.destroy()
        else socket.write(`*3\r\n:${runs}\r\n:1700000000000\r\n:0\r\n`)
      }
    })
  })
  await new Promise<void>((listening) => server.listen(0, '127.0.0.1', listening))
  const redis = new Redis({ port: (server.address() as AddressInfo).port })
  onTestFinished(() => {
    redis.disconnect()
    server.close()
  })
  const { backend } = backendOn(redis)
  await expect(backend.publish({ key: 'chat', kind: 'text' }, 'lost')).rejects.toThrow()
  // A resent script would reach Redis before this one, on the same reconnected connection.
  expect(await backend.publish({ key: 'chat', kind: 'text' }, 'next')).toMatchObject({ seq: 2 })
  expect(runs).toBe(2)
})

/** One RESP command at the start of `buffer`, or undefined while it's incomplete. */
function readCommand(buffer: Buffer): { args: string[]; length: number } | undefined {
  const line = (from: number) => {
    const end = buffer.indexOf('\r\n', from)
    return end === -1 ? undefined : { text: buffer.toString('latin1', from + 1, end), next: end + 2 }
  }
  const header = line(0)
  if (header === undefined) return undefined
  const args: string[] = []
  let offset = header.next
  for (let index = 0; index < Number(header.text); index++) {
    const size = line(offset)
    if (size === undefined || size.next + Number(size.text) + 2 > buffer.length) return undefined
    args.push(buffer.toString('latin1', size.next, size.next + Number(size.text)))
    offset = size.next + Number(size.text) + 2
  }
  return { args, length: offset }
}

test('duplicates the subscriber from a standalone client or a live Cluster node, connecting a lazyConnect Cluster first', async () => {
  const cluster = new Cluster([{ host: '127.0.0.1', port: 6379 }], {
    lazyConnect: true,
  })
  const ended = new Redis({ lazyConnect: true })
  const live = new Redis({ lazyConnect: true })
  onTestFinished(() => [cluster, ended, live].forEach((redis) => redis.disconnect()))
  ended.disconnect()
  const connect = vi.spyOn(cluster, 'connect').mockImplementation(async () => {
    cluster.status = 'ready'
  })
  const nodes = vi.spyOn(cluster, 'nodes').mockReturnValue([ended])
  await expect(createSubscriberSocket(cluster)).rejects.toThrow('Redis Cluster has no available nodes')
  nodes.mockReturnValue([ended, live])
  const sockets = [await createSubscriberSocket(cluster), await createSubscriberSocket(live)]
  sockets.forEach((socket) => socket.disconnect())
  expect(connect).toHaveBeenCalledOnce()
})

test("waits for a connecting Cluster's masters instead of reporting none", async () => {
  const cluster = new Cluster([{ host: '127.0.0.1', port: 6379 }], {
    lazyConnect: true,
  })
  const master = new Redis({ lazyConnect: true })
  onTestFinished(() => [cluster, master].forEach((redis) => redis.disconnect()))
  // A non-lazy Cluster is 'connecting' from its constructor until its node pool fills.
  cluster.status = 'connecting'
  const nodes = vi.spyOn(cluster, 'nodes').mockReturnValue([])
  const opening = createSubscriberSocket(cluster)
  // A subscriber reopened while the Cluster still connects shares the one wait: the app's Cluster gets no more listeners.
  const reopening = createSubscriberSocket(cluster)
  expect([cluster.listenerCount('ready'), cluster.listenerCount('close')]).toEqual([1, 1])
  nodes.mockReturnValue([master])
  cluster.status = 'ready'
  cluster.emit('ready')
  for (const socket of [await opening, await reopening]) socket.disconnect()
  // A failed connect is an outage to report, whether the Cluster retries it (it never emits 'end' then) or gives up.
  cluster.status = 'connecting'
  const failing = createSubscriberSocket(cluster)
  cluster.emit('close')
  await expect(failing).rejects.toThrow('Redis Cluster connection closed')
})

const publishInput = { route: { key: 'chat', kind: 'text' }, payload: '' } as const
test("reads integer replies as ioredis returns them, numbers or, with a shared client's stringNumbers, strings", () => {
  for (const reply of [
    [7, 1_700_000_000_000, 2],
    ['7', '1700000000000', '2'],
  ])
    expect(REDIS_COMMANDS.publish.parse(reply, publishInput)).toEqual({
      seq: 7,
      timestamp: 1_700_000_000_000,
      receivers: 2,
    })
  for (const reply of [1, '1'])
    expect(REDIS_COMMANDS.validateGeneration.parse(reply, { roomId: 'room', inc: 'inc' })).toBe(true)
})

test('names Pub/Sub channels per database, as Pub/Sub spans every database', async () => {
  const clients = [0, 1].map((db) => new Redis({ lazyConnect: true, db }))
  onTestFinished(() => clients.forEach((redis) => redis.disconnect()))
  const channels = await Promise.all(
    clients.map(async (redis) => {
      const { backend, commands } = backendOn(redis)
      const publish = vi
        .spyOn(commands as unknown as Record<string, () => Promise<unknown>>, REDIS_COMMANDS.publish.name)
        .mockResolvedValue([1, 1, 0])
      await backend.publish({ key: 'chat', kind: 'text' }, '')
      return (publish.mock.calls[0] as unknown as [unknown[]])[0][1]
    }),
  )
  expect(channels[0]).not.toBe(channels[1])
})

test('moves the subscriber to the next master on each reconnect, so a failed master it never used does not hold it', async () => {
  const cluster = new Cluster([{ host: '127.0.0.1', port: 6379 }], {
    lazyConnect: true,
  })
  const masters = [0, 1].map(() => new Redis({ lazyConnect: true }))
  onTestFinished(() => [cluster, ...masters].forEach((redis) => redis.disconnect()))
  vi.spyOn(cluster, 'connect').mockImplementation(async () => {
    cluster.status = 'ready'
  })
  vi.spyOn(cluster, 'nodes').mockReturnValue(masters)
  const duplicates = masters.map((master) => vi.spyOn(master, 'duplicate'))
  const sockets = [await createSubscriberSocket(cluster), await createSubscriberSocket(cluster)]
  sockets.forEach((socket) => socket.disconnect())
  expect(duplicates.map((duplicate) => duplicate.mock.calls.length)).toEqual([1, 1])
})

test("takes the subscriber from a replica when the Cluster's pool labels no live master, as after a one-shard failover", async () => {
  const cluster = new Cluster([{ host: '127.0.0.1', port: 6379 }], {
    lazyConnect: true,
  })
  const promoted = new Redis({ lazyConnect: true })
  onTestFinished(() => [cluster, promoted].forEach((redis) => redis.disconnect()))
  vi.spyOn(cluster, 'connect').mockImplementation(async () => {
    cluster.status = 'ready'
  })
  // The pool never refreshed: the promoted replica is still labelled a replica.
  vi.spyOn(cluster, 'nodes').mockImplementation((role) => (role === 'master' ? [] : [promoted]))
  const duplicate = vi.spyOn(promoted, 'duplicate')
  const socket = await createSubscriberSocket(cluster)
  socket.disconnect()
  expect(duplicate).toHaveBeenCalledOnce()
})

test("passes a script more keys than a function call takes arguments, as a long-lived room's close does", async () => {
  const received: unknown[][] = []
  const redis = { dropGeneration: async (...args: unknown[]) => void received.push(args) }
  const keys = Array.from({ length: 500_000 }, (_, index) => `lane:${index}`)
  await callDefinedCommand(redis as never, 'dropGeneration', keys)
  expect((received[0] as [string[]])[0]).toHaveLength(500_000)
})
