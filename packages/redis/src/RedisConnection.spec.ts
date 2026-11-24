import { createServer, type Server } from 'node:net'
import { setTimeout as sleep } from 'node:timers/promises'
import { inspect } from 'node:util'
import { Cluster, Redis } from 'ioredis'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { RedisConnection } from './RedisConnection.js'
import { addressOf, createSubscriberSocket, deploymentOf, type RedisClient } from './ioredis.js'
import { RAW_COMMANDS, type RawCommand } from './rawCommands.js'
import { redisFailure } from './redisFailure.js'
import { RedisSubscriptions } from './RedisSubscriptions.js'

const clients: RedisClient[] = []
afterEach(() => {
  for (const client of clients.splice(0)) client.disconnect()
  vi.restoreAllMocks()
})

/** A client that connects only on a command, which these tests never send. */
function client(options: { username?: string; password?: string; tls?: object; port?: number } = {}): Redis {
  const redis = new Redis({ lazyConnect: true, host: '127.0.0.1', port: 6390, ...options })
  clients.push(redis)
  return redis
}

function replyError(message: string): Error {
  const error = new Error(message)
  error.name = 'ReplyError'
  return error
}

/** A reply error as ioredis gives it: the command it answered, with its arguments, is on it. */
function answered(message: string, command: { name: string; args: unknown[] }): Error {
  return Object.assign(replyError(message), { command })
}

function installed(redis: RedisClient): RedisConnection {
  const connection = new RedisConnection({ redis, prefix: 'tf:' })
  connection.holdFor(redis)
  return connection
}

describe('a client installed again', () => {
  it('with another user and password: the backend connects as that user, and closes its connection of the first', async () => {
    const connection = installed(client({ username: 'alice', password: 'one' }))
    const first = connection.client
    connection.holdFor(client({ username: 'bob', password: 'two' }))
    expect(connection.client.options).toMatchObject({ username: 'bob', password: 'two' })
    expect((await connection.openSubscriberSocket()) as Redis).toMatchObject({
      options: { username: 'bob', password: 'two' },
    })
    expect(first.status).toBe('end')
  })

  it('with TLS where the first had none: the backend connects with TLS', () => {
    const connection = installed(client())
    connection.holdFor(client({ tls: { servername: 'redis.example' } }))
    const { options } = connection.client
    expect('tls' in options ? options.tls : undefined).toEqual({ servername: 'redis.example' })
  })

  it('a client of the same options: the backend keeps its connection', () => {
    const connection = installed(client({ username: 'alice', password: 'one' }))
    const first = connection.client
    connection.holdFor(client({ username: 'alice', password: 'one' }))
    expect(connection.client).toBe(first)
    expect(first.status).toBe('wait')
  })

  it('the same client twice: the backend keeps its connection, which ends once that client does', async () => {
    const redis = client({ username: 'alice', password: 'one' })
    const connection = installed(redis)
    const first = connection.client
    connection.holdFor(redis)
    expect(connection.client).toBe(first)
    redis.disconnect()
    await new Promise((resolve) => setImmediate(resolve))
    expect(first.status).toBe('end')
  })

  it("a replaced client's end leaves the backend's new connection open", async () => {
    const replaced = client({ username: 'alice', password: 'one' })
    const connection = installed(replaced)
    connection.holdFor(client({ username: 'bob', password: 'two' }))
    const current = connection.client
    replaced.disconnect()
    await new Promise((resolve) => setImmediate(resolve))
    expect(current.status).toBe('wait')
  })
})

describe('a client of a replica', () => {
  it('a Sentinel client set to reach a replica is refused at install, saying so', () => {
    const sentinel = new Redis({
      lazyConnect: true,
      sentinels: [{ host: '127.0.0.1', port: 26390 }],
      name: 'main',
      role: 'slave',
    })
    clients.push(sentinel)
    expect(() => new RedisConnection({ redis: sentinel, prefix: 'tf:' })).toThrow(
      "installRedis(): the client's Sentinel role is 'slave', so it reaches a replica, and Telefunc writes to the primary: leave out `role`",
    )
  })

  it('a command a replica refuses with READONLY is an error that names the replica and READONLY, and no cause', () => {
    const readOnly = replyError("READONLY You can't write against a read only replica.")
    const failure = redisFailure(readOnly, client(), true)
    expect(failure).toMatchObject({
      name: 'BackendError',
      message:
        "Telefunc's Redis backend reached a replica at 127.0.0.1:6390, which refuses writes (READONLY): give installRedis() a client of the primary",
      code: 'unavailable',
      outcome: 'not-run',
    })
    expect(failure).not.toHaveProperty('cause')
  })
})

/** A port whose server refuses every login, as Redis does a wrong password, and closes the connection. */
async function refusingNode(): Promise<{ port: number; server: Server }> {
  const server = createServer((socket) => {
    socket.on('error', () => {})
    socket.once('data', () => socket.end('-WRONGPASS invalid username-password pair or user is disabled.\r\n'))
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('the refusing node has no port')
  return { port: address.port, server }
}

describe('a backend client that ended before anything waited on it', () => {
  it('a refused login: a command made after the end rejects with the refusal, not as unreachable', async () => {
    const { port, server } = await refusingNode()
    // The app's client connects at once, so the backend's copy of it does too, and gives up at its first failed
    // connect, as the app's retry strategy says.
    const app = new Redis({ host: '127.0.0.1', port, password: 'pw', retryStrategy: () => null })
    app.on('error', () => {})
    clients.push(app)
    const connection = installed(app)
    while (connection.client.status !== 'end') await sleep(10)
    const failure = await connection.send(RAW_COMMANDS.hkeys, 'tf:0:k').then(
      () => 'answered',
      (error: unknown) => error,
    )
    server.close()

    expect(failure).toMatchObject({
      code: 'denied',
      outcome: 'not-run',
      message: expect.stringContaining("refused the login of Telefunc's Redis backend (WRONGPASS)"),
    })
  })
})

describe('a failure of Redis, as the backend reports it', () => {
  it("a refused login: Telefunc's error, with Redis's code and what to do, and not the password", () => {
    const refused = answered('WRONGPASS invalid username-password pair or user is disabled.', {
      name: 'auth',
      args: ['tf', 'pw-SECRET'],
    })
    const failure = redisFailure(refused, client(), true)
    expect(failure).toMatchObject({
      message:
        "Redis at 127.0.0.1:6390 refused the login of Telefunc's Redis backend (WRONGPASS): check the username and password of the client given to installRedis()",
    })
    const text = inspect(failure, { depth: 8 })
    expect(text).not.toContain('pw-SECRET')
    expect(text).not.toContain('args')
  })

  it("a script Redis fails: Telefunc's error, with Redis's code, and not the message it was publishing", () => {
    const failed = answered(
      'WRONGTYPE Operation against a key holding the wrong kind of value script: 06f79f6ab4, on @user_script:1.',
      {
        name: 'evalsha',
        args: ['06f79f6ab4', '2', 'tf:0:seq:{k}', 'tf:0:t:{k}', Buffer.from('body-SECRET')],
      },
    )
    const failure = redisFailure(failed, client(), true)
    expect(failure).toMatchObject({
      message:
        "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (WRONGTYPE): a key under Telefunc's prefix holds a value Telefunc didn't write; keep that prefix to Telefunc",
      outcome: 'may-have-run',
    })
    const text = inspect(failure, { depth: 8 })
    expect(text).not.toContain('body-SECRET')
    expect(text).not.toContain('args')
  })
})

/** What a real client rejects a write with when nothing listens at its port: `options` decide which error it is. */
async function rejectionOfWrite(options: {
  enableOfflineQueue: boolean
  maxRetriesPerRequest: number | null
  retryStrategy: () => number | null
}): Promise<unknown> {
  const redis = new Redis({ host: '127.0.0.1', port: 6390, lazyConnect: true, ...options })
  redis.on('error', () => {})
  clients.push(redis)
  return redis.set('tf:k', 'v').then(
    () => 'answered',
    (error: unknown) => error,
  )
}

describe('a write whose connection failed, as ioredis rejects it', () => {
  it("never written, as the backend's client keeps no offline queue: it didn't run", async () => {
    const unsent = await rejectionOfWrite({
      enableOfflineQueue: false,
      maxRetriesPerRequest: null,
      retryStrategy: () => 50,
    })
    expect(redisFailure(unsent, client(), true)).toMatchObject({ code: 'unreachable', outcome: 'not-run' })
  })

  it('in the queue when the connection closed for good: it may have run', async () => {
    const closed = await rejectionOfWrite({
      enableOfflineQueue: true,
      maxRetriesPerRequest: null,
      retryStrategy: () => null,
    })
    expect(closed).toMatchObject({ message: 'Connection is closed.' })
    expect(redisFailure(closed, client(), true)).toMatchObject({ code: 'unreachable', outcome: 'may-have-run' })
  })

  it('in the queue when ioredis gave up retrying it: it may have run', async () => {
    const gaveUp = await rejectionOfWrite({
      enableOfflineQueue: true,
      maxRetriesPerRequest: 0,
      retryStrategy: () => 50,
    })
    expect(gaveUp).toMatchObject({ name: 'MaxRetriesPerRequestError' })
    expect(redisFailure(gaveUp, client(), true)).toMatchObject({ code: 'unreachable', outcome: 'may-have-run' })
  })
})

/** What `send` rejects with when the reply to the raw command `command` is lost, on a command client that was ready. */
async function replyLost(command: RawCommand<unknown>) {
  const connection = installed(client())
  vi.spyOn(connection.client, 'sendCommand').mockImplementation((...[request]) => {
    request.reject(new Error('Connection is closed.'))
    return request.promise
  })
  const sent = connection.send(command, 'tf:0:k')
  connection.client.emit('ready')
  return sent.then(
    () => 'answered',
    (error: unknown) => error,
  )
}

describe('a command sent without a script, read or write as ioredis tells it', () => {
  it('UNLINK, a write: its lost reply says it may have run', async () => {
    expect(await replyLost(RAW_COMMANDS.unlink)).toMatchObject({ code: 'unreachable', outcome: 'may-have-run' })
  })

  it("HKEYS, a read: its lost reply says it didn't run", async () => {
    expect(await replyLost(RAW_COMMANDS.hkeys)).toMatchObject({ code: 'unreachable', outcome: 'not-run' })
  })
})

describe('the address a failure names', () => {
  function cluster(nodes: ConstructorParameters<typeof Cluster>[0]): Cluster {
    const redis = new Cluster(nodes, { lazyConnect: true })
    clients.push(redis)
    return redis
  }
  function standalone(redis: Redis): Redis {
    clients.push(redis)
    return redis
  }

  const STARTUP_NODES: Array<[string, ConstructorParameters<typeof Cluster>[0], string]> = [
    ['an object', [{ host: '10.0.0.1', port: 7001 }], '10.0.0.1:7001'],
    ['an object without a host', [{ port: 7002 }], '127.0.0.1:7002'],
    ['a port', [7003], '127.0.0.1:7003'],
    ['a port as a string', ['7004'], '127.0.0.1:7004'],
    ['host:port', ['10.0.0.5:7005'], '10.0.0.5:7005'],
    ['a redis: URL with a password', ['redis://:pw-SECRET@10.0.0.6:7006'], '10.0.0.6:7006'],
    ['a rediss: URL with a username and password', ['rediss://app:pw-SECRET@10.0.0.7:7007'], '10.0.0.7:7007'],
    ['a URL without its scheme', ['//app:pw-SECRET@10.0.0.8:7008'], '10.0.0.8:7008'],
    ['a URL without a port', ['redis://:pw-SECRET@10.0.0.9'], '10.0.0.9:6379'],
    ['an IPv6 URL', ['redis://:pw-SECRET@[::1]:7010'], '[::1]:7010'],
    ['a socket path', ['/run/redis/node.sock'], '/run/redis/node.sock'],
    ['several', [{ host: '10.0.0.1', port: 7001 }, 'redis://:pw-SECRET@10.0.0.2:7002'], '10.0.0.1:7001, 10.0.0.2:7002'],
  ]
  it.each(STARTUP_NODES)(
    'a Cluster whose startup node is %s: its host and port, and no credential',
    (_form, nodes, address) => {
      const redis = cluster(nodes)
      expect(addressOf(redis)).toBe(address)
      const refused = answered('NOPERM this user has no permissions to run the evalsha command', {
        name: 'evalsha',
        args: [],
      })
      expect(inspect(redisFailure(refused, redis, true))).not.toContain('SECRET')
    },
  )

  it.each([
    [
      'a URL with a password',
      () => new Redis('redis://:pw-SECRET@10.0.0.11:7011', { lazyConnect: true }),
      '10.0.0.11:7011',
    ],
    ['a port and a host', () => new Redis(7012, '10.0.0.12', { lazyConnect: true }), '10.0.0.12:7012'],
    ['a socket path', () => new Redis({ path: '/run/redis.sock', lazyConnect: true }), '/run/redis.sock'],
  ])('a client given %s: where it connects, and no credential', (_form, make, address) => {
    expect(addressOf(standalone(make()))).toBe(address)
  })
})

describe('a state of Redis that a later call may get past', () => {
  it('NOREPLICAS: unavailable, saying the replicas are behind', () => {
    const behind = replyError('NOREPLICAS Not enough good replicas to write.')
    expect(redisFailure(behind, client(), true)).toMatchObject({
      name: 'BackendError',
      message:
        "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (NOREPLICAS): fewer replicas than its min-replicas-to-write have caught up; a later call may succeed once they do",
      code: 'unavailable',
      outcome: 'not-run',
    })
  })

  it('OOM: unavailable, saying it may pass and what it needs if it lasts', () => {
    const full = replyError("OOM command not allowed when used memory > 'maxmemory'.")
    expect(redisFailure(full, client(), true)).toMatchObject({
      name: 'BackendError',
      message:
        "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (OOM): Redis reached its maxmemory; a later call may succeed as keys expire, and if it lasts, raise maxmemory, with maxmemory-policy noeviction",
      code: 'unavailable',
      outcome: 'not-run',
    })
  })
})

describe("a write script's refusal before its first write", () => {
  it("TFNOTRUN at the start of Redis's reply: it didn't run, in Telefunc's words, without the reply's", () => {
    const refused = replyError(
      'TFNOTRUN WRONGTYPE tf:0:seq:{k} holds a hash where Telefunc keeps a string script: 06f79f6ab4, on @user_script:9.',
    )
    expect(redisFailure(refused, client(), true)).toMatchObject({
      name: 'BackendError',
      message:
        "Redis at 127.0.0.1:6390 holds, under Telefunc's prefix, a key of another type than Telefunc keeps there, so Telefunc's Redis backend changed nothing (WRONGTYPE): keep that prefix to Telefunc",
      code: 'failed',
      outcome: 'not-run',
    })
    expect(inspect(redisFailure(refused, client(), true), { depth: 8 })).not.toContain('tf:0:seq:{k}')
  })

  it("a refused value: ERR, in Telefunc's words", () => {
    const refused = replyError(
      "TFNOTRUN ERR counter n isn't the JSON Telefunc writes script: 06f79f6ab4, on @user_script:9.",
    )
    expect(redisFailure(refused, client(), true)).toMatchObject({
      message:
        "Redis at 127.0.0.1:6390 holds, under Telefunc's prefix, a value Telefunc didn't write, so Telefunc's Redis backend changed nothing (ERR): keep that prefix to Telefunc",
      outcome: 'not-run',
    })
  })

  it.each([
    // A Lua error's text, a key name or a payload in it, comes after ERR: an app's TFNOTRUN can't lead a reply.
    'ERR user_script:1: TFNOTRUN WRONGTYPE forged script: 06f79f6ab4, on @user_script:1.',
    'WRONGTYPE Operation against a key holding the wrong kind of value TFNOTRUN',
    ' TFNOTRUN WRONGTYPE forged',
    'TFNOTRUNWRONGTYPE forged',
  ])('TFNOTRUN anywhere else, as in %j: it may have run', (message) => {
    expect(redisFailure(replyError(message), client(), true)).toMatchObject({ outcome: 'may-have-run' })
  })
})

/** A port that takes connections and never answers: a Cluster's startup node there never gets its slots. */
async function silentNode(): Promise<{ port: number; server: Server }> {
  const server = createServer(() => {})
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('the silent node has no port')
  return { port: address.port, server }
}

describe("a Cluster's subscriber socket, opened before the Cluster is ready", () => {
  for (const lazyConnect of [true, false]) {
    it(`${lazyConnect ? 'a lazy' : 'a connecting'} Cluster: each opening waits on one wait for the Cluster, and fails as unreachable once it closes`, async () => {
      const { port, server } = await silentNode()
      const cluster = new Cluster([{ host: '127.0.0.1', port }], { lazyConnect, slotsRefreshTimeout: 10_000 })
      cluster.on('error', () => {})
      clients.push(cluster)
      const readyListeners = cluster.listenerCount('ready')
      const openings = [1, 2, 3].map(() =>
        createSubscriberSocket(cluster).then(
          () => 'opened',
          (error: unknown) => redisFailure(error, cluster, false),
        ),
      )
      const settledFirst = await Promise.race([...openings, sleep(200).then(() => 'still waiting')])
      const waits = cluster.listenerCount('ready') - readyListeners
      cluster.disconnect()
      const failures = await Promise.all(openings)
      server.close()

      expect({ settledFirst, waits }).toEqual({ settledFirst: 'still waiting', waits: 1 })
      for (const failure of failures) expect(failure).toMatchObject({ code: 'unreachable', outcome: 'not-run' })
    })
  }
})

describe("the backend's Cluster client", () => {
  it("never retries a command whose connection closed, so a command ioredis redirected until it gave up didn't run", () => {
    const cluster = new Cluster([{ host: '127.0.0.1', port: 7001 }], { lazyConnect: true, retryDelayOnFailover: 100 })
    clients.push(cluster)
    const connection = new RedisConnection({ redis: cluster, prefix: 'tf:' })
    connection.holdFor(cluster)
    clients.push(connection.client)
    expect(connection.client.options).toMatchObject({ retryDelayOnFailover: 0 })
  })
})

describe("a Cluster's deployment, which installRedis() keys its backend by", () => {
  it('a Cluster of the same startup nodes is the same deployment; one of other nodes is another', () => {
    const cluster = (port: number) => {
      const redis = new Cluster([{ host: '127.0.0.1', port }], { lazyConnect: true })
      clients.push(redis)
      return redis
    }
    const first = cluster(7000)
    expect(deploymentOf(first)).toBe(deploymentOf(first))
    expect(deploymentOf(cluster(7000))).toBe(deploymentOf(first))
    expect(deploymentOf(cluster(7001))).not.toBe(deploymentOf(first))
  })
})

describe('a key prefix', () => {
  it('that is not a well-formed string is refused at install, saying so; one with a `}`, a `:` or a space is not', () => {
    const at = (prefix: string) => () => new RedisConnection({ redis: client(), prefix })
    expect(at('tf:\ud800')).toThrow('installRedis(): the prefix must be a well-formed string')
    for (const prefix of ['}app:', 'a}b:', 'my app ']) expect(at(prefix)).not.toThrow()
  })
})

describe('a Cluster whose slot gets another master', () => {
  it('has its subscriptions follow the move, with the master the slot had: a failover cuts no subscriber connection on another node', () => {
    const followMoves = vi.spyOn(RedisSubscriptions.prototype, 'followMoves').mockResolvedValue()
    const cluster = new Cluster([{ host: '127.0.0.1', port: 6390 }], { lazyConnect: true })
    clients.push(cluster)
    const commands = installed(cluster).client
    if (!(commands instanceof Cluster)) throw new Error('A Cluster got a client of another kind')
    // ioredis's slot map, as a refresh leaves it: each slot's master, then its replicas.
    commands.slots[0] = ['127.0.0.1:7001', '127.0.0.1:7003']
    commands.slots[1] = ['127.0.0.1:7002']
    commands.emit('refresh')
    commands.slots[1] = ['127.0.0.1:7002', '127.0.0.1:7004']
    commands.emit('refresh')
    expect(followMoves).not.toHaveBeenCalled()
    commands.slots[0] = ['127.0.0.1:7005']
    commands.emit('refresh')
    expect(followMoves.mock.calls.map(([moved]) => moved)).toEqual([new Map([[0, '127.0.0.1:7001']])])
  })
})
