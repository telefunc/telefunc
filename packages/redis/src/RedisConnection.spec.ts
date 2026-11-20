import { inspect } from 'node:util'
import { Redis } from 'ioredis'
import { afterEach, describe, expect, it } from 'vitest'
import { RedisConnection } from './RedisConnection.js'

const clients: Redis[] = []
afterEach(() => {
  for (const client of clients.splice(0)) client.disconnect()
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

/** All of a rejection as Node prints it, its causes and own properties too. */
async function printed(rejection: Promise<unknown>): Promise<string> {
  return inspect(await rejection.catch((error: unknown) => error), { depth: 8 })
}

function installed(redis: Redis): RedisConnection {
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
    expect(connection.client.options.tls).toEqual({ servername: 'redis.example' })
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

  it('a command a replica refuses with READONLY rejects with an error that names the replica and READONLY, and no cause', async () => {
    const connection = installed(client())
    const readOnly = replyError("READONLY You can't write against a read only replica.")
    const rejection = connection.send(() => Promise.reject(readOnly))
    await expect(rejection).rejects.toThrow(
      "Telefunc's Redis backend reached a replica at 127.0.0.1:6390, which refuses writes (READONLY): give installRedis() a client of the primary",
    )
    await expect(rejection).rejects.toMatchObject({
      name: 'BackendError',
      code: 'unavailable',
      outcome: 'may-have-run',
    })
    await expect(rejection).rejects.not.toHaveProperty('cause')
  })
})

describe('a failure of Redis, as a command rejects with it', () => {
  it("a refused login: Telefunc's error, with Redis's code and what to do, and not the password", async () => {
    const connection = installed(client())
    const refused = answered('WRONGPASS invalid username-password pair or user is disabled.', {
      name: 'auth',
      args: ['tf', 'pw-SECRET'],
    })
    const rejection = connection.send(() => Promise.reject(refused))
    await expect(rejection).rejects.toThrow(
      "Redis at 127.0.0.1:6390 refused the login of Telefunc's Redis backend (WRONGPASS): check the username and password of the client given to installRedis()",
    )
    const text = await printed(rejection)
    expect(text).not.toContain('pw-SECRET')
    expect(text).not.toContain('args')
  })

  it("a script Redis fails: Telefunc's error, with Redis's code, and not the message it was publishing", async () => {
    const connection = installed(client())
    const failed = answered(
      'WRONGTYPE Operation against a key holding the wrong kind of value script: 06f79f6ab4, on @user_script:1.',
      {
        name: 'evalsha',
        args: ['06f79f6ab4', '2', 'tf:0:seq:{k}', 'tf:0:t:{k}', Buffer.from('body-SECRET')],
      },
    )
    const rejection = connection.send(() => Promise.reject(failed))
    await expect(rejection).rejects.toThrow(
      "Redis at 127.0.0.1:6390 failed a command of Telefunc's Redis backend (WRONGTYPE): a key under Telefunc's prefix holds a value Telefunc didn't write; keep that prefix to Telefunc",
    )
    const text = await printed(rejection)
    expect(text).not.toContain('body-SECRET')
    expect(text).not.toContain('args')
  })
})
