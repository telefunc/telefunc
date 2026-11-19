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
