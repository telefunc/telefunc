import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { BackendError, Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'
import { installRedis } from './index.js'

// A socket that doesn't exist: every connection fails at once, on any machine, and no command is sent.
const redis = new Redis({ path: '/nonexistent/telefunc-redis-backend-spec.sock', retryStrategy: () => 50 })
redis.on('error', () => {})
installRedis(redis)
afterAll(() => redis.disconnect())

describe('a call Redis fails, caught as an app catches it', () => {
  it("Broadcast.publish() rejects with a BackendError that says what failed and that it didn't run", async () => {
    const failure = await Promise.resolve(Broadcast.publish('chat', 'hi')).catch((error: unknown) => error)
    expect(failure).toBeInstanceOf(BackendError)
    expect(failure).toMatchObject({ code: 'unreachable', outcome: 'not-run' })
  })

  // The file's first Room call, so it also imports Room's server code, which Room loads on first use (`load.ts` in
  // packages/room). On Node 22, in the full unit suite, the call took up to 3.19 s, against 52 ms once the code is
  // loaded. A busier machine took it past 5 s, so the budget is three times that sum, rounded up.
  it("Room.create() rejects with a BackendError that says what failed and that it didn't run", async () => {
    const failure = await Room.create('lobby').catch((error: unknown) => error)
    expect(failure).toBeInstanceOf(BackendError)
    expect(failure).toMatchObject({ code: 'unreachable', outcome: 'not-run' })
  }, 10_000)
})
