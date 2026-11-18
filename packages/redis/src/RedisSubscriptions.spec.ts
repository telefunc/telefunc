import { describe, expect, it } from 'vitest'
import { createDeferred, type DriverAttempt } from 'telefunc/__internal'
import type { SubscriberSocket } from './ioredis.js'
import { NewSubscriptionWait } from './NewSubscriptionWait.js'
import type { RedisConnection } from './RedisConnection.js'
import { RedisSubscriptions } from './RedisSubscriptions.js'

const TEXT = 'text-channel'
const BINARY = 'binary-channel'
/** A key's two kinds share a seq, so a text publish waits for both, and fails only on its own, as `broadcastWaitRule`
 *  has it. */
const RULE = { queue: TEXT, channels: [TEXT, BINARY], failsOn: [TEXT], budget: TEXT }

type ReadStarts = () => Promise<Map<string, { seq: number; timestamp: number }>>

function refusal(): Error {
  const error = new Error('NOPERM this user has no permissions to access the channel')
  error.name = 'ReplyError'
  return error
}

function setup(subscribe: SubscriberSocket['subscribe'] = async () => {}) {
  const socket: SubscriberSocket = {
    connect: async () => {},
    subscribe,
    unsubscribe: async () => {},
    ping: async () => {},
    disconnect() {},
    on: () => socket,
  }
  const connection = { openSubscriberSocket: async () => socket } as unknown as RedisConnection
  const subscriptions = new RedisSubscriptions({ connection })
  const listen = (channel: string, readStarts: ReadStarts = async () => new Map()): DriverAttempt =>
    subscriptions.subscribe({
      channel,
      invalidationChannel: null,
      frames: { fanIn: false, placed: true, text: true, lanesEndWithMembers: false },
      readStarts,
      receiver: { deliver() {} },
      reportMissed() {},
    })
  return { subscriptions, listen, sends: new NewSubscriptionWait({ subscriptions }) }
}

describe('a publish waiting on its key’s new subscriptions', () => {
  it('goes out once the other kind’s start read, failed and read again, is ready: a failed read is no end', async () => {
    const { subscriptions, listen, sends } = setup()
    listen(TEXT)
    await Promise.all(subscriptions.establishingOn([TEXT]))
    const reads = [
      createDeferred<Map<string, { seq: number; timestamp: number }>>(),
      createDeferred<Map<string, { seq: number; timestamp: number }>>(),
    ]
    let read = 0
    const binary = listen(BINARY, () => reads[read++]!.promise)

    const waiting = sends.waitTurn(RULE, 'hello')!
    reads[0]!.reject(new Error('the command connection dropped'))
    await expect.poll(() => read, { timeout: 3_000 }).toBe(2)
    expect(binary.state()).toBe('establishing')
    reads[1]!.resolve(new Map())

    await expect(waiting.turn).resolves.toBe('hello')
    expect(binary.state()).toBe('ready')
  })

  it('fails with the reason when Redis refuses the subscription of its own kind it waits for', async () => {
    const refused = refusal()
    const { listen, sends } = setup(async (...channels) => {
      if (channels.includes(TEXT)) throw refused
    })
    listen(TEXT)
    listen(BINARY)

    await expect(sends.waitTurn(RULE, 'hello')!.turn).rejects.toBe(refused)
  })

  it('goes out when Redis refuses only the other kind’s subscription, which it waited for', async () => {
    const { listen, sends } = setup(async (...channels) => {
      if (channels.includes(BINARY)) throw refusal()
    })
    listen(TEXT)
    listen(BINARY)

    await expect(sends.waitTurn(RULE, 'hello')!.turn).resolves.toBe('hello')
  })
})
