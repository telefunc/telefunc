import { describe, expect, it } from 'vitest'
import { createDeferred, type DriverAttempt } from 'telefunc/__internal'
import type { SubscriberSocket } from './ioredis.js'
import { NewSubscriptionWait } from './NewSubscriptionWait.js'
import type { RedisConnection } from './RedisConnection.js'
import { RedisSubscriptions } from './RedisSubscriptions.js'

const TEXT = 'text-channel'
const BINARY = 'binary-channel'
/** A key's two kinds share a seq, so a publish of either waits for both, as `broadcastWaitRule` has it. */
const RULE = { queue: TEXT, channels: [TEXT, BINARY], budget: TEXT }

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
  it('goes out when the other kind’s start read fails: that is no refusal of this publish', async () => {
    const { subscriptions, listen, sends } = setup()
    listen(TEXT)
    await Promise.all(subscriptions.establishingOn([TEXT]))
    const read = createDeferred<Map<string, { seq: number; timestamp: number }>>()
    const binary = listen(BINARY, () => read.promise)

    const waiting = sends.waitTurn(RULE, 'hello')!
    read.reject(new Error('the command connection dropped'))

    await expect(waiting.turn).resolves.toBe('hello')
    expect(binary.state()).toBe('closed')
  })

  it('fails with the reason when Redis refuses a subscription it waits for', async () => {
    const refused = refusal()
    const { listen, sends } = setup(async (...channels) => {
      if (channels.includes(BINARY)) throw refused
    })
    listen(TEXT)
    listen(BINARY)

    await expect(sends.waitTurn(RULE, 'hello')!.turn).rejects.toBe(refused)
  })
})
