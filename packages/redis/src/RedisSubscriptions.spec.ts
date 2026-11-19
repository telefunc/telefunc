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

function replyError(message: string): Error {
  const error = new Error(message)
  error.name = 'ReplyError'
  return error
}

function refusal(): Error {
  return replyError('NOPERM this user has no permissions to access the channel')
}

/** Replies Redis 5.0.14, 6.0.20, 6.2.24, 7.0.15, 7.2.17, 7.4.12, 8.0.6 and 8.2.11 and Dragonfly gave through ioredis to a
 *  call its user's ACL denies or a replica refuses, at top level and inside a script. */
const REFUSALS = [
  "NOPERM this user has no permissions to run the 'get' command or its subcommand",
  'NOPERM this user has no permissions to access one of the channels used as arguments',
  "NOPERM User probe has no permissions to run the 'get' command",
  'NOPERM No permissions to access a key',
  "ERR Error running script (call to f_d3c21d0c2b9ca22f82737626a27bcaf5d288f99f): @user_script:1: @user_script: 1: The user executing the script can't run this command or subcommand",
  "ERR Error running script (call to f_9ddb5d56e1d5c3a3b21c1aad774273186b4a6d34): @user_script:1: @user_script: 1: The user executing the script can't publish to the channel mentioned in the command",
  "ERR The user executing the script can't run this command or subcommand script: d3c21d0c2b9ca22f82737626a27bcaf5d288f99f, on @user_script:1.",
  "ERR ACL failure in script: User probe has no permissions to run the 'get' command script: d3c21d0c2b9ca22f82737626a27bcaf5d288f99f, on @user_script:1.",
  'ERR ACL failure in script: No permissions to access a channel script: 9ddb5d56e1d5c3a3b21c1aad774273186b4a6d34, on @user_script:1.',
  "READONLY You can't write against a read only replica.",
  "READONLY You can't write against a read only replica. script: bd6f3018b28bbdb36260f19d3c44afed740db987, on @user_script:1.",
  "ERR Error running script (call to f_bd6f3018b28bbdb36260f19d3c44afed740db987): @user_script:1: @user_script: 1: -READONLY You can't write against a read only replica.",
  'ERR Error running script (call to ee33e8171bc4a2b477d56ebe06c4d9545f5b840a): @user_script:2: -NOPERM probe has no ACL permissions',
]
/** Replies that pass, and a dropped command connection. */
const PASSING = [
  replyError('LOADING Redis is loading the dataset in memory'),
  replyError('BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSCRIPT.'),
  new Error('Connection is closed.'),
]

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

describe('a start read Redis answers with an error', () => {
  for (const message of REFUSALS) {
    it(`closes the subscription with Redis's error, where it reads ${message.slice(0, 60)}`, async () => {
      const refused = replyError(message)
      let reads = 0
      const { subscriptions, listen } = setup()
      const attempt = listen(TEXT, async () => {
        reads++
        throw refused
      })
      const closed = new Promise((resolve) => attempt.onStateChange((state, reason) => resolve([state, reason])))

      await expect(Promise.all(subscriptions.establishingOn([TEXT]))).rejects.toBe(refused)
      await expect(closed).resolves.toEqual(['closed', refused])
      expect(reads).toBe(1)
    })
  }

  for (const error of PASSING) {
    it(`reads again, where it is ${error.message.slice(0, 40)}`, async () => {
      let reads = 0
      const { listen } = setup()
      const attempt = listen(TEXT, async () => {
        if (++reads === 1) throw error
        return new Map()
      })

      await expect.poll(() => attempt.state(), { timeout: 3_000 }).toBe('ready')
      expect(reads).toBe(2)
    })
  }
})

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
