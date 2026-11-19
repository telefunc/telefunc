import { afterEach, describe, expect, it, vi } from 'vitest'
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
  /** What Redis sends the subscriber socket: a frame on a channel. */
  let publish = (_channel: string, _frame: Buffer) => {}
  const socket: SubscriberSocket = {
    connect: async () => {},
    subscribe,
    unsubscribe: async () => {},
    ping: async () => {},
    disconnect() {},
    on(event: string, listener: (channel: Buffer, frame: Buffer) => void) {
      if (event === 'messageBuffer') publish = (channel, frame) => listener(Buffer.from(channel), frame)
      return socket
    },
  }
  const connection = { openSubscriberSocket: async () => socket } as unknown as RedisConnection
  const subscriptions = new RedisSubscriptions({ connection })
  const delivered: string[] = []
  const listen = (
    channel: string,
    readStarts: ReadStarts = async () => new Map(),
    frames = { fanIn: false, placed: true, text: true, lanesEndWithMembers: false },
  ): DriverAttempt =>
    subscriptions.subscribe({
      channel,
      invalidationChannel: null,
      frames,
      readStarts,
      receiver: {
        deliver(payload) {
          delivered.push(String(payload))
        },
      },
      reportMissed() {},
    })
  return {
    subscriptions,
    listen,
    delivered,
    publish: (channel: string, frame: Buffer) => publish(channel, frame),
    sends: new NewSubscriptionWait({ subscriptions }),
  }
}

/** A frame as commands.ts's scripts write it: the lane key on a fan-in channel, the place, then
 *  `[seq_hi][seq_lo][ts_hi][ts_lo][payload]`, u32 big-endian. */
function frameOf(place: number, text: string, { lane, seq = place }: { lane?: string; seq?: number } = {}): Buffer {
  const header = Buffer.alloc(24)
  header.writeBigUInt64BE(BigInt(place), 0)
  header.writeUInt32BE(Math.floor(seq / 0x1_0000_0000), 8)
  header.writeUInt32BE(seq % 0x1_0000_0000, 12)
  header.writeUInt32BE(0, 16)
  header.writeUInt32BE(1_700_000_000, 20)
  const laneKey = lane === undefined ? Buffer.alloc(0) : Buffer.concat([Buffer.alloc(2), Buffer.from(lane, 'latin1')])
  if (lane !== undefined) laneKey.writeUInt16BE(lane.length, 0)
  return Buffer.concat([laneKey, header, Buffer.from(text)])
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

describe("a frame on a subscribed channel that isn't in the form Telefunc's scripts publish", () => {
  const reports: unknown[][] = []
  afterEach(() => {
    vi.restoreAllMocks()
    reports.length = 0
  })
  const spyReports = () => vi.spyOn(console, 'error').mockImplementation((...args) => void reports.push(args))
  const FOREIGN: [string, Buffer][] = [
    ['3 bytes', Buffer.from([1, 2, 3])],
    ['a place and no ordering header', frameOf(1, '').subarray(0, 12)],
    ['a seq of 0', frameOf(1, 'x', { seq: 0 })],
    ['a place of 0', frameOf(0, 'x', { seq: 1 })],
  ]

  for (const [name, frame] of FOREIGN) {
    it(`is dropped, reported once without calling itself a bug, and the subscription goes on: ${name}`, async () => {
      spyReports()
      const { subscriptions, listen, publish, delivered } = setup()
      listen(TEXT)
      await Promise.all(subscriptions.establishingOn([TEXT]))

      publish(TEXT, frame)
      publish(TEXT, frame)
      publish(TEXT, frameOf(1, 'after'))

      expect(delivered).toEqual(['after'])
      expect(reports).toHaveLength(1)
      expect(String(reports[0])).toContain(`Telefunc's Redis backend dropped a message on ${TEXT}`)
      expect(String(reports[0])).not.toMatch(/bug/i)
    })
  }

  it('is dropped where its lane key runs past its end, on a channel of many lanes', async () => {
    spyReports()
    const { subscriptions, listen, publish, delivered } = setup()
    listen(TEXT, async () => new Map(), { fanIn: true, placed: true, text: true, lanesEndWithMembers: false })
    await Promise.all(subscriptions.establishingOn([TEXT]))
    const overlong = frameOf(1, 'x', { lane: 'lane' })
    overlong.writeUInt16BE(500, 0)

    publish(TEXT, overlong)
    publish(TEXT, frameOf(1, 'after', { lane: 'lane' }))

    expect(delivered).toEqual(['after'])
    expect(reports).toHaveLength(1)
  })

  it('is dropped where it arrives while the subscription is confirmed, which then goes live', async () => {
    spyReports()
    const starts = createDeferred<Map<string, { seq: number; timestamp: number }>>()
    const { listen, publish, delivered } = setup()
    const attempt = listen(TEXT, () => starts.promise)
    await new Promise((resolve) => setImmediate(resolve))

    publish(TEXT, Buffer.from([1, 2, 3]))
    publish(TEXT, frameOf(1, 'early'))
    starts.resolve(new Map())

    await expect.poll(() => attempt.state()).toBe('ready')
    expect(delivered).toEqual(['early'])
    expect(reports).toHaveLength(1)
    expect(String(reports[0])).not.toMatch(/bug/i)
  })
})
