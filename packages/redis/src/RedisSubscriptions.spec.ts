import { inspect } from 'node:util'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { createDeferred, type DriverAttempt } from 'telefunc/__internal'
import { slotOf, type FormerMaster, type RedisClient, type SubscriberSocket } from './ioredis.js'
import { NewSubscriptionWait } from './NewSubscriptionWait.js'
import type { RedisConnection } from './RedisConnection.js'
import { RedisSubscriptions } from './RedisSubscriptions.js'
import type { Starts } from './orderRecord.js'

const TEXT = 'text-channel'
const BINARY = 'binary-channel'
/** A key's two kinds share a seq, so a text publish waits for both, and fails only on its own, as `broadcastWaitRule`
 *  has it. */
const RULE = { queue: TEXT, channels: [TEXT, BINARY], failsOn: [TEXT], budget: TEXT }

type ReadStarts = (on?: RedisClient) => Promise<Starts>

/** No lane has a commit yet, on the history `authority`. */
const none = (authority = 'a'): Starts => ({ authority, starts: new Map() })

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
  'TFINFO INFO',
]
/** Replies that pass, and a dropped command connection. */
const PASSING = [
  replyError('LOADING Redis is loading the dataset in memory'),
  replyError('BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSCRIPT.'),
  new Error('Connection is closed.'),
  replyError("TFSTALE a subscription's start reached a replica"),
]

function setup(
  subscribe: SubscriberSocket['subscribe'] = async () => {},
  connect: SubscriberSocket['connect'] = async () => {},
) {
  /** What Redis sends the subscriber socket: a frame on a channel. */
  let publish = (_channel: string, _frame: Buffer) => {}
  /** The subscriber connection drops. */
  let close = () => {}
  const socket: SubscriberSocket = {
    connect,
    subscribe,
    unsubscribe: async () => {},
    ping: async () => {},
    disconnect() {},
    on(event: string, listener: (channel: Buffer, frame: Buffer) => void) {
      if (event === 'messageBuffer') publish = (channel, frame) => listener(Buffer.from(channel), frame)
      if (event === 'close') close = listener as unknown as () => void
      return socket
    },
  }
  const connection = {
    openSubscriberSocket: async () => socket,
    client: { options: { host: '127.0.0.1', port: 6390 } },
  } as unknown as RedisConnection
  const subscriptions = new RedisSubscriptions({ connection })
  const delivered: string[] = []
  const listen = (
    channel: string,
    readStarts: ReadStarts = async () => none(),
    frames = { fanIn: false, placed: true, text: true, lanesEndWithMembers: false },
    lossKey = channel,
  ): DriverAttempt =>
    subscriptions.subscribe({
      channel,
      invalidationChannel: null,
      frames,
      laneChannel: frames.fanIn ? (laneKey) => `${channel}:${laneKey}` : null,
      readStarts,
      receiver: {
        deliver(payload) {
          delivered.push(String(payload))
        },
      },
      reportMissed(count) {
        missed.push(count)
      },
      lossKey,
    })
  const missed: Array<number | null> = []
  return {
    subscriptions,
    listen,
    delivered,
    missed,
    close: () => close(),
    publish: (channel: string, frame: Buffer) => publish(channel, frame),
    sends: new NewSubscriptionWait({ subscriptions }),
  }
}

/** A frame as commands.ts's scripts write it: the lane key on a fan-in channel, the place where the source's kinds
 *  share one seq (`placed`), then `[seq_hi][seq_lo][ts_hi][ts_lo]`, u32 big-endian, the commit's id and its
 *  predecessor's, and the payload. Its id is its place unless given, and so is the one before it, so a lane's frames
 *  name the one before them. */
function frameOf(
  place: number,
  text: string,
  {
    lane,
    placed = true,
    seq = place,
    id = BigInt(place),
    prev = BigInt(Math.max(place - 1, 0)),
  }: { lane?: string; placed?: boolean; seq?: number; id?: bigint; prev?: bigint } = {},
): Buffer {
  const header = Buffer.alloc(40)
  header.writeBigUInt64BE(BigInt(place), 0)
  header.writeUInt32BE(Math.floor(seq / 0x1_0000_0000), 8)
  header.writeUInt32BE(seq % 0x1_0000_0000, 12)
  header.writeUInt32BE(0, 16)
  header.writeUInt32BE(1_700_000_000, 20)
  header.writeBigUInt64BE(id, 24)
  header.writeBigUInt64BE(prev, 32)
  const laneKey = lane === undefined ? Buffer.alloc(0) : Buffer.concat([Buffer.alloc(2), Buffer.from(lane, 'latin1')])
  if (lane !== undefined) laneKey.writeUInt16BE(lane.length, 0)
  return Buffer.concat([laneKey, placed ? header : header.subarray(8), Buffer.from(text)])
}

describe('a start read Redis answers with an error', () => {
  for (const message of REFUSALS) {
    it(`closes the subscription with Telefunc's refusal, where it reads ${message.slice(0, 60)}`, async () => {
      let reads = 0
      const { subscriptions, listen } = setup()
      const attempt = listen(TEXT, async () => {
        reads++
        throw replyError(message)
      })
      const closed = new Promise((resolve) => attempt.onStateChange((state, reason) => resolve([state, reason])))

      const refusal = await Promise.all(subscriptions.establishingOn([TEXT])).catch((error: unknown) => error)
      expect(refusal).toMatchObject({
        refused: true,
        code: message.includes('READONLY') ? 'READONLY' : expect.any(String),
      })
      await expect(closed).resolves.toEqual(['closed', refusal])
      expect(reads).toBe(1)
    })
  }

  for (const error of PASSING) {
    it(`reads again, where it is ${error.message.slice(0, 40)}`, async () => {
      let reads = 0
      const { listen } = setup()
      const attempt = listen(TEXT, async () => {
        if (++reads === 1) throw error
        return none()
      })

      await expect.poll(() => attempt.state(), { timeout: 3_000 }).toBe('ready')
      expect(reads).toBe(2)
    })
  }
})

describe('a resume', () => {
  const lane = (place: number) => ({ authority: 'a', starts: new Map([['', { place, id: BigInt(place) }]]) })

  it('counts what it missed on the history it last read, and reports nothing where it missed nothing', async () => {
    const { listen, publish, close, missed } = setup()
    let start = lane(1)
    const attempt = listen(TEXT, async () => start)
    await expect.poll(() => attempt.state()).toBe('ready')
    publish(TEXT, frameOf(2, 'm2'))
    close()
    await expect.poll(() => attempt.state()).toBe('ready')
    close()
    start = lane(4)
    await expect.poll(() => missed.length).toBe(1)
    expect(missed).toEqual([2])
  })

  it('tells once where its read, ahead on the same history, finds the frames that reached it forking below the lane', async () => {
    const { listen, publish, close, delivered, missed } = setup()
    let start = lane(4)
    let reading = createDeferred()
    reading.resolve()
    let reads = 0
    const attempt = listen(TEXT, async () => {
      reads++
      await reading.promise
      return start
    })
    await expect.poll(() => attempt.state()).toBe('ready')
    publish(TEXT, frameOf(5, 'm5'))
    // The record was replaced from place 4 on and stands at y7; y6 and y7 reach the new connection during its read.
    reading = createDeferred()
    close()
    await expect.poll(() => reads).toBe(2)
    publish(TEXT, frameOf(6, 'y6', { id: BigInt(106), prev: BigInt(105) }))
    publish(TEXT, frameOf(7, 'y7', { id: BigInt(107), prev: BigInt(106) }))
    start = { authority: 'a', starts: new Map([['', { place: 7, id: BigInt(107) }]]) }
    reading.resolve()
    await expect.poll(() => attempt.state()).toBe('ready')
    expect(delivered).toEqual(['m5', 'y6', 'y7'])
    expect(missed).toEqual([null])
  })

  it('reports once, with no count, that it may have missed some, from another history, with lanes or none', async () => {
    for (const form of ['same place', 'ahead', 'none'] as const) {
      const { listen, close, missed } = setup()
      let start = form === 'none' ? none('a') : lane(3)
      const attempt = listen(TEXT, async () => start)
      await expect.poll(() => attempt.state()).toBe('ready')
      close()
      start = { ...(form === 'ahead' ? lane(5) : form === 'none' ? none() : lane(3)), authority: 'b' }
      await expect.poll(() => missed.length).toBe(1)
      await new Promise((resolve) => setImmediate(resolve))
      expect(missed).toEqual([null])
    }
  })
})

describe('a Cluster slot whose master changed', () => {
  const lane = (place: number, authority = 'a') => ({
    authority,
    starts: new Map([['', { place, id: BigInt(place) }]]),
  })
  const moved = new Map([[slotOf(TEXT), '127.0.0.1:7001']])
  /** The connection to the old master, which a start read is sent on to read from its own data. */
  const OLD = {} as RedisClient
  const node = (view: Pick<FormerMaster, 'master' | 'history'> & Partial<FormerMaster>): FormerMaster => ({
    client: OLD as FormerMaster['client'],
    previous: null,
    serves: async () => false,
    close() {},
    ...view,
  })
  /** The old master of history 'a', which gave the slot away. */
  const resharded = node({ master: true, history: 'a' })

  it('resumes the attempt, told of a loss, where the old master is gone, serves the slot still, is of another history, or resynced in full', async () => {
    const views: Array<FormerMaster | null> = [
      null,
      node({ master: true, history: 'a', serves: async () => true }),
      node({ master: true, history: 'x' }),
      // A replica now that took the new master's history whole: what it had past the new master's copy is gone.
      node({ master: false, history: 'b' }),
    ]
    for (const view of views) {
      const { subscriptions, listen, missed } = setup()
      let start = lane(3)
      const attempt = listen(TEXT, async () => start)
      await expect.poll(() => attempt.state()).toBe('ready')
      // A failover's new master: another history, which lacks what the old one accepted and never replicated.
      start = lane(3, 'b')
      await subscriptions.followMoves(moved, async () => view)
      await expect.poll(() => missed).toEqual([null])
      expect(attempt.state()).toBe('ready')
    }
  })

  it('tells a failover once where its frames reach the lane before the check: the read they make tells it, and it follows the new history', async () => {
    const { subscriptions, listen, publish, delivered, missed } = setup()
    let start = lane(3)
    const attempt = listen(TEXT, async () => start)
    await expect.poll(() => attempt.state()).toBe('ready')
    const states: string[] = []
    attempt.onStateChange((state) => states.push(state))
    publish(TEXT, frameOf(4, 'm4'))
    // The new master's first frame takes m4's place: the attempt reads where its key stands, as after a cut, and finds
    // its master of another history now.
    start = { authority: 'b', starts: new Map([['', { place: 4, id: BigInt(104) }]]) }
    publish(TEXT, frameOf(4, 'y4', { id: BigInt(104) }))
    await expect.poll(() => missed).toEqual([null])
    await subscriptions.followMoves(moved, async () => null)
    expect(states).toEqual(['lost', 'ready'])
    expect(missed).toEqual([null])
    // y5 goes on from y4, and another failover's master stands at y5: nothing in the frames told the lane of it.
    publish(TEXT, frameOf(5, 'y5', { id: BigInt(105), prev: BigInt(104) }))
    start = { authority: 'c', starts: new Map([['', { place: 5, id: BigInt(105) }]]) }
    await subscriptions.followMoves(moved, async () => null)
    await expect.poll(() => states).toEqual(['lost', 'ready', 'lost', 'ready'])
    expect(missed).toEqual([null, null])
    expect(delivered).toEqual(['m4', 'y4', 'y5'])
  })

  it('delivers both sides of a fork once each, under one report, in either order they arrive', async () => {
    // The old master's x11 and the new master's y11 both follow x10.
    const x11 = frameOf(11, 'x11')
    const y11 = frameOf(11, 'y11', { id: BigInt(111) })
    for (const newFirst of [false, true]) {
      const { listen, publish, delivered, missed } = setup()
      let start = lane(10)
      const attempt = listen(TEXT, async () => start)
      await expect.poll(() => attempt.state()).toBe('ready')
      start = { authority: 'b', starts: new Map([['', { place: 11, id: BigInt(111) }]]) }
      const order = newFirst ? ['y11', 'x11'] : ['x11', 'y11']
      for (const text of order) publish(TEXT, text === 'x11' ? x11 : y11)
      await expect.poll(() => delivered).toEqual(order)
      publish(TEXT, frameOf(12, 'y12', { id: BigInt(112), prev: BigInt(111) }))
      expect(delivered).toEqual([...order, 'y12'])
      expect(missed).toEqual([null])
    }
  })

  it('tells a failover once where the old master’s late frame follows the new history’s first', async () => {
    const { listen, publish, delivered, missed } = setup()
    let start = lane(3)
    const attempt = listen(TEXT, async () => start)
    await expect.poll(() => attempt.state()).toBe('ready')
    publish(TEXT, frameOf(4, 'm4'))
    start = { authority: 'b', starts: new Map([['', { place: 4, id: BigInt(104) }]]) }
    publish(TEXT, frameOf(4, 'y4', { id: BigInt(104) }))
    await expect.poll(() => delivered).toEqual(['m4', 'y4'])
    // Two writers for a moment: the old master's m5 reaches the lane after the new master's y4, then y5.
    publish(TEXT, frameOf(5, 'm5'))
    publish(TEXT, frameOf(5, 'y5', { id: BigInt(105), prev: BigInt(104) }))
    expect(attempt.state()).toBe('ready')
    expect(delivered).toEqual(['m4', 'y4', 'm5', 'y5'])
    expect(missed).toEqual([null])
  })

  it('tells a failover once where the check comes before its frames: the frames before the new start are of the change told', async () => {
    const { subscriptions, listen, publish, delivered, missed } = setup()
    let start = lane(3)
    const attempt = listen(TEXT, async () => start)
    await expect.poll(() => attempt.state()).toBe('ready')
    publish(TEXT, frameOf(4, 'm4'))
    // The new master forked at m3 and has y4, y5; neither has reached the lane when the check reads its start.
    start = { authority: 'b', starts: new Map([['', { place: 5, id: BigInt(105) }]]) }
    await subscriptions.followMoves(moved, async () => null)
    await expect.poll(() => missed).toEqual([null])
    publish(TEXT, frameOf(4, 'y4', { id: BigInt(104) }))
    publish(TEXT, frameOf(5, 'y5', { id: BigInt(105), prev: BigInt(104) }))
    expect(missed).toEqual([null])
    expect(delivered).toEqual(['m4', 'y4', 'y5'])
  })

  it('tells a failover once to a loss key whose other attempt met it in its frames first', async () => {
    const { subscriptions, listen, publish, missed } = setup()
    let start = lane(3)
    const attempts = [TEXT, BINARY].map((channel) => listen(channel, async () => start, undefined, 'room'))
    await expect.poll(() => attempts.map((attempt) => attempt.state())).toEqual(['ready', 'ready'])
    publish(TEXT, frameOf(4, 'm4'))
    start = { authority: 'b', starts: new Map([['', { place: 4, id: BigInt(104) }]]) }
    publish(TEXT, frameOf(4, 'y4', { id: BigInt(104) }))
    await expect.poll(() => missed).toEqual([null])
    // BINARY carried nothing: only the old master, gone, could show it lost nothing.
    await subscriptions.followMoves(new Map([...moved, [slotOf(BINARY), '127.0.0.1:7001']]), async () => null)
    await expect.poll(() => attempts.map((attempt) => attempt.state())).toEqual(['ready', 'ready'])
    expect(missed).toEqual([null])
  })

  it('tells a failover once to a loss key whose two attempts each meet it in their frames', async () => {
    const { subscriptions, listen, publish, missed } = setup()
    let start = lane(3)
    const attempts = [TEXT, BINARY].map((channel) => listen(channel, async () => start, undefined, 'room'))
    await expect.poll(() => attempts.map((attempt) => attempt.state())).toEqual(['ready', 'ready'])
    for (const channel of [TEXT, BINARY]) publish(channel, frameOf(4, 'm4'))
    start = { authority: 'b', starts: new Map([['', { place: 4, id: BigInt(104) }]]) }
    for (const channel of [TEXT, BINARY]) publish(channel, frameOf(4, 'y4', { id: BigInt(104) }))
    await subscriptions.followMoves(new Map([...moved, [slotOf(BINARY), '127.0.0.1:7001']]), async () => null)
    await expect.poll(() => attempts.map((attempt) => attempt.state())).toEqual(['ready', 'ready'])
    expect(missed).toEqual([null])
  })

  it('tells a failover once to a fan-in attempt whose two lanes each meet it in their frames', async () => {
    const { subscriptions, listen, publish, delivered, missed } = setup()
    const startsAt = (authority: string, id: bigint) => ({
      authority,
      starts: new Map(['x', 'z'].map((laneKey) => [laneKey, { place: Number(id % BigInt(100)), id }])),
    })
    let start = startsAt('a', BigInt(3))
    const fanIn = { fanIn: true, placed: false, text: true, lanesEndWithMembers: false }
    const attempt = listen(TEXT, async () => start, fanIn, 'room')
    await expect.poll(() => attempt.state()).toBe('ready')
    for (const laneKey of ['x', 'z']) publish(TEXT, frameOf(4, `m4${laneKey}`, { lane: laneKey, placed: false }))
    start = startsAt('b', BigInt(104))
    for (const laneKey of ['x', 'z']) {
      publish(TEXT, frameOf(4, `y4${laneKey}`, { lane: laneKey, placed: false, id: BigInt(104) }))
    }
    await subscriptions.followMoves(moved, async () => null)
    await expect.poll(() => attempt.state()).toBe('ready')
    expect(missed).toEqual([null])
    for (const laneKey of ['x', 'z']) {
      expect(delivered.filter((text) => text.endsWith(laneKey))).toEqual([`m4${laneKey}`, `y4${laneKey}`])
    }
  })

  it('tells a failover once to a loss key whose attempt met it in its frames, where a cut then resumes both', async () => {
    const { listen, publish, close, missed } = setup()
    let start = lane(3)
    const attempts = [TEXT, BINARY].map((channel) => listen(channel, async () => start, undefined, 'room'))
    await expect.poll(() => attempts.map((attempt) => attempt.state())).toEqual(['ready', 'ready'])
    publish(TEXT, frameOf(4, 'm4'))
    start = { authority: 'b', starts: new Map([['', { place: 4, id: BigInt(104) }]]) }
    publish(TEXT, frameOf(4, 'y4', { id: BigInt(104) }))
    await expect.poll(() => attempts.map((attempt) => attempt.state())).toEqual(['ready', 'ready'])
    const resumed = attempts.map((attempt) => {
      const states: string[] = []
      attempt.onStateChange((state) => states.push(state))
      return states
    })
    close()
    await expect.poll(() => resumed).toEqual(attempts.map(() => ['lost', 'ready']))
    expect(missed).toEqual([null])
  })

  it('tells a failover once where the read that places the fork its frames brought fails first', async () => {
    const { subscriptions, listen, publish, delivered, missed } = setup()
    let start = lane(3)
    let failures = 0
    const attempt = listen(TEXT, async () => {
      if (failures === 0) return start
      failures--
      throw new Error('Connection is closed.')
    })
    await expect.poll(() => attempt.state()).toBe('ready')
    publish(TEXT, frameOf(4, 'm4'))
    start = { authority: 'b', starts: new Map([['', { place: 4, id: BigInt(104) }]]) }
    failures = 1
    publish(TEXT, frameOf(4, 'y4', { id: BigInt(104) }))
    await subscriptions.followMoves(moved, async () => null)
    await expect.poll(() => delivered, { timeout: 3_000 }).toEqual(['m4', 'y4'])
    await subscriptions.followMoves(moved, async () => null)
    expect(attempt.state()).toBe('ready')
    expect(missed).toEqual([null])
  })

  it('reports where the lane went on, as next, from the start of the last switch or resharding into a failover', async () => {
    for (const first of ['switch', 'resharding'] as const) {
      const { subscriptions, listen, publish, missed } = setup()
      let start = lane(3)
      const attempt = listen(TEXT, async () => start)
      await expect.poll(() => attempt.state()).toBe('ready')
      // Where the other history stands, a commit with id 100 + its place.
      let place = 3
      if (first === 'switch') {
        // A reported failover to a master at another commit in place 3.
        start = { authority: 'b', starts: new Map([['', { place, id: BigInt(103) }]]) }
        await subscriptions.followMoves(moved, async () => null)
        await expect.poll(() => missed).toEqual([null])
      } else {
        // A reported fork, y4 replacing m4 within the history, then a resharding.
        publish(TEXT, frameOf(4, 'm4'))
        start = { authority: 'a', starts: new Map([['', { place: 4, id: BigInt(104) }]]) }
        publish(TEXT, frameOf(++place, 'y4', { id: BigInt(104) }))
        await expect.poll(() => missed).toEqual([null])
        start = { authority: 'b', starts: new Map([['', { place, id: BigInt(104) }]]) }
        await subscriptions.followMoves(moved, async () => resharded)
      }
      publish(TEXT, frameOf(++place, 'z', { id: BigInt(100 + place), prev: BigInt(99 + place) }))
      start = { authority: 'c', starts: new Map([['', { place, id: BigInt(100 + place) }]]) }
      await subscriptions.followMoves(moved, async () => null)
      await expect.poll(() => missed).toEqual([null, null])
    }
  })

  it('goes on where the old master rejoined by a partial resync, or holds no commit its lanes lack', async () => {
    const cases: Array<[string, FormerMaster, Starts]> = [
      [
        'a partial resync, which Redis grants only to a node with nothing the new master lacks',
        node({ master: false, history: 'b', previous: 'a' }),
        lane(3),
      ],
      [
        'a replica not yet resynced, whose record is at the commit the lane started at',
        node({ master: false, history: 'a' }),
        lane(3),
      ],
      [
        'a replica not yet resynced, whose last commit the new master holds too',
        node({ master: false, history: 'a' }),
        lane(4),
      ],
    ]
    for (const [, old, there] of cases) {
      const { subscriptions, listen, close, missed } = setup()
      let start = lane(3)
      const attempt = listen(TEXT, async (on) => (on === OLD ? there : start))
      await expect.poll(() => attempt.state()).toBe('ready')
      const states: string[] = []
      attempt.onStateChange((state) => states.push(state))
      start = { authority: 'b', starts: new Map([['', { place: 4, id: BigInt(4) }]]) }
      await subscriptions.followMoves(moved, async () => old)
      expect(states).toEqual([])
      // It follows the new master's history: a cut after resumes from it with no unknown loss.
      close()
      await expect.poll(() => states).toEqual(['lost', 'ready'])
      expect(missed).toEqual([1])
    }
  })

  it('resumes, told of a loss, where the old master, not yet resynced, holds a commit its lanes never got', async () => {
    const { subscriptions, listen, missed } = setup()
    let start = lane(3)
    const there = { authority: 'a', starts: new Map([['', { place: 4, id: BigInt(4) }]]) }
    const attempt = listen(TEXT, async (on) => (on === OLD ? there : start))
    await expect.poll(() => attempt.state()).toBe('ready')
    // The new master has its own commit at place 4.
    start = { authority: 'b', starts: new Map([['', { place: 4, id: BigInt(104) }]]) }
    await subscriptions.followMoves(moved, async () => node({ master: false, history: 'a' }))
    await expect.poll(() => missed).toEqual([null])
  })

  it('tells one unknown loss of one change to the attempts that share a loss key, and the next change again', async () => {
    const { subscriptions, listen, missed } = setup()
    let start = lane(3)
    // A Room's channels in one slot: one room, one loss key.
    const attempts = [TEXT, BINARY].map((channel) => listen(channel, async () => start, undefined, 'room'))
    await expect.poll(() => attempts.map((attempt) => attempt.state())).toEqual(['ready', 'ready'])
    const both = new Map([...moved, [slotOf(BINARY), '127.0.0.1:7001']])
    start = lane(3, 'b')
    await subscriptions.followMoves(both, async () => null)
    await expect.poll(() => attempts.map((attempt) => attempt.state())).toEqual(['ready', 'ready'])
    expect(missed).toEqual([null])
    start = lane(3, 'c')
    await subscriptions.followMoves(both, async () => null)
    await expect.poll(() => missed).toEqual([null, null])
  })

  it('goes on where the old master, of the history it started from, sends its key elsewhere: a resharding', async () => {
    const { subscriptions, listen, close, missed } = setup()
    let start = lane(3)
    const attempt = listen(TEXT, async () => start)
    await expect.poll(() => attempt.state()).toBe('ready')
    const states: string[] = []
    attempt.onStateChange((state) => states.push(state))
    // The new master, which the key moved to with its record.
    start = lane(3, 'b')
    await subscriptions.followMoves(moved, async () => resharded)
    expect(states).toEqual([])
    // A cut after it resumes from the new master's history, which it follows now: nothing was missed.
    close()
    await expect.poll(() => states).toEqual(['lost', 'ready'])
    expect(missed).toEqual([])
  })
})

describe('a publish waiting on its key’s new subscriptions', () => {
  it('goes out once the other kind’s start read, failed and read again, is ready: a failed read is no end', async () => {
    const { subscriptions, listen, sends } = setup()
    listen(TEXT)
    await Promise.all(subscriptions.establishingOn([TEXT]))
    const reads = [createDeferred<Starts>(), createDeferred<Starts>()]
    let read = 0
    const binary = listen(BINARY, () => reads[read++]!.promise)

    const waiting = sends.waitTurn(RULE, 'hello')!
    reads[0]!.reject(new Error('the command connection dropped'))
    await expect.poll(() => read, { timeout: 3_000 }).toBe(2)
    expect(binary.state()).toBe('establishing')
    reads[1]!.resolve(none())

    await expect(waiting.turn).resolves.toBe('hello')
    expect(binary.state()).toBe('ready')
  })

  it('fails with the refusal when Redis refuses the subscription of its own kind it waits for', async () => {
    const { listen, sends } = setup(async (...channels) => {
      if (channels.includes(TEXT)) throw refusal()
    })
    listen(TEXT)
    listen(BINARY)

    await expect(sends.waitTurn(RULE, 'hello')!.turn).rejects.toMatchObject({ refused: true, code: 'NOPERM' })
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
    ['an id of 0', frameOf(1, 'x', { id: BigInt(0) })],
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
    listen(TEXT, async () => none(), { fanIn: true, placed: true, text: true, lanesEndWithMembers: false })
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
    const starts = createDeferred<Starts>()
    const { listen, publish, delivered } = setup()
    const attempt = listen(TEXT, () => starts.promise)
    await new Promise((resolve) => setImmediate(resolve))

    publish(TEXT, Buffer.from([1, 2, 3]))
    publish(TEXT, frameOf(1, 'early'))
    starts.resolve(none())

    await expect.poll(() => attempt.state()).toBe('ready')
    expect(delivered).toEqual(['early'])
    expect(reports).toHaveLength(1)
    expect(String(reports[0])).not.toMatch(/bug/i)
  })
})

describe('a subscriber connection Redis refuses at its login', () => {
  afterEach(() => vi.restoreAllMocks())

  it("is reported once as Telefunc's error, with Redis's code, and not the password", async () => {
    const reports: string[] = []
    vi.spyOn(console, 'error').mockImplementation((...args) => void reports.push(inspect(args, { depth: 8 })))
    const refused = Object.assign(replyError('WRONGPASS invalid username-password pair or user is disabled.'), {
      command: { name: 'hello', args: ['3', 'AUTH', 'tf', 'pw-SECRET'] },
    })
    const { listen } = setup(undefined, async () => {
      throw refused
    })
    listen(TEXT)

    await expect.poll(() => reports.length).toBe(1)
    expect(reports[0]).toContain(
      "Redis at 127.0.0.1:6390 refused the login of Telefunc's Redis backend (WRONGPASS): check the username and password of the client given to installRedis()",
    )
    expect(reports[0]).not.toContain('pw-SECRET')
    expect(reports[0]).not.toContain('args')
  })
})
