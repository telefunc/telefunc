import { EventEmitter } from 'node:events'
import type { SubscriberSocket } from './ioredis.js'
import { expect, onTestFinished, test, vi } from 'vitest'
import {
  encodeLaneKey,
  type BackendPayload,
  type BackendReceiver,
  type SubscriptionAttempt,
  type SubscriptionState,
} from 'telefunc/__internal'
import { RedisSubscriptionDriver } from './subscriber.js'
import { channelKey, commitChannels } from './keys.js'
import { REDIS_DELIVERY_FENCE_BYTE } from './commands.js'

/** Resolves once the attempt is ready; rejects if it ends first. */
function untilReady(attempt: SubscriptionAttempt): Promise<void> {
  return new Promise((resolve, reject) => {
    const settle = (state: SubscriptionState, reason?: Error): boolean => {
      if (state === 'ready') resolve()
      else if (state === 'closed') reject(reason ?? new Error(`attempt ${state}`))
      else return false
      return true
    }
    if (settle(attempt.state())) return
    const stop = attempt.onStateChange((state, reason) => {
      if (settle(state, reason)) stop()
    })
  })
}

function fakeSubscriber() {
  const subscribed: string[][] = []
  const socket = Object.assign(new EventEmitter(), {
    connect: async () => {},
    subscribe: async (...channels: string[]) => void subscribed.push(channels),
    unsubscribe: async () => {},
    disconnect() {},
  })
  return {
    socket,
    subscribed,
    deliver: (channel: string, frame: Uint8Array) =>
      socket.emit('messageBuffer', Buffer.from(channel), Buffer.from(frame)),
  }
}

function orderingFrame(seq: number, payload: string, timestamp = 1): Uint8Array {
  const bytes = new TextEncoder().encode(payload)
  const frame = new Uint8Array(16 + bytes.byteLength)
  const view = new DataView(frame.buffer)
  view.setUint32(4, seq)
  view.setUint32(12, timestamp)
  frame.set(bytes, 16)
  return frame
}

/** A frame on an every-track channel: its lane key, two bytes of length first, then the lane's ordering frame. */
function everyTrackFrame(laneKey: string, seq: number, payload: string): Uint8Array {
  const key = new TextEncoder().encode(laneKey)
  const ordered = orderingFrame(seq, payload)
  const frame = new Uint8Array(2 + key.byteLength + ordered.byteLength)
  new DataView(frame.buffer).setUint16(0, key.byteLength)
  frame.set(key, 2)
  frame.set(ordered, 2 + key.byteLength)
  return frame
}

function driverWith(
  sockets: ReturnType<typeof fakeSubscriber>[],
  validateGeneration: () => Promise<boolean> = async () => true,
) {
  const createSubscriber = vi.fn(async () => {
    const next = fakeSubscriber()
    sockets.push(next)
    return next.socket as unknown as SubscriberSocket
  })
  return {
    driver: new RedisSubscriptionDriver({ prefix: 'tf:', createSubscriber, validateGeneration }),
    createSubscriber,
  }
}

function openAttempt(
  driver: RedisSubscriptionDriver,
  source: Parameters<RedisSubscriptionDriver['bind']>[0],
  receiver: BackendReceiver<BackendPayload> = () => {},
) {
  return driver.bind(source).open(receiver, () => 1)
}

const route = { key: 'chat', kind: 'text' } as const

test('shares one subscriber connection across lanes', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver, createSubscriber } = driverWith(sockets)
  const first = openAttempt(driver, route)
  const second = openAttempt(driver, { key: 'other', kind: 'binary' })
  await Promise.all([untilReady(first), untilReady(second)])
  expect(createSubscriber).toHaveBeenCalledOnce()
  expect(sockets[0]!.subscribed.flat()).toHaveLength(2)
})

test('re-subscribes on a fresh connection and resumes delivery after a drop', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const received: BackendPayload[] = []
  const states: SubscriptionState[] = []
  const attempt = openAttempt(driver, route, (payload) => void received.push(payload))
  await untilReady(attempt)
  attempt.onStateChange((state) => states.push(state))
  const channel = sockets[0]!.subscribed[0]![0]!
  sockets[0]!.deliver(channel, orderingFrame(5, 'one'))
  sockets[0]!.socket.emit('close')
  await vi.waitFor(() => expect(states).toEqual(['lost', 'ready']))
  expect(sockets[1]!.subscribed).toEqual([[channel]])
  sockets[1]!.deliver(channel, orderingFrame(1, 'two'))
  expect(received).toEqual(['one', 'two'])
})

test("reports each outage with its own connection's error, not an earlier connection's", async () => {
  const report = vi.spyOn(console, 'error').mockImplementation(() => {})
  onTestFinished(() => report.mockRestore())
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const attempt = openAttempt(driver, route)
  await untilReady(attempt)
  sockets[0]!.socket.emit('error', new Error('connect ECONNREFUSED'))
  sockets[0]!.socket.emit('close')
  await vi.waitFor(() => expect(sockets[1]?.subscribed).toHaveLength(1))
  await vi.waitFor(() => expect(attempt.state()).toBe('ready'))
  // The server closes the connection cleanly: no 'error' event.
  sockets[1]!.socket.emit('close')
  await vi.waitFor(() => expect(report).toHaveBeenCalledTimes(2))
  expect(report.mock.calls.map(([error]) => (error as Error).message)).toEqual([
    'connect ECONNREFUSED',
    'Redis subscriber connection closed',
  ])
  await attempt.unsubscribe()
})

test('reports the outage of a connection opened after an earlier outage released the last one', async () => {
  const report = vi.spyOn(console, 'error').mockImplementation(() => {})
  onTestFinished(() => report.mockRestore())
  const createSubscriber = vi.fn(async (): Promise<SubscriberSocket> => {
    throw new Error('connect ECONNREFUSED')
  })
  const driver = new RedisSubscriptionDriver({ prefix: 'tf:', createSubscriber, validateGeneration: async () => true })
  const first = openAttempt(driver, route)
  await vi.waitFor(() => expect(report).toHaveBeenCalledOnce())
  // Its last subscription leaves mid-outage, which releases the connection; a later one starts afresh.
  await first.unsubscribe()
  const second = openAttempt(driver, route)
  await vi.waitFor(() => expect(report).toHaveBeenCalledTimes(2))
  await second.unsubscribe()
})

test('an every-track lane drops a frame only when it is late on its own lane, as a Cluster resharding can deliver it', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const received: string[] = []
  const attempt = openAttempt(
    driver,
    { roomId: 'room', inc: 'inc', lane: { kind: 'binary-all', member: 'm' } },
    (payload, info) => void received.push(`${new TextDecoder().decode(payload as Uint8Array)}@${info.seq}`),
  )
  await untilReady(attempt)
  const channel = channelKey('tf:', 'room', 'inc', 'binary-all:m')
  for (const [lane, seq] of [
    ['camera', 5],
    ['mic', 1],
    ['camera', 4],
    ['mic', 2],
    ['mic', 2],
    ['camera', 6],
  ] as const)
    sockets[0]!.deliver(channel, everyTrackFrame(`binary:m:${lane}`, seq, lane))
  expect(received).toEqual(['camera@5', 'mic@1', 'mic@2', 'camera@6'])
})

test('an every-member lane drops a frame only when it is late on its own lane, as a Cluster resharding can deliver it', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const received: string[] = []
  const attempt = openAttempt(
    driver,
    { roomId: 'room', inc: 'inc', lane: { kind: 'binary-track', track: 'mic' } },
    (payload, info) => void received.push(`${new TextDecoder().decode(payload as Uint8Array)}@${info.seq}`),
  )
  await untilReady(attempt)
  const channel = channelKey('tf:', 'room', 'inc', 'binary-track:mic')
  for (const [member, seq] of [
    ['a', 5],
    ['b', 1],
    ['a', 4],
    ['b', 2],
    ['b', 2],
    ['a', 6],
  ] as const)
    sockets[0]!.deliver(channel, everyTrackFrame(`binary:${member}:mic`, seq, member))
  expect(received).toEqual(['a@5', 'b@1', 'b@2', 'a@6'])
})

test('a frame that arrives past a missing one waits for it, and both go out in order on their own lane', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const received: string[] = []
  const attempt = openAttempt(
    driver,
    { roomId: 'room', inc: 'inc', lane: { kind: 'binary-track', track: 'mic' } },
    (payload, info) => void received.push(`${new TextDecoder().decode(payload as Uint8Array)}@${info.seq}`),
  )
  await untilReady(attempt)
  const channel = channelKey('tf:', 'room', 'inc', 'binary-track:mic')
  for (const [member, seq] of [
    ['a', 1],
    ['a', 3],
    ['b', 7],
    ['a', 4],
    ['b', 8],
    ['a', 2],
  ] as const)
    sockets[0]!.deliver(channel, everyTrackFrame(`binary:${member}:mic`, seq, member))
  expect(received).toEqual(['a@1', 'b@7', 'b@8', 'a@2', 'a@3', 'a@4'])
})

test('a frame whose predecessor never arrives goes out after the wait, and the predecessor arriving later is dropped', async () => {
  vi.useFakeTimers()
  onTestFinished(() => void vi.useRealTimers())
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const received: string[] = []
  const source = { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } } as const
  const attempt = openAttempt(
    driver,
    source,
    (payload) => void received.push(new TextDecoder().decode(payload as Uint8Array)),
  )
  await vi.advanceTimersByTimeAsync(0)
  await untilReady(attempt)
  const channel = sockets[0]!.subscribed[0]![0]!
  sockets[0]!.deliver(channel, orderingFrame(1, 'one'))
  sockets[0]!.deliver(channel, orderingFrame(3, 'three'))
  await vi.advanceTimersByTimeAsync(999)
  expect(received).toEqual(['one'])
  await vi.advanceTimersByTimeAsync(1)
  expect(received).toEqual(['one', 'three'])
  const fence = driver.prepareFence(
    commitChannels('tf:', source.roomId, source.inc, source.lane, encodeLaneKey(source.lane)),
  )
  const token = fence.tokens.lane
  const late = orderingFrame(2, 'two')
  const fenced = new Uint8Array(1 + token.length + late.byteLength)
  fenced[0] = REDIS_DELIVERY_FENCE_BYTE
  fenced.set(new TextEncoder().encode(token), 1)
  fenced.set(late, 1 + token.length)
  sockets[0]!.deliver(channel, fenced)
  await expect(fence.delivery).resolves.toBeUndefined()
  expect(received).toEqual(['one', 'three'])
  await attempt.unsubscribe()
})

test('a lane whose seq a failover rewound goes on from the rewound frame, and a frame older than the last is still dropped', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const received: string[] = []
  const attempt = openAttempt(driver, route, (payload, info) => void received.push(`${payload}@${info.seq}`))
  await untilReady(attempt)
  const channel = sockets[0]!.subscribed[0]![0]!
  for (const [seq, timestamp, payload] of [
    [5, 10, 'a'],
    [6, 11, 'b'],
    [5, 20, 'c'],
    [6, 21, 'd'],
    [4, 9, 'late'],
    [7, 22, 'e'],
  ] as const)
    sockets[0]!.deliver(channel, orderingFrame(seq, payload, timestamp))
  expect(received).toEqual(['a@5', 'b@6', 'c@5', 'd@6', 'e@7'])
})

test("a fence resolves when its subscription's owner releases it: no receiver is left to hand off to", async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const source = { roomId: 'room', inc: 'inc', lane: { kind: 'control' } } as const
  const attempt = openAttempt(driver, source)
  await untilReady(attempt)
  const fence = driver.prepareFence(
    commitChannels('tf:', source.roomId, source.inc, source.lane, encodeLaneKey(source.lane)),
  )
  await attempt.unsubscribe()
  await expect(fence.delivery).resolves.toBeUndefined()
})

test('a SUBSCRIBE that keeps failing backs off and is reported once', async () => {
  vi.useFakeTimers()
  onTestFinished(() => void vi.useRealTimers())
  const report = vi.spyOn(console, 'error').mockImplementation(() => {})
  onTestFinished(() => report.mockRestore())
  const refused = new Error('NOPERM this user has no permissions to access one of the channels')
  const createSubscriber = vi.fn(async () => {
    const { socket } = fakeSubscriber()
    socket.subscribe = async () => {
      throw refused
    }
    return socket as unknown as SubscriberSocket
  })
  const driver = new RedisSubscriptionDriver({ prefix: 'tf:', createSubscriber, validateGeneration: async () => true })
  const attempt = openAttempt(driver, route)
  await vi.advanceTimersByTimeAsync(10_000)
  // The delay doubles from 50 ms to 2 s: ten connections in ten seconds, not one every 50 ms.
  expect(createSubscriber).toHaveBeenCalledTimes(10)
  expect(report).toHaveBeenCalledOnce()
  expect(report).toHaveBeenCalledWith(refused)
  await attempt.unsubscribe()
})

test('a subscriber dropping before the commit returns rejects its delivery without an unhandled rejection', async () => {
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  onTestFinished(() => void process.off('unhandledRejection', onUnhandled))
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const source = { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } } as const
  const attempt = openAttempt(driver, source)
  await untilReady(attempt)
  const fence = driver.prepareFence(
    commitChannels('tf:', source.roomId, source.inc, source.lane, encodeLaneKey(source.lane)),
  )
  sockets[0]!.socket.emit('close')
  await new Promise((resolve) => setTimeout(resolve, 10))
  expect(unhandled).toEqual([])
  await expect(fence.delivery).rejects.toThrow()
})

test('a failed generation check ends that Room lane only, not the shared connection', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const failure = new Error('command connection lost')
  const { driver } = driverWith(sockets, async () => {
    throw failure
  })
  const broadcast = openAttempt(driver, route)
  await untilReady(broadcast)
  const room = openAttempt(driver, { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } })
  await expect(untilReady(room)).rejects.toBe(failure)
  expect(broadcast.state()).toBe('ready')
  expect(sockets).toHaveLength(1)
})

test('terminates a Room lane whose incarnation closed while the connection was down', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  let open = true
  const { driver } = driverWith(sockets, async () => open)
  const attempt = openAttempt(driver, { roomId: 'room', inc: 'inc', lane: { kind: 'control' } })
  await untilReady(attempt)
  open = false
  sockets[0]!.socket.emit('close')
  await vi.waitFor(() => expect(attempt.state()).toBe('closed'))
})

test('releases the connection once the last subscription leaves', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const attempt = openAttempt(driver, route)
  await untilReady(attempt)
  const disconnect = vi.spyOn(sockets[0]!.socket, 'disconnect')
  await attempt.unsubscribe()
  expect(disconnect).toHaveBeenCalledOnce()
})

test("opens and closes a lane beside 100,000 subscribed ones as fast as beside 1,000, touching only that lane's channel", async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const lane = (i: number) => ({ roomId: 'room', inc: 'inc', lane: { kind: 'inbox', member: `m${i}` } }) as const
  let opened = 0
  // The median time to open one more lane and wait until it is ready.
  const perJoin = async (subscribed: number): Promise<number> => {
    while (opened < subscribed) openAttempt(driver, lane(opened++))
    const times: number[] = []
    for (let i = 0; i < 101; i++) {
      const started = performance.now()
      await untilReady(openAttempt(driver, lane(opened++)))
      times.push(performance.now() - started)
    }
    return times.sort((a, b) => a - b)[50]!
  }
  await perJoin(0)
  const small = await perJoin(1_000)
  const large = await perJoin(100_000)
  expect(large / small).toBeLessThan(4)
  const unsubscribe = vi.spyOn(sockets[0]!.socket, 'unsubscribe')
  const last = openAttempt(driver, lane(opened++))
  await untilReady(last)
  await last.unsubscribe()
  await vi.waitFor(() => expect(unsubscribe).toHaveBeenCalledOnce())
  expect(unsubscribe).toHaveBeenCalledWith(channelKey('tf:', 'room', 'inc', encodeLaneKey(lane(opened - 1).lane)))
})
