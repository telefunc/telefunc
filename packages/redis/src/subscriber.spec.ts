import { EventEmitter } from 'node:events'
import type { SubscriberSocket } from './ioredis.js'
import { expect, onTestFinished, test, vi } from 'vitest'
import type { SubscriptionAttempt, SubscriptionState } from 'telefunc/__internal'
import { RedisSubscriptionDriver } from './subscriber.js'

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

function orderingFrame(seq: number, payload: number): Uint8Array {
  const frame = new Uint8Array(17)
  const view = new DataView(frame.buffer)
  view.setUint32(4, seq)
  view.setUint32(12, 1)
  frame[16] = payload
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

const route = { key: 'chat', kind: 'text' } as const

test('shares one subscriber connection across lanes', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver, createSubscriber } = driverWith(sockets)
  const first = driver.bind(route).open(
    () => {},
    () => 1,
  )
  const second = driver.bind({ key: 'other', kind: 'binary' }).open(
    () => {},
    () => 1,
  )
  await Promise.all([untilReady(first), untilReady(second)])
  expect(createSubscriber).toHaveBeenCalledOnce()
  expect(sockets[0]!.subscribed.flat()).toHaveLength(2)
})

test('re-subscribes on a fresh connection and resumes delivery after a drop', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const received: number[] = []
  const states: SubscriptionState[] = []
  const attempt = driver.bind(route).open(
    (payload) => void received.push(payload[0]!),
    () => 1,
  )
  await untilReady(attempt)
  attempt.onStateChange((state) => states.push(state))
  const channel = sockets[0]!.subscribed[0]![0]!
  sockets[0]!.deliver(channel, orderingFrame(5, 1))
  sockets[0]!.socket.emit('close')
  await vi.waitFor(() => expect(states).toEqual(['lost', 'ready']))
  expect(sockets[1]!.subscribed).toEqual([[channel]])
  sockets[1]!.deliver(channel, orderingFrame(1, 2))
  expect(received).toEqual([1, 2])
})

test("reports each outage with its own connection's error, not an earlier connection's", async () => {
  const report = vi.spyOn(console, 'error').mockImplementation(() => {})
  onTestFinished(() => report.mockRestore())
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const attempt = driver.bind(route).open(
    () => {},
    () => 1,
  )
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
  const first = driver.bind(route).open(
    () => {},
    () => 1,
  )
  await vi.waitFor(() => expect(report).toHaveBeenCalledOnce())
  // Its last subscription leaves mid-outage, which releases the connection; a later one starts afresh.
  await first.unsubscribe()
  const second = driver.bind(route).open(
    () => {},
    () => 1,
  )
  await vi.waitFor(() => expect(report).toHaveBeenCalledTimes(2))
  await second.unsubscribe()
})

test("a fence resolves when its subscription's owner releases it: no receiver is left to hand off to", async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const source = { roomId: 'room', inc: 'inc', lane: { kind: 'control' } } as const
  const attempt = driver.bind(source).open(
    () => {},
    () => 1,
  )
  await untilReady(attempt)
  const fence = driver.prepareFence(source)
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
  const attempt = driver.bind(route).open(
    () => {},
    () => 1,
  )
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
  const attempt = driver.bind(source).open(
    () => {},
    () => 1,
  )
  await untilReady(attempt)
  const fence = driver.prepareFence(source)
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
  const broadcast = driver.bind(route).open(
    () => {},
    () => 1,
  )
  await untilReady(broadcast)
  const room = driver.bind({ roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } }).open(
    () => {},
    () => 1,
  )
  await expect(untilReady(room)).rejects.toBe(failure)
  expect(broadcast.state()).toBe('ready')
  expect(sockets).toHaveLength(1)
})

test('terminates a Room lane whose incarnation closed while the connection was down', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  let open = true
  const { driver } = driverWith(sockets, async () => open)
  const attempt = driver.bind({ roomId: 'room', inc: 'inc', lane: { kind: 'control' } }).open(
    () => {},
    () => 1,
  )
  await untilReady(attempt)
  open = false
  sockets[0]!.socket.emit('close')
  await vi.waitFor(() => expect(attempt.state()).toBe('closed'))
})

test('releases the connection once the last subscription leaves', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const attempt = driver.bind(route).open(
    () => {},
    () => 1,
  )
  await untilReady(attempt)
  const disconnect = vi.spyOn(sockets[0]!.socket, 'disconnect')
  await attempt.unsubscribe()
  expect(disconnect).toHaveBeenCalledOnce()
})
