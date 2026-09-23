import { EventEmitter } from 'node:events'
import type { Redis } from 'ioredis'
import { expect, onTestFinished, test, vi } from 'vitest'
import type { SubscriptionAttemptState } from 'telefunc/__internal'
import { RedisSubscriptionDriver } from './subscriber-transport.js'

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
    return next.socket as unknown as Redis
  })
  return {
    driver: new RedisSubscriptionDriver({ prefix: 'tf:', createSubscriber, validateGeneration }),
    createSubscriber,
  }
}

const lane = { key: 'chat', kind: 'text' } as const

test('shares one subscriber connection across lanes', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver, createSubscriber } = driverWith(sockets)
  const first = driver.bind(lane).open(
    () => {},
    () => 1,
  )
  const second = driver.bind({ key: 'other', kind: 'binary' }).open(
    () => {},
    () => 1,
  )
  await Promise.all([first.ready, second.ready])
  expect(createSubscriber).toHaveBeenCalledOnce()
  expect(sockets[0]!.subscribed.flat()).toHaveLength(2)
})

test('re-subscribes on a fresh connection and resumes delivery after a drop', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const received: number[] = []
  const states: SubscriptionAttemptState[] = []
  const attempt = driver.bind(lane).open(
    (payload) => void received.push(payload[0]!),
    () => 1,
  )
  await attempt.ready
  attempt.onStateChange((state) => states.push(state))
  const channel = sockets[0]!.subscribed[0]![0]!
  sockets[0]!.deliver(channel, orderingFrame(5, 1))
  sockets[0]!.socket.emit('close')
  await vi.waitFor(() => expect(states).toEqual(['lost', 'ready']))
  expect(sockets[1]!.subscribed).toEqual([[channel]])
  sockets[1]!.deliver(channel, orderingFrame(1, 2))
  expect(received).toEqual([1, 2])
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
  await attempt.ready
  const flush = driver.prepareFlush(source)
  sockets[0]!.socket.emit('close')
  await new Promise((resolve) => setTimeout(resolve, 10))
  expect(unhandled).toEqual([])
  await expect(flush.delivery).rejects.toThrow()
})

test('terminates a Room lane whose incarnation closed while the connection was down', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  let open = true
  const { driver } = driverWith(sockets, async () => open)
  const attempt = driver.bind({ roomId: 'room', inc: 'inc', lane: { kind: 'control' } }).open(
    () => {},
    () => 1,
  )
  await attempt.ready
  open = false
  sockets[0]!.socket.emit('close')
  await vi.waitFor(() => expect(attempt.state()).toBe('terminated'))
})

test('releases the connection once the last subscription leaves', async () => {
  const sockets: ReturnType<typeof fakeSubscriber>[] = []
  const { driver } = driverWith(sockets)
  const attempt = driver.bind(lane).open(
    () => {},
    () => 1,
  )
  await attempt.ready
  const disconnect = vi.spyOn(sockets[0]!.socket, 'disconnect')
  await attempt.unsubscribe()
  expect(disconnect).toHaveBeenCalledOnce()
})
