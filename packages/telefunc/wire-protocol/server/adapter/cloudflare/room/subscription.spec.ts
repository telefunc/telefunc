import { afterEach, expect, test, vi } from 'vitest'
import type { CloudflareRoomAuthorityStub } from './backend.js'
import { encodeLaneKey } from '../../../../backend/room/lane-key.js'
import { ROUTE_RENEW_EVERY_MS } from './routes.js'
import { CloudflareRoomSessionManager, type CloudflareRoomSubscriptionAttempt } from './subscription.js'

const source = { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } } as const
const route = { roomId: 'room', inc: 'inc', laneKey: encodeLaneKey({ kind: 'semantic' }), sessionDoId: 'session' }
const frame = (byte: number, leaseId: string) => ({
  ...route,
  leaseId,
  payload: new Uint8Array([byte]),
  info: { seq: byte, timestamp: 1 },
})

function openAttempt(
  authority: Record<string, (...args: never[]) => Promise<unknown>> = {},
  received: number[] = [],
  manager = new CloudflareRoomSessionManager('session'),
) {
  const stub = {
    registerRoute: async () => ({ ok: true }),
    renewRoute: async () => true,
    unsubscribeRoute: async () => {},
    ...authority,
  }
  const attempt = manager.openSubscription(
    source,
    () => stub as unknown as CloudflareRoomAuthorityStub,
    (payload) => void received.push(payload[0]!),
  )
  return attempt
}

function endOf(attempt: CloudflareRoomSubscriptionAttempt): Promise<Error | undefined> {
  return new Promise((resolve) => attempt.onStateChange((state, reason) => state === 'closed' && resolve(reason)))
}

afterEach(() => {
  vi.useRealTimers()
})
test('a session delivers a frame for the lease its subscription holds, and drops one for another lease', async () => {
  const manager = new CloudflareRoomSessionManager('session')
  const received: number[] = []
  const attempt = openAttempt({}, received, manager)
  await vi.waitFor(() => expect(attempt.state()).toBe('ready'))
  manager.deliver(frame(1, attempt.leaseId))
  // A lease this session held before a restart.
  manager.deliver(frame(2, 'stale-lease'))
  expect(received).toEqual([1])
  await attempt.unsubscribe()
})

test('an attempt takes a frame the authority delivers before its registration reply arrives', async () => {
  const manager = new CloudflareRoomSessionManager('session')
  const received: number[] = []
  const registered = Promise.withResolvers<{ ok: true }>()
  const attempt = openAttempt({ registerRoute: () => registered.promise }, received, manager)
  // The authority fans out once its transaction stored the route, over another stub than the one the reply takes.
  manager.deliver(frame(1, attempt.leaseId))
  expect(attempt.state()).toBe('establishing')
  expect(received).toEqual([1])
  registered.resolve({ ok: true })
  await vi.waitFor(() => expect(attempt.state()).toBe('ready'))
  await attempt.unsubscribe()
})

test("a session's route calls to a room share one ordered stub, so a released attempt's calls can't overtake its successor's", async () => {
  const manager = new CloudflareRoomSessionManager('session')
  const calls: string[] = []
  const registering = Promise.withResolvers<void>()
  const stub = (name: string) => ({
    registerRoute: async ({ leaseId }: { leaseId: string }) => {
      calls.push(`${name}:register:${leaseId}`)
      await registering.promise
      return { ok: true }
    },
    unsubscribeRoute: async ({ leaseId }: { leaseId: string }) => void calls.push(`${name}:unsubscribe:${leaseId}`),
  })
  const first = openAttempt(stub('first'), [], manager)
  void first.unsubscribe()
  const second = openAttempt(stub('second'), [], manager)
  registering.resolve()
  await vi.waitFor(() => expect(second.state()).toBe('ready'))
  expect(calls).toEqual([
    `first:register:${first.leaseId}`,
    `first:unsubscribe:${first.leaseId}`,
    `first:register:${second.leaseId}`,
  ])
  await second.unsubscribe()
})

test('a route call after a failed call to its room opens a fresh stub, as a stub that rejected may be broken', async () => {
  vi.useFakeTimers()
  const manager = new CloudflareRoomSessionManager('session')
  const renewedThrough: number[] = []
  let opened = 0
  const openAuthority = () => {
    const stub = opened++
    return {
      registerRoute: async () => ({ ok: true }),
      renewRoute: async () => {
        renewedThrough.push(stub)
        return true
      },
      unsubscribeRoute: async () => {},
    } as unknown as CloudflareRoomAuthorityStub
  }
  const attempt = manager.openSubscription(source, openAuthority, () => {})
  await vi.advanceTimersByTimeAsync(0)
  expect(attempt.state()).toBe('ready')
  // A commit to the room through its own stub (the second) fails, so that stub may be broken.
  await manager.callAuthority('room', openAuthority, () => Promise.reject(new Error('reset'))).catch(() => {})
  await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
  expect(renewedThrough).toEqual([2])
  await attempt.unsubscribe()
})

test.each([
  [
    'its registration throws',
    async () => {
      throw new Error('authority unreachable')
    },
    'authority unreachable',
  ],
  [
    'the room refuses its route',
    async () => ({ rejected: true, reason: 'no open incarnation' }),
    'no open incarnation',
  ],
])('an attempt whose %s ends closed, with the reason', async (_name, registerRoute, reason) => {
  await expect(endOf(openAttempt({ registerRoute }))).resolves.toMatchObject({ message: reason })
})

test.each([
  ['stays ready and renews again while its route is live', async () => true, 'ready'],
  ['ends when its route lapsed or its generation was dropped', async () => false, 'closed'],
])('a ready attempt %s', async (_name, renewRoute, state) => {
  vi.useFakeTimers()
  let released = 0
  const attempt = openAttempt({ renewRoute, unsubscribeRoute: async () => void released++ })
  await vi.advanceTimersByTimeAsync(0)
  expect(attempt.state()).toBe('ready')
  await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
  expect(attempt.state()).toBe(state)
  expect(released).toBe(0)
  await attempt.unsubscribe()
})

test('an attempt whose renewal throws ends with that error as its reason', async () => {
  vi.useFakeTimers()
  const unreachable = new Error('authority unreachable')
  const attempt = openAttempt({
    renewRoute: async () => {
      throw unreachable
    },
  })
  const ended = endOf(attempt)
  await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
  await expect(ended).resolves.toBe(unreachable)
})

test('a session drops a delivery to an attempt that ended at renewal, and its route is released once', async () => {
  vi.useFakeTimers()
  let released = 0
  const received: number[] = []
  const manager = new CloudflareRoomSessionManager('session')
  const attempt = openAttempt(
    { renewRoute: async () => false, unsubscribeRoute: async () => void released++ },
    received,
    manager,
  )
  await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
  expect(attempt.state()).toBe('closed')
  manager.deliver(frame(1, attempt.leaseId))
  expect(received).toEqual([])
  await attempt.unsubscribe()
  expect(released).toBe(1)
})

test('an attempt unsubscribed while its registration is in flight renews nothing', async () => {
  vi.useFakeTimers()
  const registered = Promise.withResolvers<{ ok: true }>()
  const renewals: unknown[] = []
  const attempt = openAttempt({
    registerRoute: () => registered.promise,
    renewRoute: async () => renewals.push('renew') > 0,
  })
  const unsubscribed = attempt.unsubscribe()
  registered.resolve({ ok: true })
  await unsubscribed
  await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS * 2)
  expect({ state: attempt.state(), renewals }).toEqual({ state: 'closed', renewals: [] })
})

test('an attempt that ended while a renewal was in flight renews no more', async () => {
  vi.useFakeTimers()
  const renewals: Array<() => void> = []
  const attempt = openAttempt({
    renewRoute: () => new Promise((resolve) => renewals.push(() => resolve(true))),
  })
  await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
  void attempt.unsubscribe()
  renewals[0]!()
  await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
  expect(attempt.state()).toBe('closed')
  expect(renewals).toHaveLength(1)
})
