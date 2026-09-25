import { expect, test, vi } from 'vitest'
import '../../../../../node/server/async_hooks.js'
import {
  CloudflareBackend,
  CloudflareRoomSessionManager,
  type CloudflareRoomAuthorityStub,
  type CloudflareRoomNamespace,
} from './backend.js'
import { encodeLaneKey } from '../../../../backend/room/lane-key.js'
import { ROUTE_RENEW_EVERY_MS } from './routes.js'
import type { CloudflareRoomSubscriptionAttempt } from './subscription.js'
import { CloudflareBroadcastTransport } from '../broadcast.js'
import { withCloudflareSession } from '../session.js'

const noBroadcast = () => {
  throw new Error('this spec uses no Broadcast')
}
const broadcast = new CloudflareBroadcastTransport({
  baseInstanceName: 'telefunc',
  locationFallback: 'weur',
  namespace: noBroadcast,
})

test("a session's commits to a room reach its authority in the order Room sent them", async () => {
  const arrived: string[] = []
  let opened = 0
  // Like Cloudflare's stubs: calls through one stub arrive in call order, calls through different stubs race; the
  // first stub opened is the slowest.
  const rooms = {
    idFromName: (name: string) => name,
    get: () => {
      const latency = opened++ === 0 ? 20 : 0
      let arrival = Promise.resolve()
      return {
        commitLane(_inc: string, _lane: unknown, payload: Uint8Array) {
          arrival = arrival.then(() => new Promise((resolve) => setTimeout(resolve, latency)))
          return arrival.then(() => {
            arrived.push(new TextDecoder().decode(payload))
            return { seq: arrived.length, timestamp: 1, receivers: 0, deliveryToken: 'token' }
          })
        },
        awaitDelivery: async () => {},
      }
    },
  } as unknown as CloudflareRoomNamespace
  const backend = new CloudflareBackend({ rooms: () => rooms, broadcast })
  const manager = new CloudflareRoomSessionManager('session')
  const commit = (text: string) =>
    backend.commitLane('room', 'inc', { kind: 'semantic' }, new TextEncoder().encode(text))
  await withCloudflareSession({ room: () => manager, broadcast: noBroadcast }, () =>
    Promise.all([commit('first'), commit('second')]),
  )
  expect(arrived).toEqual(['first', 'second'])
})

test('outside a session, a commit goes through a stub of its own, and a stale answer comes back as it is', async () => {
  const stale = { stale: 'head' }
  const rooms = { idFromName: (name: string) => name, get: () => ({ commitLane: async () => stale }) }
  const backend = new CloudflareBackend({ rooms: () => rooms as unknown as CloudflareRoomNamespace, broadcast })
  await expect(backend.commitLane('room', 'inc', { kind: 'semantic' }, new Uint8Array())).resolves.toBe(stale)
})

test('a session delivers a frame for the lease its subscription holds, and drops one for another lease', async () => {
  const manager = new CloudflareRoomSessionManager('session')
  const received: number[] = []
  const authority = { registerRoute: async () => ({ ok: true }), unsubscribeRoute: async () => {} }
  const attempt = manager.openSubscription(
    { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } },
    () => authority as unknown as CloudflareRoomAuthorityStub,
    (payload) => void received.push(payload[0]!),
  )
  await vi.waitFor(() => expect(attempt.state()).toBe('ready'))
  const route = { roomId: 'room', inc: 'inc', laneKey: encodeLaneKey({ kind: 'semantic' }), sessionDoId: 'session' }
  const frame = (byte: number, leaseId: string) => ({
    ...route,
    leaseId,
    payload: new Uint8Array([byte]),
    seq: byte,
    timestamp: 1,
  })
  await manager.deliver(frame(1, attempt.leaseId))
  // A lease this session held before a restart.
  await manager.deliver(frame(2, 'stale-lease'))
  expect(received).toEqual([1])
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
  const source = { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } } as const
  const first = manager.openSubscription(
    source,
    () => stub('first') as unknown as CloudflareRoomAuthorityStub,
    () => {},
  )
  void first.unsubscribe()
  const second = manager.openSubscription(
    source,
    () => stub('second') as unknown as CloudflareRoomAuthorityStub,
    () => {},
  )
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
  try {
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
    const attempt = manager.openSubscription(
      { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } },
      openAuthority,
      () => {},
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(attempt.state()).toBe('ready')
    // A commit to the room through its own stub (the second) fails, so that stub may be broken.
    await manager.authorityCalls.call('room', openAuthority, () => Promise.reject(new Error('reset'))).catch(() => {})
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
    expect(renewedThrough).toEqual([2])
    await attempt.unsubscribe()
  } finally {
    vi.useRealTimers()
  }
})

function openAttempt(authority: Record<string, (...args: never[]) => Promise<unknown>>, received: number[] = []) {
  const route = {
    registerRoute: async () => ({ ok: true }),
    renewRoute: async () => true,
    unsubscribeRoute: async () => {},
    ...authority,
  }
  const attempt = new CloudflareRoomSessionManager('session').openSubscription(
    { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } },
    () => route as unknown as CloudflareRoomAuthorityStub,
    (payload) => void received.push(payload[0]!),
  )
  return attempt
}

function endOf(attempt: CloudflareRoomSubscriptionAttempt): Promise<Error | undefined> {
  return new Promise((resolve) => attempt.onStateChange((state, reason) => state === 'closed' && resolve(reason)))
}

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
  ['stays ready and renews again while its route is live', async () => true, 'ready', 0],
  ['ends when its route lapsed or its generation was dropped', async () => false, 'closed', 0],
  [
    'ends when the authority is unreachable',
    async () => {
      throw new Error('unreachable')
    },
    'closed',
    0,
  ],
])('a ready attempt %s', async (_name, renewRoute, state, releases) => {
  vi.useFakeTimers()
  try {
    let released = 0
    const attempt = openAttempt({ renewRoute, unsubscribeRoute: async () => void released++ })
    await vi.advanceTimersByTimeAsync(0)
    expect(attempt.state()).toBe('ready')
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
    expect(attempt.state()).toBe(state)
    expect(released).toBe(releases)
    await attempt.unsubscribe()
  } finally {
    vi.useRealTimers()
  }
})

test('an attempt whose renewal throws ends with that error as its reason', async () => {
  vi.useFakeTimers()
  try {
    const unreachable = new Error('authority unreachable')
    const attempt = openAttempt({
      renewRoute: async () => {
        throw unreachable
      },
    })
    const ended = endOf(attempt)
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
    await expect(ended).resolves.toBe(unreachable)
  } finally {
    vi.useRealTimers()
  }
})

test('an ended attempt drops later deliveries, and its route is released once', async () => {
  vi.useFakeTimers()
  try {
    let released = 0
    const received: number[] = []
    const attempt = openAttempt(
      { renewRoute: async () => false, unsubscribeRoute: async () => void released++ },
      received,
    )
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
    expect(attempt.state()).toBe('closed')
    attempt.deliver(new Uint8Array([1]), 1, 1)
    expect(received).toEqual([])
    await attempt.unsubscribe()
    await attempt.unsubscribe()
    expect(released).toBe(1)
  } finally {
    vi.useRealTimers()
  }
})

test('an attempt that ended while a renewal was in flight renews no more', async () => {
  vi.useFakeTimers()
  try {
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
  } finally {
    vi.useRealTimers()
  }
})
