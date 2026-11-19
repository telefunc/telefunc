import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { CloudflareMember } from 'telefunc/__internal'
import { RoomSessionSubscriptions } from './RoomSessionSubscriptions.js'
import { ROUTE_RENEW_EVERY_MS } from './routes.js'
import type { CloudflareRoomDriver } from './CloudflareRoomDriver.js'
import type { RegisterWire, RenewWire } from './RoomAuthority.js'
import type { LaneId } from '../backend/contract.js'

/** A session's Room subscriptions against an authority that answers each call from `answers`, in order; opening a stub
 *  to it throws the next of `answers.open`, while one is left. */
function setup(
  answers: {
    register: Array<RegisterWire | Error | Promise<RegisterWire>>
    renew: Array<RenewWire | Error>
    open?: Array<Error | undefined>
  },
  lane: LaneId = { kind: 'semantic' },
) {
  const calls = { register: 0, renew: 0 }
  const startedAt = Date.now()
  /** When each registration was made, in ms after the lane subscribed. */
  const registeredAt: number[] = []
  const answer = <T>(queue: Array<T | Error | Promise<T>>): Promise<T> => {
    const next = queue.shift()!
    if (next instanceof Promise) return next
    return next instanceof Error ? Promise.reject(next) : Promise.resolve(next)
  }
  const authority = {
    registerRoute: () => (calls.register++, registeredAt.push(Date.now() - startedAt), answer(answers.register)),
    renewRoutes: () => (calls.renew++, answer(answers.renew).then((renewal) => [renewal])),
    unsubscribeRoute: async () => {},
  }
  const member = { bucket: 'weur', subscribeLocal: () => () => {} } as unknown as CloudflareMember
  const driver = {
    stub: () => {
      const refusal = answers.open?.shift()
      if (refusal) throw refusal
      return authority
    },
  } as unknown as CloudflareRoomDriver
  const subscriptions = new RoomSessionSubscriptions({ sessionId: 'session', member, driver })
  const receiver = { deliver() {} }
  const attempt = subscriptions.subscribe({ roomId: 'room', inc: 'inc', lane }, receiver)
  const states: string[] = []
  const reasons: unknown[] = []
  attempt.onStateChange((state, reason) => {
    states.push(state)
    if (reason !== undefined) reasons.push([state, reason])
  })
  /** Another lane of the room, on the same ordered stub. */
  const subscribe = (other: LaneId) => subscriptions.subscribe({ roomId: 'room', inc: 'inc', lane: other }, receiver)
  return { attempt, calls, registeredAt, states, reasons, subscribe }
}

/** Cloudflare marks a transient failure `.retryable`, and one it says not to retry `.overloaded`. */
const transient = (overloaded: boolean, message = 'reset') =>
  Object.assign(new Error(message), { retryable: true, overloaded })

beforeEach(() => {
  vi.useFakeTimers()
  // The longest waits Cloudflare's backoff draws.
  vi.spyOn(Math, 'random').mockReturnValue(1)
  vi.spyOn(console, 'error').mockImplementation(() => {})
})
afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('a Cloudflare Room lane', () => {
  it('whose route lapsed is lost, registers its route again, and is ready again: a lapse is no end', async () => {
    const { attempt, calls, states } = setup({ register: ['registered', 'registered'], renew: ['lapsed'] })
    await vi.advanceTimersByTimeAsync(0)
    expect(attempt.state()).toBe('ready')
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
    await vi.advanceTimersByTimeAsync(5_000)
    expect(calls).toEqual({ register: 2, renew: 1 })
    expect(states).toEqual(['ready', 'lost', 'ready'])
  })

  // Cloudflare's backoff draws each wait as `100 ms × random × 2^n`.
  it('whose first route registration failed as retryable twice registers it again after the waits the backoff draws, and is ready once it is registered', async () => {
    const drawn: Record<string, unknown> = {}
    for (const random of [1, 0.5]) {
      vi.spyOn(Math, 'random').mockReturnValue(random)
      const { attempt, registeredAt, states } = setup({
        register: [transient(false, 'first'), transient(false, 'second'), 'registered'],
        renew: [],
      })
      await vi.advanceTimersByTimeAsync(1_000)
      drawn[random] = { registeredAt, state: attempt.state(), states }
    }
    expect(drawn).toEqual({
      1: { registeredAt: [0, 100, 300], state: 'ready', states: ['ready'] },
      0.5: { registeredAt: [0, 50, 150], state: 'ready', states: ['ready'] },
    })
  })

  it('whose first route registration fails as retryable 3 times closes with the last error, and is not registered again', async () => {
    const last = transient(false, 'third')
    const { calls, reasons } = setup({
      register: [transient(false, 'first'), transient(false, 'second'), last],
      renew: [],
    })
    await vi.advanceTimersByTimeAsync(60_000)
    expect(calls.register).toBe(3)
    expect(reasons).toHaveLength(1)
    expect(reasons[0]).toEqual(['closed', last])
    expect(reasons[0]).toContain(last)
  })

  for (const [fails, first] of [
    ['as overloaded', transient(true)],
    ['otherwise', new Error('reset')],
  ] as const) {
    it(`whose first route registration fails ${fails} closes with that error at once`, async () => {
      const { calls, reasons } = setup({ register: [first], renew: [] })
      await vi.advanceTimersByTimeAsync(60_000)
      expect(calls.register).toBe(1)
      expect(reasons).toHaveLength(1)
      expect(reasons[0]).toContain(first)
    })
  }

  it('whose first route registration, made again, fails as overloaded closes with that error, and is not registered again', async () => {
    const overloaded = transient(true, 'overloaded')
    const { calls, reasons } = setup({ register: [transient(false), overloaded, 'registered'], renew: [] })
    await vi.advanceTimersByTimeAsync(60_000)
    expect(calls.register).toBe(2)
    expect(reasons).toHaveLength(1)
    expect(reasons[0]).toContain(overloaded)
  })

  // The runtime retries a call itself for up to 10 s; a failure that reaches the code that late isn't made again.
  it('whose first route registration fails as retryable 10 seconds in closes with that error, and is not registered again', async () => {
    const failed = transient(false)
    const late = new Promise<never>((_, reject) => setTimeout(() => reject(failed), 10_000))
    const { calls, reasons } = setup({ register: [late, 'registered'], renew: [] })
    await vi.advanceTimersByTimeAsync(12_000)
    expect(calls.register).toBe(1)
    expect(reasons).toHaveLength(1)
    expect(reasons[0]).toContain(failed)
  })

  // An inbox is ready once its registration went out: a message to its member then follows it to the authority.
  it('that is an inbox whose first route registration failed as retryable is lost until it is sent again', async () => {
    const { attempt, states } = setup(
      { register: [transient(false), 'registered'], renew: [] },
      { kind: 'inbox', member: 'm' },
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(attempt.state()).toBe('lost')
    await vi.advanceTimersByTimeAsync(1_000)
    // Ready when `subscribe()` returned.
    expect(states).toEqual(['lost', 'ready'])
  })

  // A retry made on a fresh stub waits until the failed stub's calls settled, which can be past the 10 s.
  it('whose first route registration would be made again past 10 seconds closes with its last error, and is not made again', async () => {
    const failed = transient(false)
    const late = new Promise<never>((_, reject) => setTimeout(() => reject(failed), 9_800))
    const held = new Promise<RegisterWire>((resolve) => setTimeout(() => resolve('registered'), 10_300))
    const { attempt, calls, subscribe } = setup({ register: [late, held, 'registered'], renew: [] })
    const reasons: unknown[] = []
    attempt.onStateChange((state, reason) => reasons.push([state, reason]))
    subscribe({ kind: 'control' })
    await vi.advanceTimersByTimeAsync(12_000)
    expect(calls.register).toBe(2)
    expect(reasons).toEqual([['closed', failed]])
    expect(reasons[0]).toContain(failed)
  })

  it('unsubscribed while its first route registration waits 50 ms to be made again, registers it no more', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const waiting = setup({ register: [transient(false), 'registered'], renew: [] })
    const unsubscribed = setup({ register: [transient(false), 'registered'], renew: [] })
    await vi.advanceTimersByTimeAsync(25)
    await unsubscribed.attempt.unsubscribe()
    await vi.advanceTimersByTimeAsync(25)
    expect(waiting.registeredAt).toEqual([0, 50])
    await vi.advanceTimersByTimeAsync(60_000)
    expect(unsubscribed.registeredAt).toEqual([0])
  })

  // Cloudflare limits how fast a stub may be created, and its example creates the stub inside the attempt.
  for (const [when, answers] of [
    ['for its first registration', { register: [], open: [transient(true)] }],
    ['for the registration it makes again', { register: [transient(false)], open: [undefined, transient(true)] }],
  ] as const) {
    it(`whose stub to the authority can't be opened ${when} closes with that error`, async () => {
      const { attempt, calls, reasons } = setup({ register: [...answers.register], renew: [], open: [...answers.open] })
      const refusal = answers.open.at(-1)
      await vi.advanceTimersByTimeAsync(60_000)
      expect(attempt.state()).toBe('closed')
      expect(reasons).toHaveLength(1)
      expect(reasons[0]).toContain(refusal)
      expect(calls.register).toBe(answers.register.length)
    })
  }

  it('whose incarnation was dropped ends on purpose, with no reason', async () => {
    const { attempt, calls } = setup({ register: ['registered'], renew: ['dropped'] })
    const reasons: unknown[] = []
    attempt.onStateChange((state, reason) => reasons.push([state, reason]))
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS + 5_000)
    expect(attempt.state()).toBe('closed')
    expect(reasons.at(-1)).toEqual(['closed', undefined])
    expect(calls.register).toBe(1)
  })
})
