import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { CloudflareMember } from 'telefunc/__internal'
import { RoomSessionSubscriptions } from './RoomSessionSubscriptions.js'
import { ROUTE_RENEW_EVERY_MS } from './routes.js'
import type { CloudflareRoomDriver } from './CloudflareRoomDriver.js'
import type { RegisterWire, RenewWire } from './RoomAuthority.js'

/** A session's Room subscriptions against an authority that answers each call from `answers`, in order; opening a stub
 *  to it throws the next of `answers.open`, while one is left. */
function setup(answers: {
  register: Array<RegisterWire | Error | Promise<never>>
  renew: Array<RenewWire | Error>
  open?: Array<Error | undefined>
}) {
  const calls = { register: 0, renew: 0 }
  const answer = <T>(queue: Array<T | Error | Promise<never>>): Promise<T> => {
    const next = queue.shift()!
    if (next instanceof Promise) return next
    return next instanceof Error ? Promise.reject(next) : Promise.resolve(next)
  }
  const authority = {
    registerRoute: () => (calls.register++, answer(answers.register)),
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
  const attempt = subscriptions.subscribe({ roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } }, receiver)
  const states: string[] = []
  const reasons: unknown[] = []
  attempt.onStateChange((state, reason) => {
    states.push(state)
    if (reason !== undefined) reasons.push([state, reason instanceof Error ? reason.message : reason])
  })
  return { attempt, calls, states, reasons }
}

/** Cloudflare marks a transient failure `.retryable`, and one it says not to retry `.overloaded`. */
const transient = (overloaded: boolean) => Object.assign(new Error('reset'), { retryable: true, overloaded })

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

  it('whose first route registration failed as retryable registers it again within a second, and is ready once it is registered', async () => {
    const { attempt, calls, states } = setup({ register: [transient(false), 'registered'], renew: [] })
    await vi.advanceTimersByTimeAsync(1_000)
    expect(calls.register).toBe(2)
    expect(attempt.state()).toBe('ready')
    expect(states).toEqual(['ready'])
  })

  it('whose first route registration fails as retryable 3 times closes with that error, and is not registered again', async () => {
    const { calls, reasons } = setup({ register: [transient(false), transient(false), transient(false)], renew: [] })
    await vi.advanceTimersByTimeAsync(60_000)
    expect({ registered: calls.register, reasons }).toEqual({ registered: 3, reasons: [['closed', 'reset']] })
  })

  for (const [fails, error] of [
    ['as overloaded', transient(true)],
    ['otherwise', new Error('reset')],
  ] as const) {
    it(`whose first route registration fails ${fails} closes with that error at once`, async () => {
      const { calls, reasons } = setup({ register: [error], renew: [] })
      await vi.advanceTimersByTimeAsync(60_000)
      expect({ registered: calls.register, reasons }).toEqual({ registered: 1, reasons: [['closed', 'reset']] })
    })
  }

  // The runtime retries a call itself for up to 10 s; a failure that reaches the code that late isn't made again.
  it('whose first route registration fails as retryable 10 seconds in closes with that error, and is not registered again', async () => {
    const late = new Promise<never>((_, reject) => setTimeout(() => reject(transient(false)), 10_000))
    const { calls, reasons } = setup({ register: [late, 'registered'], renew: [] })
    await vi.advanceTimersByTimeAsync(12_000)
    expect({ registered: calls.register, reasons }).toEqual({ registered: 1, reasons: [['closed', 'reset']] })
  })

  it('unsubscribed while its first route registration waits to be made again, registers it no more', async () => {
    const { attempt, calls } = setup({ register: [transient(false), 'registered'], renew: [] })
    await vi.advanceTimersByTimeAsync(0)
    await attempt.unsubscribe()
    await vi.advanceTimersByTimeAsync(60_000)
    expect(calls.register).toBe(1)
  })

  // Cloudflare limits how fast a stub may be created, and its example creates the stub inside the attempt.
  for (const [when, answers] of [
    ['for its first registration', { register: [], open: [transient(true)] }],
    ['for the registration it makes again', { register: [transient(false)], open: [undefined, transient(true)] }],
  ] as const) {
    it(`whose stub to the authority can't be opened ${when} closes with that error`, async () => {
      const { attempt, calls, reasons } = setup({ register: [...answers.register], renew: [], open: [...answers.open] })
      await vi.advanceTimersByTimeAsync(60_000)
      expect(attempt.state()).toBe('closed')
      expect(reasons).toEqual([['closed', 'reset']])
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
