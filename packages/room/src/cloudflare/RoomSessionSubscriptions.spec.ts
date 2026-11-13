import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { CloudflareMember } from 'telefunc/__internal'
import { RoomSessionSubscriptions } from './RoomSessionSubscriptions.js'
import { ROUTE_RENEW_EVERY_MS } from './routes.js'
import type { CloudflareRoomDriver } from './CloudflareRoomDriver.js'
import type { RegisterWire, RenewWire } from './RoomAuthority.js'

/** A session's Room subscriptions against an authority that answers each call from `answers`, in order. */
function setup(answers: { register: Array<RegisterWire | Error>; renew: Array<RenewWire | Error> }) {
  const calls = { register: 0, renew: 0 }
  const answer = <T>(queue: Array<T | Error>): Promise<T> => {
    const next = queue.shift()!
    return next instanceof Error ? Promise.reject(next) : Promise.resolve(next)
  }
  const authority = {
    registerRoute: () => (calls.register++, answer(answers.register)),
    renewRoutes: () => (calls.renew++, answer(answers.renew).then((renewal) => [renewal])),
    unsubscribeRoute: async () => {},
  }
  const member = { bucket: 'weur', subscribeLocal: () => () => {} } as unknown as CloudflareMember
  const driver = { stub: () => authority } as unknown as CloudflareRoomDriver
  const subscriptions = new RoomSessionSubscriptions({ sessionId: 'session', member, driver })
  const receiver = { deliver() {} }
  const attempt = subscriptions.subscribe({ roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } }, receiver)
  const states: string[] = []
  attempt.onStateChange((state) => states.push(state))
  return { attempt, calls, states }
}

beforeEach(() => vi.useFakeTimers())
afterEach(() => vi.useRealTimers())

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

  it('whose route registration failed registers it again, and is ready once it is registered', async () => {
    const { attempt, calls, states } = setup({
      register: [new Error('the authority is overloaded'), 'registered'],
      renew: [],
    })
    vi.spyOn(console, 'error').mockImplementation(() => {})
    await vi.advanceTimersByTimeAsync(5_000)
    expect(calls.register).toBe(2)
    expect(attempt.state()).toBe('ready')
    expect(states).toEqual(['ready'])
  })

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
