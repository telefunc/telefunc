import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  LEASE_RENEW_EVERY_MS as ROUTE_RENEW_EVERY_MS,
  OrderedStubs,
  type BackendReceiver,
  type CloudflareMember,
  type TelefuncExtensionNamespace,
} from 'telefunc/__internal'
import { RoomSessionSubscriptions } from './RoomSessionSubscriptions.js'
import { CloudflareRoomDriver } from './CloudflareRoomDriver.js'
import type { OperationWire, RegisterWire, RenewWire } from './RoomAuthority.js'
import type { LaneId } from '../backend/contract.js'
import type { RoomOperationRequest } from '../server/store/roomOperations.js'

/** A session's Room subscriptions against an authority that answers each call from `answers`, in order; opening a stub
 *  to it throws the next of `answers.open`, while one is left. Its lane is `lane`, subscribed as `registeredBy` says. */
function setup(
  answers: {
    register: Array<RegisterWire | Error | Promise<RegisterWire>>
    renew: Array<RenewWire | Error | Promise<RenewWire>>
    open?: Array<Error | undefined>
    /** The answers to operations that carry an inbox's route. */
    carry?: Array<OperationWire | Error | Promise<OperationWire>>
    /** The grant's answer, where it isn't at once, or the answers to each grant asked for, in order. */
    grant?: Promise<number> | Array<number | Error | Promise<number>>
    /** The answers to the session's commits. */
    commit?: Array<Error>
  },
  lane: LaneId = { kind: 'semantic' },
  registeredBy?: 'operation',
) {
  const calls = { register: 0, renew: 0 }
  /** How many grants the session asked the authority for. */
  const grants = { asked: 0 }
  /** The routes the session removed at the authority. */
  const removed: unknown[] = []
  /** The operations that carried an inbox's route, as the authority got them, and the order each was sent under. */
  const carried: unknown[] = []
  const carriedOrders: unknown[] = []
  const startedAt = Date.now()
  /** When each registration was made, in ms after the lane subscribed. */
  const registeredAt: number[] = []
  const answer = <T>(queue: Array<T | Error | Promise<T>>): Promise<T> => {
    const next = queue.shift()
    if (next === undefined) throw new Error('The test gave the authority no answer for this call')
    if (next instanceof Promise) return next
    return next instanceof Error ? Promise.reject(next) : Promise.resolve(next)
  }
  /** The authority's answer to each call the session makes, by method. */
  const authority: Record<string, (args: unknown[]) => Promise<unknown>> = {
    grantHolder: () => {
      grants.asked++
      const { grant } = answers
      return Array.isArray(grant) ? answer(grant) : (grant ?? Promise.resolve(1))
    },
    runOperationWithInbox: (args) => {
      carried.push(args[1])
      if (typeof args[2] === 'object' && args[2] !== null && 'order' in args[2]) carriedOrders.push(args[2].order)
      return answer(answers.carry ?? [])
    },
    registerRoute: () => (calls.register++, registeredAt.push(Date.now() - startedAt), answer(answers.register)),
    renewRoutes: () => (calls.renew++, answer(answers.renew).then((renewal) => [renewal])),
    unsubscribeRoute: (args) => (removed.push(args[0]), Promise.resolve('removed')),
    commitLane: () => answer(answers.commit ?? []),
  }
  const namespace: TelefuncExtensionNamespace = {
    idFromName: (name) => name,
    idFromString: (id) => id,
    get: () => {
      const refusal = answers.open?.shift()
      if (refusal) throw refusal
      return {
        telefuncDurableObjectCall: (_extension, method, args) =>
          authority[method]?.(args) ?? Promise.reject(new Error(`The test's authority takes no ${method} call`)),
      }
    },
  }
  /** What the session hands Broadcast to take the lane's frames, by route key. */
  const takes = new Map<string, BackendReceiver>()
  const member: CloudflareMember = {
    bucket: 'weur',
    hostKey: 'host',
    subscribeLocal: (route, taker) => (takes.set(route.key, taker), () => {}),
  }
  const driver = new CloudflareRoomDriver({ getNamespace: () => namespace, locationFallback: 'weur' })
  const subscriptions = new RoomSessionSubscriptions({ sessionId: 'session', member, driver })
  /** The seq of each frame the lane's receiver got. */
  const delivered: number[] = []
  const receiver: BackendReceiver = { deliver: (_payload, info) => void delivered.push(info.seq) }
  const attempt = subscriptions.subscribe({ roomId: 'room', inc: 'inc', lane, registeredBy }, receiver)
  const states: string[] = []
  const reasons: unknown[] = []
  attempt.onStateChange((state, reason) => {
    states.push(state)
    if (reason !== undefined) reasons.push([state, reason])
  })
  /** Another lane of the room, on the same ordered stub. */
  const subscribe = (other: LaneId) => subscriptions.subscribe({ roomId: 'room', inc: 'inc', lane: other }, receiver)
  /** The room's authority hands the lane's frame `seq` to this session. */
  const handOn = (seq: number) => {
    for (const taker of takes.values()) taker.deliver(new Uint8Array(), { seq, timestamp: seq })
  }
  /** A commit of the session's to the room, through its ordered stub, as the driver's `commitLane` sends it. */
  const commit = () =>
    subscriptions
      .callAuthority('room', (stub) => stub.commitLane('room', 'inc', { kind: 'semantic' }, new Uint8Array()))
      .catch(() => {})
  /** The session's attempt on `lane`, as its driver finds it. */
  const attemptOn = () => {
    const found = subscriptions.attemptOf('room', 'inc', lane)
    if (found === undefined) throw new Error('The lane has no attempt')
    return found
  }
  return {
    attempt,
    calls,
    grants,
    carried,
    carriedOrders,
    removed,
    registeredAt,
    states,
    reasons,
    subscribe,
    delivered,
    handOn,
    subscriptions,
    attemptOn,
    commit,
  }
}

/** Cloudflare marks a transient failure `.retryable`, and one it says not to retry `.overloaded`. */
const transient = (overloaded: boolean, message = 'reset') =>
  Object.assign(new Error(message), { retryable: true, overloaded })

/** Whether `order`, as a call carried it, is under `grant`. */
const isOrderOf = (order: unknown, grant: number | null) =>
  typeof order === 'object' && order !== null && 'grant' in order && order.grant === grant

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

  it('whose renewal the room may have applied, its answer lost, is lost, registers its route again, and is ready again', async () => {
    const { calls, states } = setup({ register: ['registered', 'registered'], renew: [new Error('connection reset')] })
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
    await vi.advanceTimersByTimeAsync(5_000)
    expect({ states, calls }).toEqual({ states: ['ready', 'lost', 'ready'], calls: { register: 2, renew: 1 } })
  })

  it('whose renewal the room never answers is lost once the renewal is given up, before its 90 s lease could have lapsed, and registers its route again', async () => {
    const { calls, states } = setup({ register: ['registered', 'registered'], renew: [new Promise<never>(() => {})] })
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(2 * ROUTE_RENEW_EVERY_MS - 1)
    expect(states).toEqual(['ready'])
    await vi.advanceTimersByTimeAsync(5_000)
    expect({ states, calls }).toEqual({ states: ['ready', 'lost', 'ready'], calls: { register: 2, renew: 1 } })
  })

  it('unsubscribed and subscribed again while its renewal waits for its answer, a lapse answered late changes nothing of the new subscription', async () => {
    const late = new Promise<RenewWire>((resolve) => setTimeout(() => resolve('lapsed'), ROUTE_RENEW_EVERY_MS + 5_000))
    const { attempt, calls, states, subscribe } = setup({ register: ['registered', 'registered'], renew: [late] })
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS + 1_000)
    await attempt.unsubscribe()
    const again = subscribe({ kind: 'semantic' })
    const after: string[] = []
    again.onStateChange((state) => void after.push(state))
    await vi.advanceTimersByTimeAsync(10_000)
    expect({ states, after, state: again.state(), calls }).toEqual({
      states: ['ready', 'closed'],
      after: ['ready'],
      state: 'ready',
      calls: { register: 2, renew: 1 },
    })
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
    const late = new Promise<never>((_, reject) => setTimeout(() => reject(failed), 9_999))
    const { calls, reasons } = setup({ register: [late, 'registered'], renew: [] })
    await vi.advanceTimersByTimeAsync(12_000)
    expect(calls.register).toBe(1)
    expect(reasons).toHaveLength(1)
    expect(reasons[0]).toContain(failed)
  })

  // A member's join waits for its inbox: a private message to the member can't reach the authority before its route.
  it('that is an inbox is ready once its route is registered, not when the registration went out, also where its first registration failed as retryable', async () => {
    const registered = Promise.withResolvers<RegisterWire>()
    const { attempt, states } = setup(
      { register: [transient(false), registered.promise], renew: [] },
      { kind: 'inbox', member: 'm' },
    )
    await vi.advanceTimersByTimeAsync(1_000)
    expect(attempt.state()).not.toBe('ready')
    registered.resolve('registered')
    await vi.advanceTimersByTimeAsync(0)
    expect(states).toEqual(['ready'])
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

  // The room's authority hands a stored commit on again after a restart, where it may have been handed on before.
  it('that is handed one frame twice delivers it once', async () => {
    const { delivered, handOn } = setup({ register: ['registered'], renew: [] }, { kind: 'control' })
    await vi.advanceTimersByTimeAsync(0)
    handOn(1)
    handOn(2)
    handOn(1)
    handOn(2)
    handOn(3)
    expect(delivered).toEqual([1, 2, 3])
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

describe('a call of a Cloudflare Room lane to its room', () => {
  it("that a lane makes right after it subscribed again, past the room's drop of this session's grant, comes after the lane's registration", async () => {
    // The room's authority: it holds one grant of this session's at a time, and drops it as its alarm does.
    let current: number | null = null
    let granted = 0
    /** What the room's authority got, in the order it got it. */
    const got: string[] = []
    const calls: Record<string, (args: unknown[]) => Promise<unknown>> = {
      grantHolder: async () => (got.push('grant'), (current = ++granted)),
      registerRoute: async ([, order]) => {
        const held = isOrderOf(order, current)
        got.push(held ? 'register' : 'register refused: regrant')
        return held ? 'registered' : 'regrant'
      },
      unsubscribeRoute: async ([, order]) => (isOrderOf(order, current) ? 'removed' : 'regrant'),
      commitLane: async () => (got.push('commit'), { kind: 'accepted', seq: 1, timestamp: 1 }),
    }
    const namespace: TelefuncExtensionNamespace = {
      idFromName: (name) => name,
      idFromString: (id) => id,
      get: () => ({
        telefuncDurableObjectCall: (_extension, method, args) => calls[method]?.(args) ?? Promise.resolve(),
      }),
    }
    const member: CloudflareMember = { bucket: 'weur', hostKey: 'host', subscribeLocal: () => () => {} }
    const driver = new CloudflareRoomDriver({ getNamespace: () => namespace, locationFallback: 'weur' })
    const subscriptions = new RoomSessionSubscriptions({ sessionId: 'session', member, driver })
    const subscribe = () =>
      subscriptions.subscribe({ roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } }, { deliver() {} })
    const first = subscribe()
    await vi.advanceTimersByTimeAsync(0)
    await first.unsubscribe()
    // Past its last write by more than the time the room keeps an idle holder, the room drops this session's grant.
    await vi.advanceTimersByTimeAsync(100_000)
    current = null
    got.length = 0
    subscribe()
    void subscriptions.callAuthority('room', (authority) =>
      authority.commitLane('room', 'inc', { kind: 'semantic' }, new Uint8Array()),
    )
    await vi.advanceTimersByTimeAsync(1_000)
    expect(got.filter((call) => call === 'register' || call === 'commit')).toEqual(['register', 'commit'])
  })

  const join: RoomOperationRequest = {
    name: 'join',
    args: {
      lease: { id: 'server', created: false },
      hold: null,
      id: 'm',
      record: { joinedAt: 0, metaSeq: 0, metaGen: 0 },
      meta: {},
      join: { __r: 'join', id: 'm', meta: {}, joinedAt: 0 },
    },
  }
  const joined: OperationWire = { kind: 'done', value: 'joined', counters: new Map(), registered: true }

  const inbox: LaneId = { kind: 'inbox', member: 'm' }
  const refused = () => new Error('Test: the room refused the join')

  // A member's join carries its inbox's route: the join applies with the inbox's lease, or not at all.
  it("that is a member's join is the one write of the member's inbox, which is then ready and renews", async () => {
    const { attempt, calls, carried, subscriptions, states, attemptOn } = setup(
      { register: [], renew: ['renewed', 'renewed'], carry: [joined] },
      inbox,
      'operation',
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(await subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })).toBe(joined)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(states).toEqual(['ready'])
    expect({ ...calls, carried }).toEqual({ register: 0, renew: 2, carried: [join] })
    expect(attempt.state()).toBe('ready')
  })

  // A refusal the inbox's own registration would have got, which closed the inbox its join had registered.
  for (const [label, refusal] of [
    ['unmarked', () => new Error('Test: the authority refused the inbox')],
    ['overloaded', () => transient(true)],
  ] as const) {
    it(`that is a member's join keeps the inbox it registered where the room would refuse the inbox's own registration (${label})`, async () => {
      const answer = Promise.withResolvers<OperationWire>()
      const { attempt, calls, removed, reasons, subscriptions, attemptOn } = setup(
        { register: [refusal()], renew: ['renewed'], carry: [answer.promise] },
        inbox,
        'operation',
      )
      const carrying = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })
      await vi.advanceTimersByTimeAsync(0)
      answer.resolve(joined)
      expect(await carrying).toBe(joined)
      await vi.advanceTimersByTimeAsync(20_000)
      expect({ state: attempt.state(), register: calls.register, removed, reasons }).toEqual({
        state: 'ready',
        register: 0,
        removed: [],
        reasons: [],
      })
    })
  }

  // The join's rejection is the refusal's one report: the inbox closes with no reason, so core reports nothing of it.
  it("that is a member's join the room refused reports the refusal once, to the join, and leaves no route, timer or attempt once the member's admission unsubscribes the inbox", async () => {
    const logged = vi.spyOn(console, 'error')
    const { attempt, calls, removed, states, reasons, subscriptions, attemptOn } = setup(
      { register: [], renew: [], carry: [refused()] },
      inbox,
      'operation',
    )
    await expect(subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })).rejects.toThrow(
      'Test: the room refused the join',
    )
    await attempt.unsubscribe()
    await vi.advanceTimersByTimeAsync(60_000)
    expect({ states, reasons, logged: logged.mock.calls.length }).toEqual({
      states: ['closed'],
      reasons: [],
      logged: 0,
    })
    expect({ register: calls.register, removed: removed.length, timers: vi.getTimerCount() }).toEqual({
      register: 0,
      removed: 1,
      timers: 0,
    })
    expect(subscriptions.attemptOf('room', 'inc', inbox)).toBeUndefined()
  })

  it("that is a member's join never sent, as it was given up while it waited, leaves nothing at the room once the member's admission unsubscribes the inbox", async () => {
    const grant = Promise.withResolvers<number>()
    const { attempt, calls, carried, removed, subscriptions, attemptOn } = setup(
      { register: [], renew: [], carry: [joined], grant: grant.promise },
      inbox,
      'operation',
    )
    let wanted = true
    const sent = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000, wanted: () => wanted })
    wanted = false
    grant.resolve(1)
    await expect(sent).rejects.toThrow('not sent: unwanted')
    await attempt.unsubscribe()
    await vi.advanceTimersByTimeAsync(60_000)
    expect({ register: calls.register, carried, removed, timers: vi.getTimerCount() }).toEqual({
      register: 0,
      carried: [],
      removed: [],
      timers: 0,
    })
  })

  it("that is a member's join whose inbox was unsubscribed while the join waited in its line isn't sent, and leaves nothing at the room", async () => {
    const grant = Promise.withResolvers<number>()
    const { attempt, carried, removed, subscriptions, attemptOn } = setup(
      { register: [], renew: [], carry: [joined], grant: grant.promise },
      inbox,
      'operation',
    )
    const sent = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })
    await attempt.unsubscribe()
    grant.resolve(1)
    await expect(sent).rejects.toThrow('not sent: unwanted')
    await vi.advanceTimersByTimeAsync(60_000)
    expect({ carried, removed, timers: vi.getTimerCount() }).toEqual({ carried: [], removed: [], timers: 0 })
  })

  it("that is a member's join unsubscribed while the join is sent removes the inbox's route behind the join", async () => {
    const answer = Promise.withResolvers<OperationWire>()
    const { attempt, carried, removed, subscriptions, attemptOn } = setup(
      { register: [], renew: [], carry: [answer.promise] },
      inbox,
      'operation',
    )
    const carrying = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })
    await vi.advanceTimersByTimeAsync(0)
    expect(carried).toEqual([join])
    const unsubscribed = attempt.unsubscribe()
    answer.resolve(joined)
    await carrying
    await unsubscribed
    await vi.advanceTimersByTimeAsync(60_000)
    expect({ state: attempt.state(), removed: removed.length, timers: vi.getTimerCount() }).toEqual({
      state: 'closed',
      removed: 1,
      timers: 0,
    })
  })

  // Cloudflare marks a call whose answer was lost `.retryable`: the room may have applied the join, which its other
  // participants then saw.
  it("that is a member's join whose answer was lost is sent again under the same order, and answered as the room answered the join", async () => {
    const { attempt, carriedOrders, states, subscriptions, attemptOn } = setup(
      { register: [], renew: ['renewed'], carry: [transient(false, 'answer lost'), joined] },
      inbox,
      'operation',
    )
    const carrying = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })
    await vi.advanceTimersByTimeAsync(1_000)
    expect(await carrying).toBe(joined)
    expect({ carriedOrders, states, state: attempt.state() }).toEqual({
      carriedOrders: [
        { grant: 1, seq: 1 },
        { grant: 1, seq: 1 },
      ],
      states: ['ready'],
      state: 'ready',
    })
  })

  it("that is a member's join whose answer was lost each time it was sent fails with the last loss, sent 3 times", async () => {
    const last = transient(false, 'third')
    const { carried, subscriptions, attemptOn } = setup(
      { register: [], renew: [], carry: [transient(false, 'first'), transient(false, 'second'), last] },
      inbox,
      'operation',
    )
    const carrying = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })
    carrying.catch(() => {})
    await vi.advanceTimersByTimeAsync(1_000)
    await expect(carrying).rejects.toBe(last)
    expect(carried).toHaveLength(3)
  })

  it("that is a member's join whose answer was lost, as one Cloudflare says not to retry, fails with it, sent once", async () => {
    const failed = new Error('reset')
    const { carried, subscriptions, attemptOn } = setup(
      { register: [], renew: [], carry: [failed, joined] },
      inbox,
      'operation',
    )
    await expect(subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })).rejects.toBe(failed)
    expect(carried).toHaveLength(1)
  })

  it("that is a member's join that wrote nothing, its hosting lease lost, leaves the inbox waiting, and the join sent again registers it", async () => {
    const wroteNothing: OperationWire = { kind: 'done', value: 'lease-lost', counters: new Map() }
    const { attempt, calls, carried, states, subscriptions, attemptOn } = setup(
      { register: [], renew: ['renewed'], carry: [wroteNothing, joined] },
      inbox,
      'operation',
    )
    expect(await subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })).toBe(wroteNothing)
    await vi.advanceTimersByTimeAsync(20_000)
    expect({ state: attempt.state(), register: calls.register, timers: vi.getTimerCount() }).toEqual({
      state: 'establishing',
      register: 0,
      timers: 0,
    })
    expect(await subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })).toBe(joined)
    expect({ states, register: calls.register, carried: carried.length }).toEqual({
      states: ['ready'],
      register: 0,
      carried: 2,
    })
  })

  // The room's authority issues a grant, which takes a round trip, before the first write it orders.
  it("that is a member's join, sent beside its room's other lanes before this session has a grant there, waits for the one grant they asked for", async () => {
    const grant = Promise.withResolvers<number>()
    const { grants, carried, calls, subscribe, subscriptions, attemptOn } = setup(
      { register: ['registered'], renew: [], carry: [joined], grant: grant.promise },
      inbox,
      'operation',
    )
    subscribe({ kind: 'control' })
    const carrying = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })
    await vi.advanceTimersByTimeAsync(100)
    grant.resolve(1)
    expect(await carrying).toBe(joined)
    expect({ grants: grants.asked, register: calls.register, carried: carried.length }).toEqual({
      grants: 1,
      register: 1,
      carried: 1,
    })
  })

  it('made beside another, more than 60 s after the last write the room counted, waits with it for one new grant', async () => {
    const { attempt, grants, calls, subscribe } = setup({
      register: ['registered', 'registered', 'registered'],
      renew: [],
    })
    await vi.advanceTimersByTimeAsync(0)
    await attempt.unsubscribe()
    await vi.advanceTimersByTimeAsync(61_000)
    subscribe({ kind: 'control' })
    subscribe({ kind: 'inbox', member: 'm' })
    await vi.advanceTimersByTimeAsync(0)
    expect({ grants: grants.asked, register: calls.register }).toEqual({ grants: 2, register: 3 })
  })

  it("that is a member's join, waiting for a grant another lane asked for whose answer was lost, asks for one of its own and is sent", async () => {
    const { carried, grants, subscribe, subscriptions, attemptOn } = setup(
      { register: ['registered'], renew: [], carry: [joined], grant: [transient(false, 'grant lost'), 1] },
      inbox,
      'operation',
    )
    subscribe({ kind: 'control' })
    const carrying = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })
    await vi.advanceTimersByTimeAsync(1_000)
    expect(await carrying).toBe(joined)
    expect({ carried: carried.length, grants: grants.asked }).toEqual({ carried: 1, grants: 2 })
  })

  it("that is a member's join, waiting for a grant another lane asked for which that lane's earlier deadline gave up, asks for one of its own within its own deadline", async () => {
    const { carried, grants, subscribe, subscriptions, attemptOn } = setup(
      { register: [], renew: [], carry: [joined], grant: [new Promise<never>(() => {}), 1] },
      inbox,
      'operation',
    )
    subscribe({ kind: 'control' })
    await vi.advanceTimersByTimeAsync(5_000)
    const carrying = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })
    await vi.advanceTimersByTimeAsync(6_000)
    expect(await carrying).toBe(joined)
    expect({ carried: carried.length, grants: grants.asked }).toEqual({ carried: 1, grants: 2 })
  })

  it('whose renewals the room answered keeps its grant there for a write made more than 60 s after its first', async () => {
    const { grants, subscribe } = setup({ register: ['registered', 'registered'], renew: ['renewed', 'renewed'] })
    await vi.advanceTimersByTimeAsync(2 * ROUTE_RENEW_EVERY_MS + 5_000)
    subscribe({ kind: 'control' })
    await vi.advanceTimersByTimeAsync(0)
    expect(grants.asked).toBe(1)
  })

  it('that waited in its line behind a failed call is not sent once its lane was unsubscribed meanwhile', async () => {
    const held = Promise.withResolvers<RegisterWire>()
    // The first lane's registration fails while the second's is on the same stub; the first's next try waits for it.
    const { attempt, calls, subscribe } = setup({ register: [transient(false), held.promise, 'registered'], renew: [] })
    subscribe({ kind: 'control' })
    await vi.advanceTimersByTimeAsync(150)
    expect(calls.register).toBe(2)
    // Its removal goes behind the second lane's registration too.
    void attempt.unsubscribe()
    held.resolve('registered')
    await vi.advanceTimersByTimeAsync(1_000)
    expect(calls.register).toBe(2)
  })

  it('that waited in its line behind a failed call another lane waits on forever settles within its own time', async () => {
    // The first lane's registration fails while the second's is on the same stub and never answered.
    const failed = transient(false)
    const { attempt, reasons, subscribe } = setup({
      register: [failed, new Promise<never>(() => {}), 'registered'],
      renew: [],
    })
    subscribe({ kind: 'control' })
    await vi.advanceTimersByTimeAsync(10_000)
    expect(attempt.state()).toBe('closed')
    expect(reasons).toEqual([['closed', failed]])
  })

  it('that the room never answers closes a first registration within 10 s of its first try', async () => {
    const { attempt, reasons } = setup({ register: [new Promise<never>(() => {})], renew: [] })
    await vi.advanceTimersByTimeAsync(10_000)
    expect(attempt.state()).toBe('closed')
    expect(reasons).toHaveLength(1)
  })

  it('that the room answered overloaded is followed by no other call to the room at once, a first registration neither', async () => {
    const { calls, subscribe } = setup({
      register: ['registered', 'registered', 'registered'],
      renew: [transient(true)],
    })
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS)
    expect(calls).toEqual({ register: 1, renew: 1 })
    subscribe({ kind: 'control' })
    await vi.advanceTimersByTimeAsync(500)
    expect(calls).toEqual({ register: 1, renew: 1 })
  })

  it('whose renewal falls in a pause another call of the session caused stays ready, is tried no more while the pause lasts, and renews once it ends', async () => {
    const tried = vi.spyOn(OrderedStubs.prototype, 'call')
    const { attempt, calls, states, subscribe } = setup(
      { register: ['registered', transient(true)], renew: ['renewed'] },
      { kind: 'control' },
    )
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS - 1_000)
    // Another lane's first registration is answered overloaded, 1 s before this lane's renewal: the room is paused 10 s.
    subscribe({ kind: 'semantic' })
    await vi.advanceTimersByTimeAsync(0)
    const before = tried.mock.calls.length
    await vi.advanceTimersByTimeAsync(9_500)
    expect({ states, renew: calls.renew, tries: tried.mock.calls.length - before }).toEqual({
      states: ['ready'],
      renew: 0,
      tries: 1,
    })
    await vi.advanceTimersByTimeAsync(2_500)
    expect({ states, renew: calls.renew, state: attempt.state() }).toEqual({
      states: ['ready'],
      renew: 1,
      state: 'ready',
    })
  })

  it('whose renewals the pause keeps from being sent for longer than its 90 s lease is lost, and registers again once the room takes calls', async () => {
    const { calls, states, commit } = setup(
      { register: ['registered', 'registered'], renew: [], commit: Array.from({ length: 9 }, () => transient(true)) },
      { kind: 'control' },
    )
    await vi.advanceTimersByTimeAsync(0)
    // A commit answered overloaded every 9 s keeps the room paused from 25 s to 107 s.
    await vi.advanceTimersByTimeAsync(25_000)
    for (let at = 25_000; at < 100_000; at += 9_000) {
      void commit()
      await vi.advanceTimersByTimeAsync(9_000)
    }
    expect({ states, calls }).toEqual({ states: ['ready', 'lost'], calls: { register: 1, renew: 0 } })
    await vi.advanceTimersByTimeAsync(10_000)
    expect({ states, calls }).toEqual({ states: ['ready', 'lost', 'ready'], calls: { register: 2, renew: 0 } })
  })

  it('whose first registration was answered 8 s after it went out is lost once 90 s have passed since it went out, while the pause keeps its renewals from being sent', async () => {
    const answered = new Promise<RegisterWire>((resolve) => setTimeout(() => resolve('registered'), 8_000))
    const { calls, states, commit } = setup(
      { register: [answered, 'registered'], renew: [], commit: Array.from({ length: 9 }, () => transient(true)) },
      { kind: 'control' },
    )
    await vi.advanceTimersByTimeAsync(0)
    // A commit answered overloaded every 9 s keeps the room paused from 21 s to 103 s, and the renewal due at 38 s is
    // tried at each pause's end: 40 s, 49 s, and so on to 94 s.
    await vi.advanceTimersByTimeAsync(21_000)
    for (let at = 21_000; at < 95_000; at += 9_000) {
      void commit()
      await vi.advanceTimersByTimeAsync(Math.min(9_000, 95_000 - at))
    }
    expect({ states, calls }).toEqual({ states: ['ready', 'lost'], calls: { register: 1, renew: 0 } })
  })

  it('whose renewal waits for a pause sends none once it is unsubscribed, and leaves no timer once the pause is over', async () => {
    const { attempt, calls, subscribe } = setup(
      { register: ['registered', transient(true)], renew: ['renewed'] },
      { kind: 'control' },
    )
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(ROUTE_RENEW_EVERY_MS - 1_000)
    subscribe({ kind: 'semantic' })
    await vi.advanceTimersByTimeAsync(2_000)
    await attempt.unsubscribe()
    // The routes' removals wait for the pause too.
    await vi.advanceTimersByTimeAsync(60_000)
    expect({ renew: calls.renew, timers: vi.getTimerCount() }).toEqual({ renew: 0, timers: 0 })
  })

  it('whose first registration the room refused as overloaded is followed by no other call to it at once', async () => {
    const { attempt, calls, subscribe } = setup({ register: [transient(true), 'registered'], renew: [] })
    await vi.advanceTimersByTimeAsync(0)
    expect(attempt.state()).toBe('closed')
    subscribe({ kind: 'control' })
    await vi.advanceTimersByTimeAsync(500)
    expect(calls.register).toBe(1)
  })
})
