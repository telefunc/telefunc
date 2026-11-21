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
 *  to it throws the next of `answers.open`, while one is left. */
function setup(
  answers: {
    register: Array<RegisterWire | Error | Promise<RegisterWire>>
    renew: Array<RenewWire | Error>
    open?: Array<Error | undefined>
    /** The answers to operations that carry an inbox's route. */
    carry?: Array<OperationWire | Error | Promise<OperationWire>>
    /** The grant's answer, where it isn't at once. */
    grant?: Promise<number>
    /** The answers to the session's commits. */
    commit?: Array<Error>
  },
  lane: LaneId = { kind: 'semantic' },
) {
  const calls = { register: 0, renew: 0 }
  /** The operations that carried an inbox's route, as the authority got them. */
  const carried: unknown[] = []
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
    grantHolder: () => answers.grant ?? Promise.resolve(1),
    runOperationWithInbox: (args) => (carried.push(args[1]), answer(answers.carry ?? [])),
    registerRoute: () => (calls.register++, registeredAt.push(Date.now() - startedAt), answer(answers.register)),
    renewRoutes: () => (calls.renew++, answer(answers.renew).then((renewal) => [renewal])),
    unsubscribeRoute: () => Promise.resolve('removed'),
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
  const attempt = subscriptions.subscribe({ roomId: 'room', inc: 'inc', lane }, receiver)
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
    carried,
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

  // A member's join carries its inbox's route: the join applies with the inbox's lease, or not at all.
  it("that carries an inbox's route makes the inbox ready, whatever the inbox's own registration answers after", async () => {
    const own = Promise.withResolvers<RegisterWire>()
    const { attempt, calls, carried, subscriptions, states, attemptOn } = setup(
      { register: [own.promise], renew: ['renewed', 'renewed'], carry: [joined] },
      { kind: 'inbox', member: 'm' },
    )
    expect(await subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000 })).toBe(joined)
    expect(attempt.state()).toBe('ready')
    own.resolve('stale')
    await vi.advanceTimersByTimeAsync(60_000)
    expect(states).toEqual(['ready'])
    expect({ ...calls, carried }).toEqual({ register: 1, renew: 2, carried: [join] })
  })

  it("that carries an inbox's route is not sent once the join it makes was given up while it waited", async () => {
    const grant = Promise.withResolvers<number>()
    const { carried, subscriptions, attemptOn } = setup(
      { register: ['registered'], renew: [], carry: [joined], grant: grant.promise },
      { kind: 'inbox', member: 'm' },
    )
    let wanted = true
    const sent = subscriptions.runCarrying(attemptOn(), join, { deadline: Date.now() + 10_000, wanted: () => wanted })
    wanted = false
    grant.resolve(1)
    await expect(sent).rejects.toThrow('not sent: unwanted')
    expect(carried).toEqual([])
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
