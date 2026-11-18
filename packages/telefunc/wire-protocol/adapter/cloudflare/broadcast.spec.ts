import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { AsyncLocalStorage } from 'node:async_hooks'
import {
  DEFAULT_BROADCAST_BUCKETS,
  assertLocationFallbackIsScaled,
  getBucketCoordinatorScale,
  getDeterministicKeyBucketIndex,
  resolveCloudflareLocationHint,
  resolveSessionRoutingTarget,
} from './routing.js'
import { CloudflareBroadcastAuthorityState, CloudflareBroadcast } from './broadcast.js'
import type { BroadcastCalls, BroadcastPresenceRequest } from './broadcast.js'
import { OrderedStubs } from './OrderedStubs.js'
import type { TelefuncDurableObjectNamespace } from './namespace.js'
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { sqliteStorage } from '../../test/cloudflareSql.js'
import { CloudflareBackend } from './CloudflareBackend.js'
import { servesRequest, withCloudflareSession, type CloudflareSession } from './session.js'
import { disposeBackend, installBackend } from '../../backend/install.js'
import { broadcastRouteKey } from '../../backend/broadcast/routeKey.js'
import { Broadcast, ServerBroadcast } from '../../channel/server/ServerBroadcast.js'
import { getChannelMux } from '../../channel/server/ChannelMux.js'
import { installAsyncContext } from '../../../node/server/context/async.js'

type CloudflareRequest = Request & { cf?: { colo?: string; continent?: string } }

function createCloudflareRequest({ colo, continent }: { colo?: string; continent?: string } = {}): CloudflareRequest {
  const request = new Request('https://telefunc.test') as CloudflareRequest
  request.cf = { colo, continent }
  return request
}

function createAuthorityState() {
  return new CloudflareBroadcastAuthorityState({ storage: sqliteStorage().storage } as DurableObjectState)
}

async function flushMicrotasks(turns = 6): Promise<void> {
  for (let index = 0; index < turns; index++) {
    await Promise.resolve()
  }
}

function createBasicBinding(
  overrides?: Partial<{
    onPublish: (id: { name: string }, request: any) => any
    onForward: (id: { name: string }, request: any) => any
    onDeliver: (id: { name: string }, request: any) => any
    onPresence: (id: { name: string }, request: BroadcastPresenceRequest) => Promise<boolean>
  }>,
) {
  return {
    idFromName(name: string) {
      return { name }
    },
    idFromString(id: string) {
      return { name: id }
    },
    get(id: { name: string }) {
      return {
        telefuncBroadcastPublish(request: any) {
          return overrides?.onPublish?.(id, request) ?? Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
        telefuncBroadcastForward(request: any) {
          return overrides?.onForward?.(id, request) ?? Promise.resolve(0)
        },
        telefuncBroadcastDeliver(request: any) {
          return overrides?.onDeliver?.(id, request) ?? Promise.resolve()
        },
        telefuncBroadcastPresence(request: BroadcastPresenceRequest) {
          return overrides?.onPresence?.(id, request) ?? Promise.resolve(false)
        },
      }
    },
  } as unknown as TelefuncDurableObjectNamespace
}

function createBroadcast(binding = createBasicBinding()): CloudflareBroadcast {
  return new CloudflareBroadcast({
    baseInstanceName: 'telefunc',
    scale: 1,
    locationFallback: 'weur',
    getNamespace: () => binding,
  })
}

describe('cloudflare broadcast routing', () => {
  it('uses the six default canonical buckets for mapped Cloudflare locations', () => {
    expect(DEFAULT_BROADCAST_BUCKETS).toEqual(['wnam', 'enam', 'weur', 'eeur', 'apac', 'oc'])
  })

  it('stores direct session-placement buckets for colos', () => {
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.LAX).toBe('wnam')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.ORD).toBe('enam')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.LHR).toBe('weur')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.WAW).toBe('eeur')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.BOM).toBe('apac')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.SYD).toBe('oc')
  })

  it('resolves request colos to canonical location hints', () => {
    const losAngeles = createCloudflareRequest({ colo: 'LAX' })
    const chicago = createCloudflareRequest({ colo: 'ORD' })
    const london = createCloudflareRequest({ colo: 'LHR' })
    const warsaw = createCloudflareRequest({ colo: 'WAW' })
    const mumbai = createCloudflareRequest({ colo: 'BOM' })
    const sydney = createCloudflareRequest({ colo: 'SYD' })

    expect(resolveCloudflareLocationHint(losAngeles, 'weur')).toBe('wnam')
    expect(resolveCloudflareLocationHint(chicago, 'weur')).toBe('enam')
    expect(resolveCloudflareLocationHint(london, 'weur')).toBe('weur')
    expect(resolveCloudflareLocationHint(warsaw, 'weur')).toBe('eeur')
    expect(resolveCloudflareLocationHint(mumbai, 'weur')).toBe('apac')
    expect(resolveCloudflareLocationHint(sydney, 'weur')).toBe('oc')
  })

  it('maps unambiguous continents directly to session-placement buckets', () => {
    expect(resolveCloudflareLocationHint(createCloudflareRequest({ continent: 'AF' }), 'weur')).toBe('weur')
    expect(resolveCloudflareLocationHint(createCloudflareRequest({ continent: 'AS' }), 'weur')).toBe('apac')
    expect(resolveCloudflareLocationHint(createCloudflareRequest({ continent: 'OC' }), 'weur')).toBe('oc')
    expect(resolveCloudflareLocationHint(createCloudflareRequest({ continent: 'SA' }), 'weur')).toBe('enam')
  })

  it('prefers a mapped continent bucket when the colo is unmapped', () => {
    const unknown = createCloudflareRequest({ colo: 'ZZZ', continent: 'AF' })

    expect(resolveCloudflareLocationHint(unknown, 'weur')).toBe('weur')
  })

  it('falls back to locationFallback for ambiguous continents', () => {
    const request = createCloudflareRequest({ colo: 'ZZZ', continent: 'EU' })

    expect(resolveCloudflareLocationHint(request, 'weur')).toBe('weur')
    expect(resolveCloudflareLocationHint(request, 'apac')).toBe('apac')
  })

  it('falls back to locationFallback when cf.continent is unavailable', () => {
    const request = createCloudflareRequest({ colo: 'ZZZ' })

    expect(resolveCloudflareLocationHint(request, 'weur')).toBe('weur')
  })

  it('falls back to locationFallback when neither colo nor continent exists', () => {
    expect(resolveCloudflareLocationHint(createCloudflareRequest(), 'weur')).toBe('weur')
  })

  it('maps the same room to the same bucket-coordinator offset for a bucket', () => {
    const shardCount = getBucketCoordinatorScale(2, 'weur')

    expect(getDeterministicKeyBucketIndex('room/alpha', shardCount)).toBe(
      getDeterministicKeyBucketIndex('room/alpha', shardCount),
    )
  })

  it('assigns room keys only within the bucket-coordinator subset', () => {
    for (const bucket of ['weur', 'apac', 'oc'] as const) {
      expect(getDeterministicKeyBucketIndex('room/alpha', getBucketCoordinatorScale(2, bucket))).toBe(0)
    }
  })

  it('partitions bucket coordinators at ceil(sessionScale / 2) per bucket', () => {
    expect(getBucketCoordinatorScale(1, 'wnam')).toBe(1)
    expect(getBucketCoordinatorScale(2, 'wnam')).toBe(1)
    expect(getBucketCoordinatorScale(3, 'wnam')).toBe(2)
    expect(getBucketCoordinatorScale(4, 'wnam')).toBe(2)
    expect(getBucketCoordinatorScale(4, 'enam')).toBe(2)
  })

  it('uses only canonical bucket scale entries', () => {
    expect(getBucketCoordinatorScale({ weur: 2, apac: 1 }, 'apac')).toBe(1)
  })

  it('resolves session targets from request location and scale', () => {
    const exactRequest = createCloudflareRequest({ colo: 'LHR' })
    const unknownRequest = createCloudflareRequest({ continent: 'EU' })
    const exactTarget = resolveSessionRoutingTarget(
      'telefunc',
      { weur: 2, apac: 1 },
      exactRequest,
      'weur',
      'token',
      null,
    )
    const fallbackTarget = resolveSessionRoutingTarget(
      'telefunc',
      { weur: 1, apac: 1 },
      unknownRequest,
      'weur',
      'token',
      null,
    )

    expect(exactTarget).toMatchObject({
      sessionInstanceName: expect.stringMatching(/^telefunc-shard-weur-/),
      locationBucket: 'weur',
    })
    expect(fallbackTarget).toMatchObject({
      sessionInstanceName: 'telefunc-shard-weur-0',
      locationBucket: 'weur',
    })
  })

  it('routes a recognized region missing from the scale map to locationFallback instead of throwing', () => {
    // `ABQ` resolves to `wnam`, which is absent from this per-region scale map.
    const wnamRequest = createCloudflareRequest({ colo: 'ABQ' })
    const target = resolveSessionRoutingTarget('telefunc', { weur: 2, apac: 1 }, wnamRequest, 'weur', 'token', null)

    expect(target).toMatchObject({
      sessionInstanceName: expect.stringMatching(/^telefunc-shard-weur-[01]$/),
      locationBucket: 'weur',
    })
  })

  it('rejects a per-region scale map whose locationFallback region has no shards', () => {
    // `locationFallback` is where unlisted regions land, so it must itself be scaled.
    expect(() => assertLocationFallbackIsScaled({ enam: 3, apac: 2 }, 'weur')).toThrow(/locationFallback/)
    expect(() => assertLocationFallbackIsScaled({ weur: 2, apac: 1 }, 'weur')).not.toThrow()
    // A uniform numeric scale (or the default) applies to every region, so any fallback is fine.
    expect(() => assertLocationFallbackIsScaled(4, 'weur')).not.toThrow()
    expect(() => assertLocationFallbackIsScaled(undefined, 'weur')).not.toThrow()
  })

  it('authority forwards once to each populated bucket coordinator', async () => {
    const authorityState = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinators: string[] = []
    const broadcast: CloudflareBroadcast = createBroadcast(
      createBasicBinding({
        onForward(id) {
          coordinators.push(id.name)
          return Promise.resolve(0)
        },
      }),
    )
    for (const bucket of ['weur', 'apac', 'eeur'] as const) {
      authorityState.setPresence({ key: 'room:test', kind: 'text', member: `telefunc-shard-${bucket}-0`, bucket })
    }
    await broadcast.publishToSubscribers(authorityState, calls, {
      key: 'room:test',
      kind: 'text',
      payload: '{"text":"hello"}',
    })
    expect(coordinators.sort()).toEqual([
      'telefunc:broadcast:apac:0',
      'telefunc:broadcast:eeur:0',
      'telefunc:broadcast:weur:0',
    ])
  })

  it('a member that unsubscribes withdraws its presence, so the next publish forwards to no coordinator', async () => {
    const authorityState = createAuthorityState()
    const coordinators: string[] = []
    const broadcast = createBroadcast(
      createBasicBinding({
        onForward(id) {
          coordinators.push(id.name)
          return Promise.resolve(0)
        },
        // The key's authority, as a member's presence reaches it.
        onPresence: (_id, request) => Promise.resolve(authorityState.setPresence(request)),
      }),
    )
    const member = broadcast.member('telefunc-shard-weur-0', new OrderedStubs())
    member.locate('weur')
    const attempt = member.openSubscription({ key: 'room:test', kind: 'text' }, { deliver() {} })
    await flushMicrotasks()
    const publish = (payload: string) =>
      broadcast.publishToSubscribers(authorityState, new OrderedStubs(), { key: 'room:test', kind: 'text', payload })

    await publish('"first"')
    expect(coordinators).toEqual(['telefunc:broadcast:weur:0'])

    await attempt.unsubscribe()
    await publish('"second"')
    expect(coordinators).toEqual(['telefunc:broadcast:weur:0'])
  })

  it('forwarded publish delivers to DO names listed in the request', async () => {
    const deliveredTo: string[] = []
    const broadcast = createBroadcast(
      createBasicBinding({
        onDeliver(id) {
          deliveredTo.push(id.name)
          return Promise.resolve()
        },
      }),
    )

    await broadcast.forwardToBucket(new OrderedStubs(), {
      routes: [{ key: 'room:test', kind: 'text' }],
      payload: '{"text":"hello"}',
      info: { seq: 1, timestamp: Date.now() },
      members: ['telefunc-shard-weur-0', 'telefunc-shard-weur-1'],
    })

    expect(deliveredTo.sort()).toEqual(['telefunc-shard-weur-0', 'telefunc-shard-weur-1'])
  })

  it('serializes authority dispatch without blocking later publishes on remote delivery completion', async () => {
    const authorityState = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorPublishes: string[] = []
    const firstRemotePublish = Promise.withResolvers<number>()
    const broadcast = createBroadcast(
      createBasicBinding({
        onForward(id, { payload: text }) {
          coordinatorPublishes.push(`${id.name}:${text}`)
          if (id.name.includes(':broadcast:apac:') && text === '{"text":"first"}') return firstRemotePublish.promise
          return Promise.resolve(0)
        },
      }),
    )
    for (const bucket of ['weur', 'apac'] as const) {
      authorityState.setPresence({ key: 'room:test', kind: 'text', member: `telefunc-shard-${bucket}-0`, bucket })
    }
    const firstPublish = broadcast.publishToSubscribers(authorityState, calls, {
      key: 'room:test',
      kind: 'text',
      payload: '{"text":"first"}',
    })
    await flushMicrotasks(8)
    const secondPublish = broadcast.publishToSubscribers(authorityState, calls, {
      key: 'room:test',
      kind: 'text',
      payload: '{"text":"second"}',
    })
    await flushMicrotasks(8)

    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"first"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"first"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"second"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"second"}')

    firstRemotePublish.resolve(0)
    await Promise.all([firstPublish, secondPublish])

    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"second"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"second"}')
  })
})

// What a subscriber's `ready` says, on a Worker without AsyncLocalStorage: the REAL ServerBroadcast on the REAL
// CloudflareBackend and CloudflareBroadcast, each session's member registering its presence with the key's authority,
// whose RPC this test answers from a real authority state, refusing or holding it where a case asks.
describe("a subscriber's `ready` on Cloudflare", () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
    return disposeBackend()
  })

  const REFUSAL = 'presence refused: 429 Too Many Requests'

  /** A Worker whose sessions subscribe through its CloudflareBackend. The key's authority stores what reaches it unless
   *  `refuse` or `fail` is set, and a call then rejects with REFUSAL or with `fail`; `held`, where set when a presence
   *  call is made, holds that call's answer until it settles; `throwAtOnce` makes the call throw before it returns. */
  function worker() {
    const authority = createAuthorityState()
    const state: { refuse: boolean; fail?: Error; held?: Promise<void>; throwAtOnce?: Error } = { refuse: false }
    const presences: BroadcastPresenceRequest[] = []
    const binding = createBasicBinding({
      onPresence(_id, request) {
        if (state.throwAtOnce) throw state.throwAtOnce
        const { refuse, fail, held } = state
        presences.push(request)
        // The authority applies the calls one stub carries in the order they come, whenever its answer reaches the member.
        const lapsed = refuse || fail ? false : authority.setPresence(request)
        return (async () => {
          await held
          if (refuse) throw new Error(REFUSAL)
          if (fail) throw fail
          return lapsed
        })()
      },
      onPublish: (_id, request) => broadcast.publishToSubscribers(authority, new OrderedStubs(), request),
      onForward: (_id, request) => broadcast.forwardToBucket(new OrderedStubs(), request),
      // Each member Durable Object takes a delivery in its own session, as its `telefuncBroadcastDeliver` does.
      onDeliver(id, request) {
        const session = byName.get(id.name)
        if (session === undefined) return Promise.resolve()
        delivering.in = id.name
        try {
          withCloudflareSession(session, () => session.broadcast.deliver(request))
        } finally {
          delivering.in = null
        }
        return Promise.resolve()
      },
    })
    const byName = new Map<string, CloudflareSession>()
    /** The member Durable Object a delivery runs in now. */
    const delivering: { in: string | null } = { in: null }
    const broadcast = createBroadcast(binding)
    installBackend(
      () => new CloudflareBackend({ planeKit: { getNamespace: () => binding, locationFallback: 'weur' }, broadcast }),
      [crypto.randomUUID()],
    )
    const session = (name: string): CloudflareSession => {
      const member = broadcast.member(name, new OrderedStubs())
      member.locate('weur')
      const session = { broadcast: member, mux: getChannelMux() }
      byName.set(name, session)
      return session
    }
    const sessions = { first: session('telefunc-shard-weur-0'), second: session('telefunc-shard-weur-1') }
    return {
      state,
      presences,
      /** The members the authority holds a live presence of for the key's text route. */
      present: (key: string) => authority.livePresence(broadcastRouteKey({ key, kind: 'text' }), Date.now()),
      inObject: <T>(run: () => T, session = sessions.first) => withCloudflareSession(session, run),
      sessions,
      session,
      delivering,
    }
  }

  /** What `promise` settles with: 'resolved', or the message of the error it rejects with, else of that error's cause. */
  function outcome(promise: Promise<unknown>): Promise<string> {
    return promise.then(
      () => 'resolved',
      (err: unknown) => (err instanceof Error && err.cause instanceof Error ? err.cause.message : String(err)),
    )
  }

  // A page subscribes and calls at once, so its call brings the subscription before its wire attaches; the call runs in
  // the session's Durable Object, which knows the request without AsyncLocalStorage.
  describe("a page's subscription its call brings, on a Worker without AsyncLocalStorage", () => {
    /** A broadcast a call of the first session returned, its want brought by that session's next call. */
    const brought = (w: ReturnType<typeof worker>) => {
      const returning = new Request('https://telefunc.test/_telefunc?session=page', { method: 'POST' })
      const call = new Request('https://telefunc.test/_telefunc?session=page', { method: 'POST' })
      // Both reached the session's Durable Object, which `fetch` records.
      servesRequest(w.sessions.first, returning)
      servesRequest(w.sessions.first, call)
      const broadcast = new ServerBroadcast<string>({ key: 'room:test' })
      // As `carryChannel` stamps the broadcast a call returns.
      broadcast._end._callRequest = returning
      getChannelMux().registerChannel(broadcast._end)
      const inEffect = getChannelMux().applyCallWants(call, [
        { channel: broadcast.id, version: 1, text: true, binary: false },
      ])
      return { broadcast, inEffect }
    }

    it("a page's subscription its call brings before its first attach, on a Worker without AsyncLocalStorage, is the Durable Object's that the call reached", async () => {
      const w = worker()
      const { broadcast, inEffect } = brought(w)
      await inEffect
      expect({ closed: broadcast.isClosed, members: w.presences.map(({ member }) => member) }).toEqual({
        closed: false,
        members: ['telefunc-shard-weur-0'],
      })
      broadcast.abort()
    })

    it("holds its call until the key's authority answers, and the call runs after it", async () => {
      const w = worker()
      const stored = Promise.withResolvers<void>()
      w.state.held = stored.promise
      const { broadcast, inEffect } = brought(w)
      let ran = false
      void Promise.resolve(inEffect).then(() => (ran = true))
      await new Promise((resolve) => setTimeout(resolve, 0))
      expect(ran).toBe(false)
      stored.resolve()
      await vi.waitFor(() => expect(ran).toBe(true))
      broadcast.abort()
    })

    it("ends the page's broadcast, its subscription not in effect, where the key's authority refuses it, and the call goes on", async () => {
      const w = worker()
      w.state.refuse = true
      const { broadcast, inEffect } = brought(w)
      await inEffect
      expect(await outcome(broadcast.closed)).toBe(REFUSAL)
    })
  })

  it('a subscriber with no page, on a Worker without AsyncLocalStorage, throws an error that names the flag', () => {
    const { inObject } = worker()
    expect(() => inObject(() => Broadcast.subscribe('room:test', () => {}))).toThrow(
      // spellcheck-ignore  nodejs_als is Cloudflare's AsyncLocalStorage compatibility flag
      'add "nodejs_als" to the compatibility_flags',
    )
  })

  // Last in its file: AsyncLocalStorage, once installed, stays for the file's tests that come after it. Server code that
  // subscribes needs it.
  describe('on a Worker with AsyncLocalStorage', () => {
    beforeAll(() => installAsyncContext(AsyncLocalStorage))

    // Its publishes reach this Durable Object only where another one's publish finds its presence at the key's authority.
    it("a subscriber's `ready` resolves once the key's authority stored its Durable Object's presence", async () => {
      const { state, inObject, present } = worker()
      const stored = Promise.withResolvers<void>()
      state.held = stored.promise
      const remove = inObject(() => Broadcast.subscribe('room:test', () => {}))
      let ready = false
      void remove.ready.then(() => (ready = true))
      await new Promise((resolve) => setTimeout(resolve, 0))
      expect(ready).toBe(false)
      stored.resolve()
      await vi.waitFor(() => expect(ready).toBe(true))
      expect(present('room:test').map(({ member }) => member)).toEqual(['telefunc-shard-weur-0'])
      remove()
    })

    // A presence the authority refuses to store is a subscription that failed, as a refused one is on any other backend.
    describe("where the key's authority refuses the presence write", () => {
      it("a subscriber's `ready` rejects with the authority's error, the subscriber is removed, and a later one of the same Durable Object registers again", async () => {
        const { state, presences, inObject } = worker()
        state.refuse = true
        const first = inObject(() => Broadcast.subscribe('room:test', () => {}))
        expect(await outcome(first.ready)).toBe(REFUSAL)
        await new Promise((resolve) => setTimeout(resolve, 0))
        expect(presences).toHaveLength(1)
        const second = inObject(() => Broadcast.subscribe('room:test', () => {}))
        expect(await outcome(second.ready)).toBe(REFUSAL)
        expect(presences).toHaveLength(2)
        first()
        second()
        state.refuse = false
        const third = inObject(() => Broadcast.subscribe('room:test', () => {}))
        expect(await outcome(third.ready)).toBe('resolved')
        third()
      })

      // Cloudflare marks a transient failure `.retryable`, and one it says not to retry `.overloaded`. Neither appeared on
      // any error local workerd raised, so these errors are made here.
      const transient = (overloaded: boolean) =>
        Object.assign(new Error('Durable Object is overloaded or reset'), { retryable: true, overloaded })

      it("a first presence write that fails as retryable keeps the subscriber's `ready` pending, and is written again at the next refresh", async () => {
        vi.useFakeTimers()
        const { state, presences, present, inObject } = worker()
        state.fail = transient(false)
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        let ready = 'pending'
        void outcome(subscriber.ready).then((settled) => (ready = settled))
        await vi.advanceTimersByTimeAsync(0)
        expect({ ready, written: presences.length }).toEqual({ ready: 'pending', written: 1 })
        state.fail = undefined
        await vi.advanceTimersByTimeAsync(30_000)
        expect({ ready, present: present('room:test').map(({ member }) => member) }).toEqual({
          ready: 'resolved',
          present: ['telefunc-shard-weur-0'],
        })
        subscriber()
      })

      it("a first presence write that fails as overloaded rejects the subscriber's `ready`, and is not written again", async () => {
        vi.useFakeTimers()
        const { state, presences, inObject } = worker()
        state.fail = transient(true)
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        let ready = 'pending'
        void outcome(subscriber.ready).then((settled) => (ready = settled))
        await vi.advanceTimersByTimeAsync(30_000)
        expect({ ready, written: presences.length }).toEqual({
          ready: 'Durable Object is overloaded or reset',
          written: 1,
        })
        subscriber()
      })

      it('a subscriber removed while its retryable first presence write waits for the next refresh leaves no write', async () => {
        vi.useFakeTimers()
        const { state, presences, inObject } = worker()
        state.fail = transient(false)
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await vi.advanceTimersByTimeAsync(0)
        subscriber()
        state.fail = undefined
        await vi.advanceTimersByTimeAsync(60_000)
        expect(presences).toHaveLength(1)
      })

      // A refresh that fails loses the subscription for a while, whatever the failure: the next one that succeeds restores it.
      it('a subscriber whose presence refresh fails hears what another Durable Object publishes once a later refresh succeeds', async () => {
        vi.useFakeTimers()
        vi.spyOn(console, 'error').mockImplementation(() => {})
        const { state, inObject, sessions } = worker()
        const heard: string[] = []
        const subscriber = inObject(() => Broadcast.subscribe<string>('room:test', (text) => heard.push(text)))
        await vi.advanceTimersByTimeAsync(0)
        await subscriber.ready
        state.refuse = true
        await vi.advanceTimersByTimeAsync(30_000)
        state.refuse = false
        await vi.advanceTimersByTimeAsync(30_000)
        await inObject(() => Broadcast.publish('room:test', 'after'), sessions.second)
        await vi.advanceTimersByTimeAsync(0)
        expect(heard).toEqual(['after'])
        subscriber()
      })

      it('a presence write that throws at once fails as a refused one: `ready` rejects with its error, and a publish goes on', async () => {
        const { state, inObject } = worker()
        state.throwAtOnce = new Error('RPC invocation failed')
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        expect(await outcome(subscriber.ready)).toBe('RPC invocation failed')
        const published = await Promise.race([
          Promise.resolve(inObject(() => Broadcast.publish('room:test', 'hello'))).then(() => 'published'),
          new Promise((resolve) => setTimeout(() => resolve('held'), 1_000)),
        ])
        expect(published).toBe('published')
        subscriber()
      })

      // A withdrawal that fails lapses with the presence's time to live; its error is logged, not thrown at the subscriber.
      it('a presence delete that throws at once is warned of as a refused one', async () => {
        const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
        const { state, inObject } = worker()
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await subscriber.ready
        state.throwAtOnce = new Error('RPC invocation failed')
        subscriber()
        await vi.waitFor(() =>
          expect(
            logged.mock.calls.some(([error]) => error instanceof Error && error.message.includes('RPC invocation')),
          ).toBe(true),
        )
      })

      // The authority applies a refresh it answers late before a withdrawal asked of it meanwhile, as one stub carries both.
      for (const comesBack of [false, true]) {
        it(`a refresh the key's authority acknowledges after the Durable Object's last subscriber left${comesBack ? ', and a new one came' : ''} ${comesBack ? 'keeps' : 'leaves no'} presence`, async () => {
          vi.useFakeTimers()
          const { state, inObject, present } = worker()
          const left = inObject(() => Broadcast.subscribe('room:test', () => {}))
          await vi.advanceTimersByTimeAsync(0)
          await left.ready
          const written = present('room:test').map(({ member }) => member)
          const acknowledge = Promise.withResolvers<void>()
          state.held = acknowledge.promise
          await vi.advanceTimersByTimeAsync(30_000)
          state.held = undefined
          left()
          const back = comesBack ? inObject(() => Broadcast.subscribe('room:test', () => {})) : undefined
          acknowledge.resolve()
          await vi.advanceTimersByTimeAsync(0)
          await back?.ready
          expect(present('room:test').map(({ member }) => member)).toEqual(comesBack ? written : [])
          back?.()
        })
      }

      // A Durable Object subscribes again while the withdrawal of its presence is on its way: that of its release, or the
      // one after a refresh the authority answered once it had left. Its one stub to the authority carries the withdrawal
      // ahead of the new registration (OrderedStubs), so the authority applies them in that order.
      for (const withdrawing of ['its release', 'a late refresh'] as const) {
        it(`a Durable Object that subscribes again while the withdrawal after ${withdrawing} is under way keeps its presence`, async () => {
          vi.useFakeTimers()
          const { state, inObject, present, presences } = worker()
          const left = inObject(() => Broadcast.subscribe('room:test', () => {}))
          await vi.advanceTimersByTimeAsync(0)
          await left.ready
          const written = present('room:test').map(({ member }) => member)
          const answer = Promise.withResolvers<void>()
          state.held = answer.promise
          if (withdrawing === 'a late refresh') await vi.advanceTimersByTimeAsync(30_000)
          left()
          await vi.advanceTimersByTimeAsync(0)
          state.held = undefined
          const back = inObject(() => Broadcast.subscribe('room:test', () => {}))
          answer.resolve()
          await vi.advanceTimersByTimeAsync(0)
          await back.ready
          expect({
            asked: presences.map(({ bucket }) => (bucket === null ? 'withdraw' : 'record')).slice(-2),
            present: present('room:test').map(({ member }) => member),
          }).toEqual({ asked: ['withdraw', 'record'], present: written })
          back()
        })
      }

      it("a broadcast that subscribes closes with the authority's error", async () => {
        const { state, inObject } = worker()
        state.refuse = true
        const room = inObject(() => {
          const broadcast = new ServerBroadcast<string>({ key: 'room:test:refused' })
          broadcast.subscribe(() => {})
          return broadcast
        })
        expect(await outcome(room.closed)).toBe(REFUSAL)
      })

      // A removed subscriber's `ready` keeps what its subscription's start came to.
      it("a static subscriber's remover keeps the authority's first refusal in its `ready` once it is removed", async () => {
        const { state, inObject } = worker()
        state.refuse = true
        const remove = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await new Promise((resolve) => setTimeout(resolve, 0))
        remove()
        state.refuse = false
        expect(await outcome(remove.ready)).toBe(REFUSAL)
      })
    })

    it('records what a call subscribes after an `await` under the Durable Object that received that call', async () => {
      const w = worker()
      const deliveredIn: string[] = []
      const subscribingTelefunction = async (name: string, resumeAfter: Promise<void>) => {
        await resumeAfter
        const channel = new ServerBroadcast<string>({ key: 'room:test:call' })
        const subscribed = channel.subscribe(() => deliveredIn.push(`${name}'s listener in ${w.delivering.in}`))
        await subscribed.ready
        return channel
      }
      const secondCallStart = Promise.withResolvers<void>()
      // The first call resumes only after the second one ran, so each subscribes while the other's call is under way.
      const firstCall = w.inObject(
        () => subscribingTelefunction('telefunc-shard-weur-1', secondCallStart.promise),
        w.session('telefunc-shard-weur-1'),
      )
      const secondCall = w.inObject(() => {
        secondCallStart.resolve()
        return subscribingTelefunction('telefunc-shard-weur-2', Promise.resolve())
      }, w.session('telefunc-shard-weur-2'))
      const channels = await Promise.all([firstCall, secondCall])
      await channels[0].publish('hello')

      expect({ deliveredIn: deliveredIn.sort(), members: w.presences.map(({ member }) => member).sort() }).toEqual({
        deliveredIn: [
          "telefunc-shard-weur-1's listener in telefunc-shard-weur-1",
          "telefunc-shard-weur-2's listener in telefunc-shard-weur-2",
        ],
        members: ['telefunc-shard-weur-1', 'telefunc-shard-weur-2'],
      })
      for (const channel of channels) channel.abort()
    })

    it('records what a listener subscribes while a publish is delivered under the Durable Object it runs in', async () => {
      const w = worker()
      const deliveredIn: Array<string | null> = []
      const joined = new ServerBroadcast<string>({ key: 'room:test:joined' })
      const joining = Promise.withResolvers<Promise<void>>()
      const join = w.inObject(
        () =>
          new ServerBroadcast<string>({ key: 'room:test:join' }).subscribe(() =>
            joining.resolve(joined.subscribe(() => deliveredIn.push(w.delivering.in)).ready),
          ),
        w.session('telefunc-shard-weur-1'),
      )
      await join.ready
      await Broadcast.publish('room:test:join', 'join')
      await await joining.promise
      await joined.publish('hello')

      expect(deliveredIn).toEqual(['telefunc-shard-weur-1'])
      joined.abort()
    })

    it('a subscriber with no page, outside the code of any Durable Object, throws an error that says where to subscribe', () => {
      worker()
      expect(() => Broadcast.subscribe('room:test', () => {})).toThrow(
        'subscribe from a telefunction or a channel handler, not from outside a request',
      )
    })
  })
})
