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
import type {
  BroadcastCalls,
  BroadcastHoldRequest,
  BroadcastLeaseRequest,
  BroadcastReleaseRequest,
} from './broadcast.js'
import { OrderedStubs } from './OrderedStubs.js'
import type { TelefuncDurableObjectNamespace } from './namespace.js'
import type { BroadcastRoute } from '../../backend/driver.js'
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { sqliteStorage } from '../../test/cloudflareSql.js'
import { CloudflareBackend } from './CloudflareBackend.js'
import { servesRequest, withCloudflareSession, type CloudflareSession } from './session.js'
import { disposeBackend, installBackend } from '../../backend/install.js'
import { broadcastRouteKey } from '../../backend/broadcast/routeKey.js'
import { Broadcast, ServerBroadcast } from '../../channel/server/ServerBroadcast.js'
import { getChannelMux } from '../../channel/server/ChannelMux.js'
import { installAsyncContext } from '../../../node/server/context/async.js'
import { onBug } from '../../../node/server/runTelefunc/onBug.js'

/** What the server reported through `onBug()`. */
const bugs: unknown[] = []
onBug((bug) => bugs.push(bug))

type CloudflareRequest = Request & { cf?: { colo?: string; continent?: string } }

function createCloudflareRequest({ colo, continent }: { colo?: string; continent?: string } = {}): CloudflareRequest {
  const request = new Request('https://telefunc.test') as CloudflareRequest
  request.cf = { colo, continent }
  return request
}

function createAuthorityState() {
  return new CloudflareBroadcastAuthorityState({ storage: sqliteStorage().storage } as DurableObjectState)
}

/** A member Durable Object's call about its presence at a key's authority. */
type PresenceCall =
  | { op: 'grant'; member: string }
  | ({ op: 'hold' } & BroadcastHoldRequest)
  | ({ op: 'renew' } & BroadcastLeaseRequest)
  | ({ op: 'release' } & BroadcastReleaseRequest)

/** The key's authority, as `call` reaches it. */
function applyPresence(authority: CloudflareBroadcastAuthorityState, call: PresenceCall): unknown {
  if (call.op === 'grant') return authority.grant(call.member)
  if (call.op === 'hold') return authority.hold(call)
  if (call.op === 'renew') return authority.renew(call)
  return authority.release(call)
}

/** `member`'s presence on the text route of `key`, recorded at `authority` in `bucket`. */
async function recordPresence(
  authority: CloudflareBroadcastAuthorityState,
  key: string,
  member: string,
  bucket: BroadcastHoldRequest['bucket'],
) {
  const grant = await authority.grant(member)
  await authority.hold({ key, kind: 'text', member, lease: crypto.randomUUID(), bucket, order: { grant, seq: 1 } })
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
    onPresence: (id: { name: string }, call: PresenceCall) => Promise<unknown>
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
          return overrides?.onDeliver?.(id, request) ?? Promise.resolve(0)
        },
        telefuncBroadcastGrant(member: string) {
          return overrides?.onPresence?.(id, { op: 'grant', member }) ?? Promise.resolve(1)
        },
        telefuncBroadcastHold(request: BroadcastHoldRequest) {
          return overrides?.onPresence?.(id, { op: 'hold', ...request }) ?? Promise.resolve('registered')
        },
        telefuncBroadcastRenew(request: BroadcastLeaseRequest) {
          return overrides?.onPresence?.(id, { op: 'renew', ...request }) ?? Promise.resolve('renewed')
        },
        telefuncBroadcastRelease(request: BroadcastReleaseRequest) {
          return overrides?.onPresence?.(id, { op: 'release', ...request }) ?? Promise.resolve('removed')
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
      await recordPresence(authorityState, 'room:test', `telefunc-shard-${bucket}-0`, bucket)
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
        onPresence: (_id, call) => Promise.resolve(applyPresence(authorityState, call)),
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
      await recordPresence(authorityState, 'room:test', `telefunc-shard-${bucket}-0`, bucket)
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
    const state: {
      refuse: boolean
      fail?: Error
      /** Where set, only calls of this kind fail with `fail`. */
      failOnly?: PresenceCall['op']
      held?: Promise<void>
      throwAtOnce?: Error
    } = { refuse: false }
    /** The writes of presence that reached the authority, not counting the grants each member asked for first. */
    const presences: Exclude<PresenceCall, { op: 'grant' }>[] = []
    const binding = createBasicBinding({
      onPresence(_id, call) {
        if (call.op === 'grant') return Promise.resolve(applyPresence(authority, call))
        if (state.throwAtOnce) throw state.throwAtOnce
        const { refuse, held } = state
        const fail = state.failOnly === undefined || state.failOnly === call.op ? state.fail : undefined
        presences.push(call)
        // The authority applies the calls one stub carries in the order they come, whenever its answer reaches the member.
        const answer = refuse || fail ? null : applyPresence(authority, call)
        return (async () => {
          await held
          if (refuse) throw new Error(REFUSAL)
          if (fail) throw fail
          return answer
        })()
      },
      onPublish: (_id, request) => broadcast.publishToSubscribers(authority, new OrderedStubs(), request),
      onForward: (_id, request) => broadcast.forwardToBucket(new OrderedStubs(), request),
      // Each member Durable Object takes a delivery in its own session, as its `telefuncBroadcastDeliver` does.
      onDeliver(id, request) {
        const session = byName.get(id.name)
        if (session === undefined) return Promise.resolve(0)
        delivering.in = id.name
        try {
          return Promise.resolve(withCloudflareSession(session, () => session.broadcast.deliver(request)))
        } finally {
          delivering.in = null
        }
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
      /** A sender's delivery of `routes` to the named member DOs, through the coordinators, as Room's fanout sends it. */
      forward: (routes: BroadcastRoute[], members: string[], owed: boolean) =>
        broadcast.forward(
          new OrderedStubs(),
          'room:test',
          { routes, payload: 'frame', info: { seq: 1, timestamp: 1 }, ...(owed ? { owed: true } : {}) },
          members.map((member) => ({ member, bucket: 'weur' as const })),
        ),
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

    // A private message's sender learns that it was lost where a Durable Object it went to had no receiver for it.
    describe('a delivery whose sender is owed every receiver', () => {
      const route: BroadcastRoute = { key: 'room:lane', kind: 'text' }
      const [first, second] = ['telefunc-shard-weur-0', 'telefunc-shard-weur-1']

      it('to a Durable Object that has no receiver for its route rejects', async () => {
        const { forward } = worker()
        await expect(forward([route], [first], true)).rejects.toThrow('lost to 1/1')
      })

      it('to two Durable Objects rejects where one has no receiver, though the other took it', async () => {
        const { forward, sessions } = worker()
        const taken: unknown[] = []
        sessions.first.broadcast.subscribeLocal(route, { deliver: (payload) => void taken.push(payload) })
        await expect(forward([route], [first, second], true)).rejects.toThrow('lost to 1/2')
        expect(taken).toEqual(['frame'])
      })

      it('to a Durable Object whose receiver was released on purpose counts as taken', async () => {
        const { forward, sessions } = worker()
        const release = sessions.first.broadcast.subscribeLocal(route, { deliver() {} })
        release()
        await expect(forward([route], [first], true)).resolves.toBeUndefined()
      })

      it('that is a publish, owed no receiver, resolves where a Durable Object has none', async () => {
        const { forward } = worker()
        await expect(forward([route], [first], false)).resolves.toBeUndefined()
      })
    })

    // A page that subscribes and then announces itself, as a chat's "joined", hears its own announcement.
    it('a publish the Durable Object makes right after it subscribes reaches the authority after its presence', async () => {
      const { inObject } = worker()
      const heard: string[] = []
      const subscriber = inObject(() => {
        const subscriber = Broadcast.subscribe<string>('room:test', (text) => heard.push(text))
        void Broadcast.publish('room:test', 'joined')
        return subscriber
      })
      await subscriber.ready
      await vi.waitFor(() => expect(heard).toEqual(['joined']))
      subscriber()
    })

    // A presence the authority refuses to store is a subscription that failed, as a refused one is on any other backend.
    describe("where the key's authority refuses the presence write", () => {
      it("a subscriber's `ready` rejects with the authority's error, the subscriber is removed, and a later one of the same Durable Object registers again", async () => {
        const { state, presences, inObject } = worker()
        state.refuse = true
        const first = inObject(() => Broadcast.subscribe('room:test', () => {}))
        expect(await outcome(first.ready)).toBe(REFUSAL)
        await new Promise((resolve) => setTimeout(resolve, 0))
        // Each refused first write is released after, which the authority refuses too until it takes it.
        const holds = () => presences.filter(({ op }) => op === 'hold')
        expect(holds()).toHaveLength(1)
        const second = inObject(() => Broadcast.subscribe('room:test', () => {}))
        expect(await outcome(second.ready)).toBe(REFUSAL)
        expect(holds()).toHaveLength(2)
        first()
        second()
        state.refuse = false
        const third = inObject(() => Broadcast.subscribe('room:test', () => {}))
        expect(await outcome(third.ready)).toBe('resolved')
        third()
      })

      // Cloudflare marks a transient failure `.retryable`, and one it says not to retry `.overloaded`. Neither appeared on
      // any error local workerd raised, so these errors are made here.
      const transient = (overloaded: boolean, message = 'Durable Object is overloaded or reset') =>
        Object.assign(new Error(message), { retryable: true, overloaded })

      /** What the subscriber's `ready` settled with (`'resolved'`, the cause of its rejection, or `'pending'`), when each
       *  presence write reached the authority in ms after it subscribed, and the bugs reported, `ms` after it subscribed:
       *  the n-th write fails with `failures[n]`, while one is left, and the subscriber is removed at `removeAt`. */
      async function firstRegistration(failures: Error[], { ms = 2_000, random = 1, removeAt = Infinity } = {}) {
        vi.useFakeTimers()
        vi.spyOn(Math, 'random').mockReturnValue(random)
        vi.spyOn(console, 'error').mockImplementation(() => {})
        bugs.length = 0
        // A test may make more than one.
        disposeBackend()
        const { state, presences, inObject } = worker()
        const startedAt = Date.now()
        const writtenAt: number[] = []
        // A first write that failed is released after; only the first writes count here.
        const holds = () => presences.filter(({ op }) => op === 'hold')
        const written = () => {
          while (writtenAt.length < holds().length) writtenAt.push(Date.now() - startedAt)
          state.fail = failures[holds().length]
        }
        state.fail = failures[0]
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        // Its first write goes once the authority granted its Durable Object, in this millisecond.
        await vi.advanceTimersByTimeAsync(0)
        written()
        let ready: unknown = 'pending'
        subscriber.ready.then(
          () => (ready = 'resolved'),
          (err: unknown) => (ready = err instanceof Error ? err.cause : err),
        )
        for (let elapsed = 1; elapsed <= ms; elapsed++) {
          await vi.advanceTimersByTimeAsync(1)
          written()
          if (elapsed === removeAt) subscriber()
        }
        if (removeAt === Infinity) subscriber()
        return { ready, writtenAt, bugs: [...bugs] }
      }

      it("a first presence write that fails as retryable twice is written again after the waits the backoff draws, and the subscriber's `ready` resolves once it is stored", async () => {
        const failures = [transient(false, 'first'), transient(false, 'second')]
        expect({
          1: await firstRegistration(failures, { random: 1 }),
          0.5: await firstRegistration(failures, { random: 0.5 }),
        }).toEqual({
          1: { ready: 'resolved', writtenAt: [0, 100, 300], bugs: [] },
          0.5: { ready: 'resolved', writtenAt: [0, 50, 150], bugs: [] },
        })
      })

      it("a first presence write that fails as retryable 3 times rejects the subscriber's `ready` with the last error, reports it once, and is not written again", async () => {
        const last = transient(false, 'third')
        const { ready, writtenAt, bugs } = await firstRegistration([
          transient(false, 'first'),
          transient(false, 'second'),
          last,
        ])
        expect(writtenAt).toEqual([0, 100, 300])
        expect(ready).toBe(last)
        expect(bugs.map((bug) => (bug instanceof Error ? bug.cause : bug))).toEqual([last])
        expect(bugs.map((bug) => bug instanceof Error && bug.cause === last)).toEqual([true])
      })

      it("a first presence write that fails as overloaded, at once or once written again, rejects the subscriber's `ready` with that error, and is not written again", async () => {
        const overloaded = transient(true, 'overloaded')
        const atOnce = await firstRegistration([overloaded])
        const writtenAgain = await firstRegistration([transient(false), overloaded])
        expect({ atOnce: atOnce.writtenAt, writtenAgain: writtenAgain.writtenAt }).toEqual({
          atOnce: [0],
          writtenAgain: [0, 100],
        })
        expect([atOnce.ready, writtenAgain.ready]).toEqual([overloaded, overloaded])
        expect([atOnce.ready === overloaded, writtenAgain.ready === overloaded]).toEqual([true, true])
        expect([atOnce.bugs.length, writtenAgain.bugs.length]).toEqual([1, 1])
      })

      // The runtime retries a call itself for up to 10 s; a failure that reaches the code that late isn't made again.
      it("a first presence write that fails as retryable 10 seconds in rejects the subscriber's `ready` with that error, and is not written again", async () => {
        vi.useFakeTimers()
        vi.spyOn(console, 'error').mockImplementation(() => {})
        const { state, presences, inObject } = worker()
        const failed = transient(false)
        state.held = new Promise((resolve) => setTimeout(resolve, 9_999))
        state.fail = failed
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        let ready: unknown = 'pending'
        subscriber.ready.catch((err: unknown) => (ready = err instanceof Error ? err.cause : err))
        // Its first write goes once the authority granted its Durable Object.
        await vi.advanceTimersByTimeAsync(0)
        state.held = undefined
        await vi.advanceTimersByTimeAsync(12_000)
        expect(presences.filter(({ op }) => op === 'hold')).toHaveLength(1)
        expect(ready).toBe(failed)
        subscriber()
      })

      it('a subscriber removed while its retryable first presence write waits 50 ms to be written again is not written again', async () => {
        const waiting = await firstRegistration([transient(false)], { ms: 60, random: 0.5 })
        const removed = await firstRegistration([transient(false)], { random: 0.5, removeAt: 25 })
        expect({ waiting: waiting.writtenAt, removed: removed.writtenAt }).toEqual({ waiting: [0, 50], removed: [0] })
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

      // A publish the authority answered overloaded pauses this Durable Object's calls to it for 10 s.
      const overloadedPublish = async (inObject: <T>(run: () => T) => T) => {
        vi.spyOn(CloudflareBroadcast.prototype, 'publishToSubscribers').mockRejectedValueOnce(transient(true))
        await Promise.resolve(inObject(() => Broadcast.publish('room:test', 'hello'))).catch(() => {})
      }

      it('a held presence whose refresh falls in a pause a publish answered overloaded caused is not reported lost, is tried no more while the pause lasts, and is refreshed once it ends', async () => {
        vi.useFakeTimers()
        const tried = vi.spyOn(OrderedStubs.prototype, 'call')
        const reported = vi.spyOn(console, 'error').mockImplementation(() => {})
        const lost = () => reported.mock.calls.filter(([error]) => String(error).includes('presence for')).length
        const { inObject, present, presences } = worker()
        const renewals = () => presences.filter((call) => call.op === 'renew').length
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await vi.advanceTimersByTimeAsync(0)
        await subscriber.ready
        await vi.advanceTimersByTimeAsync(29_000)
        await overloadedPublish(inObject)
        const before = tried.mock.calls.length
        await vi.advanceTimersByTimeAsync(9_500)
        expect({
          present: present('room:test').length,
          lost: lost(),
          renewals: renewals(),
          tries: tried.mock.calls.length - before,
        }).toEqual({ present: 1, lost: 0, renewals: 0, tries: 1 })
        await vi.advanceTimersByTimeAsync(2_500)
        expect({ present: present('room:test').length, lost: lost(), renewals: renewals() }).toEqual({
          present: 1,
          lost: 0,
          renewals: 1,
        })
        subscriber()
      })

      it('a held presence whose refreshes the pause keeps from being sent for longer than its 90 s is reported lost, and recorded again once the authority takes calls', async () => {
        vi.useFakeTimers()
        const reported = vi.spyOn(console, 'error').mockImplementation(() => {})
        const lost = () => reported.mock.calls.filter(([error]) => String(error).includes('presence for')).length
        const { inObject, present, presences } = worker()
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await vi.advanceTimersByTimeAsync(0)
        await subscriber.ready
        // A publish answered overloaded every 9 s keeps the authority paused from 25 s to 107 s.
        await vi.advanceTimersByTimeAsync(25_000)
        for (let at = 25_000; at < 100_000; at += 9_000) {
          await overloadedPublish(inObject)
          await vi.advanceTimersByTimeAsync(9_000)
        }
        expect({ lost: lost(), renewals: presences.filter((call) => call.op === 'renew').length }).toEqual({
          lost: 1,
          renewals: 0,
        })
        await vi.advanceTimersByTimeAsync(60_000)
        expect(present('room:test').map(({ member }) => member)).toEqual(['telefunc-shard-weur-0'])
        subscriber()
      })

      it('a held presence whose refresh waits for a pause is refreshed no more, and leaves no timer, once its last subscriber left', async () => {
        vi.useFakeTimers()
        vi.spyOn(console, 'error').mockImplementation(() => {})
        const { inObject, presences } = worker()
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await vi.advanceTimersByTimeAsync(0)
        await subscriber.ready
        await vi.advanceTimersByTimeAsync(29_000)
        await overloadedPublish(inObject)
        await vi.advanceTimersByTimeAsync(2_000)
        subscriber()
        await vi.advanceTimersByTimeAsync(20_000)
        expect({
          renewals: presences.filter((call) => call.op === 'renew').length,
          timers: vi.getTimerCount(),
        }).toEqual({ renewals: 0, timers: 0 })
      })

      it("a first presence write the key's authority never answers rejects the subscriber's `ready` within 10 s", async () => {
        vi.useFakeTimers()
        vi.spyOn(console, 'error').mockImplementation(() => {})
        const { state, inObject } = worker()
        state.held = new Promise<void>(() => {})
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        let ready = 'pending'
        subscriber.ready.then(
          () => (ready = 'resolved'),
          () => (ready = 'rejected'),
        )
        await vi.advanceTimersByTimeAsync(0)
        state.held = undefined
        await vi.advanceTimersByTimeAsync(10_000)
        expect(ready).toBe('rejected')
        subscriber()
      })

      it('a presence that lapsed is reported lost once, however many refreshes the authority tells of it before it is recorded again', async () => {
        vi.useFakeTimers()
        const reported = vi.spyOn(console, 'error').mockImplementation(() => {})
        const lost = () =>
          reported.mock.calls.filter(([text]) => String(text).startsWith('Broadcast delivery of')).length
        const { state, inObject, present } = worker()
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await vi.advanceTimersByTimeAsync(0)
        await subscriber.ready
        // Its refreshes fail past the presence's 90 s.
        state.fail = new Error('reset')
        await vi.advanceTimersByTimeAsync(90_000)
        expect([present('room:test'), lost()]).toEqual([[], 0])
        // The next refresh is told of the lapse; recording the presence again fails twice, then succeeds.
        state.failOnly = 'hold'
        await vi.advanceTimersByTimeAsync(60_000)
        expect([present('room:test'), lost()]).toEqual([[], 1])
        state.fail = undefined
        await vi.advanceTimersByTimeAsync(30_000)
        expect([present('room:test').map(({ member }) => member), lost()]).toEqual([['telefunc-shard-weur-0'], 1])
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

      it('whose publish right after it subscribed, 100 s after its last subscriber left and another Durable Object subscribed, reaches its presence', async () => {
        vi.useFakeTimers()
        vi.spyOn(console, 'error').mockImplementation(() => {})
        const { inObject, sessions } = worker()
        const first = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await vi.advanceTimersByTimeAsync(0)
        await first.ready
        first()
        await vi.advanceTimersByTimeAsync(100_000)
        // Another Durable Object subscribes: its grant drops the first one's.
        const other = inObject(() => Broadcast.subscribe('room:test', () => {}), sessions.second)
        await vi.advanceTimersByTimeAsync(0)
        await other.ready
        const heard: string[] = []
        const back = inObject(() => {
          const back = Broadcast.subscribe<string>('room:test', (text) => heard.push(text))
          void Broadcast.publish('room:test', 'joined')
          return back
        })
        for (let turn = 0; turn < 5; turn++) await vi.advanceTimersByTimeAsync(0)
        await back.ready
        for (let turn = 0; turn < 5; turn++) await vi.advanceTimersByTimeAsync(0)
        expect(heard).toEqual(['joined'])
        back()
        other()
      })

      it('whose refresh is answered lapsed after the last subscriber left stores no presence', async () => {
        vi.useFakeTimers()
        vi.spyOn(console, 'error').mockImplementation(() => {})
        const { state, inObject, present } = worker()
        const left = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await vi.advanceTimersByTimeAsync(0)
        await left.ready
        // Its refreshes fail past the presence's 90 s.
        state.fail = new Error('reset')
        await vi.advanceTimersByTimeAsync(90_000)
        state.fail = undefined
        // The next refresh reaches the authority; its answer comes after the last subscriber left.
        const answer = Promise.withResolvers<void>()
        state.held = answer.promise
        await vi.advanceTimersByTimeAsync(30_000)
        state.held = undefined
        left()
        await vi.advanceTimersByTimeAsync(0)
        answer.resolve()
        await vi.advanceTimersByTimeAsync(0)
        await vi.advanceTimersByTimeAsync(0)
        expect(present('room:test').map(({ member }) => member)).toEqual([])
      })

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
            asked: presences.map(({ op }) => (op === 'release' ? 'withdraw' : 'record')).slice(-2),
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

describe("a Durable Object's delivery to a receiver that throws, ahead of another route's receiver", () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('is reported, and the route after it and the deliveries after it still arrive', () => {
    const reports: unknown[] = []
    vi.spyOn(console, 'error').mockImplementation((error) => void reports.push(error))
    const member = createBroadcast().member('telefunc-shard-weur-0', new OrderedStubs())
    const throwing: BroadcastRoute = { key: 'room:throwing', kind: 'text' }
    const healthyRoute: BroadcastRoute = { key: 'room:healthy', kind: 'text' }
    const thrower: unknown[] = []
    const healthy: unknown[] = []
    member.subscribeLocal(throwing, {
      deliver(payload) {
        thrower.push(payload)
        throw new Error('Test: a receiver throws')
      },
    })
    member.subscribeLocal(healthyRoute, { deliver: (payload) => void healthy.push(payload) })

    const missing = [1, 2].map((seq) =>
      member.deliver({
        routes: [throwing, healthyRoute],
        payload: `"${seq}"`,
        info: { seq, timestamp: 1 },
        owed: true,
      }),
    )

    expect({ missing, thrower, healthy }).toEqual({ missing: [0, 0], thrower: ['"1"', '"2"'], healthy: ['"1"', '"2"'] })
    expect(reports.map(String)).toEqual(['Error: Test: a receiver throws', 'Error: Test: a receiver throws'])
  })
})
