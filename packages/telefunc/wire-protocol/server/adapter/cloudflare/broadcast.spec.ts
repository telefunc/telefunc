import { DatabaseSync, type SQLInputValue } from 'node:sqlite'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  DEFAULT_BROADCAST_BUCKETS,
  assertLocationFallbackIsScaled,
  getBucketCoordinatorShardIndices,
  getDeterministicKeyBucketIndex,
  getShardIndicesForBucket,
  resolveCloudflareLocationHint,
  resolveSessionRoutingTarget,
} from './routing.js'
import '../../../../node/server/async_hooks.js'
import { CloudflareBroadcastAuthorityState, CloudflareBroadcastTransport } from './broadcast.js'
import type { BroadcastCalls, BroadcastPresenceRequest, CloudflareBroadcastMember } from './broadcast.js'
import { withCloudflareSession } from './session.js'
import type { BroadcastLane } from '../../../backend/broadcast/contract.js'
import { broadcastRouteKey } from '../../../backend/broadcast/route-key.js'
import { OrderedStubs } from './ordered-stubs.js'
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { ServerBroadcast } from '../../server-broadcast.js'
import { disposeBackend, installBackend } from '../../../backend/install.js'
import { CloudflareRoomBackend } from './room/backend.js'
import type { SubscriptionAttempt, SubscriptionState } from '../../../backend/subscription.js'

const encode = (text: string) => new TextEncoder().encode(text)
const decode = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

/** Resolves once the attempt is ready; rejects if it ends first. */
function untilReady(attempt: SubscriptionAttempt): Promise<void> {
  return new Promise((resolve, reject) => {
    const settle = (state: SubscriptionState, reason?: Error): boolean => {
      if (state === 'ready') resolve()
      else if (state === 'closed') reject(reason ?? new Error(`attempt ${state}`))
      else return false
      return true
    }
    if (settle(attempt.state())) return
    const stop = attempt.onStateChange((state, reason) => {
      if (settle(state, reason)) stop()
    })
  })
}

type CloudflareRequest = Request & { cf?: { colo?: string; continent?: string } }

afterEach(async () => {
  await disposeBackend()
})

function installCloudflareTransport(transport: CloudflareBroadcastTransport): void {
  installBackend(
    () =>
      new CloudflareRoomBackend({
        rooms: () => {
          throw new Error('Broadcast specs use no Room namespace')
        },
        broadcast: transport,
      }),
  )
}

function createCloudflareRequest({ colo, continent }: { colo?: string; continent?: string } = {}): CloudflareRequest {
  const request = new Request('https://telefunc.test') as CloudflareRequest
  request.cf = { colo, continent }
  return request
}

/** Cloudflare's SQLite storage API over node:sqlite, as far as the adapter uses it. */
function createSqlState(): DurableObjectState {
  const db = new DatabaseSync(':memory:')
  const cursor = (rows: unknown[]) => ({ toArray: () => rows, one: () => rows[0] })
  const sql = {
    exec(query: string, ...bindings: SQLInputValue[]) {
      if (bindings.length === 0 && !/^\s*SELECT/i.test(query)) {
        db.exec(query)
        return cursor([])
      }
      const statement = db.prepare(query)
      return cursor(/^\s*SELECT/i.test(query) ? statement.all(...bindings) : (statement.run(...bindings), []))
    },
  }
  const storage = {
    sql,
    transactionSync<T>(fn: () => T): T {
      db.exec('BEGIN')
      try {
        const result = fn()
        db.exec('COMMIT')
        return result
      } catch (error) {
        db.exec('ROLLBACK')
        throw error
      }
    },
  }
  return { storage } as unknown as DurableObjectState
}

function createAuthorityState(state = createSqlState()) {
  return new CloudflareBroadcastAuthorityState(state)
}

type PresenceHooks = { beforeRecord?: () => Promise<void>; beforeWithdraw?: () => Promise<void> }

/** The key's authority as a binding reaches it: presence writes arrive in call order, as through one stub, each after
 *  its hook, and land in `authority`. */
function presenceAt(authority: CloudflareBroadcastAuthorityState, hooks: PresenceHooks = {}) {
  let arrival = Promise.resolve()
  return (_id: unknown, request: BroadcastPresenceRequest): Promise<void> =>
    (arrival = arrival.then(async () => {
      await (request.bucket === null ? hooks.beforeWithdraw : hooks.beforeRecord)?.()
      authority.setPresence(request)
    }))
}

function liveMembers(authority: CloudflareBroadcastAuthorityState, lane: BroadcastLane) {
  return Object.fromEntries(authority.livePresence(broadcastRouteKey(lane), Date.now()))
}

async function flushMicrotasks(turns = 6): Promise<void> {
  for (let index = 0; index < turns; index++) {
    await Promise.resolve()
  }
}

async function flushCoordinatorTurn(): Promise<void> {
  await flushMicrotasks()
  await new Promise<void>((resolve) => setTimeout(resolve, 0))
}

function createBasicBinding(
  overrides?: Partial<{
    onPublish: (id: { name: string }, request: any) => any
    onForward: (id: { name: string }, request: any) => any
    onDeliver: (id: { name: string }, request: any) => any
    onPresence: (id: { name: string }, request: any) => any
  }>,
) {
  return {
    idFromName(name: string) {
      return {
        name,
        equals(other: { name: string }) {
          return other.name === name
        },
      }
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
          return overrides?.onForward?.(id, request) ?? Promise.resolve()
        },
        telefuncBroadcastDeliver(request: any) {
          return overrides?.onDeliver?.(id, request) ?? Promise.resolve()
        },
        telefuncBroadcastPresence(request: any) {
          return overrides?.onPresence?.(id, request) ?? Promise.resolve()
        },
      }
    },
  } as unknown as DurableObjectNamespace
}

/** Stubs that deliver like Cloudflare's: calls through one stub arrive in call order, after that stub's latency;
 *  calls through different stubs race. The n-th stub to carry a call gets `latencies[n]` (default 0). */
function createRacingBinding(
  latencies: number[],
  handlers: {
    onForward: (request: any) => Promise<void>
    onDeliver: (request: any) => Promise<void>
    onPresence: (request: any) => Promise<void>
  },
) {
  let used = 0
  return {
    idFromName(name: string) {
      return { name }
    },
    idFromString(id: string) {
      return { name: id }
    },
    get() {
      let latency: number | undefined
      let arrival = Promise.resolve()
      const send =
        (handler: (request: any) => Promise<void>) =>
        (request: any): Promise<void> => {
          const delay = (latency ??= latencies[used++] ?? 0)
          arrival = arrival.then(() => new Promise((resolve) => setTimeout(resolve, delay)))
          return arrival.then(() => handler(request))
        }
      return {
        telefuncBroadcastForward: send(handlers.onForward),
        telefuncBroadcastDeliver: send(handlers.onDeliver),
        telefuncBroadcastPresence: handlers.onPresence,
      }
    },
  } as unknown as DurableObjectNamespace
}

function createTransport(binding = createBasicBinding()): CloudflareBroadcastTransport {
  return new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1, namespace: () => binding })
}

/** A session DO's Broadcast membership, placed in weur. */
function createMember(transport: CloudflareBroadcastTransport, id = 'member-weur-0'): CloudflareBroadcastMember {
  const member = transport.member(id, new OrderedStubs())
  member.locate('weur')
  return member
}

/** Runs `fn` as code in the member's session DO. */
function inSession<T>(member: CloudflareBroadcastMember, fn: () => T): T {
  return withCloudflareSession(
    {
      room: () => {
        throw new Error('Broadcast specs use no Room')
      },
      broadcast: () => member,
    },
    fn,
  )
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
    const shardIndices = getBucketCoordinatorShardIndices(2, 'weur')

    expect(getDeterministicKeyBucketIndex('room/alpha', shardIndices.length)).toBe(
      getDeterministicKeyBucketIndex('room/alpha', shardIndices.length),
    )
  })

  it('assigns room keys only within the bucket-coordinator subset', () => {
    const weurShards = getBucketCoordinatorShardIndices(2, 'weur')
    const apacShards = getBucketCoordinatorShardIndices(2, 'apac')
    const ocShards = getBucketCoordinatorShardIndices(2, 'oc')

    expect(weurShards).toContain(weurShards[getDeterministicKeyBucketIndex('room/alpha', weurShards.length)]!)
    expect(apacShards).toContain(apacShards[getDeterministicKeyBucketIndex('room/alpha', apacShards.length)]!)
    expect(ocShards).toContain(ocShards[getDeterministicKeyBucketIndex('room/alpha', ocShards.length)]!)
  })

  it('partitions shards by bucket when the scale is uniform', () => {
    expect(getShardIndicesForBucket(2, 'wnam')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'enam')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'weur')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'eeur')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'apac')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'oc')).toEqual([0, 1])
  })

  it('partitions bucket coordinators at ceil(sessionScale / 2) per bucket', () => {
    expect(getBucketCoordinatorShardIndices(1, 'wnam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices(2, 'wnam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices(3, 'wnam')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices(4, 'wnam')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices(4, 'enam')).toEqual([0, 1])
  })

  it('uses scale maps to control the session and bucket-coordinator shard subsets together', () => {
    expect(getShardIndicesForBucket({ weur: 2, enam: 1 }, 'enam')).toEqual([0])
    expect(getShardIndicesForBucket({ weur: 2, enam: 1 }, 'weur')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices({ weur: 2, enam: 1 }, 'enam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 2, enam: 1 }, 'weur')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 3, enam: 1 }, 'weur')).toEqual([0, 1])
  })

  it('uses only canonical bucket scale entries', () => {
    expect(getShardIndicesForBucket({ weur: 2, apac: 1 }, 'weur')).toEqual([0, 1])
    expect(getShardIndicesForBucket({ weur: 2, apac: 1 }, 'apac')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 2, apac: 1 }, 'apac')).toEqual([0])
  })

  it('resolves session targets from request location and scale', () => {
    const exactRequest = createCloudflareRequest({ colo: 'LHR' })
    const unknownRequest = createCloudflareRequest({ continent: 'EU' })
    const exactTarget = resolveSessionRoutingTarget('telefunc', { weur: 2, apac: 1 }, exactRequest, 'weur')
    const fallbackTarget = resolveSessionRoutingTarget('telefunc', { weur: 1, apac: 1 }, unknownRequest, 'weur')

    expect(exactTarget).toMatchObject({
      sessionInstanceName: expect.stringMatching(/^telefunc-shard-weur-/),
      locationBucket: 'weur',
    })
    expect(fallbackTarget).toMatchObject({
      sessionInstanceName: 'telefunc-shard-weur-0',
      locationBucket: 'weur',
      shardOrdinal: 0,
    })
  })

  it('routes a recognized region missing from the scale map to locationFallback instead of throwing', () => {
    // `ABQ` resolves to `wnam`, which is absent from this per-region scale map.
    const wnamRequest = createCloudflareRequest({ colo: 'ABQ' })
    const target = resolveSessionRoutingTarget('telefunc', { weur: 2, apac: 1 }, wnamRequest, 'weur')

    expect(target).toMatchObject({
      sessionInstanceName: expect.stringMatching(/^telefunc-shard-weur-/),
      locationBucket: 'weur',
    })
    expect([0, 1]).toContain(target.shardOrdinal)
  })

  it('rejects a per-region scale map whose locationFallback region has no shards', () => {
    // `locationFallback` is where unlisted regions land, so it must itself be scaled.
    expect(() => assertLocationFallbackIsScaled({ enam: 3, apac: 2 }, 'weur')).toThrow(/locationFallback/)
    expect(() => assertLocationFallbackIsScaled({ weur: 2, apac: 1 }, 'weur')).not.toThrow()
    // A uniform numeric scale (or the default) applies to every region, so any fallback is fine.
    expect(() => assertLocationFallbackIsScaled(4, 'weur')).not.toThrow()
    expect(() => assertLocationFallbackIsScaled(undefined, 'weur')).not.toThrow()
  })

  it('records presence at the key’s authority on subscribe and reads it during publish fanout', async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const transport = createTransport(
      createBasicBinding({
        onPresence: presenceAt(authority),
        onPublish: (_, request) => transport.publishToSubscribers(authority, calls, request),
      }),
    )
    const member = createMember(transport)
    const lane = { key: 'room:test', kind: 'text' } as const
    const subscription = member.openSubscription(lane, () => {})
    await untilReady(subscription)
    expect(liveMembers(authority, lane)).toEqual({ weur: ['member-weur-0'] })
    const binary = await transport.publish({ key: 'room:test', kind: 'binary' }, new Uint8Array([1]))
    const text = await transport.publish(lane, encode('"text"'))
    expect([binary.receivers, text.receivers]).toEqual([0, 1])
    await subscription.unsubscribe()
  })

  it('keeps the first-touch authority bucket in publish receipts', async () => {
    const authorityState = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const transport = createTransport(createBasicBinding())
    // The key's first publish, from weur, fixes its authority bucket.
    authorityState.sequence('room:first-touch', 'weur')
    for (const bucket of ['weur', 'apac'] as const) {
      await authorityState.setPresence({
        key: 'room:first-touch',
        kind: 'text',
        member: `telefunc-shard-${bucket}-0`,
        bucket,
      })
    }
    const receipt = await transport.publishToSubscribers(authorityState, calls, {
      key: 'room:first-touch',
      kind: 'text',
      locationBucket: 'apac',
      payload: encode('{"text":"hello"}'),
    })
    expect(receipt).toMatchObject({ seq: 2, meta: { authorityBucket: 'weur' } })
    expect((receipt.meta!.fanoutBuckets as string[]).sort()).toEqual(['apac', 'weur'])
    expect(receipt.timestamp).toEqual(expect.any(Number))
  })

  it('rejects generic Broadcast sequence exhaustion before persisting an unsafe cursor', () => {
    const key = 'room:exhausted'
    const state = createSqlState()
    const authority = createAuthorityState(state)
    authority.sequence(key, 'weur')
    state.storage.sql.exec('UPDATE broadcast_key SET seq = ? WHERE key = ?', Number.MAX_SAFE_INTEGER, key)
    expect(() => authority.sequence(key, 'weur')).toThrow('sequence exhausted')
    const [row] = state.storage.sql.exec<{ seq: number }>('SELECT seq FROM broadcast_key WHERE key = ?', key).toArray()
    expect(row!.seq).toBe(Number.MAX_SAFE_INTEGER)
  })

  it('waits for the authority to record presence before publishing', async () => {
    const recorded = Promise.withResolvers<void>()
    const publishTargets: string[] = []
    const transport = createTransport(
      createBasicBinding({
        onPresence: () => recorded.promise,
        onPublish(id) {
          publishTargets.push(id.name)
          return Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
      }),
    )
    installCloudflareTransport(transport)
    await inSession(createMember(transport), async () => {
      const room = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
      // subscribe() records presence at the authority — publish should wait for it
      room.subscribe(() => {})
      room.publish({ text: 'hello' })
      await flushMicrotasks(2)
      expect(publishTargets).toEqual([])
      recorded.resolve()
      await flushCoordinatorTurn()
      expect(publishTargets).toEqual(['telefunc:broadcast:authority:room:test'])
    })
  })

  it('a publish held behind another session’s subscription still leaves from its own session', async () => {
    const recorded = Promise.withResolvers<void>()
    const publishBuckets: Array<string | null> = []
    const transport = createTransport(
      createBasicBinding({
        onPresence: () => recorded.promise,
        onPublish(_id, request) {
          publishBuckets.push(request.locationBucket)
          return Promise.resolve({ seq: publishBuckets.length, timestamp: Date.now() })
        },
      }),
    )
    installCloudflareTransport(transport)
    const weur = createMember(transport)
    const enam = transport.member('member-enam-0', new OrderedStubs())
    enam.locate('enam')
    const publishFrom = (member: CloudflareBroadcastMember) =>
      inSession(member, () => new ServerBroadcast<string>({ key: 'room:test' }).publish(member.bucket!))
    inSession(weur, () => new ServerBroadcast<string>({ key: 'room:test' }).subscribe(() => {}))
    const published = [publishFrom(weur), publishFrom(enam)]
    recorded.resolve()
    await Promise.all(published)
    expect(publishBuckets).toEqual(['weur', 'enam'])
  })

  it('does not deliver locally before ordered publish setup completes', async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorCalls: BroadcastCalls = new OrderedStubs()
    const recorded = Promise.withResolvers<void>()
    const received: string[] = []
    const transport: CloudflareBroadcastTransport = createTransport(
      createBasicBinding({
        onPresence: presenceAt(authority, { beforeRecord: () => recorded.promise }),
        onPublish(_id, request) {
          return transport.publishToSubscribers(authority, calls, request)
        },
        onForward(_id, request) {
          return transport.forwardToBucket(coordinatorCalls, request)
        },
        onDeliver(_id, request) {
          return member.deliver(request)
        },
      }),
    )
    installCloudflareTransport(transport)
    const member = createMember(transport)
    await inSession(member, async () => {
      const subscriber = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
      subscriber.subscribe((message) => {
        received.push(message.text)
      })
      const publisher = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
      publisher.publish({ text: 'hello' })
      await flushMicrotasks(2)
      expect(received).toEqual([])
      recorded.resolve()
      await flushCoordinatorTurn()
      expect(received).toEqual(['hello'])
    })
  })

  it('resolves publish ack with authority metadata after cold-path setup completes', async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorCalls: BroadcastCalls = new OrderedStubs()
    const recorded = Promise.withResolvers<void>()
    const transport: CloudflareBroadcastTransport = createTransport(
      createBasicBinding({
        onPresence: presenceAt(authority, { beforeRecord: () => recorded.promise }),
        onPublish(_id, request) {
          return transport.publishToSubscribers(authority, calls, request)
        },
        onForward(_id, request) {
          return transport.forwardToBucket(coordinatorCalls, request)
        },
        onDeliver(_id, request) {
          return member.deliver(request)
        },
      }),
    )
    installCloudflareTransport(transport)
    const member = createMember(transport)
    await inSession(member, async () => {
      const subscriber = new ServerBroadcast<{ text: string }>({ key: 'room:test:ack' })
      subscriber.subscribe(() => undefined)
      const publisher = new ServerBroadcast<{ text: string }>({ key: 'room:test:ack' })
      const receiptPromise = publisher.publish({ text: 'hello' })
      await flushMicrotasks(2)
      recorded.resolve()
      const receipt = await receiptPromise
      expect(receipt).toMatchObject({
        key: 'room:test:ack',
        seq: 1,
        meta: {
          authorityBucket: 'weur',
          fanoutBuckets: ['weur'],
        },
      })
      expect(receipt.timestamp).toEqual(expect.any(Number))
    })
  })

  it('authority forwards once to each populated bucket coordinator', async () => {
    const authorityState = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinators: string[] = []
    const transport: CloudflareBroadcastTransport = createTransport(
      createBasicBinding({
        onForward(id) {
          coordinators.push(id.name)
          return Promise.resolve()
        },
      }),
    )
    await authorityState.setPresence({
      key: 'room:test',
      kind: 'text',
      member: 'telefunc-shard-weur-0',
      bucket: 'weur',
    })
    await authorityState.setPresence({
      key: 'room:test',
      kind: 'text',
      member: 'telefunc-shard-apac-0',
      bucket: 'apac',
    })
    await authorityState.setPresence({
      key: 'room:test',
      kind: 'text',
      member: 'telefunc-shard-eeur-0',
      bucket: 'eeur',
    })
    await transport.publishToSubscribers(authorityState, calls, {
      key: 'room:test',
      kind: 'text',
      locationBucket: 'weur',
      payload: encode('{"text":"hello"}'),
    })
    expect(coordinators.sort()).toEqual([
      'telefunc:broadcast:apac:0',
      'telefunc:broadcast:eeur:0',
      'telefunc:broadcast:weur:0',
    ])
  })

  it('a member that fails to take a publish loses it: the publish resolves and the loss is logged', async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorCalls: BroadcastCalls = new OrderedStubs()
    const transport: CloudflareBroadcastTransport = createTransport(
      createBasicBinding({
        onPresence: presenceAt(authority),
        onPublish: (_id, request) => transport.publishToSubscribers(authority, calls, request),
        onForward: (_id, request) => transport.forwardToBucket(coordinatorCalls, request),
        onDeliver: () => Promise.reject(new Error('member reset')),
      }),
    )
    installCloudflareTransport(transport)
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    await inSession(createMember(transport), async () => {
      const room = new ServerBroadcast<string>({ key: 'room:test' })
      room.subscribe(() => {})
      await expect(room.publish('lost')).resolves.toMatchObject({ receivers: 1 })
    })
    expect(report).toHaveBeenCalledWith(expect.stringContaining("delivery of 'room:test' lost to 1/1"))
  })

  it('a forward delivers wide ordering positions to every named DO', async () => {
    const deliveredTo: string[] = []
    const received: Array<{ text: string; seq: number; timestamp: number }> = []
    const transport: CloudflareBroadcastTransport = createTransport(
      createBasicBinding({
        onDeliver(id, request) {
          deliveredTo.push(id.name)
          return member.deliver(request)
        },
      }),
    )
    const member = createMember(transport)
    const subscription = member.openSubscription({ key: 'room:test', kind: 'text' }, (payload, info) => {
      received.push({ text: decode(payload), ...info })
    })
    await untilReady(subscription)
    await transport.forwardToBucket(new OrderedStubs(), {
      key: 'room:test',
      kind: 'text',
      payload: encode('{"text":"hello"}'),
      info: { seq: 0x1_0000_0000, timestamp: 0x1_0000_0001 },
      members: ['member-weur-0', 'member-weur-1'],
    })
    expect(deliveredTo.sort()).toEqual(['member-weur-0', 'member-weur-1'])
    expect(received).toEqual([
      { text: '{"text":"hello"}', seq: 0x1_0000_0000, timestamp: 0x1_0000_0001 },
      { text: '{"text":"hello"}', seq: 0x1_0000_0000, timestamp: 0x1_0000_0001 },
    ])
    await subscription.unsubscribe()
  })

  it('delivers a key’s publishes to each member in seq order while calls through different stubs race', async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorCalls: BroadcastCalls = new OrderedStubs()
    // The first stub opened is the slowest, so a publish sent through a fresh stub overtakes the one before it.
    const transport: CloudflareBroadcastTransport = createTransport(
      createRacingBinding([20], {
        onForward: (request) => transport.forwardToBucket(coordinatorCalls, request),
        onDeliver: async (request) => member.deliver(request),
        onPresence: async (request) => authority.setPresence(request),
      }),
    )
    const member = createMember(transport)
    const received: number[] = []
    const lane = { key: 'room:order', kind: 'text' } as const
    const subscription = member.openSubscription(lane, (_payload, info) => void received.push(info.seq))
    await untilReady(subscription)
    const publish = () =>
      transport.publishToSubscribers(authority, calls, { ...lane, locationBucket: 'weur', payload: encode('"x"') })
    await Promise.all([publish(), publish(), publish()])
    expect(received).toEqual([1, 2, 3])
    await subscription.unsubscribe()
  })

  it('a subscription is ready once the key’s authority holds its presence, so the next publish reaches it', async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorCalls: BroadcastCalls = new OrderedStubs()
    const transport = createTransport(
      createBasicBinding({
        onPresence: presenceAt(authority),
        onForward: (_, request) => transport.forwardToBucket(coordinatorCalls, request),
        onDeliver: (_, request) => member.deliver(request),
      }),
    )
    const member = createMember(transport)
    const lane = { key: 'room:fresh', kind: 'text' } as const
    const received: string[] = []
    const subscription = member.openSubscription(lane, (payload) => void received.push(decode(payload)))
    await untilReady(subscription)
    await transport.publishToSubscribers(authority, calls, {
      ...lane,
      locationBucket: 'weur',
      payload: encode('"first"'),
    })
    expect(received).toEqual(['"first"'])
    await subscription.unsubscribe()
  })

  it('publishes from outside a session, as from a cron trigger, without a bucket', async () => {
    const coordinatorPublishes: Array<{ name: string; key: string; locationBucket: string | null; text: string }> = []
    const transport: CloudflareBroadcastTransport = createTransport(
      createBasicBinding({
        onPublish(id, { key, locationBucket, payload }) {
          coordinatorPublishes.push({ name: id.name, key, locationBucket, text: decode(payload) })
          return Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
      }),
    )
    installCloudflareTransport(transport)
    const room = new ServerBroadcast<{ text: string }>({ key: 'room:test:no-ctx' })

    expect(() => room.publish({ text: 'hello' })).not.toThrow()

    await flushCoordinatorTurn()

    expect(coordinatorPublishes).toEqual([
      {
        name: 'telefunc:broadcast:authority:room:test:no-ctx',
        key: 'room:test:no-ctx',
        locationBucket: null,
        text: '{"text":"hello"}',
      },
    ])
  })

  it('a member registers presence only once it knows its bucket', async () => {
    const transport = createTransport()
    const member = transport.member('member-unplaced', new OrderedStubs())
    const subscription = member.openSubscription({ key: 'room:test', kind: 'text' }, () => {})
    await expect(untilReady(subscription)).rejects.toThrow('knows its bucket')
  })

  it('serializes authority dispatch without blocking later publishes on remote delivery completion', async () => {
    const authorityState = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorPublishes: string[] = []
    let releaseFirstRemotePublish: (() => void) | null = null
    const firstRemotePublishReady = new Promise<void>((resolve) => {
      releaseFirstRemotePublish = resolve
    })
    const transport: CloudflareBroadcastTransport = createTransport({
      idFromName(name: string) {
        return {
          name,
          equals(other: { name: string }) {
            return other.name === name
          },
        }
      },
      get(id: { name: string }) {
        return {
          telefuncBroadcastForward({ payload }: any) {
            const text = decode(payload)
            coordinatorPublishes.push(`${id.name}:${text}`)
            if (id.name.includes(':broadcast:apac:') && text === '{"text":"first"}') return firstRemotePublishReady
            return Promise.resolve()
          },
          telefuncBroadcastDeliver() {
            return Promise.resolve()
          },
        }
      },
    } as unknown as DurableObjectNamespace)
    await authorityState.setPresence({
      key: 'room:test',
      kind: 'text',
      member: 'telefunc-shard-weur-0',
      bucket: 'weur',
    })
    await authorityState.setPresence({
      key: 'room:test',
      kind: 'text',
      member: 'telefunc-shard-apac-0',
      bucket: 'apac',
    })
    const firstPublish = transport.publishToSubscribers(authorityState, calls, {
      key: 'room:test',
      kind: 'text',
      locationBucket: 'weur',
      payload: encode('{"text":"first"}'),
    })
    await flushMicrotasks(8)
    const secondPublish = transport.publishToSubscribers(authorityState, calls, {
      key: 'room:test',
      kind: 'text',
      locationBucket: 'weur',
      payload: encode('{"text":"second"}'),
    })
    await flushMicrotasks(8)

    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"first"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"first"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"second"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"second"}')

    releaseFirstRemotePublish!()
    await Promise.all([firstPublish, secondPublish])

    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"second"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"second"}')
  })

  it('withdraws presence at the authority on unsubscribe', async () => {
    const authority = createAuthorityState()
    const transport = createTransport(createBasicBinding({ onPresence: presenceAt(authority) }))
    const member = createMember(transport)
    const lane = { key: 'room:test', kind: 'text' } as const
    const subscription = member.openSubscription(lane, () => {})
    await untilReady(subscription)
    expect(liveMembers(authority, lane)).toEqual({ weur: ['member-weur-0'] })
    await subscription.unsubscribe()
    expect(liveMembers(authority, lane)).toEqual({})
  })

  it('keeps presence generation-safe across setup and withdrawal churn', async () => {
    const setup = Promise.withResolvers<void>()
    const hooks: PresenceHooks = { beforeRecord: () => setup.promise }
    const authority = createAuthorityState()
    const transport = createTransport(createBasicBinding({ onPresence: presenceAt(authority, hooks) }))
    const member = createMember(transport)
    const lane = { key: 'room:presence-churn', kind: 'text' } as const
    const first = member.openSubscription(lane, () => {})
    await first.unsubscribe()
    const successor = member.openSubscription(lane, () => {})
    setup.resolve()
    await untilReady(successor)
    await flushMicrotasks()
    expect(liveMembers(authority, lane)).toEqual({ weur: ['member-weur-0'] })
    const releaseWithdrawal = Promise.withResolvers<void>()
    hooks.beforeWithdraw = () => releaseWithdrawal.promise
    const teardown = successor.unsubscribe()
    const replacement = member.openSubscription(lane, () => {})
    await flushMicrotasks()
    expect(replacement.state()).toBe('establishing')
    releaseWithdrawal.resolve()
    await Promise.all([teardown, untilReady(replacement)])
    expect(liveMembers(authority, lane)).toEqual({ weur: ['member-weur-0'] })
    await replacement.unsubscribe()
  })

  it('a subscription opened during a deferred presence teardown establishes fresh presence', async () => {
    const setup = Promise.withResolvers<void>()
    const withdrawing = Promise.withResolvers<void>()
    const withdrawal = Promise.withResolvers<void>()
    const hooks: PresenceHooks = { beforeRecord: () => setup.promise }
    const authority = createAuthorityState()
    const transport = createTransport(createBasicBinding({ onPresence: presenceAt(authority, hooks) }))
    const member = createMember(transport)
    const lane = { key: 'room:deferred-teardown', kind: 'text' } as const
    await member.openSubscription(lane, () => {}).unsubscribe()
    hooks.beforeWithdraw = () => {
      withdrawing.resolve()
      return withdrawal.promise
    }
    setup.resolve()
    await withdrawing.promise
    const replacement = member.openSubscription(lane, () => {})
    withdrawal.resolve()
    await untilReady(replacement)
    await flushMicrotasks()
    expect(liveMembers(authority, lane)).toEqual({ weur: ['member-weur-0'] })
    await replacement.unsubscribe()
  })

  it('surfaces presence refresh loss and recovery through subscription state', async () => {
    vi.useFakeTimers()
    let presenceCalls = 0
    const transport = createTransport(
      createBasicBinding({
        onPresence: () => {
          presenceCalls += 1
          return presenceCalls === 2 ? Promise.reject(new Error('presence refresh rejected')) : Promise.resolve()
        },
      }),
    )
    const member = createMember(transport)
    const subscription = member.openSubscription({ key: 'room:refresh', kind: 'text' }, () => {})
    await untilReady(subscription)
    const states: string[] = []
    const stopObserving = subscription.onStateChange((state) => states.push(state))
    try {
      await vi.advanceTimersByTimeAsync(30_000)
      expect(subscription.state()).toBe('lost')
      expect(states).toEqual(['lost'])
      await vi.advanceTimersByTimeAsync(30_000)
      expect(subscription.state()).toBe('ready')
      expect(states).toEqual(['lost', 'ready'])
    } finally {
      stopObserving()
      await subscription.unsubscribe()
      vi.useRealTimers()
    }
  })
})
