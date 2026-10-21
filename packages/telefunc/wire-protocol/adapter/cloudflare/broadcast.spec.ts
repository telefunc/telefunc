import { describe, expect, it } from 'vitest'
import {
  DEFAULT_BROADCAST_BUCKETS,
  assertLocationFallbackIsScaled,
  getBucketCoordinatorShardIndices,
  getDeterministicKeyBucketIndex,
  resolveCloudflareLocationHint,
  resolveSessionRoutingTarget,
} from './routing.js'
import { CloudflareBroadcastAuthorityState, CloudflareBroadcast } from './broadcast.js'
import type { BroadcastCalls } from './broadcast.js'
import { OrderedStubs } from './OrderedStubs.js'
import type { TelefuncDurableObjectNamespace } from './namespace.js'
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { sqliteStorage } from '../../test/cloudflareSql.js'

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
          return overrides?.onForward?.(id, request) ?? Promise.resolve()
        },
        telefuncBroadcastDeliver(request: any) {
          return overrides?.onDeliver?.(id, request) ?? Promise.resolve()
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
    namespace: () => binding,
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

  it('partitions bucket coordinators at ceil(sessionScale / 2) per bucket', () => {
    expect(getBucketCoordinatorShardIndices(1, 'wnam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices(2, 'wnam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices(3, 'wnam')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices(4, 'wnam')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices(4, 'enam')).toEqual([0, 1])
  })

  it('uses scale maps to control the bucket-coordinator shard subsets', () => {
    expect(getBucketCoordinatorShardIndices({ weur: 2, enam: 1 }, 'enam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 2, enam: 1 }, 'weur')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 3, enam: 1 }, 'weur')).toEqual([0, 1])
  })

  it('uses only canonical bucket scale entries', () => {
    expect(getBucketCoordinatorShardIndices({ weur: 2, apac: 1 }, 'apac')).toEqual([0])
  })

  it('resolves session targets from request location and scale', () => {
    const exactRequest = createCloudflareRequest({ colo: 'LHR' })
    const unknownRequest = createCloudflareRequest({ continent: 'EU' })
    const exactTarget = resolveSessionRoutingTarget('telefunc', { weur: 2, apac: 1 }, exactRequest, 'weur', 'token')
    const fallbackTarget = resolveSessionRoutingTarget(
      'telefunc',
      { weur: 1, apac: 1 },
      unknownRequest,
      'weur',
      'token',
    )

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
    const target = resolveSessionRoutingTarget('telefunc', { weur: 2, apac: 1 }, wnamRequest, 'weur', 'token')

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

  it('authority forwards once to each populated bucket coordinator', async () => {
    const authorityState = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinators: string[] = []
    const broadcast: CloudflareBroadcast = createBroadcast(
      createBasicBinding({
        onForward(id) {
          coordinators.push(id.name)
          return Promise.resolve()
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
      key: 'room:test',
      kind: 'text',
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
    const firstRemotePublish = Promise.withResolvers<void>()
    const broadcast = createBroadcast(
      createBasicBinding({
        onForward(id, { payload: text }) {
          coordinatorPublishes.push(`${id.name}:${text}`)
          if (id.name.includes(':broadcast:apac:') && text === '{"text":"first"}') return firstRemotePublish.promise
          return Promise.resolve()
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

    firstRemotePublish.resolve()
    await Promise.all([firstPublish, secondPublish])

    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"second"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"second"}')
  })
})
