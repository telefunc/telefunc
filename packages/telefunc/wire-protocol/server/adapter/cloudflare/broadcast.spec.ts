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
import { encodePublishText, type WirePublishInfo } from '../../../shared-ws.js'
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { ServerBroadcast } from '../../server-broadcast.js'
import { getBroadcastAdapter, _resetBroadcastAdapterForTesting } from '../../broadcast.js'

type CloudflareRequest = Request & { cf?: { colo?: string; continent?: string } }

function createCloudflareRequest({ colo, continent }: { colo?: string; continent?: string } = {}): CloudflareRequest {
  const request = new Request('https://telefunc.test') as CloudflareRequest
  request.cf = { colo, continent }
  return request
}

function createAuthorityState(kv?: KVNamespace) {
  const stored = new Map<string, unknown>()
  let alarm: number | null = null
  const state = {
    storage: {
      async get<T>(key: string) {
        return stored.get(key) as T | undefined
      },
      async put(key: string, value: unknown) {
        stored.set(key, value)
      },
      async delete(key: string) {
        stored.delete(key)
      },
      async list<T>({ prefix }: { prefix: string }) {
        const entries = new Map<string, T>()
        for (const [key, value] of stored) {
          if (!key.startsWith(prefix)) continue
          entries.set(key, value as T)
        }
        return entries
      },
      async getAlarm() {
        return alarm
      },
      async setAlarm(time: number) {
        alarm = time
      },
      async deleteAlarm() {
        alarm = null
      },
    },
  } as unknown as DurableObjectState
  return new CloudflareBroadcastAuthorityState(state, kv)
}

function createMockKV(): KVNamespace {
  const store = new Map<string, { value: string; expirationTtl?: number }>()
  return {
    async get(key: string) {
      return store.get(key)?.value ?? null
    },
    async put(key: string, value: string, options?: { expirationTtl?: number }) {
      store.set(key, { value, expirationTtl: options?.expirationTtl })
    },
    _ttl(key: string) {
      return store.get(key)?.expirationTtl
    },
    async delete(key: string) {
      store.delete(key)
    },
    async list({ prefix, cursor }: { prefix?: string; cursor?: string }) {
      void cursor
      const keys: Array<{ name: string; expiration?: number }> = []
      for (const name of store.keys()) {
        if (prefix && !name.startsWith(prefix)) continue
        keys.push({ name })
      }
      return { keys, list_complete: true, cursor: '' }
    },
  } as unknown as KVNamespace
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
    onDeliver: (id: { name: string }, request: any) => any
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
    get(id: { name: string }) {
      return {
        telefuncBroadcastPublish(request: any) {
          return overrides?.onPublish?.(id, request) ?? Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
        telefuncBroadcastDeliver(request: any) {
          return overrides?.onDeliver?.(id, request) ?? Promise.resolve()
        },
      }
    },
  } as unknown as DurableObjectNamespace
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

  it('writes KV presence on subscribe and reads it during publish fanout', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    const previousTransport = getBroadcastAdapter()

    transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
    transport.attachKV(kv)
    transport.attachIsolateInfo('telefunc-shard-weur-0', 'weur')
    _resetBroadcastAdapterForTesting(transport)

    transport.subscribe('room:test', () => {})
    await flushMicrotasks()

    // KV should have a presence record with the representative DO name as value
    const value = await kv.get(`tfps:${encodeURIComponent('room:test')}:weur:telefunc-shard-weur-0`)
    expect(value).toBe('telefunc-shard-weur-0')

    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('keeps the first-touch authority bucket in publish receipts', async () => {
    const authorityState = createAuthorityState()
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()

    await authorityState.getOrInitAuthorityBucket('room:first-touch', 'weur')

    // Set up KV presence for two buckets
    await kv.put(`tfps:${encodeURIComponent('room:first-touch')}:weur:telefunc-shard-weur-0`, 'telefunc-shard-weur-0', {
      expirationTtl: 90,
    })
    await kv.put(`tfps:${encodeURIComponent('room:first-touch')}:apac:telefunc-shard-apac-0`, 'telefunc-shard-apac-0', {
      expirationTtl: 90,
    })

    transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
    transport.attachKV(kv)

    const receipt = await transport.publishToSubscribers(authorityState, {
      key: 'room:first-touch',
      locationBucket: 'apac',
      serialized: '{"text":"hello"}',
      forwarded: false,
    })

    expect(receipt).toMatchObject({
      seq: 1,
      meta: {
        authorityBucket: 'weur',
        fanoutBuckets: ['weur', 'apac'],
      },
    })
    expect(receipt.timestamp).toEqual(expect.any(Number))
  })

  it('waits for KV presence setup before authority publish fanout', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    const previousTransport = getBroadcastAdapter()
    const publishTargets: string[] = []
    let releaseKVPut: (() => void) | null = null
    const kvPutReady = new Promise<void>((resolve) => {
      releaseKVPut = resolve
    })

    // Intercept KV put to control timing
    const originalPut = kv.put.bind(kv)
    kv.put = (async (key: string, value: string, options?: any) => {
      await kvPutReady
      return originalPut(key, value, options)
    }) as any

    transport.attachBinding(
      createBasicBinding({
        onPublish(id, request) {
          publishTargets.push(id.name)
          return Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
      }),
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)
    transport.attachIsolateInfo('telefunc-shard-weur-0', 'weur')
    _resetBroadcastAdapterForTesting(transport)

    const room = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
    // subscribe() triggers KV presence setup — publish should wait for it
    room.subscribe(() => {})
    room.publish({ text: 'hello' })

    await flushMicrotasks(2)
    expect(publishTargets).toEqual([])

    releaseKVPut!()
    await flushCoordinatorTurn()

    expect(publishTargets).toEqual(['telefunc:broadcast:authority:room:test'])

    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('does not deliver locally before ordered publish setup completes', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    const received: string[] = []
    const previousTransport = getBroadcastAdapter()
    let releaseKVPut: (() => void) | null = null
    const kvPutReady = new Promise<void>((resolve) => {
      releaseKVPut = resolve
    })

    const originalPut = kv.put.bind(kv)
    kv.put = (async (key: string, value: string, options?: any) => {
      await kvPutReady
      return originalPut(key, value, options)
    }) as any

    const localRegistry = (() => {
      // Access the local registry after attachIsolateInfo sets it up
      transport.attachIsolateInfo('telefunc-shard-weur-0', 'weur')
      // We need access to the local registry for the mock binding deliver path.
      // Use deliverToLocal which reads from the internal registry.
      return null
    })()
    void localRegistry

    transport.attachBinding(
      createBasicBinding({
        onPublish(id, request) {
          return transport.publishToSubscribers(createAuthorityState(), {
            ...request,
            locationBucket: request.locationBucket,
          })
        },
        onDeliver(id, request) {
          transport.deliverToLocal(request)
          return Promise.resolve()
        },
      }),
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)
    _resetBroadcastAdapterForTesting(transport)

    const subscriber = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
    subscriber.subscribe((message) => {
      received.push(message.text)
    })
    const publisher = new ServerBroadcast<{ text: string }>({ key: 'room:test' })

    publisher.publish({ text: 'hello' })
    await flushMicrotasks(2)
    expect(received).toEqual([])

    releaseKVPut!()
    await flushCoordinatorTurn()

    expect(received).toEqual(['hello'])

    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('resolves publish ack with authority metadata after cold-path setup completes', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    const previousTransport = getBroadcastAdapter()
    let releaseKVPut: (() => void) | null = null
    const kvPutReady = new Promise<void>((resolve) => {
      releaseKVPut = resolve
    })

    const originalPut = kv.put.bind(kv)
    kv.put = (async (key: string, value: string, options?: any) => {
      await kvPutReady
      return originalPut(key, value, options)
    }) as any

    transport.attachIsolateInfo('telefunc-shard-weur-0', 'weur')
    transport.attachBinding(
      createBasicBinding({
        onPublish(id, request) {
          return transport.publishToSubscribers(createAuthorityState(), {
            ...request,
            locationBucket: request.locationBucket,
          })
        },
        onDeliver(id, request) {
          transport.deliverToLocal(request)
          return Promise.resolve()
        },
      }),
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)
    _resetBroadcastAdapterForTesting(transport)

    const subscriber = new ServerBroadcast<{ text: string }>({ key: 'room:test:ack' })
    subscriber.subscribe(() => undefined)
    const publisher = new ServerBroadcast<{ text: string }>({ key: 'room:test:ack' })
    const receiptPromise = publisher.publish({ text: 'hello' })

    await flushMicrotasks(2)
    releaseKVPut!()

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

    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('authority forwards once to each populated bucket coordinator', async () => {
    const authorityState = createAuthorityState()
    const kv = createMockKV()
    const forwardedBuckets: string[] = []
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })

    transport.attachBinding(
      createBasicBinding({
        onPublish(id, { locationBucket }) {
          forwardedBuckets.push(locationBucket)
          return Promise.resolve()
        },
      }),
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)

    // Set up KV presence for three buckets
    await kv.put(`tfps:${encodeURIComponent('room:test')}:weur:telefunc-shard-weur-0`, 'telefunc-shard-weur-0', {
      expirationTtl: 90,
    })
    await kv.put(`tfps:${encodeURIComponent('room:test')}:apac:telefunc-shard-apac-0`, 'telefunc-shard-apac-0', {
      expirationTtl: 90,
    })
    await kv.put(`tfps:${encodeURIComponent('room:test')}:eeur:telefunc-shard-eeur-0`, 'telefunc-shard-eeur-0', {
      expirationTtl: 90,
    })

    await transport.publishToSubscribers(authorityState, {
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"hello"}',
      forwarded: false,
    })

    expect(forwardedBuckets.sort()).toEqual(['apac', 'eeur', 'weur'])
  })

  it('forwarded publish delivers to DO names listed in the request', async () => {
    const authorityState = createAuthorityState()
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const deliveredTo: string[] = []

    transport.attachBinding(
      createBasicBinding({
        onDeliver(id) {
          deliveredTo.push(id.name)
          return Promise.resolve()
        },
      }),
      'TelefuncDurableObject',
    )

    await transport.publishToSubscribers(authorityState, {
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"hello"}',
      forwarded: true,
      doNames: ['telefunc-shard-weur-0', 'telefunc-shard-weur-1'],
      info: { seq: 1, timestamp: Date.now() },
    })

    expect(deliveredTo.sort()).toEqual(['telefunc-shard-weur-0', 'telefunc-shard-weur-1'])
  })

  it('can publish without request context — uses isolate state directly', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    const coordinatorPublishes: Array<{ name: string; key: string; locationBucket: string; serialized: string }> = []
    const previousTransport = getBroadcastAdapter()

    transport.attachBinding(
      createBasicBinding({
        onPublish(id, { key, locationBucket, serialized }) {
          coordinatorPublishes.push({ name: id.name, key, locationBucket, serialized })
          return Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
      }),
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)
    transport.attachIsolateInfo('telefunc-shard-weur-0', 'weur')
    _resetBroadcastAdapterForTesting(transport)

    // No request context needed — isolate state provides locationBucket
    const room = new ServerBroadcast<{ text: string }>({ key: 'room:test:no-ctx' })

    expect(() => room.publish({ text: 'hello' })).not.toThrow()

    await flushCoordinatorTurn()

    expect(coordinatorPublishes).toEqual([
      {
        name: expect.stringContaining(':broadcast:'),
        key: 'room:test:no-ctx',
        locationBucket: expect.any(String),
        serialized: '{"text":"hello"}',
      },
    ])

    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('asserts when isolate info is not attached before subscribe', () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    const previousTransport = getBroadcastAdapter()

    transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
    transport.attachKV(kv)
    _resetBroadcastAdapterForTesting(transport)

    expect(() => transport.subscribe('room:test', () => {})).toThrow('attachIsolateInfo()')

    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('serializes authority dispatch without blocking later publishes on remote delivery completion', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const authorityState = createAuthorityState()
    const kv = createMockKV()
    const coordinatorPublishes: string[] = []
    let releaseFirstRemotePublish: (() => void) | null = null
    const firstRemotePublishReady = new Promise<void>((resolve) => {
      releaseFirstRemotePublish = resolve
    })

    transport.attachBinding(
      {
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
            telefuncBroadcastPublish({ serialized }: any) {
              coordinatorPublishes.push(`${id.name}:${serialized}`)
              if (id.name.includes(':broadcast:apac:') && serialized === '{"text":"first"}')
                return firstRemotePublishReady
              return Promise.resolve()
            },
            telefuncBroadcastDeliver() {
              return Promise.resolve()
            },
          }
        },
      } as unknown as DurableObjectNamespace,
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)

    // Set up KV presence for two buckets
    await kv.put(`tfps:${encodeURIComponent('room:test')}:weur:telefunc-shard-weur-0`, 'telefunc-shard-weur-0', {
      expirationTtl: 90,
    })
    await kv.put(`tfps:${encodeURIComponent('room:test')}:apac:telefunc-shard-apac-0`, 'telefunc-shard-apac-0', {
      expirationTtl: 90,
    })

    const firstPublish = transport.publishToSubscribers(authorityState, {
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"first"}',
      forwarded: false,
    })
    await flushMicrotasks(8)

    const secondPublish = transport.publishToSubscribers(authorityState, {
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"second"}',
      forwarded: false,
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

  it('deletes KV presence on unsubscribe', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    const previousTransport = getBroadcastAdapter()

    transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
    transport.attachKV(kv)
    transport.attachIsolateInfo('telefunc-shard-weur-0', 'weur')
    _resetBroadcastAdapterForTesting(transport)

    const unsub = transport.subscribe('room:test', () => {})
    await flushMicrotasks()

    const presenceKey = `tfps:${encodeURIComponent('room:test')}:weur:telefunc-shard-weur-0`
    expect(await kv.get(presenceKey)).toBe('telefunc-shard-weur-0')

    unsub()
    await flushMicrotasks()

    expect(await kv.get(presenceKey)).toBeNull()

    _resetBroadcastAdapterForTesting(previousTransport)
  })
})

describe('cloudflare KV store (backs `Room` state)', () => {
  function createTransportWithKV() {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    transport.attachKV(kv)
    return { transport, kv }
  }

  it('round-trips values under the `tfkv:` namespace and strips it from keys()', async () => {
    const { transport, kv } = createTransportWithKV()

    expect(await transport.get('telefunc:room:lobby:config')).toBeNull()
    await transport.set('telefunc:room:lobby:config', '{"a":1}')
    expect(await transport.get('telefunc:room:lobby:config')).toBe('{"a":1}')
    expect(await kv.get('tfkv:telefunc:room:lobby:config')).toBe('{"a":1}') // namespaced in Workers KV

    await transport.set('telefunc:room:lobby:m:x', '{}')
    expect((await transport.keys('telefunc:room:')).sort()).toEqual([
      'telefunc:room:lobby:config',
      'telefunc:room:lobby:m:x',
    ])

    await transport.delete('telefunc:room:lobby:config')
    expect(await transport.get('telefunc:room:lobby:config')).toBeNull()
  })

  it('passes TTLs through as expirationTtl, rounding up to the 60s Workers KV floor', async () => {
    const { transport, kv } = createTransportWithKV()
    const ttlOf = (key: string) => (kv as unknown as { _ttl(k: string): number | undefined })._ttl(key)

    await transport.set('telefunc:room:r:m:x', '{}', { ttlMs: 180_000 })
    expect(ttlOf('tfkv:telefunc:room:r:m:x')).toBe(180)

    await transport.set('telefunc:room:r:m:y', '{}', { ttlMs: 1_500 }) // below the floor: up, never down
    expect(ttlOf('tfkv:telefunc:room:r:m:y')).toBe(60)

    await transport.set('telefunc:room:r:config', '{}') // no TTL — config records persist
    expect(ttlOf('tfkv:telefunc:room:r:config')).toBe(undefined)
  })

  it('does not leak broadcast presence records into the KV keyspace', async () => {
    const { transport } = createTransportWithKV()
    transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
    transport.attachIsolateInfo('telefunc-shard-weur-0', 'weur')

    transport.subscribe('telefunc:room:lobby', () => {})
    await flushMicrotasks()
    await transport.set('telefunc:room:lobby:config', '{}')

    expect(await transport.keys('')).toEqual(['telefunc:room:lobby:config'])
  })
})

describe('cloudflare room-state routing (hybrid tiers)', () => {
  // A binding whose authority stub records each room-state RPC, so a test can prove which tier — the
  // Workers KV replica or the authority Durable Object — a given transport op actually hit.
  function createRoomStateBinding(calls: string[], store = new Map<string, string>()) {
    const stub = {
      async telefuncRoomStateGet(key: string) {
        calls.push(`get:${key}`)
        return store.get(key) ?? null
      },
      async telefuncRoomStateKeys(prefix: string) {
        calls.push(`keys:${prefix}`)
        return [...store.keys()].filter((key) => key.startsWith(prefix))
      },
      async telefuncRoomStateSet(key: string, value: string, _ttlMs?: number, replicate?: boolean) {
        calls.push(`set:${key}:replicate=${replicate}`)
        store.set(key, value)
      },
      async telefuncRoomStateDelete(key: string, replicate?: boolean) {
        calls.push(`delete:${key}:replicate=${replicate}`)
        store.delete(key)
      },
      async telefuncRoomStateSetIfAbsent(key: string, value: string) {
        calls.push(`setIfAbsent:${key}`)
        if (store.has(key)) return false
        store.set(key, value)
        return true
      },
      async telefuncRoomStateCompareAndSet(
        key: string,
        expected: string | null,
        next: string | null,
        _ttlMs?: number,
        replicate?: boolean,
      ) {
        calls.push(`cas:${key}:replicate=${replicate}`)
        if ((store.get(key) ?? null) !== expected) return false
        if (next === null) store.delete(key)
        else store.set(key, next)
        return true
      },
    }
    return {
      idFromName: (name: string) => ({ name, equals: (other: { name: string }) => other.name === name }),
      get: () => stub,
    } as unknown as DurableObjectNamespace
  }

  function setup() {
    const calls: string[] = []
    const kv = createMockKV()
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    transport.attachBinding(createRoomStateBinding(calls), 'TelefuncDurableObject')
    transport.attachKV(kv)
    transport.attachIsolateInfo('telefunc-shard-weur-0', 'weur')
    return { transport, calls, kv }
  }

  it('serves directory reads from the KV replica, never the authority', async () => {
    const { transport, calls, kv } = setup()
    await kv.put('tfkv:room:a:config', '{"x":1}')
    await kv.put('tfkv:room:a:m:1', '{}')

    expect(await transport.get('room:a:config', { partitionKey: 'p' })).toBe('{"x":1}')
    expect((await transport.keys('room:a:', { partitionKey: 'p' })).sort()).toEqual(['room:a:config', 'room:a:m:1'])
    expect(calls).toEqual([]) // the authority was never consulted
  })

  it('sends consistent reads to the authority, bypassing the replica', async () => {
    const { transport, calls } = setup()
    await transport.get('room:a:rt', { partitionKey: 'p', consistent: true })
    await transport.keys('room:a:rt:', { partitionKey: 'p', consistent: true })
    expect(calls).toEqual(['get:room:a:rt', 'keys:room:a:rt:'])
  })

  it('writes through the authority — replicated by default, authority-only when consistent', async () => {
    const { transport, calls } = setup()
    await transport.set('room:a:m:1', '{}', { partitionKey: 'p' }) // directory → mirrored
    await transport.set('room:a:rt', 'frame', { partitionKey: 'p', consistent: true }) // retained → authority-only
    await transport.delete('room:a:m:1', { partitionKey: 'p' })
    await transport.delete('room:a:rt', { partitionKey: 'p', consistent: true })
    expect(calls).toEqual([
      'set:room:a:m:1:replicate=true',
      'set:room:a:rt:replicate=false',
      'delete:room:a:m:1:replicate=true',
      'delete:room:a:rt:replicate=false',
    ])
  })

  it('runs the atomic writes (setIfAbsent, update) against the authority', async () => {
    const { transport, calls } = setup()
    expect(await transport.setIfAbsent('room:a:config', 'v', { partitionKey: 'p' })).toBe(true)
    expect(await transport.update('room:a:config', () => 'v2', { partitionKey: 'p' })).toBe('v2')
    // Directory `update()` replicates its CAS to the KV read replica (default).
    expect(calls).toEqual(['setIfAbsent:room:a:config', 'get:room:a:config', 'cas:room:a:config:replicate=true'])
  })

  it('keeps an authority-only compare-and-set off the replica when consistent', async () => {
    const { transport, calls } = setup()
    // The `:o` order watermark is written `consistent` on every publish — its CAS must not replicate,
    // or every semantic publish mirrors a dead-weight write to Workers KV (and risks its write ceiling).
    await transport.update('room:a:o', () => '{"seq":1,"timestamp":1}', { partitionKey: 'p', consistent: true })
    expect(calls).toEqual(['get:room:a:o', 'cas:room:a:o:replicate=false'])
  })

  it('keeps the unpartitioned room index on the replica alone', async () => {
    const { transport, calls } = setup()
    await transport.set('idx:a', '')
    expect(await transport.get('idx:a')).toBe('')
    expect(await transport.keys('idx:')).toEqual(['idx:a'])
    await transport.delete('idx:a')
    expect(await transport.get('idx:a')).toBeNull()
    expect(calls).toEqual([]) // the index has no authority
  })
})

describe('cloudflare commitFrame (atomic assign-order + retain + publish)', () => {
  // NOTE: exercises the authority half (`commitFrameOnAuthority`) against the in-memory fake DO state +
  // KV harness — the same fakes `roomState.spec.ts` uses. Real Durable Object certification is pending
  // (P7); there's no live-DO Room-over-Cloudflare test yet, so this stays at the adapter level.
  afterEach(() => vi.useRealTimers())

  // A binding whose authority stub records every frame the ordered fanout forwards, so a test can read
  // back the exact (seq,timestamp) each receiver would see.
  function setup() {
    const forwarded: Array<{ serialized?: string; info?: WirePublishInfo }> = []
    const kv = createMockKV()
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    transport.attachBinding(
      createBasicBinding({
        onPublish(_id, request) {
          forwarded.push({ serialized: request.serialized, info: request.info })
          return Promise.resolve()
        },
      }),
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)
    return { transport, authorityState: createAuthorityState(kv), kv, forwarded }
  }

  // Room keys: the partition (control lane) home, the order watermark, the text channel, the retained key.
  const base = {
    partitionKey: 'telefunc:room:r',
    orderKey: 'telefunc:room:r:o',
    channelKey: 'telefunc:room:r:t',
    orderTtlMs: 3_600_000,
  }

  it('assigns the room order, retains the framed bytes, and fans out carrying that order', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(1_000)
    const { transport, authorityState, kv, forwarded } = setup()
    await kv.put(`tfps:${encodeURIComponent(base.channelKey)}:weur:telefunc-shard-weur-0`, 'telefunc-shard-weur-0', {
      expirationTtl: 90,
    })

    const result = await transport.commitFrameOnAuthority(authorityState, {
      ...base,
      fences: [],
      payload: '{"text":"hi"}',
      retainKey: 'telefunc:room:r:rt',
    })

    expect(result).toEqual({ ok: true, seq: 1, timestamp: 1_000, receivers: 1 })
    // The order watermark is persisted (authority-only — never mirrored to the KV replica).
    expect(await authorityState.roomStateGet(base.orderKey)).toBe('{"seq":1,"timestamp":1000}')
    expect(await kv.get(`tfkv:${base.orderKey}`)).toBeNull()
    // Retained bytes are self-framed with the committed order — byte-for-byte what the room decodes back.
    expect(await authorityState.roomStateGet('telefunc:room:r:rt')).toBe(
      encodePublishText('{"text":"hi"}', { seq: 1, timestamp: 1_000 }),
    )
    expect(await kv.get('tfkv:telefunc:room:r:rt')).toBeNull()
    // The fanout carried the committed (seq,timestamp) — not a fresh per-key seq.
    expect(forwarded).toEqual([{ serialized: '{"text":"hi"}', info: { seq: 1, timestamp: 1_000 } }])
  })

  it('clamps the monotonic clock across commits — increment at one instant, reset when time advances', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(5_000)
    const { transport, authorityState } = setup()
    const commit = () => transport.commitFrameOnAuthority(authorityState, { ...base, fences: [], payload: 'x' })

    expect(await commit()).toMatchObject({ ok: true, seq: 1, timestamp: 5_000 })
    expect(await commit()).toMatchObject({ ok: true, seq: 2, timestamp: 5_000 }) // same instant → clamp + ++seq
    vi.setSystemTime(5_010)
    expect(await commit()).toMatchObject({ ok: true, seq: 1, timestamp: 5_010 }) // time advanced → seq resets
  })

  it('refuses on a stale fence — nothing is ordered, retained, or published', async () => {
    const { transport, authorityState, forwarded } = setup()
    await authorityState.roomStateSet('telefunc:room:r:gen', 'g2')

    const result = await transport.commitFrameOnAuthority(authorityState, {
      ...base,
      fences: [{ key: 'telefunc:room:r:gen', expected: 'g1' }], // stored is 'g2' → stale
      payload: 'x',
      retainKey: 'telefunc:room:r:rt',
    })

    expect(result).toEqual({ ok: false, reason: 'stale-fence' })
    expect(await authorityState.roomStateGet(base.orderKey)).toBeNull() // clock untouched
    expect(await authorityState.roomStateGet('telefunc:room:r:rt')).toBeNull() // nothing retained
    expect(forwarded).toEqual([]) // nothing published
  })

  it('commits when every fence still holds', async () => {
    const { transport, authorityState } = setup()
    await authorityState.roomStateSet('telefunc:room:r:gen', 'g1')
    const result = await transport.commitFrameOnAuthority(authorityState, {
      ...base,
      fences: [{ key: 'telefunc:room:r:gen', expected: 'g1' }],
      payload: 'x',
    })
    expect(result).toMatchObject({ ok: true, seq: 1 })
  })
})
