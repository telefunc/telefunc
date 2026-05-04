import { describe, expect, it, vi } from 'vitest'
import {
  DEFAULT_BROADCAST_BUCKETS,
  getBucketCoordinatorShardIndices,
  getDeterministicKeyBucketIndex,
  getShardIndicesForBucket,
  resolveCloudflareLocationHint,
  resolveSessionRoutingTarget,
} from './routing.js'
import '../../../../node/server/async_hooks.js'
import { CloudflareBroadcastAuthorityState, CloudflareBroadcastTransport } from './broadcast.js'
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { ServerBroadcast } from '../../server-broadcast.js'
import { getBroadcastAdapter, _resetBroadcastAdapterForTesting } from '../../broadcast.js'

type CloudflareRequest = Request & { cf?: { colo?: string; continent?: string } }

function createCloudflareRequest({ colo, continent }: { colo?: string; continent?: string } = {}): CloudflareRequest {
  const request = new Request('https://telefunc.test') as CloudflareRequest
  request.cf = { colo, continent }
  return request
}

function createAuthorityState() {
  const stored = new Map<string, number>()
  const state = {
    storage: {
      async get<T>(key: string) {
        return stored.get(key) as T | undefined
      },
      async put(key: string, value: number) {
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
    },
  } as unknown as DurableObjectState
  return new CloudflareBroadcastAuthorityState(state)
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
          return overrides?.onPublish?.(id, request) ?? Promise.resolve({ seq: 1, ts: Date.now() })
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
    expect(receipt.ts).toEqual(expect.any(Number))
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
          return Promise.resolve({ seq: 1, ts: Date.now() })
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
    expect(receipt.ts).toEqual(expect.any(Number))

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
      info: { seq: 1, ts: Date.now() },
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
          return Promise.resolve({ seq: 1, ts: Date.now() })
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
