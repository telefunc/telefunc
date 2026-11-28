import { AsyncLocalStorage } from 'node:async_hooks'
import { describe, expect, it, vi } from 'vitest'

vi.mock('cloudflare:workers', () => ({
  DurableObject: class {
    protected readonly ctx: DurableObjectState
    protected readonly env: Cloudflare.Env
    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      this.ctx = ctx
      this.env = env
    }
  },
}))
// The test's Durable Objects take no WebSocket.
vi.mock('crossws/adapters/cloudflare', () => ({ default: () => ({ handleDurableInit() {} }) }))
import {
  DEFAULT_BROADCAST_BUCKETS,
  assertLocationFallbackIsScaled,
  getBucketCoordinatorScale,
  getDeterministicKeyBucketIndex,
  resolveCloudflareLocationHint,
  resolveSessionRoutingTarget,
} from './routing.js'
import { CloudflareBroadcastAuthorityState, CloudflareBroadcastTransport } from './broadcast.js'
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { Broadcast, ServerBroadcast } from '../../channel/server/ServerBroadcast.js'
import { getBroadcastAdapter, _resetBroadcastAdapterForTesting } from '../../broadcast/broadcastAdapter.js'
import { getChannelMux } from '../../channel/server/ChannelMux.js'
import { Telefunc } from '../../../serve/cloudflare.js'
import type { BroadcastDeliverRequest } from './broadcast.js'
import type { CloudflareScale } from './routing.js'
import { ATTACHABLE_NOW } from '../../channel/server/ServerChannel.js'

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

/** `writes`, where given, gets each put and delete in order. */
function createMockKV(writes?: string[]): KVNamespace {
  const store = new Map<string, { value: string; expirationTtl?: number }>()
  return {
    async get(key: string) {
      return store.get(key)?.value ?? null
    },
    async put(key: string, value: string, options?: { expirationTtl?: number }) {
      writes?.push(`put ${key}`)
      store.set(key, { value, expirationTtl: options?.expirationTtl })
    },
    async delete(key: string) {
      writes?.push(`delete ${key}`)
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

/** A transport on a Worker with AsyncLocalStorage, so that `runInObject` names the Durable Object. */
function createTransport(scale: CloudflareScale): CloudflareBroadcastTransport {
  return new CloudflareBroadcastTransport({
    baseInstanceName: 'telefunc',
    scale,
    runningObject: new AsyncLocalStorage(),
  })
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

  it("writes the subscribing Durable Object's KV presence on subscribe", async () => {
    const transport = createTransport(1)
    const kv = createMockKV()
    const previousTransport = getBroadcastAdapter()

    transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
    transport.attachKV(kv)
    transport.attachIsolateInfo('weur')
    _resetBroadcastAdapterForTesting(transport)

    transport.runInObject('telefunc-shard-weur-0', () => transport.subscribe('room:test', () => {}))
    await flushMicrotasks()

    // KV should have a presence record with the subscribing DO's name as value
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
    const transport = createTransport(1)
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
        onPublish(id) {
          publishTargets.push(id.name)
          return Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
      }),
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)
    transport.attachIsolateInfo('weur')
    _resetBroadcastAdapterForTesting(transport)

    const room = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
    // subscribe() triggers KV presence setup — publish should wait for it
    transport.runInObject('telefunc-shard-weur-0', () => room.subscribe(() => {}))
    room.publish({ text: 'hello' })

    await flushMicrotasks(2)
    expect(publishTargets).toEqual([])

    releaseKVPut!()
    await flushCoordinatorTurn()

    expect(publishTargets).toEqual(['telefunc:broadcast:authority:room:test'])

    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('does not deliver locally before ordered publish setup completes', async () => {
    const transport = createTransport(1)
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

    transport.attachIsolateInfo('weur')

    transport.attachBinding(
      createBasicBinding({
        onPublish(_id, request) {
          return transport.publishToSubscribers(createAuthorityState(), {
            ...request,
            locationBucket: request.locationBucket,
          })
        },
        onDeliver(_id, request) {
          transport.deliverToLocal(request)
          return Promise.resolve()
        },
      }),
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)
    _resetBroadcastAdapterForTesting(transport)

    const subscriber = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
    transport.runInObject('telefunc-shard-weur-0', () =>
      subscriber.subscribe((message) => {
        received.push(message.text)
      }),
    )
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
    const transport = createTransport(1)
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

    transport.attachIsolateInfo('weur')
    transport.attachBinding(
      createBasicBinding({
        onPublish(_id, request) {
          return transport.publishToSubscribers(createAuthorityState(), {
            ...request,
            locationBucket: request.locationBucket,
          })
        },
        onDeliver(_id, request) {
          transport.deliverToLocal(request)
          return Promise.resolve()
        },
      }),
      'TelefuncDurableObject',
    )
    transport.attachKV(kv)
    _resetBroadcastAdapterForTesting(transport)

    const subscriber = new ServerBroadcast<{ text: string }>({ key: 'room:test:ack' })
    transport.runInObject('telefunc-shard-weur-0', () => subscriber.subscribe(() => undefined))
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
        onPublish(_id, { locationBucket }) {
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
    transport.attachIsolateInfo('weur')
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

    // Each coordinator has both, in publish order, while the first's remote delivery is still under way.
    const publishesTo = (target: string) =>
      coordinatorPublishes.filter((entry) => entry.startsWith(target)).map((entry) => entry.slice(target.length))
    expect(publishesTo('telefunc:broadcast:weur:0:')).toEqual(['{"text":"first"}', '{"text":"second"}'])
    expect(publishesTo('telefunc:broadcast:apac:0:')).toEqual(['{"text":"first"}', '{"text":"second"}'])

    releaseFirstRemotePublish!()
    await Promise.all([firstPublish, secondPublish])
  })

  // Its publishes reach this Durable Object only where another one's publish finds its presence in KV.
  it("a subscriber's `opened` resolves once KV wrote its Durable Object's presence", async () => {
    const transport = createTransport(1)
    const previousTransport = getBroadcastAdapter()
    let wrote = () => {}
    const kv = createMockKV()
    const put = kv.put.bind(kv)
    kv.put = async (...args: Parameters<KVNamespace['put']>) => {
      await new Promise<void>((resolve) => (wrote = resolve))
      return put(...args)
    }
    transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
    transport.attachKV(kv)
    transport.attachIsolateInfo('weur')
    _resetBroadcastAdapterForTesting(transport)

    const remove = transport.runInObject('telefunc-shard-weur-0', () => Broadcast.subscribe('room:test', () => {}))
    let opened = false
    void remove.opened.then(() => (opened = true))
    await flushCoordinatorTurn()
    expect(opened).toBe(false)
    wrote()
    await flushCoordinatorTurn()
    expect(opened).toBe(true)

    remove()
    _resetBroadcastAdapterForTesting(previousTransport)
  })

  // A presence KV refuses to write is a subscription that failed, as a refused one is on any other backend.
  describe('where KV refuses the presence write', () => {
    const refusing = () => {
      const transport = createTransport(1)
      const kv = createMockKV()
      const put = kv.put.bind(kv)
      const remove = kv.delete.bind(kv)
      /** `held`: where set, KV acknowledges the writes and deletes asked of it meanwhile once it settles. */
      const state: { refuse: boolean; held?: Promise<void> } = { refuse: true }
      kv.put = async (...args: Parameters<KVNamespace['put']>) => {
        const { refuse, held } = state
        await held
        if (refuse) throw new Error('KV PUT failed: 429 Too Many Requests')
        return put(...args)
      }
      kv.delete = async (...args: Parameters<KVNamespace['delete']>) => {
        const { held } = state
        await held
        return remove(...args)
      }
      transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
      transport.attachKV(kv)
      transport.attachIsolateInfo('weur')
      _resetBroadcastAdapterForTesting(transport)
      const inObject = <T>(run: () => T) => transport.runInObject('telefunc-shard-weur-0', run)
      const inOtherObject = <T>(run: () => T) => transport.runInObject('telefunc-shard-weur-1', run)
      return { state, kv, inObject, inOtherObject }
    }

    it("a subscriber's `opened` rejects with KV's error, and so does a later one of the same Durable Object, until it has none", async () => {
      const previousTransport = getBroadcastAdapter()
      const { state, inObject } = refusing()
      const first = inObject(() => Broadcast.subscribe('room:test', () => {}))
      await expect(first.opened).rejects.toThrow('429')
      const second = inObject(() => Broadcast.subscribe('room:test', () => {}))
      await expect(second.opened).rejects.toThrow('429')
      first()
      second()
      state.refuse = false
      const third = inObject(() => Broadcast.subscribe('room:test', () => {}))
      await expect(third.opened).resolves.toBe(undefined)
      third()
      _resetBroadcastAdapterForTesting(previousTransport)
    })

    it("once the refresh writes the presence, a later subscriber's `opened` resolves, and the refused one's stays rejected", async () => {
      vi.useFakeTimers()
      const previousTransport = getBroadcastAdapter()
      try {
        const { state, inObject } = refusing()
        const refused = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await expect(refused.opened).rejects.toThrow('429')
        state.refuse = false
        // Telefunc's next try at the presence, every 30 s.
        await vi.advanceTimersByTimeAsync(30_000)
        const later = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await expect(later.opened).resolves.toBe(undefined)
        await expect(refused.opened).rejects.toThrow('429')
        refused()
        later()
      } finally {
        _resetBroadcastAdapterForTesting(previousTransport)
        vi.useRealTimers()
      }
    })

    it('a refresh KV acknowledges after its Durable Object left and came back, refused, leaves that refusal', async () => {
      vi.useFakeTimers()
      const previousTransport = getBroadcastAdapter()
      try {
        const { state, inObject, inOtherObject } = refusing()
        state.refuse = false
        const left = inObject(() => Broadcast.subscribe('room:test', () => {}))
        // Another Durable Object's subscriber keeps the key's refresh going.
        const other = inOtherObject(() => Broadcast.subscribe('room:test', () => {}))
        await Promise.all([left.opened, other.opened])
        let acknowledge = () => {}
        state.held = new Promise((resolve) => {
          acknowledge = resolve
        })
        await vi.advanceTimersByTimeAsync(30_000)
        state.held = undefined
        left()
        state.refuse = true
        const back = inObject(() => Broadcast.subscribe('room:test', () => {}))
        acknowledge()
        await expect(back.opened).rejects.toThrow('429')
        const later = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await expect(later.opened).rejects.toThrow('429')
        back()
        later()
        other()
      } finally {
        _resetBroadcastAdapterForTesting(previousTransport)
        vi.useRealTimers()
      }
    })

    it('a presence write that throws at once fails as a refused one: `opened` rejects with its error, and a publish goes on', async () => {
      const previousTransport = getBroadcastAdapter()
      const { kv, inObject } = refusing()
      kv.put = () => {
        throw new Error('KV binding invocation failed')
      }
      const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
      await expect(subscriber.opened).rejects.toThrow('KV binding invocation failed')
      // A publish from here waits for the presence writes in flight here.
      const published = await Promise.race([
        Promise.resolve(inObject(() => Broadcast.publish('room:test', 'hello'))).then(() => 'published'),
        new Promise((resolve) => setTimeout(() => resolve('held'), 1_000)),
      ])
      expect(published).toBe('published')
      subscriber()
      _resetBroadcastAdapterForTesting(previousTransport)
    })

    it('a presence delete that throws at once is warned of as a refused one', async () => {
      const previousTransport = getBroadcastAdapter()
      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
      try {
        const { state, kv, inObject } = refusing()
        state.refuse = false
        kv.delete = () => {
          throw new Error('KV binding invocation failed')
        }
        const subscriber = inObject(() => Broadcast.subscribe('room:test', () => {}))
        await subscriber.opened
        subscriber()
        await flushMicrotasks()
        expect(warn.mock.calls.map(([warning]) => String(warning).includes('KV binding invocation failed'))).toEqual([
          true,
        ])
      } finally {
        warn.mockRestore()
        _resetBroadcastAdapterForTesting(previousTransport)
      }
    })

    // KV writes a refresh it acknowledges late after a delete asked of it meanwhile.
    for (const comesBack of [false, true]) {
      it(`a refresh KV acknowledges after the Durable Object's last subscriber left${comesBack ? ', and a new one came' : ''} ${comesBack ? 'keeps' : 'leaves no'} presence`, async () => {
        vi.useFakeTimers()
        const previousTransport = getBroadcastAdapter()
        try {
          const { state, kv, inObject } = refusing()
          state.refuse = false
          const presences = async () => (await kv.list({ prefix: 'tfps:' })).keys.map(({ name }) => name)
          const left = inObject(() => Broadcast.subscribe('room:test', () => {}))
          await left.opened
          const written = await presences()
          let acknowledge = () => {}
          state.held = new Promise((resolve) => {
            acknowledge = resolve
          })
          await vi.advanceTimersByTimeAsync(30_000)
          state.held = undefined
          left()
          const back = comesBack ? inObject(() => Broadcast.subscribe('room:test', () => {})) : undefined
          acknowledge()
          await vi.advanceTimersByTimeAsync(0)
          await back?.opened
          expect(await presences()).toEqual(comesBack ? written : [])
          back?.()
        } finally {
          _resetBroadcastAdapterForTesting(previousTransport)
          vi.useRealTimers()
        }
      })
    }

    // A Durable Object subscribes again while KV still holds the delete of its presence: that of its release, or the one
    // after a refresh KV acknowledged once it had left.
    for (const deleting of ['its release', 'a late refresh'] as const) {
      it(`a Durable Object that subscribes again while the delete after ${deleting} is under way keeps its presence, and its \`opened\` waits for that delete`, async () => {
        vi.useFakeTimers()
        const previousTransport = getBroadcastAdapter()
        try {
          const { state, kv, inObject } = refusing()
          state.refuse = false
          const presences = async () => (await kv.list({ prefix: 'tfps:' })).keys.map(({ name }) => name)
          const hold = () => {
            let acknowledge = () => {}
            state.held = new Promise((resolve) => {
              acknowledge = resolve
            })
            return acknowledge
          }
          const left = inObject(() => Broadcast.subscribe('room:test', () => {}))
          await left.opened
          const written = await presences()
          let acknowledgeDelete: () => void
          if (deleting === 'its release') {
            acknowledgeDelete = hold()
            left()
            await vi.advanceTimersByTimeAsync(0)
          } else {
            const acknowledgeRefresh = hold()
            await vi.advanceTimersByTimeAsync(30_000)
            state.held = undefined
            left()
            acknowledgeDelete = hold()
            acknowledgeRefresh()
            await vi.advanceTimersByTimeAsync(0)
          }
          state.held = undefined
          const back = inObject(() => Broadcast.subscribe('room:test', () => {}))
          let backReady = false
          void back.opened?.then(() => (backReady = true))
          await vi.advanceTimersByTimeAsync(0)
          expect(backReady).toBe(false)
          acknowledgeDelete()
          await back.opened
          expect(await presences()).toEqual(written)
          back()
        } finally {
          _resetBroadcastAdapterForTesting(previousTransport)
          vi.useRealTimers()
        }
      })
    }

    // A key cut through a surrogate pair, as one shortened by its string length can be: its presence's name can't be
    // written, which fails as KV refusing it does.
    describe('a key whose presence name `encodeURIComponent` throws on', () => {
      const KEY = 'room:\u{1F600}'.slice(0, -1)
      const warned = (warn: { mock: { calls: unknown[][] } }) =>
        warn.mock.calls.filter(([warning]) => String(warning).includes('URI malformed')).length

      it("a subscriber's `opened` rejects with its error, which is warned of", async () => {
        const previousTransport = getBroadcastAdapter()
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
        try {
          const { state, inObject } = refusing()
          state.refuse = false
          const subscriber = inObject(() => Broadcast.subscribe(KEY, () => {}))
          await expect(subscriber.opened).rejects.toThrow('URI malformed')
          expect(warned(warn)).toBe(1)
          subscriber()
        } finally {
          warn.mockRestore()
          _resetBroadcastAdapterForTesting(previousTransport)
        }
      })

      it('a broadcast that subscribes, as for a page, closes with its error', async () => {
        const previousTransport = getBroadcastAdapter()
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
        try {
          const { state, inObject } = refusing()
          state.refuse = false
          const room = inObject(() => {
            const broadcast = new ServerBroadcast<string>({ key: KEY })
            broadcast.subscribe(() => {})
            return broadcast
          })
          await expect(room.closed).resolves.toHaveProperty('message', expect.stringContaining('URI malformed'))
        } finally {
          warn.mockRestore()
          _resetBroadcastAdapterForTesting(previousTransport)
        }
      })
    })

    it("a broadcast that subscribes closes with KV's error", async () => {
      const previousTransport = getBroadcastAdapter()
      const { inObject } = refusing()
      const room = inObject(() => {
        const broadcast = new ServerBroadcast<string>({ key: 'room:test:refused' })
        broadcast.subscribe(() => {})
        return broadcast
      })
      await expect(room.closed).resolves.toHaveProperty('message', expect.stringContaining('429'))
      _resetBroadcastAdapterForTesting(previousTransport)
    })
  })

  it('deletes KV presence on unsubscribe', async () => {
    const transport = createTransport(1)
    const kv = createMockKV()
    const previousTransport = getBroadcastAdapter()

    transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
    transport.attachKV(kv)
    transport.attachIsolateInfo('weur')
    _resetBroadcastAdapterForTesting(transport)

    const { unsubscribe } = transport.runInObject('telefunc-shard-weur-0', () =>
      transport.subscribe('room:test', () => {}),
    )
    await flushMicrotasks()

    const presenceKey = `tfps:${encodeURIComponent('room:test')}:weur:telefunc-shard-weur-0`
    expect(await kv.get(presenceKey)).toBe('telefunc-shard-weur-0')

    unsubscribe()
    await flushMicrotasks()

    expect(await kv.get(presenceKey)).toBeNull()

    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('a subscriber with no page, on a Worker without AsyncLocalStorage, throws an error that names the flag', () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    transport.attachIsolateInfo('weur')
    expect(() =>
      transport.runInObject('telefunc-shard-weur-0', () => transport.subscribe('room:test', () => {})),
    ).toThrow(
      // spellcheck-ignore  nodejs_als is Cloudflare's AsyncLocalStorage compatibility flag
      'add "nodejs_als" to the compatibility_flags',
    )
  })

  // The page subscribes and calls at once, so its call brings the subscription before its wire attaches.
  it("a page's subscription its call brings before its first attach, on a Worker without AsyncLocalStorage, is the Durable Object's that the call reached", async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kvWrites: string[] = []
    const previousTransport = getBroadcastAdapter()
    transport.attachBinding(createBasicBinding(), 'TelefuncDurableObject')
    transport.attachKV(createMockKV(kvWrites))
    transport.attachIsolateInfo('weur')
    _resetBroadcastAdapterForTesting(transport)
    const call = new Request('https://telefunc.test/_telefunc?session=page', { method: 'POST' })
    transport.attachRequest(call, 'telefunc-shard-weur-0')
    const broadcast = new ServerBroadcast<string>({ key: 'room:test' })
    // As `carryChannel` registers the broadcast a call returns.
    broadcast._carriedBy = call
    getChannelMux().registerChannel(broadcast, ATTACHABLE_NOW, 'page')
    await getChannelMux().applyCallWants(call, [{ channel: broadcast.id, version: 1, text: true, binary: false }])
    expect(broadcast.isClosed).toBe(false)
    expect(kvWrites).toEqual([`put tfps:${encodeURIComponent('room:test')}:weur:telefunc-shard-weur-0`])
    broadcast.abort()
    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('a subscriber with no page, outside the code of any Durable Object, throws an error that says where to subscribe', () => {
    const transport = createTransport(1)
    transport.attachIsolateInfo('weur')
    expect(() => transport.subscribe('room:test', () => {})).toThrow(
      'subscribes on the server only in code a Durable Object runs',
    )
  })

  it('records what a call subscribes after an `await` under the Durable Object that received that call', async () => {
    const transport = createTransport(5)
    const previousTransport = getBroadcastAdapter()
    let deliveringIn: string | null = null
    transport.attachBinding(
      createBasicBinding({
        onPublish: (_id, request) => transport.publishToSubscribers(createAuthorityState(), request),
        onDeliver(id, request) {
          deliveringIn = id.name
          transport.deliverToLocal(request)
          deliveringIn = null
          return Promise.resolve()
        },
      }),
      'TelefuncDurableObject',
    )
    const kvWrites: string[] = []
    transport.attachKV(createMockKV(kvWrites))
    transport.attachIsolateInfo('weur')
    _resetBroadcastAdapterForTesting(transport)

    const deliveredIn: string[] = []
    async function subscribingTelefunction(doName: string, resumeAfter: Promise<void>) {
      await resumeAfter
      const channel = new ServerBroadcast<string>({ key: 'room:test:call' })
      channel.subscribe(() => deliveredIn.push(`${doName}'s listener in ${deliveringIn}`))
      return channel
    }
    let secondCallStarted = () => {}
    const secondCallStart = new Promise<void>((resolve) => {
      secondCallStarted = resolve
    })
    // The first call resumes only after the second one ran, so each subscribes while the other's call is under way.
    const firstCall = transport.runInObject('telefunc-shard-weur-1', () =>
      subscribingTelefunction('telefunc-shard-weur-1', secondCallStart),
    )
    const secondCall = transport.runInObject('telefunc-shard-weur-2', () => {
      secondCallStarted()
      return subscribingTelefunction('telefunc-shard-weur-2', flushCoordinatorTurn())
    })
    const channels = await Promise.all([firstCall, secondCall])
    await flushCoordinatorTurn()
    await channels[0].publish('hello')
    await flushCoordinatorTurn()

    expect(deliveredIn.sort()).toEqual([
      "telefunc-shard-weur-1's listener in telefunc-shard-weur-1",
      "telefunc-shard-weur-2's listener in telefunc-shard-weur-2",
    ])
    expect(kvWrites.sort()).toEqual([
      `put tfps:${encodeURIComponent('room:test:call')}:weur:telefunc-shard-weur-1`,
      `put tfps:${encodeURIComponent('room:test:call')}:weur:telefunc-shard-weur-2`,
    ])
    for (const channel of channels) channel.abort()
    _resetBroadcastAdapterForTesting(previousTransport)
  })

  it('records what a listener subscribes while a publish is delivered under the Durable Object it runs in', async () => {
    const transport = createTransport(5)
    const previousTransport = getBroadcastAdapter()
    _resetBroadcastAdapterForTesting(transport)
    type DeliveringObject = { telefuncBroadcastDeliver(request: BroadcastDeliverRequest): void }
    const isDeliveringObject = (object: object): object is DeliveringObject =>
      'telefuncBroadcastDeliver' in object && typeof object.telefuncBroadcastDeliver === 'function'
    const objects = new Map<string, DeliveringObject>()
    let deliveringIn: string | null = null
    const binding = createBasicBinding({
      onPublish: (_id, request) => transport.publishToSubscribers(createAuthorityState(), request),
      onDeliver(id, request) {
        deliveringIn = id.name
        objects.get(id.name)?.telefuncBroadcastDeliver(request)
        deliveringIn = null
        return Promise.resolve()
      },
    })
    const env = { TelefuncDurableObject: binding, TelefuncKV: createMockKV() }
    const { TelefuncDurableObject } = new Telefunc({ scale: 5 })
    for (const name of ['telefunc-shard-weur-0', 'telefunc-shard-weur-1']) {
      const ctx = { id: { name }, storage: {} } as unknown as DurableObjectState
      const object = new TelefuncDurableObject(ctx, env)
      if (!isDeliveringObject(object)) throw new Error('Expected a Durable Object that takes Broadcast deliveries')
      objects.set(name, object)
    }
    transport.attachIsolateInfo('weur')

    const deliveredIn: Array<string | null> = []
    const joined = new ServerBroadcast<string>({ key: 'room:test:joined' })
    transport.runInObject('telefunc-shard-weur-1', () =>
      new ServerBroadcast<string>({ key: 'room:test:join' }).subscribe(() =>
        joined.subscribe(() => deliveredIn.push(deliveringIn)),
      ),
    )
    await flushCoordinatorTurn()
    await Broadcast.publish('room:test:join', 'join')
    await flushCoordinatorTurn()
    await joined.publish('hello')
    await flushCoordinatorTurn()

    expect(deliveredIn).toEqual(['telefunc-shard-weur-1'])
    _resetBroadcastAdapterForTesting(previousTransport)
  })
})
