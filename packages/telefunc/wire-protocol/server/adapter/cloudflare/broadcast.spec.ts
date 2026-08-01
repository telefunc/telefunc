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
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { ServerBroadcast } from '../../server-broadcast.js'
import { disposeBackend, installBackend } from '../../../backend/install.js'
import { BACKEND_SPI_VERSION, type BackendDriverPair } from '../../../backend/driver-pair.js'
import { CloudflareRoomBackend } from './room/backend.js'

type CloudflareRequest = Request & { cf?: { colo?: string; continent?: string } }

afterEach(async () => {
  await disposeBackend()
})

function installCloudflareTransport(transport: CloudflareBroadcastTransport): void {
  const driver = new CloudflareRoomBackend(transport)
  const pair: BackendDriverPair = {
    spiVersion: BACKEND_SPI_VERSION,
    driver,
    dispose: () => driver.dispose(),
  }
  installBackend(() => pair)
}

function createCloudflareRequest({ colo, continent }: { colo?: string; continent?: string } = {}): CloudflareRequest {
  const request = new Request('https://telefunc.test') as CloudflareRequest
  request.cf = { colo, continent }
  return request
}

function createAuthorityState(initial: ReadonlyArray<readonly [string, unknown]> = []) {
  const stored = new Map<string, unknown>(initial)
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
  return new CloudflareBroadcastAuthorityState(state)
}

type MockKVHooks = { beforePut?: () => Promise<void>; beforeDelete?: () => Promise<void> }

function createMockKV(hooks: MockKVHooks = {}): KVNamespace {
  const store = new Map<string, { value: string; expirationTtl?: number }>()
  return {
    async get(key: string) {
      return store.get(key)?.value ?? null
    },
    async put(key: string, value: string, options?: { expirationTtl?: number }) {
      await hooks.beforePut?.()
      store.set(key, { value, expirationTtl: options?.expirationTtl })
    },
    _ttl(key: string) {
      return store.get(key)?.expirationTtl
    },
    async delete(key: string) {
      await hooks.beforeDelete?.()
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

function configureTransport(
  transport: CloudflareBroadcastTransport,
  kv: KVNamespace,
  binding: DurableObjectNamespace,
  isolate = true,
): CloudflareBroadcastTransport {
  transport.attachBinding(binding, 'TelefuncDurableObject')
  transport.attachKV(kv)
  if (isolate) transport.attachIsolateInfo('telefunc-shard-weur-0', 'weur')
  return transport
}

function createTransport(kv = createMockKV(), isolate = true): CloudflareBroadcastTransport {
  return configureTransport(
    new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 }),
    kv,
    createBasicBinding(),
    isolate,
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

  it('writes KV presence on subscribe and reads it during publish fanout', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    const authority = createAuthorityState()
    const binding = createBasicBinding({
      onPublish: (_, request) => transport.publishToSubscribers(authority, request),
    })
    configureTransport(transport, kv, binding)
    const subscription = transport.openSubscription({ key: 'room:test', kind: 'text' }, () => {})
    await subscription.ready
    const value = await kv.get('tfps:text%3Aroom%3Atest:weur:telefunc-shard-weur-0')
    expect(value).toBe('telefunc-shard-weur-0')
    const binary = await transport.publish({ key: 'room:test', kind: 'binary' }, new Uint8Array([1]))
    const text = await transport.publish({ key: 'room:test', kind: 'text' }, new TextEncoder().encode('"text"'))
    expect([binary.receivers, text.receivers]).toEqual([0, 1])
    await subscription.unsubscribe()
  })

  it('keeps the first-touch authority bucket in publish receipts', async () => {
    const authorityState = createAuthorityState()
    const kv = createMockKV()
    const transport = createTransport(kv, false)
    await authorityState.getOrInitAuthorityBucket('room:first-touch', 'weur')
    await kv.put('tfps:text%3Aroom%3Afirst-touch:weur:telefunc-shard-weur-0', 'telefunc-shard-weur-0', {
      expirationTtl: 90,
    })
    await kv.put('tfps:text%3Aroom%3Afirst-touch:apac:telefunc-shard-apac-0', 'telefunc-shard-apac-0', {
      expirationTtl: 90,
    })
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

  it('rejects generic Broadcast sequence exhaustion before persisting an unsafe cursor', async () => {
    const key = 'room:exhausted'
    const authority = createAuthorityState([[`broadcast:${key}:sequence`, Number.MAX_SAFE_INTEGER]])
    await expect(authority.getNextKeySeq(key)).rejects.toThrow('sequence exhausted')
  })

  it('waits for KV presence setup before authority publish fanout', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kvPutReady = Promise.withResolvers<void>()
    const kv = createMockKV({ beforePut: () => kvPutReady.promise })
    const publishTargets: string[] = []
    configureTransport(
      transport,
      kv,
      createBasicBinding({
        onPublish(id, request) {
          publishTargets.push(id.name)
          return Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
      }),
    )
    installCloudflareTransport(transport)
    const room = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
    // subscribe() triggers KV presence setup — publish should wait for it
    room.subscribe(() => {})
    room.publish({ text: 'hello' })
    await flushMicrotasks(2)
    expect(publishTargets).toEqual([])
    kvPutReady.resolve()
    await flushCoordinatorTurn()
    expect(publishTargets).toEqual(['telefunc:broadcast:authority:room:test'])
  })

  it('does not deliver locally before ordered publish setup completes', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kvPutReady = Promise.withResolvers<void>()
    const kv = createMockKV({ beforePut: () => kvPutReady.promise })
    const received: string[] = []
    configureTransport(
      transport,
      kv,
      createBasicBinding({
        onPublish(id, request) {
          return transport.publishToSubscribers(createAuthorityState(), {
            ...request,
            locationBucket: request.locationBucket,
          })
        },
        onDeliver(id, request) {
          return transport.deliverToLocal(request)
        },
      }),
    )
    installCloudflareTransport(transport)
    const subscriber = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
    subscriber.subscribe((message) => {
      received.push(message.text)
    })
    const publisher = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
    publisher.publish({ text: 'hello' })
    await flushMicrotasks(2)
    expect(received).toEqual([])
    kvPutReady.resolve()
    await flushCoordinatorTurn()
    expect(received).toEqual(['hello'])
  })

  it('resolves publish ack with authority metadata after cold-path setup completes', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kvPutReady = Promise.withResolvers<void>()
    const kv = createMockKV({ beforePut: () => kvPutReady.promise })
    configureTransport(
      transport,
      kv,
      createBasicBinding({
        onPublish(id, request) {
          return transport.publishToSubscribers(createAuthorityState(), {
            ...request,
            locationBucket: request.locationBucket,
          })
        },
        onDeliver(id, request) {
          return transport.deliverToLocal(request)
        },
      }),
    )
    installCloudflareTransport(transport)
    const subscriber = new ServerBroadcast<{ text: string }>({ key: 'room:test:ack' })
    subscriber.subscribe(() => undefined)
    const publisher = new ServerBroadcast<{ text: string }>({ key: 'room:test:ack' })
    const receiptPromise = publisher.publish({ text: 'hello' })
    await flushMicrotasks(2)
    kvPutReady.resolve()
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

  it('authority forwards once to each populated bucket coordinator', async () => {
    const authorityState = createAuthorityState()
    const kv = createMockKV()
    const forwardedBuckets: string[] = []
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    configureTransport(
      transport,
      kv,
      createBasicBinding({
        onPublish(id, { locationBucket }) {
          forwardedBuckets.push(locationBucket)
          return Promise.resolve()
        },
      }),
      false,
    )
    await kv.put('tfps:text%3Aroom%3Atest:weur:telefunc-shard-weur-0', 'telefunc-shard-weur-0', {
      expirationTtl: 90,
    })
    await kv.put('tfps:text%3Aroom%3Atest:apac:telefunc-shard-apac-0', 'telefunc-shard-apac-0', {
      expirationTtl: 90,
    })
    await kv.put('tfps:text%3Aroom%3Atest:eeur:telefunc-shard-eeur-0', 'telefunc-shard-eeur-0', {
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

  it('forwarded publish round-trips a wide ordering frame to every named DO', async () => {
    const authorityState = createAuthorityState()
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const deliveredTo: string[] = []
    const received: Array<{ text: string; seq: number; timestamp: number }> = []
    configureTransport(
      transport,
      createMockKV(),
      createBasicBinding({
        onDeliver(id, request) {
          deliveredTo.push(id.name)
          return transport.deliverToLocal(request)
        },
      }),
    )
    const subscription = transport.openSubscription({ key: 'room:test', kind: 'text' }, (payload, info) => {
      received.push({ text: new TextDecoder().decode(payload), ...info })
    })
    await subscription.ready
    await transport.publishToSubscribers(authorityState, {
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"hello"}',
      forwarded: true,
      doNames: ['telefunc-shard-weur-0', 'telefunc-shard-weur-1'],
      info: { seq: 0x1_0000_0000, timestamp: 0x1_0000_0001 },
    })
    expect(deliveredTo.sort()).toEqual(['telefunc-shard-weur-0', 'telefunc-shard-weur-1'])
    expect(received).toEqual([
      { text: '{"text":"hello"}', seq: 0x1_0000_0000, timestamp: 0x1_0000_0001 },
      { text: '{"text":"hello"}', seq: 0x1_0000_0000, timestamp: 0x1_0000_0001 },
    ])
    await subscription.unsubscribe()
  })

  it('can publish without request context — uses isolate state directly', async () => {
    const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
    const kv = createMockKV()
    const coordinatorPublishes: Array<{ name: string; key: string; locationBucket: string; serialized: string }> = []
    configureTransport(
      transport,
      kv,
      createBasicBinding({
        onPublish(id, { key, locationBucket, serialized }) {
          coordinatorPublishes.push({ name: id.name, key, locationBucket, serialized })
          return Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
      }),
    )
    installCloudflareTransport(transport)
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
  })

  it('asserts when isolate info is not attached before subscribe', () => {
    const kv = createMockKV()
    const transport = createTransport(kv, false)
    expect(() => transport.openSubscription({ key: 'room:test', kind: 'text' }, () => {})).toThrow(
      'attachIsolateInfo()',
    )
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
    configureTransport(
      transport,
      kv,
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
      false,
    )
    await kv.put('tfps:text%3Aroom%3Atest:weur:telefunc-shard-weur-0', 'telefunc-shard-weur-0', {
      expirationTtl: 90,
    })
    await kv.put('tfps:text%3Aroom%3Atest:apac:telefunc-shard-apac-0', 'telefunc-shard-apac-0', {
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
    const kv = createMockKV()
    const transport = createTransport(kv)
    const subscription = transport.openSubscription({ key: 'room:test', kind: 'text' }, () => {})
    await subscription.ready
    const key = 'tfps:text%3Aroom%3Atest:weur:telefunc-shard-weur-0'
    expect(await kv.get(key)).toBe('telefunc-shard-weur-0')
    await subscription.unsubscribe()
    expect(await kv.get(key)).toBeNull()
  })

  it('keeps KV presence generation-safe across setup and delete churn', async () => {
    const setup = Promise.withResolvers<void>()
    const hooks: MockKVHooks = { beforePut: () => setup.promise }
    const kv = createMockKV(hooks)
    const transport = createTransport(kv)
    const lane = { key: 'room:presence-churn', kind: 'text' } as const
    const presenceKey = 'tfps:text%3Aroom%3Apresence-churn:weur:telefunc-shard-weur-0'
    const first = transport.openSubscription(lane, () => {})
    await first.unsubscribe()
    const successor = transport.openSubscription(lane, () => {})
    setup.resolve()
    await successor.ready
    await flushMicrotasks()
    expect(await kv.get(presenceKey)).toBe('telefunc-shard-weur-0')
    const releaseDeletion = Promise.withResolvers<void>()
    hooks.beforeDelete = () => releaseDeletion.promise
    const teardown = successor.unsubscribe()
    const replacement = transport.openSubscription(lane, () => {})
    await flushMicrotasks()
    expect(replacement.state()).toBe('establishing')
    releaseDeletion.resolve()
    await Promise.all([teardown, replacement.ready])
    expect(await kv.get(presenceKey)).toBe('telefunc-shard-weur-0')
    await replacement.unsubscribe()
  })

  it('surfaces presence refresh loss and recovery through subscription state', async () => {
    vi.useFakeTimers()
    const kv = createMockKV()
    const transport = createTransport(kv)
    const originalPut = kv.put.bind(kv)
    let putCalls = 0
    kv.put = (async (key: string, value: string, options?: Parameters<KVNamespace['put']>[2]) => {
      putCalls += 1
      if (putCalls === 2) throw new Error('presence refresh rejected')
      return originalPut(key, value, options)
    }) as KVNamespace['put']
    const subscription = transport.openSubscription({ key: 'room:refresh', kind: 'text' }, () => {})
    await subscription.ready
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
