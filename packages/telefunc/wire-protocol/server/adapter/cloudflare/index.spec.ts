import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => {
  const crosswsAdapter = {
    handleDurableInit: vi.fn(),
    handleDurableUpgrade: vi.fn(),
    handleDurableMessage: vi.fn(),
    handleDurableClose: vi.fn(),
  }
  class MockCloudflareBroadcastAuthorityState {
    readonly state: DurableObjectState
    constructor(state: DurableObjectState) {
      this.state = state
      mocks.authorityInstances.push(this)
    }
  }
  class MockCloudflareBroadcastTransport {
    readonly options: unknown
    readonly attachBinding = vi.fn()
    readonly attachKV = vi.fn()
    readonly attachIsolateInfo = vi.fn()
    readonly publishToSubscribers = vi.fn()
    readonly deliverToLocal = vi.fn()
    readonly dispose = vi.fn(async () => {})
    constructor(options: unknown) {
      this.options = options
      mocks.transportInstances.push(this)
    }
  }
  return {
    crosswsAdapter,
    crosswsFactory: vi.fn(() => crosswsAdapter),
    enableChannelTransports: vi.fn(),
    getServerConfig: vi.fn(() => ({ telefuncUrl: '/_telefunc', channel: { transports: ['WS'] } })),
    telefuncMock: vi.fn(async () => ({
      statusCode: 200,
      headers: [['content-type', 'application/json']] as HeadersInit,
      getReadableWebStream() {
        return new ReadableStream()
      },
    })),
    asyncMode: false,
    rawContext: null as Record<symbol, unknown> | null,
    transportInstances: [] as MockCloudflareBroadcastTransport[],
    authorityInstances: [] as MockCloudflareBroadcastAuthorityState[],
    MockCloudflareBroadcastAuthorityState,
    MockCloudflareBroadcastTransport,
  }
})

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

vi.mock('crossws/adapters/cloudflare', () => ({
  default: mocks.crosswsFactory,
}))

vi.mock('../../ws.js', () => ({
  getTelefuncChannelHooks: vi.fn(() => ({ onMessage: vi.fn() })),
}))

vi.mock('../../../../node/server/serverConfig.js', () => ({
  getServerConfig: mocks.getServerConfig,
  enableChannelTransports: mocks.enableChannelTransports,
}))

vi.mock('../../../../node/server/telefunc.js', () => ({
  serve: mocks.telefuncMock,
}))

vi.mock('../../../../node/server/context/context.js', () => ({
  getRawContext: () => mocks.rawContext,
  isAsyncMode: () => mocks.asyncMode,
  restoreContext: <T>(context: Record<symbol, unknown>, fn: () => T): T => {
    const previous = mocks.rawContext
    mocks.rawContext = context
    try {
      const result = fn()
      if (result instanceof Promise) {
        return result.finally(() => {
          mocks.rawContext = previous
        }) as T
      }
      mocks.rawContext = previous
      return result
    } catch (error) {
      mocks.rawContext = previous
      throw error
    }
  },
}))

vi.mock('./broadcast.js', () => ({
  CloudflareBroadcastAuthorityState: mocks.MockCloudflareBroadcastAuthorityState,
  CloudflareBroadcastTransport: mocks.MockCloudflareBroadcastTransport,
}))

vi.mock('./routing.js', () => ({
  TELEFUNC_BROADCAST_BUCKET_HEADER: 'x-telefunc-broadcast-bucket',
  TELEFUNC_SESSION_HEADER: 'x-telefunc-session',
  TELEFUNC_SHARD_HEADER: 'x-telefunc-shard',
  assertLocationFallbackIsScaled: vi.fn(),
  resolveSessionRoutingTarget: vi.fn(
    (baseInstanceName: string, scale: unknown, request: Request, locationFallback: string) => {
      void scale
      void request
      void locationFallback
      return {
        sessionInstanceName: `${baseInstanceName}-shard-weur-0`,
        locationBucket: 'weur',
        shardOrdinal: 0,
      }
    },
  ),
}))

import { Telefunc } from '../../../../serve/cloudflare.js'
import { BACKEND_SPI_VERSION, type BackendDriverPair } from '../../../backend/driver-pair.js'
import { disposeBackend, getRoomBackend, installBackend } from '../../../backend/install.js'
import { MemoryBackend } from '../../../backend/memory/backend.js'

const memoryPair = (driver: MemoryBackend): BackendDriverPair => ({
  spiVersion: BACKEND_SPI_VERSION,
  driver,
  dispose: () => driver.dispose(),
})

function createMockKV(): KVNamespace {
  const store = new Map<string, { value: string; expirationTtl?: number }>()
  return {
    async get(key: string, type?: string) {
      const entry = store.get(key)
      if (!entry) return null
      return type === 'json' ? JSON.parse(entry.value) : entry.value
    },
    async put(key: string, value: string, options?: { expirationTtl?: number }) {
      store.set(key, { value, expirationTtl: options?.expirationTtl })
    },
    async delete(key: string) {
      store.delete(key)
    },
  } as unknown as KVNamespace
}

function createBinding() {
  const fetch = vi.fn(async (request: Request) => new Response(request.headers.get('x-telefunc-shard') ?? 'missing'))
  const get = vi.fn((id: { name: string }, options?: { locationHint: string }) => {
    void id
    void options
    return { fetch }
  })
  const idFromName = vi.fn((name: string) => ({
    name,
    equals(other: { name: string }) {
      return other.name === name
    },
  }))
  const jurisdiction = vi.fn(() => binding)
  const binding = { get, idFromName, jurisdiction }
  return { binding, get, fetch, idFromName, jurisdiction }
}

beforeEach(() => {
  mocks.crosswsFactory.mockClear()
  mocks.crosswsAdapter.handleDurableInit.mockReset()
  mocks.crosswsAdapter.handleDurableUpgrade.mockReset()
  mocks.crosswsAdapter.handleDurableMessage.mockReset()
  mocks.crosswsAdapter.handleDurableClose.mockReset()
  mocks.enableChannelTransports.mockClear()
  mocks.getServerConfig.mockReset()
  mocks.getServerConfig.mockReturnValue({ telefuncUrl: '/_telefunc', channel: { transports: ['WS'] } })
  mocks.telefuncMock.mockClear()
  mocks.telefuncMock.mockResolvedValue({
    statusCode: 200,
    headers: [['content-type', 'application/json']] as HeadersInit,
    getReadableWebStream() {
      return new ReadableStream()
    },
  })
  mocks.asyncMode = false
  mocks.rawContext = null
  mocks.transportInstances.length = 0
  mocks.authorityInstances.length = 0
})

afterEach(async () => {
  await disposeBackend()
})

describe('cloudflare adapter entrypoint', () => {
  it('resolves shard from KV token and forwards routing headers', async () => {
    const { binding, get, fetch } = createBinding()
    const tf = new Telefunc()
    const kv = createMockKV()
    await kv.put('session:my-token', JSON.stringify({ s: 'telefunc-shard-weur-1', b: 'weur' }))
    const request = new Request('https://telefunc.test/_telefunc?session=my-token')
    const response = await tf.serve({
      request,
      env: { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      ctx: { waitUntil: vi.fn() } as unknown as ExecutionContext,
    })
    expect(mocks.enableChannelTransports).toHaveBeenCalled()
    expect(mocks.transportInstances).toHaveLength(1)
    expect(get).toHaveBeenCalledWith(expect.objectContaining({ name: 'telefunc-shard-weur-1' }), {
      locationHint: 'weur',
    })
    expect(fetch).toHaveBeenCalledTimes(1)
    const forwardedRequest = fetch.mock.calls[0]![0] as Request
    expect(forwardedRequest.headers.get('x-telefunc-shard')).toBe('telefunc-shard-weur-1')
    expect(forwardedRequest.headers.get('x-telefunc-broadcast-bucket')).toBe('weur')
    expect(response?.headers.get('x-telefunc-session')).toBe('my-token')
  })

  it('derives a new shard and stores a KV token when no token is provided', async () => {
    const { binding, get, fetch } = createBinding()
    const tf = new Telefunc()
    const kv = createMockKV()
    const putGate = Promise.withResolvers<void>()
    const originalPut = kv.put.bind(kv)
    kv.put = (async (...args: Parameters<KVNamespace['put']>) => {
      await putGate.promise
      return originalPut(...args)
    }) as KVNamespace['put']
    const waitUntilFns: Array<Promise<unknown>> = []
    const request = new Request('https://telefunc.test/_telefunc')
    const responsePromise = tf.serve({
      request,
      env: { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      ctx: { waitUntil: (p: Promise<unknown>) => waitUntilFns.push(p) } as unknown as ExecutionContext,
    })
    expect(
      await Promise.race([
        responsePromise.then(() => 'exposed' as const),
        new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), 0)),
      ]),
    ).toBe('pending')
    putGate.resolve()
    const response = await responsePromise
    expect(get).toHaveBeenCalledWith(expect.objectContaining({ name: 'telefunc-shard-weur-0' }), {
      locationHint: 'weur',
    })
    expect(fetch).toHaveBeenCalledTimes(1)
    const token = response?.headers.get('x-telefunc-session')
    expect(token).toBeTruthy()
    expect(token).toMatch(/^telefunc-shard-weur-0:/)
    await Promise.all(waitUntilFns)
    const stored = await kv.get(`session:${token}`, 'json')
    expect(stored).toEqual({ s: 'telefunc-shard-weur-0', b: 'weur' })
  })

  it('returns undefined for non-telefunc traffic', async () => {
    const tf = new Telefunc()
    for (const path of ['/other', '/_telefunc-other']) {
      await expect(
        tf.serve({
          request: new Request(`https://telefunc.test${path}`),
          env: {} as Cloudflare.Env,
          ctx: {} as ExecutionContext,
        }),
      ).resolves.toBeUndefined()
    }
  })

  it('asserts when binding is missing for telefunc traffic', async () => {
    const tf = new Telefunc()
    await expect(
      tf.serve({
        request: new Request('https://telefunc.test/_telefunc'),
        env: {} as Cloudflare.Env,
        ctx: {} as ExecutionContext,
      }),
    ).rejects.toThrow('Missing Cloudflare Durable Object binding')
  })

  it('returns 400 for websocket upgrades when websocket transport is disabled', async () => {
    const { binding } = createBinding()
    mocks.getServerConfig.mockReturnValue({ telefuncUrl: '/_telefunc', channel: { transports: [] } })
    const tf = new Telefunc()
    const request = new Request('https://telefunc.test/_telefunc', { headers: { upgrade: 'websocket' } })
    const response = await tf.serve({
      request,
      env: { TelefuncDurableObject: binding } as unknown as Cloudflare.Env,
      ctx: {} as ExecutionContext,
    })
    expect(response?.status).toBe(400)
  })

  it('applies jurisdiction wrapping before binding lookups', async () => {
    const { binding, jurisdiction } = createBinding()
    const kv = createMockKV()
    const tf = new Telefunc({ jurisdiction: 'eu' as DurableObjectJurisdiction })
    await tf.serve({
      request: new Request('https://telefunc.test/_telefunc'),
      env: { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      ctx: { waitUntil: vi.fn() } as unknown as ExecutionContext,
    })
    expect(jurisdiction).toHaveBeenCalledWith('eu')
  })

  it('passes base transport options to the broadcast transport', () => {
    new Telefunc()
    expect(mocks.transportInstances[0]?.options).toEqual(
      expect.objectContaining({ baseInstanceName: 'telefunc', scale: undefined }),
    )
  })

  it('installs the Durable Object Room backend from the documented Cloudflare setup alone', async () => {
    new Telefunc()
    await expect(getRoomBackend().readHead('cloudflare-default-probe')).rejects.toThrow(
      'Cloudflare Room requires await-safe context',
    )
  })

  it('keeps an explicit Room backend installation as the Cloudflare policy override', () => {
    const explicit = new MemoryBackend()
    installBackend(() => memoryPair(explicit))
    const selected = getRoomBackend()
    new Telefunc()
    expect(getRoomBackend()).toBe(selected)
  })

  it('lets an explicit Room backend override an already-installed Cloudflare default', () => {
    new Telefunc()
    const explicit = new MemoryBackend()
    installBackend(() => memoryPair(explicit))
    const selected = getRoomBackend()
    expect(getRoomBackend()).toBe(selected)
  })

  it('keeps the same Durable Object Room backend across repeated Worker entry evaluation', () => {
    new Telefunc()
    const installed = getRoomBackend()
    new Telefunc()
    expect(getRoomBackend()).toBe(installed)
    expect(mocks.transportInstances).toHaveLength(1)
  })

  it('reports the normative Room binding diagnostic instead of using the memory backend', async () => {
    mocks.asyncMode = true
    const { binding } = createBinding()
    const tf = new Telefunc()
    const DurableClass = tf.TelefuncDurableObject
    const instance = new DurableClass(
      {
        id: { toString: () => 'telefunc-room-binding-probe' },
        getWebSockets: () => [],
      } as unknown as DurableObjectState,
      { TelefuncDurableObject: binding } as unknown as Cloudflare.Env,
    ) as InstanceType<typeof DurableClass> & { fetch(request: Request): Promise<Response> }
    mocks.telefuncMock.mockImplementationOnce(async () => {
      await getRoomBackend().readHead('binding-probe')
      throw new Error('Room backend unexpectedly returned without a binding')
    })
    await expect(instance.fetch(new Request('https://telefunc.test/_telefunc'))).rejects.toThrow(
      'Missing Cloudflare Room Durable Object binding "TelefuncRoomDurableObject". Add it to your wrangler.jsonc.',
    )
  })

  it('publishes the named SQLite Room authority and carries the configured session binding into it', () => {
    const tf = new Telefunc({ bindingName: 'CustomTelefuncSession', roomBindingName: 'CustomRoomAuthority' })
    expect(tf.TelefuncRoomDurableObject.name).toBe('TelefuncRoomDurableObject')
    expect(() => new tf.TelefuncRoomDurableObject({} as DurableObjectState, {} as Cloudflare.Env)).toThrow(
      'Missing Cloudflare session Durable Object binding "CustomTelefuncSession" in TelefuncRoomDurableObject constructor.',
    )
  })

  it('wires the durable object runtime and delegates fetch, websocket, and broadcast methods', async () => {
    const { binding } = createBinding()
    const tf = new Telefunc({ context: vi.fn(async () => ({ userId: 'user-1' })) })
    const DurableClass = tf.TelefuncDurableObject
    const hibernatedSocket = { close: vi.fn() }
    const ctx = {
      id: { name: 'telefunc-shard-weur-1' },
      getWebSockets: () => [hibernatedSocket],
    } as unknown as DurableObjectState
    const instance = new DurableClass(ctx, {
      TelefuncDurableObject: binding,
    } as unknown as Cloudflare.Env) as InstanceType<typeof DurableClass> & {
      fetch(request: Request): Promise<Response>
      webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void
      webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): void
      telefuncBroadcastPublish(request: unknown): unknown
      telefuncBroadcastDeliver(request: unknown): void
      telefuncRoomInvalidate(request: unknown): void
    }
    expect(mocks.transportInstances[0]?.attachBinding).toHaveBeenCalledWith(binding, 'TelefuncDurableObject')
    expect(mocks.crosswsAdapter.handleDurableInit).toHaveBeenCalledWith(instance, ctx, {
      TelefuncDurableObject: binding,
    })
    expect(hibernatedSocket.close).not.toHaveBeenCalled()
    mocks.crosswsAdapter.handleDurableUpgrade.mockResolvedValue(new Response('upgrade'))
    const upgradeResponse = await instance.fetch(
      new Request('https://telefunc.test/_telefunc', { headers: { upgrade: 'websocket' } }),
    )
    expect(upgradeResponse).toBeInstanceOf(Response)
    expect(mocks.crosswsAdapter.handleDurableUpgrade).toHaveBeenCalled()
    await instance.fetch(
      new Request('https://telefunc.test/_telefunc', {
        headers: { 'x-telefunc-shard': 'telefunc-shard-weur-1', 'x-telefunc-broadcast-bucket': 'weur' },
      }),
    )
    expect(mocks.telefuncMock).toHaveBeenCalled()
    expect(mocks.transportInstances[0]?.attachIsolateInfo).toHaveBeenCalledWith('telefunc-shard-weur-1', 'weur')
    instance.webSocketMessage({} as WebSocket, 'payload')
    expect(mocks.crosswsAdapter.handleDurableMessage).toHaveBeenCalledWith(instance, expect.anything(), 'payload')
    instance.webSocketClose({} as WebSocket, 1000, 'done', true)
    expect(mocks.crosswsAdapter.handleDurableClose).toHaveBeenCalledWith(
      instance,
      expect.anything(),
      1000,
      'done',
      true,
    )
    instance.telefuncBroadcastPublish({
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"hello"}',
      forwarded: false,
    })
    expect(mocks.transportInstances[0]?.publishToSubscribers).toHaveBeenCalledWith(mocks.authorityInstances[0], {
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"hello"}',
      forwarded: false,
    })
    instance.telefuncBroadcastDeliver({
      key: 'room:test',
      serialized: '{"text":"hello"}',
      info: { seq: 1, timestamp: Date.now() },
    })
    expect(mocks.transportInstances[0]?.deliverToLocal).toHaveBeenCalledWith({
      key: 'room:test',
      serialized: '{"text":"hello"}',
      info: expect.any(Object),
    })
    expect(hibernatedSocket.close).not.toHaveBeenCalled()
    // Importing and using the ordinary Cloudflare adapter remains flag-free. Only the first Room entry
    // asks for the opt-in async carrier and reports the recipe diagnostic.
    const invalidation = {
      roomId: 'room',
      inc: 'inc',
      laneKey: 'lane',
      subscriberDoId: 'id',
      leaseId: 'lease',
      generationToken: 'generation',
    }
    expect(() => instance.telefuncRoomInvalidate(invalidation)).toThrow('Cloudflare Room requires await-safe context')
    expect(hibernatedSocket.close).not.toHaveBeenCalled()
    mocks.asyncMode = true
    instance.telefuncRoomInvalidate(invalidation)
    expect(hibernatedSocket.close).toHaveBeenCalledWith(1012, 'Telefunc session reset; reconnect')
  })
})
