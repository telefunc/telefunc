import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => {
  const crosswsAdapter = {
    handleDurableInit: vi.fn(),
    handleDurableUpgrade: vi.fn(),
    handleDurableMessage: vi.fn(),
    handleDurableClose: vi.fn(),
  }

  class MockCloudflarePubSubRegistry {
    readonly state: DurableObjectState
    readonly deliverLocal = vi.fn()

    constructor(state: DurableObjectState) {
      this.state = state
      mocks.registryInstances.push(this)
    }
  }

  class MockCloudflarePubSubTransport {
    readonly options: unknown
    readonly attachBinding = vi.fn()
    readonly attachKV = vi.fn()
    readonly attachSessionRegistry = vi.fn()
    readonly publishToSubscribers = vi.fn()

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
    setPubSubTransport: vi.fn(),
    transportInstances: [] as MockCloudflarePubSubTransport[],
    registryInstances: [] as MockCloudflarePubSubRegistry[],
    MockCloudflarePubSubRegistry,
    MockCloudflarePubSubTransport,
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
  telefunc: mocks.telefuncMock,
}))

vi.mock('../../pubsub.js', () => ({
  setPubSubTransport: mocks.setPubSubTransport,
}))

vi.mock('./pubsub.js', () => ({
  CloudflarePubSubRegistry: mocks.MockCloudflarePubSubRegistry,
  CloudflarePubSubTransport: mocks.MockCloudflarePubSubTransport,
}))

vi.mock('./routing.js', () => ({
  TELEFUNC_PUBSUB_BUCKET_HEADER: 'x-telefunc-pubsub-bucket',
  TELEFUNC_SESSION_HEADER: 'x-telefunc-session',
  TELEFUNC_SHARD_HEADER: 'x-telefunc-shard',
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

import { telefuncWebSocket } from './index.js'

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
  mocks.setPubSubTransport.mockClear()
  mocks.transportInstances.length = 0
  mocks.registryInstances.length = 0
})

describe('cloudflare adapter entrypoint', () => {
  it('resolves shard from KV token and forwards routing headers', async () => {
    const { binding, get, fetch } = createBinding()
    const ws = telefuncWebSocket()
    const kv = createMockKV()
    await kv.put('session:my-token', JSON.stringify({ s: 'telefunc-shard-weur-1', b: 'weur' }))
    const request = new Request('https://telefunc.test/_telefunc?session=my-token')

    const response = await ws.handleTelefunc(
      request,
      { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    )

    expect(mocks.enableChannelTransports).toHaveBeenCalled()
    expect(mocks.setPubSubTransport).toHaveBeenCalledWith(mocks.transportInstances[0])
    expect(get).toHaveBeenCalledWith(expect.objectContaining({ name: 'telefunc-shard-weur-1' }), {
      locationHint: 'weur',
    })
    expect(fetch).toHaveBeenCalledTimes(1)

    const forwardedRequest = fetch.mock.calls[0]![0] as Request
    expect(forwardedRequest.headers.get('x-telefunc-shard')).toBe('telefunc-shard-weur-1')
    expect(forwardedRequest.headers.get('x-telefunc-pubsub-bucket')).toBe('weur')

    // Response carries the same opaque token back
    expect(response?.headers.get('x-telefunc-session')).toBe('my-token')
  })

  it('derives a new shard and stores a KV token when no token is provided', async () => {
    const { binding, get, fetch } = createBinding()
    const ws = telefuncWebSocket()
    const kv = createMockKV()
    const waitUntilFns: Array<Promise<unknown>> = []
    const request = new Request('https://telefunc.test/_telefunc')

    const response = await ws.handleTelefunc(
      request,
      { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      { waitUntil: (p: Promise<unknown>) => waitUntilFns.push(p) } as unknown as ExecutionContext,
    )

    expect(get).toHaveBeenCalledWith(expect.objectContaining({ name: 'telefunc-shard-weur-0' }), {
      locationHint: 'weur',
    })
    expect(fetch).toHaveBeenCalledTimes(1)

    // A new opaque token is returned in the response header
    const token = response?.headers.get('x-telefunc-session')
    expect(token).toBeTruthy()
    // Token is prefixed with the instance name
    expect(token).toMatch(/^telefunc-shard-weur-0:/)

    // The token is stored in KV via waitUntil
    await Promise.all(waitUntilFns)
    const stored = await kv.get(`session:${token}`, 'json')
    expect(stored).toEqual({ s: 'telefunc-shard-weur-0', b: 'weur' })
  })

  it('returns undefined for non-telefunc traffic or missing bindings', () => {
    const ws = telefuncWebSocket()

    expect(
      ws.handleTelefunc(new Request('https://telefunc.test/other'), {} as Cloudflare.Env, {} as ExecutionContext),
    ).toBeUndefined()
    expect(
      ws.handleTelefunc(new Request('https://telefunc.test/_telefunc'), {} as Cloudflare.Env, {} as ExecutionContext),
    ).toBeUndefined()
  })

  it('returns 400 for websocket upgrades when websocket transport is disabled', async () => {
    const { binding } = createBinding()
    mocks.getServerConfig.mockReturnValue({ telefuncUrl: '/_telefunc', channel: { transports: [] } })
    const ws = telefuncWebSocket()
    const request = new Request('https://telefunc.test/_telefunc', { headers: { upgrade: 'websocket' } })

    const response = await ws.handleTelefunc(
      request,
      { TelefuncDurableObject: binding } as unknown as Cloudflare.Env,
      {} as ExecutionContext,
    )

    expect(response?.status).toBe(400)
  })

  it('applies jurisdiction wrapping before binding lookups', async () => {
    const { binding, jurisdiction } = createBinding()
    const kv = createMockKV()
    const ws = telefuncWebSocket({ jurisdiction: 'eu' as DurableObjectJurisdiction })

    await ws.handleTelefunc(
      new Request('https://telefunc.test/_telefunc'),
      { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      { waitUntil: vi.fn() } as unknown as ExecutionContext,
    )

    expect(jurisdiction).toHaveBeenCalledWith('eu')
  })

  it('passes base transport options to the pubsub transport', () => {
    telefuncWebSocket()

    expect(mocks.transportInstances[0]?.options).toEqual(
      expect.objectContaining({ baseInstanceName: 'telefunc', scale: undefined }),
    )
  })

  it('wires the durable object runtime and delegates fetch, websocket, and pubsub methods', async () => {
    const { binding } = createBinding()
    const ws = telefuncWebSocket({ context: vi.fn(async () => ({ userId: 'user-1' })) })
    const DurableClass = ws.createDurableObjectClass()
    const ctx = { id: { name: 'telefunc-shard-weur-1' } } as DurableObjectState
    const instance = new DurableClass(ctx, { TelefuncDurableObject: binding } as unknown as Cloudflare.Env)

    expect(mocks.transportInstances[0]?.attachBinding).toHaveBeenCalledWith(binding, 'TelefuncDurableObject')
    expect(mocks.crosswsAdapter.handleDurableInit).toHaveBeenCalledWith(instance, ctx, {
      TelefuncDurableObject: binding,
    })

    mocks.crosswsAdapter.handleDurableUpgrade.mockResolvedValue(new Response('upgrade'))
    const upgradeResponse = await instance.fetch(
      new Request('https://telefunc.test/_telefunc', { headers: { upgrade: 'websocket' } }),
    )
    expect(upgradeResponse).toBeInstanceOf(Response)
    expect(mocks.crosswsAdapter.handleDurableUpgrade).toHaveBeenCalled()

    const response = await instance.fetch(
      new Request('https://telefunc.test/_telefunc', { headers: { 'x-telefunc-shard': 'telefunc-shard-weur-1' } }),
    )
    expect(mocks.telefuncMock).toHaveBeenCalled()
    expect(mocks.transportInstances[0]?.attachSessionRegistry).toHaveBeenCalledWith(
      'telefunc-shard-weur-1',
      mocks.registryInstances[0],
    )

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

    instance.telefuncPubSubPublish({
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"hello"}',
      sourceChannelId: 'channel-1',
      sourceSessionInstanceName: 'telefunc-shard-weur-1',
      forwarded: false,
    })
    expect(mocks.transportInstances[0]?.publishToSubscribers).toHaveBeenCalledWith(mocks.registryInstances[0], {
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"hello"}',
      sourceChannelId: 'channel-1',
      sourceSessionInstanceName: 'telefunc-shard-weur-1',
      forwarded: false,
    })

    instance.telefuncPubSubDeliver({
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"hello"}',
      sourceChannelId: 'channel-1',
    })
    expect(mocks.registryInstances[0]?.deliverLocal).toHaveBeenCalledWith({
      key: 'room:test',
      locationBucket: 'weur',
      serialized: '{"text":"hello"}',
      sourceChannelId: 'channel-1',
    })
  })
})
