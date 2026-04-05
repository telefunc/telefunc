import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => {
  const crosswsAdapter = {
    handleDurableInit: vi.fn(),
    handleDurableUpgrade: vi.fn(),
    handleDurableMessage: vi.fn(),
    handleDurableClose: vi.fn(),
  }

  class MockCloudflarePubSubAuthorityState {
    readonly state: DurableObjectState

    constructor(state: DurableObjectState) {
      this.state = state
      mocks.authorityInstances.push(this)
    }
  }

  class MockCloudflarePubSubTransport {
    readonly options: unknown
    readonly attachBinding = vi.fn()
    readonly attachKV = vi.fn()
    readonly attachIsolateInfo = vi.fn()
    readonly publishToSubscribers = vi.fn()
    readonly deliverToLocal = vi.fn()

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
    setPubSubAdapter: vi.fn(),
    transportInstances: [] as MockCloudflarePubSubTransport[],
    authorityInstances: [] as MockCloudflarePubSubAuthorityState[],
    MockCloudflarePubSubAuthorityState,
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
  serve: mocks.telefuncMock,
}))

vi.mock('../../pubsub.js', () => ({
  setPubSubAdapter: mocks.setPubSubAdapter,
}))

vi.mock('./pubsub.js', () => ({
  CloudflarePubSubAuthorityState: mocks.MockCloudflarePubSubAuthorityState,
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

import { telefunc } from '../../../../serve/cloudflare.js'

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
  mocks.setPubSubAdapter.mockClear()
  mocks.transportInstances.length = 0
  mocks.authorityInstances.length = 0
})

describe('cloudflare adapter entrypoint', () => {
  it('resolves shard from KV token and forwards routing headers', async () => {
    const { binding, get, fetch } = createBinding()
    const tf = telefunc()
    const kv = createMockKV()
    await kv.put('session:my-token', JSON.stringify({ s: 'telefunc-shard-weur-1', b: 'weur' }))
    const request = new Request('https://telefunc.test/_telefunc?session=my-token')

    const response = await tf.serve({
      request,
      env: { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      ctx: { waitUntil: vi.fn() } as unknown as ExecutionContext,
    })

    expect(mocks.enableChannelTransports).toHaveBeenCalled()
    expect(mocks.setPubSubAdapter).toHaveBeenCalledWith(mocks.transportInstances[0])
    expect(get).toHaveBeenCalledWith(expect.objectContaining({ name: 'telefunc-shard-weur-1' }), {
      locationHint: 'weur',
    })
    expect(fetch).toHaveBeenCalledTimes(1)

    const forwardedRequest = fetch.mock.calls[0]![0] as Request
    expect(forwardedRequest.headers.get('x-telefunc-shard')).toBe('telefunc-shard-weur-1')
    expect(forwardedRequest.headers.get('x-telefunc-pubsub-bucket')).toBe('weur')

    expect(response?.headers.get('x-telefunc-session')).toBe('my-token')
  })

  it('derives a new shard and stores a KV token when no token is provided', async () => {
    const { binding, get, fetch } = createBinding()
    const tf = telefunc()
    const kv = createMockKV()
    const waitUntilFns: Array<Promise<unknown>> = []
    const request = new Request('https://telefunc.test/_telefunc')

    const response = await tf.serve({
      request,
      env: { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      ctx: { waitUntil: (p: Promise<unknown>) => waitUntilFns.push(p) } as unknown as ExecutionContext,
    })

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
    const tf = telefunc()

    await expect(
      tf.serve({
        request: new Request('https://telefunc.test/other'),
        env: {} as Cloudflare.Env,
        ctx: {} as ExecutionContext,
      }),
    ).resolves.toBeUndefined()
  })

  it('asserts when binding is missing for telefunc traffic', async () => {
    const tf = telefunc()

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
    const tf = telefunc()
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
    const tf = telefunc({ jurisdiction: 'eu' as DurableObjectJurisdiction })

    await tf.serve({
      request: new Request('https://telefunc.test/_telefunc'),
      env: { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      ctx: { waitUntil: vi.fn() } as unknown as ExecutionContext,
    })

    expect(jurisdiction).toHaveBeenCalledWith('eu')
  })

  it('passes base transport options to the pubsub transport', () => {
    telefunc()

    expect(mocks.transportInstances[0]?.options).toEqual(
      expect.objectContaining({ baseInstanceName: 'telefunc', scale: undefined }),
    )
  })

  it('wires the durable object runtime and delegates fetch, websocket, and pubsub methods', async () => {
    const { binding } = createBinding()
    const tf = telefunc({ context: vi.fn(async () => ({ userId: 'user-1' })) })
    const DurableClass = tf.TelefuncDurableObject
    const ctx = { id: { name: 'telefunc-shard-weur-1' } } as DurableObjectState
    const instance = new DurableClass(ctx, {
      TelefuncDurableObject: binding,
    } as unknown as Cloudflare.Env) as InstanceType<typeof DurableClass> & {
      fetch(request: Request): Promise<Response>
      webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void
      webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): void
      telefuncPubSubPublish(request: unknown): unknown
      telefuncPubSubDeliver(request: unknown): void
    }

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
      new Request('https://telefunc.test/_telefunc', {
        headers: { 'x-telefunc-shard': 'telefunc-shard-weur-1', 'x-telefunc-pubsub-bucket': 'weur' },
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

    instance.telefuncPubSubPublish({
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

    instance.telefuncPubSubDeliver({
      key: 'room:test',
      serialized: '{"text":"hello"}',
      info: { seq: 1, ts: Date.now() },
    })
    expect(mocks.transportInstances[0]?.deliverToLocal).toHaveBeenCalledWith({
      key: 'room:test',
      serialized: '{"text":"hello"}',
      info: expect.any(Object),
    })
  })
})
