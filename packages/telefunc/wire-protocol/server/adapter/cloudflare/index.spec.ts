import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import { OrderedStubs } from './ordered-stubs.js'

const mocks = vi.hoisted(() => {
  const crosswsAdapter = {
    handleDurableInit: vi.fn(),
    handleDurableUpgrade: vi.fn(),
    handleDurableMessage: vi.fn(),
    handleDurableClose: vi.fn(),
  }
  class MockCloudflareBroadcastAuthorityState {
    readonly state: DurableObjectState
    readonly setPresence = vi.fn()
    constructor(state: DurableObjectState) {
      this.state = state
      mocks.authorityInstances.push(this)
    }
  }
  class MockCloudflareBroadcastTransport {
    readonly options: unknown
    readonly attachBinding = vi.fn()
    readonly publishToSubscribers = vi.fn()
    readonly forwardToBucket = vi.fn()
    readonly members: Array<{ id: string; locate: Mock; deliver: Mock }> = []
    readonly member = vi.fn((id: string) => {
      const member = { id, locate: vi.fn(), deliver: vi.fn() }
      this.members.push(member)
      return member
    })
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
    rawContext: null as Record<symbol, unknown> | null,
    workerEnv: {} as Record<string, unknown>,
    transportInstances: [] as MockCloudflareBroadcastTransport[],
    authorityInstances: [] as MockCloudflareBroadcastAuthorityState[],
    MockCloudflareBroadcastAuthorityState,
    MockCloudflareBroadcastTransport,
  }
})

vi.mock('cloudflare:workers', () => ({
  env: mocks.workerEnv,
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

vi.mock('../../../../node/server/async_hooks.js', () => ({}))

vi.mock('../../../../node/server/context/context.js', () => ({
  getRawContext: () => mocks.rawContext,
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
import { disposeBackend, getRoomBackend, installBackend } from '../../../backend/install.js'
import { MemoryBackend } from '../../../backend/memory/backend.js'
import type {
  BroadcastDeliverRequest,
  BroadcastForwardRequest,
  BroadcastPresenceRequest,
  BroadcastPublishRequest,
} from './broadcast.js'

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
  const fetch = vi.fn(async (request: Request) => new Response(request.headers.get('x-telefunc-broadcast-bucket')))
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
  mocks.rawContext = null
  for (const key of Object.keys(mocks.workerEnv)) delete mocks.workerEnv[key]
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
    const room = createBinding()
    const readHead = vi.fn(async () => null)
    room.get.mockReturnValue({ readHead } as never)
    mocks.workerEnv.TelefuncDurableObject = room.binding
    new Telefunc()
    // Outside any request or Durable Object context, as from a cron trigger.
    await expect(getRoomBackend().readHead('cloudflare-default-probe')).resolves.toBeNull()
    expect(room.idFromName).toHaveBeenCalledWith('__telefunc_room__:cloudflare-default-probe')
    expect(readHead).toHaveBeenCalled()
  })

  it('names the session a subscription needs when made outside one', () => {
    mocks.workerEnv.TelefuncDurableObject = createBinding().binding
    new Telefunc()
    const subscribe = () => getRoomBackend().subscribeLane('r', 'i', { kind: 'control' }, () => {})
    expect(subscribe).toThrow('A Cloudflare subscription delivers to a Telefunc session')
  })

  it('rejects another backend once the Cloudflare one is installed', () => {
    new Telefunc()
    const selected = getRoomBackend()
    expect(() => installBackend(() => new MemoryBackend())).toThrow('a backend is already active')
    expect(getRoomBackend()).toBe(selected)
  })

  it('keeps the same Durable Object Room backend across repeated Worker entry evaluation', () => {
    new Telefunc()
    const installed = getRoomBackend()
    new Telefunc()
    expect(getRoomBackend()).toBe(installed)
    expect(mocks.transportInstances).toHaveLength(1)
  })

  it('reports the missing binding instead of using the memory backend', async () => {
    const { binding } = createBinding()
    const tf = new Telefunc()
    const DurableClass = tf.TelefuncDurableObject
    const instance = new DurableClass(
      { id: { toString: () => 'telefunc-room-binding-probe' } } as unknown as DurableObjectState,
      { TelefuncDurableObject: binding } as unknown as Cloudflare.Env,
    ) as InstanceType<typeof DurableClass> & { fetch(request: Request): Promise<Response> }
    mocks.telefuncMock.mockImplementationOnce(async () => {
      await getRoomBackend().readHead('binding-probe')
      throw new Error('Room backend unexpectedly returned without a binding')
    })
    await expect(instance.fetch(new Request('https://telefunc.test/_telefunc'))).rejects.toThrow(
      'Missing Cloudflare Durable Object binding "TelefuncDurableObject". Add it to your wrangler.jsonc.',
    )
  })

  it('restricts the Room authority and its fan-out coordinators to the jurisdiction', async () => {
    const session = createBinding()
    const env = { TelefuncDurableObject: session.binding } as unknown as Cloudflare.Env
    Object.assign(mocks.workerEnv, env)
    const tf = new Telefunc({ jurisdiction: 'eu' as DurableObjectJurisdiction })
    const DurableClass = tf.TelefuncDurableObject
    const ctx = { id: { toString: () => 'jurisdiction-probe' } } as unknown as DurableObjectState
    // The instance's roles, room authority fanout included, use the namespace it is constructed with.
    const instance = new DurableClass(ctx, env) as InstanceType<typeof DurableClass> & {
      fetch(request: Request): Promise<Response>
    }
    expect(session.jurisdiction).toHaveBeenCalledWith('eu')
    session.jurisdiction.mockClear()
    mocks.telefuncMock.mockImplementationOnce(async () => {
      await getRoomBackend()
        .readHead('jurisdiction-probe')
        .catch(() => {})
      throw new Error('probe done')
    })
    await Promise.resolve(instance.fetch(new Request('https://telefunc.test/_telefunc'))).catch(() => {})
    expect(session.jurisdiction).toHaveBeenCalledWith('eu')
  })

  it('exports one Durable Object class for every role, on the configured binding', () => {
    const tf = new Telefunc({ bindingName: 'CustomTelefuncSession' })
    expect(Object.keys(tf).sort()).toEqual(['TelefuncDurableObject', 'serve'])
    expect(() => new tf.TelefuncDurableObject({} as DurableObjectState, {} as Cloudflare.Env)).toThrow(
      'Missing Cloudflare Durable Object binding "CustomTelefuncSession". Add it to your wrangler.jsonc.',
    )
  })

  it('wires the durable object runtime and delegates fetch, websocket, and broadcast methods', async () => {
    const { binding } = createBinding()
    const tf = new Telefunc({ context: vi.fn(async () => ({ userId: 'user-1' })) })
    const DurableClass = tf.TelefuncDurableObject
    const ctx = { id: { toString: () => 'session-probe-id' } } as unknown as DurableObjectState
    const instance = new DurableClass(ctx, {
      TelefuncDurableObject: binding,
    } as unknown as Cloudflare.Env) as InstanceType<typeof DurableClass> & {
      fetch(request: Request): Promise<Response>
      webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void
      webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): void
      telefuncBroadcastPublish(request: BroadcastPublishRequest): unknown
      telefuncBroadcastForward(request: BroadcastForwardRequest): unknown
      telefuncBroadcastDeliver(request: BroadcastDeliverRequest): void
      telefuncBroadcastPresence(request: BroadcastPresenceRequest): void
      telefuncRoomInvalidate(request: unknown): void
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
    await instance.fetch(
      new Request('https://telefunc.test/_telefunc', {
        headers: { 'x-telefunc-broadcast-bucket': 'weur' },
      }),
    )
    expect(mocks.telefuncMock).toHaveBeenCalled()
    // The session DO is a Broadcast member addressed by its id, placed in the bucket its requests carry.
    const member = mocks.transportInstances[0]!.members[0]!
    expect(member.id).toBe('session-probe-id')
    expect(member.locate).toHaveBeenCalledWith('weur')
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
    const publish = {
      key: 'room:test',
      kind: 'text' as const,
      locationBucket: 'weur' as const,
      payload: new Uint8Array(),
    }
    instance.telefuncBroadcastPublish(publish)
    const publishToSubscribers = mocks.transportInstances[0]!.publishToSubscribers
    expect(publishToSubscribers).toHaveBeenCalledWith(mocks.authorityInstances[0], expect.any(OrderedStubs), publish)
    const forward = { ...publish, info: { seq: 1, timestamp: 1 }, members: ['member-id'] }
    instance.telefuncBroadcastForward(forward)
    // The authority and coordinator roles send through the one DO's ordered stubs.
    expect(mocks.transportInstances[0]?.forwardToBucket).toHaveBeenCalledWith(
      publishToSubscribers.mock.calls[0]![1],
      forward,
    )
    const delivery = {
      key: 'room:test',
      kind: 'text' as const,
      payload: new Uint8Array([1]),
      info: { seq: 1, timestamp: 1 },
    }
    instance.telefuncBroadcastDeliver(delivery)
    expect(member.deliver).toHaveBeenCalledWith(delivery)
    const presence = {
      key: 'room:test',
      kind: 'text' as const,
      member: 'member-id',
      bucket: 'weur' as const,
    }
    instance.telefuncBroadcastPresence(presence)
    expect(mocks.authorityInstances[0]?.setPresence).toHaveBeenCalledWith(presence)
    const invalidation = {
      roomId: 'room',
      inc: 'inc',
      laneKey: 'lane',
      sessionDoId: 'id',
      leaseId: 'lease',
    }
    instance.telefuncRoomInvalidate(invalidation)
  })
})
