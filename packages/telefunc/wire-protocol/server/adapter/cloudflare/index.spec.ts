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

  class MockCloudflareBroadcast {
    readonly options: unknown
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
    transportInstances: [] as MockCloudflareBroadcast[],
    authorityInstances: [] as MockCloudflareBroadcastAuthorityState[],
    MockCloudflareBroadcastAuthorityState,
    MockCloudflareBroadcast,
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
  getTelefuncChannelHooks: vi.fn(() => ({ message: vi.fn() })),
}))

vi.mock('../../../../node/server/serverConfig.js', () => ({
  getServerConfig: mocks.getServerConfig,
  enableChannelTransports: mocks.enableChannelTransports,
  lowerMaxFrameBytes: vi.fn(),
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
  CloudflareBroadcast: mocks.MockCloudflareBroadcast,
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
import { resolveSessionRoutingTarget } from './routing.js'
import { disposeBackend, getRoomBackend } from '../../../backend/install.js'
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
  const fetch = vi.fn(async (_request: Request) => new Response())
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

  it('derives a new shard and token when no token is provided, and hands the token to the session Durable Object', async () => {
    const { binding, get, fetch } = createBinding()
    const tf = new Telefunc()
    const kv = createMockKV()
    const request = new Request('https://telefunc.test/_telefunc')

    const response = await tf.serve({
      request,
      env: { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      ctx: { waitUntil: vi.fn() } as unknown as ExecutionContext,
    })

    expect(get).toHaveBeenCalledWith(expect.objectContaining({ name: 'telefunc-shard-weur-0' }), {
      locationHint: 'weur',
    })
    expect(fetch).toHaveBeenCalledTimes(1)

    const token = response?.headers.get('x-telefunc-session')
    expect(token).toMatch(/^[0-9a-f-]{36}$/)
    expect((fetch.mock.calls[0]![0] as Request).headers.get('x-telefunc-session')).toBe(token)
  })

  it("leaves a page's pin to its session Durable Object, however many of its first requests miss KV", async () => {
    const { binding } = createBinding()
    const tf = new Telefunc()
    const kv = createMockKV()
    const put = vi.spyOn(kv, 'put')
    for (let i = 0; i < 3; i++) {
      await tf.serve({
        request: new Request('https://telefunc.test/_telefunc?session=new-token'),
        env: { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
        ctx: { waitUntil: (p: Promise<unknown>) => void p } as unknown as ExecutionContext,
      })
    }
    expect(put).not.toHaveBeenCalled()
  })

  it('keeps a presented token whose KV entry lapsed, and routes it by that token again', async () => {
    const { binding } = createBinding()
    const tf = new Telefunc()
    const kv = createMockKV()
    const response = await tf.serve({
      request: new Request('https://telefunc.test/_telefunc?session=lapsed-token'),
      env: { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env,
      ctx: { waitUntil: (p: Promise<unknown>) => void p.then(() => {}) } as unknown as ExecutionContext,
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(vi.mocked(resolveSessionRoutingTarget)).toHaveBeenLastCalledWith(
      'telefunc',
      undefined,
      expect.any(Request),
      'weur',
      'lapsed-token',
    )
    expect(response?.headers.get('x-telefunc-session')).toBe('lapsed-token')
  })

  it('takes the session token from the session query parameter only', async () => {
    const { binding } = createBinding()
    const tf = new Telefunc()
    const response = await tf.serve({
      request: new Request('https://telefunc.test/_telefunc', { headers: { 'x-telefunc-session': 'header-token' } }),
      env: { TelefuncDurableObject: binding, TelefuncKV: createMockKV() } as unknown as Cloudflare.Env,
      ctx: { waitUntil: (p: Promise<unknown>) => void p.then(() => {}) } as unknown as ExecutionContext,
    })
    expect(response?.headers.get('x-telefunc-session')).toMatch(/^[0-9a-f-]{36}$/)
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

  it('passes base options and the Worker env namespace to the Cloudflare Broadcast driver', () => {
    const { binding } = createBinding()
    mocks.workerEnv.TelefuncDurableObject = binding
    new Telefunc()
    const options = mocks.transportInstances[0]?.options as { namespace: () => DurableObjectNamespace }
    expect(options).toEqual(expect.objectContaining({ baseInstanceName: 'telefunc', scale: undefined }))
    expect(options.namespace()).toBe(binding)
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

  it('refuses a second setup on another binding, which the installed backend would not address', () => {
    new Telefunc()
    expect(() => new Telefunc({ bindingName: 'OtherTelefuncDurableObject' })).toThrow(
      'a different backend is already installed',
    )
  })

  it('keeps the same Durable Object Room backend across repeated Worker entry evaluation', () => {
    new Telefunc()
    const installed = getRoomBackend()
    new Telefunc()
    expect(getRoomBackend()).toBe(installed)
    expect(mocks.transportInstances).toHaveLength(1)
  })

  it('serves a telefunction request without a context when the setup names none', async () => {
    const tf = new Telefunc()
    const instance = new tf.TelefuncDurableObject(
      { id: { toString: () => 'session-without-context' } } as unknown as DurableObjectState,
      { TelefuncDurableObject: createBinding().binding } as unknown as Cloudflare.Env,
    ) as InstanceType<typeof tf.TelefuncDurableObject> & { fetch(request: Request): Promise<Response> }
    await instance.fetch(new Request('https://telefunc.test/_telefunc'))
    expect(mocks.telefuncMock).toHaveBeenCalledWith({ request: expect.any(Request) })
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
    }

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
        headers: { 'x-telefunc-broadcast-bucket': 'weur' },
      }),
    )
    expect(mocks.telefuncMock).toHaveBeenCalledWith({ request: expect.any(Request), context: { userId: 'user-1' } })
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
      payload: '"hello"',
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
      payload: '"hello"',
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
  })
})

describe("the session Durable Object's pin", () => {
  /** The request doesn't await its pin's KV write: a macrotask lets it settle. */
  const flush = () => new Promise((resolve) => setTimeout(resolve, 0))
  function sessionObject(kv: KVNamespace) {
    const { binding } = createBinding()
    const tf = new Telefunc()
    const ctx = { id: { name: 'telefunc-shard-weur-0' } }
    const env = { TelefuncDurableObject: binding, TelefuncKV: kv } as unknown as Cloudflare.Env
    // The Worker's env, which `cloudflare:workers` gives its Durable Objects too.
    Object.assign(mocks.workerEnv, env)
    const instance = new tf.TelefuncDurableObject(ctx as unknown as DurableObjectState, env) as unknown as {
      fetch(request: Request): Promise<Response>
    }
    const request = (token: string) =>
      instance.fetch(
        new Request('https://telefunc.test/_telefunc', {
          headers: {
            'x-telefunc-shard': 'telefunc-shard-weur-0',
            'x-telefunc-broadcast-bucket': 'weur',
            'x-telefunc-session': token,
          },
        }),
      )
    return { request }
  }

  it('writes a token once for its first requests, then again only as the pin nears its TTL', async () => {
    const kv = createMockKV()
    const put = vi.spyOn(kv, 'put')
    const { request } = sessionObject(kv)
    await Promise.all([request('token-a'), request('token-a')])
    await request('token-a')
    await flush()
    expect(put).toHaveBeenCalledTimes(1)
    expect(await kv.get('session:token-a', 'json')).toEqual({ s: 'telefunc-shard-weur-0', b: 'weur' })
    vi.useFakeTimers({ now: Date.now() + 13 * 60 * 60 * 1000 })
    try {
      await request('token-a')
    } finally {
      vi.useRealTimers()
    }
    await flush()
    expect(put).toHaveBeenCalledTimes(2)
  })

  it("renews a page's pin as the WebSocket it opened keeps carrying its messages, with no request in between", async () => {
    const kv = createMockKV()
    const put = vi.spyOn(kv, 'put')
    const { request } = sessionObject(kv)
    const hooks = (mocks.crosswsFactory.mock.calls.at(-1) as unknown as [{ hooks: Record<string, Function> }])[0].hooks
    // The request the page's WebSocket was opened with, as the socket's peer keeps it.
    const upgrade = new Request('https://telefunc.test/_telefunc?session=token-c', {
      headers: {
        'x-telefunc-shard': 'telefunc-shard-weur-0',
        'x-telefunc-broadcast-bucket': 'weur',
        'x-telefunc-session': 'token-c',
      },
    })
    await request('token-c')
    await flush()
    expect(put).toHaveBeenCalledTimes(1)
    vi.useFakeTimers({ now: Date.now() + 13 * 60 * 60 * 1000 })
    try {
      hooks.message!({ request: upgrade }, { uint8Array: () => new Uint8Array() })
    } finally {
      vi.useRealTimers()
    }
    await flush()
    expect(put).toHaveBeenCalledTimes(2)
  })

  it('reports a pin it failed to write, and writes it on the next request', async () => {
    const kv = createMockKV()
    const put = vi.spyOn(kv, 'put').mockRejectedValueOnce(new Error('KV is down'))
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { request } = sessionObject(kv)
    await request('token-b')
    await flush()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('KV is down'))
    await request('token-b')
    await flush()
    expect(put).toHaveBeenCalledTimes(2)
    warn.mockRestore()
  })
})
