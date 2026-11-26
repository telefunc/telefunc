import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import { OrderedStubs } from './OrderedStubs.js'
import type { HttpResponse } from '../../../node/server/runTelefunc.js'

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

  class MockCloudflareBroadcast {
    readonly options: unknown
    readonly publishToSubscribers = vi.fn()
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

  /** A JSON answer of Telefunc's `serve()`, as far as the adapter reads it. */
  function jsonResponse(): Pick<HttpResponse, 'statusCode' | 'headers' | 'getReadableWebStream'> {
    return {
      statusCode: 200,
      headers: [['content-type', 'application/json']],
      getReadableWebStream() {
        return new ReadableStream()
      },
    }
  }

  return {
    crosswsAdapter,
    crosswsFactory: vi.fn(() => crosswsAdapter),
    setAdapterMaxFrameBytes: vi.fn(),
    setAdapterMaxMemory: vi.fn(),
    getServerConfig: vi.fn(() => ({ telefuncUrl: '/_telefunc', channel: { transports: ['WS'] } })),
    jsonResponse,
    telefuncMock: vi.fn(async () => jsonResponse()),
    transportInstances: new Array<MockCloudflareBroadcast>(),
    authorityInstances: new Array<MockCloudflareBroadcastAuthorityState>(),
    MockCloudflareBroadcastAuthorityState,
    MockCloudflareBroadcast,
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

vi.mock('../../channel/server/ws.js', () => ({
  getTelefuncChannelOptions: vi.fn(() => ({ hooks: {} })),
}))

vi.mock('../../../node/server/serverConfig.js', () => ({
  getServerConfig: mocks.getServerConfig,
  getTelefuncUrl: () => '/_telefunc',
  setAdapterMaxFrameBytes: mocks.setAdapterMaxFrameBytes,
  setAdapterMaxMemory: mocks.setAdapterMaxMemory,
}))

vi.mock('../../../node/server/telefunc.js', () => ({
  serveRuntimeRequest: mocks.telefuncMock,
}))

vi.mock('../../../node/server/runTelefunc/loadTelefuncFilesUsingVite.js', () => ({
  preloadServerProductionEntry: vi.fn(async () => {}),
}))

vi.mock('./CloudflareBroadcastAuthorityState.js', () => ({
  CloudflareBroadcastAuthorityState: mocks.MockCloudflareBroadcastAuthorityState,
}))

vi.mock('./CloudflareBroadcast.js', () => ({
  CloudflareBroadcast: mocks.MockCloudflareBroadcast,
}))

import { Telefunc } from '../../../serve/cloudflare.js'
import { resolveSessionRoutingTarget } from './routing.js'
import { disposeBackend } from '../../backend/install.js'
import type { BroadcastPublishRequest } from './broadcastRpc.js'
import type { BroadcastDelivery } from './extensions.js'
import { assert } from '../../../utils/assert.js'
import { durableObjectState, webSocket } from '../../test/cloudflareRuntime.js'
import { upgradesTakenHere } from '../../../node/server/runTelefunc/takesUpgrades.js'

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
  /** The Worker's env, whose `TelefuncDurableObject` is the binding. */
  const bindings = { TelefuncDurableObject: binding }
  const env: Cloudflare.Env = bindings
  return { binding, env, get, fetch, idFromName, jurisdiction }
}

beforeEach(() => {
  mocks.crosswsFactory.mockClear()
  mocks.crosswsAdapter.handleDurableInit.mockReset()
  mocks.crosswsAdapter.handleDurableUpgrade.mockReset()
  mocks.crosswsAdapter.handleDurableMessage.mockReset()
  mocks.crosswsAdapter.handleDurableClose.mockReset()
  mocks.setAdapterMaxFrameBytes.mockClear()
  mocks.getServerConfig.mockReset()
  mocks.getServerConfig.mockReturnValue({ telefuncUrl: '/_telefunc', channel: { transports: ['WS'] } })
  mocks.telefuncMock.mockClear()
  mocks.telefuncMock.mockResolvedValue(mocks.jsonResponse())
  mocks.transportInstances.length = 0
  mocks.authorityInstances.length = 0
})

afterEach(() => disposeBackend())

describe('cloudflare adapter entrypoint', () => {
  it('routes by the home its page names and forwards routing headers', async () => {
    const { env, get, fetch } = createBinding()
    const tf = new Telefunc()
    const request = new Request('https://telefunc.test/_telefunc?session=my-token&home=oc', {
      headers: { 'x-telefunc-request': 'txt' },
    })

    await tf.serve({
      request,
      env,
    })

    expect(mocks.transportInstances).toHaveLength(1)
    expect(get).toHaveBeenCalledWith(expect.objectContaining({ name: 'telefunc-shard-oc-0' }), { locationHint: 'oc' })
    expect(fetch).toHaveBeenCalledTimes(1)

    const [forwarded] = fetch.mock.calls
    assert(forwarded !== undefined)
    const [forwardedRequest] = forwarded
    expect(forwardedRequest.headers.get('x-telefunc-shard')).toBe('telefunc-shard-oc-0')
    expect(forwardedRequest.headers.get('x-telefunc-broadcast-bucket')).toBe('oc')
  })

  it('returns undefined for non-telefunc traffic', async () => {
    const tf = new Telefunc()

    await expect(
      tf.serve({
        request: new Request('https://telefunc.test/other'),
        env: {},
      }),
    ).resolves.toBeUndefined()
  })

  it('asserts when binding is missing for telefunc traffic', async () => {
    const tf = new Telefunc()

    await expect(
      tf.serve({
        request: new Request('https://telefunc.test/_telefunc'),
        env: {},
      }),
    ).rejects.toThrow('Missing Cloudflare Durable Object binding')
  })

  it('returns 400 for websocket upgrades when websocket transport is disabled', async () => {
    const { env } = createBinding()
    mocks.getServerConfig.mockReturnValue({ telefuncUrl: '/_telefunc', channel: { transports: [] } })
    const tf = new Telefunc()
    const request = new Request('https://telefunc.test/_telefunc', { headers: { upgrade: 'websocket' } })

    const response = await tf.serve({
      request,
      env,
    })

    expect(response?.status).toBe(400)
  })

  it.each([
    ['an SSE request of another wire version', { 'x-telefunc-request': 'sse' }, 409, 'reload'],
    ['a request that names no kind', {}, 400, 'route'],
  ])('refuses %s as Telefunc, which says so in its media type', async (_request, headers, status, kind) => {
    const { binding } = createBinding()
    const env = { TelefuncDurableObject: binding }
    const tf = new Telefunc()
    const request = new Request('https://telefunc.test/_telefunc', { method: 'POST', headers, body: 'x' })

    const response = await tf.serve({ request, env })

    expect(response?.status).toBe(status)
    expect(response?.headers.get('content-type')).toMatch(
      new RegExp(`^text/vnd\\.telefunc\\.refusal; kind=${kind}; reason="[^"]+"$`),
    )
  })

  it('applies jurisdiction wrapping before binding lookups', async () => {
    const { env, jurisdiction } = createBinding()
    const tf = new Telefunc({ jurisdiction: 'eu' })

    await tf.serve({
      request: new Request('https://telefunc.test/_telefunc'),
      env,
    })

    expect(jurisdiction).toHaveBeenCalledWith('eu')
  })

  it('passes base transport options to the broadcast transport', () => {
    new Telefunc()

    expect(mocks.transportInstances[0]?.options).toEqual(
      expect.objectContaining({ baseInstanceName: 'telefunc', scale: undefined }),
    )
  })

  it('wires the durable object runtime and delegates fetch, websocket, and broadcast methods', async () => {
    const { binding, env } = createBinding()
    const tf = new Telefunc({ context: vi.fn(async () => ({ userId: 'user-1' })) })
    const DurableClass = tf.TelefuncDurableObject
    const ctx = durableObjectState('telefunc-shard-weur-1')
    // The class's public type is a `DurableObject`, which names none of the methods Cloudflare calls on it.
    const instance = new DurableClass(ctx, env) as InstanceType<typeof DurableClass> & {
      fetch(request: Request): Promise<Response>
      webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void
      webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): void
      telefuncBroadcastPublish(request: BroadcastPublishRequest): unknown
      telefuncBroadcastDeliver(delivery: BroadcastDelivery): void
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
        headers: { 'x-telefunc-shard': 'telefunc-shard-weur-1', 'x-telefunc-broadcast-bucket': 'weur' },
      }),
    )
    // The Durable Object answers the upgrade itself, so its pages are offered WebSocket.
    expect(mocks.telefuncMock).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ clientGone: expect.any(AbortSignal), takesUpgrades: upgradesTakenHere }),
    )
    expect(response.headers.get('x-telefunc-home')).toBe('weur')
    const [transport] = mocks.transportInstances
    const [member] = transport?.members ?? []
    assert(member !== undefined)
    expect(member.locate).toHaveBeenCalledWith('weur')

    instance.webSocketMessage(webSocket(vi.fn()), 'payload')
    expect(mocks.crosswsAdapter.handleDurableMessage).toHaveBeenCalledWith(instance, expect.anything(), 'payload')

    const closing = vi.fn()
    instance.webSocketClose(webSocket(closing), 1000, 'done', true)
    expect(closing).toHaveBeenCalledWith(1000, 'done')
    expect(mocks.crosswsAdapter.handleDurableClose).toHaveBeenCalledWith(
      instance,
      expect.anything(),
      1000,
      'done',
      true,
    )

    const publish = { key: 'room:test', kind: 'text' as const, payload: '{"text":"hello"}' }
    instance.telefuncBroadcastPublish(publish)
    expect(mocks.transportInstances[0]?.publishToSubscribers).toHaveBeenCalledWith(
      mocks.authorityInstances[0],
      expect.any(OrderedStubs),
      publish,
    )

    const delivery = {
      routes: [{ key: 'room:test', kind: 'text' as const }],
      payload: '{"text":"hello"}',
      info: { seq: 1, timestamp: Date.now() },
    }
    instance.telefuncBroadcastDeliver(delivery)
    expect(member.deliver).toHaveBeenCalledWith(delivery)
  })
})
