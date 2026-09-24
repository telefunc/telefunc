/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject, env as workerEnv } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../wire-protocol/server/ws.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import { installBackend } from '../wire-protocol/backend/install.js'
import {
  CloudflareBroadcastAuthorityState,
  CloudflareBroadcastTransport,
} from '../wire-protocol/server/adapter/cloudflare/broadcast.js'
import type {
  BroadcastCalls,
  BroadcastDeliverRequest,
  BroadcastForwardRequest,
  BroadcastPublishRequest,
} from '../wire-protocol/server/adapter/cloudflare/broadcast.js'
import { OrderedStubs } from '../wire-protocol/server/adapter/cloudflare/ordered-stubs.js'
import {
  TELEFUNC_BROADCAST_BUCKET_HEADER,
  TELEFUNC_SESSION_HEADER,
  TELEFUNC_SHARD_HEADER,
  assertLocationFallbackIsScaled,
  resolveSessionRoutingTarget,
} from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { assertUsage } from '../utils/assert.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import type { CloudflareScale, LocationBucket } from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import {
  CloudflareRoomSessionManager,
  CloudflareRoomBackend,
  materializeCloudflareRoomSessionManager,
  withCloudflareRoomSessionManager,
  type CloudflareRoomNamespace,
  type RoomSessionDeliveryRequest,
  type RoomSessionInvalidationRequest,
} from '../wire-protocol/server/adapter/cloudflare/room/backend.js'
import { createTelefuncRoomDurableObjectClass } from '../wire-protocol/server/adapter/cloudflare/room/do.js'
import {
  dispatchRoomFanout,
  type RoomFanoutNamespace,
  type RoomFanoutRequest,
} from '../wire-protocol/server/adapter/cloudflare/room/fanout.js'
import { isAsyncMode } from '../node/server/context/context.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { isTelefuncRequest, toResponse } from './shared.js'

const SHARD_TOKEN_TTL_SECONDS = 86400

type CloudflareOptions = {
  bindingName?: string
  kvBindingName?: string
  instanceName?: string
  context?: (request: Request, env: Cloudflare.Env) => TelefuncNamespace.Context | Promise<TelefuncNamespace.Context>
  scale?: CloudflareScale
  locationFallback?: DurableObjectLocationHint
  jurisdiction?: DurableObjectJurisdiction
  roomBindingName?: string
}

type StoredShardToken = {
  s: string
  b: LocationBucket
}

type ServeInput = {
  request: Request
  env: Cloudflare.Env
  ctx: ExecutionContext
}

interface TelefuncServe {
  serve(input: ServeInput): Promise<Response | undefined>
  TelefuncDurableObject: new (ctx: DurableObjectState, env: Cloudflare.Env) => DurableObject
  TelefuncRoomDurableObject: new (ctx: DurableObjectState, env: Cloudflare.Env) => DurableObject
}

interface Telefunc extends TelefuncServe {}
class Telefunc {
  constructor(options?: CloudflareOptions) {
    return telefunc(options)
  }
}

function telefunc(options?: CloudflareOptions): TelefuncServe {
  enableChannelTransports([CHANNEL_TRANSPORT.WS])
  const bindingName = options?.bindingName ?? 'TelefuncDurableObject'
  const kvBindingName = options?.kvBindingName ?? 'TelefuncKV'
  const baseInstanceName = options?.instanceName ?? 'telefunc'
  const scale = options?.scale
  const locationFallback = options?.locationFallback ?? 'weur'
  assertLocationFallbackIsScaled(scale, locationFallback)
  const jurisdiction = options?.jurisdiction
  const roomBindingName = options?.roomBindingName ?? 'TelefuncRoomDurableObject'

  const crosswsAdapter = crossws({
    bindingName,
    instanceName: baseInstanceName,
    hooks: getTelefuncChannelHooks(),
  })
  function requireBinding<T>(env: Cloudflare.Env, name: string, kind: string): T {
    const binding = (env as Record<string, T | undefined>)[name]
    assertUsage(binding, `Missing Cloudflare ${kind} binding "${name}". Add it to your wrangler.jsonc.`)
    return binding
  }
  function scoped(namespace: DurableObjectNamespace): DurableObjectNamespace {
    return jurisdiction ? namespace.jurisdiction(jurisdiction) : namespace
  }
  function sessionNamespace(env: Cloudflare.Env): DurableObjectNamespace {
    return scoped(requireBinding(env, bindingName, 'Durable Object'))
  }
  function roomNamespace(env: Cloudflare.Env): CloudflareRoomNamespace {
    return scoped(requireBinding(env, roomBindingName, 'Room Durable Object')) as unknown as CloudflareRoomNamespace
  }
  function kvNamespace(env: Cloudflare.Env): KVNamespace | undefined {
    return (env as Record<string, KVNamespace | undefined>)[kvBindingName]
  }

  const cloudflareBackend = installBackend(
    () =>
      new CloudflareRoomBackend({
        rooms: () => roomNamespace(workerEnv as Cloudflare.Env),
        broadcast: new CloudflareBroadcastTransport({ baseInstanceName, scale }),
      }),
    ['cloudflare', baseInstanceName, JSON.stringify(scale ?? null), roomBindingName, jurisdiction ?? null],
  )
  const broadcast = cloudflareBackend.broadcast

  const getContext = options?.context

  const TelefuncDurableObject = class extends DurableObject {
    private readonly authorityState: CloudflareBroadcastAuthorityState
    private readonly broadcastCalls: BroadcastCalls = new OrderedStubs()
    private roomManager: CloudflareRoomSessionManager | null = null

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env)
      broadcast.attachBinding(sessionNamespace(env), bindingName)
      const kv = kvNamespace(env)
      if (kv) broadcast.attachKV(kv)
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      crosswsAdapter.handleDurableInit(this, ctx, env)
      // Room subscriptions live in memory, so a socket that used Room before this construction lost them.
      for (const socket of ctx.getWebSockets()) {
        if (socket.deserializeAttachment()?.__telefuncRoom === true)
          socket.close(1012, 'Telefunc session reset; reconnect')
      }
    }

    async fetch(request: Request) {
      return this.runWithRoomManager(async () => {
        const shard = request.headers.get(TELEFUNC_SHARD_HEADER)
        const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
        if (shard && bucket) {
          broadcast.attachIsolateInfo(shard, bucket)
        }
        if (request.headers.get('upgrade') === 'websocket') {
          return crosswsAdapter.handleDurableUpgrade(this, request)
        }
        const context = getContext ? await getContext(request, this.env as Cloudflare.Env) : undefined
        return toResponse(await serveTelefunc(context ? { request, context } : { request }))
      })
    }

    webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
      return this.runWithRoomManager(() => crosswsAdapter.handleDurableMessage(this, ws, message), ws)
    }

    webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
      return this.runWithRoomManager(() => crosswsAdapter.handleDurableClose(this, ws, code, reason, wasClean))
    }

    telefuncBroadcastPublish(request: BroadcastPublishRequest) {
      return broadcast.publishToSubscribers(this.authorityState, this.broadcastCalls, request)
    }

    telefuncBroadcastForward(request: BroadcastForwardRequest) {
      return broadcast.forwardToBucket(this.broadcastCalls, request)
    }

    telefuncBroadcastDeliver(request: BroadcastDeliverRequest) {
      return broadcast.deliverToLocal(request)
    }

    telefuncRoomDeliver(request: RoomSessionDeliveryRequest): Promise<void> {
      return this.runWithRoomManager(() => materializeCloudflareRoomSessionManager().deliver(request))
    }

    telefuncRoomInvalidate(request: RoomSessionInvalidationRequest): void {
      return this.runWithRoomManager(() => materializeCloudflareRoomSessionManager().invalidate(request))
    }

    telefuncRoomFanout(request: RoomFanoutRequest) {
      return dispatchRoomFanout(sessionNamespace(this.env) as unknown as RoomFanoutNamespace, request)
    }

    // Only a Room subscription materializes the manager, and marks the socket it came through.
    private runWithRoomManager<T>(fn: () => T, socket?: WebSocket): T {
      if (!isAsyncMode()) return fn()
      return withCloudflareRoomSessionManager(() => {
        if (socket) markRoomSocket(socket)
        return (this.roomManager ??= new CloudflareRoomSessionManager(this.ctx.id.toString()))
      }, fn)
    }
  }

  const TelefuncRoomDurableObject = createTelefuncRoomDurableObjectClass((env) =>
    sessionNamespace(env as Cloudflare.Env),
  )

  return {
    async serve({ request, env }: ServeInput): Promise<Response | undefined> {
      if (!isTelefuncRequest(request)) return undefined
      const config = getServerConfig()

      const binding = sessionNamespace(env)

      const isWebSocketRequest = request.headers.get('upgrade') === 'websocket'
      if (isWebSocketRequest && !config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
        return new Response(null, { status: 400 })
      }

      const kv = requireBinding<KVNamespace>(env, kvBindingName, 'KV namespace')
      const sessionToken =
        request.headers.get(TELEFUNC_SESSION_HEADER) || new URL(request.url).searchParams.get('session')

      let sessionInstanceName: string | undefined
      let locationBucket: LocationBucket | undefined
      let token = sessionToken

      if (token) {
        const stored = await kv.get<StoredShardToken>(`session:${token}`, 'json')
        if (stored) {
          sessionInstanceName = stored.s
          locationBucket = stored.b
        }
      }

      if (!sessionInstanceName || !locationBucket) {
        const target = resolveSessionRoutingTarget(baseInstanceName, scale, request, locationFallback)
        sessionInstanceName = target.sessionInstanceName
        locationBucket = target.locationBucket
        token = `${sessionInstanceName}:${crypto.randomUUID()}`
        const value: StoredShardToken = { s: sessionInstanceName, b: locationBucket }
        await kv.put(`session:${token}`, JSON.stringify(value), { expirationTtl: SHARD_TOKEN_TTL_SECONDS })
      }

      const forwardedHeaders = new Headers(request.headers as Headers)
      forwardedHeaders.set(TELEFUNC_SHARD_HEADER, sessionInstanceName)
      forwardedHeaders.set(TELEFUNC_BROADCAST_BUCKET_HEADER, locationBucket)
      const forwardedRequest = new Request(request, { headers: forwardedHeaders })

      const doResponse = await binding
        .get(binding.idFromName(sessionInstanceName), { locationHint: locationBucket })
        .fetch(forwardedRequest)

      if (!isWebSocketRequest && token) {
        const headers = new Headers(doResponse.headers)
        headers.set(TELEFUNC_SESSION_HEADER, token)
        return new Response(doResponse.body, { status: doResponse.status, headers })
      }

      return doResponse
    },
    TelefuncDurableObject,
    TelefuncRoomDurableObject,
  }
}

/** Marks the socket in crossws's attachment state, so a later construction knows it used Room. */
function markRoomSocket(socket: WebSocket): void {
  const state = ((socket as WebSocket & { _crosswsState?: Record<string, unknown> })._crosswsState ??
    socket.deserializeAttachment() ??
    {}) as Record<string, unknown>
  socket.serializeAttachment(Object.assign(state, { __telefuncRoom: true }))
}
