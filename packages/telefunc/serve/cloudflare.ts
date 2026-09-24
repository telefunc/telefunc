/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject, env as workerEnv } from 'cloudflare:workers'
// A subscription finds its session through AsyncLocalStorage, which the setup's compatibility flag enables.
import '../node/server/async_hooks.js'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../wire-protocol/server/ws.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import { installBackend } from '../wire-protocol/backend/install.js'
import {
  CloudflareBroadcastAuthorityState,
  CloudflareBroadcastTransport,
  type CloudflareBroadcastMember,
} from '../wire-protocol/server/adapter/cloudflare/broadcast.js'
import type {
  BroadcastCalls,
  BroadcastDeliverRequest,
  BroadcastForwardRequest,
  BroadcastPresenceRequest,
  BroadcastPublishRequest,
} from '../wire-protocol/server/adapter/cloudflare/broadcast.js'
import { OrderedStubs } from '../wire-protocol/server/adapter/cloudflare/ordered-stubs.js'
import {
  TELEFUNC_BROADCAST_BUCKET_HEADER,
  TELEFUNC_SESSION_HEADER,
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
  type CloudflareRoomNamespace,
  type RoomSessionDeliveryRequest,
  type RoomSessionInvalidationRequest,
} from '../wire-protocol/server/adapter/cloudflare/room/backend.js'
import { RoomAuthorityHost } from '../wire-protocol/server/adapter/cloudflare/room/do.js'
import { withCloudflareSession, type CloudflareSession } from '../wire-protocol/server/adapter/cloudflare/session.js'
import {
  dispatchRoomFanout,
  type RoomFanoutNamespace,
  type RoomFanoutRequest,
} from '../wire-protocol/server/adapter/cloudflare/room/fanout.js'
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
  const cloudflareBackend = installBackend(
    () =>
      new CloudflareRoomBackend({
        rooms: () => sessionNamespace(workerEnv as Cloudflare.Env) as unknown as CloudflareRoomNamespace,
        broadcast: new CloudflareBroadcastTransport({
          baseInstanceName,
          scale,
          namespace: () => sessionNamespace(workerEnv as Cloudflare.Env),
        }),
      }),
    ['cloudflare', baseInstanceName, JSON.stringify(scale ?? null), jurisdiction ?? null],
  )
  const broadcast = cloudflareBackend.broadcast

  const getContext = options?.context

  // One class for every role; an instance's name decides which: a session shard, a Broadcast key authority or
  // coordinator, a room authority, the room directory or a room fanout coordinator.
  const TelefuncDurableObject = class extends RoomAuthorityHost<Cloudflare.Env> {
    private readonly authorityState: CloudflareBroadcastAuthorityState
    private readonly broadcastCalls: BroadcastCalls = new OrderedStubs()
    private readonly broadcastMember: CloudflareBroadcastMember
    private roomManager: CloudflareRoomSessionManager | null = null
    // Only a Room subscription materializes the manager.
    private readonly session: CloudflareSession = {
      room: () => (this.roomManager ??= new CloudflareRoomSessionManager(this.ctx.id.toString())),
      broadcast: () => this.broadcastMember,
    }

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env, sessionNamespace(env) as unknown as RoomFanoutNamespace)
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      this.broadcastMember = broadcast.member(ctx.id.toString(), this.broadcastCalls)
      crosswsAdapter.handleDurableInit(this, ctx, env)
    }

    async fetch(request: Request) {
      return this.runInSession(async () => {
        const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
        if (bucket) this.broadcastMember.locate(bucket)
        if (request.headers.get('upgrade') === 'websocket') {
          return crosswsAdapter.handleDurableUpgrade(this, request)
        }
        const context = getContext ? await getContext(request, this.env as Cloudflare.Env) : undefined
        return toResponse(await serveTelefunc(context ? { request, context } : { request }))
      })
    }

    webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
      return this.runInSession(() => crosswsAdapter.handleDurableMessage(this, ws, message))
    }

    webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
      return this.runInSession(() => crosswsAdapter.handleDurableClose(this, ws, code, reason, wasClean))
    }

    telefuncBroadcastPublish(request: BroadcastPublishRequest) {
      return broadcast.publishToSubscribers(this.authorityState, this.broadcastCalls, request)
    }

    telefuncBroadcastForward(request: BroadcastForwardRequest) {
      return broadcast.forwardToBucket(this.broadcastCalls, request)
    }

    telefuncBroadcastDeliver(request: BroadcastDeliverRequest) {
      return this.runInSession(() => this.broadcastMember.deliver(request))
    }

    telefuncBroadcastPresence(request: BroadcastPresenceRequest) {
      return this.authorityState.setPresence(request)
    }

    telefuncRoomDeliver(request: RoomSessionDeliveryRequest): void {
      return this.runInSession(() => this.session.room().deliver(request))
    }

    telefuncRoomInvalidate(request: RoomSessionInvalidationRequest): void {
      return this.runInSession(() => this.session.room().invalidate(request))
    }

    telefuncRoomFanout(request: RoomFanoutRequest) {
      return dispatchRoomFanout(sessionNamespace(this.env) as unknown as RoomFanoutNamespace, request)
    }

    private runInSession<T>(fn: () => T): T {
      return withCloudflareSession(this.session, fn)
    }
  }

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
  }
}
