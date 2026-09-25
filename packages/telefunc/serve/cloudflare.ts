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
  getScaleCountForBucket,
  resolveSessionRoutingTarget,
} from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { assertUsage } from '../utils/assert.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import type { CloudflareScale, LocationBucket } from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import {
  CloudflareBackend,
  type CloudflareRoomNamespace,
} from '../wire-protocol/server/adapter/cloudflare/room/backend.js'
import {
  CloudflareRoomSessionManager,
  type RoomSessionDeliveryRequest,
} from '../wire-protocol/server/adapter/cloudflare/room/subscription.js'
import { RoomAuthority } from '../wire-protocol/server/adapter/cloudflare/room/do.js'
import { withCloudflareSession, type CloudflareSession } from '../wire-protocol/server/adapter/cloudflare/session.js'
import type { RoomSessionNamespace } from '../wire-protocol/server/adapter/cloudflare/room/fanout.js'
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
  function telefuncNamespace(env: Cloudflare.Env): DurableObjectNamespace {
    return scoped(requireBinding(env, bindingName, 'Durable Object'))
  }
  const cloudflareBackend = installBackend(
    () =>
      new CloudflareBackend({
        rooms: () => telefuncNamespace(workerEnv as Cloudflare.Env) as unknown as CloudflareRoomNamespace,
        broadcast: new CloudflareBroadcastTransport({
          baseInstanceName,
          scale,
          locationFallback,
          namespace: () => telefuncNamespace(workerEnv as Cloudflare.Env),
        }),
      }),
    ['cloudflare', baseInstanceName, JSON.stringify(scale ?? null), locationFallback, jurisdiction ?? null],
  )
  const broadcast = cloudflareBackend.broadcast

  const getContext = options?.context

  // One class for every role; an instance's name decides which: a session shard, a Broadcast key authority or
  // coordinator, a room authority or the room directory.
  const TelefuncDurableObject = class extends RoomAuthority<Cloudflare.Env> {
    private readonly authorityState: CloudflareBroadcastAuthorityState
    private readonly broadcastCalls: BroadcastCalls = new OrderedStubs()
    private readonly session: CloudflareSession

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env, telefuncNamespace(env) as unknown as RoomSessionNamespace)
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      const id = ctx.id.toString()
      this.session = {
        room: new CloudflareRoomSessionManager(id),
        broadcast: broadcast.member(id, this.broadcastCalls),
      }
      crosswsAdapter.handleDurableInit(this, ctx, env)
    }

    async fetch(request: Request) {
      return this.runInSession(async () => {
        const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
        if (bucket) this.session.broadcast.locate(bucket)
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
      return this.runInSession(() => this.session.broadcast.deliver(request))
    }

    telefuncBroadcastPresence(request: BroadcastPresenceRequest) {
      return this.authorityState.setPresence(request)
    }

    telefuncRoomDeliver(request: RoomSessionDeliveryRequest): void {
      return this.runInSession(() => this.session.room.deliver(request))
    }

    private runInSession<T>(fn: () => T): T {
      return withCloudflareSession(this.session, fn)
    }
  }

  return {
    async serve({ request, env }: ServeInput): Promise<Response | undefined> {
      if (!isTelefuncRequest(request)) return undefined
      const config = getServerConfig()

      const binding = telefuncNamespace(env)

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
        // A token from before a redeploy that dropped its region routes anew: that region has no Durable Objects now.
        if (stored && getScaleCountForBucket(scale, stored.b) > 0) {
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
