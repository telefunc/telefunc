/// <reference types="@cloudflare/workers-types" />

export { telefunc }
export type { CloudflareWebSocketOptions }

import { DurableObject } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../wire-protocol/server/ws.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import { installBroadcastAdapter } from '../wire-protocol/server/broadcast.js'
import {
  CloudflareBroadcastAuthorityState,
  CloudflareBroadcastTransport,
} from '../wire-protocol/server/adapter/cloudflare/broadcast.js'
import type {
  BroadcastDeliverRequest,
  BroadcastPublishRequest,
} from '../wire-protocol/server/adapter/cloudflare/broadcast.js'
import {
  TELEFUNC_BROADCAST_BUCKET_HEADER,
  TELEFUNC_SESSION_HEADER,
  TELEFUNC_SHARD_HEADER,
  resolveSessionRoutingTarget,
} from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { assertUsage } from '../utils/assert.js'
import type { Telefunc } from '../node/server/context/getContext.js'
import type { CloudflareScale, LocationBucket } from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'

const SHARD_TOKEN_TTL_SECONDS = 86400

type CloudflareWebSocketOptions = {
  bindingName?: string
  kvBindingName?: string
  instanceName?: string
  context?: (request: Request, env: Cloudflare.Env) => Telefunc.Context | Promise<Telefunc.Context>
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

function telefunc(options?: CloudflareWebSocketOptions): TelefuncServe {
  enableChannelTransports([CHANNEL_TRANSPORT.WS])
  const bindingName = options?.bindingName ?? 'TelefuncDurableObject'
  const kvBindingName = options?.kvBindingName ?? 'TelefuncKV'
  const baseInstanceName = options?.instanceName ?? 'telefunc'
  const scale = options?.scale
  const locationFallback = options?.locationFallback ?? 'weur'
  const jurisdiction = options?.jurisdiction

  const crosswsAdapter = crossws({
    bindingName,
    instanceName: baseInstanceName,
    hooks: getTelefuncChannelHooks(),
  })
  const broadcast = new CloudflareBroadcastTransport({
    baseInstanceName,
    scale,
  })

  function getBinding(env: Cloudflare.Env): DurableObjectNamespace | undefined {
    const baseBinding = (env as Record<string, DurableObjectNamespace | undefined>)[bindingName]
    return baseBinding && jurisdiction ? baseBinding.jurisdiction(jurisdiction) : baseBinding
  }

  function getKVBinding(env: Cloudflare.Env): KVNamespace | undefined {
    return (env as Record<string, KVNamespace | undefined>)[kvBindingName]
  }

  installBroadcastAdapter(broadcast)

  const getContext = options?.context

  const TelefuncDurableObject = class extends DurableObject {
    private readonly authorityState: CloudflareBroadcastAuthorityState

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env)
      const binding = getBinding(env)
      assertUsage(binding, `Missing Cloudflare Durable Object binding "${bindingName}" in Durable Object constructor.`)
      broadcast.attachBinding(binding, bindingName)
      const kv = getKVBinding(env)
      if (kv) broadcast.attachKV(kv)
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      crosswsAdapter.handleDurableInit(this, ctx, env)
    }

    async fetch(request: Request) {
      const shard = request.headers.get(TELEFUNC_SHARD_HEADER)
      const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
      if (shard && bucket) {
        broadcast.attachIsolateInfo(shard, bucket)
      }
      if (request.headers.get('upgrade') === 'websocket') {
        return crosswsAdapter.handleDurableUpgrade(this, request)
      }
      const context = getContext ? await getContext(request, this.env as Cloudflare.Env) : undefined
      const httpResponse = await serveTelefunc(context ? { request, context } : { request })
      return new Response(httpResponse.getReadableWebStream(), {
        status: httpResponse.statusCode,
        headers: httpResponse.headers,
      })
    }

    webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
      return crosswsAdapter.handleDurableMessage(this, ws, message)
    }

    webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
      return crosswsAdapter.handleDurableClose(this, ws, code, reason, wasClean)
    }

    telefuncBroadcastPublish(request: BroadcastPublishRequest) {
      return broadcast.publishToSubscribers(this.authorityState, request)
    }

    telefuncBroadcastDeliver(request: BroadcastDeliverRequest) {
      broadcast.deliverToLocal(request)
    }
  }

  return {
    async serve({ request, env, ctx }: ServeInput): Promise<Response | undefined> {
      const config = getServerConfig()
      if (!new URL(request.url).pathname.startsWith(config.telefuncUrl)) return undefined

      const binding = getBinding(env)
      assertUsage(binding, `Missing Cloudflare Durable Object binding "${bindingName}". Add it to your wrangler.jsonc.`)

      const isWebSocketRequest = request.headers.get('upgrade') === 'websocket'
      if (isWebSocketRequest && !config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
        return new Response(null, { status: 400 })
      }

      const kv = getKVBinding(env)
      assertUsage(kv, `Missing Cloudflare KV namespace binding "${kvBindingName}". Add it to your wrangler.jsonc.`)
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
        ctx.waitUntil(kv.put(`session:${token}`, JSON.stringify(value), { expirationTtl: SHARD_TOKEN_TTL_SECONDS }))
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
  }
}
