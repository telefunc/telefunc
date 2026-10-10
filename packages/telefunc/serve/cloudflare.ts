/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../wire-protocol/server/ws.js'
import { getServerConfig, enableChannelTransports, setAdapterMaxFrameBytes } from '../node/server/serverConfig.js'
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
  assertLocationFallbackIsScaled,
  resolveSessionRoutingTarget,
} from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { assertUsage, assertWarning } from '../utils/assert.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import type { CloudflareScale, LocationBucket } from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { isTelefuncRequest } from './shared.js'

const SHARD_TOKEN_TTL_SECONDS = 86400
/** A session Durable Object writes its pin again once it's this old, well before KV lets it lapse. */
const SHARD_TOKEN_REPIN_MS = (SHARD_TOKEN_TTL_SECONDS * 1000) / 2

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
  // workerd closes a WebSocket that receives a larger message (1009).
  setAdapterMaxFrameBytes(32 * 1024 * 1024)
  const bindingName = options?.bindingName ?? 'TelefuncDurableObject'
  const kvBindingName = options?.kvBindingName ?? 'TelefuncKV'
  const baseInstanceName = options?.instanceName ?? 'telefunc'
  const scale = options?.scale
  const locationFallback = options?.locationFallback ?? 'weur'
  assertLocationFallbackIsScaled(scale, locationFallback)
  const jurisdiction = options?.jurisdiction

  /** When each session token this isolate's Durable Objects serve was last pinned in KV, oldest first. */
  const pinnedAt = new Map<string, number>()
  let sessionKV: KVNamespace | undefined

  /** The Worker routes a token by its KV pin, so a page's requests from elsewhere reach the Durable Object that serves
   *  it, which writes it: once, and again before KV lets it lapse, where a Worker missing KV's cached lookup would write
   *  it on every request of the page's first minute. */
  function pinSession(request: Request): void {
    const token = request.headers.get(TELEFUNC_SESSION_HEADER)
    const shard = request.headers.get(TELEFUNC_SHARD_HEADER)
    const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
    const kv = sessionKV
    if (!token || !shard || !bucket || !kv) return
    const now = Date.now()
    const at = pinnedAt.get(token)
    if (at !== undefined && now - at < SHARD_TOKEN_REPIN_MS) return
    pinnedAt.delete(token)
    pinnedAt.set(token, now)
    for (const [oldToken, oldAt] of pinnedAt) {
      if (now - oldAt < SHARD_TOKEN_TTL_SECONDS * 1000) break
      pinnedAt.delete(oldToken)
    }
    const pin: StoredShardToken = { s: shard, b: bucket }
    // A Durable Object lives on while the write is pending.
    kv.put(`session:${token}`, JSON.stringify(pin), { expirationTtl: SHARD_TOKEN_TTL_SECONDS }).catch(
      (err: unknown) => {
        pinnedAt.delete(token)
        assertWarning(
          false,
          `A session's shard couldn't be pinned in KV, and is retried on its next request: ${String(err)}`,
          {
            onlyOnce: false,
          },
        )
      },
    )
  }

  const channelHooks = getTelefuncChannelHooks()
  const crosswsAdapter = crossws({
    bindingName,
    instanceName: baseInstanceName,
    hooks: {
      ...channelHooks,
      // A page may use its session over its WebSocket alone: each message renews the pin, as a request does, from the
      // upgrade request the socket was opened with.
      message: (peer, message) => {
        pinSession(peer.request)
        return channelHooks.message?.(peer, message)
      },
    },
  })
  // Factory runs only on first install. Bundler quirks can evaluate the user's entry twice in the same isolate;
  // we want every evaluation to share one transport instance.
  const broadcast = installBroadcastAdapter(() => new CloudflareBroadcastTransport({ baseInstanceName, scale }))

  function getBinding(env: Cloudflare.Env): DurableObjectNamespace | undefined {
    const baseBinding = (env as Record<string, DurableObjectNamespace | undefined>)[bindingName]
    return baseBinding && jurisdiction ? baseBinding.jurisdiction(jurisdiction) : baseBinding
  }

  function getKVBinding(env: Cloudflare.Env): KVNamespace | undefined {
    return (env as Record<string, KVNamespace | undefined>)[kvBindingName]
  }

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
      sessionKV = kv
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      crosswsAdapter.handleDurableInit(this, ctx, env)
    }

    async fetch(request: Request) {
      const shard = request.headers.get(TELEFUNC_SHARD_HEADER)
      const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
      if (shard && bucket) broadcast.attachIsolateInfo(shard, bucket)
      pinSession(request)
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
    async serve({ request, env }: ServeInput): Promise<Response | undefined> {
      if (!isTelefuncRequest(request)) return undefined
      const config = getServerConfig()

      const binding = getBinding(env)
      assertUsage(binding, `Missing Cloudflare Durable Object binding "${bindingName}". Add it to your wrangler.jsonc.`)

      const isWebSocketRequest = request.headers.get('upgrade') === 'websocket'
      if (isWebSocketRequest && !config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
        return new Response(null, { status: 400 })
      }

      const kv = getKVBinding(env)
      assertUsage(kv, `Missing Cloudflare KV namespace binding "${kvBindingName}". Add it to your wrangler.jsonc.`)
      const presented = new URL(request.url).searchParams.get('session')
      const token = presented || crypto.randomUUID()
      const stored = presented ? await kv.get<StoredShardToken>(`session:${token}`, 'json') : null
      // A presented token keeps its shard: a client names one before its first call, and a lapsed one stays. The
      // session Durable Object pins it.
      const { sessionInstanceName, locationBucket } = stored
        ? { sessionInstanceName: stored.s, locationBucket: stored.b }
        : resolveSessionRoutingTarget(baseInstanceName, scale, request, locationFallback, token)

      const forwardedHeaders = new Headers(request.headers as Headers)
      forwardedHeaders.set(TELEFUNC_SESSION_HEADER, token)
      forwardedHeaders.set(TELEFUNC_SHARD_HEADER, sessionInstanceName)
      forwardedHeaders.set(TELEFUNC_BROADCAST_BUCKET_HEADER, locationBucket)
      const forwardedRequest = new Request(request, { headers: forwardedHeaders })

      const doResponse = await binding
        .get(binding.idFromName(sessionInstanceName), { locationHint: locationBucket })
        .fetch(forwardedRequest)

      if (!isWebSocketRequest) {
        const headers = new Headers(doResponse.headers)
        headers.set(TELEFUNC_SESSION_HEADER, token)
        return new Response(doResponse.body, { status: doResponse.status, headers })
      }

      return doResponse
    },
    TelefuncDurableObject,
  }
}
