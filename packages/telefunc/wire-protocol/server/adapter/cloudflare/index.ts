/// <reference types="@cloudflare/workers-types" />
export { telefuncWebSocket }
export type { CloudflareWebSocketOptions }

import '../../../../node/server/async_hooks.js'
import { DurableObject } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../../ws.js'
import { getServerConfig, enableChannelTransports } from '../../../../node/server/serverConfig.js'
import { telefunc } from '../../../../node/server/telefunc.js'
import { setPubSubTransport } from '../../pubsub.js'
import { CloudflarePubSubRegistry, CloudflarePubSubTransport } from './pubsub.js'
import type { PubSubDeliverRequest, PubSubPublishRequest } from './pubsub.js'
import { TELEFUNC_PUBSUB_BUCKET_HEADER, TELEFUNC_SHARD_HEADER, resolveSessionRoutingTarget } from './routing.js'
import { assertUsage } from '../../../../utils/assert.js'
import type { Telefunc } from '../../../../node/server/getContext.js'
import type { CloudflareScale, LocationBucket } from './routing.js'
import { CHANNEL_TRANSPORT } from '../../../constants.js'

const SHARD_TOKEN_TTL_SECONDS = 86400

interface TelefuncAdapter {
  /**
   * Normalizes one incoming Telefunc request onto a concrete session shard.
   * Returning `undefined` lets non-Telefunc traffic keep flowing through the user's worker.
   */
  handleTelefunc(request: Request, env: Cloudflare.Env, ctx: ExecutionContext): Promise<Response> | undefined
  /**
   * Produces the Durable Object class used for session traffic, bucket-coordinator RPC, and key-authority RPC.
   * Keeping all roles in one namespace avoids extra bindings and cross-namespace routing.
   */
  createDurableObjectClass(): new (ctx: DurableObjectState, env: Cloudflare.Env) => DurableObject
}

type CloudflareWebSocketOptions = {
  /** Durable Object binding name. Default: `'TelefuncDurableObject'`. */
  bindingName?: string
  /** KV namespace binding name for shard tokens and pub/sub presence. Default: `'TelefuncKV'`. */
  kvBindingName?: string
  /** Instance name for the DO. Default: `'telefunc'`. */
  instanceName?: string
  /** Returns the Telefunc context for each request (`getContext()`). */
  context?: (request: Request, env: Cloudflare.Env) => Telefunc.Context | Promise<Telefunc.Context>
  /**
   * Number of session Durable Objects per active location bucket, or a per-bucket map such as `{ weur: 2, enam: 1 }`.
   * Pub/sub bucket coordinators derive from this at `ceil(sessionScale / 2)` per active bucket. Default: `1`.
   */
  scale?: CloudflareScale
  /** Canonical Cloudflare location hint to use when the incoming request cannot be mapped precisely. Default: `'weur'`. */
  locationFallback?: DurableObjectLocationHint
  /** Restrict Durable Objects to a Cloudflare jurisdiction such as `'eu'` or `'fedramp'`. */
  jurisdiction?: DurableObjectJurisdiction
}

type StoredShardToken = {
  /** Session instance name */
  s: string
  /** Location bucket */
  b: LocationBucket
}

/**
 * Set up Telefunc WebSocket support for Cloudflare Workers.
 *
 * @example
 * ```ts
 * import { telefuncWebSocket } from 'telefunc/websocket/cloudflare'
 *
 * const ws = telefuncWebSocket()
 *
 * export const TelefuncDurableObject = ws.createDurableObjectClass()
 *
 * export default {
 *   async fetch(request: Request, env: Env, ctx: ExecutionContext) {
 *     const resp = ws.handleTelefunc(request, env, ctx)
 *     if (resp) return resp
 *     // ... rest of your fetch handler
 *   },
 * }
 * ```
 *
 * In `wrangler.jsonc`:
 * ```jsonc
 * {
 *   "compatibility_flags": ["nodejs_als"],
 *   "durable_objects": {
 *     "bindings": [{ "name": "TelefuncDurableObject", "class_name": "TelefuncDurableObject" }]
 *   },
 *   "kv_namespaces": [{ "binding": "TelefuncKV", "id": "..." }],
 *   "migrations": [{ "tag": "v1", "new_classes": ["TelefuncDurableObject"] }]
 * }
 * ```
 */
function telefuncWebSocket(options?: CloudflareWebSocketOptions): TelefuncAdapter {
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
  const pubSub = new CloudflarePubSubTransport({
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

  setPubSubTransport(pubSub)

  async function resolveShardAndForward(
    request: Request,
    env: Cloudflare.Env,
    ctx: ExecutionContext,
    binding: DurableObjectNamespace,
    config: ReturnType<typeof getServerConfig>,
  ): Promise<Response> {
    const isWebSocketRequest = request.headers.get('upgrade') === 'websocket'
    if (isWebSocketRequest && !config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
      return new Response(null, { status: 400 })
    }

    const kv = getKVBinding(env)
    assertUsage(kv, `Missing Cloudflare KV namespace binding "${kvBindingName}". Add it to your wrangler.jsonc.`)
    const shardToken = new URL(request.url).searchParams.get('shard')

    let sessionInstanceName: string | undefined
    let locationBucket: LocationBucket | undefined
    let token = shardToken

    // Resolve shard from KV token
    if (token) {
      const stored = await kv.get<StoredShardToken>(`shard:${token}`, 'json')
      if (stored) {
        sessionInstanceName = stored.s
        locationBucket = stored.b
      }
    }

    // Derive from request location if no valid token
    if (!sessionInstanceName || !locationBucket) {
      const target = resolveSessionRoutingTarget(baseInstanceName, scale, request, locationFallback)
      sessionInstanceName = target.sessionInstanceName
      locationBucket = target.locationBucket
      token = crypto.randomUUID()
      const value: StoredShardToken = { s: sessionInstanceName, b: locationBucket }
      ctx.waitUntil(kv.put(`shard:${token}`, JSON.stringify(value), { expirationTtl: SHARD_TOKEN_TTL_SECONDS }))
    }

    const forwardedHeaders = new Headers(request.headers as Headers)
    forwardedHeaders.set(TELEFUNC_SHARD_HEADER, sessionInstanceName)
    forwardedHeaders.set(TELEFUNC_PUBSUB_BUCKET_HEADER, locationBucket)
    const forwardedRequest = new Request(request, { headers: forwardedHeaders })

    const doResponse = await binding
      .get(binding.idFromName(sessionInstanceName), { locationHint: locationBucket })
      .fetch(forwardedRequest)

    // For HTTP responses, set the opaque shard token header
    if (!isWebSocketRequest && token) {
      const headers = new Headers(doResponse.headers)
      headers.set(TELEFUNC_SHARD_HEADER, token)
      return new Response(doResponse.body, { status: doResponse.status, headers })
    }

    return doResponse
  }

  return {
    handleTelefunc(request: Request, env: Cloudflare.Env, ctx: ExecutionContext) {
      const url = new URL(request.url)
      const config = getServerConfig()
      if (!url.pathname.startsWith(config.telefuncUrl)) return undefined
      const binding = getBinding(env)
      if (!binding) return undefined
      return resolveShardAndForward(request, env, ctx, binding, config)
    },

    /**
     * Builds the runtime class for session shards, bucket coordinators, and key authorities.
     * The same class exposes all RPC roles so keyed fanout stays inside the same namespace.
     */
    createDurableObjectClass() {
      const getContext = options?.context
      return class extends DurableObject {
        private readonly pubSubRegistry: CloudflarePubSubRegistry

        constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
          super(ctx, env)
          const binding = getBinding(env)
          assertUsage(
            binding,
            `Missing Cloudflare Durable Object binding "${bindingName}" in Durable Object constructor.`,
          )
          pubSub.attachBinding(binding, bindingName)
          const kv = getKVBinding(env)
          if (kv) pubSub.attachKV(kv)
          this.pubSubRegistry = new CloudflarePubSubRegistry(ctx)
          crosswsAdapter.handleDurableInit(this, ctx, env)
        }

        async fetch(request: Request) {
          const shard = request.headers.get(TELEFUNC_SHARD_HEADER)
          if (shard) {
            pubSub.attachSessionRegistry(shard, this.pubSubRegistry)
          }
          if (request.headers.get('upgrade') === 'websocket') {
            return crosswsAdapter.handleDurableUpgrade(this, request)
          }
          const context = getContext ? await getContext(request, this.env as Cloudflare.Env) : undefined
          const httpResponse = await telefunc(context ? { request, context } : { request })
          return new Response(httpResponse.getReadableWebStream(), {
            status: httpResponse.statusCode,
            headers: httpResponse.headers as HeadersInit,
          })
        }

        webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
          return crosswsAdapter.handleDurableMessage(this, ws, message)
        }

        webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
          return crosswsAdapter.handleDurableClose(this, ws, code, reason, wasClean)
        }

        telefuncPubSubPublish(request: PubSubPublishRequest) {
          return pubSub.publishToSubscribers(this.pubSubRegistry, request)
        }

        telefuncPubSubDeliver(request: PubSubDeliverRequest) {
          return this.pubSubRegistry.deliverLocal(request)
        }
      }
    },
  }
}
