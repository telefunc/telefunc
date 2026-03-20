/// <reference types="@cloudflare/workers-types" />
export { telefuncWebSocket }

import { DurableObject } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../ws.js'
import { getServerConfig, enableChannelTransports } from '../../../node/server/serverConfig.js'
import { telefunc } from '../../../node/server/telefunc.js'
import { assertWarning } from '../../../utils/assert.js'
import type { Telefunc } from '../../../node/server/getContext.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'

/** Return type of {@link telefuncWebSocket}. */
interface TelefuncAdapter {
  /** Intercepts Telefunc requests (HTTP + WebSocket). Returns `undefined` for non-Telefunc requests. */
  handleTelefunc(request: Request, env: Cloudflare.Env, ctx: ExecutionContext): Promise<Response> | undefined
  /** Creates the Durable Object class. Export it from your worker and register it in `wrangler.toml`. */
  createDurableObjectClass(): new (ctx: DurableObjectState, env: Cloudflare.Env) => DurableObject
}

type CloudflareWebSocketOptions = {
  /** Durable Object binding name. Default: `'TelefuncDurableObject'`. */
  bindingName?: string
  /** Instance name for the DO. Default: `'telefunc'`. */
  instanceName?: string
  /** Returns the Telefunc context for each request (`getContext()`). */
  context?: (request: Request, env: Cloudflare.Env) => Telefunc.Context | Promise<Telefunc.Context>
  /** Number of parallel Durable Objects. Default: `1`. */
  shards?: number
  /** Pin each browser session to one shard. Default: `true`. Only relevant when `shards > 1`. */
  stickyShards?: boolean
  /** Location hint for initial DO placement. See CF docs for valid values (e.g. `'weur'`, `'enam'`). */
  locationHint?: DurableObjectLocationHint
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
 *   "durable_objects": {
 *     "bindings": [{ "name": "TelefuncDurableObject", "class_name": "TelefuncDurableObject" }]
 *   },
 *   "migrations": [{ "tag": "v1", "new_classes": ["TelefuncDurableObject"] }]
 * }
 * ```
 */
function telefuncWebSocket(options?: CloudflareWebSocketOptions): TelefuncAdapter {
  const bindingName = options?.bindingName ?? 'TelefuncDurableObject'
  const baseInstanceName = options?.instanceName ?? 'telefunc'
  const shards = options?.shards ?? 1
  const stickyShards = options?.stickyShards ?? true
  const locationHint = options?.locationHint

  const ws = crossws({
    bindingName,
    instanceName: baseInstanceName,
    hooks: getTelefuncChannelHooks(),
  })

  function getBinding(env: Cloudflare.Env): DurableObjectNamespace | undefined {
    return (env as Record<string, DurableObjectNamespace | undefined>)[bindingName]
  }

  function getStub(binding: DurableObjectNamespace, shardIndex: number): DurableObjectStub {
    const instanceName = shards > 1 ? `${baseInstanceName}-${shardIndex}` : baseInstanceName
    return binding.get(binding.idFromName(instanceName), locationHint ? { locationHint } : undefined)
  }

  function randomShard(): number {
    return shards > 1 ? Math.floor(Math.random() * shards) : 0
  }

  /** Parse a trusted ?shard= param (sent by our own client). Returns null if absent or invalid. */
  function parseShardParam(param: string | null): number | null {
    if (param === null) return null
    const n = Number(param)
    return Number.isInteger(n) && n >= 0 && n < shards ? n : null
  }

  return {
    handleTelefunc(request: Request, env: Cloudflare.Env, _ctx: ExecutionContext) {
      enableChannelTransports([CHANNEL_TRANSPORT.WS])
      const url = new URL(request.url)
      const config = getServerConfig()
      if (!url.pathname.startsWith(config.telefuncUrl)) return undefined
      const binding = getBinding(env)
      if (!binding) return undefined

      const rawShardParam = url.searchParams.get('shard')
      const shardParam = parseShardParam(rawShardParam)

      if (request.headers.get('upgrade') === 'websocket') {
        if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
          return Promise.resolve(new Response(null, { status: 400 }))
        }
        assertWarning(
          shardParam !== null,
          'WebSocket upgrade received without a valid ?shard= param — falling back to shard 0. This usually means a proxy stripped the query string. Check that your infrastructure forwards the full URL.',
          { onlyOnce: false },
        )
        return getStub(binding, shardParam ?? 0).fetch(request)
      }

      // HTTP: honour the client's sticky shard when present, otherwise pick randomly.
      if (rawShardParam !== null && shardParam === null) {
        assertWarning(
          false,
          `HTTP request received an invalid ?shard= param (${rawShardParam}) — falling back to a random shard. This likely means shard stickiness is broken. Check that the shard value is not being modified in transit.`,
          { onlyOnce: false },
        )
      }
      const shardIndex = shardParam ?? randomShard()
      const forwardedHeaders = new Headers(request.headers as Headers)
      forwardedHeaders.set('x-telefunc-shard', String(shardIndex))
      const forwardedRequest = new Request(request, { headers: forwardedHeaders })
      return getStub(binding, shardIndex).fetch(forwardedRequest)
    },

    createDurableObjectClass() {
      const adapter = ws
      const getContext = options?.context
      return class extends DurableObject {
        constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
          super(ctx, env)
          adapter.handleDurableInit(this, ctx, env)
        }
        async fetch(request: Request) {
          if (request.headers.get('upgrade') === 'websocket') {
            return adapter.handleDurableUpgrade(this, request)
          }
          const shard = request.headers.get('x-telefunc-shard')
          const context = getContext ? await getContext(request, this.env as Cloudflare.Env) : undefined
          const httpResponse = await telefunc(context ? { request, context } : { request })
          const responseHeaders = new Headers(httpResponse.headers as HeadersInit)
          // Always echo the shard so the client can route WS connections to the right DO.
          if (shard) {
            responseHeaders.set('x-telefunc-shard', shard)
            if (stickyShards) responseHeaders.set('x-telefunc-sticky', 'true')
          }
          return new Response(httpResponse.getReadableWebStream(), {
            status: httpResponse.statusCode,
            headers: responseHeaders,
          })
        }
        webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
          return adapter.handleDurableMessage(this, ws, message)
        }
        webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
          return adapter.handleDurableClose(this, ws, code, reason, wasClean)
        }
        webSocketPublish(topic: string, message: unknown, opts: unknown) {
          return adapter.handleDurablePublish(this, topic, message, opts)
        }
      }
    },
  }
}
