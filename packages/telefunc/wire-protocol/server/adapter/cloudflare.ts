/// <reference types="@cloudflare/workers-types" />
export { telefuncWebSocket }

import { DurableObject } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../ws.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { telefunc } from '../../../node/server/telefunc.js'
import { setCurrentShard } from '../channel.js'
import { assertWarning } from '../../../utils/assert.js'
import type { Telefunc } from '../../../node/server/getContext.js'

/** Return type of {@link telefuncWebSocket}. */
interface TelefuncAdapter {
  /**
   * Add this to your worker's `fetch` handler. It intercepts Telefunc requests
   * (both regular calls and WebSocket connections) and handles them automatically.
   * Returns `undefined` for any request that isn't related to Telefunc.
   */
  handleTelefunc(request: Request, env: Cloudflare.Env, ctx: ExecutionContext): Promise<Response> | undefined
  /**
   * Creates the Durable Object class that powers real-time channels.
   * Export the result from your worker and register it in `wrangler.toml`.
   *
   * @example
   * ```ts
   * export const $TelefuncDurableObject = ws.createDurableObjectClass()
   * ```
   */
  createDurableObjectClass(): new (ctx: DurableObjectState, env: Cloudflare.Env) => DurableObject
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
 * export const $TelefuncDurableObject = ws.createDurableObjectClass()
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
 * In `wrangler.toml`:
 * ```toml
 * [[durable_objects.bindings]]
 * name = "$TelefuncDurableObject"
 * class_name = "$TelefuncDurableObject"
 *
 * [[migrations]]
 * tag = "v1"
 * new_classes = ["$TelefuncDurableObject"]
 * ```
 */
function telefuncWebSocket(options?: {
  /**
   * The name of the Durable Object binding in your `wrangler.toml`.
   * Only change this if you used a custom name instead of `$TelefuncDurableObject`.
   * Default: `$TelefuncDurableObject`.
   */
  bindingName?: string
  /**
   * A name to identify this Telefunc instance.
   * Only relevant if you're running multiple independent Telefunc setups in the same Worker.
   * Default: `telefunc`.
   */
  instanceName?: string
  /**
   * A function that returns the Telefunc context for each request.
   * Use this to attach the logged-in user, auth token, or any other
   * per-request data that your telefunctions can access via `getContext()`.
   *
   * @example
   * ```ts
   * telefuncWebSocket({
   *   context: async (request, env) => ({
   *     user: await getUserFromCookie(request, env),
   *   })
   * })
   * ```
   */
  context?: (request: Request, env: Cloudflare.Env) => Telefunc.Context | Promise<Telefunc.Context>
  /**
   * How many parallel instances to run (default: `1`).
   *
   * Increase this if you have a large number of concurrent open channels and hit
   * memory or CPU limits on a single instance. Each shard handles an independent
   * subset of channels — Telefunc takes care of routing automatically.
   *
   * Start with `1` and only increase if you actually need it.
   */
  shards?: number
  /**
   * Pin each browser session to one shard (default: `true`).
   *
   * When enabled, the server tells the client which shard handled its request.
   * The client then sends that shard back on every subsequent call, so a user's
   * open channels and regular telefunction calls all land on the same Durable
   * Object instance — avoiding redundant WebSocket connections and keeping
   * per-user in-memory state in one place.
   *
   * Only relevant when `shards > 1`. Set to `false` if you intentionally want
   * calls load-balanced across shards (e.g. stateless workers with no channels).
   */
  stickyShards?: boolean
}): TelefuncAdapter {
  const bindingName = options?.bindingName ?? '$TelefuncDurableObject'
  const baseInstanceName = options?.instanceName ?? 'telefunc'
  const shards = options?.shards ?? 1
  const stickyShards = options?.stickyShards ?? true

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
    return binding.get(binding.idFromName(instanceName))
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
      const url = new URL(request.url)
      const telefuncUrl = getServerConfig().telefuncUrl
      if (!url.pathname.startsWith(telefuncUrl)) return undefined
      const binding = getBinding(env)
      if (!binding) return undefined

      const rawShardParam = url.searchParams.get('shard')
      const shardParam = parseShardParam(rawShardParam)

      if (request.headers.get('upgrade') === 'websocket') {
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
          // Set shard context so createChannel() prefixes IDs with the shard index.
          const shard = request.headers.get('x-telefunc-shard')
          if (shard) setCurrentShard(shard)
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
