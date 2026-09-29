/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject, env as workerEnv } from 'cloudflare:workers'
// A subscription finds its session through AsyncLocalStorage, which the setup's compatibility flag enables.
import '../node/server/async_hooks.js'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../wire-protocol/server/ws.js'
import { ChannelMux } from '../wire-protocol/server/mux.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import { installBackend } from '../wire-protocol/backend/install.js'
import {
  CloudflareBroadcastAuthorityState,
  CloudflareBroadcast,
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
  TELEFUNC_SHARD_HEADER,
  assertLocationFallbackIsScaled,
  resolveSessionRoutingTarget,
} from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { assertUsage, assertWarning } from '../utils/assert.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import type { CloudflareScale, LocationBucket } from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { CloudflareBackend } from '../wire-protocol/server/adapter/cloudflare/room/backend.js'
import {
  CloudflareRoomSessionManager,
  type RoomSessionDeliveryRequest,
} from '../wire-protocol/server/adapter/cloudflare/room/subscription.js'
import { RoomAuthority } from '../wire-protocol/server/adapter/cloudflare/room/do.js'
import { withCloudflareSession, type CloudflareSession } from '../wire-protocol/server/adapter/cloudflare/session.js'
import type { TelefuncDurableObjectNamespace } from '../wire-protocol/server/adapter/cloudflare/namespace.js'
import { isTelefuncRequest, toResponse } from './shared.js'

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

  /** When each session token this isolate's Durable Objects serve was last pinned in KV, oldest first. */
  const pinnedAt = new Map<string, number>()

  /** The Worker routes a token by its KV pin, so a page's requests from elsewhere reach the Durable Object that serves
   *  it, which writes it: once, and again before KV lets it lapse, where a Worker missing KV's cached lookup would write
   *  it on every request of the page's first minute. */
  function pinSession(request: Request): void {
    const token = request.headers.get(TELEFUNC_SESSION_HEADER)
    const shard = request.headers.get(TELEFUNC_SHARD_HEADER)
    const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
    if (!token || !shard || !bucket) return
    const now = Date.now()
    const at = pinnedAt.get(token)
    if (at !== undefined && now - at < SHARD_TOKEN_REPIN_MS) return
    const kv = requireBinding<KVNamespace>(workerEnv, kvBindingName, 'KV namespace')
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
  function requireBinding<T>(env: Cloudflare.Env, name: string, kind: string): T {
    const binding = (env as Record<string, T | undefined>)[name]
    assertUsage(binding, `Missing Cloudflare ${kind} binding "${name}". Add it to your wrangler.jsonc.`)
    return binding
  }
  function telefuncNamespace(env: Cloudflare.Env): TelefuncDurableObjectNamespace {
    const namespace = requireBinding<TelefuncDurableObjectNamespace>(env, bindingName, 'Durable Object')
    return jurisdiction ? namespace.jurisdiction(jurisdiction) : namespace
  }
  const cloudflareBackend = installBackend(
    () =>
      new CloudflareBackend({
        rooms: () => telefuncNamespace(workerEnv),
        broadcast: new CloudflareBroadcast({
          baseInstanceName,
          scale,
          locationFallback,
          namespace: () => telefuncNamespace(workerEnv),
        }),
      }),
    [
      'cloudflare',
      bindingName,
      baseInstanceName,
      JSON.stringify(scale ?? null),
      locationFallback,
      jurisdiction ?? null,
    ],
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
      super(ctx, env, telefuncNamespace(env))
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      const id = ctx.id.toString()
      this.session = {
        room: new CloudflareRoomSessionManager(id),
        broadcast: broadcast.member(id, this.broadcastCalls),
        mux: new ChannelMux(),
      }
      crosswsAdapter.handleDurableInit(this, ctx, env)
    }

    async fetch(request: Request) {
      return this.runInSession(async () => {
        const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
        if (bucket) this.session.broadcast.locate(bucket)
        pinSession(request)
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

    telefuncRoomDeliver(request: RoomSessionDeliveryRequest) {
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
