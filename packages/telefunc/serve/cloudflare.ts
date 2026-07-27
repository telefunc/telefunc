/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../wire-protocol/server/ws.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import { installBroadcastAdapter } from '../wire-protocol/server/broadcast.js'
import { setDefaultRoomBackend } from '../wire-protocol/backend/install.js'
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
import { assertUsage } from '../utils/assert.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import type { CloudflareScale, LocationBucket } from '../wire-protocol/server/adapter/cloudflare/routing.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import {
  CloudflareRoomSessionManager,
  CloudflareRoomBackend,
  CLOUDFLARE_ROOM_CONTEXT_ERROR,
  getCloudflareRoomSessionManager,
  requireCloudflareRoomNamespace,
  withCloudflareRoomSessionManager,
  type CloudflareRoomNamespace,
  type RoomShardDeliveryRequest,
  type RoomShardInvalidationRequest,
} from '../wire-protocol/server/adapter/cloudflare/room/backend.js'
import { createTelefuncRoomDurableObjectClass } from '../wire-protocol/server/adapter/cloudflare/room/do.js'
import { isAsyncMode } from '../node/server/context/context.js'

const SHARD_TOKEN_TTL_SECONDS = 86400
const SESSION_RESET_CLOSE_CODE = 1012
const SESSION_RESET_CLOSE_REASON = 'Telefunc session reset; reconnect'

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
  // Factory runs only on first install. Bundler quirks can evaluate the user's entry twice in the same isolate;
  // we want every evaluation to share one transport instance.
  const broadcast = installBroadcastAdapter(() => new CloudflareBroadcastTransport({ baseInstanceName, scale }))
  setDefaultRoomBackend(() => new CloudflareRoomBackend(), CloudflareRoomBackend)

  function getBinding(env: Cloudflare.Env): DurableObjectNamespace | undefined {
    const baseBinding = (env as Record<string, DurableObjectNamespace | undefined>)[bindingName]
    return baseBinding && jurisdiction ? baseBinding.jurisdiction(jurisdiction) : baseBinding
  }

  function getKVBinding(env: Cloudflare.Env): KVNamespace | undefined {
    return (env as Record<string, KVNamespace | undefined>)[kvBindingName]
  }

  function getRoomBinding(env: Cloudflare.Env): DurableObjectNamespace {
    return requireCloudflareRoomNamespace(env, roomBindingName) as unknown as DurableObjectNamespace
  }

  const getContext = options?.context

  const TelefuncDurableObject = class extends DurableObject {
    private readonly authorityState: CloudflareBroadcastAuthorityState
    private roomManager!: CloudflareRoomSessionManager

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env)
      const binding = getBinding(env)
      assertUsage(binding, `Missing Cloudflare Durable Object binding "${bindingName}" in Durable Object constructor.`)
      broadcast.attachBinding(binding, bindingName)
      const kv = getKVBinding(env)
      if (kv) broadcast.attachKV(kv)
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      this.resetRoomSessionEpoch()
      crosswsAdapter.handleDurableInit(this, ctx, env)
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
        const httpResponse = await serveTelefunc(context ? { request, context } : { request })
        return new Response(httpResponse.getReadableWebStream(), {
          status: httpResponse.statusCode,
          headers: httpResponse.headers,
        })
      })
    }

    webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
      return this.runWithRoomManager(() => crosswsAdapter.handleDurableMessage(this, ws, message))
    }

    webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
      return this.runWithRoomManager(() => crosswsAdapter.handleDurableClose(this, ws, code, reason, wasClean))
    }

    telefuncBroadcastPublish(request: BroadcastPublishRequest) {
      return this.runWithRoomManager(() => broadcast.publishToSubscribers(this.authorityState, request))
    }

    telefuncBroadcastDeliver(request: BroadcastDeliverRequest) {
      return this.runWithRoomManager(() => broadcast.deliverToLocal(request))
    }

    telefuncRoomDeliver(request: RoomShardDeliveryRequest): Promise<void> {
      return this.runWithRoomManager(() => {
        if (getCloudflareRoomSessionManager() !== this.roomManager) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
        return this.roomManager.deliver(request)
      })
    }

    telefuncRoomInvalidate(request: RoomShardInvalidationRequest): void {
      return this.runWithRoomManager(() => {
        if (getCloudflareRoomSessionManager() !== this.roomManager) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
        return this.roomManager.invalidate(request)
      })
    }

    protected runWithRoomManager<T>(fn: () => T): T {
      // Non-Room Cloudflare keeps its flag-free path. A Room operation calls getCloudflareRoomSessionManager
      // from the proxy and gets the normative diagnostic if async mode was not enabled by the user.
      return isAsyncMode() ? withCloudflareRoomSessionManager(this.roomManager, fn) : fn()
    }

    /** Production epoch transition. Construction and explicit runtime recovery execute this exact path,
     * so a surviving socket can never remain paired with callbacks from a prior JS epoch. */
    protected resetRoomSessionEpoch(): void {
      this.roomManager?.dispose()
      this.roomManager = this.createRoomManager()
      this.closeRecoveredSockets()
    }

    private createRoomManager(): CloudflareRoomSessionManager {
      return new CloudflareRoomSessionManager(
        this.ctx.id.toString(),
        () => getRoomBinding(this.env as Cloudflare.Env) as unknown as CloudflareRoomNamespace,
      )
    }

    private closeRecoveredSockets(): void {
      for (const socket of this.ctx.getWebSockets?.() ?? []) {
        socket.close(SESSION_RESET_CLOSE_CODE, SESSION_RESET_CLOSE_REASON)
      }
    }
  }

  const TelefuncRoomDurableObject = createTelefuncRoomDurableObjectClass(bindingName)

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
    TelefuncRoomDurableObject,
  }
}
