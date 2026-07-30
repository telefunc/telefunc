/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../wire-protocol/server/ws.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import { setDefaultBackend } from '../wire-protocol/backend/install.js'
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
import {
  dispatchRoomShardFanout,
  type RoomShardFanoutNamespace,
  type RoomShardFanoutRequest,
} from '../wire-protocol/server/adapter/cloudflare/room/fanout.js'
import { isAsyncMode } from '../node/server/context/context.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { isTelefuncRequest } from './shared.js'

const SHARD_TOKEN_TTL_SECONDS = 86400
const SESSION_RESET_CLOSE_CODE = 1012
const SESSION_RESET_CLOSE_REASON = 'Telefunc session reset; reconnect'
const cloudflareBackendSlot = getGlobalObject<{
  current?: { identity: string; backend: CloudflareRoomBackend }
}>('serve/cloudflare.backend.ts', () => ({}))

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
  // Stable configuration shares the raw driver without displacing an explicit backend in either call order.
  const backendIdentity = cloudflareBackendIdentity(baseInstanceName, scale)
  let cloudflareBackend = cloudflareBackendSlot.current?.backend
  if (
    cloudflareBackendSlot.current?.identity !== backendIdentity ||
    cloudflareBackend === undefined ||
    cloudflareBackend.disposed
  ) {
    cloudflareBackend = new CloudflareRoomBackend(new CloudflareBroadcastTransport({ baseInstanceName, scale }))
    cloudflareBackendSlot.current = { identity: backendIdentity, backend: cloudflareBackend }
  }
  setDefaultBackend(() => cloudflareBackend, backendIdentity)
  const broadcast = cloudflareBackend.broadcast

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
    private roomManager: CloudflareRoomSessionManager | null = null

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
      return broadcast.publishToSubscribers(this.authorityState, request)
    }

    telefuncBroadcastDeliver(request: BroadcastDeliverRequest) {
      return broadcast.deliverToLocal(request)
    }

    telefuncRoomDeliver(request: RoomShardDeliveryRequest): Promise<void> {
      return this.runWithRoomManager(() => {
        const roomManager = getCloudflareRoomSessionManager()
        if (roomManager !== this.roomManager) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
        return roomManager.deliver(request)
      })
    }

    telefuncRoomInvalidate(request: RoomShardInvalidationRequest): void {
      return this.runWithRoomManager(() => {
        const roomManager = getCloudflareRoomSessionManager()
        if (roomManager !== this.roomManager) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
        return roomManager.invalidate(request)
      })
    }

    telefuncRoomFanout(request: RoomShardFanoutRequest) {
      const binding = getBinding(this.env)
      assertUsage(binding, `Missing Cloudflare Durable Object binding "${bindingName}" during Room fanout.`)
      return dispatchRoomShardFanout(binding as unknown as RoomShardFanoutNamespace, request)
    }

    protected runWithRoomManager<T>(fn: () => T): T {
      // The first real Room call materializes the epoch; ordinary fetch/socket work stays Room-free.
      return isAsyncMode() ? withCloudflareRoomSessionManager(() => this.activateRoomManager(), fn) : fn()
    }

    private activateRoomManager(): CloudflareRoomSessionManager {
      if (this.roomManager) return this.roomManager
      this.roomManager = this.createRoomManager()
      this.closeRecoveredSockets()
      return this.roomManager
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

function cloudflareBackendIdentity(baseInstanceName: string, scale: CloudflareScale | undefined): string {
  const normalizedScale =
    typeof scale === 'object' && scale !== null
      ? Object.entries(scale).sort(([left], [right]) => left.localeCompare(right))
      : (scale ?? null)
  return JSON.stringify([baseInstanceName, normalizedScale])
}
