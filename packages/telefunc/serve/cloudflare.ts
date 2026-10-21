/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject, env as workerEnv } from 'cloudflare:workers'
// A subscription finds its session through AsyncLocalStorage, which the setup's compatibility flag enables.
import '../node/server/async_hooks.js'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../wire-protocol/channel/server/ws.js'
import { ChannelMux } from '../wire-protocol/channel/server/mux.js'
import { getServerConfig, enableChannelTransports, setAdapterMaxFrameBytes } from '../node/server/serverConfig.js'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import { installBackend } from '../wire-protocol/backend/install.js'
import {
  CloudflareBroadcastAuthorityState,
  CloudflareBroadcast,
} from '../wire-protocol/adapter/cloudflare/broadcast.js'
import type {
  BroadcastCalls,
  BroadcastDeliverRequest,
  BroadcastForwardRequest,
  BroadcastPresenceRequest,
  BroadcastPublishRequest,
} from '../wire-protocol/adapter/cloudflare/broadcast.js'
import { OrderedStubs } from '../wire-protocol/adapter/cloudflare/OrderedStubs.js'
import {
  TELEFUNC_BROADCAST_BUCKET_HEADER,
  TELEFUNC_SESSION_HEADER,
  TELEFUNC_SHARD_HEADER,
  assertLocationFallbackIsScaled,
  resolveSessionRoutingTarget,
} from '../wire-protocol/adapter/cloudflare/routing.js'
import { SessionPins, readSessionPin } from '../wire-protocol/adapter/cloudflare/sessionPin.js'
import { assertUsage } from '../utils/assert.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import type { CloudflareScale, LocationBucket } from '../wire-protocol/adapter/cloudflare/routing.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { CloudflareBackend } from '../wire-protocol/adapter/cloudflare/backend.js'
import {
  CloudflareRoomSessionManager,
  type RoomSessionDeliveryRequest,
} from '../wire-protocol/adapter/cloudflare/room/subscription.js'
import { RoomAuthority } from '../wire-protocol/adapter/cloudflare/room/do.js'
import { withCloudflareSession, type CloudflareSession } from '../wire-protocol/adapter/cloudflare/session.js'
import type { TelefuncDurableObjectNamespace } from '../wire-protocol/adapter/cloudflare/namespace.js'
import { isTelefuncRequest, toResponse } from './shared.js'

/** A Durable Object takes a WebSocket message of 32 MiB at most. */
const DURABLE_OBJECT_MAX_RECEIVED_MESSAGE_BYTES = 32 * 1024 * 1024

type CloudflareOptions = {
  bindingName?: string
  kvBindingName?: string
  instanceName?: string
  context?: (request: Request, env: Cloudflare.Env) => TelefuncNamespace.Context | Promise<TelefuncNamespace.Context>
  scale?: CloudflareScale
  locationFallback?: DurableObjectLocationHint
  jurisdiction?: DurableObjectJurisdiction
}

type ServeInput = {
  request: Request
  env: Cloudflare.Env
  /** Not read: serve() leaves no work running past its response. */
  ctx?: ExecutionContext
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
  setAdapterMaxFrameBytes(DURABLE_OBJECT_MAX_RECEIVED_MESSAGE_BYTES)
  const bindingName = options?.bindingName ?? 'TelefuncDurableObject'
  const kvBindingName = options?.kvBindingName ?? 'TelefuncKV'
  const baseInstanceName = options?.instanceName ?? 'telefunc'
  const scale = options?.scale
  const locationFallback = options?.locationFallback ?? 'weur'
  assertLocationFallbackIsScaled(scale, locationFallback)
  const jurisdiction = options?.jurisdiction

  const sessionPins = new SessionPins()
  function pinSession(request: Request): void {
    sessionPins.pin(request, () => requireBinding<KVNamespace>(workerEnv, kvBindingName, 'KV namespace'))
  }

  // A page may use its session over its WebSocket alone, so each message renews its pin when due, as a request does.
  const hooks = getTelefuncChannelHooks({ onMessage: (peer) => pinSession(peer.request) })
  const crosswsAdapter = crossws({ bindingName, instanceName: baseInstanceName, hooks })
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
    readonly #authorityState: CloudflareBroadcastAuthorityState
    readonly #broadcastCalls: BroadcastCalls = new OrderedStubs()
    readonly #session: CloudflareSession

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env, telefuncNamespace(env))
      this.#authorityState = new CloudflareBroadcastAuthorityState(ctx)
      const id = ctx.id.toString()
      this.#session = {
        room: new CloudflareRoomSessionManager(id),
        broadcast: broadcast.member(id, this.#broadcastCalls),
        mux: new ChannelMux(),
      }
      crosswsAdapter.handleDurableInit(this, ctx, env)
    }

    async fetch(request: Request) {
      return this.#runInSession(async () => {
        const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
        if (bucket) this.#session.broadcast.locate(bucket)
        pinSession(request)
        if (request.headers.get('upgrade') === 'websocket') {
          return crosswsAdapter.handleDurableUpgrade(this, request)
        }
        const context = getContext ? await getContext(request, this.env) : undefined
        return toResponse(await serveTelefunc(context ? { request, context } : { request }))
      })
    }

    webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
      return this.#runInSession(() => crosswsAdapter.handleDurableMessage(this, ws, message))
    }

    webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
      // Before compatibility date 2026-04-07 a client's close waits for this answer; close() refuses 1005 and 1006.
      if (code === 1005) ws.close()
      else if (wasClean) ws.close(code, reason)
      return this.#runInSession(() => crosswsAdapter.handleDurableClose(this, ws, code, reason, wasClean))
    }

    /** crossws has no handler for a socket's error, so its wire ends here. */
    webSocketError(ws: WebSocket) {
      return this.#runInSession(() => crosswsAdapter.handleDurableClose(this, ws, 1006, 'error', false))
    }

    telefuncBroadcastPublish(request: BroadcastPublishRequest) {
      return broadcast.publishToSubscribers(this.#authorityState, this.#broadcastCalls, request)
    }

    telefuncBroadcastForward(request: BroadcastForwardRequest) {
      return broadcast.forwardToBucket(this.#broadcastCalls, request)
    }

    telefuncBroadcastDeliver(request: BroadcastDeliverRequest) {
      return this.#runInSession(() => this.#session.broadcast.deliver(request))
    }

    telefuncBroadcastPresence(request: BroadcastPresenceRequest) {
      return this.#authorityState.setPresence(request)
    }

    telefuncRoomDeliver(request: RoomSessionDeliveryRequest) {
      return this.#runInSession(() => this.#session.room.deliver(request))
    }

    #runInSession<T>(fn: () => T): T {
      return withCloudflareSession(this.#session, fn)
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
      const pinned = presented ? await readSessionPin(kv, presented) : null
      // Without a pin, the token's hash picks its shard in the request's region.
      const { sessionInstanceName, locationBucket } =
        pinned ?? resolveSessionRoutingTarget(baseInstanceName, scale, request, locationFallback, token)

      const forwardedHeaders = new Headers(request.headers as Headers)
      // A token the client didn't name is never presented again: its session Durable Object pins none.
      if (presented) forwardedHeaders.set(TELEFUNC_SESSION_HEADER, presented)
      else forwardedHeaders.delete(TELEFUNC_SESSION_HEADER)
      forwardedHeaders.set(TELEFUNC_SHARD_HEADER, sessionInstanceName)
      forwardedHeaders.set(TELEFUNC_BROADCAST_BUCKET_HEADER, locationBucket)
      const forwardedRequest = new Request(request, { headers: forwardedHeaders })

      return binding
        .get(binding.idFromName(sessionInstanceName), { locationHint: locationBucket })
        .fetch(forwardedRequest)
    },
    TelefuncDurableObject,
  }
}

function requireBinding<T>(env: Cloudflare.Env, name: string, kind: string): T {
  const binding = (env as Record<string, T | undefined>)[name]
  assertUsage(binding, `Missing Cloudflare ${kind} binding "${name}". Add it to your wrangler.jsonc.`)
  return binding
}
