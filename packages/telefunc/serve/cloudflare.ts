/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelHooks } from '../wire-protocol/channel/server/ws.js'
import { getServerConfig, enableChannelTransports, setAdapterMaxFrameBytes } from '../node/server/serverConfig.js'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import { installBroadcastAdapter } from '../wire-protocol/broadcast/broadcastAdapter.js'
import {
  CloudflareBroadcastAuthorityState,
  CloudflareBroadcastTransport,
} from '../wire-protocol/adapter/cloudflare/broadcast.js'
import type { BroadcastDeliverRequest, BroadcastPublishRequest } from '../wire-protocol/adapter/cloudflare/broadcast.js'
import {
  TELEFUNC_BROADCAST_BUCKET_HEADER,
  TELEFUNC_SESSION_HEADER,
  TELEFUNC_SHARD_HEADER,
  assertLocationFallbackIsScaled,
  readForwardedTarget,
  resolveSessionRoutingTarget,
} from '../wire-protocol/adapter/cloudflare/routing.js'
import { SessionPins, readSessionPin } from '../wire-protocol/adapter/cloudflare/sessionPin.js'
import { assertUsage } from '../utils/assert.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import type { CloudflareScale } from '../wire-protocol/adapter/cloudflare/routing.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { isTelefuncRequest } from './shared.js'

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

  // A page may use its session over its WebSocket alone, so each message renews its pin when due, as a request does.
  const hooks = getTelefuncChannelHooks({ onMessage: (peer) => sessionPins.pin(peer.request) })
  const crosswsAdapter = crossws({ bindingName, instanceName: baseInstanceName, hooks })
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
    private readonly instanceName: string

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env)
      const { name } = ctx.id
      assertUsage(name, 'Telefunc needs `ctx.id.name` in its Durable Objects: update wrangler.')
      this.instanceName = name
      const binding = getBinding(env)
      assertUsage(binding, `Missing Cloudflare Durable Object binding "${bindingName}" in Durable Object constructor.`)
      broadcast.attachBinding(binding, bindingName)
      const kv = getKVBinding(env)
      if (kv) {
        broadcast.attachKV(kv)
        sessionPins.attachKV(kv)
      }
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      crosswsAdapter.handleDurableInit(this, ctx, env)
    }

    async fetch(request: Request) {
      broadcast.attachRequest(request, this.instanceName)
      const target = readForwardedTarget(request)
      if (target) broadcast.attachIsolateInfo(target.sessionInstanceName, target.locationBucket)
      sessionPins.pin(request)
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
      // Before compatibility date 2026-04-07 a client's close waits for this answer; close() refuses 1005 and 1006.
      if (code === 1005) ws.close()
      else if (wasClean) ws.close(code, reason)
      return crosswsAdapter.handleDurableClose(this, ws, code, reason, wasClean)
    }

    // TODO: remove once Telefunc's own WebSocket adapter replaces crossws.
    /** crossws has no handler for a socket's error, so its wire ends here. */
    webSocketError(ws: WebSocket) {
      return crosswsAdapter.handleDurableClose(this, ws, 1006, 'error', false)
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
