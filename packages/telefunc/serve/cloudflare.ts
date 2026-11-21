/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject } from 'cloudflare:workers'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { getServerConfig, setAdapterMaxFrameBytes, setAdapterMaxMemory } from '../node/server/serverConfig.js'
import { serveRuntimeRequest } from '../node/server/telefunc.js'
import { noTransportEvidence } from '../node/server/runTelefunc/clientLeft.js'
import { upgradesTakenHere } from '../node/server/runTelefunc/takesUpgrades.js'
import { installBroadcastAdapter } from '../wire-protocol/broadcast/broadcastAdapter.js'
import {
  CloudflareBroadcastAuthorityState,
  CloudflareBroadcastTransport,
} from '../wire-protocol/adapter/cloudflare/broadcast.js'
import type { BroadcastDeliverRequest, BroadcastPublishRequest } from '../wire-protocol/adapter/cloudflare/broadcast.js'
import {
  TELEFUNC_BROADCAST_BUCKET_HEADER,
  TELEFUNC_SHARD_HEADER,
  assertLocationFallbackIsScaled,
  readForwardedTarget,
  resolveSessionRoutingTarget,
} from '../wire-protocol/adapter/cloudflare/routing.js'
import { assertUsage } from '../utils/assert.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import type { CloudflareScale } from '../wire-protocol/adapter/cloudflare/routing.js'
import { CHANNEL_TRANSPORT, SESSION_HOME_HEADER, STATUS_CODE_WIRE_VERSION } from '../wire-protocol/constants.js'
import { getTelefuncRequestUrl } from './shared.js'
import { REQUEST_KIND, getRequestKind, namesWireVersion } from '../wire-protocol/wire/requestKind.js'
import { STATUS_BODY_MALFORMED_REQUEST, STATUS_CODE_MALFORMED_REQUEST } from '../shared/constants.js'

// A subscription finds its Durable Object through AsyncLocalStorage, which a compatibility flag enables. A Worker without
// it still serves telefunctions and channels; a Broadcast subscribed on the server asks for the flag (broadcast.ts). The
// specifier is a variable so that the bundler leaves the import to the runtime, and doesn't warn a Worker without the
// flag about it.
const asyncHooksModule = 'node:async_hooks'
const asyncHooks: typeof import('node:async_hooks') | null = await import(/* @vite-ignore */ asyncHooksModule).catch(
  () => null,
)

/** A Durable Object takes a WebSocket message of 32 MiB at most. */
const DURABLE_OBJECT_MAX_RECEIVED_MESSAGE_BYTES = 32 * 1024 * 1024
/** An isolate has 128 MB, heap included, and is replaced past it, with the channels it holds. A channel holds this and its
 *  credit window, as much again: half an isolate at most, where the usual default would pass it with one channel. */
const DURABLE_OBJECT_MAX_MEMORY = 64 * 1024 * 1024

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
  setAdapterMaxFrameBytes(DURABLE_OBJECT_MAX_RECEIVED_MESSAGE_BYTES)
  setAdapterMaxMemory(DURABLE_OBJECT_MAX_MEMORY)
  const bindingName = options?.bindingName ?? 'TelefuncDurableObject'
  const kvBindingName = options?.kvBindingName ?? 'TelefuncKV'
  const baseInstanceName = options?.instanceName ?? 'telefunc'
  const scale = options?.scale
  const locationFallback = options?.locationFallback ?? 'weur'
  assertLocationFallbackIsScaled(scale, locationFallback)
  const jurisdiction = options?.jurisdiction

  const crosswsAdapter = crossws({
    bindingName,
    instanceName: baseInstanceName,
    ...getTelefuncChannelOptions({ copiesOnSend: true }),
  })
  // Factory runs only on first install. Bundler quirks can evaluate the user's entry twice in the same isolate;
  // we want every evaluation to share one transport instance.
  const broadcast = installBroadcastAdapter(
    () =>
      new CloudflareBroadcastTransport({
        baseInstanceName,
        scale,
        runningObject: asyncHooks && new asyncHooks.AsyncLocalStorage<string>(),
      }),
  )

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
      if (kv) broadcast.attachKV(kv)
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      crosswsAdapter.handleDurableInit(this, ctx, env)
    }

    // App code runs in every entry but the constructor and `telefuncBroadcastPublish`: each runs it as this object's.
    async fetch(request: Request) {
      return broadcast.runInObject(this.instanceName, async () => {
        broadcast.attachRequest(request, this.instanceName)
        const target = readForwardedTarget(request)
        if (target) broadcast.attachIsolateInfo(target.locationBucket)
        if (request.headers.get('upgrade') === 'websocket') {
          // crossws accepts it for hibernation, yet this object never hibernates while one is open, and is billed for
          // that time: its channels live in memory, and each wire's ping deadline is a timer (`armPingDeadline`).
          return crosswsAdapter.handleDurableUpgrade(this, request)
        }
        const context = getContext ? await getContext(request, this.env as Cloudflare.Env) : undefined
        const httpResponse = await serveRuntimeRequest(
          context ? { request, context } : { request },
          noTransportEvidence,
          upgradesTakenHere,
        )
        const headers = new Headers(httpResponse.headers)
        // The page names it from then on, so it reaches this Durable Object from any region.
        if (target) headers.set(SESSION_HOME_HEADER, target.locationBucket)
        return new Response(httpResponse.getReadableWebStream(), { status: httpResponse.statusCode, headers })
      })
    }

    webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
      return broadcast.runInObject(this.instanceName, () => crosswsAdapter.handleDurableMessage(this, ws, message))
    }

    webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
      // Before compatibility date 2026-04-07 a client's close waits for this answer; close() refuses 1005 and 1006.
      if (code === 1005) ws.close()
      else if (wasClean) ws.close(code, reason)
      return broadcast.runInObject(this.instanceName, () =>
        crosswsAdapter.handleDurableClose(this, ws, code, reason, wasClean),
      )
    }

    // TODO: remove once Telefunc's own WebSocket adapter replaces crossws.
    /** crossws has no handler for a socket's error, so its wire ends here. */
    webSocketError(ws: WebSocket) {
      return broadcast.runInObject(this.instanceName, () =>
        crosswsAdapter.handleDurableClose(this, ws, 1006, 'error', false),
      )
    }

    telefuncBroadcastPublish(request: BroadcastPublishRequest) {
      return broadcast.publishToSubscribers(this.authorityState, request)
    }

    telefuncBroadcastDeliver(request: BroadcastDeliverRequest) {
      broadcast.runInObject(this.instanceName, () => broadcast.deliverToLocal(request))
    }
  }

  return {
    async serve({ request, env }: ServeInput): Promise<Response | undefined> {
      const url = getTelefuncRequestUrl(request)
      if (url === null) return undefined
      const config = getServerConfig()

      const binding = getBinding(env)
      assertUsage(binding, `Missing Cloudflare Durable Object binding "${bindingName}". Add it to your wrangler.jsonc.`)

      const isWebSocketRequest = request.headers.get('upgrade') === 'websocket'
      if (isWebSocketRequest && !config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
        return new Response(null, { status: 400 })
      }
      // Reject before forwarding: a Durable Object that answers without reading the forwarded body fails the request.
      const requestKind = isWebSocketRequest ? null : getRequestKind(request, config.telefuncUrl)
      if (requestKind === REQUEST_KIND.INVALID) {
        return new Response(STATUS_BODY_MALFORMED_REQUEST, {
          status: STATUS_CODE_MALFORMED_REQUEST,
          headers: { 'Content-Type': 'text/plain' },
        })
      }
      if (requestKind === REQUEST_KIND.SSE && !namesWireVersion(request)) {
        return new Response('reload the page', { status: STATUS_CODE_WIRE_VERSION })
      }

      const kv = getKVBinding(env)
      assertUsage(kv, `Missing Cloudflare KV namespace binding "${kvBindingName}". Add it to your wrangler.jsonc.`)
      const { searchParams } = url
      const token = searchParams.get('session') || crypto.randomUUID()
      // No lookup and nothing shared: each request names the home its session's responses gave its page.
      const { sessionInstanceName, locationBucket } = resolveSessionRoutingTarget(
        baseInstanceName,
        scale,
        request,
        locationFallback,
        token,
        searchParams.get('home'),
      )

      const forwardedHeaders = new Headers(request.headers as Headers)
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
