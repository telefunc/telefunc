/// <reference types="@cloudflare/workers-types" />

export { Telefunc }
export type { CloudflareOptions }

import { DurableObject, env as workerEnv } from 'cloudflare:workers'
import { installAsyncContext } from '../node/server/context/async.js'
import crossws from 'crossws/adapters/cloudflare'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { ChannelMux } from '../wire-protocol/channel/server/ChannelMux.js'
import { getServerConfig, setAdapterMaxFrameBytes, setAdapterMaxMemory } from '../node/server/serverConfig.js'
import { serveRuntimeRequest } from '../node/server/telefunc.js'
import { noTransportEvidence } from '../node/server/runTelefunc/clientLeft.js'
import { upgradesTakenHere } from '../node/server/runTelefunc/takesUpgrades.js'
import { preloadServerProductionEntry } from '../node/server/runTelefunc/loadTelefuncFilesUsingVite.js'
import { installBackend } from '../wire-protocol/backend/install.js'
import {
  CloudflareBroadcastAuthorityState,
  CloudflareBroadcast,
} from '../wire-protocol/adapter/cloudflare/broadcast.js'
import type {
  BroadcastCalls,
  BroadcastForwardRequest,
  BroadcastHoldRequest,
  BroadcastLeaseRequest,
  BroadcastReleaseRequest,
  BroadcastPublishRequest,
} from '../wire-protocol/adapter/cloudflare/broadcast.js'
import { OrderedStubs } from '../wire-protocol/adapter/cloudflare/OrderedStubs.js'
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
import { CloudflareBackend } from '../wire-protocol/adapter/cloudflare/CloudflareBackend.js'
import {
  getCloudflareExtension,
  type BroadcastDelivery,
  type CloudflareExtensionDurableObject,
} from '../wire-protocol/adapter/cloudflare/extensions.js'
import {
  servesRequest,
  cloudflareSession,
  withCloudflareSession,
  type CloudflareSession,
} from '../wire-protocol/adapter/cloudflare/session.js'
import type { TelefuncDurableObjectNamespace } from '../wire-protocol/adapter/cloudflare/namespace.js'
import { getTelefuncRequestUrl, toResponse } from './shared.js'
import { REQUEST_KIND, getRequestKind, getSessionToken, namesWireVersion } from '../wire-protocol/wire/requestKind.js'
import { STATUS_CODE_MALFORMED_REQUEST } from '../shared/constants.js'
import { REQUEST_KIND_REFUSAL, WIRE_VERSION_REFUSAL, refusalContentType } from '../wire-protocol/refusal.js'

// A subscription finds its session through AsyncLocalStorage, which a compatibility flag enables. A Worker without it
// still serves telefunctions and channels; Broadcast and Room ask for the flag (session.ts). The specifier is a
// variable so that the bundler leaves the import to the runtime, and doesn't warn a Worker without the flag about it.
const asyncHooksModule = 'node:async_hooks'
const asyncHooks: typeof import('node:async_hooks') | null = await import(/* @vite-ignore */ asyncHooksModule).catch(
  () => null,
)
if (asyncHooks) installAsyncContext(asyncHooks.AsyncLocalStorage)

// At Worker start, so that the server entry (the build's config, the extensions' server entries) runs before the first
// request and before a Durable Object's first call, which may be an alarm()
await preloadServerProductionEntry()

/** The extension a Durable Object belongs to, kept in its storage so that its alarm() finds the extension after an
 *  eviction. */
const EXTENSION_STORAGE_KEY = 'telefunc:extension'

/** A Durable Object takes a WebSocket message of 32 MiB at most. */
const DURABLE_OBJECT_MAX_RECEIVED_MESSAGE_BYTES = 32 * 1024 * 1024
/** An isolate has 128 MB, heap included, and is replaced past it, with the channels it holds. A channel holds this and its
 *  credit window, as much again: half an isolate at most, where the usual default would pass it with one channel. */
const DURABLE_OBJECT_MAX_MEMORY = 64 * 1024 * 1024

type CloudflareOptions = {
  bindingName?: string
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
  function telefuncNamespace(env: Cloudflare.Env): TelefuncDurableObjectNamespace {
    const namespace = requireBinding<TelefuncDurableObjectNamespace>(env, bindingName, 'Durable Object')
    return jurisdiction ? namespace.jurisdiction(jurisdiction) : namespace
  }
  const cloudflareBackend = installBackend(
    () =>
      new CloudflareBackend({
        planeKit: { getNamespace: () => telefuncNamespace(workerEnv), locationFallback },
        broadcast: new CloudflareBroadcast({
          baseInstanceName,
          scale,
          locationFallback,
          getNamespace: () => telefuncNamespace(workerEnv),
        }),
      }),
    ['cloudflare', bindingName, baseInstanceName, JSON.stringify(scale ?? null), jurisdiction ?? null],
  )
  const broadcast = cloudflareBackend.broadcast

  const getContext = options?.context

  // One class for every role; an instance's name decides which: a session shard, a Broadcast key authority or
  // coordinator, or an extension's `durableObject`.
  const TelefuncDurableObject = class extends DurableObject<Cloudflare.Env> {
    private readonly authorityState: CloudflareBroadcastAuthorityState
    private readonly broadcastCalls: BroadcastCalls = new OrderedStubs()
    private readonly session: CloudflareSession
    private readonly durableObjects = new Map<string, CloudflareExtensionDurableObject>()
    /** The extension this Durable Object's storage is known to hold. */
    private storedExtension: string | undefined

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env)
      this.authorityState = new CloudflareBroadcastAuthorityState(ctx)
      const id = ctx.id.toString()
      this.session = cloudflareSession(broadcast.member(id, this.broadcastCalls), new ChannelMux({ hostId: id }))
      crosswsAdapter.handleDurableInit(this, ctx, env)
    }

    // App code runs in every entry but the constructor and `telefuncBroadcastPublish`: each runs it as this object's.
    async fetch(request: Request) {
      servesRequest(this.session, request)
      return withCloudflareSession(this.session, async () => {
        const target = readForwardedTarget(request)
        if (target) this.session.broadcast.locate(target.locationBucket)
        if (request.headers.get('upgrade') === 'websocket') {
          // crossws accepts it for hibernation, yet this object never hibernates while one is open, and is billed for
          // that time: its channels live in memory, and each wire's ping deadline is a timer (`armPingDeadline`).
          return crosswsAdapter.handleDurableUpgrade(this, request)
        }
        const context = getContext ? await getContext(request, this.env) : undefined
        const response = toResponse(
          await serveRuntimeRequest(context ? { request, context } : { request }, {
            clientLeft: noTransportEvidence,
            clientGone: request.signal,
            takesUpgrades: upgradesTakenHere,
          }),
        )
        // The page names it from then on, so it reaches this Durable Object from any region.
        if (target) response.headers.set(SESSION_HOME_HEADER, target.locationBucket)
        return response
      })
    }

    webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
      return withCloudflareSession(this.session, () => crosswsAdapter.handleDurableMessage(this, ws, message))
    }

    webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
      // Before compatibility date 2026-04-07 a client's close waits for this answer; close() refuses 1005 and 1006.
      if (code === 1005) ws.close()
      else if (wasClean) ws.close(code, reason)
      return withCloudflareSession(this.session, () =>
        crosswsAdapter.handleDurableClose(this, ws, code, reason, wasClean),
      )
    }

    /** crossws has no handler for a socket's error, so its wire ends here. */
    webSocketError(ws: WebSocket) {
      return withCloudflareSession(this.session, () =>
        crosswsAdapter.handleDurableClose(this, ws, 1006, 'error', false),
      )
    }

    telefuncBroadcastPublish(request: BroadcastPublishRequest) {
      return broadcast.publishToSubscribers(this.authorityState, this.broadcastCalls, request)
    }

    telefuncBroadcastForward(request: BroadcastForwardRequest) {
      return broadcast.forwardToBucket(this.broadcastCalls, request)
    }

    telefuncBroadcastDeliver(delivery: BroadcastDelivery) {
      return withCloudflareSession(this.session, () => this.session.broadcast.deliver(delivery))
    }

    telefuncBroadcastGrant(member: string) {
      return this.authorityState.grant(member)
    }

    telefuncBroadcastHold(request: BroadcastHoldRequest) {
      return this.authorityState.hold(request)
    }

    telefuncBroadcastRenew(request: BroadcastLeaseRequest) {
      return this.authorityState.renew(request)
    }

    telefuncBroadcastRelease(request: BroadcastReleaseRequest) {
      return this.authorityState.release(request)
    }

    /** The RPC method every extension's `durableObject` is called through. */
    async telefuncDurableObjectCall(name: string, method: string, args: unknown[]): Promise<unknown> {
      const { methods } = this.#durableObject(name)
      const call = Object.hasOwn(methods, method) ? methods[method] : undefined
      assertUsage(call, `The Cloudflare extension ${name} serves no Durable Object method ${method}()`)
      if (this.storedExtension !== name) {
        if ((await this.ctx.storage.get(EXTENSION_STORAGE_KEY)) !== name) {
          await this.ctx.storage.put(EXTENSION_STORAGE_KEY, name)
        }
        this.storedExtension = name
      }
      return Reflect.apply(call, methods, args)
    }

    /** An extension's `durableObject` schedules its alarms; a Broadcast key's authority, which belongs to no extension,
     *  its sweep. */
    async alarm(): Promise<void> {
      const name = await this.ctx.storage.get<string>(EXTENSION_STORAGE_KEY)
      if (name === undefined) return this.authorityState.alarm()
      this.storedExtension = name
      await this.#durableObject(name).alarm?.()
    }

    // This one is `#`, not TypeScript `private`: Workers RPC lets any stub call a method on a Durable Object's
    // prototype, a TypeScript `private` one too, but reaches no `#` method and no own instance field.

    /** Built once, synchronously, so that concurrent first calls share it. */
    #durableObject(name: string): CloudflareExtensionDurableObject {
      const built = this.durableObjects.get(name)
      if (built) return built
      const extension = getCloudflareExtension(name)
      assertUsage(
        extension?.durableObject,
        `The Cloudflare extension ${name} isn't registered: is it installed, and does your worker's build use ` +
          `Telefunc's Vite plugin?`,
      )
      const regions = { forward: broadcast.forward.bind(broadcast, this.broadcastCalls) }
      const getNamespace = () => telefuncNamespace(this.env)
      const durableObject = extension.durableObject(this.ctx as never, this.env as never, { regions, getNamespace })
      this.durableObjects.set(name, durableObject)
      return durableObject
    }
  }

  return {
    async serve({ request, env }: ServeInput): Promise<Response | undefined> {
      const url = getTelefuncRequestUrl(request)
      if (url === null) return undefined
      const config = getServerConfig()

      const binding = telefuncNamespace(env)

      const isWebSocketRequest = request.headers.get('upgrade') === 'websocket'
      if (isWebSocketRequest && !config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
        return new Response(null, { status: 400 })
      }
      // Reject before forwarding: a Durable Object that answers without reading the forwarded body fails the request.
      const requestKind = isWebSocketRequest ? null : getRequestKind(request, config.telefuncUrl)
      if (requestKind === REQUEST_KIND.INVALID) {
        return new Response(REQUEST_KIND_REFUSAL.reason, {
          status: STATUS_CODE_MALFORMED_REQUEST,
          headers: { 'Content-Type': refusalContentType(REQUEST_KIND_REFUSAL) },
        })
      }
      if (requestKind === REQUEST_KIND.SSE && !namesWireVersion(request)) {
        return new Response(WIRE_VERSION_REFUSAL.reason, {
          status: STATUS_CODE_WIRE_VERSION,
          headers: { 'Content-Type': refusalContentType(WIRE_VERSION_REFUSAL) },
        })
      }

      // No lookup and nothing shared: each request names the home its session's responses gave its page.
      const { sessionInstanceName, locationBucket } = resolveSessionRoutingTarget(
        baseInstanceName,
        scale,
        request,
        locationFallback,
        getSessionToken(request) || crypto.randomUUID(),
        url.searchParams.get('home'),
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

function requireBinding<T>(env: Cloudflare.Env, name: string, kind: string): T {
  const binding = (env as Record<string, T | undefined>)[name]
  assertUsage(binding, `Missing Cloudflare ${kind} binding "${name}". Add it to your wrangler.jsonc.`)
  return binding
}
