export {
  registerCloudflareExtension,
  getCloudflareExtension,
  getCloudflareExtensionPlanes,
  currentCloudflareMember,
  CLOUDFLARE_BUCKETS,
}
export type {
  BroadcastDelivery,
  CloudflareExtension,
  CloudflareExtensionDurableObject,
  CloudflareExtensionKit,
  CloudflareMember,
  CloudflarePlaneKit,
  CloudflareRegions,
  TelefuncExtensionNamespace,
  TelefuncExtensionStub,
}

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import type { BackendReceiver, BroadcastPayload, BroadcastRoute } from '../../backend/driver.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import { currentCloudflareSession } from './session.js'
import { DEFAULT_BROADCAST_BUCKETS } from './routing.js'

// The types name no Cloudflare global, so that telefunc/__internal type-checks without @cloudflare/workers-types.

/** The Telefunc Durable Object binding, as an extension calls it. */
type TelefuncExtensionNamespace = {
  idFromName(name: string): unknown
  idFromString(id: string): unknown
  /** `locationHint` places the Durable Object near a bucket on its first `get()`. */
  get(id: unknown, options?: { locationHint?: string }): TelefuncExtensionStub
}

/** The methods of a Telefunc Durable Object that extensions call. */
type TelefuncExtensionStub = {
  /** Calls `method` of the extension's `durableObject` on this Durable Object, which then belongs to that extension. */
  telefuncDurableObjectCall(name: string, method: string, args: unknown[]): Promise<unknown>
}

/** A sequenced payload for the subscribers of `routes` on each member DO it reaches. */
type BroadcastDelivery = { routes: BroadcastRoute[]; payload: BroadcastPayload; info: OrderingInfo }

/** The buckets a Durable Object can be placed in, by `locationHint`. */
const CLOUDFLARE_BUCKETS: readonly string[] = DEFAULT_BROADCAST_BUCKETS

/** Broadcast's regions, as an extension's Durable Object sends through them. */
type CloudflareRegions = {
  /** Sends `delivery` across to each bucket of `members` once, to the coordinator `key` picks there, which hands it to
   *  the bucket's members; settles once every coordinator did. Calls made in order reach each coordinator in order. */
  forward(
    key: string,
    delivery: BroadcastDelivery,
    members: Iterable<{ member: string; bucket: string }>,
  ): Promise<void>
}

/** What an extension's Durable Object gets from Telefunc. */
type CloudflareExtensionKit = {
  /** Sends through Broadcast's regions from this Durable Object. */
  regions: CloudflareRegions
  /** The Telefunc Durable Object namespace, for calls to other Durable Objects. */
  getNamespace: () => TelefuncExtensionNamespace
}

/** What an extension's backend plane gets from Telefunc. */
type CloudflarePlaneKit = {
  /** Reads the Telefunc Durable Object namespace of the Worker's current env. */
  getNamespace: () => TelefuncExtensionNamespace
  /** The bucket of a call that has no session's bucket to go by: the `locationFallback` option. */
  locationFallback: string
}

/** The Broadcast member of a session Durable Object, as an extension's code running in it uses it. */
type CloudflareMember = {
  /** The bucket the session was placed in, from the request that reached it; null before one did. */
  readonly bucket: string | null
  /** This incarnation of the session DO, as a driver's `hostKey()`. */
  readonly hostKey: string
  /** Takes `route`'s deliveries for `receiver` with no presence at a key authority, for a sender that keeps its own
   *  list of receivers; returns what stops them. */
  subscribeLocal(route: BroadcastRoute, receiver: BackendReceiver): () => void
}

/** The object an extension's Durable Object calls go to, by method name. */
type CloudflareExtensionDurableObject = object & { alarm?(): Promise<void> }

type CloudflareExtension = {
  name: string
  /** Builds the extension's part of a Durable Object, once, on its first `telefuncDurableObjectCall()`; `ctx` and
   *  `env` are the Durable Object's `DurableObjectState` and `Cloudflare.Env`. Its `alarm()` gets the Durable Object's
   *  alarms. */
  durableObject?: (ctx: never, env: never, kit: CloudflareExtensionKit) => CloudflareExtensionDurableObject
  /** The backend planes the Cloudflare backend serves for the extension, by `backendPlane()` key. */
  planes?: Record<symbol, (kit: CloudflarePlaneKit) => unknown>
}

const globalObject = getGlobalObject<{ extensions: Map<string, CloudflareExtension> }>('extensions.ts', () => ({
  extensions: new Map(),
}))

/** Registering a name again replaces its extension, as a module re-evaluated in dev does. */
function registerCloudflareExtension(extension: CloudflareExtension): void {
  globalObject.extensions.set(extension.name, extension)
}

function getCloudflareExtension(name: string): CloudflareExtension | undefined {
  return globalObject.extensions.get(name)
}

/** The Broadcast member of the session Durable Object whose call this is, undefined outside one. */
function currentCloudflareMember(): CloudflareMember | undefined {
  return currentCloudflareSession()?.broadcast
}

function getCloudflareExtensionPlanes(kit: CloudflarePlaneKit): Record<symbol, () => unknown> {
  const planes: Record<symbol, () => unknown> = {}
  for (const { planes: own = {} } of globalObject.extensions.values()) {
    for (const key of Object.getOwnPropertySymbols(own)) planes[key] = () => own[key]!(kit)
  }
  return planes
}
