export { registerCloudflareExtension, getCloudflareExtension, getCloudflareExtensionPlanes }
export type { CloudflareExtension, CloudflareExtensionRole, TelefuncExtensionNamespace, TelefuncExtensionStub }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'

// The types name no Cloudflare global, so that telefunc/__internal type-checks without @cloudflare/workers-types.

/** The Telefunc Durable Object binding, as an extension calls it. */
type TelefuncExtensionNamespace = {
  idFromName(name: string): unknown
  idFromString(id: string): unknown
  get(id: unknown): TelefuncExtensionStub
}

/** The methods of a Telefunc Durable Object that extensions call. */
type TelefuncExtensionStub = {
  /** Calls `method` of the extension's role on this Durable Object, which then has that role. */
  telefuncRole(name: string, method: string, args: unknown[]): Promise<unknown>
  /** Calls the extension's session call `method`, in the context of this session Durable Object. */
  telefuncSessionCall(name: string, method: string, args: unknown[]): Promise<unknown>
}

/** The object a role's calls go to, by method name. */
type CloudflareExtensionRole = object & { alarm?(): Promise<void> }

type CloudflareExtension = {
  name: string
  /** Builds the extension's role on a Durable Object, once, on its first `telefuncRole()` call; `ctx` and `env` are
   *  the Durable Object's `DurableObjectState` and `Cloudflare.Env`. The role's `alarm()` gets the Durable Object's
   *  alarms. */
  role?: (ctx: never, env: never, namespace: () => TelefuncExtensionNamespace) => CloudflareExtensionRole
  /** The calls a session Durable Object takes from other Durable Objects through `telefuncSessionCall()`. */
  session?: Record<string, (...args: never[]) => unknown>
  /** The backend planes the Cloudflare backend serves for the extension, by `backendPlane()` key. */
  planes?: Record<symbol, (namespace: () => TelefuncExtensionNamespace) => unknown>
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

function getCloudflareExtensionPlanes(namespace: () => TelefuncExtensionNamespace): Record<symbol, () => unknown> {
  const planes: Record<symbol, () => unknown> = {}
  for (const { planes: own = {} } of globalObject.extensions.values()) {
    for (const key of Object.getOwnPropertySymbols(own)) planes[key] = () => own[key]!(namespace)
  }
  return planes
}
