/// <reference types="@cloudflare/workers-types" />
export { CloudflareBackend }

import type {
  BackendReceiver,
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  DriverAttempt,
  PublishResult,
} from '../../backend/driver.js'
import type { CloudflareBroadcast } from './broadcast.js'
import { getCloudflareExtensionPlanes, type TelefuncExtensionNamespace } from './extensions.js'
import { currentCloudflareSession, requireCloudflareSession } from './session.js'

type CloudflareBackendOptions = {
  getNamespace: () => TelefuncExtensionNamespace
  broadcast: CloudflareBroadcast
}

/** A subscription requires its session DO's context. */
class CloudflareBackend implements BroadcastDriver {
  readonly broadcast: CloudflareBroadcast
  private readonly getNamespace: () => TelefuncExtensionNamespace

  constructor({ getNamespace, broadcast }: CloudflareBackendOptions) {
    this.broadcast = broadcast
    this.getNamespace = getNamespace
  }

  /** The planes of the Cloudflare extensions registered so far. */
  get planes(): { readonly [key: symbol]: (kit: never) => unknown } {
    return getCloudflareExtensionPlanes(this.getNamespace)
  }

  publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    return this.broadcast.publish(route, payload)
  }

  scope(): string | null {
    return currentCloudflareSession()?.broadcast.scope ?? null
  }

  subscribe(route: BroadcastRoute, receiver: BackendReceiver): DriverAttempt {
    return requireCloudflareSession().broadcast.openSubscription(route, receiver)
  }
}
