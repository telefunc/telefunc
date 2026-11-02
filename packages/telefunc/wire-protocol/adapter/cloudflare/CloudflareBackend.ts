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
import { getCloudflareExtensionPlanes, type CloudflarePlaneKit } from './extensions.js'
import { currentCloudflareSession, requireCloudflareSession } from './session.js'

type CloudflareBackendOptions = {
  planeKit: CloudflarePlaneKit
  broadcast: CloudflareBroadcast
}

/** A subscription requires its session DO's context. */
class CloudflareBackend implements BroadcastDriver {
  readonly broadcast: CloudflareBroadcast
  private readonly planeKit: CloudflarePlaneKit

  constructor({ planeKit, broadcast }: CloudflareBackendOptions) {
    this.broadcast = broadcast
    this.planeKit = planeKit
  }

  /** The planes of the Cloudflare extensions registered so far. */
  get planes(): { readonly [key: symbol]: (kit: never) => unknown } {
    return getCloudflareExtensionPlanes(this.planeKit)
  }

  publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    return this.broadcast.publish(route, payload)
  }

  hostKey(): string | null {
    return currentCloudflareSession()?.broadcast.hostKey ?? null
  }

  subscribe(route: BroadcastRoute, receiver: BackendReceiver): DriverAttempt {
    return requireCloudflareSession().broadcast.openSubscription(route, receiver)
  }
}
