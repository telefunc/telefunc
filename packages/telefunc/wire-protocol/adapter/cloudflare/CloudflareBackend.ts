/// <reference types="@cloudflare/workers-types" />
export { CloudflareBackend }

import type {
  BackendReceiver,
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  DriverAttempt,
  PlaneServer,
  PublishResult,
} from '../../backend/driver.js'
import type { CloudflareBroadcast } from './broadcast.js'
import { currentCloudflareHostKey, getCloudflareExtensionPlanes, type CloudflarePlaneKit } from './extensions.js'
import { currentCloudflareSession } from './session.js'
import { assert } from '../../../utils/assert.js'

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
  get planes(): PlaneServer[] {
    return getCloudflareExtensionPlanes(this.planeKit)
  }

  publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    return this.broadcast.publish(route, payload)
  }

  hostKey(): string {
    return currentCloudflareHostKey()
  }

  subscribe(route: BroadcastRoute, receiver: BackendReceiver): DriverAttempt {
    const session = currentCloudflareSession()
    assert(session !== undefined) // `hostKey()` refused a subscribe outside a session
    return session.broadcast.openSubscription(route, receiver)
  }
}
