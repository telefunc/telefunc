/// <reference types="@cloudflare/workers-types" />
export { CloudflareBackend }

import type {
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PublishResult,
  Subscriber,
  Unsubscribe,
} from '../../backend/driver.js'
import type { CloudflareBroadcast } from './broadcast.js'
import { getCloudflareExtensionPlanes, type TelefuncExtensionNamespace } from './extensions.js'
import { requireCloudflareSession } from './session.js'

/** A subscription requires its session DO's context. */
class CloudflareBackend implements BroadcastDriver {
  readonly broadcast: CloudflareBroadcast
  private readonly namespace: () => TelefuncExtensionNamespace

  constructor({
    namespace,
    broadcast,
  }: {
    namespace: () => TelefuncExtensionNamespace
    broadcast: CloudflareBroadcast
  }) {
    this.broadcast = broadcast
    this.namespace = namespace
  }

  /** The planes of the Cloudflare extensions registered so far. */
  get planes(): { readonly [key: symbol]: (kit: never) => unknown } {
    return getCloudflareExtensionPlanes(this.namespace)
  }

  publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    return this.broadcast.publish(route, payload)
  }

  subscribe(route: BroadcastRoute, subscriber: Subscriber): Unsubscribe {
    return requireCloudflareSession().broadcast.subscribe(route, subscriber)
  }
}
