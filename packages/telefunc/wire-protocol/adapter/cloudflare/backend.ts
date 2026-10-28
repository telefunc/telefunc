/// <reference types="@cloudflare/workers-types" />
export { CloudflareBackend }

import type {
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PublishResult,
} from '../../backend/broadcast/contract.js'
import type { SubscriptionDriver } from '../../backend/subscription.js'
import type { CloudflareBroadcast } from './broadcast.js'
import { getCloudflareExtensionPlanes, type TelefuncExtensionNamespace } from './extensions.js'
import { currentCloudflareSession, requireCloudflareSession } from './session.js'

/** A subscription requires its session DO's context. */
class CloudflareBackend implements BroadcastDriver {
  readonly broadcast: CloudflareBroadcast
  readonly subscriptions: SubscriptionDriver<BroadcastRoute>
  readonly #namespace: () => TelefuncExtensionNamespace

  constructor({
    namespace,
    broadcast,
  }: {
    namespace: () => TelefuncExtensionNamespace
    broadcast: CloudflareBroadcast
  }) {
    this.broadcast = broadcast
    this.#namespace = namespace
    this.subscriptions = {
      bind: (source) => {
        const session = requireCloudflareSession()
        return {
          partition: session.broadcast.partition,
          open: (receiver) => session.broadcast.openSubscription(source, receiver),
        }
      },
      partitionHere: () => currentCloudflareSession()?.broadcast.partition ?? null,
    }
  }

  /** The planes of the Cloudflare extensions registered so far. */
  get planes(): { readonly [key: symbol]: (kit: never) => unknown } {
    return getCloudflareExtensionPlanes(this.#namespace)
  }

  publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    return this.broadcast.publish(route, payload)
  }
}
