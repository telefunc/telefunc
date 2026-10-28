/// <reference types="@cloudflare/workers-types" />
export { CloudflareBackend }

import type {
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PublishResult,
} from '../../backend/broadcast/contract.js'
import type { SubscriptionDriver } from '../../backend/subscription.js'
import { ROOM_PLANE } from '../../backend/room/plane.js'
import type { CloudflareBroadcast } from './broadcast.js'
import { CloudflareRoomDriver, type CloudflareRoomNamespace } from './room/driver.js'
import { currentCloudflareSession, requireCloudflareSession } from './session.js'

/** A subscription requires its session DO's context. */
class CloudflareBackend implements BroadcastDriver {
  readonly broadcast: CloudflareBroadcast
  readonly subscriptions: SubscriptionDriver<BroadcastRoute>
  readonly planes: { readonly [ROOM_PLANE]: () => CloudflareRoomDriver }

  constructor({ rooms, broadcast }: { rooms: () => CloudflareRoomNamespace; broadcast: CloudflareBroadcast }) {
    this.broadcast = broadcast
    this.planes = { [ROOM_PLANE]: () => new CloudflareRoomDriver({ rooms }) }
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

  publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    return this.broadcast.publish(route, payload)
  }
}
