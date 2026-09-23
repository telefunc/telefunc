export { superviseBroadcastDriver }

import { ChannelOverflowError } from '../../channel-errors.js'
import { CHANNEL_BUFFER_LIMIT_BYTES } from '../../constants.js'
import { SubscriptionManager } from '../subscription-manager.js'
import type { BroadcastBackend, BroadcastDriver, BroadcastLane, PublishResult } from './contract.js'
import { broadcastRouteKey } from './route-key.js'
import { assertDriverPosition } from '../driver-position.js'
import { isPromise } from '../../../utils/isPromise.js'

function checked(result: PublishResult): PublishResult {
  assertDriverPosition(result)
  return result
}

const PENDING_PUBLISH_LIMIT = 1024

type PendingPublish = {
  payload: Uint8Array
  resolve: (result: PublishResult | Promise<PublishResult>) => void
  reject: (error: unknown) => void
}
type PendingRoute = { lane: BroadcastLane; entries: PendingPublish[]; bytes: number }

/** Owns the Broadcast subscription manager and the publish-readiness gate: a publish waits until this instance's own
 *  subscriptions on its route settle, so its local subscribers receive it; one that ends instead doesn't fail the publish. */
function superviseBroadcastDriver(driver: BroadcastDriver): BroadcastBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, broadcastRouteKey)
  const pending = new Map<string, PendingRoute>()
  let disposal: Promise<void> | undefined

  const publishNow = (lane: BroadcastLane, payload: Uint8Array): PublishResult | Promise<PublishResult> => {
    const result = driver.publish(lane, payload)
    return isPromise(result) ? result.then(checked) : checked(result)
  }

  const publish = (lane: BroadcastLane, payload: Uint8Array): PublishResult | Promise<PublishResult> => {
    const routeKey = broadcastRouteKey(lane)
    const waiting = pending.get(routeKey)
    if (waiting === undefined && subscriptions.settledWaits(lane).length === 0) return publishNow(lane, payload)
    const owned = payload.slice()
    const route = waiting ?? { lane, entries: [], bytes: 0 }
    if (route.entries.length >= PENDING_PUBLISH_LIMIT || route.bytes + owned.byteLength > CHANNEL_BUFFER_LIMIT_BYTES) {
      return Promise.reject(new ChannelOverflowError('Broadcast readiness buffer overflow'))
    }
    route.bytes += owned.byteLength
    const result = new Promise<PublishResult>((resolve, reject) =>
      route.entries.push({ payload: owned, resolve, reject }),
    )
    if (waiting === undefined) {
      pending.set(routeKey, route)
      void flush(routeKey, route)
    }
    return result
  }

  const flush = async (routeKey: string, route: PendingRoute): Promise<void> => {
    while (route.entries.length > 0) {
      for (
        let waits = subscriptions.settledWaits(route.lane);
        waits.length > 0;
        waits = subscriptions.settledWaits(route.lane)
      )
        await Promise.all(waits)
      const entries = route.entries.splice(0)
      route.bytes = 0
      for (const entry of entries) {
        try {
          entry.resolve(publishNow(route.lane, entry.payload))
        } catch (error) {
          entry.reject(error)
        }
      }
    }
    pending.delete(routeKey)
  }

  return {
    publish,
    subscribe: (lane, receiver) =>
      subscriptions.subscribe(lane, (payload, info) => {
        assertDriverPosition(info)
        return receiver(payload, info)
      }),
    dispose: () => (disposal ??= subscriptions.dispose()),
  }
}
