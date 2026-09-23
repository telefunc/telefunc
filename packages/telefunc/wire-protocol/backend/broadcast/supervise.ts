export { superviseBroadcastDriver }

import { ChannelOverflowError } from '../../channel-errors.js'
import { CHANNEL_BUFFER_LIMIT_BYTES } from '../../constants.js'
import { SubscriptionManager } from '../subscription-manager.js'
import type { BroadcastBackend, BroadcastDriver, BroadcastLane, PublishResult } from './contract.js'
import { broadcastRouteKey } from './route-key.js'

const PENDING_PUBLISH_LIMIT = 1024

type PendingPublish = {
  payload: Uint8Array
  resolve: (result: PublishResult | Promise<PublishResult>) => void
  reject: (error: unknown) => void
}
type PendingRoute = { lane: BroadcastLane; entries: PendingPublish[]; bytes: number }

/** Owns the Broadcast subscription manager and the publish-readiness gate: a publish waits until this
 *  instance's own subscriptions on its route are ready, so its local subscribers receive it. */
function superviseBroadcastDriver(driver: BroadcastDriver): BroadcastBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, broadcastRouteKey)
  const pending = new Map<string, PendingRoute>()
  let disposal: Promise<void> | undefined

  const publish = (lane: BroadcastLane, payload: Uint8Array): PublishResult | Promise<PublishResult> => {
    const routeKey = broadcastRouteKey(lane)
    let route = pending.get(routeKey)
    if (route === undefined && subscriptions.readinessWaits(lane).length === 0) return driver.publish(lane, payload)
    const owned = payload.slice()
    const startFlushing = route === undefined
    route ??= { lane, entries: [], bytes: 0 }
    if (route.entries.length >= PENDING_PUBLISH_LIMIT || route.bytes + owned.byteLength > CHANNEL_BUFFER_LIMIT_BYTES) {
      return Promise.reject(new ChannelOverflowError('Broadcast readiness buffer overflow'))
    }
    pending.set(routeKey, route)
    route.bytes += owned.byteLength
    const held = route
    const result = new Promise<PublishResult>((resolve, reject) =>
      held.entries.push({ payload: owned, resolve, reject }),
    )
    if (startFlushing) void flush(routeKey, held)
    return result
  }

  const flush = async (routeKey: string, route: PendingRoute): Promise<void> => {
    try {
      while (route.entries.length > 0) {
        for (let waits = subscriptions.readinessWaits(route.lane); waits.length > 0; ) {
          await Promise.all(waits)
          waits = subscriptions.readinessWaits(route.lane)
        }
        const entries = route.entries.splice(0)
        route.bytes = 0
        for (const entry of entries) {
          try {
            entry.resolve(driver.publish(route.lane, entry.payload))
          } catch (error) {
            entry.reject(error)
          }
        }
      }
    } catch (error) {
      // A subscription on the route terminated while the publish waited for it.
      for (const entry of route.entries.splice(0)) entry.reject(error)
      route.bytes = 0
    } finally {
      pending.delete(routeKey)
    }
  }

  return {
    publish,
    subscribe: (lane, receiver) => subscriptions.subscribe(lane, receiver),
    dispose: () => (disposal ??= subscriptions.dispose()),
  }
}
