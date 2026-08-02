export { superviseBroadcastDriver }

import { SubscriptionManager } from '../subscription-manager.js'
import type { BroadcastBackend, BroadcastDriver } from './contract.js'
import { broadcastRouteKey } from './route-key.js'

/** Owns the Broadcast subscription manager, including publish-readiness buffering. */
function superviseBroadcastDriver(
  driver: BroadcastDriver,
  dispose: () => void | Promise<void> = () => {},
): BroadcastBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, broadcastRouteKey)
  let disposal: Promise<void> | undefined

  return {
    publish: (lane, payload) =>
      subscriptions.publish(lane, payload, (ownedPayload) => driver.publish(lane, ownedPayload)),
    subscribe: (lane, receiver) => subscriptions.subscribe(lane, receiver),
    dispose: () => (disposal ??= subscriptions.dispose().then(dispose)),
  }
}
