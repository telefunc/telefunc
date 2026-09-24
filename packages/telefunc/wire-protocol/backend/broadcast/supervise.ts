export { superviseBroadcastDriver }

import { ChannelOverflowError } from '../../channel-errors.js'
import { BROADCAST_ESTABLISH_HOLD_MS } from '../../constants.js'
import { SubscriptionManager } from '../subscription-manager.js'
import type { BroadcastBackend, BroadcastDriver, BroadcastLane, PublishResult } from './contract.js'
import { broadcastRouteKey } from './route-key.js'
import { assertDriverPosition } from '../driver-position.js'
import { isPromise } from '../../../utils/isPromise.js'
import { raceTimeout } from '../../../utils/raceTimeout.js'

function checked(result: PublishResult): PublishResult {
  assertDriverPosition(result)
  return result
}

const PENDING_PUBLISH_LIMIT = 1024

type PendingRoute = { established: Promise<void>; count: number; bytes: number }

/** Owns the Broadcast subscription manager and the publish-readiness gate: a publish waits, within the hold, until this
 *  instance's own new subscriptions on its route are established, so its local subscribers receive it. One that ends
 *  or never establishes doesn't fail the publish, and a later loss holds nothing. */
function superviseBroadcastDriver(driver: BroadcastDriver): BroadcastBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, broadcastRouteKey)
  const pending = new Map<string, PendingRoute>()
  let disposal: Promise<void> | undefined

  const publishNow = (lane: BroadcastLane, payload: Uint8Array): PublishResult | Promise<PublishResult> => {
    const result = driver.publish(lane, payload)
    return isPromise(result) ? result.then(checked) : checked(result)
  }

  const publish = (
    lane: BroadcastLane,
    payload: Uint8Array,
    bufferLimit: number,
  ): PublishResult | Promise<PublishResult> => {
    const routeKey = broadcastRouteKey(lane)
    const waiting = pending.get(routeKey)
    if (waiting === undefined && !subscriptions.hasEstablishing(lane)) return publishNow(lane, payload)
    const owned = payload.slice()
    const route = waiting ?? {
      established: raceTimeout(subscriptions.established(lane), BROADCAST_ESTABLISH_HOLD_MS, () => {}),
      count: 0,
      bytes: 0,
    }
    if (route.count >= PENDING_PUBLISH_LIMIT || route.bytes + owned.byteLength > bufferLimit) {
      return Promise.reject(new ChannelOverflowError('Broadcast readiness buffer overflow'))
    }
    pending.set(routeKey, route)
    route.count++
    route.bytes += owned.byteLength
    // Each caller continues in its own async context (on Cloudflare, its own session DO); reactions to one promise run
    // in call order.
    return route.established.then(() => {
      if (--route.count === 0) pending.delete(routeKey)
      route.bytes -= owned.byteLength
      return publishNow(lane, owned)
    })
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
