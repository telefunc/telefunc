export { superviseBroadcastDriver }

import { ChannelOverflowError } from '../../channel-errors.js'
import { BROADCAST_ESTABLISH_HOLD_MS } from '../../constants.js'
import { SubscriptionManager } from '../subscription-manager.js'
import type { BroadcastBackend, BroadcastDriver, BroadcastRoute, PublishResult } from './contract.js'
import { broadcastRouteKey } from './route-key.js'
import { assertDriverPosition } from '../driver-position.js'
import { isPromise } from '../../../utils/isPromise.js'
import { raceTimeout } from '../../../utils/raceTimeout.js'

function checked(result: PublishResult): PublishResult {
  assertDriverPosition(result)
  return result
}

const PENDING_PUBLISH_LIMIT = 1024

type HeldPublishes = { established: Promise<void>; count: number; bytes: number }

/** Owns the Broadcast subscription manager and the publish-readiness gate: a publish waits, within the hold, until this
 *  instance's own new subscriptions on its route are established, so its local subscribers receive it. One that ends
 *  or never establishes doesn't fail the publish, and a later loss holds nothing. */
function superviseBroadcastDriver(driver: BroadcastDriver): BroadcastBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, broadcastRouteKey)
  const pending = new Map<string, HeldPublishes>()
  let disposal: Promise<void> | undefined

  const publishNow = (route: BroadcastRoute, payload: Uint8Array): PublishResult | Promise<PublishResult> => {
    const result = driver.publish(route, payload)
    return isPromise(result) ? result.then(checked) : checked(result)
  }

  const publish = (
    route: BroadcastRoute,
    payload: Uint8Array,
    bufferLimit: number,
  ): PublishResult | Promise<PublishResult> => {
    const routeKey = broadcastRouteKey(route)
    const waiting = pending.get(routeKey)
    if (waiting === undefined && !subscriptions.hasEstablishing(route)) return publishNow(route, payload)
    const owned = payload.slice()
    const held = waiting ?? {
      established: raceTimeout(subscriptions.established(route), BROADCAST_ESTABLISH_HOLD_MS, () => {}),
      count: 0,
      bytes: 0,
    }
    if (held.count >= PENDING_PUBLISH_LIMIT || held.bytes + owned.byteLength > bufferLimit) {
      return Promise.reject(new ChannelOverflowError('Broadcast readiness buffer overflow'))
    }
    pending.set(routeKey, held)
    held.count++
    held.bytes += owned.byteLength
    // Each caller continues in its own async context (on Cloudflare, its own session DO); reactions to one promise run
    // in call order.
    return held.established.then(() => {
      if (--held.count === 0) pending.delete(routeKey)
      held.bytes -= owned.byteLength
      return publishNow(route, owned)
    })
  }

  return {
    publish,
    subscribe: (route, receiver) =>
      subscriptions.subscribe(route, (payload, info) => {
        assertDriverPosition(info)
        return receiver(payload, info)
      }),
    hasSubscriptions: () => subscriptions.hasSubscriptions(),
    dispose: () => (disposal ??= subscriptions.dispose()),
  }
}
