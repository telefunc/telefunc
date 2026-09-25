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

type HeldPublishes = { established: Promise<void>; count: number; bytes: Record<BroadcastRoute['kind'], number> }

/** Owns the Broadcast subscription manager and the publish-readiness gate: a publish waits, within the hold, until this
 *  instance's own new subscriptions on its key are established, so its local subscribers receive it; later publishes
 *  on the key, of either kind, queue behind it. One that ends or never establishes doesn't fail the publish, and a
 *  later loss holds nothing. */
function superviseBroadcastDriver(driver: BroadcastDriver): BroadcastBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, broadcastRouteKey)
  const pending = new Map<string, HeldPublishes>()
  let disposal: Promise<void> | undefined

  const publishNow = (route: BroadcastRoute, payload: Uint8Array): PublishResult | Promise<PublishResult> => {
    const result = driver.publish(route, payload)
    return isPromise(result) ? result.then(checked) : checked(result)
  }

  /** Resolves once no subscription on the key, of either kind, is establishing, including ones started meanwhile. */
  const keyEstablished = async (key: string): Promise<void> => {
    const routes = [
      { key, kind: 'text' },
      { key, kind: 'binary' },
    ] as const
    while (routes.some((route) => subscriptions.hasEstablishing(route)))
      await Promise.all(routes.map((route) => subscriptions.established(route)))
  }

  const publish = (
    route: BroadcastRoute,
    payload: Uint8Array,
    bufferLimit: number,
  ): PublishResult | Promise<PublishResult> => {
    const { key, kind } = route
    const waiting = pending.get(key)
    if (waiting === undefined && !subscriptions.hasEstablishing(route)) return publishNow(route, payload)
    const owned = payload.slice()
    const held = waiting ?? {
      established: raceTimeout(keyEstablished(key), BROADCAST_ESTABLISH_HOLD_MS, () => {}),
      count: 0,
      bytes: { text: 0, binary: 0 },
    }
    if (held.count >= PENDING_PUBLISH_LIMIT || held.bytes[kind] + owned.byteLength > bufferLimit) {
      return Promise.reject(new ChannelOverflowError('Broadcast readiness buffer overflow'))
    }
    pending.set(key, held)
    held.count++
    held.bytes[kind] += owned.byteLength
    // Each caller continues in its own async context (on Cloudflare, its own session DO); reactions to one promise run
    // in call order.
    return held.established.then(() => {
      if (--held.count === 0) pending.delete(key)
      held.bytes[kind] -= owned.byteLength
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
