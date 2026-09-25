export { superviseBroadcastDriver }

import { ChannelOverflowError } from '../../channel-errors.js'
import { SubscriptionManager } from '../subscription-manager.js'
import type { BroadcastBackend, BroadcastDriver, BroadcastRoute, PublishResult } from './contract.js'
import { broadcastRouteKey } from './route-key.js'
import { assertDriverPosition } from '../driver-position.js'
import { isPromise } from '../../../utils/isPromise.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'

function checked(result: PublishResult): PublishResult {
  assertDriverPosition(result)
  return result
}

const PENDING_PUBLISH_LIMIT = 1024

/** A held publish is bounded like a channel's buffered sends; read only then, as resolving the config is costly. */
function heldByteLimit(kind: BroadcastRoute['kind']): number {
  const { channel } = getServerConfig()
  return kind === 'binary' ? channel.bufferLimitBinary : channel.bufferLimit
}

/** Owns the Broadcast subscription manager and the publish-readiness gate: a publish waits, within the hold, until this
 *  instance's own new subscriptions on its key are established, so its local subscribers receive it; later publishes
 *  on the key, of either kind, queue behind it. One that ends or never establishes doesn't fail the publish, and a
 *  later loss holds nothing. */
function superviseBroadcastDriver(driver: BroadcastDriver): BroadcastBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, broadcastRouteKey)
  let disposal: Promise<void> | undefined

  const publishNow = (route: BroadcastRoute, payload: Uint8Array): PublishResult | Promise<PublishResult> => {
    const result = driver.publish(route, payload)
    return isPromise(result) ? result.then(checked) : checked(result)
  }

  const publish = (route: BroadcastRoute, payload: Uint8Array): PublishResult | Promise<PublishResult> => {
    const { key, kind } = route
    // A driver may send later (a queued or re-sent command, an ordered RPC); `slice()` of a Node Buffer is a view.
    const owned = new Uint8Array(payload)
    const keyRoutes = [
      { key, kind: 'text' },
      { key, kind: 'binary' },
    ] as const
    return subscriptions.afterEstablished(key, keyRoutes, () => publishNow(route, owned), {
      class: kind,
      bytes: owned.byteLength,
      fits: (sends, bytes) => sends <= PENDING_PUBLISH_LIMIT && bytes <= heldByteLimit(kind),
      overflow: () => new ChannelOverflowError('Broadcast readiness buffer overflow'),
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
