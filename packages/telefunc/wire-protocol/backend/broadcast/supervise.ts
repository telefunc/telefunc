export { superviseBroadcastDriver }

import { ChannelOverflowError } from '../../channel-errors.js'
import { SubscriptionManager } from '../subscription-manager.js'
import type { BroadcastBackend, BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult } from './contract.js'
import type { BackendPayload, BackendReceiver } from '../subscription.js'
import type { OrderingInfo } from '../../ordering-frame.js'
import { broadcastRouteKey } from './route-key.js'
import { assertDriverPosition } from '../driver-position.js'
import { assert } from '../../../utils/assert.js'
import { isPromise } from '../../../utils/isPromise.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
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

/** A text route carries a string, a binary route bytes. */
function checkDelivery(route: BroadcastRoute, payload: BackendPayload, info: OrderingInfo): void {
  assertDriverPosition(info)
  assert((route.kind === 'text') === (typeof payload === 'string'))
}

/** Owns the Broadcast subscription manager and the publish-readiness gate: a publish waits, within the hold, until this
 *  instance's own new subscriptions on its key are established, so its local subscribers receive it; later publishes
 *  on the key, of either kind, queue behind it. One that ends or never establishes doesn't fail the publish, and a
 *  later loss holds nothing. */
function superviseBroadcastDriver(driver: BroadcastDriver): BroadcastBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, broadcastRouteKey, checkDelivery)
  let disposal: Promise<void> | undefined

  const publishNow = (route: BroadcastRoute, payload: BroadcastPayload): PublishResult | Promise<PublishResult> => {
    const result = driver.publish(route, payload)
    return isPromise(result) ? result.then(checked) : checked(result)
  }

  const publish = (route: BroadcastRoute, payload: BroadcastPayload): PublishResult | Promise<PublishResult> => {
    const { key, kind } = route
    // A driver may send later (a queued or re-sent command, an ordered RPC), so bytes are copied: `slice()` of a Node
    // Buffer is a view. A string can't change.
    const owned = typeof payload === 'string' ? payload : new Uint8Array(payload)
    return subscriptions.afterEstablished(
      () => publishNow(route, owned),
      () => ({
        key,
        sources: [
          { key, kind: 'text' },
          { key, kind: 'binary' },
        ],
        weight: {
          class: kind,
          bytes: () => (typeof owned === 'string' ? utf8ByteLength(owned) : owned.byteLength),
          fits: (sends, bytes) => sends <= PENDING_PUBLISH_LIMIT && bytes <= heldByteLimit(kind),
          overflow: () => new ChannelOverflowError('Broadcast readiness buffer overflow'),
        },
      }),
    )
  }

  return {
    publish,
    // The manager hands a route's consumers only the deliveries checkDelivery passed.
    subscribe: (route, receiver) => subscriptions.subscribe(route, receiver as BackendReceiver<BackendPayload>),
    dispose: () => (disposal ??= subscriptions.dispose()),
  }
}
