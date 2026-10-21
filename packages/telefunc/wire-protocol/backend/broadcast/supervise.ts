export { superviseBroadcastDriver }

import { SubscriptionManager } from '../SubscriptionManager.js'
import { heldSendWeight } from '../heldSendWeight.js'
import type { BroadcastBackend, BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult } from './contract.js'
import type { BackendPayload, BackendReceiver } from '../subscription.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import type { ChannelPublishAck } from '../../channel/channel.js'
import { broadcastRouteKey } from './routeKey.js'
import { assertDriverPosition } from '../driverPosition.js'
import { assert } from '../../../utils/assert.js'
import { isPromise } from '../../../utils/isPromise.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'

/** The driver's receipt as the public ack, carrying its key like a subscriber's `info`. */
function toAck(key: string, result: PublishResult): ChannelPublishAck {
  assertDriverPosition(result)
  const { seq, timestamp, receivers } = result
  return receivers === undefined ? { key, seq, timestamp } : { key, seq, timestamp, receivers }
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
  const subscriptions = new SubscriptionManager({
    driver: driver.subscriptions,
    sourceKey: broadcastRouteKey,
    checkDelivery,
  })

  const publishNow = (
    route: BroadcastRoute,
    payload: BroadcastPayload,
  ): ChannelPublishAck | Promise<ChannelPublishAck> => {
    const result = driver.publish(route, payload)
    return isPromise(result) ? result.then((receipt) => toAck(route.key, receipt)) : toAck(route.key, result)
  }

  const publish = (
    route: BroadcastRoute,
    payload: BroadcastPayload,
  ): ChannelPublishAck | Promise<ChannelPublishAck> => {
    const { key, kind } = route
    let owned = payload
    return subscriptions.afterEstablished(
      () => publishNow(route, owned),
      () => ({
        key,
        sources: [
          { key, kind: 'text' },
          { key, kind: 'binary' },
        ],
        weight: heldSendWeight(
          kind,
          broadcastRouteKey(route),
          () => (typeof payload === 'string' ? utf8ByteLength(payload) : payload.byteLength),
          'Broadcast',
        ),
        // Only a held publish copies: the caller may reuse its bytes once the publish returns.
        onHeld: () => {
          if (typeof payload !== 'string') owned = new Uint8Array(payload)
        },
      }),
    )
  }

  return {
    publish,
    // The manager hands a route's consumers only the deliveries checkDelivery passed.
    subscribe: (route, receiver) => subscriptions.subscribe(route, receiver as BackendReceiver<BackendPayload>),
    dispose: () => subscriptions.dispose(),
  }
}
