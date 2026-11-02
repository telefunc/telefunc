export { BroadcastPlane }

import { Subscriptions, type BackendReceiver, type BackendSubscription } from '../Subscriptions.js'
import type { BackendPayload, BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult } from '../driver.js'
import type { BroadcastKind } from '../../wire/payloads.js'
import type { ChannelPublishAck } from '../../channel/channel.js'
import { broadcastRouteKey } from './routeKey.js'
import { assertDriverOrdering } from '../driverOrdering.js'
import { isPromise } from '../../../utils/isPromise.js'

type BroadcastPlaneOptions = { driver: BroadcastDriver }

/** What core consumes: Broadcast on a driver, its publishes checked, its subscriptions shared by route and host. */
class BroadcastPlane {
  private readonly driver: BroadcastDriver
  private readonly subscriptions: Subscriptions<BroadcastRoute>

  constructor({ driver }: BroadcastPlaneOptions) {
    this.driver = driver
    this.subscriptions = new Subscriptions({ driver })
  }

  /** Publishes `payload` as it is at the call. */
  publish<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): ChannelPublishAck | Promise<ChannelPublishAck> {
    const result = this.driver.publish(route, payload)
    return isPromise(result) ? result.then((receipt) => toAck(route.key, receipt)) : toAck(route.key, result)
  }

  subscribe<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    receiver: BackendReceiver<BroadcastPayload<Kind>>,
  ): BackendSubscription {
    const source = { key: broadcastRouteKey(route), delivers: route.kind }
    const receive = receiver as BackendReceiver<BackendPayload>
    const lastSeq = this.driver.lastSeq?.(route.key) ?? 0
    if (lastSeq === 0) return this.subscriptions.subscribe(route, source, receive)
    // A consumer gets only what is published after it subscribed, as the driver fixed each publish's recipients.
    return this.subscriptions.subscribe(route, source, (payload, ordering) => {
      if (ordering.seq > lastSeq) receive(payload, ordering)
    })
  }

  dispose(): Promise<void> {
    return this.subscriptions.dispose()
  }
}

/** The driver's receipt as the public ack, carrying its key like a subscriber's `info`. */
function toAck(key: string, result: PublishResult): ChannelPublishAck {
  assertDriverOrdering(result)
  const { seq, timestamp, receivers } = result
  return receivers === undefined ? { key, seq, timestamp } : { key, seq, timestamp, receivers }
}
