export { BroadcastPlane }

import { SubscriptionManager, type BackendSubscription, type SourceRules } from '../SubscriptionManager.js'
import type { BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult } from '../driver.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import type { BroadcastKind } from '../../wire/payloads.js'
import type { ChannelPublishAck } from '../../channel/channel.js'
import { broadcastRouteKey } from './routeKey.js'
import { assertDriverOrdering } from '../driverOrdering.js'
import { isPromise } from '../../../utils/isPromise.js'

type BroadcastPlaneOptions = { driver: BroadcastDriver }

const broadcastSources: SourceRules<BroadcastRoute> = {
  key: broadcastRouteKey,
  deliveryKind: (route) => route.kind,
  // A key's two kinds share its order, so a publish of either waits behind the key's waiting ones.
  waitOf: (route) => ({
    domain: route.key,
    waitsFor: [
      { key: route.key, kind: 'text' },
      { key: route.key, kind: 'binary' },
    ],
    budget: broadcastRouteKey(route),
    limit: route.kind,
  }),
}

/** What core consumes: Broadcast on a driver, its publishes checked, its subscriptions shared by route and host. */
class BroadcastPlane {
  private readonly driver: BroadcastDriver
  private readonly subscriptions: SubscriptionManager<BroadcastRoute>

  constructor({ driver }: BroadcastPlaneOptions) {
    this.driver = driver
    this.subscriptions = new SubscriptionManager({ driver, sources: broadcastSources, sends: 'Broadcast publish' })
  }

  /** Publishes `payload` as it is at the call. */
  publish<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): ChannelPublishAck | Promise<ChannelPublishAck> {
    return this.subscriptions.establishing
      ? this.subscriptions.afterEstablished(route, payload, (owned) => this.publishNow(route, owned))
      : this.publishNow(route, payload)
  }

  subscribe<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    receive: (payload: BroadcastPayload<Kind>, ordering: OrderingInfo) => void,
  ): BackendSubscription {
    // The manager checked that a route delivers its kind.
    const deliver = receive as (payload: BroadcastPayload, ordering: OrderingInfo) => void
    const published = this.driver.published?.(route.key) ?? 0
    if (published === 0) return this.subscriptions.subscribe(route, deliver)
    // A consumer gets only what is published after it subscribed, as the driver fixed each publish's recipients.
    return this.subscriptions.subscribe(route, (payload, ordering) => {
      if (ordering.seq > published) deliver(payload, ordering)
    })
  }

  dispose(): Promise<void> {
    return this.subscriptions.dispose()
  }

  private publishNow<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): ChannelPublishAck | Promise<ChannelPublishAck> {
    const result = this.driver.publish(route, payload)
    return isPromise(result) ? result.then((receipt) => toAck(route.key, receipt)) : toAck(route.key, result)
  }
}

/** The driver's receipt as the public ack, carrying its key like a subscriber's `info`. */
function toAck(key: string, result: PublishResult): ChannelPublishAck {
  assertDriverOrdering(result)
  const { seq, timestamp, receivers } = result
  return receivers === undefined ? { key, seq, timestamp } : { key, seq, timestamp, receivers }
}
