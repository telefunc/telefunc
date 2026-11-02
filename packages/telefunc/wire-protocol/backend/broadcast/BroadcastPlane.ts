export { BroadcastPlane }

import { SharedSubscriptions, type BackendSubscription, type SourceRules } from '../SharedSubscriptions.js'
import type { BackendListener, BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult } from '../driver.js'
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
  sendWaitsFor: (route) => ({
    orderedWith: route.key,
    subscriptions: [
      { key: route.key, kind: 'text' },
      { key: route.key, kind: 'binary' },
    ],
    countsAgainst: broadcastRouteKey(route),
    bufferLimit: route.kind === 'binary' ? 'bufferLimitBinary' : 'bufferLimit',
  }),
}

/** What core consumes: Broadcast on a driver, its publishes checked, its subscriptions shared by route and host. */
class BroadcastPlane {
  private readonly driver: BroadcastDriver
  private readonly subscriptions: SharedSubscriptions<BroadcastRoute>

  constructor({ driver }: BroadcastPlaneOptions) {
    this.driver = driver
    this.subscriptions = new SharedSubscriptions({ driver, sources: broadcastSources, sendName: 'Broadcast publish' })
  }

  /** Publishes `payload` as it is at the call. */
  publish<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): ChannelPublishAck | Promise<ChannelPublishAck> {
    return this.subscriptions.sendsMayWait ? this.publishInTurn(route, payload) : this.publishNow(route, payload)
  }

  subscribe<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    listener: BackendListener<BroadcastPayload<Kind>>,
  ): BackendSubscription {
    // The shared subscriptions checked that a route delivers its kind.
    const deliver = listener as BackendListener
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

  /** While a subscription on this host establishes: publishes in its turn, then lets later publishes past. */
  private async publishInTurn<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): Promise<ChannelPublishAck> {
    const waiting = this.subscriptions.waitTurn(route, payload)
    if (waiting === null) return this.publishNow(route, payload)
    const owned = await waiting.turn
    try {
      return this.publishNow(route, owned)
    } finally {
      waiting.sent()
    }
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
  return { key, seq: result.seq, timestamp: result.timestamp }
}
