export { superviseBroadcastDriver }

import type { BackendReceiver, Subscriptions } from '../Subscriptions.js'
import type { BroadcastPlane } from './contract.js'
import type { BackendPayload, BroadcastDriver, BroadcastRoute, PublishResult } from '../driver.js'
import type { ChannelPublishAck } from '../../channel/channel.js'
import { broadcastRouteKey } from './routeKey.js'
import { assertDriverOrdering } from '../driverOrdering.js'
import { isPromise } from '../../../utils/isPromise.js'

/** The driver's receipt as the public ack, carrying its key like a subscriber's `info`. */
function toAck(key: string, result: PublishResult): ChannelPublishAck {
  assertDriverOrdering(result)
  const { seq, timestamp, receivers } = result
  return receivers === undefined ? { key, seq, timestamp } : { key, seq, timestamp, receivers }
}

/** Broadcast on a driver: its publishes checked, its subscriptions shared by route and host. */
function superviseBroadcastDriver(
  driver: BroadcastDriver,
  subscriptions: Subscriptions<BroadcastRoute>,
): BroadcastPlane {
  return {
    publish: (route, payload) => {
      const result = driver.publish(route, payload)
      return isPromise(result) ? result.then((receipt) => toAck(route.key, receipt)) : toAck(route.key, result)
    },
    subscribe: (route, receiver) => {
      const source = { key: broadcastRouteKey(route), delivers: route.kind }
      const receive = receiver as BackendReceiver<BackendPayload>
      const lastSeq = driver.lastSeq?.(route.key) ?? 0
      if (lastSeq === 0) return subscriptions.subscribe(route, source, receive)
      // A consumer gets only what is published after it subscribed, as the driver fixed each publish's recipients.
      return subscriptions.subscribe(route, source, (payload, ordering) => {
        if (ordering.seq > lastSeq) receive(payload, ordering)
      })
    },
    dispose: () => subscriptions.dispose(),
  }
}
