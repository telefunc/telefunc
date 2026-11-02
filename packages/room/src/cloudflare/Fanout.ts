export { Fanout }

import type { CloudflareRegions, OrderingInfo } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_PREFIX } from '../server/constants.js'
import { laneRoute, type RouteInstallation } from './routes.js'

type FanoutOptions = { regions: CloudflareRegions }

/** A room authority's deliveries, through Broadcast's regions: a frame crosses to each bucket with a subscribed
 *  session once, and the bucket's coordinator hands it to each of them. An incarnation's frames go through the one
 *  coordinator its id picks in each bucket, so each session DO gets them in commit order. */
class Fanout {
  private readonly regions: CloudflareRegions

  constructor({ regions }: FanoutOptions) {
    this.regions = regions
  }

  /** Hands a frame committed on `laneKeys` to the session DO of every route, once per session DO however many of the
   *  lanes it subscribes to; the returned delivery resolves once each took it, and rejects once one failed to take
   *  it. */
  send(
    inc: string,
    laneKeys: readonly string[],
    routes: RouteInstallation[],
    payload: Uint8Array,
    info: OrderingInfo,
  ): Promise<void> {
    const members = new Map(routes.map(({ sessionDoId, bucket }) => [sessionDoId, { member: sessionDoId, bucket }]))
    const delivery = { routes: laneKeys.map((laneKey) => laneRoute(inc, laneKey)), payload, info }
    const sent = this.regions.forward(ROOM_CLOUDFLARE_PREFIX + inc, delivery, members.values())
    // Logged here: no committer awaits a binary or data frame's delivery.
    sent.catch((error: unknown) => console.error(error))
    return sent
  }
}
