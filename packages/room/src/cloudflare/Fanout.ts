export { Fanout }

import type { CloudflareRegions, OrderingInfo } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_PREFIX } from '../server/constants.js'
import { laneRoute } from './routes.js'

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
   *  it, or, where `owed`, had no receiver for it. */
  send(
    inc: string,
    laneKeys: readonly string[],
    routes: { holder: string; bucket: string }[],
    payload: Uint8Array,
    info: OrderingInfo,
    owed: boolean,
  ): Promise<void> {
    const members = new Map(routes.map(({ holder, bucket }) => [holder, { member: holder, bucket }]))
    const routed = laneKeys.map((laneKey) => laneRoute(inc, laneKey))
    const delivery = owed ? { routes: routed, payload, info, owed: true as const } : { routes: routed, payload, info }
    return this.regions.forward(ROOM_CLOUDFLARE_PREFIX + inc, delivery, members.values())
  }
}
