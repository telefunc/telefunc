export { broadcastRouteKey }

import type { BroadcastRoute } from '../driver.js'

function broadcastRouteKey(route: BroadcastRoute): string {
  return `${route.kind}:${encodeURIComponent(route.key)}`
}
