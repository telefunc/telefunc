export { broadcastRouteKey }

import type { BroadcastRoute } from './contract.js'

function broadcastRouteKey(route: BroadcastRoute): string {
  return `${route.kind}:${encodeURIComponent(route.key)}`
}
