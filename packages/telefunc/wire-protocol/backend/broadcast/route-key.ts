export { broadcastRouteKey }

import type { BroadcastRoute } from './contract.js'

const broadcastRouteKey = (route: BroadcastRoute) => `${route.kind}:${encodeURIComponent(route.key)}`
