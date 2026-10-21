export { broadcastRouteKey }

import type { BroadcastRoute } from './contract.js'

const broadcastRouteKey = (route: BroadcastRoute): string => `${route.kind}:${encodeURIComponent(route.key)}`
