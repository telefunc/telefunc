export { installRedis }
export type { InstallRedisOptions }

import { deploymentOf, type RedisClient } from './ioredis.js'
import { installBackend } from 'telefunc/__internal'
import { RedisBackend } from './backend.js'
import { DEFAULT_PREFIX } from './keys.js'

/** Installs the Redis backend, so Broadcast and Room work across instances. Pair with sticky-session routing
 *  at the load balancer so each client's channel traffic stays on one instance. */
function installRedis(redis: RedisClient, options: InstallRedisOptions = {}): void {
  const prefix = options.prefix ?? DEFAULT_PREFIX
  // A module evaluated again (a dev server, HMR) builds a new client for the same deployment, which keeps the backend.
  installBackend(() => new RedisBackend({ redis, prefix }), ['redis', deploymentOf(redis), prefix])
}

type InstallRedisOptions = {
  /** Default: `tf:`. */
  prefix?: string
}
