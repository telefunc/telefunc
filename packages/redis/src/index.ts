export { installRedis }
export type { InstallRedisOptions }

import type { RedisClient } from './ioredis.js'
import { installBackend } from 'telefunc/__internal'
import { RedisBackend } from './room/backend.js'
import { DEFAULT_ROOM_PREFIX } from './room/layout.js'

function installRedis(redis: RedisClient, options: InstallRedisOptions = {}): void {
  const prefix = options.prefix ?? DEFAULT_ROOM_PREFIX
  installBackend(() => new RedisBackend({ redis, prefix }), ['redis', redis, prefix])
}

type InstallRedisOptions = {
  /** Default: `tf:`. */
  prefix?: string
}
