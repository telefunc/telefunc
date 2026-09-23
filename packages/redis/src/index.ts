export { installRedis }
export type { InstallRedisOptions }

import type { RedisClient } from './ioredis.js'
import { installBackend } from 'telefunc/__internal'
import { RedisBackend } from './backend.js'
import { DEFAULT_PREFIX } from './keys.js'

function installRedis(redis: RedisClient, options: InstallRedisOptions = {}): void {
  const prefix = options.prefix ?? DEFAULT_PREFIX
  installBackend(() => new RedisBackend({ redis, prefix }), ['redis', redis, prefix])
}

type InstallRedisOptions = {
  /** Default: `tf:`. */
  prefix?: string
}
