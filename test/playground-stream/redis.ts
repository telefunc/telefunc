export { redis, anotherServer }

import IORedis from 'ioredis'
import { RedisTransport } from '@telefunc/redis'

/** The connection Telefunc's Redis transport publishes on, when REDIS_URL is set. */
const redis = process.env.REDIS_URL ? new IORedis(process.env.REDIS_URL) : undefined

/** The broadcast transport of another server on the same Redis, as a publish from another instance reaches it. Through
 *  REDIS_ANOTHER_SERVER_URL where set: a lane that delays this server's own connection gives the other a direct one. */
const anotherServer = process.env.REDIS_URL
  ? new RedisTransport({ redis: new IORedis(process.env.REDIS_ANOTHER_SERVER_URL ?? process.env.REDIS_URL) })
  : undefined
