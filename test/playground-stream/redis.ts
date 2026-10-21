export { redis }

import IORedis from 'ioredis'

/** The connection Telefunc's Redis transport publishes on, when REDIS_URL is set. */
const redis = process.env.REDIS_URL ? new IORedis(process.env.REDIS_URL) : undefined
// ioredis reconnects by itself while a test restarts Redis.
redis?.on('error', () => {})
