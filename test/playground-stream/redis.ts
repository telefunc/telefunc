export { redis, dropRedisSubscribers }

import IORedis from 'ioredis'

/** The connection Telefunc's Redis transport publishes on, when REDIS_URL is set. */
const redis = process.env.REDIS_URL ? new IORedis(process.env.REDIS_URL) : undefined
// ioredis reconnects by itself while a test restarts Redis.
redis?.on('error', () => {})

/** Drops this server's subscriber connection to Redis, and only that: what it publishes meanwhile still goes out. */
async function dropRedisSubscribers() {
  const subscribers = String(await redis!.client('LIST')).matchAll(/^id=(\d+).* name=telefunc-subscriber-/gm)
  await Promise.all([...subscribers].map(([, id]) => redis!.client('KILL', 'ID', id!)))
}
