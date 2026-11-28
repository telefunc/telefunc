export { redis, dropRedisSubscribers, whileRedisSubscribersDown }

import IORedis from 'ioredis'
import { sleep } from './sleep'
import { present } from '../checked'

/** The connection Telefunc's Redis transport publishes on, when REDIS_URL is set. */
const redis = process.env.REDIS_URL ? new IORedis(process.env.REDIS_URL) : undefined
// ioredis reconnects by itself while a test restarts Redis.
redis?.on('error', () => {})

/** Drops this server's subscriber connection to Redis, and only that: what it publishes meanwhile still goes out. */
async function dropRedisSubscribers() {
  const store = present(redis, 'a Redis: REDIS_URL is set')
  const subscribers = String(await store.client('LIST')).matchAll(/^id=(\d+).* name=telefunc-subscriber-/gm)
  await Promise.all([...subscribers].map(([, id]) => store.client('KILL', 'ID', present(id, "a client's id"))))
}

/** Drops this server's subscriber connection to Redis and keeps it down while `meanwhile` runs: until it ends, the store
 *  refuses every new connection (its `maxclients` at 1; the connections it has stay), so nothing published meanwhile can
 *  reach this server's subscription. The connection comes back afterwards, as after any drop. Only on the store
 *  `pnpm preview:redis` started for this playground (REDIS_OWNED_BY_PLAYGROUND), never on one another run may share,
 *  and its limit is back however `meanwhile` ends. */
async function whileRedisSubscribersDown<T>(meanwhile: () => Promise<T>): Promise<T> {
  if (redis === undefined) throw new Error('No Redis: REDIS_URL is not set')
  if (process.env.REDIS_OWNED_BY_PLAYGROUND !== '1') {
    throw new Error(
      "REDIS_OWNED_BY_PLAYGROUND isn't 1, so this Redis isn't the one `pnpm preview:redis` started: its connections aren't held",
    )
  }
  const admin = redis
  const reply: unknown = await admin.config('GET', 'maxclients')
  const limit = Array.isArray(reply) ? reply[1] : undefined
  if (typeof limit !== 'string') throw new Error(`CONFIG GET maxclients answered ${JSON.stringify(reply)}`)
  await admin.config('SET', 'maxclients', '1')
  try {
    await dropRedisSubscribers()
    // The drop as the store sees it: no subscriber of this server's is connected, and none can connect again yet.
    for (let tries = 1; / name=telefunc-subscriber-/.test(String(await admin.client('LIST'))); tries++) {
      if (tries > 100) throw new Error("This server's Redis subscriber is still connected after CLIENT KILL")
      await sleep(10)
    }
    return await meanwhile()
  } finally {
    await admin.config('SET', 'maxclients', limit)
  }
}
