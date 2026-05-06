export { cleanupState, resetCleanupState, getCleanupStateSnapshot }

import IORedis from 'ioredis'

type CleanupState = Record<string, string>

// Cluster-safe state. Writes go through to Redis when REDIS_URL is set so the API
// endpoint on instance B can see what a telefunction wrote on instance A. Reads via
// `getCleanupStateSnapshot()` always pull from Redis (authoritative); local map is
// kept too so synchronous reads inside telefunctions still work for same-instance
// happy paths.
const REDIS_KEY = '__telefunc_test_cleanupState'
const localKey = Symbol.for(REDIS_KEY)
const localState: CleanupState = ((globalThis as Record<symbol, unknown>)[localKey] ??= {}) as CleanupState

const redis: IORedis | null = process.env.REDIS_URL ? new IORedis(process.env.REDIS_URL) : null

const cleanupState = new Proxy(localState, {
  set(target, key, value) {
    if (typeof key !== 'string') return false
    target[key] = value
    if (redis) void redis.hset(REDIS_KEY, key, String(value))
    return true
  },
  deleteProperty(target, key) {
    if (typeof key !== 'string') return false
    delete target[key]
    if (redis) void redis.hdel(REDIS_KEY, key)
    return true
  },
})

async function resetCleanupState() {
  for (const k of Object.keys(localState)) delete localState[k]
  if (redis) await redis.del(REDIS_KEY)
}

async function getCleanupStateSnapshot(): Promise<CleanupState> {
  if (redis) return await redis.hgetall(REDIS_KEY)
  return { ...localState }
}
