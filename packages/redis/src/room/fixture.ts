// The Redis BackendFixture for the shared conformance suite (convergence W2) plus a couple of seams the
// Redis-specific stable-read scenarios need. DARK test infrastructure: reached only by the gated live
// lane (`vitest.room.config.ts`) and the Redis-specific spec — never by `@telefunc/redis`'s barrel.
//
// Fixture obligations honored (from W2a's lessons):
//   (a) the authority clock STARTS aligned with the caller clock (`Date.now()` at create) and diverges
//       only through `advanceAuthority`. A real Redis server clock cannot be advanced, so the backend
//       drives every time-sensitive Lua from this injected clock instead (layout.ts NOW seam); a backend
//       that read a caller's `Date.now()` would still fail every I13 killer, because this clock is the
//       shared authority, not the caller's.
//   (b) traces are declared honestly: Redis hands frames to the broker and never observes a receiver, so
//       handoffAwaitsReceiver=false and perTargetFailure=false; a head CX is a network round-trip, so
//       cxAppliesSynchronously=false — which OBLIGATES concurrentHeadCxBarrier (the suite throws without
//       it) and the barrier-forced I13(c) variant in the Redis-specific spec.

import { Redis } from 'ioredis'
import type {
  BackendFixture,
  BackendHarness,
  BackendTraces,
} from '../../../telefunc/wire-protocol/backend/conformance/harness.js'
import {
  CELLS_CX_LUA,
  COMMIT_LUA,
  DIRECTORY_DELETE_LUA,
  DIRECTORY_PUT_LUA,
  HEAD_CX_LUA,
  RETAINED_DELETE_LUA,
} from './layout.js'
import { RedisRoomBackend } from './backend.js'

const REDIS_TRACES: BackendTraces = {
  handoffAwaitsReceiver: false,
  perTargetFailure: false,
  cxAppliesSynchronously: false,
}

let fixtureSeq = 0

// A namespace unique per fixture instance so parallel vitest workers sharing one Redis never collide —
// no FLUSHDB (which would clobber a sibling worker), just an unshared prefix reclaimed on dispose.
function uniquePrefix(): string {
  const pid = typeof process !== 'undefined' && process.pid !== undefined ? process.pid : 0
  return `tfc:${pid}:${Date.now().toString(36)}:${++fixtureSeq}:`
}

// Realizes the two serial linearizations I13(c) needs on a backend whose CX application is asynchronous.
// The two head CXs are issued (queued on the single publisher connection) before either is awaited; the
// connection's FIFO command queue is what "releases them in the given order" — Redis then executes each
// script atomically in that order (spi.md I13 race-realization note). `first` executes fully before
// `second` observes the head, so asserting each order realizes that linearization.
const concurrentHeadCxBarrier = async <T>(first: () => Promise<T>, second: () => Promise<T>): Promise<[T, T]> => {
  const p1 = first()
  const p2 = second()
  return Promise.all([p1, p2]) as Promise<[T, T]>
}

export type StableReadProbe = (info: { roomId: string; inc: string }) => void | Promise<void>
export type SubscribeProbe = (channel: string) => void | Promise<void>
export type DropGenerationProbe = (info: { roomId: string; inc: string }) => void | Promise<void>
export type DirectoryDeleteProbe = (info: { roomId: string; incTag: string }) => void | Promise<void>

// The fixture the shared suite consumes, widened with the seams the Redis-specific stable-read scenarios
// use: the raw client (to force an insert/delete out of band), the concrete backend, and a settable
// probe fired inside the stable-read window.
export type RedisBackendFixture = BackendFixture & {
  backend: RedisRoomBackend
  redis: Redis
  subscriber: Redis
  subscriberId: number
  prefix: string
  setStableReadProbe(fn: StableReadProbe | null): void
  setBeforeSubscribe(fn: SubscribeProbe | null): void
  setBeforeDropGenerationUnregister(fn: DropGenerationProbe | null): void
  setBeforeDirectoryDeleteApply(fn: DirectoryDeleteProbe | null): void
}

export async function createRedisFixture(
  url: string,
  opts: { maxRetainedPayloadBytes?: number; useRedisAuthority?: boolean } = {},
): Promise<RedisBackendFixture> {
  const redis = new Redis(url, { maxRetriesPerRequest: 2, lazyConnect: false })
  const subscriber = new Redis(url, {
    autoResubscribe: false,
    lazyConnect: false,
    maxRetriesPerRequest: 1,
    retryStrategy: (attempt) => (attempt <= 5 ? 0 : null),
  })
  const subscriberId = Number(await subscriber.client('ID'))
  const prefix = uniquePrefix()
  // Authority time starts ALIGNED with the caller clock and only `advanceAuthority` moves it.
  let clock = Date.now()
  let probe: StableReadProbe | null = null
  let beforeSubscribe: SubscribeProbe | null = null
  let beforeDropGenerationUnregister: DropGenerationProbe | null = null
  let beforeDirectoryDeleteApply: DirectoryDeleteProbe | null = null

  const backend = new RedisRoomBackend({
    redis,
    subscriber,
    prefix,
    maxRetainedPayloadBytes: opts.maxRetainedPayloadBytes,
    authorityNow: opts.useRedisAuthority === true ? undefined : () => clock,
    stableReadProbe: (info) => probe?.(info),
    subscriptionRetryDelay: () => 0,
    testHooks: {
      beforeSubscribe: (channel) => beforeSubscribe?.(channel),
      beforeDropGenerationUnregister: (info) => beforeDropGenerationUnregister?.(info),
      beforeDirectoryDeleteApply: (info) => beforeDirectoryDeleteApply?.(info),
    },
  })

  // Pre-cache the scripts so a head-CX race never pays a one-off NOSCRIPT round-trip that could perturb
  // the FIFO ordering the barrier relies on.
  await Promise.all([
    redis.script('LOAD', HEAD_CX_LUA),
    redis.script('LOAD', CELLS_CX_LUA),
    redis.script('LOAD', COMMIT_LUA),
    redis.script('LOAD', RETAINED_DELETE_LUA),
    redis.script('LOAD', DIRECTORY_PUT_LUA),
    redis.script('LOAD', DIRECTORY_DELETE_LUA),
  ])

  return {
    backend,
    redis,
    subscriber,
    subscriberId,
    prefix,
    traces: REDIS_TRACES,
    authorityNow: () => clock,
    advanceAuthority: (ms) => {
      clock += ms
    },
    concurrentHeadCxBarrier,
    setStableReadProbe: (fn) => {
      probe = fn
    },
    setBeforeSubscribe: (fn) => {
      beforeSubscribe = fn
    },
    setBeforeDropGenerationUnregister: (fn) => {
      beforeDropGenerationUnregister = fn
    },
    setBeforeDirectoryDeleteApply: (fn) => {
      beforeDirectoryDeleteApply = fn
    },
    dispose: async () => {
      await backend.dispose()
      // Best-effort reclamation of this fixture's namespace (safe under parallel workers — the prefix is
      // unshared, so this never touches another fixture's keys).
      try {
        const keys = await scanPrefix(redis, prefix)
        if (keys.length > 0) await redis.unlink(...keys)
      } catch {
        // reclamation is best-effort; a live server FLUSHDBs between runs anyway
      }
      try {
        await redis.quit()
      } catch {
        redis.disconnect()
      }
    },
  }
}

export function makeRedisHarness(url: string): BackendHarness {
  return { name: 'redis', create: () => createRedisFixture(url) }
}

async function scanPrefix(redis: Redis, prefix: string): Promise<string[]> {
  const keys: string[] = []
  let cursor = '0'
  do {
    const [next, page] = await redis.scan(cursor, 'MATCH', `${prefix}*`, 'COUNT', 500)
    cursor = next
    for (const key of page) keys.push(key)
  } while (cursor !== '0')
  return keys
}
