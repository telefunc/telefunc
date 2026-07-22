// The Redis BackendFixture for the shared conformance suite plus the deterministic controls used by the
// Redis-specific race scenarios. This test-only module is reached only by the gated live lane and is
// excluded from the package emit.
//
// Fixture obligations honored (from W2a's lessons):
//   (a) the authority clock STARTS aligned with the caller clock (`Date.now()` at create) and diverges
//       only through `advanceAuthority`. A real Redis server clock cannot be advanced, so the backend
//       command decorator supplies that clock to every time-sensitive Lua call; a backend
//       that read a caller's `Date.now()` would still fail every I13 killer, because this clock is the
//       shared authority, not the caller's.
//   (b) traces are declared honestly: Redis hands frames to the broker and never observes a receiver, so
//       handoffAwaitsReceiver=false and perTargetFailure=false; a head CX is a network round-trip, so
//       cxAppliesSynchronously=false — which OBLIGATES concurrentHeadCxBarrier (the suite throws without
//       it) and the barrier-forced I13(c) variant in the Redis-specific spec.

import { Redis } from 'ioredis'
import { callDefinedCommand } from '../callDefinedCommand.js'
import type {
  BackendFixture,
  BackendHarness,
  BackendTraces,
} from '../../../telefunc/wire-protocol/backend/conformance/harness.js'
import {
  generationTokensKey,
  gensKey,
  headKey,
  routeCaptureExpiriesKey,
  routeCapturesKey,
  REDIS_ROOM_COMMANDS,
} from './layout.js'
import { REDIS_GENERATION_CAPTURE_TTL_MS, RedisRoomBackend } from './backend.js'

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
export type GenerationCaptureProbe = (info: {
  roomId: string
  inc: string
  attemptId: string
  createdAt: number
  token: string
}) => void | Promise<void>
export type DropGenerationProbe = (info: { roomId: string; inc: string }) => void | Promise<void>
export type DirectoryDeleteProbe = (info: { roomId: string; incTag: string }) => void | Promise<void>
export type RedisRoomCommandCall = { name: string; args: readonly unknown[] }

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
  setAfterSubscribeAck(fn: SubscribeProbe | null): void
  setAfterGenerationCapture(fn: GenerationCaptureProbe | null): void
  setBeforeDropGenerationUnregister(fn: DropGenerationProbe | null): void
  setBeforeDirectoryDeleteApply(fn: DirectoryDeleteProbe | null): void
  commandCalls(): readonly RedisRoomCommandCall[]
  captureGenerationAttemptForTest(roomId: string, inc: string, attemptId: string, createdAt: number): Promise<string>
  countGenerationCapturesForTest(roomId: string): Promise<number>
  createPeerBackend(): Promise<{ backend: RedisRoomBackend; dispose(): Promise<void> }>
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
  let afterSubscribeAck: SubscribeProbe | null = null
  let afterGenerationCapture: GenerationCaptureProbe | null = null
  let beforeDropGenerationUnregister: DropGenerationProbe | null = null
  let beforeDirectoryDeleteApply: DirectoryDeleteProbe | null = null
  let bypassCaptureProbe = 0
  const acknowledgedChannels: string[] = []
  const commandCalls: RedisRoomCommandCall[] = []
  const restorers: Array<() => void> = []
  const peerDisposers = new Set<() => Promise<void>>()
  const realSetTimeout = globalThis.setTimeout
  globalThis.setTimeout = ((handler: TimerHandler, timeout?: number, ...args: unknown[]) =>
    realSetTimeout(
      handler,
      timeout !== undefined && timeout >= 100 && timeout <= 4_000 ? 0 : timeout,
      ...args,
    )) as typeof setTimeout
  restorers.push(() => {
    globalThis.setTimeout = realSetTimeout
  })

  const runProbe = async (label: string, probe: () => void | Promise<void>): Promise<void> => {
    let timer: ReturnType<typeof setTimeout> | undefined
    const watchdog = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Redis fixture probe '${label}' exceeded 5 seconds`)), 5_000)
      timer.unref?.()
    })
    try {
      await Promise.race([Promise.resolve().then(probe), watchdog])
    } finally {
      if (timer !== undefined) clearTimeout(timer)
    }
  }

  type DynamicCommands = Record<string, (...args: unknown[]) => Promise<unknown>>
  const wrapCommand = (
    client: Redis,
    name: string,
    wrapper: (invoke: (args: unknown[]) => Promise<unknown>, args: unknown[]) => Promise<unknown>,
  ): void => {
    const commands = client as unknown as DynamicCommands
    const original = commands[name]
    if (original === undefined)
      throw new Error(`Redis fixture: command '${name}' was not defined by the shipped backend`)
    commands[name] = (...args) => wrapper((next) => original.apply(client, next), args)
    restorers.push(() => {
      commands[name] = original
    })
  }

  const installAuthorityClock = (client: Redis): void => {
    const now = (): string => (opts.useRedisAuthority === true ? '' : String(clock))
    for (const [name, index] of [
      [REDIS_ROOM_COMMANDS.headCx.name, 4],
      [REDIS_ROOM_COMMANDS.captureGeneration.name, 5],
      [REDIS_ROOM_COMMANDS.validateGeneration.name, 5],
      [REDIS_ROOM_COMMANDS.commit.name, 5],
    ] as const) {
      wrapCommand(client, name, async (invoke, args) => {
        args[index] = now()
        return await invoke(args)
      })
    }
    wrapCommand(client, REDIS_ROOM_COMMANDS.cellsCx.name, async (invoke, args) => {
      args[Number(args[0]) + 1] = now()
      return await invoke(args)
    })
    if (opts.useRedisAuthority !== true) {
      const originalTime = client.time.bind(client)
      client.time = (async () => {
        const seconds = Math.floor(clock / 1_000)
        const microseconds = (clock - seconds * 1_000) * 1_000
        return [seconds, microseconds]
      }) as typeof client.time
      restorers.push(() => {
        client.time = originalTime
      })
    }
  }

  const roomIdFromKey = (key: unknown): string => {
    const match = String(key).match(/room:\{([^}]*)\}/)
    return match === null ? '' : decodeURIComponent(match[1] as string)
  }

  const duplicate = redis.duplicate.bind(redis)
  redis.duplicate = (() => subscriber) as typeof redis.duplicate
  let backend: RedisRoomBackend
  try {
    backend = new RedisRoomBackend({ redis, prefix, maxRetainedPayloadBytes: opts.maxRetainedPayloadBytes })
  } finally {
    redis.duplicate = duplicate
  }
  installAuthorityClock(redis)
  wrapCommand(redis, REDIS_ROOM_COMMANDS.validateGeneration.name, async (invoke, args) => {
    const channel = acknowledgedChannels.shift()
    if (channel !== undefined && afterSubscribeAck !== null) {
      await runProbe('after subscribe acknowledgement', () => afterSubscribeAck?.(channel))
    }
    return await invoke(args)
  })
  wrapCommand(redis, REDIS_ROOM_COMMANDS.captureGeneration.name, async (invoke, args) => {
    const reply = await invoke(args)
    if (afterGenerationCapture !== null && bypassCaptureProbe === 0) {
      const parsed = JSON.parse(String(reply)) as { token?: string }
      await runProbe('after generation capture', () =>
        afterGenerationCapture?.({
          roomId: roomIdFromKey(args[0]),
          inc: String(args[6]),
          attemptId: String(args[7]),
          createdAt: Number(args[8]),
          token: parsed.token ?? '',
        }),
      )
    }
    return reply
  })
  const mgetBuffer = redis.mgetBuffer.bind(redis)
  redis.mgetBuffer = (async (...keys: string[]) => {
    if (probe !== null) {
      const first = keys[0] ?? ''
      const incMatch = first.match(/:g:([^:]+):c:/)
      await runProbe('stable read', () => probe?.({ roomId: roomIdFromKey(first), inc: incMatch?.[1] ?? '' }))
    }
    return await mgetBuffer(...keys)
  }) as typeof redis.mgetBuffer
  restorers.push(() => {
    redis.mgetBuffer = mgetBuffer
  })
  wrapCommand(redis, REDIS_ROOM_COMMANDS.directoryDelete.name, async (invoke, args) => {
    if (beforeDirectoryDeleteApply !== null) {
      await runProbe('before directory delete', () =>
        beforeDirectoryDeleteApply?.({ roomId: String(args[2]), incTag: String(args[3]) }),
      )
    }
    return await invoke(args)
  })
  wrapCommand(redis, REDIS_ROOM_COMMANDS.dropGenerationFinalize.name, async (invoke, args) => {
    if (beforeDropGenerationUnregister !== null) {
      await runProbe('before generation unregister', () =>
        beforeDropGenerationUnregister?.({ roomId: roomIdFromKey(args[0]), inc: String(args[2]) }),
      )
    }
    return await invoke(args)
  })
  const subscribe = subscriber.subscribe.bind(subscriber)
  subscriber.subscribe = (async (...channels: string[]) => {
    if (beforeSubscribe !== null) {
      await runProbe('before subscribe', () => beforeSubscribe?.(channels[0] as string))
    }
    const result = await subscribe(...channels)
    acknowledgedChannels.push(channels[0] as string)
    return result
  }) as typeof subscriber.subscribe
  restorers.push(() => {
    subscriber.subscribe = subscribe
  })
  for (const command of Object.values(REDIS_ROOM_COMMANDS)) {
    wrapCommand(redis, command.name, async (invoke, args) => {
      commandCalls.push({ name: command.name, args: [...args] })
      return await invoke(args)
    })
  }

  // Pre-cache the scripts so a head-CX race never pays a one-off NOSCRIPT round-trip that could perturb
  // the FIFO ordering the barrier relies on.
  await Promise.all(Object.values(REDIS_ROOM_COMMANDS).map((command) => redis.script('LOAD', command.lua)))

  return {
    backend,
    redis,
    subscriber,
    subscriberId,
    prefix,
    traces: REDIS_TRACES,
    // Redis PUBLISH counts subscribed connections, not the callbacks multiplexed behind this fixture's
    // one subscriber connection. Keep these exact: two local callbacks still form one broker target,
    // and detaching either sibling leaves that same one connection subscribed for the survivor.
    expectedReceivers: { twoLocalSubscriptionsSameLane: 1, oneLocalSubscriptionAfterSiblingDetach: 1 },
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
    setAfterSubscribeAck: (fn) => {
      afterSubscribeAck = fn
    },
    setAfterGenerationCapture: (fn) => {
      afterGenerationCapture = fn
    },
    setBeforeDropGenerationUnregister: (fn) => {
      beforeDropGenerationUnregister = fn
    },
    setBeforeDirectoryDeleteApply: (fn) => {
      beforeDirectoryDeleteApply = fn
    },
    commandCalls: () => commandCalls,
    captureGenerationAttemptForTest: async (roomId, inc, attemptId, createdAt) => {
      bypassCaptureProbe++
      try {
        const reply = (await callDefinedCommand(redis, REDIS_ROOM_COMMANDS.captureGeneration.name, [
          headKey(prefix, roomId),
          gensKey(prefix, roomId),
          generationTokensKey(prefix, roomId),
          routeCapturesKey(prefix, roomId),
          routeCaptureExpiriesKey(prefix, roomId),
          String(clock),
          inc,
          attemptId,
          String(createdAt),
          String(REDIS_GENERATION_CAPTURE_TTL_MS),
        ])) as string
        const parsed = JSON.parse(reply) as { ok: true; token: string } | { rejected: true; reason: string }
        if ('rejected' in parsed) throw new Error(`subscribeLane: ${parsed.reason}`)
        return parsed.token
      } finally {
        bypassCaptureProbe--
      }
    },
    countGenerationCapturesForTest: async (roomId) => await redis.hlen(routeCapturesKey(prefix, roomId)),
    createPeerBackend: async () => {
      const peerRedis = redis.duplicate({ maxRetriesPerRequest: 2 })
      const peer = new RedisRoomBackend({ redis: peerRedis, prefix })
      installAuthorityClock(peerRedis)
      let disposed = false
      const dispose = async (): Promise<void> => {
        if (disposed) return
        disposed = true
        peerDisposers.delete(dispose)
        await peer.dispose()
        try {
          await peerRedis.quit()
        } catch {
          peerRedis.disconnect()
        }
      }
      peerDisposers.add(dispose)
      return {
        backend: peer,
        dispose,
      }
    },
    dispose: async () => {
      probe = null
      beforeSubscribe = null
      afterSubscribeAck = null
      acknowledgedChannels.length = 0
      commandCalls.length = 0
      afterGenerationCapture = null
      beforeDropGenerationUnregister = null
      beforeDirectoryDeleteApply = null
      await Promise.allSettled([...peerDisposers].map((dispose) => dispose()))
      await backend.dispose()
      for (const restore of restorers.reverse()) restore()
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
