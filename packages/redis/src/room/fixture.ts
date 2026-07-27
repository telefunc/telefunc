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

import { createHash } from 'node:crypto'
import { Cluster, Redis } from 'ioredis'
import { callDefinedCommand } from '../callDefinedCommand.js'
import type {
  BackendFixture,
  BackendHarness,
  BackendTraces,
} from '../../../telefunc/wire-protocol/backend/conformance/harness.js'
import {
  channelKey,
  generationTokensKey,
  gensKey,
  headKey,
  laneKey,
  orderKey,
  routeCaptureExpiriesKey,
  routeCapturesKey,
  REDIS_ROOM_COMMANDS,
} from './layout.js'
import { REDIS_GENERATION_CAPTURE_TTL_MS, RedisRoomBackend } from './backend.js'
import { installRedisSubscriptionScheduler, type RedisSubscriptionScheduler } from './subscriber-transport.js'

const REDIS_TRACES: BackendTraces = {
  handoffAwaitsReceiver: false,
  perTargetFailure: false,
  cxAppliesSynchronously: false,
}

let fixtureSeq = 0

const acceleratedSubscriptionScheduler: RedisSubscriptionScheduler = {
  schedule(_delayMs, task) {
    const handle = setTimeout(() => void task(), 0)
    return () => clearTimeout(handle)
  },
}

export type RedisClusterNode = { host: string; port: number }
type RoomRedisClient = Redis | Cluster

function configuredClusterNodes(): RedisClusterNode[] | undefined {
  const raw = process.env.TELEFUNC_TEST_REDIS_CLUSTER_NODES
  if (raw === undefined || raw === '') return undefined
  return parseRedisClusterNodes(raw)
}

export function parseRedisClusterNodes(raw: string): RedisClusterNode[] {
  const nodes = raw.split(',').map((entry) => {
    const [host, portText] = entry.trim().split(':')
    const port = Number(portText)
    if (host === undefined || host === '' || !Number.isInteger(port) || port <= 0 || port > 65_535) {
      throw new Error(`Redis fixture: invalid TELEFUNC_TEST_REDIS_CLUSTER_NODES entry '${entry}'`)
    }
    return { host, port }
  })
  if (nodes.length < 3) throw new Error('Redis fixture: Cluster certification requires at least three startup nodes')
  return nodes
}

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
export type RedisRoomCommandCall = { name: string; args: readonly unknown[] }

// The fixture the shared suite consumes, widened with the seams the Redis-specific stable-read scenarios
// use: the raw client (to force an insert/delete out of band), the concrete backend, and a settable
// probe fired inside the stable-read window.
export type RedisBackendFixture = BackendFixture & {
  backend: RedisRoomBackend
  redis: RoomRedisClient
  subscriber: Redis
  allowedReceiverCountsAtAuthority: NonNullable<BackendFixture['allowedReceiverCountsAtAuthority']>
  prefix: string
  setStableReadProbe(fn: StableReadProbe | null): void
  setBeforeSubscribe(fn: SubscribeProbe | null): void
  setAfterSubscribeAck(fn: SubscribeProbe | null): void
  setAfterGenerationCapture(fn: GenerationCaptureProbe | null): void
  setBeforeDropGenerationUnregister(fn: DropGenerationProbe | null): void
  commandCalls(): readonly RedisRoomCommandCall[]
  captureGenerationAttemptForTest(roomId: string, inc: string, attemptId: string, createdAt: number): Promise<string>
  countGenerationCapturesForTest(roomId: string): Promise<number>
  pubSubClientIdsForTest(): Promise<number[]>
  pubSubSubscriberCountForTest(channel: string): Promise<number>
  killSubscriberForTest(id?: number): Promise<void>
  createPeerBackend(): Promise<{ backend: RedisRoomBackend; dispose(): Promise<void> }>
}

export async function createRedisFixture(
  url: string,
  opts: {
    maxRetainedPayloadBytes?: number
    useRedisAuthority?: boolean
    clusterNodes?: RedisClusterNode[]
  } = {},
): Promise<RedisBackendFixture> {
  const clusterNodes = opts.clusterNodes ?? configuredClusterNodes()
  const prefix = uniquePrefix()
  const redis: RoomRedisClient =
    clusterNodes === undefined
      ? new Redis(url, { maxRetriesPerRequest: 2, lazyConnect: false })
      : new Cluster(
          clusterNodes.map((node) => ({ ...node })),
          {
            scaleReads: 'master',
            redisOptions: {
              connectionName: `${prefix}-publisher`,
              maxRetriesPerRequest: 2,
              lazyConnect: false,
            },
            clusterRetryStrategy: (attempt) => (attempt <= 5 ? 20 : null),
          },
        )
  if (redis instanceof Cluster) redis.on('error', () => {})
  if (redis instanceof Cluster) {
    // Resolve the complete live topology before selecting the fixture-owned direct Pub/Sub master.
    await redis.ping()
  }
  const subscriber =
    redis instanceof Cluster
      ? (
          redis
            .nodes('master')
            .sort((left, right) =>
              `${left.options.host ?? ''}:${left.options.port ?? ''}`.localeCompare(
                `${right.options.host ?? ''}:${right.options.port ?? ''}`,
              ),
            )[0] as Redis
        ).duplicate({
          autoResubscribe: false,
          connectionName: `${prefix}-subscriber`,
          lazyConnect: false,
          maxRetriesPerRequest: 1,
          retryStrategy: (attempt) => (attempt <= 5 ? 0 : null),
        })
      : new Redis(url, {
          autoResubscribe: false,
          connectionName: `${prefix}-subscriber`,
          lazyConnect: false,
          maxRetriesPerRequest: 1,
          retryStrategy: (attempt) => (attempt <= 5 ? 0 : null),
        })
  subscriber.on('error', () => {})
  await subscriber.ping()
  // Authority time starts ALIGNED with the caller clock and only `advanceAuthority` moves it.
  let clock = Date.now()
  let probe: StableReadProbe | null = null
  let beforeSubscribe: SubscribeProbe | null = null
  let afterSubscribeAck: SubscribeProbe | null = null
  let afterGenerationCapture: GenerationCaptureProbe | null = null
  let beforeDropGenerationUnregister: DropGenerationProbe | null = null
  let bypassCaptureProbe = 0
  const acknowledgedChannels: string[] = []
  const commandCalls: RedisRoomCommandCall[] = []
  const restorers: Array<() => void> = []
  const peerDisposers = new Set<() => Promise<void>>()

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
    client: RoomRedisClient,
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

  const installAuthorityClock = (client: RoomRedisClient): void => {
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
  const duplicatedMasters = redis instanceof Cluster ? redis.nodes('master') : []
  const masterDuplicates = duplicatedMasters.map((master) => [master, master.duplicate.bind(master)] as const)
  if (redis instanceof Cluster) {
    for (const master of duplicatedMasters) master.duplicate = (() => subscriber) as typeof master.duplicate
  } else {
    redis.duplicate = (() => subscriber) as typeof redis.duplicate
  }
  let backend: RedisRoomBackend
  const restoreScheduler = installRedisSubscriptionScheduler(redis, acceleratedSubscriptionScheduler)
  try {
    backend = new RedisRoomBackend({ redis, prefix, maxRetainedPayloadBytes: opts.maxRetainedPayloadBytes })
  } finally {
    restoreScheduler()
    redis.duplicate = duplicate
    for (const [master, original] of masterDuplicates) master.duplicate = original
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

  if (redis instanceof Cluster) {
    type EvalSha = (sha: string, numberOfKeys: number, ...args: unknown[]) => Promise<unknown>
    type Eval = (lua: string, numberOfKeys: number, ...args: unknown[]) => Promise<unknown>
    const evalClient = redis as unknown as { evalsha: EvalSha; eval: Eval }
    const originalEvalSha = evalClient.evalsha.bind(redis)
    const originalEval = evalClient.eval.bind(redis)
    type RedisRoomCommand = (typeof REDIS_ROOM_COMMANDS)[keyof typeof REDIS_ROOM_COMMANDS]
    const commandsByLua = new Map<string, RedisRoomCommand>(
      Object.values(REDIS_ROOM_COMMANDS).map((command) => [command.lua, command]),
    )
    const commandsBySha = new Map(
      Object.values(REDIS_ROOM_COMMANDS).map((command) => [
        createHash('sha1').update(command.lua).digest('hex'),
        command,
      ]),
    )
    evalClient.evalsha = async (sha, numberOfKeys, ...rawArgs) => {
      const command = commandsBySha.get(sha)
      if (command === undefined) return await originalEvalSha(sha, numberOfKeys, ...rawArgs)
      const dynamic = command.numberOfKeys === null
      const args = dynamic ? [String(numberOfKeys), ...rawArgs] : rawArgs
      commandCalls.push({ name: command.name, args: [...args] })

      const now = opts.useRedisAuthority === true ? '' : String(clock)
      if (
        command.name === REDIS_ROOM_COMMANDS.headCx.name ||
        command.name === REDIS_ROOM_COMMANDS.captureGeneration.name ||
        command.name === REDIS_ROOM_COMMANDS.validateGeneration.name ||
        command.name === REDIS_ROOM_COMMANDS.commit.name
      ) {
        args[command.name === REDIS_ROOM_COMMANDS.headCx.name ? 4 : 5] = now
      } else if (command.name === REDIS_ROOM_COMMANDS.cellsCx.name) {
        args[Number(args[0]) + 1] = now
      }

      if (command.name === REDIS_ROOM_COMMANDS.validateGeneration.name) {
        const channel = acknowledgedChannels.shift()
        if (channel !== undefined && afterSubscribeAck !== null) {
          await runProbe('after subscribe acknowledgement', () => afterSubscribeAck?.(channel))
        }
      }
      if (command.name === REDIS_ROOM_COMMANDS.dropGenerationFinalize.name && beforeDropGenerationUnregister !== null) {
        await runProbe('before generation unregister', () =>
          beforeDropGenerationUnregister?.({ roomId: roomIdFromKey(args[0]), inc: String(args[2]) }),
        )
      }

      const actualArgs = dynamic ? args.slice(1) : args
      const reply = await originalEvalSha(sha, numberOfKeys, ...actualArgs)
      if (
        command.name === REDIS_ROOM_COMMANDS.captureGeneration.name &&
        afterGenerationCapture !== null &&
        bypassCaptureProbe === 0
      ) {
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
    }
    restorers.push(() => {
      evalClient.evalsha = originalEvalSha
    })
    evalClient.eval = async (lua, numberOfKeys, ...rawArgs) => {
      const command = commandsByLua.get(lua)
      if (command === undefined) return await originalEval(lua, numberOfKeys, ...rawArgs)
      const dynamic = command.numberOfKeys === null
      const args = dynamic ? [String(numberOfKeys), ...rawArgs] : rawArgs
      commandCalls.push({ name: command.name, args: [...args] })
      const now = opts.useRedisAuthority === true ? '' : String(clock)
      if (
        command.name === REDIS_ROOM_COMMANDS.headCx.name ||
        command.name === REDIS_ROOM_COMMANDS.captureGeneration.name ||
        command.name === REDIS_ROOM_COMMANDS.validateGeneration.name ||
        command.name === REDIS_ROOM_COMMANDS.commit.name
      ) {
        args[command.name === REDIS_ROOM_COMMANDS.headCx.name ? 4 : 5] = now
      } else if (command.name === REDIS_ROOM_COMMANDS.cellsCx.name) {
        args[Number(args[0]) + 1] = now
      }
      if (command.name === REDIS_ROOM_COMMANDS.validateGeneration.name) {
        const channel = acknowledgedChannels.shift()
        if (channel !== undefined && afterSubscribeAck !== null) {
          await runProbe('after subscribe acknowledgement', () => afterSubscribeAck?.(channel))
        }
      }
      if (command.name === REDIS_ROOM_COMMANDS.dropGenerationFinalize.name && beforeDropGenerationUnregister !== null) {
        await runProbe('before generation unregister', () =>
          beforeDropGenerationUnregister?.({ roomId: roomIdFromKey(args[0]), inc: String(args[2]) }),
        )
      }
      const actualArgs = dynamic ? args.slice(1) : args
      const reply = await originalEval(lua, numberOfKeys, ...actualArgs)
      if (
        command.name === REDIS_ROOM_COMMANDS.captureGeneration.name &&
        afterGenerationCapture !== null &&
        bypassCaptureProbe === 0
      ) {
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
    }
    restorers.push(() => {
      evalClient.eval = originalEval
    })
  }

  // Pre-cache the scripts so a head-CX race never pays a one-off NOSCRIPT round-trip that could perturb
  // the FIFO ordering the barrier relies on.
  const scriptClients = redis instanceof Cluster ? redis.nodes('master') : [redis]
  await Promise.all(
    scriptClients.flatMap((client) =>
      Object.values(REDIS_ROOM_COMMANDS).map(async (command) => {
        await client.script('LOAD', command.lua)
      }),
    ),
  )

  const fixture: RedisBackendFixture = {
    backend,
    redis,
    subscriber,
    prefix,
    traces: REDIS_TRACES,
    // Redis PUBLISH counts subscribed connections, not the callbacks multiplexed behind this fixture's
    // one subscriber connection. Keep these exact: two local callbacks still form one broker target,
    // and detaching either sibling leaves that same one connection subscribed for the survivor.
    expectedReceivers: { twoLocalSubscriptionsSameLane: 1, oneLocalSubscriptionAfterSiblingDetach: 1 },
    allowedReceiverCountsAtAuthority: async (roomId, inc, lane, globalFallback) => {
      if (!(redis instanceof Cluster) || globalFallback === 0) return globalFallback
      const masters = redis.nodes('master')
      if (masters.length < 3) throw new Error('Redis fixture: receiver authority requires three live masters')
      const key = channelKey(prefix, roomId, inc, laneKey(lane))
      const slot = Number(await masters[0]?.cluster('KEYSLOT', key))
      const rawSlots = (await redis.cluster('SLOTS')) as unknown as Array<
        [number, number, [string | Buffer, number, ...unknown[]]]
      >
      const range = rawSlots.find(([start, end]) => slot >= start && slot <= end)
      if (range === undefined) throw new Error(`Redis fixture: no owner for slot ${slot}`)
      const ownerHost = Buffer.isBuffer(range[2][0]) ? range[2][0].toString() : range[2][0]
      const ownerPort = Number(range[2][1])
      const subscriberHost = subscriber.options.host ?? '127.0.0.1'
      const subscriberPort = Number(subscriber.options.port)
      const sameHost = ownerHost === subscriberHost || new Set([ownerHost, subscriberHost]).size === 1
      return sameHost && ownerPort === subscriberPort ? globalFallback : 0
    },
    authorityNow: () => clock,
    advanceAuthority: (ms) => {
      clock += ms
    },
    orderControl: {
      setAuthority: (now) => {
        clock = now
      },
      runMaintenance: async (roomId) => {
        await fixture.backend.listGenerations(roomId)
      },
      reconstructBackend: async () => {
        const peer = await fixture.createPeerBackend()
        fixture.backend = peer.backend
      },
      seedWatermark: async (roomId, inc, lane, seq, timestamp) => {
        await redis.set(orderKey(prefix, roomId, inc, laneKey(lane)), `${seq}:${timestamp}`)
      },
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
    pubSubClientIdsForTest: async () => await pubSubClientIds(redis),
    pubSubSubscriberCountForTest: async (channel) => await pubSubSubscriberCount(redis, channel),
    killSubscriberForTest: async (id) => {
      const nodes = redis instanceof Cluster ? redis.nodes('master') : [redis]
      let target: number
      let owner: Redis
      if (id === undefined) {
        const current = await currentSubscriberTarget(subscriber, redis)
        target = current.id
        owner = nodes.find(
          (node) => node.options.host === current.host && Number(node.options.port) === Number(current.port),
        ) as Redis
        if (owner === undefined) {
          throw new Error(`Redis fixture: subscriber owner ${current.host}:${current.port} is unavailable`)
        }
      } else {
        target = id
        const owners = await pubSubClientOwners(nodes, target)
        if (owners.length !== 1) {
          throw new Error(`Redis fixture: expected one owner for Pub/Sub client ${target}, found ${owners.length}`)
        }
        owner = owners[0] as Redis
      }
      const admin = owner as unknown as { client(command: string, ...args: string[]): Promise<unknown> }
      if (Number(await admin.client('KILL', 'ID', String(target))) !== 1) {
        throw new Error(`Redis fixture: subscriber client ${target} was not killed on its exact owner`)
      }
    },
    createPeerBackend: async () => {
      const peerRedis =
        redis instanceof Cluster
          ? redis.duplicate(undefined, { redisOptions: { maxRetriesPerRequest: 2 } })
          : redis.duplicate({ maxRetriesPerRequest: 2 })
      if (peerRedis instanceof Cluster) {
        peerRedis.on('error', () => {})
        await peerRedis.ping()
      }
      const restorePeerScheduler = installRedisSubscriptionScheduler(peerRedis, acceleratedSubscriptionScheduler)
      let peer: RedisRoomBackend
      try {
        peer = new RedisRoomBackend({ redis: peerRedis, prefix })
      } finally {
        restorePeerScheduler()
      }
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
  return fixture
}

export function makeRedisHarness(url: string): BackendHarness {
  return { name: 'redis', create: () => createRedisFixture(url) }
}

export function makeRedisClusterHarness(url: string, clusterNodes: RedisClusterNode[]): BackendHarness {
  return { name: 'redis-cluster', create: () => createRedisFixture(url, { clusterNodes }) }
}

async function scanPrefix(redis: RoomRedisClient, prefix: string): Promise<string[]> {
  const nodes = redis instanceof Cluster ? redis.nodes('master') : [redis]
  const keys = new Set<string>()
  for (const node of nodes) {
    let cursor = '0'
    do {
      const [next, page] = await node.scan(cursor, 'MATCH', `${prefix}*`, 'COUNT', 500)
      cursor = next
      for (const key of page) keys.add(key)
    } while (cursor !== '0')
  }
  return [...keys]
}

async function currentSubscriberTarget(
  subscriber: Redis,
  publisher: RoomRedisClient,
): Promise<{ id: number; host: string; port: number }> {
  // Redis 6 permits only Pub/Sub commands on a subscribed socket, so resolve the exact client id from
  // its fixture-unique name using an ordinary admin connection to the same owner.
  const { host, port, connectionName } = subscriber.options
  const nodes = publisher instanceof Cluster ? publisher.nodes('master') : [publisher]
  const owner = nodes.find((node) => node.options.host === host && Number(node.options.port) === Number(port))
  if (owner === undefined) throw new Error(`Redis fixture: subscriber owner ${host}:${port} is unavailable`)
  const admin = owner as unknown as { client(command: string, ...args: string[]): Promise<unknown> }
  const list = String(await admin.client('LIST', 'TYPE', 'pubsub'))
  const matches = list
    .split('\n')
    .filter((line) => line.includes(`name=${connectionName ?? ''} `))
    .map((line) => line.match(/(?:^|\s)id=(\d+)(?:\s|$)/)?.[1])
    .filter((id): id is string => id !== undefined)
  if (matches.length !== 1) {
    throw new Error(`Redis fixture: expected one named subscriber on ${host}:${port}, found ${matches.length}`)
  }
  return { id: Number(matches[0]), host: host ?? '127.0.0.1', port: Number(port) }
}

async function pubSubClientOwners(nodes: Redis[], id: number): Promise<Redis[]> {
  const owners: Redis[] = []
  for (const node of nodes) {
    const admin = node as unknown as { client(command: string, ...args: string[]): Promise<unknown> }
    const list = String(await admin.client('LIST', 'TYPE', 'pubsub'))
    if (list.split('\n').some((line) => line.match(/(?:^|\s)id=(\d+)(?:\s|$)/)?.[1] === String(id))) {
      owners.push(node)
    }
  }
  return owners
}

async function pubSubClientIds(redis: RoomRedisClient): Promise<number[]> {
  const nodes = redis instanceof Cluster ? redis.nodes('master') : [redis]
  const ids: number[] = []
  for (const node of nodes) {
    const admin = node as unknown as { client(command: string, ...args: string[]): Promise<unknown> }
    const list = String(await admin.client('LIST', 'TYPE', 'pubsub'))
    for (const line of list.split('\n')) {
      const match = line.match(/(?:^|\s)id=(\d+)(?:\s|$)/)
      if (match !== null) ids.push(Number(match[1]))
    }
  }
  return ids
}

async function pubSubSubscriberCount(redis: RoomRedisClient, channel: string): Promise<number> {
  const nodes = redis instanceof Cluster ? redis.nodes('master') : [redis]
  const counts = await Promise.all(nodes.map(async (node) => Number((await node.pubsub('NUMSUB', channel))[1])))
  return counts.reduce((total, count) => total + count, 0)
}
