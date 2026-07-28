// Test-only real-Redis fixture for the compact Redis realization/certification spec. Its controlled
// authority clock starts aligned with Date.now() and moves only when a named Redis scenario requests it.

import { createHash } from 'node:crypto'
import { Cluster, Redis } from 'ioredis'
import type { BackendSpi } from '../../../telefunc/wire-protocol/backend/spi.js'
import { superviseBackend } from '../../../telefunc/wire-protocol/backend/supervised-backend.js'
import { REDIS_ROOM_COMMANDS } from './layout.js'
import { RedisRoomBackend } from './backend.js'

let fixtureSeq = 0

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

export type RedisRoomCommandCall = { name: string; args: readonly unknown[] }

export type RedisBackendFixture = {
  backend: BackendSpi
  advanceAuthority(ms: number): void
  orderControl: {
    setAuthority(now: number): void
    reconstructBackend(): Promise<void>
  }
  commandCalls(): readonly RedisRoomCommandCall[]
  createPeerBackend(): Promise<{ backend: BackendSpi; dispose(): Promise<void> }>
  dispose(): Promise<void>
}

export async function createRedisFixture(
  url: string,
  opts: { clusterNodes?: RedisClusterNode[] } = {},
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
  // Authority time starts ALIGNED with the caller clock and only `advanceAuthority` moves it.
  let clock = Date.now()
  const commandCalls: RedisRoomCommandCall[] = []
  const restorers: Array<() => void> = []
  const peerDisposers = new Set<() => Promise<void>>()

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
    const now = (): string => String(clock)
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

  const backend = superviseBackend(new RedisRoomBackend({ redis, prefix }))
  installAuthorityClock(redis)
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

      const now = String(clock)
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

      const actualArgs = dynamic ? args.slice(1) : args
      return await originalEvalSha(sha, numberOfKeys, ...actualArgs)
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
      const now = String(clock)
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
      const actualArgs = dynamic ? args.slice(1) : args
      return await originalEval(lua, numberOfKeys, ...actualArgs)
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
    advanceAuthority: (ms) => {
      clock += ms
    },
    orderControl: {
      setAuthority: (now) => {
        clock = now
      },
      reconstructBackend: async () => {
        const peer = await fixture.createPeerBackend()
        fixture.backend = peer.backend
      },
    },
    commandCalls: () => commandCalls,
    createPeerBackend: async () => {
      const peerRedis =
        redis instanceof Cluster
          ? redis.duplicate(undefined, { redisOptions: { maxRetriesPerRequest: 2 } })
          : redis.duplicate({ maxRetriesPerRequest: 2 })
      if (peerRedis instanceof Cluster) {
        peerRedis.on('error', () => {})
        await peerRedis.ping()
      }
      const peer = superviseBackend(new RedisRoomBackend({ redis: peerRedis, prefix }))
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
      commandCalls.length = 0
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
