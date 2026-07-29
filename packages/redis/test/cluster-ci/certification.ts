import { createHash } from 'node:crypto'
import { Cluster, Redis } from 'ioredis'
import type { BackendSpi, CommitAccepted, LaneId, RoomHead, SubscriptionState } from 'telefunc/backend'
import { superviseBackend } from '../../../telefunc/wire-protocol/backend/supervised-backend.js'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { RedisRoomBackend } from '../../src/index.js'
import {
  DIRECTORY_PUT_LUA,
  directoryIndexKey,
  headKey,
  laneKey,
  orderKey,
  REDIS_ROOM_COMMANDS,
} from '../../src/room/layout.js'

type RedisClusterNode = { host: string; port: number }
type Master = RedisClusterNode & { id: string; start: number; end: number; client: Redis }
type CommandCall = { name: string; keyCount: number; args: unknown[] }
type CommandDefinition = { name: string; lua: string; numberOfKeys: number | null }

const rawNodes = process.env.REDIS_CLUSTER_NODES
if (!rawNodes) throw new Error('Redis Cluster CI certification requires REDIS_CLUSTER_NODES')
const CLUSTER_NODES = rawNodes.split(',').map((entry): RedisClusterNode => {
  const separator = entry.lastIndexOf(':')
  const host = entry.slice(0, separator)
  const port = Number(entry.slice(separator + 1))
  if (!host || !Number.isInteger(port) || port <= 0 || port > 65_535) {
    throw new Error(`Invalid REDIS_CLUSTER_NODES entry: ${entry}`)
  }
  return { host, port }
})
if (CLUSTER_NODES.length !== 3) throw new Error('Redis Cluster CI certification requires exactly three masters')

const SEMANTIC_LANE: LaneId = { kind: 'semantic' }

describe('Redis real three-master Cluster CI certification', () => {
  let cluster: Cluster
  let masters: Master[]

  beforeAll(async () => {
    cluster = clusterClient(CLUSTER_NODES)
    await cluster.ping()
    masters = await readMasters(CLUSTER_NODES)
  })

  afterAll(async () => {
    await Promise.allSettled((masters ?? []).map(({ client }) => client.quit()))
    if (cluster !== undefined) await cluster.quit().catch(() => cluster.disconnect())
  })

  it('covers all slots and every shipped command with complete co-slotted KEYS declarations', async () => {
    expect(masters).toHaveLength(3)
    expect(masters.reduce((total, master) => total + master.end - master.start + 1, 0)).toBe(16_384)
    for (const master of masters) expect((await clusterInfo(master.client)).cluster_state).toBe('ok')

    const prefix = uniquePrefix('runtime-slots')
    const client = clusterClient(CLUSTER_NODES)
    const observation = observeCommands(client)
    const backend = redisBackend(client, prefix)
    const genericCalls = observation.wrapDefinedCommand('tfPublish')
    const roomId = 'runtime-slot} proof'
    const inc = 'runtime-slot-inc'
    let subscription: ReturnType<BackendSpi['subscribeLane']> | undefined
    try {
      expect(backend.capabilities).toMatchObject({ clusterSafe: true, receivers: 'node-local' })
      const head = await open(backend, roomId, inc)
      const cells = await backend.readCells(roomId, inc, { keys: [] })
      if ('staleInc' in cells) throw new Error('fresh generation was unexpectedly stale')
      expect(
        await backend.compareExchangeCells(roomId, inc, cells.revision, [
          { key: 'cell} escape', set: { bytes: bytes('value') } },
        ]),
      ).toBe('committed')
      subscription = backend.subscribeLane(roomId, inc, SEMANTIC_LANE, () => {})
      await subscription.ready
      await accepted(
        await backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('payload'), {
          retain: true,
          requiredCellKeys: ['cell} escape'],
        }),
      ).delivery
      const currentCells = await backend.readCells(roomId, inc, { keys: ['cell} escape'] })
      if ('staleInc' in currentCells) throw new Error('cell fence generation vanished')
      expect(await backend.compareExchangeCells(roomId, inc, currentCells.revision, [{ key: 'cell} escape' }])).toBe(
        'committed',
      )
      expect(
        await backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('fenced'), {
          requiredCellKeys: ['cell} escape'],
        }),
      ).toEqual({ stale: true })
      await backend.deleteRetained(roomId, inc, SEMANTIC_LANE)
      await backend.directoryPut(roomId, inc)
      await backend.directoryDelete(roomId, inc)
      await backend.publish({ key: 'generic} escape', kind: 'binary' }, bytes('generic'))
      await subscription.unsubscribe()
      subscription = undefined
      const closed = await close(backend, roomId, head)
      expect(closed.state).toBe('closed')
      await backend.dropGeneration(roomId, inc)

      const expectedNames = new Set(['tfPublish', ...Object.values(REDIS_ROOM_COMMANDS).map(({ name }) => name)])
      expect(new Set(observation.definitions.map(({ name }) => name))).toEqual(expectedNames)
      for (const definition of observation.definitions) {
        if (definition.numberOfKeys === null) continue
        const referenced = [...definition.lua.matchAll(/\bKEYS\[(\d+)\]/g)].map((match) => Number(match[1]))
        expect(new Set(referenced), `${definition.name}: Lua KEYS references`).toEqual(
          new Set(Array.from({ length: definition.numberOfKeys }, (_, index) => index + 1)),
        )
      }

      expect(genericCalls).toHaveLength(1)
      const genericKeys = genericCalls[0]?.slice(0, 2).map(String) ?? []
      expect(new Set(genericKeys.map(redisSlot)), `tfPublish: ${genericKeys.join(', ')}`).toEqual(
        new Set([redisSlot(genericKeys[0] as string)]),
      )
      for (const descriptor of Object.values(REDIS_ROOM_COMMANDS)) {
        const calls = observation.calls.filter(({ name }) => name === descriptor.name)
        expect(calls.length, descriptor.name).toBeGreaterThan(0)
        for (const call of calls) {
          const keys = call.args.slice(0, call.keyCount).map(String)
          expect(new Set(keys.map(redisSlot)), `${descriptor.name}: ${keys.join(', ')}`).toEqual(
            new Set([redisSlot(keys[0] as string)]),
          )
        }
      }
    } finally {
      await subscription?.unsubscribe().catch(() => {})
      observation.restore()
      await backend.dispose()
      await client.quit().catch(() => client.disconnect())
    }
  })

  it('observes genuine CROSSSLOT and undeclared-key failures', async () => {
    const prefix = uniquePrefix('crossslot')
    const directory = directoryIndexKey(prefix)
    const other = await keyOnDifferentMaster(directory, prefix)
    await expect(cluster.eval(DIRECTORY_PUT_LUA, 2, directory, other, 'room', 'inc')).rejects.toThrow(/CROSSSLOT/)
    await cluster.set(directory, 'declared')
    await cluster.set(other, 'argv')
    await expect(cluster.eval("return redis.call('GET', ARGV[1])", 1, directory, other)).rejects.toThrow(
      /non local key|CROSSSLOT/i,
    )
  })

  it('round-trips MAX_SAFE seq through commit, retain, fresh read, then rejects before effects', async () => {
    const prefix = uniquePrefix('max-safe')
    const roomId = 'max-safe-room'
    const inc = 'max-safe-inc'
    const backend = redisBackend(cluster, prefix)
    const fresh = redisBackend(cluster, prefix)
    const observed: number[] = []
    let subscription: ReturnType<BackendSpi['subscribeLane']> | undefined
    try {
      await open(backend, roomId, inc)
      subscription = backend.subscribeLane(roomId, inc, SEMANTIC_LANE, (_payload, info) => observed.push(info.seq))
      await subscription.ready
      await cluster.set(orderKey(prefix, roomId, inc, laneKey(SEMANTIC_LANE)), `${Number.MAX_SAFE_INTEGER - 1}:1`)
      const last = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from('last'), { retain: true }))
      await last.delivery
      await waitFor(() => observed.length === 1)
      expect(last.seq).toBe(Number.MAX_SAFE_INTEGER)
      expect(await fresh.readRetained(roomId, inc, SEMANTIC_LANE)).toMatchObject({ seq: Number.MAX_SAFE_INTEGER })
      await expect(
        backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from('overflow'), { retain: true }),
      ).rejects.toThrow('sequence exhausted')
      expect(observed).toEqual([Number.MAX_SAFE_INTEGER])
      const retained = await fresh.readRetained(roomId, inc, SEMANTIC_LANE)
      expect(retained?.seq).toBe(Number.MAX_SAFE_INTEGER)
      expect([...new Uint8Array(retained?.payload ?? [])]).toEqual([...Buffer.from('last')])
    } finally {
      await subscription?.unsubscribe()
      await Promise.all([backend.dispose(), fresh.dispose()])
    }
  })

  it('recovers lost -> failing first replacement -> successful second -> delivery', async () => {
    const topology = cluster.nodes('master').sort(compareRedisNodes)
    const originals = topology.map((node) => [node, node.duplicate.bind(node)] as const)
    let failNext = false
    let opens = 0
    for (const [node, original] of originals) {
      node.duplicate = ((options?: unknown) => {
        const client = original(options as never)
        opens++
        if (failNext) {
          failNext = false
          const subscribe = client.subscribe.bind(client)
          let failed = false
          client.subscribe = (async (...channels: string[]) => {
            if (!failed) {
              failed = true
              throw new Error('synthetic first replacement failure')
            }
            return await subscribe(...channels)
          }) as typeof client.subscribe
        }
        return client
      }) as typeof node.duplicate
    }

    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const baseline = new Set((await pubSubClients()).map(({ id }) => id))
    const backend = redisBackend(cluster, uniquePrefix('replacement'))
    const states: SubscriptionState[] = []
    const observed: string[] = []
    let subscription: ReturnType<BackendSpi['subscribeLane']> | undefined
    try {
      await open(backend, 'replacement-room', 'replacement-inc')
      subscription = backend.subscribeLane('replacement-room', 'replacement-inc', SEMANTIC_LANE, (payload) =>
        observed.push(Buffer.from(payload).toString()),
      )
      subscription.onStateChange((state) => states.push(state))
      await subscription.ready
      const [first] = await waitForValue(async () => (await pubSubClients()).filter(({ id }) => !baseline.has(id)))
      if (first === undefined) throw new Error('initial Redis subscriber was not observed')
      failNext = true
      await first.owner.client.call('CLIENT', 'KILL', 'ID', String(first.id))
      await waitFor(() => states.includes('lost') && subscription?.state() === 'ready' && opens >= 3, 15_000)
      const committed = accepted(
        await backend.commitLane('replacement-room', 'replacement-inc', SEMANTIC_LANE, Buffer.from('recovered')),
      )
      await committed.delivery
      await waitFor(() => observed.length === 1)
      expect(states).toContain('lost')
      expect(states.at(-1)).toBe('ready')
      expect(observed).toEqual(['recovered'])
      expect(report.mock.calls.some(([error]) => String(error).includes('synthetic first replacement failure'))).toBe(
        true,
      )
    } finally {
      for (const [node, original] of originals) node.duplicate = original as typeof node.duplicate
      report.mockRestore()
      await subscription?.unsubscribe().catch(() => {})
      await backend.dispose()
    }
  })

  it('rejects a held PONG from the previous subscriber connection instead of crediting stale delivery', async () => {
    const topology = cluster.nodes('master').sort(compareRedisNodes)
    const originals = topology.map((node) => [node, node.duplicate.bind(node)] as const)
    const subscribers: Redis[] = []
    for (const [node, original] of originals) {
      node.duplicate = ((options?: unknown) => {
        const client = original(options as never)
        subscribers.push(client)
        return client
      }) as typeof node.duplicate
    }

    const baseline = new Set((await pubSubClients()).map(({ id }) => id))
    const backend = redisBackend(cluster, uniquePrefix('stale-pong'))
    const states: SubscriptionState[] = []
    const observed: string[] = []
    const oldPongHeld = deferred()
    const releaseOldPong = deferred()
    let subscription: ReturnType<BackendSpi['subscribeLane']> | undefined
    let restorePing: (() => void) | undefined
    try {
      await open(backend, 'stale-pong-room', 'stale-pong-inc')
      subscription = backend.subscribeLane('stale-pong-room', 'stale-pong-inc', SEMANTIC_LANE, (payload) =>
        observed.push(Buffer.from(payload).toString()),
      )
      subscription.onStateChange((state) => states.push(state))
      await subscription.ready

      const subscriber = subscribers[0]
      if (subscriber === undefined) throw new Error('owned Redis subscriber was not observed')
      const ping = subscriber.ping.bind(subscriber)
      let holdOnce = true
      subscriber.ping = (async () => {
        const pong = await ping()
        if (holdOnce) {
          holdOnce = false
          oldPongHeld.resolve()
          await releaseOldPong.promise
        }
        return pong
      }) as typeof subscriber.ping
      restorePing = () => {
        subscriber.ping = ping
      }

      const committed = accepted(
        await backend.commitLane('stale-pong-room', 'stale-pong-inc', SEMANTIC_LANE, Buffer.from('old-epoch')),
      )
      const deliveryOutcome = committed.delivery.then(
        () => null,
        (error: unknown) => error,
      )
      await oldPongHeld.promise
      await waitFor(() => observed.length === 1)

      const [first] = await waitForValue(async () => (await pubSubClients()).filter(({ id }) => !baseline.has(id)))
      if (first === undefined) throw new Error('initial Redis subscriber connection was not observed')
      await first.owner.client.call('CLIENT', 'KILL', 'ID', String(first.id))
      await waitFor(() => states.includes('lost') && subscription?.state() === 'ready')
      releaseOldPong.resolve()

      const error = await deliveryOutcome
      expect(error).toBeInstanceOf(Error)
      expect(String(error)).toMatch(/PING crossed source/)
      expect(observed).toEqual(['old-epoch'])
    } finally {
      releaseOldPong.resolve()
      restorePing?.()
      for (const [node, original] of originals) node.duplicate = original as typeof node.duplicate
      await subscription?.unsubscribe().catch(() => {})
      await backend.dispose()
    }
  })

  it('reports exact node-local receiver counts while still delivering cross-node', async () => {
    const prefix = uniquePrefix('receivers')
    const backend = redisBackend(cluster, prefix)
    // RedisRoomBackend sorts master endpoints before round-robin subscriber selection. Mirror that
    // order, but identify masters by Cluster node ID: Compose nodes all listen on port 6379.
    const subscriberOrder = [...masters].sort(compareMasterEndpoints)
    const cases = [
      { label: 'same', roomMasterId: (subscriberOrder[0] as Master).id, expected: 1 },
      { label: 'cross', roomMasterId: (subscriberOrder[2] as Master).id, expected: 0 },
    ]
    const subscriptions: Array<ReturnType<BackendSpi['subscribeLane']>> = []
    try {
      for (const scenario of cases) {
        const roomId = await roomOnMaster(prefix, scenario.roomMasterId, scenario.label)
        const inc = `${scenario.label}-inc`
        await open(backend, roomId, inc)
        const observed: string[] = []
        const subscription = backend.subscribeLane(roomId, inc, SEMANTIC_LANE, (payload) =>
          observed.push(Buffer.from(payload).toString()),
        )
        subscriptions.push(subscription)
        await subscription.ready
        const result = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from(scenario.label)))
        expect(result.receivers).toBe(scenario.expected)
        await result.delivery
        await waitFor(() => observed.length === 1)
      }
    } finally {
      await Promise.allSettled(subscriptions.map((subscription) => subscription.unsubscribe()))
      await backend.dispose()
    }
  })

  it('tries EVALSHA first and recovers a genuine NOSCRIPT after relocation', async () => {
    const prefix = uniquePrefix('noscript')
    const roomId = 'noscript-room'
    const inc = 'noscript-inc'
    const client = clusterClient(CLUSTER_NODES)
    const backend = redisBackend(client, prefix)
    const calls: Array<'evalsha' | 'eval'> = []
    const evaluator = client as unknown as {
      evalsha(sha: string, numberOfKeys: number, ...args: unknown[]): Promise<unknown>
      eval(lua: string, numberOfKeys: number, ...args: unknown[]): Promise<unknown>
    }
    const evalsha = evaluator.evalsha.bind(client)
    const evalLua = evaluator.eval.bind(client)
    evaluator.evalsha = async (...args) => {
      calls.push('evalsha')
      return await evalsha(...args)
    }
    evaluator.eval = async (...args) => {
      calls.push('eval')
      return await evalLua(...args)
    }
    let moved: { slot: number; source: Master; target: Master } | undefined
    try {
      await open(backend, roomId, inc)
      accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from('prime')))
      const slotNumber = await slot(headKey(prefix, roomId))
      const source = owner(slotNumber)
      const target = otherMaster(source)
      moved = { slot: slotNumber, source, target }
      await target.client.script('FLUSH')
      await moveSlot(slotNumber, source, target, true)
      calls.length = 0
      expect(accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from('fallback'))).seq).toBe(2)
      expect(calls).toEqual(['evalsha', 'eval'])
    } finally {
      if (moved !== undefined) await restoreSlot(moved.slot, moved.source, moved.target)
      evaluator.evalsha = evalsha
      evaluator.eval = evalLua
      await backend.dispose()
      await client.quit().catch(() => client.disconnect())
    }
  })

  it('follows a real MOVED while a no-redirection control fails', async () => {
    const prefix = uniquePrefix('moved')
    const roomId = 'moved-room'
    const inc = 'moved-inc'
    const backend = redisBackend(cluster, prefix)
    const controlClient = clusterClient(CLUSTER_NODES, 0)
    const control = new RedisRoomBackend({ redis: controlClient, prefix })
    let moved: { slot: number; source: Master; target: Master } | undefined
    try {
      await open(backend, roomId, inc)
      await control.readHead(roomId)
      const slotNumber = await slot(headKey(prefix, roomId))
      const source = owner(slotNumber)
      const target = otherMaster(source)
      moved = { slot: slotNumber, source, target }
      await moveSlot(slotNumber, source, target, true)
      await expect(source.client.get(headKey(prefix, roomId))).rejects.toThrow(/MOVED/)
      await expect(control.readHead(roomId)).rejects.toThrow(/redirection|MOVED/i)
      expect((await backend.readHead(roomId))?.head.currentInc).toBe(inc)
    } finally {
      if (moved !== undefined) await restoreSlot(moved.slot, moved.source, moved.target)
      await Promise.allSettled([backend.dispose(), control.dispose(), controlClient.quit()])
    }
  })

  it('follows a real ASK while a no-ASKING control fails', async () => {
    const prefix = uniquePrefix('ask')
    const roomId = 'ask-room'
    const inc = 'ask-inc'
    const client = clusterClient(CLUSTER_NODES)
    const backend = redisBackend(client, prefix)
    const controlClient = clusterClient(CLUSTER_NODES, 0)
    const control = new RedisRoomBackend({ redis: controlClient, prefix })
    let migration: { slot: number; source: Master; target: Master; finalized: boolean } | undefined
    try {
      await open(backend, roomId, inc)
      await control.readHead(roomId)
      const slotNumber = await slot(headKey(prefix, roomId))
      const source = owner(slotNumber)
      const target = otherMaster(source)
      migration = { slot: slotNumber, source, target, finalized: false }
      await target.client.cluster('SETSLOT', slotNumber, 'IMPORTING', source.id)
      await source.client.cluster('SETSLOT', slotNumber, 'MIGRATING', target.id)
      const keys = (await source.client.cluster('GETKEYSINSLOT', slotNumber, 10_000)) as string[]
      await migrateKeys(source, target, keys)
      await expect(source.client.get(headKey(prefix, roomId))).rejects.toThrow(/ASK/)
      await expect(control.readHead(roomId)).rejects.toThrow(/redirection|ASK/i)
      expect((await backend.readHead(roomId))?.head.currentInc).toBe(inc)
      for (const master of masters) await master.client.cluster('SETSLOT', slotNumber, 'NODE', target.id)
      migration.finalized = true
    } finally {
      if (migration !== undefined) {
        if (migration.finalized) await restoreSlot(migration.slot, migration.source, migration.target)
        else await restorePartialSlot(migration.slot, migration.source, migration.target)
      }
      await Promise.allSettled([backend.dispose(), control.dispose(), client.quit(), controlClient.quit()])
    }
  })

  function owner(slotNumber: number): Master {
    const match = masters.find(({ start, end }) => slotNumber >= start && slotNumber <= end)
    if (match === undefined) throw new Error(`slot ${slotNumber} has no owner`)
    return match
  }

  function otherMaster(source: Master): Master {
    const target = masters.find(({ id }) => id !== source.id)
    if (target === undefined) throw new Error(`no relocation target exists for Cluster node '${source.id}'`)
    return target
  }

  async function slot(key: string): Promise<number> {
    return Number(await masters[0]?.client.cluster('KEYSLOT', key))
  }

  async function keyOnDifferentMaster(key: string, prefix: string): Promise<string> {
    const firstMasterId = owner(await slot(key)).id
    for (let index = 0; index < 50_000; index++) {
      const candidate = headKey(prefix, `cross-${index}`)
      if (owner(await slot(candidate)).id !== firstMasterId) return candidate
    }
    throw new Error('failed to find a key on another master')
  }

  async function roomOnMaster(prefix: string, masterId: string, label: string): Promise<string> {
    for (let index = 0; index < 50_000; index++) {
      const roomId = `${label}-${index}`
      if (owner(await slot(headKey(prefix, roomId))).id === masterId) return roomId
    }
    throw new Error(`failed to find a room on master '${masterId}'`)
  }

  async function pubSubClients(): Promise<Array<{ id: number; owner: Master }>> {
    const clients: Array<{ id: number; owner: Master }> = []
    for (const master of masters) {
      const list = String(await master.client.call('CLIENT', 'LIST', 'TYPE', 'PUBSUB'))
      for (const line of list.split('\n')) {
        if (!line.includes('name=telefunc-subscriber-')) continue
        const id = line.match(/(?:^|\s)id=(\d+)(?:\s|$)/)?.[1]
        if (id !== undefined) clients.push({ id: Number(id), owner: master })
      }
    }
    return clients
  }

  async function moveSlot(slotNumber: number, source: Master, target: Master, finalize: boolean): Promise<void> {
    await target.client.cluster('SETSLOT', slotNumber, 'IMPORTING', source.id)
    await source.client.cluster('SETSLOT', slotNumber, 'MIGRATING', target.id)
    const keys = (await source.client.cluster('GETKEYSINSLOT', slotNumber, 10_000)) as string[]
    await migrateKeys(source, target, keys)
    if (finalize) for (const master of masters) await master.client.cluster('SETSLOT', slotNumber, 'NODE', target.id)
  }

  async function migrateKeys(source: Master, target: Master, keys: string[]): Promise<void> {
    for (let index = 0; index < keys.length; index += 50) {
      const batch = keys.slice(index, index + 50)
      if (batch.length > 0) {
        await source.client.call('MIGRATE', target.host, String(target.port), '', '0', '30000', 'KEYS', ...batch)
      }
    }
  }

  async function restoreSlot(slotNumber: number, original: Master, current: Master): Promise<void> {
    if (original.id === current.id) return
    await original.client.cluster('SETSLOT', slotNumber, 'IMPORTING', current.id)
    await current.client.cluster('SETSLOT', slotNumber, 'MIGRATING', original.id)
    const keys = (await current.client.cluster('GETKEYSINSLOT', slotNumber, 10_000)) as string[]
    await migrateKeys(current, original, keys)
    for (const master of masters) await master.client.cluster('SETSLOT', slotNumber, 'NODE', original.id)
    await assertClusterOk()
  }

  async function restorePartialSlot(slotNumber: number, original: Master, partial: Master): Promise<void> {
    const migrated = (await partial.client.cluster('GETKEYSINSLOT', slotNumber, 10_000)) as string[]
    if (migrated.length === 0) {
      await original.client.cluster('SETSLOT', slotNumber, 'STABLE')
      await partial.client.cluster('SETSLOT', slotNumber, 'STABLE')
      for (const master of masters) await master.client.cluster('SETSLOT', slotNumber, 'NODE', original.id)
      await assertClusterOk()
      return
    }
    for (const master of masters) await master.client.cluster('SETSLOT', slotNumber, 'NODE', partial.id)
    await restoreSlot(slotNumber, original, partial)
  }

  async function assertClusterOk(): Promise<void> {
    for (const master of masters) expect((await clusterInfo(master.client)).cluster_state).toBe('ok')
  }
})

function clusterClient(nodes: RedisClusterNode[], maxRedirections = 16): Cluster {
  const client = new Cluster(nodes, {
    scaleReads: 'master',
    maxRedirections,
    slotsRefreshTimeout: 2_000,
    redisOptions: { maxRetriesPerRequest: 2 },
    clusterRetryStrategy: (attempt) => (attempt <= 5 ? 20 : null),
  })
  client.on('error', () => {})
  return client
}

async function readMasters(nodes: RedisClusterNode[]): Promise<Master[]> {
  const seed = new Redis(nodes[0] as RedisClusterNode)
  const raw = (await seed.cluster('SLOTS')) as unknown as Array<[number, number, [string, number, string]]>
  await seed.quit()
  return raw.map(([start, end, [host, port, id]]) => ({
    start,
    end,
    host,
    port,
    id,
    client: new Redis({ host, port, maxRetriesPerRequest: 2 }),
  }))
}

function redisBackend(redis: Redis | Cluster, prefix: string): BackendSpi {
  return superviseBackend(new RedisRoomBackend({ redis, prefix }))
}

async function open(backend: BackendSpi, roomId: string, inc: string): Promise<RoomHead> {
  const result = await backend.compareExchangeHead(
    roomId,
    { expect: 'absent' },
    { head: { currentInc: inc, state: 'open', config: bytes('redis-cluster-ci') } },
  )
  if (!('ok' in result) || !('head' in result)) throw new Error(`failed to open '${roomId}'`)
  return result.head
}

async function close(backend: BackendSpi, roomId: string, head: RoomHead): Promise<RoomHead> {
  const leaseId = `lease-${Date.now().toString(36)}`
  const closing = await backend.compareExchangeHead(
    roomId,
    { expect: { rev: head.rev } },
    {
      head: {
        currentInc: head.currentInc,
        state: 'closing',
        config: head.config,
        closeLease: { id: leaseId, durationMs: 1_000 },
      },
    },
  )
  if (!('ok' in closing) || !('head' in closing)) throw new Error(`failed to enter closing for '${roomId}'`)
  const closed = await backend.compareExchangeHead(
    roomId,
    { expect: { rev: closing.head.rev, closingLease: leaseId } },
    { head: { currentInc: null, state: 'closed', config: closing.head.config }, ttlMs: 60_000 },
  )
  if (!('ok' in closed) || !('head' in closed)) throw new Error(`failed to finalize close for '${roomId}'`)
  return closed.head
}

function accepted(result: Awaited<ReturnType<BackendSpi['commitLane']>>): CommitAccepted {
  if (!('accepted' in result)) throw new Error('commit was unexpectedly stale')
  return result
}

function uniquePrefix(label: string): string {
  return `redis-cluster-ci:${process.pid}:${Date.now().toString(36)}:${label}:`
}

function compareRedisNodes(left: Redis, right: Redis): number {
  return `${left.options.host ?? ''}:${left.options.port ?? ''}`.localeCompare(
    `${right.options.host ?? ''}:${right.options.port ?? ''}`,
  )
}

function compareMasterEndpoints(left: RedisClusterNode, right: RedisClusterNode): number {
  return `${left.host}:${left.port}`.localeCompare(`${right.host}:${right.port}`)
}

function redisSlot(key: string): number {
  const start = key.indexOf('{')
  const end = start < 0 ? -1 : key.indexOf('}', start + 1)
  const tagged = start >= 0 && end > start + 1 ? key.slice(start + 1, end) : key
  let crc = 0
  for (const byte of new TextEncoder().encode(tagged)) {
    crc ^= byte << 8
    for (let bit = 0; bit < 8; bit++) crc = (crc & 0x8000) === 0 ? crc << 1 : (crc << 1) ^ 0x1021
  }
  return crc & 0x3fff
}

async function clusterInfo(client: Redis): Promise<Record<string, string>> {
  return Object.fromEntries(
    String(await client.cluster('INFO'))
      .trim()
      .split('\n')
      .map((line) => line.trim().split(':', 2) as [string, string]),
  )
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error(`condition did not settle within ${timeoutMs} ms`)
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

async function waitForValue<T>(read: () => Promise<T[]>, timeoutMs = 10_000): Promise<T[]> {
  let value: T[] = []
  await waitFor(async () => {
    value = await read()
    return value.length > 0
  }, timeoutMs)
  return value
}

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value)
}

function observeCommands(client: Cluster): {
  definitions: CommandDefinition[]
  calls: CommandCall[]
  wrapDefinedCommand(name: string): unknown[][]
  restore(): void
} {
  const definitions: CommandDefinition[] = []
  const calls: CommandCall[] = []
  const restores: Array<() => void> = []
  const defineCommand = client.defineCommand.bind(client)
  client.defineCommand = ((name: string, options: { lua: string; numberOfKeys?: number }) => {
    definitions.push({ name, lua: options.lua, numberOfKeys: options.numberOfKeys ?? null })
    defineCommand(name, options)
  }) as typeof client.defineCommand
  restores.push(() => {
    client.defineCommand = defineCommand as typeof client.defineCommand
  })

  const bySha = new Map(
    Object.values(REDIS_ROOM_COMMANDS).map((descriptor) => [
      createHash('sha1').update(descriptor.lua).digest('hex'),
      descriptor,
    ]),
  )
  const byLua = new Map(Object.values(REDIS_ROOM_COMMANDS).map((descriptor) => [descriptor.lua, descriptor]))
  const evaluator = client as unknown as {
    evalsha(sha: string, numberOfKeys: number, ...args: unknown[]): Promise<unknown>
    eval(lua: string, numberOfKeys: number, ...args: unknown[]): Promise<unknown>
  }
  const evalsha = evaluator.evalsha.bind(client)
  const evalLua = evaluator.eval.bind(client)
  evaluator.evalsha = async (sha, numberOfKeys, ...args) => {
    const descriptor = bySha.get(sha)
    if (descriptor !== undefined) calls.push({ name: descriptor.name, keyCount: numberOfKeys, args })
    return await evalsha(sha, numberOfKeys, ...args)
  }
  evaluator.eval = async (lua, numberOfKeys, ...args) => {
    const descriptor = byLua.get(lua)
    if (descriptor !== undefined) calls.push({ name: descriptor.name, keyCount: numberOfKeys, args })
    return await evalLua(lua, numberOfKeys, ...args)
  }
  restores.push(() => {
    evaluator.evalsha = evalsha
    evaluator.eval = evalLua
  })

  return {
    definitions,
    calls,
    wrapDefinedCommand(name) {
      const target = client as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>
      const command = target[name]
      if (command === undefined) throw new Error(`defined command '${name}' was not installed`)
      const bound = command.bind(client)
      const observed: unknown[][] = []
      target[name] = async (...args) => {
        observed.push(args)
        return await bound(...args)
      }
      restores.push(() => {
        target[name] = command
      })
      return observed
    },
    restore() {
      for (const restore of restores.reverse()) restore()
    },
  }
}
