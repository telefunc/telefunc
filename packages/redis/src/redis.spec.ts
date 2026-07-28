import { Cluster, Redis } from 'ioredis'
import type { BackendSpi, CommitAccepted, LaneId, RoomHead, SubscriptionState } from 'telefunc/backend'
import { disposeBackend, getBackend, installBackend } from 'telefunc/backend'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import {
  bytes,
  cellsOf,
  enterClosing,
  finalizeClose,
  okHead,
  openRoom,
  SEMANTIC,
} from '../../telefunc/wire-protocol/backend/conformance/scenario.js'
import { superviseBackend } from '../../telefunc/wire-protocol/backend/supervised-backend.js'
import { installRedis, RedisRoomBackend } from './index.js'
import { createRedisFixture, parseRedisClusterNodes, type RedisClusterNode } from './room/fixture.js'
import { DIRECTORY_PUT_LUA, directoryIndexKey, headKey, laneKey, orderKey, REDIS_ROOM_COMMANDS } from './room/layout.js'

const REDIS_URL = process.env.TELEFUNC_TEST_REAL_REDIS
const RAW_CLUSTER_NODES = process.env.TELEFUNC_TEST_REDIS_CLUSTER_NODES
const CLUSTER_NODES =
  RAW_CLUSTER_NODES === undefined || RAW_CLUSTER_NODES === '' ? undefined : parseRedisClusterNodes(RAW_CLUSTER_NODES)
const SEMANTIC_LANE: LaneId = { kind: 'semantic' }

describe('Redis public installation', () => {
  afterEach(async () => {
    await disposeBackend()
  })

  it('installs one complete default, stays idempotent, and never overrides an explicit backend', () => {
    const redis = new ConstructionOnlyRedis()
    installRedis(redis as unknown as Redis, { prefix: 'install:' })
    const automatic = getBackend()
    expect(automatic.spiVersion).toBe(1)
    expect(redis.commands).toContain('tfPublish')
    expect(redis.commands).toContain('tfRoomCommit')

    installRedis(redis as unknown as Redis, { prefix: 'install:' })
    expect(getBackend()).toBe(automatic)

    const explicit = installBackend(
      () => new RedisRoomBackend({ redis: new ConstructionOnlyRedis() as unknown as Redis }),
    )
    installRedis(new ConstructionOnlyRedis() as unknown as Redis)
    expect(getBackend()).toBe(explicit)
  })
})

describe.skipIf(REDIS_URL === undefined && CLUSTER_NODES === undefined)('Redis-specific realization', () => {
  it('activates cross-instance durable Room delivery through installRedis alone', async () => {
    const prefix = uniquePrefix('install-cross-instance')
    const [installedClient, peerClient] = await realClients()
    const peer = redisBackend(peerClient, prefix)
    const roomId = 'install-cross-instance'
    const observed: string[] = []
    let subscription: ReturnType<BackendSpi['subscribeLane']> | undefined
    try {
      installRedis(installedClient, { prefix })
      const installed = getBackend()
      const { inc } = await openRoom(installed, roomId)
      expect(await peer.readHead(roomId)).not.toBeNull()

      subscription = peer.subscribeLane(roomId, inc, SEMANTIC_LANE, (payload) => {
        observed.push(Buffer.from(payload).toString())
      })
      await subscription.ready
      await accepted(await installed.commitLane(roomId, inc, SEMANTIC_LANE, bytes('from-installed-instance'))).delivery
      await waitFor(() => observed.length === 1)
      expect(observed).toEqual(['from-installed-instance'])
    } finally {
      await subscription?.unsubscribe()
      await Promise.allSettled([disposeBackend(), peer.dispose()])
      await closeClients(installedClient, peerClient)
    }
  })

  it('uses the shared 16-byte frame for generic Broadcast and rejects seq exhaustion before publish', async () => {
    const prefix = uniquePrefix('generic')
    const [publisher, subscriber] = await realClients()
    const source = redisBackend(publisher, prefix)
    const target = redisBackend(subscriber, prefix)
    const key = 'generic-wide'
    const observed: Array<{ payload: number[]; seq: number }> = []
    const subscription = target.subscribe({ key, kind: 'binary' }, (payload, info) => {
      observed.push({ payload: [...payload], seq: info.seq })
    })
    try {
      await subscription.ready
      await publisher.set(`${prefix}seq:{${key}}`, String(0xffff_ffff))
      const wide = await source.publish({ key, kind: 'binary' }, new Uint8Array([1, 2, 3]))
      expect(wide.seq).toBe(0x1_0000_0000)
      await waitFor(() => observed.length === 1)
      expect(observed).toEqual([{ payload: [1, 2, 3], seq: 0x1_0000_0000 }])

      await publisher.set(`${prefix}seq:{${key}}`, String(Number.MAX_SAFE_INTEGER - 1))
      expect((await source.publish({ key, kind: 'binary' }, new Uint8Array([4]))).seq).toBe(Number.MAX_SAFE_INTEGER)
      await expect(source.publish({ key, kind: 'binary' }, new Uint8Array([5]))).rejects.toThrow('sequence exhausted')
      await waitFor(() => observed.length === 2)
      expect(observed[1]).toEqual({ payload: [4], seq: Number.MAX_SAFE_INTEGER })
    } finally {
      await subscription.unsubscribe()
      await Promise.all([source.dispose(), target.dispose()])
      await closeClients(publisher, subscriber)
    }
  })

  it('keeps seq monotonic across timestamp advance, inactivity, and backend reconstruction', async () => {
    const fx = await realFixture()
    try {
      const roomId = 'monotonic-no-reset'
      fx.orderControl.setAuthority(10_000)
      const { inc } = await openRoom(fx.backend, roomId)
      const marks: Array<{ seq: number; timestamp: number }> = []
      marks.push(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('one'))))
      fx.orderControl.setAuthority(20_000)
      marks.push(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('two'))))
      fx.advanceAuthority(30 * 24 * 60 * 60 * 1_000)
      marks.push(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('three'))))
      await fx.orderControl.reconstructBackend()
      marks.push(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('four'))))
      expect(marks.map(({ seq }) => seq)).toEqual([1, 2, 3, 4])
      expect(marks.map(({ timestamp }) => timestamp)).toEqual(
        [...marks.map(({ timestamp }) => timestamp)].sort((left, right) => left - right),
      )
    } finally {
      await fx.dispose()
    }
  })

  it('makes retained ifSeq compare-delete atomic against a concurrent replacement', async () => {
    const fx = await realFixture()
    const peer = await fx.createPeerBackend()
    try {
      const roomId = 'retained-if-seq'
      const { inc } = await openRoom(fx.backend, roomId)
      accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('first'), { retain: true }))
      const second = accepted(
        await fx.backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('second'), { retain: true }),
      )
      const [, replacementResult] = await Promise.all([
        fx.backend.deleteRetained(roomId, inc, SEMANTIC_LANE, { ifSeq: second.seq }),
        peer.backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('replacement'), { retain: true }),
      ])
      const replacement = accepted(replacementResult)
      expect(await fx.backend.readRetained(roomId, inc, SEMANTIC_LANE)).toMatchObject({
        payload: bytes('replacement'),
        seq: replacement.seq,
      })
    } finally {
      await peer.dispose()
      await fx.dispose()
    }
  })

  it('rejects a commit before effects when the atomic head fence is stale', async () => {
    const fx = await realFixture()
    try {
      const roomId = 'atomic-head-fence'
      const { inc, head } = await openRoom(fx.backend, roomId)
      await enterClosing(fx.backend, roomId, head)
      expect(
        await fx.backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('must-not-commit'), { retain: true }),
      ).toEqual({ stale: true })
      expect(await fx.backend.readRetained(roomId, inc, SEMANTIC_LANE)).toBeNull()
    } finally {
      await fx.dispose()
    }
  })
})

describe.skipIf(CLUSTER_NODES === undefined)('Redis real three-master Cluster certification', () => {
  let cluster: Cluster
  let masters: Master[]

  beforeAll(async () => {
    cluster = clusterClient(CLUSTER_NODES as RedisClusterNode[])
    await cluster.ping()
    masters = await readMasters(CLUSTER_NODES as RedisClusterNode[])
  })

  afterAll(async () => {
    await Promise.allSettled((masters ?? []).map(({ client }) => client.quit()))
    if (cluster !== undefined) await cluster.quit().catch(() => cluster.disconnect())
  })

  it('covers all slots and every shipped command with actual co-slotted KEYS', async () => {
    expect(masters).toHaveLength(3)
    expect(masters.reduce((total, master) => total + master.end - master.start + 1, 0)).toBe(16_384)
    for (const master of masters) expect((await clusterInfo(master.client)).cluster_state).toBe('ok')

    const first = CLUSTER_NODES?.[0] as RedisClusterNode
    const fx = await createRedisFixture(`redis://${first.host}:${first.port}`, {
      clusterNodes: CLUSTER_NODES as RedisClusterNode[],
    })
    try {
      expect(fx.backend.capabilities).toMatchObject({ clusterSafe: true, receivers: 'node-local' })
      const roomId = 'runtime-slot} proof'
      const { inc, head } = await openRoom(fx.backend, roomId)
      const cells = cellsOf(await fx.backend.readCells(roomId, inc, { keys: [] }))
      expect(
        await fx.backend.compareExchangeCells(roomId, inc, cells.revision, [
          { key: 'cell} escape', set: { bytes: bytes('value') } },
        ]),
      ).toBe('committed')
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC_LANE, () => {})
      await sub.ready
      await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('payload'), { retain: true }))
        .delivery
      await fx.backend.deleteRetained(roomId, inc, SEMANTIC_LANE)
      await fx.backend.directoryPut(roomId, inc)
      await fx.backend.directoryDelete(roomId, inc)
      await sub.unsubscribe()
      const closing = await enterClosing(fx.backend, roomId, head)
      okHead(await finalizeClose(fx.backend, roomId, closing.head, closing.leaseId))
      await fx.backend.dropGeneration(roomId, inc)

      for (const descriptor of Object.values(REDIS_ROOM_COMMANDS)) {
        const calls = fx.commandCalls().filter(({ name }) => name === descriptor.name)
        expect(calls.length, descriptor.name).toBeGreaterThan(0)
        for (const call of calls) {
          const offset = descriptor.numberOfKeys === null ? 1 : 0
          const count = descriptor.numberOfKeys ?? Number(call.args[0])
          const keys = call.args.slice(offset, offset + count).map(String)
          expect(new Set(keys.map(redisSlot)), `${descriptor.name}: ${keys.join(', ')}`).toEqual(
            new Set([redisSlot(keys[0] as string)]),
          )
        }
      }
    } finally {
      await fx.dispose()
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
    let sub: ReturnType<BackendSpi['subscribeLane']> | undefined
    try {
      await open(backend, roomId, inc)
      sub = backend.subscribeLane(roomId, inc, SEMANTIC_LANE, (_payload, info) => observed.push(info.seq))
      await sub.ready
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
      await sub?.unsubscribe()
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
    const roomId = 'replacement-room'
    const inc = 'replacement-inc'
    const states: SubscriptionState[] = []
    const observed: string[] = []
    let sub: ReturnType<BackendSpi['subscribeLane']> | undefined
    try {
      await open(backend, roomId, inc)
      sub = backend.subscribeLane(roomId, inc, SEMANTIC_LANE, (payload) =>
        observed.push(Buffer.from(payload).toString()),
      )
      sub.onStateChange((state) => states.push(state))
      await sub.ready
      const [first] = await waitForValue(async () => (await pubSubClients()).filter(({ id }) => !baseline.has(id)))
      if (first === undefined) throw new Error('initial Redis subscriber was not observed')
      failNext = true
      await first.owner.client.call('CLIENT', 'KILL', 'ID', String(first.id))
      await waitFor(() => states.includes('lost') && sub?.state() === 'ready' && opens >= 3, 15_000)
      const committed = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from('recovered')))
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
      await sub?.unsubscribe().catch(() => {})
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
    const roomId = 'stale-pong-room'
    const inc = 'stale-pong-inc'
    const states: SubscriptionState[] = []
    const observed: string[] = []
    const oldPongHeld = deferred<void>()
    const releaseOldPong = deferred<void>()
    let sub: ReturnType<BackendSpi['subscribeLane']> | undefined
    let restorePing: (() => void) | undefined
    try {
      await open(backend, roomId, inc)
      sub = backend.subscribeLane(roomId, inc, SEMANTIC_LANE, (payload) =>
        observed.push(Buffer.from(payload).toString()),
      )
      sub.onStateChange((state) => states.push(state))
      await sub.ready

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

      const committed = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from('old-epoch')))
      const deliveryOutcome = committed.delivery.then(
        () => null,
        (error: unknown) => error,
      )
      await oldPongHeld.promise
      await waitFor(() => observed.length === 1)

      const [first] = await waitForValue(async () => (await pubSubClients()).filter(({ id }) => !baseline.has(id)))
      if (first === undefined) throw new Error('initial Redis subscriber connection was not observed')
      await first.owner.client.call('CLIENT', 'KILL', 'ID', String(first.id))
      await waitFor(() => states.includes('lost') && sub?.state() === 'ready')
      releaseOldPong.resolve()

      const error = await deliveryOutcome
      expect(error).toBeInstanceOf(Error)
      expect(String(error)).toMatch(/PING crossed source/)
      expect(observed).toEqual(['old-epoch'])
    } finally {
      releaseOldPong.resolve()
      restorePing?.()
      for (const [node, original] of originals) node.duplicate = original as typeof node.duplicate
      await sub?.unsubscribe().catch(() => {})
      await backend.dispose()
    }
  })

  it('reports exact node-local receiver counts while still delivering cross-node', async () => {
    const prefix = uniquePrefix('receivers')
    const backend = redisBackend(cluster, prefix)
    const selectedPorts = [...masters].sort((left, right) => left.port - right.port).map(({ port }) => port)
    const cases = [
      { label: 'same', roomPort: selectedPorts[0] as number, expected: 1 },
      { label: 'cross', roomPort: selectedPorts[2] as number, expected: 0 },
    ]
    const subscriptions = []
    try {
      for (const scenario of cases) {
        const roomId = await roomOnMaster(prefix, scenario.roomPort, scenario.label)
        const inc = `${scenario.label}-inc`
        await open(backend, roomId, inc)
        const observed: string[] = []
        const sub = backend.subscribeLane(roomId, inc, SEMANTIC_LANE, (payload) =>
          observed.push(Buffer.from(payload).toString()),
        )
        subscriptions.push(sub)
        await sub.ready
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
    const client = clusterClient(CLUSTER_NODES as RedisClusterNode[])
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
      const target = masters.find(({ port }) => port !== source.port) as Master
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
    const controlClient = clusterClient(CLUSTER_NODES as RedisClusterNode[], 0)
    const control = new RedisRoomBackend({ redis: controlClient, prefix })
    let moved: { slot: number; source: Master; target: Master } | undefined
    try {
      await open(backend, roomId, inc)
      await control.readHead(roomId)
      const slotNumber = await slot(headKey(prefix, roomId))
      const source = owner(slotNumber)
      const target = masters.find(({ port }) => port !== source.port) as Master
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
    const client = clusterClient(CLUSTER_NODES as RedisClusterNode[])
    const backend = redisBackend(client, prefix)
    const controlClient = clusterClient(CLUSTER_NODES as RedisClusterNode[], 0)
    const control = new RedisRoomBackend({ redis: controlClient, prefix })
    let migration: { slot: number; source: Master; target: Master; finalized: boolean } | undefined
    try {
      await open(backend, roomId, inc)
      await control.readHead(roomId)
      const slotNumber = await slot(headKey(prefix, roomId))
      const source = owner(slotNumber)
      const target = masters.find(({ port }) => port !== source.port) as Master
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

  async function slot(key: string): Promise<number> {
    return Number(await masters[0]?.client.cluster('KEYSLOT', key))
  }

  async function keyOnDifferentMaster(key: string, prefix: string): Promise<string> {
    const firstPort = owner(await slot(key)).port
    for (let i = 0; i < 50_000; i++) {
      const candidate = headKey(prefix, `cross-${i}`)
      if (owner(await slot(candidate)).port !== firstPort) return candidate
    }
    throw new Error('failed to find a key on another master')
  }

  async function roomOnMaster(prefix: string, port: number, label: string): Promise<string> {
    for (let i = 0; i < 50_000; i++) {
      const roomId = `${label}-${i}`
      if (owner(await slot(headKey(prefix, roomId))).port === port) return roomId
    }
    throw new Error(`failed to find a room on master ${port}`)
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
    for (let i = 0; i < keys.length; i += 50) {
      const batch = keys.slice(i, i + 50)
      if (batch.length > 0) {
        await source.client.call('MIGRATE', target.host, String(target.port), '', '0', '30000', 'KEYS', ...batch)
      }
    }
  }

  async function restoreSlot(slotNumber: number, original: Master, current: Master): Promise<void> {
    if (original.port === current.port) return
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

class ConstructionOnlyRedis {
  readonly commands: string[] = []
  defineCommand(name: string): void {
    this.commands.push(name)
  }
}

type Master = RedisClusterNode & { id: string; start: number; end: number; client: Redis }

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

async function realClients(): Promise<[Redis | Cluster, Redis | Cluster]> {
  if (CLUSTER_NODES !== undefined) {
    const first = clusterClient(CLUSTER_NODES)
    const second = clusterClient(CLUSTER_NODES)
    await Promise.all([first.ping(), second.ping()])
    return [first, second]
  }
  const first = new Redis(REDIS_URL as string)
  const second = new Redis(REDIS_URL as string)
  await Promise.all([first.ping(), second.ping()])
  return [first, second]
}

async function realFixture() {
  if (CLUSTER_NODES !== undefined) {
    const first = CLUSTER_NODES[0] as RedisClusterNode
    return await createRedisFixture(`redis://${first.host}:${first.port}`, { clusterNodes: CLUSTER_NODES })
  }
  return await createRedisFixture(REDIS_URL as string)
}

async function closeClients(...clients: Array<Redis | Cluster>): Promise<void> {
  await Promise.allSettled(clients.map(async (client) => await client.quit()))
}

function redisBackend(redis: Redis | Cluster, prefix: string): BackendSpi {
  return superviseBackend(new RedisRoomBackend({ redis, prefix }))
}

async function open(backend: BackendSpi, roomId: string, inc: string): Promise<RoomHead> {
  const result = await backend.compareExchangeHead(
    roomId,
    { expect: 'absent' },
    { head: { currentInc: inc, state: 'open', config: Buffer.from('redis-spec') } },
  )
  if (!('ok' in result) || !('head' in result)) throw new Error(`failed to open '${roomId}'`)
  return result.head
}

function accepted(result: Awaited<ReturnType<BackendSpi['commitLane']>>): CommitAccepted {
  if (!('accepted' in result)) throw new Error('commit was unexpectedly stale')
  return result
}

function uniquePrefix(label: string): string {
  return `redis-spec:${process.pid}:${Date.now().toString(36)}:${label}:`
}

function compareRedisNodes(left: Redis, right: Redis): number {
  return `${left.options.host ?? ''}:${left.options.port ?? ''}`.localeCompare(
    `${right.options.host ?? ''}:${right.options.port ?? ''}`,
  )
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

function deferred<T>() {
  let resolve!: (value?: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise as (value?: T) => void
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}
