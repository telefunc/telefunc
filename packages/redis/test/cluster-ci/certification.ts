import { readFileSync } from 'node:fs'
import { Cluster, Redis } from 'ioredis'
import type { BackendSpi, CommitAccepted, LaneId, RoomHead, SubscriptionState } from 'telefunc/backend'
import { afterAll, afterEach, beforeAll, describe, expect, it, onTestFinished, vi } from 'vitest'
import { installRedis, RedisRoomBackend } from '../../src/index.js'
import {
  channelKey,
  decodeRedisOrderingFrame,
  headKey,
  genPrefix,
  laneKey,
  orderKey,
  REDIS_DELIVERY_FENCE_BYTE,
  REDIS_ROOM_COMMANDS,
} from '../../src/room/layout.js'

type RedisClusterNode = { host: string; port: number }
type Master = RedisClusterNode & { id: string; ranges: Array<[number, number]>; client: Redis }
type CommandCall = { name: string; keyCount: number; args: unknown[] }
type CommandDefinition = { name: string; lua: string; numberOfKeys: number | null }
type CommandOptions = { lua: string; numberOfKeys?: number }
type Subscription = ReturnType<BackendSpi['subscribeLane']>
type LaneReceiver = Parameters<BackendSpi['subscribeLane']>[3]

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

  afterEach(() => vi.restoreAllMocks())

  afterAll(async () => {
    await Promise.allSettled((masters ?? []).map(({ client }) => client.quit()))
    if (cluster !== undefined) await cluster.quit().catch(() => cluster.disconnect())
  })

  it('requires compatible Telefunc and master-routed Cluster reads', () => {
    const manifest = JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as {
      peerDependencies: { telefunc: string }
    }
    expect(manifest.peerDependencies.telefunc).toBe('>=0.2.23')
    const scaleReads = cluster.options.scaleReads
    try {
      cluster.options.scaleReads = 'slave'
      expect(() => new RedisRoomBackend({ redis: cluster, prefix: 'rejected:' })).toThrow(/scaleReads.*master/i)
    } finally {
      cluster.options.scaleReads = scaleReads
    }
  })

  it('covers shipped command KEYS and terminates live and in-flight attempts when their generation drops', async () => {
    expect(masters).toHaveLength(3)
    expect(masters.flatMap(({ ranges }) => ranges).reduce((total, [start, end]) => total + end - start + 1, 0)).toBe(
      16_384,
    )
    expect((await clusterInfo((masters[0] as Master).client)).cluster_size).toBe('3')
    for (const master of masters) expect((await clusterInfo(master.client)).cluster_state).toBe('ok')

    const prefix = uniquePrefix('runtime-slots')
    const client = ownCluster()
    await client.ping()
    let subscriberOpens = 0
    let holdNextSubscribe = false
    const subscribeEntered = deferred()
    const releaseSubscribe = deferred()
    interceptSubscribers(client, (subscriber) => {
      subscriberOpens++
      if (!holdNextSubscribe) return
      holdNextSubscribe = false
      const subscribe = subscriber.subscribe.bind(subscriber)
      subscriber.subscribe = (async (...channels: string[]) => {
        subscribeEntered.resolve()
        await releaseSubscribe.promise
        return await subscribe(...channels)
      }) as typeof subscriber.subscribe
    })
    const observation = observeCommands(client)
    const backend = ownBackend(client, prefix)
    const authority = ownRoomBackend(client, prefix)
    const genericCalls = observation.wrapDefinedCommand('tfPublish')
    for (const { name } of Object.values(REDIS_ROOM_COMMANDS)) observation.wrapDefinedCommand(name)
    const roomId = 'runtime-slot} proof'
    const inc = 'runtime-slot-inc'
    const head = await open(backend, roomId, inc)
    const time = vi.spyOn(client, 'time').mockImplementation(async () => {
      throw new Error('control: keyless TIME escaped the room slot')
    })
    expect((await authority.readHead(roomId))?.head.currentInc).toBe(inc)
    const cells = await backend.readCells(roomId, inc, { keys: [] })
    time.mockRestore()
    if ('staleInc' in cells) throw new Error('fresh generation was unexpectedly stale')
    expect(
      await backend.compareExchangeCells(roomId, inc, cells.revision, [
        { key: 'cell} escape', set: { bytes: bytes('value') } },
      ]),
    ).toBe('committed')
    const firstSubscription = subscribe(backend, roomId, inc, () => {})
    await firstSubscription.ready
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
    const hmget = client.hmget.bind(client)
    const hmgetMock = vi.spyOn(client, 'hmget').mockImplementation((async (key: string, ...fields: string[]) => {
      await client.hdel(key, ...fields)
      return await hmget(key, ...fields)
    }) as never)
    expect(await backend.directoryList(roomId)).toEqual({ entries: [] })
    hmgetMock.mockRestore()
    await backend.directoryPut(roomId, inc)
    await backend.directoryDelete(roomId, inc)
    const genericPublish = await backend.publish({ key: 'generic} escape', kind: 'binary' }, bytes('generic'))
    expect(genericPublish.receivers).toBeUndefined()
    const closed = await close(authority, roomId, head)
    expect(closed.state).toBe('closed')
    await authority.dropGeneration(roomId, inc)
    await waitFor(() => firstSubscription.state() === 'closed')
    expect(subscriberOpens).toBe(1)
    await firstSubscription.unsubscribe()

    const delayedRoom = 'runtime-delayed-subscribe'
    const delayedInc = 'runtime-delayed-inc'
    const delayedHead = await open(backend, delayedRoom, delayedInc)
    holdNextSubscribe = true
    const delayedSubscription = subscribe(backend, delayedRoom, delayedInc, () => {})
    onTestFinished(releaseSubscribe.resolve)
    const delayedStates: SubscriptionState[] = []
    delayedSubscription.onStateChange((state) => delayedStates.push(state))
    const delayedReady = delayedSubscription.ready.catch((error: unknown) => error)
    await subscribeEntered.promise
    await close(authority, delayedRoom, delayedHead)
    await authority.dropGeneration(delayedRoom, delayedInc)
    releaseSubscribe.resolve()
    await waitFor(() => delayedSubscription.state() === 'closed')
    expect(String(await delayedReady)).toMatch(/ownership terminated/i)
    expect(delayedStates).not.toContain('lost')
    expect(subscriberOpens).toBe(2)
    await delayedSubscription.unsubscribe()

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
    expect(new Set(await Promise.all(genericKeys.map(slot))).size, `tfPublish: ${genericKeys.join(', ')}`).toBe(1)
    for (const descriptor of Object.values(REDIS_ROOM_COMMANDS)) {
      const calls = observation.calls.filter(({ name }) => name === descriptor.name)
      expect(calls.length, descriptor.name).toBeGreaterThan(0)
      for (const call of calls) {
        const keys = call.args.slice(0, call.keyCount).map(String)
        expect(new Set(await Promise.all(keys.map(slot))).size, `${descriptor.name}: ${keys.join(', ')}`).toBe(1)
      }
    }
  })

  it('round-trips MAX_SAFE seq through commit, retain, fresh read, then rejects before effects', async () => {
    const { prefix, roomId, inc } = room('max-safe')
    const backend = ownBackend(cluster, prefix)
    const fresh = ownRoomBackend(cluster, prefix)
    const observed: number[] = []
    await open(backend, roomId, inc)
    const subscription = subscribe(backend, roomId, inc, (_payload, info) => observed.push(info.seq))
    await subscription.ready
    const orderingKey = orderKey(prefix, roomId, inc, laneKey(SEMANTIC_LANE))
    await cluster.set(orderingKey, `${Number.MAX_SAFE_INTEGER - 1}:1`)
    const last = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from('last'), { retain: true }))
    await last.delivery
    await waitFor(() => observed.length === 1)
    expect(last.seq).toBe(Number.MAX_SAFE_INTEGER)
    expect(await fresh.readRetained(roomId, inc, SEMANTIC_LANE)).toMatchObject({ seq: Number.MAX_SAFE_INTEGER })
    const lastWatermark = await cluster.get(orderingKey)
    await expect(
      backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from('overflow'), { retain: true }),
    ).rejects.toThrow('sequence exhausted')
    expect(observed).toEqual([Number.MAX_SAFE_INTEGER])
    expect(await cluster.get(orderingKey)).toBe(lastWatermark)
    const retained = await fresh.readRetained(roomId, inc, SEMANTIC_LANE)
    expect(retained?.seq).toBe(Number.MAX_SAFE_INTEGER)
    expect([...new Uint8Array(retained?.payload ?? [])]).toEqual([...Buffer.from('last')])
  })

  it('rejects invalid cell TTLs before any mutation takes effect', async () => {
    const { prefix, roomId, inc } = room('cell-preflight')
    const backend = ownBackend(cluster, prefix)
    await open(backend, roomId, inc)
    const before = await backend.readCells(roomId, inc, { keys: [] })
    if ('staleInc' in before) throw new Error('opened generation was stale')
    await expect(
      backend.compareExchangeCells(roomId, inc, before.revision, [
        { key: 'first', set: { bytes: bytes('written') } },
        { key: 'invalid', set: { bytes: bytes('rejected'), ttlMs: 0 } },
      ]),
    ).rejects.toThrow(/expire|ttl/i)
    const after = await backend.readCells(roomId, inc, { keys: ['first', 'invalid'] })
    if ('staleInc' in after) throw new Error('opened generation was stale')
    expect(after.revision).toBe(before.revision)
    expect(after.cells.size).toBe(0)
  })

  it('keeps the generation manifest complete across a reshard', async () => {
    const prefix = uniquePrefix('reshard-inventory')
    const client = ownCluster()
    await client.ping()
    const backend = ownBackend(client, prefix)
    const target = masters[0] as Master
    const source = masters[1] as Master
    const roomId = await roomOnMaster(prefix, source.id, 'reshard-inventory')
    const inc = 'reshard-inventory-inc'
    const slotNumber = await slot(headKey(prefix, roomId))
    const smembers = client.smembers.bind(client)
    let relocated = false
    const relocate = async (): Promise<void> => {
      if (relocated) return
      relocated = true
      await moveSlot(slotNumber, source, target)
    }
    vi.spyOn(client, 'smembers').mockImplementation((async (key: string) => {
      if (key === `${genPrefix(prefix, roomId, inc)}:keys`) await relocate()
      return await smembers(key)
    }) as never)
    try {
      await open(backend, roomId, inc)
      accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('retained'), { retain: true }))
      expect(await backend.listRetained(roomId, inc)).toEqual([SEMANTIC_LANE])
      expect(relocated).toBe(true)
    } finally {
      if (relocated) await restoreSlot(slotNumber, source, target)
    }
  })

  it('does not drop a generation installed after an absent or overlapping snapshot', async () => {
    const { prefix, roomId, inc } = room('drop-absent-race')
    const client = ownCluster()
    await client.ping()
    const backend = ownRoomBackend(client, prefix)
    const secondDropper = ownRoomBackend(client, prefix)
    const authority = ownRoomBackend(client, prefix)
    const snapshotRead = deferred()
    const releaseDrop = deferred()
    const inventoryRead = deferred()
    const releaseInventory = deferred()
    const commands = client as unknown as Record<string, ((...args: unknown[]) => Promise<unknown>) | undefined>
    const begin = commands.tfRoomDropGenerationBegin
    const smembers = client.smembers.bind(client)
    let inventoryHeld = false
    let firstDrop: Promise<void> | undefined
    let secondDrop: Promise<void> | undefined
    const writeCell = async (value: string): Promise<void> => {
      const read = await authority.readCells(roomId, inc, { keys: [] })
      if ('staleInc' in read) throw new Error('installed generation was stale')
      expect(
        await authority.compareExchangeCells(roomId, inc, read.revision, [
          { key: 'survivor', set: { bytes: bytes(value) } },
        ]),
      ).toBe('committed')
    }
    const reinstall = async (): Promise<void> => {
      const tombstone = await authority.readHead(roomId)
      if (tombstone !== null) {
        expect(
          await authority.compareExchangeHead(roomId, { expect: { rev: tombstone.head.rev } }, { delete: true }),
        ).toEqual({ ok: true, deleted: true })
      }
      await open(authority, roomId, inc)
      await writeCell('new')
    }
    if (begin === undefined) {
      vi.spyOn(client, 'smembers').mockImplementation((async (key: string) => {
        const result = await smembers(key)
        if (key === `${genPrefix(prefix, roomId, inc)}:keys`) {
          snapshotRead.resolve()
          await releaseDrop.promise
        }
        return result
      }) as never)
    } else {
      const bound = begin.bind(client)
      commands.tfRoomDropGenerationBegin = async (...args) => {
        const result = await bound(...args)
        snapshotRead.resolve()
        await releaseDrop.promise
        return result
      }
    }
    try {
      const dropping = backend.dropGeneration(roomId, inc)
      await snapshotRead.promise
      await open(authority, roomId, inc)
      releaseDrop.resolve()
      await dropping
      expect((await authority.readHead(roomId))?.head.currentInc).toBe(inc)
      expect(await authority.listGenerations(roomId)).toContain(inc)

      if (begin !== undefined) commands.tfRoomDropGenerationBegin = begin
      await writeCell('old')
      const active = await authority.readHead(roomId)
      if (active === null) throw new Error('installed generation lost its head')
      await close(authority, roomId, active.head)
      vi.spyOn(client, 'smembers').mockImplementation((async (key: string) => {
        if (key === `${genPrefix(prefix, roomId, inc)}:keys` && !inventoryHeld) {
          inventoryHeld = true
          inventoryRead.resolve()
          await releaseInventory.promise
        }
        return await smembers(key)
      }) as never)
      firstDrop = backend.dropGeneration(roomId, inc)
      await inventoryRead.promise
      secondDrop = secondDropper.dropGeneration(roomId, inc)
      await secondDrop
      await reinstall()
      releaseInventory.resolve()
      await firstDrop
      const reinstalled = await authority.readCells(roomId, inc, { keys: ['survivor'] })
      if ('staleInc' in reinstalled) throw new Error('reinstalled generation became stale')
      expect(Buffer.from(reinstalled.cells.get('survivor') ?? []).toString()).toBe('new')
    } finally {
      releaseDrop.resolve()
      releaseInventory.resolve()
      await Promise.allSettled([firstDrop, secondDrop].filter((drop): drop is Promise<void> => drop !== undefined))
      if (begin !== undefined) commands.tfRoomDropGenerationBegin = begin
    }
  })

  it('exposes terminal attempts so consumer replacement can fail once, recover, and deliver', async () => {
    const { prefix, roomId, inc } = room('replacement')
    let failNext = false
    let injectedFailure = false
    let opens = 0
    interceptSubscribers(cluster, (client) => {
      opens++
      if (!failNext) return
      failNext = false
      const subscribe = client.subscribe.bind(client)
      let failed = false
      client.subscribe = (async (...channels: string[]) => {
        if (!failed) {
          failed = true
          injectedFailure = true
          throw new Error('synthetic first replacement failure')
        }
        return await subscribe(...channels)
      }) as typeof client.subscribe
    })

    const baseline = new Set((await pubSubClients()).map(clientIdentity))
    const backend = ownBackend(cluster, prefix)
    const states: SubscriptionState[] = []
    const observed: string[] = []
    await open(backend, roomId, inc)
    const receiver = (payload: Uint8Array) => observed.push(Buffer.from(payload).toString())
    const subscription = subscribe(backend, roomId, inc, receiver)
    subscription.onStateChange((state) => states.push(state))
    await subscription.ready
    const first = await waitForValue(async () =>
      (await pubSubClients()).filter((client) => !baseline.has(clientIdentity(client))),
    )
    failNext = true
    await first.owner.client.call('CLIENT', 'KILL', 'ID', String(first.id))
    await waitFor(() => states.includes('lost') && subscription.state() === 'closed')

    const failed = subscribe(backend, roomId, inc, receiver)
    await expect(failed.ready).rejects.toThrow('Backend subscription closed')
    expect(injectedFailure).toBe(true)
    expect(failed.state()).toBe('closed')

    const recovered = subscribe(backend, roomId, inc, receiver)
    await recovered.ready
    expect(recovered.state()).toBe('ready')
    expect(opens).toBeGreaterThanOrEqual(3)
    const committed = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('recovered')))
    await committed.delivery
    await waitFor(() => observed.length === 1)
    expect(states).toContain('lost')
    expect(observed).toEqual(['recovered'])
  })

  it('does not settle before subscriber dispatch or credit a held dispatch from a closed epoch', async () => {
    const { prefix, roomId, inc } = room('held-dispatch')
    const subscribers: Redis[] = []
    interceptSubscribers(cluster, (client) => {
      subscribers.push(client)
      return client
    })

    const baseline = new Set((await pubSubClients()).map(clientIdentity))
    const backend = ownBackend(cluster, prefix)
    const states: SubscriptionState[] = []
    const observed: string[] = []
    const receiver = (payload: Uint8Array) => observed.push(Buffer.from(payload).toString())
    const held: Array<[Buffer, Buffer]> = []
    let holding = false
    await open(backend, roomId, inc)
    const subscription = subscribe(backend, roomId, inc, receiver)
    subscription.onStateChange((state) => states.push(state))
    await subscription.ready

    const subscriber = subscribers[0]
    if (subscriber === undefined) throw new Error('owned Redis subscriber was not observed')
    const dispatch = subscriber.listeners('messageBuffer')[0] as ((channel: Buffer, frame: Buffer) => void) | undefined
    if (dispatch === undefined) throw new Error('owned Redis subscriber dispatch listener was not observed')
    const holdDispatch = (channel: Buffer, frame: Buffer): void => {
      held.push([channel, frame])
    }
    const setHolding = (next: boolean): void => {
      if (holding === next) return
      subscriber.off('messageBuffer', next ? dispatch : holdDispatch)
      subscriber.on('messageBuffer', next ? holdDispatch : dispatch)
      holding = next
    }
    onTestFinished(() => setHolding(false))
    setHolding(true)

    const lateFirst = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('late-first')))
    const lateSecond = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('late-second')))
    await waitFor(() => held.length === 4)
    setHolding(false)
    for (const [channel, frame] of held
      .filter(([, frame]) => frame[0] !== REDIS_DELIVERY_FENCE_BYTE)
      .sort(
        (left, right) => decodeRedisOrderingFrame(right[1]).info.seq - decodeRedisOrderingFrame(left[1]).info.seq,
      )) {
      dispatch(channel, frame)
    }
    for (const [channel, frame] of held.filter(([, frame]) => frame[0] === REDIS_DELIVERY_FENCE_BYTE)) {
      dispatch(channel, frame)
    }
    await Promise.all([lateFirst.delivery, lateSecond.delivery])
    expect(observed).toEqual(['late-second'])
    held.length = 0
    setHolding(true)

    const committed = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, bytes('old-epoch')))
    const deliveryOutcome = committed.delivery.catch((error: unknown) => error)
    await waitFor(() => held.length === 2)
    const expectedChannel = channelKey(prefix, roomId, inc, laneKey(SEMANTIC_LANE))
    expect(held.map(([channel]) => channel.toString())).toEqual([expectedChannel, expectedChannel])
    expect(Buffer.from(decodeRedisOrderingFrame(held[0]![1]).payload).toString()).toBe('old-epoch')
    expect(held[1]?.[1][0]).toBe(REDIS_DELIVERY_FENCE_BYTE)
    expect(await settlesWithin(committed.delivery, 100)).toBe(false)

    const first = await waitForValue(async () =>
      (await pubSubClients()).filter((client) => !baseline.has(clientIdentity(client))),
    )
    await first.owner.client.call('CLIENT', 'KILL', 'ID', String(first.id))
    await waitFor(() => states.includes('lost') && subscription.state() === 'closed')
    const replacement = subscribe(backend, roomId, inc, receiver)
    await replacement.ready
    setHolding(false)
    for (const [channel, frame] of held) dispatch(channel, frame)

    const error = await deliveryOutcome
    expect(error).toBeInstanceOf(Error)
    expect(String(error)).toMatch(/delivery fence|connection closed/i)
    expect(observed).toEqual(['late-second'])
  })

  it('omits globally unknowable receiver counts while still delivering cross-node', async () => {
    const prefix = uniquePrefix('receivers')
    const backend = ownBackend(cluster, prefix)
    // RedisRoomBackend sorts master endpoints before round-robin subscriber selection. Mirror that
    // order, but identify masters by Cluster node ID: Compose nodes all listen on port 6379.
    const subscriberOrder = [...masters].sort((left, right) =>
      `${left.host}:${left.port}`.localeCompare(`${right.host}:${right.port}`),
    )
    const cases = [
      { label: 'cross', roomMasterId: (subscriberOrder[2] as Master).id, same: false },
      { label: 'same', roomMasterId: (subscriberOrder[1] as Master).id, same: true },
    ]
    const knownClients = new Set((await pubSubClients()).map(clientIdentity))
    for (const scenario of cases) {
      const roomId = await roomOnMaster(prefix, scenario.roomMasterId, scenario.label)
      const inc = `${scenario.label}-inc`
      await open(backend, roomId, inc)
      const observed: string[] = []
      const subscription = subscribe(backend, roomId, inc, (payload) => observed.push(Buffer.from(payload).toString()))
      await subscription.ready
      const subscriber = await waitForValue(async () =>
        (await pubSubClients()).filter((client) => !knownClients.has(clientIdentity(client))),
      )
      expect(subscriber.owner.id === scenario.roomMasterId).toBe(scenario.same)
      knownClients.add(clientIdentity(subscriber))
      const result = accepted(await backend.commitLane(roomId, inc, SEMANTIC_LANE, Buffer.from(scenario.label)))
      await result.delivery
      expect(observed).toEqual([scenario.label])
      expect(result.receivers).toBeUndefined()
    }

    const emptyObserved: string[] = []
    const text = ownSubscription(
      backend.subscribe({ key: '', kind: 'text' }, (payload, info) =>
        emptyObserved.push(`text:${info.seq}:${Buffer.from(payload).toString()}`),
      ),
    )
    const binary = ownSubscription(
      backend.subscribe({ key: '', kind: 'binary' }, (payload, info) =>
        emptyObserved.push(`binary:${info.seq}:${Buffer.from(payload).toString()}`),
      ),
    )
    await Promise.all([text.ready, binary.ready])
    const first = await backend.publish({ key: '', kind: 'text' }, bytes('one'))
    const second = await backend.publish({ key: '', kind: 'binary' }, bytes('two'))
    await waitFor(() => emptyObserved.length === 2)
    expect([first.seq, second.seq]).toEqual([1, 2])
    expect(emptyObserved).toEqual(['text:1:one', 'binary:2:two'])
  })

  function own<T>(value: T, dispose: (value: T) => unknown): T {
    onTestFinished(async () => void (await dispose(value)))
    return value
  }

  const ownBackend = (redis: Redis | Cluster, prefix: string): BackendSpi =>
    own(installRedis(redis, { prefix }), (backend) => backend.dispose())

  function ownRoomBackend(redis: Redis | Cluster, prefix: string): RedisRoomBackend {
    return own(new RedisRoomBackend({ redis, prefix }), (backend) => backend.dispose())
  }

  const ownCluster = (): Cluster =>
    own(clusterClient(CLUSTER_NODES), (client) => client.quit().catch(() => client.disconnect()))

  function ownSubscription<T extends { unsubscribe(): Promise<void> }>(subscription: T): T {
    return own(subscription, (current) => current.unsubscribe())
  }

  const room = (label: string) => ({ prefix: uniquePrefix(label), roomId: `${label}-room`, inc: `${label}-inc` })

  function subscribe(backend: BackendSpi, roomId: string, inc: string, receiver: LaneReceiver): Subscription {
    return ownSubscription(backend.subscribeLane(roomId, inc, SEMANTIC_LANE, receiver))
  }

  function interceptSubscribers(redis: Cluster, onOpen: (subscriber: Redis) => void): void {
    for (const node of redis.nodes('master')) {
      const duplicate = node.duplicate.bind(node)
      vi.spyOn(node, 'duplicate').mockImplementation((options) => {
        const subscriber = duplicate(options)
        onOpen(subscriber)
        return subscriber
      })
    }
  }

  function owner(slotNumber: number): Master {
    const match = masters.find(({ ranges }) => ranges.some(([start, end]) => slotNumber >= start && slotNumber <= end))
    if (match === undefined) throw new Error(`slot ${slotNumber} has no owner`)
    return match
  }

  async function slot(key: string): Promise<number> {
    return Number(await masters[0]?.client.cluster('KEYSLOT', key))
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

  async function moveSlot(slotNumber: number, source: Master, target: Master): Promise<void> {
    await target.client.cluster('SETSLOT', slotNumber, 'IMPORTING', source.id)
    await source.client.cluster('SETSLOT', slotNumber, 'MIGRATING', target.id)
    const keys = (await source.client.cluster('GETKEYSINSLOT', slotNumber, 10_000)) as string[]
    await migrateKeys(source, target, keys)
    for (const master of masters) await master.client.cluster('SETSLOT', slotNumber, 'NODE', target.id)
  }

  async function migrateKeys(source: Master, target: Master, keys: string[]): Promise<void> {
    for (let index = 0; index < keys.length; index += 50) {
      const batch = keys.slice(index, index + 50)
      await source.client.call('MIGRATE', target.host, String(target.port), '', '0', '30000', 'KEYS', ...batch)
    }
  }

  async function restoreSlot(slotNumber: number, original: Master, current: Master): Promise<void> {
    await moveSlot(slotNumber, current, original)
    for (const master of masters) expect((await clusterInfo(master.client)).cluster_state).toBe('ok')
  }
})

function clusterClient(nodes: RedisClusterNode[]): Cluster {
  const client = new Cluster(nodes, {
    scaleReads: 'master',
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
  const masters = new Map<string, Master>()
  for (const [start, end, [host, port, id]] of raw) {
    const existing = masters.get(id)
    if (existing === undefined) {
      masters.set(id, {
        host,
        port,
        id,
        ranges: [[start, end]],
        client: new Redis({ host, port, maxRetriesPerRequest: 2 }),
      })
    } else {
      existing.ranges.push([start, end])
    }
  }
  return [...masters.values()]
}

async function open(backend: Pick<BackendSpi, 'compareExchangeHead'>, roomId: string, inc: string): Promise<RoomHead> {
  const result = await backend.compareExchangeHead(
    roomId,
    { expect: 'absent' },
    { head: { currentInc: inc, state: 'open', config: bytes('redis-cluster-ci') } },
  )
  if (!('ok' in result) || !('head' in result)) throw new Error(`failed to open '${roomId}'`)
  return result.head
}

async function close(
  backend: Pick<BackendSpi, 'compareExchangeHead'>,
  roomId: string,
  head: RoomHead,
): Promise<RoomHead> {
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

const uniquePrefix = (label: string): string => `redis-cluster-ci:${process.pid}:${Date.now().toString(36)}:${label}:`

const clientIdentity = (client: { id: number; owner: Master }): string => `${client.owner.id}:${client.id}`

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

async function waitForValue<T>(read: () => Promise<T[]>, timeoutMs = 10_000): Promise<T> {
  let value: T | undefined
  await waitFor(async () => {
    value = (await read())[0]
    return value !== undefined
  }, timeoutMs)
  return value as T
}

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void
  const promise = new Promise<void>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

async function settlesWithin(promise: Promise<unknown>, timeoutMs: number): Promise<boolean> {
  return await Promise.race([
    promise.then(
      () => true,
      () => true,
    ),
    new Promise<false>((resolve) => setTimeout(() => resolve(false), timeoutMs)),
  ])
}

const bytes = (value: string): Uint8Array => new TextEncoder().encode(value)

function observeCommands(client: Cluster): {
  definitions: CommandDefinition[]
  calls: CommandCall[]
  wrapDefinedCommand(name: string): unknown[][]
} {
  const definitions: CommandDefinition[] = []
  const calls: CommandCall[] = []
  const defineCommand = client.defineCommand.bind(client)
  vi.spyOn(client, 'defineCommand').mockImplementation(((name: string, options: CommandOptions) => {
    definitions.push({ name, lua: options.lua, numberOfKeys: options.numberOfKeys ?? null })
    defineCommand(name, options)
  }) as never)

  return {
    definitions,
    calls,
    wrapDefinedCommand(name) {
      const target = client as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>
      const command = target[name]
      if (command === undefined) throw new Error(`defined command '${name}' was not installed`)
      const bound = command.bind(client)
      const observed: unknown[][] = []
      vi.spyOn(target, name).mockImplementation(async (...args) => {
        observed.push(args)
        const definition = [...definitions].reverse().find((candidate) => candidate.name === name)
        if (definition !== undefined) {
          const dynamic = definition.numberOfKeys === null
          calls.push({
            name,
            keyCount: definition.numberOfKeys ?? Number(args[0]),
            args: dynamic ? args.slice(1) : args,
          })
        }
        return await bound(...args)
      })
      return observed
    },
  }
}
