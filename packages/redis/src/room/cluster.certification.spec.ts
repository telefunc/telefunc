// Real Redis Cluster certification for the exact shipped public backend. The W4-R launcher owns the
// three masters, data directories, ports, watchdog, and cleanup. No fake/mocked redirection is accepted.

import { createCipheriv, createHash } from 'node:crypto'
import { gzipSync } from 'node:zlib'
import { Cluster, Redis } from 'ioredis'
import type { CommitAccepted, LaneId, RoomHead } from 'telefunc/backend'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { RedisRoomBackend } from './backend.js'
import { DIRECTORY_PUT_LUA, directoryIndexKey, headKey, roomTag } from './layout.js'

type NodeAddress = { host: string; port: number }
type Master = NodeAddress & { id: string; start: number; end: number; client: Redis }

const rawNodes = process.env.TELEFUNC_TEST_REDIS_CLUSTER_NODES
const expectedClusterSafe = process.env.TELEFUNC_EXPECT_CLUSTER_SAFE === 'true'
const nodes = rawNodes === undefined || rawNodes === '' ? undefined : parseNodes(rawNodes)
const CEILING = 16 * 1024 * 1024
const SEMANTIC: LaneId = { kind: 'semantic' }
const CONTROL: LaneId = { kind: 'control' }
const DIGESTS = {
  3: {
    live: 'b0c04111efba2ab759110e270484c7cb20d2bfab686ba521452f609dbdaebe0d',
    replacement: 'a7300c4c3c5694502d753fd839f37ef337d3e79b0bcca3403fb949221b43eef3',
  },
  8: {
    live: '6162ae67579a148fc30772064840da73e07922b3433a2b9f0195480325330d52',
    replacement: '68d02663711afd14a3da5c6b0fde28cbb54502773cff2276db3ce8a67a877ba8',
  },
  16: {
    live: '6ee557e44b5af6b637a2da6e1a9c0559e715a47b7e1b2bec6da66b93ea282c44',
    replacement: '4c8e608a16595fccdcb8386d03d481221464030bd7f0438ca3a7831f8fe648b9',
  },
} as const

describe.skipIf(nodes === undefined)('RedisRoomBackend real three-master Cluster certification', () => {
  let cluster: Cluster
  let masters: Master[]

  beforeAll(async () => {
    cluster = clusterClient(nodes as NodeAddress[])
    await cluster.ping()
    masters = await readMasters(nodes as NodeAddress[])
  })

  afterAll(async () => {
    await Promise.allSettled(masters.map((master) => master.client.quit()))
    await cluster.quit()
  })

  it('reports three real masters, full slots, cluster_state:ok, and the public capability', async () => {
    expect(masters).toHaveLength(3)
    expect(masters.reduce((sum, master) => sum + master.end - master.start + 1, 0)).toBe(16_384)
    const infos = await Promise.all(masters.map((master) => master.client.cluster('INFO')))
    for (const info of infos) {
      expect(String(info)).toContain('cluster_state:ok')
      expect(String(info)).toContain('cluster_slots_assigned:16384')
      expect(String(info)).toContain('cluster_known_nodes:3')
    }

    const publicRuntime = await import('../../dist/index.js')
    const backend = new publicRuntime.RedisRoomBackend({
      redis: cluster as unknown as Redis,
      prefix: uniquePrefix('capability'),
    })
    expect(backend.capabilities.clusterSafe).toBe(expectedClusterSafe)
    expect(backend.capabilities.receivers).toBe('node-local')
    await backend.dispose()
    console.log(
      `[w4r-evidence] topology=${JSON.stringify(
        masters.map(({ id, host, port, start, end }) => ({ id, host, port, slots: [start, end] })),
      )}`,
    )
  })

  it('observes real CROSSSLOT and undeclared ARGV-key failures', async () => {
    const prefix = uniquePrefix('crossslot')
    const directory = directoryIndexKey(prefix)
    const otherRoomKey = await keyOnDifferentMaster(directory, prefix)
    expect(await slot(directory)).not.toBe(await slot(otherRoomKey))

    await expect(
      cluster.eval(DIRECTORY_PUT_LUA, 2, directory, otherRoomKey, 'literal-room', 'literal-inc'),
    ).rejects.toThrow(/CROSSSLOT/)

    await cluster.set(directory, 'declared')
    await cluster.set(otherRoomKey, 'argv')
    await expect(cluster.eval("return redis.call('GET', ARGV[1])", 1, directory, otherRoomKey)).rejects.toThrow(
      /non local key|CROSSSLOT/i,
    )
    console.log(`[w4r-evidence] crossslot declaredSlot=${await slot(directory)} argvSlot=${await slot(otherRoomKey)}`)
  })

  it('orders 100 concurrent text+announce semantic commits without loss or duplication', async () => {
    const prefix = uniquePrefix('order')
    const backend = new RedisRoomBackend({ redis: cluster, prefix })
    const roomId = 'ordered-room'
    const inc = 'ordered-inc'
    await open(backend, roomId, inc)
    const callbacks: Array<{ seq: number; id: number }> = []
    const sub = backend.subscribeLane(roomId, inc, SEMANTIC, (payload, info) => {
      callbacks.push({ seq: info.seq, id: JSON.parse(Buffer.from(payload).toString()).id as number })
    })
    await sub.ready

    const accepted = await Promise.all(
      Array.from({ length: 100 }, async (_, id) => {
        const kind = id % 2 === 0 ? 'text' : 'announce'
        const result = await backend.commitLane(roomId, inc, SEMANTIC, Buffer.from(JSON.stringify({ id, kind })))
        if (!('accepted' in result)) throw new Error(`commit ${id} was unexpectedly stale`)
        return { id, result }
      }),
    )
    await Promise.all(accepted.map(({ result }) => result.delivery))
    await waitFor(() => callbacks.length === 100, 10_000)

    const bySeq = new Map(accepted.map(({ id, result }) => [result.seq, id]))
    expect([...bySeq.keys()].sort((a, b) => a - b)).toEqual(Array.from({ length: 100 }, (_, i) => i + 1))
    expect(callbacks.map(({ seq }) => seq)).toEqual(Array.from({ length: 100 }, (_, i) => i + 1))
    expect(callbacks.map(({ seq, id }) => [seq, id])).toEqual(
      Array.from({ length: 100 }, (_, i) => [i + 1, bySeq.get(i + 1)]),
    )
    expect(new Set(callbacks.map(({ id }) => id))).toHaveLength(100)
    await sub.unsubscribe()
    await backend.dispose()
  })

  for (const mib of [3, 8, 16] as const) {
    it(`delivers, retains, replaces, fresh-reads, and rolls back exact incompressible ${mib} MiB`, async () => {
      const size = mib * 1024 * 1024
      const prefix = uniquePrefix(`payload-${mib}`)
      const client = clusterClient(nodes as NodeAddress[])
      const backend = new RedisRoomBackend({ redis: client, prefix, maxRetainedPayloadBytes: size })
      const roomId = `payload-room-${mib}`
      const inc = `payload-inc-${mib}`
      await open(backend, roomId, inc)
      const received: Array<{ length: number; digest: string }> = []
      const sub = backend.subscribeLane(roomId, inc, SEMANTIC, (payload) => received.push(describeBytes(payload)))
      await sub.ready

      const live = payload(mib, 'live')
      const replacement = payload(mib, 'replacement')
      expect(describeBytes(live)).toEqual({ length: size, digest: DIGESTS[mib].live })
      expect(describeBytes(replacement)).toEqual({ length: size, digest: DIGESTS[mib].replacement })
      expect(gzipSync(live).byteLength).toBeGreaterThan(size * 0.99)

      const first = accepted(await backend.commitLane(roomId, inc, SEMANTIC, live, { retain: true }))
      await first.delivery
      await waitFor(() => received.length === 1, 30_000)
      expect(received[0]).toEqual({ length: size, digest: DIGESTS[mib].live })
      expect(describeRetained(await backend.readRetained(roomId, inc, SEMANTIC))).toEqual({
        length: size,
        digest: DIGESTS[mib].live,
      })

      const second = accepted(await backend.commitLane(roomId, inc, SEMANTIC, replacement, { retain: true }))
      await second.delivery
      await waitFor(() => received.length === 2, 30_000)
      expect(received[1]).toEqual({ length: size, digest: DIGESTS[mib].replacement })

      const freshClient = clusterClient(nodes as NodeAddress[])
      const freshBackend = new RedisRoomBackend({ redis: freshClient, prefix, maxRetainedPayloadBytes: size })
      expect(describeRetained(await freshBackend.readRetained(roomId, inc, SEMANTIC))).toEqual({
        length: size,
        digest: DIGESTS[mib].replacement,
      })

      await expect(
        backend.commitLane(roomId, inc, SEMANTIC, payloadBytes(size + 1, `over-${mib}`), { retain: true }),
      ).rejects.toThrow(/exceeds the \d+ byte cap/)
      expect(describeRetained(await backend.readRetained(roomId, inc, SEMANTIC))).toEqual({
        length: size,
        digest: DIGESTS[mib].replacement,
      })
      expect(received).toHaveLength(2)

      await freshBackend.dispose()
      await freshClient.quit()
      await sub.unsubscribe()
      await backend.dispose()
      await client.quit()
      console.log(
        `[w4r-evidence] payload mib=${mib} bytes=${size} live=${DIGESTS[mib].live} replacement=${DIGESTS[mib].replacement}`,
      )
    })
  }

  it('lists, paginates, repairs, and drops rooms whose data spans every master', async () => {
    const prefix = uniquePrefix('directory')
    const backend = new RedisRoomBackend({ redis: cluster, prefix })
    const liveRooms: Array<{ roomId: string; inc: string; port: number }> = []
    for (const master of masters) {
      const roomId = await roomOnMaster(prefix, master.port, `live-${master.port}`)
      const inc = `inc-${master.port}`
      await open(backend, roomId, inc)
      await backend.compareExchangeCells(roomId, inc, '0', [{ key: 'proof', set: { bytes: Buffer.from(roomId) } }])
      await backend.directoryPut(roomId, inc)
      liveRooms.push({ roomId, inc, port: master.port })
    }
    const staleRooms = Array.from({ length: 105 }, (_, i) => `page-${String(i).padStart(3, '0')}`)
    for (const roomId of staleRooms) await backend.directoryPut(roomId, 'stale-inc')

    const listed = await listAll(backend, '')
    for (const room of liveRooms) expect(listed).toContainEqual({ roomId: room.roomId, incTag: room.inc })
    for (const roomId of staleRooms) expect(listed).toContainEqual({ roomId, incTag: 'stale-inc' })

    const firstMasterKeys = await scanNode(masters[0] as Master, `${prefix}room:*:head`)
    const singleMasterRooms = liveRooms.filter(({ roomId }) => firstMasterKeys.includes(headKey(prefix, roomId)))
    expect(singleMasterRooms.length).toBeLessThan(liveRooms.length)

    for (const entry of listed) {
      if ((await backend.readHead(entry.roomId)) === null) await backend.directoryDelete(entry.roomId, entry.incTag)
    }
    expect(await listAll(backend, '')).toHaveLength(liveRooms.length)

    for (const room of liveRooms) {
      await closeAndDrop(backend, room.roomId, room.inc)
      const physical = await scanAll(`${roomTag(prefix, room.roomId)}:g:${room.inc}:*`)
      expect(physical).toEqual([])
    }
    await backend.dispose()
    console.log(
      `[w4r-evidence] directory masters=${liveRooms.map(({ port }) => port).join(',')} pages=2 repaired=${staleRooms.length} singleMasterControl=${singleMasterRooms.length}/${liveRooms.length}`,
    )
  })

  it('follows a real MOVED after slot relocation while no-refresh controls fail', async () => {
    const prefix = uniquePrefix('moved')
    const roomId = 'moved-room'
    const inc = 'moved-inc'
    const backend = new RedisRoomBackend({ redis: cluster, prefix })
    await open(backend, roomId, inc)
    const key = headKey(prefix, roomId)
    const slotNumber = await slot(key)
    const source = owner(slotNumber)
    const target = masters.find((master) => master.port !== source.port) as Master

    const noRefreshClient = clusterClient(nodes as NodeAddress[], 0)
    const noRefreshBackend = new RedisRoomBackend({ redis: noRefreshClient, prefix })
    await noRefreshBackend.readHead(roomId)
    await moveSlot(slotNumber, source, target, true)

    await expect(source.client.get(key)).rejects.toThrow(/MOVED/)
    await expect(noRefreshBackend.readHead(roomId)).rejects.toThrow(/redirection|MOVED/i)
    expect((await backend.readHead(roomId))?.head.currentInc).toBe(inc)
    const committed = accepted(await backend.commitLane(roomId, inc, SEMANTIC, Buffer.from('after-moved')))
    await committed.delivery
    await assertClusterOk()

    await noRefreshBackend.dispose()
    await noRefreshClient.quit()
    await backend.dispose()
    console.log(
      `[w4r-evidence] MOVED slot=${slotNumber} source=${source.id}@${source.port} target=${target.id}@${target.port}`,
    )
  })

  it('follows a real ASK during importing/migrating while no-ASKING controls fail', async () => {
    const prefix = uniquePrefix('ask')
    const roomId = 'ask-room'
    const inc = 'ask-inc'
    const client = clusterClient(nodes as NodeAddress[])
    const backend = new RedisRoomBackend({ redis: client, prefix })
    await open(backend, roomId, inc)
    const key = headKey(prefix, roomId)
    const slotNumber = await slot(key)
    const source = owner(slotNumber)
    const target = masters.find((master) => master.port !== source.port) as Master
    const noAskingClient = clusterClient(nodes as NodeAddress[], 0)
    const noAskingBackend = new RedisRoomBackend({ redis: noAskingClient, prefix })
    await noAskingBackend.readHead(roomId)

    await target.client.cluster('SETSLOT', slotNumber, 'IMPORTING', source.id)
    await source.client.cluster('SETSLOT', slotNumber, 'MIGRATING', target.id)
    const keys = (await source.client.cluster('GETKEYSINSLOT', slotNumber, 10_000)) as string[]
    await migrateKeys(source, target, keys)

    await expect(source.client.get(key)).rejects.toThrow(/ASK/)
    await expect(target.client.get(key)).rejects.toThrow(/MOVED/)
    await target.client.asking()
    expect(await target.client.get(key)).not.toBeNull()
    await expect(noAskingBackend.readHead(roomId)).rejects.toThrow(/redirection|ASK/i)
    expect((await backend.readHead(roomId))?.head.currentInc).toBe(inc)

    for (const master of masters) await master.client.cluster('SETSLOT', slotNumber, 'NODE', target.id)
    await waitFor(async () => (await clusterInfo(target.client)).cluster_state === 'ok', 10_000)
    // Redis intentionally returns TRYAGAIN for a multi-key script while a slot is mid-rehash if some
    // operands don't exist yet. The single-key shipped read above is the safe ASK schedule; after slot
    // finalization the same long-lived backend immediately resumes atomic multi-key commits.
    const committed = accepted(await backend.commitLane(roomId, inc, SEMANTIC, Buffer.from('after-ask')))
    await committed.delivery
    await assertClusterOk()

    await noAskingBackend.dispose()
    await noAskingClient.quit()
    await backend.dispose()
    await client.quit()
    console.log(
      `[w4r-evidence] ASK slot=${slotNumber} source=${source.id}@${source.port} target=${target.id}@${target.port} migratedKeys=${keys.length}`,
    )
  })

  it('returns every client connection to baseline after public backend disposal', async () => {
    const before = await clientCounts()
    const client = clusterClient(nodes as NodeAddress[])
    const backend = new RedisRoomBackend({ redis: client, prefix: uniquePrefix('dispose') })
    await open(backend, 'dispose-room', 'dispose-inc')
    const sub = backend.subscribeLane('dispose-room', 'dispose-inc', SEMANTIC, () => {})
    await sub.ready
    await sub.unsubscribe()
    await backend.dispose()
    await client.quit()
    await waitFor(async () => {
      const after = await clientCounts()
      return after.every((count, i) => count <= (before[i] as number))
    }, 10_000)
    expect((await clientCounts()).every((count, i) => count <= (before[i] as number))).toBe(true)
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
    const firstOwner = owner(await slot(key)).port
    for (let i = 0; i < 50_000; i++) {
      const candidate = headKey(prefix, `cross-${i}`)
      if (owner(await slot(candidate)).port !== firstOwner) return candidate
    }
    throw new Error('failed to find a key on a different master')
  }

  async function roomOnMaster(prefix: string, port: number, label: string): Promise<string> {
    for (let i = 0; i < 50_000; i++) {
      const roomId = `${label}-${i}`
      if (owner(await slot(headKey(prefix, roomId))).port === port) return roomId
    }
    throw new Error(`failed to find room id on master ${port}`)
  }

  async function scanAll(pattern: string): Promise<string[]> {
    const pages = await Promise.all(masters.map((master) => scanNode(master, pattern)))
    return [...new Set(pages.flat())]
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

  async function assertClusterOk(): Promise<void> {
    for (const master of masters) expect((await clusterInfo(master.client)).cluster_state).toBe('ok')
  }

  async function clientCounts(): Promise<number[]> {
    return await Promise.all(
      masters.map(
        async ({ client }) =>
          String(await client.client('LIST'))
            .split('\n')
            .filter(Boolean).length,
      ),
    )
  }
})

function parseNodes(raw: string): NodeAddress[] {
  return raw.split(',').map((entry) => {
    const [host, portText] = entry.split(':')
    const port = Number(portText)
    if (host === undefined || host === '' || !Number.isInteger(port)) throw new Error(`invalid Cluster node '${entry}'`)
    return { host, port }
  })
}

function clusterClient(startupNodes: NodeAddress[], maxRedirections = 16): Cluster {
  const client = new Cluster(startupNodes, {
    scaleReads: 'master',
    maxRedirections,
    slotsRefreshTimeout: 2_000,
    redisOptions: { maxRetriesPerRequest: 2 },
    clusterRetryStrategy: (attempt) => (attempt <= 5 ? 20 : null),
  })
  client.on('error', () => {})
  return client
}

async function readMasters(startupNodes: NodeAddress[]): Promise<Master[]> {
  const seed = new Redis(startupNodes[0] as NodeAddress)
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

function uniquePrefix(label: string): string {
  return `w4r:${process.pid}:${Date.now().toString(36)}:${label}:`
}

async function open(backend: RedisRoomBackend, roomId: string, inc: string): Promise<RoomHead> {
  const result = await backend.compareExchangeHead(
    roomId,
    { expect: 'absent' },
    {
      head: { currentInc: inc, state: 'open', config: Buffer.from('w4r') },
    },
  )
  if (!('ok' in result) || !('head' in result)) throw new Error(`failed to open '${roomId}'`)
  return result.head
}

function accepted(result: Awaited<ReturnType<RedisRoomBackend['commitLane']>>): CommitAccepted {
  if (!('accepted' in result)) throw new Error('commit was unexpectedly stale')
  return result
}

function payload(mib: 3 | 8 | 16, variant: 'live' | 'replacement'): Buffer {
  const size = mib * 1024 * 1024
  const key = createHash('sha256').update(`telefunc-w4r:${variant}:key`).digest()
  const iv = createHash('sha256').update(`telefunc-w4r:${mib}:${variant}:iv`).digest().subarray(0, 16)
  return createCipheriv('aes-256-ctr', key, iv).update(Buffer.alloc(size))
}

function payloadBytes(size: number, label: string): Buffer {
  const key = createHash('sha256').update(`telefunc-w4r:${label}:key`).digest()
  const iv = createHash('sha256').update(`telefunc-w4r:${label}:iv`).digest().subarray(0, 16)
  return createCipheriv('aes-256-ctr', key, iv).update(Buffer.alloc(size))
}

function describeBytes(bytes: Uint8Array): { length: number; digest: string } {
  return { length: bytes.byteLength, digest: createHash('sha256').update(bytes).digest('hex') }
}

function describeRetained(retained: { payload: Uint8Array } | null): { length: number; digest: string } {
  if (retained === null) throw new Error('retained value is missing')
  return describeBytes(retained.payload)
}

async function listAll(backend: RedisRoomBackend, prefix: string): Promise<Array<{ roomId: string; incTag: string }>> {
  const entries: Array<{ roomId: string; incTag: string }> = []
  let cursor: string | undefined
  do {
    const page = await backend.directoryList(prefix, cursor)
    entries.push(...page.entries)
    cursor = page.cursor
  } while (cursor !== undefined)
  return entries
}

async function closeAndDrop(backend: RedisRoomBackend, roomId: string, inc: string): Promise<void> {
  const current = await backend.readHead(roomId)
  if (current === null) throw new Error(`missing head '${roomId}'`)
  const closing = await backend.compareExchangeHead(
    roomId,
    { expect: { rev: current.head.rev } },
    {
      head: {
        currentInc: inc,
        state: 'closing',
        config: current.head.config,
        closeLease: { id: `lease-${roomId}`, durationMs: 15_000 },
      },
    },
  )
  if (!('ok' in closing) || !('head' in closing) || closing.head.closeLease === undefined) {
    throw new Error(`failed to enter closing '${roomId}'`)
  }
  const closedEvent = accepted(
    await backend.commitLane(roomId, inc, CONTROL, Buffer.from('closed'), { closingLease: closing.head.closeLease.id }),
  )
  await closedEvent.delivery
  const closed = await backend.compareExchangeHead(
    roomId,
    { expect: { rev: closing.head.rev, closingLease: closing.head.closeLease.id } },
    { head: { currentInc: null, state: 'closed', config: closing.head.config }, ttlMs: 60_000 },
  )
  if (!('ok' in closed)) throw new Error(`failed to finalize '${roomId}'`)
  await backend.dropGeneration(roomId, inc)
  await backend.directoryDelete(roomId, inc)
}

async function scanNode(master: Master, pattern: string): Promise<string[]> {
  const keys: string[] = []
  let cursor = '0'
  do {
    const [next, page] = await master.client.scan(cursor, 'MATCH', pattern, 'COUNT', 500)
    cursor = next
    keys.push(...page)
  } while (cursor !== '0')
  return keys
}

async function clusterInfo(client: Redis): Promise<Record<string, string>> {
  const raw = String(await client.cluster('INFO'))
  return Object.fromEntries(
    raw
      .trim()
      .split('\n')
      .map((line) => line.trim().split(':', 2) as [string, string]),
  )
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error(`condition did not settle within ${timeoutMs} ms`)
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}
