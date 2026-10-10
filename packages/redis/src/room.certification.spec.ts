import { spawn } from 'node:child_process'
import { Cluster, Redis } from 'ioredis'
import { describe, expect, it, onTestFinished, vi } from 'vitest'
import { installRedis } from './index.js'
import { RedisBackend } from './backend.js'
import { encodeLaneKey } from 'telefunc/__internal'
import { disposeBackend, getRoomBackend } from '../../telefunc/dist/wire-protocol/backend/install.js'
import { Room } from '../../telefunc/dist/wire-protocol/room/server/statics.js'

// Room on a real Redis: REDIS_URL names a standalone server, REDIS_CLUSTER_NODES a Cluster's nodes; each one set runs.
type Target = { name: string; env: Record<string, string>; connect: () => Redis | Cluster }
const targets: Target[] = []
const { REDIS_URL, REDIS_CLUSTER_NODES } = process.env
if (REDIS_URL) targets.push({ name: 'Redis', env: { REDIS_URL }, connect: () => new Redis(REDIS_URL) })
if (REDIS_CLUSTER_NODES) {
  const nodes = REDIS_CLUSTER_NODES.split(',').map((entry) => {
    const separator = entry.lastIndexOf(':')
    return { host: entry.slice(0, separator), port: Number(entry.slice(separator + 1)) }
  })
  targets.push({ name: 'Redis Cluster', env: { NODES: JSON.stringify(nodes) }, connect: () => new Cluster(nodes) })
}
if (targets.length === 0) throw new Error('Room certification on Redis requires REDIS_URL or REDIS_CLUSTER_NODES')

describe.each(targets)('Room on $name', ({ env, connect }) => {
  it('commits an exchange while the cells it expects are as read, whatever else changed, and never conflicts on a count', async () => {
    const backend = new RedisBackend({ redis: ownClient(connect()), prefix: uniquePrefix('cells') })
    const opened = await backend.compareExchangeHead(
      'cells',
      { form: 'absent' },
      { head: { currentInc: 'inc', state: 'open', config: bytes('config') } },
    )
    expect('head' in opened).toBe(true)
    const exchange = (cx: Parameters<RedisBackend['compareExchangeCells']>[2]) =>
      backend.compareExchangeCells('cells', 'inc', cx)
    const created = await exchange({
      expect: [{ key: 'a', version: null }],
      mutations: [{ key: 'a', bytes: bytes('a1') }],
      counts: [{ key: 'n', delta: 0, create: true }],
    })
    expect(created).toEqual({ committed: new Map([['n', { value: 0, changedAt: expect.any(Number) }]]) })
    const read = await backend.readCells('cells', 'inc', { keys: ['a'] })
    if ('staleInc' in read) throw new Error('the room is gone')
    const a = read.versions.get('a')!
    // Another writer: a cell this exchange didn't read, and the counter.
    await exchange({ expect: [{ key: 'b', version: null }], mutations: [{ key: 'b', bytes: bytes('b1') }] })
    expect(await exchange({ expect: [], mutations: [], counts: [{ key: 'n', delta: 1 }] })).toEqual({
      committed: new Map([['n', { value: 1, changedAt: expect.any(Number) }]]),
    })
    const counted = await exchange({
      expect: [{ key: 'a', version: a }],
      mutations: [{ key: 'a', bytes: bytes('a2') }],
      counts: [{ key: 'n', delta: 2 }],
    })
    expect(counted).toEqual({ committed: new Map([['n', { value: 3, changedAt: expect.any(Number) }]]) })
    const n = typeof counted === 'object' ? counted.committed.get('n') : undefined
    // `a` was written since that read.
    expect(
      await exchange({ expect: [{ key: 'a', version: a }], mutations: [], counts: [{ key: 'n', delta: 5 }] }),
    ).toBe('conflict')
    expect(
      await exchange({
        expect: [],
        mutations: [],
        counts: [
          { key: 'n', delta: 0 },
          { key: 'missing', delta: 1 },
        ],
      }),
    ).toEqual({ committed: new Map([['n', n]]) })
    const after = await backend.readCells('cells', 'inc', { keys: ['a', 'n', 'missing'] })
    if ('staleInc' in after) throw new Error('the room is gone')
    expect([...after.cells].map(([key, value]) => [key, Buffer.from(value).toString()])).toEqual([
      ['a', 'a2'],
      ['n', JSON.stringify(n)],
    ])
  })

  it('deletes every cell under a cleared prefix before its mutations, only when the exchange commits', async () => {
    const backend = new RedisBackend({ redis: ownClient(connect()), prefix: uniquePrefix('clear') })
    await backend.compareExchangeHead(
      'clear',
      { form: 'absent' },
      { head: { currentInc: 'inc', state: 'open', config: bytes('config') } },
    )
    const exchange = (cx: Parameters<RedisBackend['compareExchangeCells']>[2]) =>
      backend.compareExchangeCells('clear', 'inc', cx)
    const set = (key: string) => ({ key, bytes: bytes(key) })
    await exchange({ expect: [], mutations: [set('p:a'), set('p:b'), set('p:\u{10ffff}'), set('pq'), set('q')] })
    expect(await exchange({ expect: [{ key: 'q', version: null }], clear: ['p:'], mutations: [] })).toBe('conflict')
    expect(await exchange({ expect: [], clear: ['p:'], mutations: [set('p:b')] })).toEqual({ committed: new Map() })
    const read = await backend.readCells('clear', 'inc', { prefix: '' })
    if ('staleInc' in read) throw new Error('the room is gone')
    expect([...read.cells.keys()].sort()).toEqual(['p:b', 'pq', 'q'])
  })

  it("delivers each of a member's binary commits to its every-track subscription with its own track's seq before the commit's delivery settles", async () => {
    installRedis(ownClient(connect()), { prefix: uniquePrefix('every-track') })
    onTestFinished(() => disposeBackend())
    const backend = getRoomBackend()
    await backend.compareExchangeHead(
      'every-track',
      { form: 'absent' },
      { head: { currentInc: 'inc', state: 'open', config: bytes('config') } },
    )
    const received: string[] = []
    const everyTrack = backend.subscribeLane(
      'every-track',
      'inc',
      { kind: 'binary-all', member: 'm' },
      (payload, info) => void received.push(`${Buffer.from(payload).toString()}@${info.seq}`),
    )
    onTestFinished(() => everyTrack.unsubscribe())
    await everyTrack.ready
    const seen: string[][] = []
    for (const [member, track, text] of [
      ['m', 'camera', 'camera-1'],
      ['m', 'mic', 'mic-1'],
      ['other', 'camera', 'other'],
      ['m', 'camera', 'camera-2'],
    ] as const) {
      const commit = await backend.commitLane('every-track', 'inc', { kind: 'binary', member, track }, bytes(text))
      if (!('accepted' in commit)) throw new Error('the room is gone')
      await commit.delivery
      seen.push([...received])
    }
    expect(seen).toEqual([
      ['camera-1@1'],
      ['camera-1@1', 'mic-1@1'],
      ['camera-1@1', 'mic-1@1'],
      ['camera-1@1', 'mic-1@1', 'camera-2@2'],
    ])
  })

  it("lists one member's retained binary lanes from that member's own set, which leaves the generation's keys once empty", async () => {
    const backend = new RedisBackend({ redis: ownClient(connect()), prefix: uniquePrefix('retained-of') })
    await backend.compareExchangeHead(
      'retained-of',
      { form: 'absent' },
      { head: { currentInc: 'inc', state: 'open', config: bytes('config') } },
    )
    const binary = (member: string, track: string) => ({ kind: 'binary', member, track }) as const
    for (const lane of [binary('m', 'camera'), binary('m', ''), binary('other', ''), { kind: 'semantic' } as const])
      await backend.commitLane('retained-of', 'inc', lane, bytes('frame'), { retain: true })
    const listed = async () => (await backend.listRetained('retained-of', 'inc', { member: 'm' })).map(encodeLaneKey)
    const generationKeys = () =>
      (backend as unknown as { _generationKeys(roomId: string, inc: string): Promise<string[]> })._generationKeys(
        'retained-of',
        'inc',
      )
    expect((await listed()).sort()).toEqual(['binary:m:', 'binary:m:camera'])
    await backend.deleteRetained('retained-of', 'inc', binary('m', 'camera'))
    expect(await listed()).toEqual(['binary:m:'])
    const before = await generationKeys()
    await backend.deleteRetained('retained-of', 'inc', binary('m', ''))
    expect(await listed()).toEqual([])
    // The retained frame and the member's set leave the generation's keys together.
    expect(before.length - (await generationKeys()).length).toBe(2)
  })

  it('admits a thousand simultaneous joins from four servers, counting each as a hold, while roster reads never fail', async () => {
    const prefix = uniquePrefix('join-burst')
    const redis = ownClient(connect())
    installRedis(redis, { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('join-burst')
    const startAt = Date.now() + 3_000
    let joining = true
    const joined = Promise.all(
      [0, 1, 2, 3].map(() => joinFrom({ ...env, PREFIX: prefix, ROOM: room.id, COUNT: '250', START_AT: `${startAt}` })),
    ).finally(() => (joining = false))
    await new Promise((resolve) => setTimeout(resolve, startAt - Date.now()))
    const sizes: number[] = []
    while (joining) sizes.push((await Room.getParticipants(room.id)).length)
    expect((await joined).flat()).toEqual([])
    // Nobody leaves, so each read lists at least what the one before it did.
    expect(sizes).toEqual([...sizes].sort((a, b) => a - b))
    expect(await Room.getParticipants(room.id)).toHaveLength(1_000)
    const backend = new RedisBackend({ redis, prefix })
    const inc = (await backend.readHead(room.id))!.currentInc!
    const read = await backend.readCells(room.id, inc, { keys: ['holds'] })
    if ('staleInc' in read) throw new Error('the room is gone')
    expect(JSON.parse(Buffer.from(read.cells.get('holds')!).toString())).toMatchObject({ value: 1_000 })
  })
  it('lists room meta keys in the order its writes gave them: Room.setMeta() its own, Room.setAttributes() after the keys already there', async () => {
    installRedis(ownClient(connect()), { prefix: uniquePrefix('meta-order') })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('meta-order', { meta: { b: 1, a: 1 } })
    const seen: string[][] = []
    const see = async () => seen.push(Object.keys((await Room.get(room.id)).meta))
    await Room.setAttributes(room.id, { c: 1, a: 2 })
    await see()
    await Room.setAttributes(room.id, { aa: 1, b: undefined, a: 3 })
    await Room.setAttributes(room.id, { b: 2 })
    await see()
    await Room.setMeta(room.id, { z: 1, y: 1, x: 1 })
    await see()
    await Room.setAttributes(room.id, { y: 2, w: 1 })
    await see()
    expect(seen).toEqual([
      ['b', 'a', 'c'],
      ['a', 'c', 'aa', 'b'],
      ['z', 'y', 'x'],
      ['z', 'y', 'x', 'w'],
    ])
  })

  it("counts a member who joins before the new room's first cell write, closing it only after they leave", async () => {
    installRedis(ownClient(connect()), { prefix: uniquePrefix('join-before-hold') })
    onTestFinished(() => disposeBackend())
    const exchange = RedisBackend.prototype.compareExchangeCells
    let joined: ReturnType<typeof Room.join> | undefined
    const spy = vi.spyOn(RedisBackend.prototype, 'compareExchangeCells').mockImplementation(async function (
      this: RedisBackend,
      roomId,
      inc,
      cx,
    ) {
      if (joined === undefined) {
        joined = Room.join(roomId)
        await joined
      }
      return await exchange.call(this, roomId, inc, cx)
    })
    onTestFinished(() => spy.mockRestore())
    const room = await Room.create('join-before-hold', { emptyTimeout: 500, departureTimeout: 2_000 })
    const member = await joined!
    await sleep(2_000)
    expect((await Room.getParticipants(room.id)).map(({ id }) => id)).toEqual([member.id])
    await member.leave()
    await sleep(1_000)
    expect(await Room.getParticipants(room.id)).toEqual([])
    await vi.waitFor(() => expect(Room.get(room.id)).rejects.toThrow('Room not found'), { timeout: 5_000 })
  })
})

/** A server that joins the room COUNT times at START_AT, and reports each join's failure. */
function joinFrom(env: Record<string, string>): Promise<string[]> {
  const server = spawn(
    process.execPath,
    [
      '--input-type=module',
      '-e',
      `import { Cluster, Redis } from 'ioredis'
import { Room } from 'telefunc'
import { installRedis } from ${JSON.stringify(new URL('../dist/index.js', import.meta.url).href)}
const { REDIS_URL, NODES, PREFIX, ROOM, COUNT, START_AT } = process.env
installRedis(NODES ? new Cluster(JSON.parse(NODES)) : new Redis(REDIS_URL), { prefix: PREFIX })
const room = await Room.get(ROOM)
await new Promise((resolve) => setTimeout(resolve, Number(START_AT) - Date.now()))
const joins = Array.from({ length: Number(COUNT) }, (_, i) => room.join({ identity: 'user-' + (i % 50) }))
const outcomes = await Promise.allSettled(joins)
const failures = outcomes.flatMap((outcome) => (outcome.status === 'rejected' ? [String(outcome.reason)] : []))
process.stdout.write(JSON.stringify(failures))
process.exit(0)`,
    ],
    { cwd: new URL('..', import.meta.url), env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'inherit'] },
  )
  let output = ''
  server.stdout.on('data', (chunk) => (output += chunk))
  return new Promise((resolve, reject) =>
    server.on('exit', (code) => (code === 0 ? resolve(JSON.parse(output)) : reject(new Error(`exit ${code}`)))),
  )
}

function ownClient<Client extends Redis | Cluster>(client: Client): Client {
  onTestFinished(async () => void (await client.quit().catch(() => client.disconnect())))
  return client
}

const uniquePrefix = (label: string): string => `redis-room-ci:${process.pid}:${Date.now().toString(36)}:${label}:`

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value)
}
