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

  it('reads a prefix a page of at most 1,024 cells at a time: each cell there throughout exactly once, whatever is written or removed between pages', async () => {
    const backend = new RedisBackend({ redis: ownClient(connect()), prefix: uniquePrefix('pages') })
    await backend.compareExchangeHead(
      'pages',
      { form: 'absent' },
      { head: { currentInc: 'inc', state: 'open', config: bytes('config') } },
    )
    const write = (keys: string[], value: Uint8Array | null) =>
      backend.compareExchangeCells('pages', 'inc', {
        expect: [],
        mutations: keys.map((key) => ({ key, bytes: value })),
      })
    const pages = async (between?: (page: number) => Promise<unknown>) => {
      const read: string[][] = []
      for await (const page of backend.readCellPages('pages', 'inc', 'p:')) {
        if ('staleInc' in page) throw new Error('the room is gone')
        read.push([...page.cells.keys()])
        await between?.(read.length)
      }
      expect(read.every((page) => page.length <= 1_024)).toBe(true)
      return read.flat()
    }
    const keys = (count: number, from = 0) =>
      Array.from({ length: count }, (_, i) => `p:${String(from + i).padStart(5, '0')}`)
    expect(await pages()).toEqual([])
    await write([...keys(1_024), 'pq', 'q'], bytes('v'))
    expect((await pages()).sort()).toEqual(keys(1_024))
    await write(keys(1_476, 1_024), bytes('v'))
    const removed = ['p:00010', 'p:02400']
    const added = ['p:00005x', 'p:02450x']
    const read = await pages(async (page) => {
      if (page !== 1) return
      await write(removed, null)
      await write(added, bytes('v'))
    })
    expect(new Set(read).size).toBe(read.length)
    const unchanged = keys(2_500).filter((key) => !removed.includes(key))
    expect(read.filter((key) => unchanged.includes(key)).sort()).toEqual(unchanged)
    expect(read.every((key) => [...unchanged, ...removed, ...added].includes(key))).toBe(true)
  })

  it('reads the roster of 100,000 members in a heartbeat tick with no Redis command over 20 ms and the event loop turning', async () => {
    const prefix = uniquePrefix('roster-pages')
    const redis = ownClient(connect())
    installRedis(redis, { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('roster-pages')
    const backend = new RedisBackend({ redis, prefix })
    const inc = (await backend.readHead(room.id))!.currentInc!
    const record = bytes(JSON.stringify({ meta: {}, joinedAt: Date.now(), session: 'seeded', metaSeq: 0 }))
    for (let from = 0; from < 100_000; from += 5_000) {
      const mutations = Array.from({ length: 5_000 }, (_, i) => ({ key: `m:member-${from + i}`, bytes: record }))
      await backend.compareExchangeCells(room.id, inc, { expect: [], mutations })
    }
    const nodes = redis instanceof Cluster ? redis.nodes('master') : [redis]
    await Promise.all(nodes.map((node) => node.slowlog('RESET')))
    const tick = async () => {
      let last = performance.now()
      let longest = 0
      const timer = setInterval(() => {
        const now = performance.now()
        longest = Math.max(longest, now - last)
        last = now
      }, 1)
      await (
        room as unknown as { _replica: { sync: { _heartbeatTick(): Promise<void> } } }
      )._replica.sync._heartbeatTick()
      clearInterval(timer)
      return Math.max(longest, performance.now() - last)
    }
    const gaps = [await tick(), await tick()]
    expect(room.count).toBe(100_000)
    // Each entry: id, time, microseconds, then the command and its arguments.
    const logged = (await Promise.all(nodes.map((node) => node.slowlog('GET', 1_000)))).flat() as [
      number,
      number,
      number,
      string[],
    ][]
    const ours = logged.filter(([, , , args]) => args.some((arg) => String(arg).includes(prefix)))
    expect(ours.map(([, , micros]) => micros).filter((micros) => micros > 20_000)).toEqual([])
    // The shorter of two ticks, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(...gaps)).toBeLessThanOrEqual(50)
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

  it("delivers each member's binary commits on a track to its every-member subscription with its own lane's seq before the commit's delivery settles", async () => {
    installRedis(ownClient(connect()), { prefix: uniquePrefix('every-member') })
    onTestFinished(() => disposeBackend())
    const backend = getRoomBackend()
    await backend.compareExchangeHead(
      'every-member',
      { form: 'absent' },
      { head: { currentInc: 'inc', state: 'open', config: bytes('config') } },
    )
    const received: string[] = []
    const everyMember = backend.subscribeLane(
      'every-member',
      'inc',
      { kind: 'binary-track', track: 'mic' },
      (payload, info) => void received.push(`${Buffer.from(payload).toString()}@${info.seq}`),
    )
    onTestFinished(() => everyMember.unsubscribe())
    await everyMember.ready
    const seen: string[][] = []
    for (const [member, track, text] of [
      ['a', 'mic', 'a-1'],
      ['b', 'mic', 'b-1'],
      ['a', 'camera', 'camera'],
      ['a', 'mic', 'a-2'],
    ] as const) {
      const commit = await backend.commitLane('every-member', 'inc', { kind: 'binary', member, track }, bytes(text))
      if (!('accepted' in commit)) throw new Error('the room is gone')
      await commit.delivery
      seen.push([...received])
    }
    expect(seen).toEqual([['a-1@1'], ['a-1@1', 'b-1@1'], ['a-1@1', 'b-1@1'], ['a-1@1', 'b-1@1', 'a-2@2']])
  })

  it("gives a room-wide subscriber that appears mid-stream a track's retained frame, and every frame after it", async () => {
    const prefix = uniquePrefix('room-wide-mid-stream')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const authority = await Room.create('room-wide-mid-stream')
    onTestFinished(() => Room.close(authority.id))
    const retainer = await authority.join()
    const streamer = await authority.join()
    await retainer.publishBinary(new Uint8Array([0]), { track: 'mic', retain: true })
    let published = 0
    let streaming = true
    const stream = (async () => {
      while (streaming) await streamer.publishBinary(new Uint8Array([++published % 256]), { track: 'mic' })
    })()
    const observer = await Room.get(authority.id)
    const retained: number[] = []
    const streamed: number[] = []
    observer.subscribeBinary((data, _info, from) => (from.id === retainer.id ? retained : streamed).push(data[0]!), {
      track: 'mic',
    })
    await vi.waitFor(() => expect(retained).toEqual([0]), { timeout: 10_000 })
    const after = published
    await vi.waitFor(() => expect(published - after).toBeGreaterThanOrEqual(300), { timeout: 10_000 })
    streaming = false
    await stream
    await vi.waitFor(() => expect(streamed.at(-1)).toBe(published % 256), { timeout: 10_000 })
    // From the first streamed frame on, each frame once, in order, through every frame published after the retained one.
    const first = published - streamed.length + 1
    expect(first).toBeLessThanOrEqual(after + 1)
    expect(streamed).toEqual(Array.from({ length: streamed.length }, (_, i) => (first + i) % 256))
  }, 30_000)

  it("delivers a member joining while another server fetches the tracks named of every member by member its first frame after the member's demand turns on", async () => {
    const prefix = uniquePrefix('by-member-late')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('by-member-late')
    onTestFinished(() => Room.close(room.id))
    const members = await Promise.all(Array.from({ length: 10 }, () => room.join()))
    const wanted = members.map((member) => {
      const tracks = new Set<string | null>()
      member.onDemand((track, on) => (on ? tracks.add(track) : tracks.delete(track)))
      return tracks
    })
    // 17 tracks of 10 members: the viewer fetches them on each member's every-track lane.
    const viewer = serverFrom(
      { ...env, PREFIX: prefix, ROOM: room.id },
      `const room = await Room.get(process.env.ROOM)
for (let i = 0; i < 17; i++) room.subscribeBinary((data, info) => write(info.track + ':' + data[0]), { track: 't' + i })
await read()
// It reads the next join 200 ms after its control event arrives, as a loaded server does.
const replica = room._replica
const onControl = replica._onControl.bind(replica)
replica._onControl = (...args) => void setTimeout(() => onControl(...args), 200)
write('slow')
await read()`,
    )
    await vi.waitFor(() => expect(wanted.every((tracks) => tracks.size === 17)).toBe(true), { timeout: 10_000 })
    viewer.send('slow')
    expect(JSON.parse(await viewer.line(0))).toBe('slow')
    const joiner = await room.join()
    joiner.onDemand((track, on) => void (on && track === 't0' && joiner.publishBinary(new Uint8Array([1]), { track })))
    const first = await Promise.race([viewer.line(1).then((line) => JSON.parse(line)), sleep(5_000).then(() => null)])
    expect(first).toBe('t0:1')
  }, 30_000)

  it('reads no demand on a server that only views the room, while the server owning its members takes it', async () => {
    const prefix = uniquePrefix('demand-viewer')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('demand-viewer')
    const member = await room.join()
    onTestFinished(() => member.leave())
    const tracks = new Set<string | null>()
    member.onDemand((track, wanted) => (wanted ? tracks.add(track) : tracks.delete(track)))
    // The owning server hears a track of every member too, so it tells demand of its own.
    onTestFinished(room.subscribeBinary(() => {}, { track: 'screen' }))
    const viewer = demandViewerFrom({ ...env, PREFIX: prefix, ROOM: room.id })
    await viewer.subscribed
    await vi.waitFor(() => expect([...tracks].sort()).toEqual(['mic', 'screen']), { timeout: 10_000 })
    await (room as unknown as { _replica: { _renewDemand(): Promise<void> } })._replica._renewDemand()
    expect(await viewer.parsedAfterRenewal()).toEqual({ parsed: 0 })
  }, 30_000)
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

  it('keeps no lane order key for any of 100 members that received a DM, published on a track and left', async () => {
    const prefix = uniquePrefix('lane-order-churn')
    const redis = ownClient(connect())
    installRedis(redis, { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('lane-order-churn')
    const sender = await room.join()
    onTestFinished(() => sender.leave())
    for (let i = 0; i < 100; i++) {
      const member = await room.join()
      await sender.send(member.id, 'hi')
      await member.publishBinary(new Uint8Array([1]), { track: 'camera' })
      await member.leave()
    }
    const backend = new RedisBackend({ redis, prefix })
    const inc = (await backend.readHead(room.id))!.currentInc!
    const generationKeys = await (
      backend as unknown as { _generationKeys(roomId: string, inc: string): Promise<string[]> }
    )._generationKeys(room.id, inc)
    const orderKeys = generationKeys.filter((key) => /:g:[^:]+:o[mt]?:/.test(key))
    expect(orderKeys).toEqual([expect.stringMatching(/:o:control$/)])
  })

  it("reads where each lane a subscription carries stands as it goes live, and forgets a member's lanes once it leaves", async () => {
    const prefix = uniquePrefix('lane-starts')
    const redis = ownClient(connect())
    installRedis(redis, { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('lane-starts')
    const a = await room.join()
    const b = await room.join()
    onTestFinished(() => b.leave())
    for (let i = 0; i < 3; i++) await a.publishBinary(new Uint8Array([i]), { track: 'mic' })
    await a.publishBinary(new Uint8Array([0]), { track: 'camera' })
    await b.publishBinary(new Uint8Array([0]), { track: 'mic' })
    const backend = new RedisBackend({ redis, prefix })
    const inc = (await backend.readHead(room.id))!.currentInc!
    type Starts = Map<string, { seq: number }> | null
    type Reads = { _readLaneStarts(source: { roomId: string; inc: string; lane: unknown }): Promise<Starts> }
    const seqs = async (lane: unknown) => {
      const starts = await (backend as unknown as Reads)._readLaneStarts({ roomId: room.id, inc, lane })
      return Object.fromEntries([...starts!].map(([key, { seq }]) => [key, seq]))
    }
    const binary = (member: string, track: string) => encodeLaneKey({ kind: 'binary', member, track })
    expect(await seqs({ kind: 'binary-track', track: 'mic' })).toEqual({
      [binary(a.id, 'mic')]: 3,
      [binary(b.id, 'mic')]: 1,
    })
    expect(await seqs({ kind: 'binary-all', member: a.id })).toEqual({
      [binary(a.id, 'mic')]: 3,
      [binary(a.id, 'camera')]: 1,
    })
    expect(await seqs({ kind: 'binary', member: a.id, track: 'mic' })).toEqual({ '': 3 })
    expect(await seqs({ kind: 'semantic' })).toEqual({})
    await a.leave()
    await vi.waitFor(async () =>
      expect(await seqs({ kind: 'binary-track', track: 'mic' })).toEqual({ [binary(b.id, 'mic')]: 1 }),
    )
    expect(await seqs({ kind: 'binary-track', track: 'camera' })).toEqual({})
  })

  it('lists the directory entries due, earliest first and a page at a time, until each is put again or deleted', async () => {
    const backend = new RedisBackend({ redis: ownClient(connect()), prefix: uniquePrefix('directory-due') })
    for (let i = 0; i < 150; i++) await backend.directoryPut(`due-${i}`, 'inc', 1_000 - i)
    await backend.directoryPut('never', 'inc', Infinity)
    const first = await backend.directoryDue(1_000)
    expect(first.map(({ roomId }) => roomId)).toEqual(Array.from({ length: 100 }, (_, i) => `due-${149 - i}`))
    for (const { roomId, incTag } of first) await backend.directoryPut(roomId, incTag, 3_000)
    await backend.directoryDelete('due-49', 'inc')
    const second = await backend.directoryDue(1_000)
    expect(second.map(({ roomId }) => roomId)).toEqual(Array.from({ length: 49 }, (_, i) => `due-${48 - i}`))
    expect(second[0]).toEqual({ roomId: 'due-48', incTag: 'inc' })
    expect((await backend.directoryDue(Number.MAX_SAFE_INTEGER)).map(({ roomId }) => roomId)).not.toContain('never')
  })

  it("lists one track's retained binary lanes across members from that track's own set, until they are deleted or the generation drops", async () => {
    const backend = new RedisBackend({ redis: ownClient(connect()), prefix: uniquePrefix('retained-of-track') })
    await backend.compareExchangeHead(
      'retained-of-track',
      { form: 'absent' },
      { head: { currentInc: 'inc', state: 'open', config: bytes('config') } },
    )
    const binary = (member: string, track: string) => ({ kind: 'binary', member, track }) as const
    for (let i = 0; i < 100; i++)
      await backend.commitLane('retained-of-track', 'inc', binary(`m${i}`, 'mic'), bytes('frame'), { retain: true })
    for (const lane of [
      binary('a', 'camera'),
      binary('b', 'camera'),
      binary('a', 'camera:1'),
      { kind: 'semantic' } as const,
    ])
      await backend.commitLane('retained-of-track', 'inc', lane, bytes('frame'), { retain: true })
    const internals = backend as unknown as { _generationKeys(roomId: string, inc: string): Promise<string[]> }
    const generationKeys = () => internals._generationKeys('retained-of-track', 'inc')
    const scan = vi.spyOn(internals, '_generationKeys')
    const listed = async (track: string) =>
      (await backend.listRetained('retained-of-track', 'inc', { track })).map(encodeLaneKey).sort()
    expect(await listed('camera')).toEqual(['binary:a:camera', 'binary:b:camera'])
    expect(await listed('camera:1')).toEqual(['binary:a:camera%3A1'])
    expect(await listed('mic')).toHaveLength(100)
    expect(scan).not.toHaveBeenCalled()
    // A member's leave deletes its lanes one by one.
    await backend.deleteRetained('retained-of-track', 'inc', binary('a', 'camera'))
    expect(await listed('camera')).toEqual(['binary:b:camera'])
    // Retaining again on a lane that holds a retained frame keeps one entry per index.
    await backend.commitLane('retained-of-track', 'inc', binary('b', 'camera'), bytes('newer'), { retain: true })
    const before = await generationKeys()
    await backend.deleteRetained('retained-of-track', 'inc', binary('b', 'camera'))
    expect(await listed('camera')).toEqual([])
    // The retained frame, its member's set and its track's set leave the generation's keys together.
    expect(before.length - (await generationKeys()).length).toBe(3)
    await backend.commitLane('retained-of-track', 'inc', binary('a', 'camera'), bytes('again'), { retain: true })
    expect(await listed('camera')).toEqual(['binary:a:camera'])
    await backend.dropGeneration('retained-of-track', 'inc')
    expect(await listed('camera:1')).toEqual([])
    expect(await listed('mic')).toEqual([])
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
  it('repairs a join a server never got from the next control frame it gets, well within a heartbeat', async () => {
    const prefix = uniquePrefix('join-gap')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('join-gap')
    const observer = await Room.get(room.id)
    // Ended before the backend is, so nothing of this view reaches for a backend after it.
    onTestFinished(observer.onJoin(() => {}))
    await observer.getParticipants()
    type Replica = { _onControl(serialized: string, ...rest: unknown[]): void }
    const replica = (observer as unknown as { _replica: Replica })._replica
    const receive = replica._onControl.bind(replica)
    let missed = 0
    replica._onControl = (serialized, ...rest) => {
      if (missed === 0 && serialized.includes('"__r":"join"')) return void missed++
      receive(serialized, ...rest)
    }
    expect(await joinFrom({ ...env, PREFIX: prefix, ROOM: room.id, COUNT: '2', START_AT: `${Date.now()}` })).toEqual([])
    await vi.waitFor(async () => expect(await observer.getParticipants()).toHaveLength(2), { timeout: 5_000 })
    expect(missed).toBe(1)
  })
  it("resolves an acknowledged send with its handler's reply as soon as a frame after the lost reply reaches its sender", async () => {
    const prefix = uniquePrefix('dm-ack-lost')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('dm-ack-lost')
    const sender = await room.join()
    const recipient = await room.join()
    onTestFinished(() => recipient.leave())
    recipient.listen((data) => `handled:${String(data)}`)
    type Replica = { _onDm(serialized: string, ...rest: unknown[]): void }
    const replica = (room as unknown as { _replica: Replica })._replica
    const receive = replica._onDm.bind(replica)
    let lost = false
    replica._onDm = (serialized, ...rest) => {
      if (!lost && serialized.includes('"__r":"dm-ack"')) return void (lost = true)
      receive(serialized, ...rest)
    }
    const response = sender.send(recipient.id, 'move', { ack: true }).then((receipt) => receipt.response, String)
    await vi.waitFor(() => expect(lost).toBe(true), { timeout: 5_000 })
    await recipient.send(sender.id, 'after')
    const timedOut = new Promise((resolve) => setTimeout(() => resolve('not within 5 s'), 5_000).unref())
    const settled = await Promise.race([response, timedOut])
    await sender.leave()
    expect(settled).toBe('handled:move')
  })
  it('reads, merges into and replaces a room meta of 100,000 keys with no Redis command over 20 ms', async () => {
    const prefix = uniquePrefix('meta-pages')
    const redis = ownClient(connect())
    installRedis(redis, { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('meta-pages')
    for (let from = 0; from < 100_000; from += 10_000)
      await Room.setAttributes(
        room.id,
        Object.fromEntries(Array.from({ length: 10_000 }, (_, i) => [`k${from + i}`, i])),
      )
    const nodes = redis instanceof Cluster ? redis.nodes('master') : [redis]
    await Promise.all(nodes.map((node) => node.slowlog('RESET')))
    expect(Object.keys((await Room.get(room.id)).meta)).toHaveLength(100_000)
    await Room.setAttributes(room.id, { k0: 'merged' })
    await Room.setMeta(room.id, { only: true })
    expect((await Room.get(room.id)).meta).toEqual({ only: true })
    // Each entry: id, time, microseconds, then the command and its arguments.
    const logged = (await Promise.all(nodes.map((node) => node.slowlog('GET', 1_000)))).flat() as [
      number,
      number,
      number,
      string[],
    ][]
    const ours = logged.filter(([, , , args]) => args.some((arg) => String(arg).includes(prefix)))
    expect(ours.map(([, , micros]) => micros).filter((micros) => micros > 20_000)).toEqual([])
  })

  it('merges one key into a room meta of 100,000 keys with the event loop turning, a view applying it, and no Redis command over 20 ms', async () => {
    const prefix = uniquePrefix('meta-merge')
    const redis = ownClient(connect())
    installRedis(redis, { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('meta-merge')
    for (let from = 0; from < 100_000; from += 10_000)
      await Room.setAttributes(
        room.id,
        Object.fromEntries(Array.from({ length: 10_000 }, (_, i) => [`k${from + i}`, i])),
      )
    const observer = await Room.get(room.id)
    // Ended before the backend is, so nothing of this view reaches for a backend after it.
    onTestFinished(observer.onUpdate(() => {}))
    const merge = async (value: number) => {
      let last = performance.now()
      let longest = 0
      const timer = setInterval(() => {
        const now = performance.now()
        longest = Math.max(longest, now - last)
        last = now
      }, 1)
      await Room.setAttributes(room.id, { k0: value })
      await vi.waitFor(() => expect(observer.meta.k0).toBe(value))
      clearInterval(timer)
      return Math.max(longest, performance.now() - last)
    }
    await merge(-1)
    const nodes = redis instanceof Cluster ? redis.nodes('master') : [redis]
    await Promise.all(nodes.map((node) => node.slowlog('RESET')))
    // The shorter of two merges, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await merge(1), await merge(2))).toBeLessThanOrEqual(50)
    const logged = (await Promise.all(nodes.map((node) => node.slowlog('GET', 1_000)))).flat() as [
      number,
      number,
      number,
      string[],
    ][]
    const ours = logged.filter(([, , , args]) => args.some((arg) => String(arg).includes(prefix)))
    expect(ours.map(([, , micros]) => micros).filter((micros) => micros > 20_000)).toEqual([])
  }, 60_000)
  it('ends the views of two servers on the stored meta, its key order and its seq while both merge into and replace it at once', async () => {
    const prefix = uniquePrefix('meta-converge')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('meta-converge', { meta: { topic: 'start' } })
    const view = await Room.get(room.id)
    onTestFinished(view.onUpdate(() => {}))
    const startAt = Date.now() + 2_000
    const other = metaWriterFrom({ ...env, PREFIX: prefix, ROOM: room.id, SIDE: 'b', START_AT: `${startAt}` })
    await new Promise((resolve) => setTimeout(resolve, startAt - Date.now()))
    await writeMetaBurst(Room, room.id, 'a')
    await other.written
    const seqOf = (room: object) => (room as { _state: { model: { metaSeq: number } } })._state.model.metaSeq
    const expected = await storedFrom({ ...env, PREFIX: prefix, ROOM: room.id })
    await vi.waitFor(() =>
      expect({ keys: Object.keys(view.meta), meta: view.meta, seq: seqOf(view) }).toEqual(expected),
    )
    expect(await other.viewAt(expected.seq)).toEqual(expected)
  }, 30_000)

  it("merges one key into a member's meta of 100,000 keys with the event loop turning, a view applying it, and no Redis command over 20 ms", async () => {
    const prefix = uniquePrefix('member-meta-merge')
    const redis = ownClient(connect())
    installRedis(redis, { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('member-meta-merge')
    const member = await room.join()
    onTestFinished(() => member.leave())
    for (let from = 0; from < 100_000; from += 10_000)
      await member.setAttributes(Object.fromEntries(Array.from({ length: 10_000 }, (_, i) => [`k${from + i}`, i])))
    const observer = await Room.get(room.id)
    // Ended before the backend is, so nothing of this view reaches for a backend after it.
    onTestFinished(observer.onJoin(() => {}))
    const remote = (await observer.getParticipant(member.id))!
    let changes = 0
    onTestFinished(observer.onChange(() => changes++))
    // Timed until the view applied the write. Reading the whole meta builds it, the reader's own cost, so it is read once
    // at the end, and its garbage lands in no timed merge.
    const merge = async (value: number) => {
      const before = changes
      let last = performance.now()
      let longest = 0
      const timer = setInterval(() => {
        const now = performance.now()
        longest = Math.max(longest, now - last)
        last = now
      }, 1)
      await member.setAttributes({ k0: value })
      await vi.waitFor(() => expect(changes).toBe(before + 1))
      clearInterval(timer)
      return Math.max(longest, performance.now() - last)
    }
    await merge(-1)
    const nodes = redis instanceof Cluster ? redis.nodes('master') : [redis]
    await Promise.all(nodes.map((node) => node.slowlog('RESET')))
    // The shorter of two merges, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await merge(1), await merge(2))).toBeLessThanOrEqual(50)
    const logged = (await Promise.all(nodes.map((node) => node.slowlog('GET', 1_000)))).flat() as [
      number,
      number,
      number,
      string[],
    ][]
    const ours = logged.filter(([, , , args]) => args.some((arg) => String(arg).includes(prefix)))
    expect(ours.map(([, , micros]) => micros).filter((micros) => micros > 20_000)).toEqual([])
    expect(remote.meta.k0).toBe(2)
  }, 60_000)
  it("ends a second server's view on a member's stored meta, its key order and its seq while its server merges into and replaces it at once", async () => {
    const prefix = uniquePrefix('member-meta-converge')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('member-meta-converge')
    const view = await Room.get(room.id)
    onTestFinished(view.onParticipantUpdate(() => {}))
    await view.getParticipants()
    const startAt = Date.now() + 2_000
    const owner = memberMetaWriterFrom({ ...env, PREFIX: prefix, ROOM: room.id, START_AT: `${startAt}` })
    const id = await owner.joined
    await owner.written
    const accepted = (room: object) =>
      (
        room as { _state: { model: { acceptedMeta(id: string): { meta: Record<string, unknown>; seq: number } } } }
      )._state.model.acceptedMeta(id)
    const expected = await storedFrom({ ...env, PREFIX: prefix, ROOM: room.id, MEMBER: id })
    expect(expected.seq).toBe(60)
    await vi.waitFor(() => {
      const { meta, seq } = accepted(view)
      expect({ keys: Object.keys(meta), meta, seq }).toEqual(expected)
    })
    expect(await owner.viewAt(expected.seq)).toEqual(expected)
  }, 30_000)

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
  it('lets each member of a server that crashed holding 10,000 across two rooms leave once, within three minutes', async () => {
    const prefix = uniquePrefix('session-crash')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const ids = ['crash-a', 'crash-b']
    const leaves = new Map<string, number>()
    let lastLeaveAt = 0
    for (const id of ids) {
      const room = await Room.create(id)
      onTestFinished(() => Room.close(id))
      room.onLeave(() => {})
      await room.getParticipants()
      const inc = (room as unknown as { _inc: string })._inc
      const control = getRoomBackend().subscribeLane(id, inc, { kind: 'control' }, (payload) => {
        const event = JSON.parse(Buffer.from(payload as Uint8Array).toString()) as { __r: string; id?: string }
        if (event.__r !== 'leave') return
        leaves.set(event.id!, (leaves.get(event.id!) ?? 0) + 1)
        lastLeaveAt = Date.now()
      })
      onTestFinished(() => control.unsubscribe())
      await control.ready
    }
    // A second server watches both rooms, its heartbeats as able to claim the crashed server's session as this one's.
    const watcher = serverFrom(
      { ...env, PREFIX: prefix, ROOMS: ids.join() },
      `for (const id of process.env.ROOMS.split(',')) {
  const room = await Room.get(id)
  room.onLeave(() => {})
  await room.getParticipants()
}
write('watching')
await read()`,
    )
    await watcher.line(0)
    const crashing = serverFrom(
      { ...env, PREFIX: prefix, ROOMS: ids.join() },
      `const joined = []
for (const id of process.env.ROOMS.split(',')) {
  const room = await Room.get(id)
  joined.push(...(await Promise.all(Array.from({ length: 5_000 }, () => room.join()))).map(({ id }) => id))
}
write(joined)
await read()`,
    )
    const joined = JSON.parse(await crashing.line(0)) as string[]
    crashing.kill()
    const crashedAt = Date.now()
    await vi.waitFor(() => expect(joined.filter((id) => !leaves.has(id))).toEqual([]), {
      timeout: 200_000,
      interval: 1_000,
    })
    expect([...new Set(leaves.values())]).toEqual([1])
    expect(lastLeaveAt - crashedAt).toBeLessThanOrEqual(180_000)
  }, 240_000)

  it('writes Redis once per server in an idle heartbeat, whatever its members', async () => {
    const prefix = uniquePrefix('idle-writes')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('idle-writes')
    onTestFinished(() => Room.close(room.id))
    await Promise.all(Array.from({ length: 1_000 }, () => room.join()))
    room.onLeave(() => {})
    const other = serverFrom(
      { ...env, PREFIX: prefix, ROOM: room.id },
      `const room = await Room.get(process.env.ROOM)
await Promise.all(Array.from({ length: 1_000 }, () => room.join()))
room.onLeave(() => {})
const writes = countWrites()
write('joined')
await read()
writes.length = 0
await room._replica.sync._heartbeatTick()
write(writes)
await read()`,
    )
    await other.line(0)
    const writes = countWrites()
    await (
      room as unknown as { _replica: { sync: { _heartbeatTick(): Promise<void> } } }
    )._replica.sync._heartbeatTick()
    other.send('tick')
    expect([writes, JSON.parse(await other.line(1))]).toEqual([['session'], ['session']])
  }, 60_000)

  it("evicts nobody when an app server's clock runs 120 seconds ahead of Redis's", async () => {
    const prefix = uniquePrefix('skewed-clock')
    installRedis(ownClient(connect()), { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('skewed-clock')
    onTestFinished(() => Room.close(room.id))
    const members = await Promise.all(Array.from({ length: 100 }, () => room.join()))
    const skewed = serverFrom(
      { ...env, PREFIX: prefix, ROOM: room.id },
      `const now = Date.now
Date.now = () => now() + 120_000
const room = await Room.get(process.env.ROOM)
const left = []
room.onLeave((member) => left.push(member.id))
await room.getParticipants()
await room._replica.sync._heartbeatTick()
write({ left, count: (await Room.getParticipants(process.env.ROOM)).length })
await read()`,
    )
    expect(JSON.parse(await skewed.line(0))).toEqual({ left: [], count: 100 })
    expect((await Room.getParticipants(room.id)).map(({ id }) => id).sort()).toEqual(members.map(({ id }) => id).sort())
  }, 60_000)
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

/** A server viewing ROOM that hears its `mic` of every member, then, asked, renews its demand and reports how many
 *  demand frames it parsed. */
function demandViewerFrom(env: Record<string, string>): {
  subscribed: Promise<void>
  parsedAfterRenewal: () => Promise<unknown>
} {
  const server = spawn(
    process.execPath,
    [
      '--input-type=module',
      '-e',
      `import { Cluster, Redis } from 'ioredis'
import { createInterface } from 'node:readline'
import { Room } from 'telefunc'
import { installRedis } from ${JSON.stringify(new URL('../dist/index.js', import.meta.url).href)}
const { REDIS_URL, NODES, PREFIX, ROOM } = process.env
installRedis(NODES ? new Cluster(JSON.parse(NODES)) : new Redis(REDIS_URL), { prefix: PREFIX })
const room = await Room.get(ROOM)
const replica = room._replica
let parsed = 0
for (const name of ['_onControl', '_onDemand']) {
  const receive = replica[name]?.bind(replica)
  if (receive) replica[name] = (serialized, ...rest) => {
    if (/"__r":"(want|covers|demand)"/.test(serialized)) parsed++
    return receive(serialized, ...rest)
  }
}
room.subscribeBinary(() => {}, { track: 'mic' })
process.stdout.write('subscribed\\n')
await createInterface({ input: process.stdin })[Symbol.asyncIterator]().next()
await replica._renewDemand()
await new Promise((resolve) => setTimeout(resolve, 500))
process.stdout.write(JSON.stringify({ parsed }) + '\\n')
process.exit(0)`,
    ],
    { cwd: new URL('..', import.meta.url), env: { ...process.env, ...env }, stdio: ['pipe', 'pipe', 'inherit'] },
  )
  const lines: string[] = []
  const waiting: Array<() => void> = []
  let buffered = ''
  server.stdout.on('data', (chunk) => {
    buffered += chunk
    const parts = buffered.split('\n')
    buffered = parts.pop()!
    lines.push(...parts)
    for (const wake of waiting.splice(0)) wake()
  })
  const line = async (index: number): Promise<string> => {
    while (lines.length <= index) await new Promise<void>((resolve) => waiting.push(resolve))
    return lines[index]!
  }
  return {
    subscribed: line(0).then(() => {}),
    parsedAfterRenewal: async () => {
      server.stdin.write('renew\n')
      return JSON.parse(await line(1))
    },
  }
}

/** A server that opens ROOM and reports the stored meta, its key order and its seq: the room's, or MEMBER's. */
function storedFrom(
  env: Record<string, string>,
): Promise<{ keys: string[]; meta: Record<string, unknown>; seq: number }> {
  const server = spawn(
    process.execPath,
    [
      '--input-type=module',
      '-e',
      `import { Cluster, Redis } from 'ioredis'
import { Room } from 'telefunc'
import { installRedis } from ${JSON.stringify(new URL('../dist/index.js', import.meta.url).href)}
const { REDIS_URL, NODES, PREFIX, ROOM, MEMBER } = process.env
installRedis(NODES ? new Cluster(JSON.parse(NODES)) : new Redis(REDIS_URL), { prefix: PREFIX })
const room = await Room.get(ROOM)
if (MEMBER) await room.getParticipants()
const { meta, seq } = MEMBER ? room._state.model.acceptedMeta(MEMBER) : { meta: room.meta, seq: room._state.model.metaSeq }
process.stdout.write(JSON.stringify({ keys: Object.keys(meta), meta, seq }))
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

/** Merges into and replaces a room meta at once, keys named by `side`. `Room` is a parameter, so a server process can
 *  run this function's source. */
async function writeMetaBurst(room: typeof Room, roomId: string, side: string): Promise<void> {
  await Promise.all(
    Array.from({ length: 60 }, (_, i) =>
      i % 10 === 9
        ? room.setMeta(roomId, { [`${side}r${i}`]: i, topic: side })
        : room.setAttributes(roomId, { [`${side}${i % 5}`]: i, [`${side}n${i}`]: i, [`${side}n${i - 2}`]: undefined }),
    ),
  )
}

/** Merges into and replaces a member's meta at once. A parameter of a server process's source, as `writeMetaBurst`. */
async function writeMemberMetaBurst(member: {
  setMeta(meta: object): Promise<void>
  setAttributes(attrs: object): Promise<void>
}): Promise<void> {
  await Promise.all(
    Array.from({ length: 60 }, (_, i) =>
      i % 10 === 9
        ? member.setMeta({ [`r${i}`]: i, topic: i })
        : member.setAttributes({ [`k${i % 5}`]: i, [`n${i}`]: i, [`n${i - 2}`]: undefined }),
    ),
  )
}

/** A server that joins ROOM and runs `writeMemberMetaBurst` on its member at START_AT, then reports its view of the
 *  member once it reaches a seq. */
function memberMetaWriterFrom(env: Record<string, string>): {
  joined: Promise<string>
  written: Promise<void>
  viewAt: (seq: number) => Promise<unknown>
} {
  const server = spawn(
    process.execPath,
    [
      '--input-type=module',
      '-e',
      `import { Cluster, Redis } from 'ioredis'
import { createInterface } from 'node:readline'
import { Room } from 'telefunc'
import { installRedis } from ${JSON.stringify(new URL('../dist/index.js', import.meta.url).href)}
const { REDIS_URL, NODES, PREFIX, ROOM, START_AT } = process.env
installRedis(NODES ? new Cluster(JSON.parse(NODES)) : new Redis(REDIS_URL), { prefix: PREFIX })
const room = await Room.get(ROOM)
const member = await room.join({ meta: { topic: 'start' } })
process.stdout.write(member.id + '\\n')
await new Promise((resolve) => setTimeout(resolve, Number(START_AT) - Date.now()))
await (${writeMemberMetaBurst.toString().replace(/^async function writeMemberMetaBurst/, 'async function')})(member)
process.stdout.write('written\\n')
const [line] = await createInterface({ input: process.stdin })[Symbol.asyncIterator]().next().then((r) => [r.value])
const seq = Number(line)
while (room._state.model.acceptedMeta(member.id).seq < seq) await new Promise((resolve) => setTimeout(resolve, 10))
const { meta } = room._state.model.acceptedMeta(member.id)
process.stdout.write(JSON.stringify({ keys: Object.keys(meta), meta, seq: room._state.model.acceptedMeta(member.id).seq }) + '\\n')
process.exit(0)`,
    ],
    { cwd: new URL('..', import.meta.url), env: { ...process.env, ...env }, stdio: ['pipe', 'pipe', 'inherit'] },
  )
  const lines: string[] = []
  const waiting: Array<() => void> = []
  let buffered = ''
  server.stdout.on('data', (chunk) => {
    buffered += chunk
    const parts = buffered.split('\n')
    buffered = parts.pop()!
    lines.push(...parts)
    for (const wake of waiting.splice(0)) wake()
  })
  const line = async (index: number): Promise<string> => {
    while (lines.length <= index) await new Promise<void>((resolve) => waiting.push(resolve))
    return lines[index]!
  }
  return {
    joined: line(0),
    written: line(1).then(() => {}),
    viewAt: async (seq) => {
      server.stdin.write(`${seq}\n`)
      return JSON.parse(await line(2))
    },
  }
}

/** A server viewing ROOM that runs `writeMetaBurst` at START_AT, then reports its view once it reaches a seq. */
function metaWriterFrom(env: Record<string, string>): {
  written: Promise<void>
  viewAt: (seq: number) => Promise<unknown>
} {
  const server = spawn(
    process.execPath,
    [
      '--input-type=module',
      '-e',
      `import { Cluster, Redis } from 'ioredis'
import { createInterface } from 'node:readline'
import { Room } from 'telefunc'
import { installRedis } from ${JSON.stringify(new URL('../dist/index.js', import.meta.url).href)}
const { REDIS_URL, NODES, PREFIX, ROOM, SIDE, START_AT } = process.env
installRedis(NODES ? new Cluster(JSON.parse(NODES)) : new Redis(REDIS_URL), { prefix: PREFIX })
const view = await Room.get(ROOM)
view.onUpdate(() => {})
await new Promise((resolve) => setTimeout(resolve, Number(START_AT) - Date.now()))
await (${writeMetaBurst.toString().replace(/^async function writeMetaBurst/, 'async function')})(Room, ROOM, SIDE)
process.stdout.write('written\\n')
const [line] = await createInterface({ input: process.stdin })[Symbol.asyncIterator]().next().then((r) => [r.value])
const seq = Number(line)
while (view._state.model.metaSeq < seq) await new Promise((resolve) => setTimeout(resolve, 10))
process.stdout.write(JSON.stringify({ keys: Object.keys(view.meta), meta: view.meta, seq: view._state.model.metaSeq }) + '\\n')
process.exit(0)`,
    ],
    { cwd: new URL('..', import.meta.url), env: { ...process.env, ...env }, stdio: ['pipe', 'pipe', 'inherit'] },
  )
  const lines: string[] = []
  const waiting: Array<() => void> = []
  let buffered = ''
  server.stdout.on('data', (chunk) => {
    buffered += chunk
    const parts = buffered.split('\n')
    buffered = parts.pop()!
    lines.push(...parts)
    for (const wake of waiting.splice(0)) wake()
  })
  const line = async (index: number): Promise<string> => {
    while (lines.length <= index) await new Promise<void>((resolve) => waiting.push(resolve))
    return lines[index]!
  }
  return {
    written: line(0).then(() => {}),
    viewAt: async (seq) => {
      server.stdin.write(`${seq}\n`)
      return JSON.parse(await line(1))
    },
  }
}

/** The cell writes of this process's Redis backends from now on, each as its first key's kind, or the method's name. */
function countWrites(): string[] {
  const writes: string[] = []
  for (const name of ['compareExchangeCells', 'compareExchangeHead', 'commitLane'] as const) {
    const write = RedisBackend.prototype[name] as (...args: unknown[]) => unknown
    vi.spyOn(RedisBackend.prototype, name).mockImplementation(function (this: RedisBackend, ...args: unknown[]) {
      const cx = args[2] as { mutations?: { key: string }[] }
      writes.push(name === 'compareExchangeCells' ? cx.mutations!.map(({ key }) => key.split(':')[0]).join() : name)
      return write.apply(this, args)
    } as never)
  }
  return writes
}

/** A server running `body` with Redis installed: it may `write(value)` a line to the spec and `await read()` one from
 *  it, and has `countWrites()`, as above. It ends with the spec's test, or at `kill()`, which stops it at once. */
function serverFrom(
  env: Record<string, string>,
  body: string,
): { line: (index: number) => Promise<string>; send: (text: string) => void; kill: () => void } {
  const server = spawn(
    process.execPath,
    [
      '--input-type=module',
      '-e',
      `import { Cluster, Redis } from 'ioredis'
import { createInterface } from 'node:readline'
import { Room } from 'telefunc'
import { installRedis } from ${JSON.stringify(new URL('../dist/index.js', import.meta.url).href)}
import { RedisBackend } from ${JSON.stringify(new URL('../dist/backend.js', import.meta.url).href)}
const { REDIS_URL, NODES, PREFIX } = process.env
installRedis(NODES ? new Cluster(JSON.parse(NODES)) : new Redis(REDIS_URL), { prefix: PREFIX })
const input = createInterface({ input: process.stdin })[Symbol.asyncIterator]()
const read = async () => (await input.next()).value
const write = (value) => process.stdout.write(JSON.stringify(value) + '\\n')
function countWrites() {
  const writes = []
  for (const name of ['compareExchangeCells', 'compareExchangeHead', 'commitLane']) {
    const original = RedisBackend.prototype[name]
    RedisBackend.prototype[name] = function (...args) {
      writes.push(name === 'compareExchangeCells' ? args[2].mutations.map(({ key }) => key.split(':')[0]).join() : name)
      return original.apply(this, args)
    }
  }
  return writes
}
${body}
process.exit(0)`,
    ],
    { cwd: new URL('..', import.meta.url), env: { ...process.env, ...env }, stdio: ['pipe', 'pipe', 'inherit'] },
  )
  onTestFinished(() => void server.kill('SIGKILL'))
  const lines: string[] = []
  const waiting: Array<() => void> = []
  let buffered = ''
  server.stdout.on('data', (chunk) => {
    buffered += chunk
    const parts = buffered.split('\n')
    buffered = parts.pop()!
    lines.push(...parts)
    for (const wake of waiting.splice(0)) wake()
  })
  const exited = new Promise<never>((_, reject) => server.on('exit', (code) => reject(new Error(`exit ${code}`))))
  exited.catch(() => {})
  return {
    line: async (index) => {
      while (lines.length <= index) await Promise.race([new Promise<void>((resolve) => waiting.push(resolve)), exited])
      return lines[index]!
    },
    send: (text) => void server.stdin.write(`${text}\n`),
    kill: () => void server.kill('SIGKILL'),
  }
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
