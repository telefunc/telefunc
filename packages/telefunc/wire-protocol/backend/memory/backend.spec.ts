import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { disposeBackend, getRoomBackend, installBackend } from '../install.js'
import type { CellsCx, LaneId } from '../room/contract.js'
import { MemoryBackend, MemoryBackendState } from './backend.js'
import { Room } from '../../room/server/statics.js'
const encoder = new TextEncoder()
const decoder = new TextDecoder()
const semanticLane = { kind: 'semantic' } as const satisfies LaneId
let driver: MemoryBackend
let memoryState: MemoryBackendState
beforeEach(async () => {
  await disposeBackend()
  memoryState = new MemoryBackendState()
  driver = new MemoryBackend({ state: memoryState })
  installBackend(() => driver)
})
afterEach(async () => {
  vi.useRealTimers()
  vi.restoreAllMocks()
  await disposeBackend()
})
describe('memory backend behind the supervised consumer', () => {
  it('covers head/cell/lane/directory/drop postconditions through the supervised consumer', async () => {
    const backend = getRoomBackend()
    const created = await backend.compareExchangeHead(
      'spi',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    if (!('head' in created)) throw new Error('head create failed')
    const absent = [{ key: 'member', version: null }]
    expect(
      await backend.compareExchangeCells('spi', 'inc-1', {
        expect: absent,
        mutations: [{ key: 'member', bytes: encoder.encode('Alice') }],
      }),
    ).toEqual({ committed: new Map() })
    expect(await backend.compareExchangeCells('spi', 'inc-1', { expect: absent, mutations: [] })).toBe('conflict')
    const received: string[] = []
    const subscription = backend.subscribeLane(
      'spi',
      'inc-1',
      semanticLane,
      (payload) => void received.push(decoder.decode(payload)),
    )
    await subscription.ready
    const commit = await backend.commitLane('spi', 'inc-1', semanticLane, encoder.encode('one'), {
      retain: true,
      requiredCellKeys: ['member'],
    })
    if (!('accepted' in commit)) throw new Error('lane commit fenced unexpectedly')
    await commit.delivery
    expect({ seq: commit.seq, receivers: commit.receivers, received }).toEqual({
      seq: 1,
      receivers: 1,
      received: ['one'],
    })
    expect(decoder.decode((await backend.readRetained('spi', 'inc-1', semanticLane))!.payload)).toBe('one')
    const currentCells = await backend.readCells('spi', 'inc-1', { keys: ['member'] })
    if ('staleInc' in currentCells) throw new Error('cell fence generation vanished')
    expect(
      await backend.compareExchangeCells('spi', 'inc-1', {
        expect: [{ key: 'member', version: currentCells.versions.get('member')! }],
        mutations: [{ key: 'member', bytes: null }],
      }),
    ).toEqual({ committed: new Map() })
    expect(
      await backend.commitLane('spi', 'inc-1', semanticLane, encoder.encode('fenced'), {
        requiredCellKeys: ['member'],
      }),
    ).toEqual({ stale: 'cell', key: 'member' })
    expect(subscription.state()).toBe('ready')
    const closing = await backend.compareExchangeHead(
      'spi',
      { form: 'rev', rev: created.head.rev },
      {
        head: {
          state: 'closing',
          currentInc: 'inc-1',
          config: created.head.config,
          closeLease: { id: 'lease-1', durationMs: 1_000 },
        },
      },
    )
    if (!('head' in closing) || closing.head.closeLease === undefined) {
      throw new Error('head close failed')
    }
    const closed = await backend.compareExchangeHead(
      'spi',
      { form: 'finalize', rev: closing.head.rev, lease: closing.head.closeLease.id },
      { head: { state: 'closed', currentInc: null, config: closing.head.config }, ttlMs: 60_000 },
    )
    if (!('head' in closed)) throw new Error('head finalize failed')
    const reopened = await backend.compareExchangeHead(
      'spi',
      { form: 'rev', rev: closed.head.rev },
      { head: { state: 'open', currentInc: 'inc-2', config: closed.head.config } },
    )
    expect(reopened).toMatchObject({ head: { state: 'open', currentInc: 'inc-2' } })
    expect(await backend.commitLane('spi', 'inc-1', semanticLane, encoder.encode('stale'))).toEqual({
      stale: 'incarnation',
    })
    await backend.dropGeneration('spi', 'inc-1')
    expect([...memoryState.rooms.get('spi')!.gens.keys()]).toEqual(['inc-2'])
    await backend.directoryPut('spi', 'inc-1', Infinity, 0)
    expect((await backend.directoryList('s')).entries).toEqual([{ roomId: 'spi', incTag: 'inc-1' }])
    await backend.directoryDelete('spi', 'wrong')
    expect((await backend.directoryList('s')).entries).toHaveLength(1)
    await backend.directoryDelete('spi', 'inc-1')
    expect((await backend.directoryList('s')).entries).toEqual([])
  })
  it('commits an exchange while the cells it expects are as read, whatever else changed, and never conflicts on a count', async () => {
    vi.useFakeTimers({ now: 1_000 })
    const backend = getRoomBackend()
    await backend.compareExchangeHead(
      'cells',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc', config: encoder.encode('config') } },
    )
    const exchange = (cx: CellsCx) => backend.compareExchangeCells('cells', 'inc', cx)
    expect(
      await exchange({
        expect: [{ key: 'a', version: null }],
        mutations: [{ key: 'a', bytes: encoder.encode('a1') }],
        counts: [{ key: 'n', delta: 0, create: true }],
      }),
    ).toEqual({ committed: new Map([['n', { value: 0, changedAt: 1_000 }]]) })
    const read = await backend.readCells('cells', 'inc', { keys: ['a'] })
    if ('staleInc' in read) throw new Error('the room is gone')
    const a = read.versions.get('a')!
    // Another writer: a cell this exchange didn't read, and the counter.
    vi.setSystemTime(2_000)
    await exchange({ expect: [{ key: 'b', version: null }], mutations: [{ key: 'b', bytes: encoder.encode('b1') }] })
    expect(await exchange({ expect: [], mutations: [], counts: [{ key: 'n', delta: 1 }] })).toEqual({
      committed: new Map([['n', { value: 1, changedAt: 2_000 }]]),
    })
    vi.setSystemTime(3_000)
    expect(
      await exchange({
        expect: [{ key: 'a', version: a }],
        mutations: [{ key: 'a', bytes: encoder.encode('a2') }],
        counts: [{ key: 'n', delta: 2 }],
      }),
    ).toEqual({ committed: new Map([['n', { value: 3, changedAt: 3_000 }]]) })
    // `a` was written since that read.
    expect(
      await exchange({ expect: [{ key: 'a', version: a }], mutations: [], counts: [{ key: 'n', delta: 5 }] }),
    ).toBe('conflict')
    vi.setSystemTime(4_000)
    expect(
      await exchange({
        expect: [],
        mutations: [],
        counts: [
          { key: 'n', delta: 0 },
          { key: 'missing', delta: 1 },
        ],
      }),
    ).toEqual({ committed: new Map([['n', { value: 3, changedAt: 3_000 }]]) })
    const after = await backend.readCells('cells', 'inc', { keys: ['a', 'n', 'missing'] })
    if ('staleInc' in after) throw new Error('the room is gone')
    expect([...after.cells].map(([key, value]) => [key, decoder.decode(value)])).toEqual([
      ['a', 'a2'],
      ['n', JSON.stringify({ value: 3, changedAt: 3_000 })],
    ])
  })
  it('advances order before delivery and preserves it across time and driver reconstruction', async () => {
    await disposeBackend()
    let now = 1
    vi.spyOn(Date, 'now').mockImplementation(() => now)
    driver = new MemoryBackend({ state: memoryState })
    installBackend(() => driver)
    const backend = getRoomBackend()
    const created = await backend.compareExchangeHead(
      'order-survivor',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    if (!('head' in created)) throw new Error('head create failed')
    let nestedMark: { seq: number; timestamp: number } | undefined
    const subscription = backend.subscribeLane('order-survivor', 'inc-1', semanticLane, async (payload) => {
      if (decoder.decode(payload) !== 'outer') return
      now = 2
      const nested = await backend.commitLane('order-survivor', 'inc-1', semanticLane, encoder.encode('inner'))
      if (!('accepted' in nested)) throw new Error('nested lane commit fenced unexpectedly')
      nestedMark = { seq: nested.seq, timestamp: nested.timestamp }
    })
    await subscription.ready
    const outer = await backend.commitLane('order-survivor', 'inc-1', semanticLane, encoder.encode('outer'))
    if (!('accepted' in outer)) throw new Error('outer lane commit fenced unexpectedly')
    await outer.delivery
    expect([{ seq: outer.seq, timestamp: outer.timestamp }, nestedMark]).toEqual([
      { seq: 1, timestamp: 1 },
      { seq: 2, timestamp: 2 },
    ])
    await subscription.unsubscribe()
    now = 3
    const reconstructedDriver = new MemoryBackend({ state: memoryState })
    const reconstructed = await reconstructedDriver.commitLane(
      'order-survivor',
      'inc-1',
      semanticLane,
      encoder.encode('reconstructed'),
    )
    expect(reconstructed).toMatchObject({ accepted: true, seq: 3, timestamp: 3 })
  })
  it("delivers each of a member's binary commits to its every-track lane with its own track's seq, and counts that lane as a receiver", async () => {
    const backend = getRoomBackend()
    await backend.compareExchangeHead(
      'every-track',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    const received: string[] = []
    const everyTrack = backend.subscribeLane(
      'every-track',
      'inc-1',
      { kind: 'binary-all', member: 'm' },
      (payload, info) => void received.push(`${decoder.decode(payload)}@${info.seq}`),
    )
    await everyTrack.ready
    const receivers: number[] = []
    for (const [member, track, text] of [
      ['m', 'camera', 'camera-1'],
      ['m', 'mic', 'mic-1'],
      ['other', 'camera', 'other'],
      ['m', 'camera', 'camera-2'],
      ['m', '', 'default-1'],
    ] as const) {
      const commit = await backend.commitLane(
        'every-track',
        'inc-1',
        { kind: 'binary', member, track },
        encoder.encode(text),
      )
      if (!('accepted' in commit)) throw new Error('lane commit fenced unexpectedly')
      await commit.delivery
      receivers.push(commit.receivers!)
    }
    expect({ received, receivers }).toEqual({
      received: ['camera-1@1', 'mic-1@1', 'camera-2@2', 'default-1@1'],
      receivers: [1, 1, 0, 1, 1],
    })
    await everyTrack.unsubscribe()
  })
  it("delivers each member's binary commits on a track to that track's every-member lane with its own lane's seq, and counts that lane as a receiver", async () => {
    const backend = getRoomBackend()
    await backend.compareExchangeHead(
      'every-member',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    const received: string[] = []
    const everyMember = backend.subscribeLane(
      'every-member',
      'inc-1',
      { kind: 'binary-track', track: 'mic' },
      (payload, info) => void received.push(`${decoder.decode(payload)}@${info.seq}`),
    )
    await everyMember.ready
    const receivers: number[] = []
    for (const [member, track, text] of [
      ['a', 'mic', 'a-1'],
      ['b', 'mic', 'b-1'],
      ['a', 'camera', 'camera'],
      ['a', 'mic', 'a-2'],
      ['b', '', 'default'],
    ] as const) {
      const commit = await backend.commitLane(
        'every-member',
        'inc-1',
        { kind: 'binary', member, track },
        encoder.encode(text),
      )
      if (!('accepted' in commit)) throw new Error('lane commit fenced unexpectedly')
      await commit.delivery
      receivers.push(commit.receivers!)
    }
    expect({ received, receivers }).toEqual({
      received: ['a-1@1', 'b-1@1', 'a-2@2'],
      receivers: [1, 1, 0, 1, 0],
    })
    await everyMember.unsubscribe()
  })
  it("lists one member's retained binary lanes from its own index, not by scanning the room's", async () => {
    const backend = getRoomBackend()
    await backend.compareExchangeHead(
      'retained-of',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    const binary = (member: string, track: string) => ({ kind: 'binary', member, track }) as const
    for (const lane of [
      binary('m', 'camera'),
      binary('m', ''),
      binary('m:1', 'camera'),
      binary('other', ''),
      semanticLane,
    ])
      await backend.commitLane('retained-of', 'inc-1', lane, encoder.encode('frame'), { retain: true })
    const scan = vi.spyOn(memoryState.rooms.get('retained-of')!.gens.get('inc-1')!.retained, 'values')
    const listed = () => backend.listRetained('retained-of', 'inc-1', { member: 'm' })
    expect(await listed()).toEqual([binary('m', 'camera'), binary('m', '')])
    await backend.deleteRetained('retained-of', 'inc-1', binary('m', 'camera'))
    expect(await listed()).toEqual([binary('m', '')])
    await backend.deleteRetained('retained-of', 'inc-1', binary('m', ''))
    expect(await listed()).toEqual([])
    expect(scan).not.toHaveBeenCalled()
  })
  it("lists one track's retained binary lanes across members from its own index, until they are deleted or the generation drops", async () => {
    const backend = getRoomBackend()
    await backend.compareExchangeHead(
      'retained-of-track',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    const binary = (member: string, track: string) => ({ kind: 'binary', member, track }) as const
    for (let i = 0; i < 1000; i++)
      await backend.commitLane('retained-of-track', 'inc-1', binary(`m${i}`, 'mic'), encoder.encode('frame'), {
        retain: true,
      })
    for (const lane of [binary('a', 'camera'), binary('b', 'camera'), binary('a', 'camera:1'), semanticLane])
      await backend.commitLane('retained-of-track', 'inc-1', lane, encoder.encode('frame'), { retain: true })
    const retained = memoryState.rooms.get('retained-of-track')!.gens.get('inc-1')!.retained
    const scan = vi.spyOn(retained, 'values')
    const reads = vi.spyOn(retained, 'get')
    const listed = () => backend.listRetained('retained-of-track', 'inc-1', { track: 'camera' })
    expect(await listed()).toEqual([binary('a', 'camera'), binary('b', 'camera')])
    expect(reads).toHaveBeenCalledTimes(2)
    // A member's leave deletes its lanes one by one.
    await backend.deleteRetained('retained-of-track', 'inc-1', binary('a', 'camera'))
    expect(await listed()).toEqual([binary('b', 'camera')])
    expect(await backend.listRetained('retained-of-track', 'inc-1', { track: 'camera:1' })).toEqual([
      binary('a', 'camera:1'),
    ])
    // Retaining again on a lane that holds a retained frame keeps one entry per index.
    await backend.commitLane('retained-of-track', 'inc-1', binary('b', 'camera'), encoder.encode('newer'), {
      retain: true,
    })
    await backend.deleteRetained('retained-of-track', 'inc-1', binary('b', 'camera'))
    expect(await listed()).toEqual([])
    await backend.commitLane('retained-of-track', 'inc-1', binary('a', 'camera'), encoder.encode('again'), {
      retain: true,
    })
    expect(await listed()).toEqual([binary('a', 'camera')])
    expect(scan).not.toHaveBeenCalled()
    await backend.dropGeneration('retained-of-track', 'inc-1')
    expect(await listed()).toEqual([])
  })
  it('does not retain or expose mutable lane aliases', async () => {
    await driver.compareExchangeHead(
      'retained-lane-alias',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    const lane = { kind: 'binary', member: 'member', track: 'original' } as LaneId
    await driver.commitLane('retained-lane-alias', 'inc-1', lane, new Uint8Array([1]), { retain: true })
    if (lane.kind !== 'binary') throw new Error('expected binary lane')
    lane.track = 'mutated-ingress'
    const listed = await driver.listRetained('retained-lane-alias', 'inc-1')
    expect(listed).toEqual([{ kind: 'binary', member: 'member', track: 'original' }])
    const returned = listed[0]
    if (returned?.kind !== 'binary') throw new Error('expected retained binary lane')
    returned.track = 'mutated-egress'
    await expect(driver.listRetained('retained-lane-alias', 'inc-1')).resolves.toEqual([
      { kind: 'binary', member: 'member', track: 'original' },
    ])
  })
  it("ends a lane the driver refuses at subscribe with the driver's reason", async () => {
    const subscription = getRoomBackend().subscribeLane('refused-room', 'refused-inc', semanticLane, () => {})
    await expect(subscription.ready).rejects.toThrow("has no open incarnation 'refused-inc'")
  })
  it('reads a prefix a page of at most 1,024 cells at a time: each cell there throughout exactly once, whatever is written or removed between pages', async () => {
    const backend = getRoomBackend()
    await backend.compareExchangeHead(
      'pages',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    const write = (keys: string[], bytes: Uint8Array | null) =>
      backend.compareExchangeCells('pages', 'inc-1', { expect: [], mutations: keys.map((key) => ({ key, bytes })) })
    const pages = async (between?: (page: number) => Promise<unknown>) => {
      const read: string[][] = []
      for await (const page of backend.readCellPages('pages', 'inc-1', 'p:')) {
        if ('staleInc' in page) throw new Error('cell read fenced unexpectedly')
        read.push([...page.cells.keys()])
        await between?.(read.length)
      }
      expect(read.every((page) => page.length <= 1_024)).toBe(true)
      return read.flat()
    }
    const keys = (count: number, from = 0) =>
      Array.from({ length: count }, (_, i) => `p:${String(from + i).padStart(5, '0')}`)
    expect(await pages()).toEqual([])
    await write([...keys(1_024), 'pq', 'q'], encoder.encode('v'))
    expect((await pages()).sort()).toEqual(keys(1_024))
    await write(keys(1_476, 1_024), encoder.encode('v'))
    const removed = ['p:00010', 'p:02400']
    const added = ['p:00005x', 'p:02450x']
    const read = await pages(async (page) => {
      if (page !== 1) return
      await write(removed, null)
      await write(added, encoder.encode('v'))
    })
    expect(new Set(read).size).toBe(read.length)
    const unchanged = keys(2_500).filter((key) => !removed.includes(key))
    expect(read.filter((key) => unchanged.includes(key)).sort()).toEqual(unchanged)
    expect(read.every((key) => [...unchanged, ...removed, ...added].includes(key))).toBe(true)
  })
  it('reads a prefix without scanning the other cells, and drops its index entries with its cells', async () => {
    const backend = getRoomBackend()
    await backend.compareExchangeHead(
      'prefixes',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    const keys = ['m:1', 'm:2', 'identity:a:1', 'identity:a:2', 'identity:ab:3', 'hold']
    const write = async (mutations: { key: string; bytes: Uint8Array | null }[]) => {
      expect(await backend.compareExchangeCells('prefixes', 'inc-1', { expect: [], mutations })).toEqual({
        committed: new Map(),
      })
    }
    const readKeys = async (prefix: string) => {
      const read = await backend.readCells('prefixes', 'inc-1', { prefix })
      if ('staleInc' in read) throw new Error('cell read fenced unexpectedly')
      return [...read.cells.keys()].sort()
    }
    await write(keys.map((key) => ({ key, bytes: encoder.encode(key) })))
    await write([{ key: 'm:1', bytes: encoder.encode('rewritten') }])
    const generation = memoryState.rooms.get('prefixes')!.gens.get('inc-1')!
    const scan = vi.spyOn(generation.cells, 'keys')
    expect(await readKeys('m:')).toEqual(['m:1', 'm:2'])
    expect(await readKeys('identity:a:')).toEqual(['identity:a:1', 'identity:a:2'])
    expect(await readKeys('identity:')).toEqual(['identity:a:1', 'identity:a:2', 'identity:ab:3'])
    expect(await readKeys('identity:a')).toEqual(['identity:a:1', 'identity:a:2', 'identity:ab:3'])
    expect(await readKeys('missing:')).toEqual([])
    expect(scan).not.toHaveBeenCalled()
    expect(await readKeys('h')).toEqual(['hold'])
    expect(await readKeys('')).toEqual([...keys].sort())
    await write([{ key: 'identity:a:1', bytes: null }])
    expect(await readKeys('identity:a:')).toEqual(['identity:a:2'])
    await write(keys.map((key) => ({ key, bytes: null })))
    expect(generation.cellsByPrefix.size).toBe(0)
  })
  it('lists the directory entries due at its own time, earliest first and a page at a time, until each is put again or deleted', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(1_000)
    const backend = getRoomBackend()
    for (let i = 0; i < 150; i++) await backend.directoryPut(`due-${i}`, 'inc', 0, -i)
    await backend.directoryPut('never', 'inc', Infinity, 0)
    await backend.directoryPut('later', 'inc', 2_000, 0)
    await backend.directoryPut('delayed', 'inc', 0, 1_500)
    const first = await backend.directoryDue()
    expect(first.map(({ roomId }) => roomId)).toEqual(Array.from({ length: 100 }, (_, i) => `due-${149 - i}`))
    expect(await backend.directoryDue()).toEqual(first)
    for (const { roomId, incTag } of first) await backend.directoryPut(roomId, incTag, 3_000, 0)
    await backend.directoryDelete('due-49', 'inc')
    const second = await backend.directoryDue()
    expect(second.map(({ roomId }) => roomId)).toEqual(Array.from({ length: 49 }, (_, i) => `due-${48 - i}`))
    expect(second[0]).toEqual({ roomId: 'due-48', incTag: 'inc' })
    vi.setSystemTime(2_000)
    expect((await backend.directoryDue()).map(({ roomId }) => roomId)).toContain('later')
    expect((await backend.directoryDue()).map(({ roomId }) => roomId)).not.toContain('delayed')
    vi.setSystemTime(2_500)
    expect((await backend.directoryDue()).map(({ roomId }) => roomId)).toContain('delayed')
    vi.setSystemTime(Number.MAX_SAFE_INTEGER)
    expect((await backend.directoryDue()).map(({ roomId }) => roomId)).not.toContain('never')
    vi.useRealTimers()
  })
  it("releases a closed room's memory record once its tombstone lapses", async () => {
    vi.useFakeTimers()
    await Room.create('released-record')
    await Room.close('released-record')
    expect(memoryState.rooms.has('released-record')).toBe(true)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(memoryState.rooms.has('released-record')).toBe(false)
    await expect(Room.create('released-record')).resolves.toMatchObject({ id: 'released-record' })
  })
})
