import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { disposeBackend, getRoomBackend, installBackend } from '../install.js'
import type { LaneId } from '../room/contract.js'
import { MemoryBackend, MemoryBackendState } from './backend.js'
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
    const cells = await backend.readCells('spi', 'inc-1', { keys: ['member'] })
    if (!('revision' in cells)) throw new Error('cell read fenced unexpectedly')
    expect(
      await backend.compareExchangeCells('spi', 'inc-1', cells.revision, [
        { key: 'member', bytes: encoder.encode('Alice') },
      ]),
    ).toBe('committed')
    expect(await backend.compareExchangeCells('spi', 'inc-1', cells.revision, [])).toBe('conflict')
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
      await backend.compareExchangeCells('spi', 'inc-1', currentCells.revision, [{ key: 'member', bytes: null }]),
    ).toBe('committed')
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
    await backend.directoryPut('spi', 'inc-1')
    expect((await backend.directoryList('s')).entries).toEqual([{ roomId: 'spi', incTag: 'inc-1' }])
    await backend.directoryDelete('spi', 'wrong')
    expect((await backend.directoryList('s')).entries).toHaveLength(1)
    await backend.directoryDelete('spi', 'inc-1')
    expect((await backend.directoryList('s')).entries).toEqual([])
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
  it('validates HeadNext shape before delegating to any raw driver', async () => {
    const backend = getRoomBackend()
    const opened = await backend.compareExchangeHead(
      'head-shape',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    if (!('head' in opened)) throw new Error('head create failed')
    const delegated = vi.spyOn(driver, 'compareExchangeHead')
    for (const durationMs of [0, Number.POSITIVE_INFINITY]) {
      await expect(
        backend.compareExchangeHead(
          'head-shape',
          { form: 'rev', rev: opened.head.rev },
          {
            head: {
              state: 'closing',
              currentInc: 'inc-1',
              config: opened.head.config,
              closeLease: { id: 'lease-1', durationMs },
            },
          },
        ),
      ).rejects.toThrow(`close lease durationMs ${durationMs} must be finite and positive`)
    }
    expect(delegated).not.toHaveBeenCalled()
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
})
