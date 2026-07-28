import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
import { IndexedPeer } from '../server/IndexedPeer.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { TAG, decode } from '../shared-ws.js'
import { ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS } from '../constants.js'
import { DEFAULT_TRACK, type RoomSnapshotMetadata } from './protocol.js'
import { ClientRoom } from './client.js'
import { Room, ServerRoom } from './server.js'
import { RoomStubChannel } from './stubs.js'
import type { ClientBroadcast } from '../client/channel.js'
import type { ChannelPublishInfo } from '../channel.js'
import { disposeBackend, getBackend, installBackend, setDefaultBackend } from '../backend/install.js'
import { HEAD_TRANSITIONS, assertHeadTransition } from '../backend/head-transitions.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import {
  SUBSCRIPTION_ESTABLISH_TIMEOUT_MS,
  SUBSCRIPTION_REPLAN_LIMIT,
  SubscriptionManager,
} from '../backend/subscriptions.js'
import type {
  BackendReceiver,
  BackendSubscriptionSource,
  LaneId,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionDriver,
  SubscriptionState,
} from '../backend/spi.js'
import { ORDERING_FRAME_LAYOUT, decodeOrderingFrame, encodeOrderingFrame } from '../ordering-frame.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const semanticLane = { kind: 'semantic' } as const satisfies LaneId
const allBinary = { everyMember: { all: true, tracks: [] }, members: {} }

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
  const report = vi.spyOn(console, 'error').mockImplementation(() => {})
  try {
    await disposeBackend()
    await settle()
  } finally {
    report.mockRestore()
  }
})

describe('Room public behavior', () => {
  it('creates, lists, updates, closes fully, and recreates a genuinely fresh domain', async () => {
    const room = (await Room.create('lifecycle', { meta: { topic: 'one' } })) as ServerRoom
    const firstInc = room._inc
    const me = await room.join({ meta: { name: 'Alice' } })
    const observer = await Room.get('lifecycle')
    let closed = 0
    let memberLeaves = 0
    observer.onClose(() => closed++)
    me.onLeave(() => memberLeaves++)

    await Room.setMeta('lifecycle', { topic: 'two' })
    expect(observer.meta).toEqual({ topic: 'two' })
    expect((await Room.list()).map(({ id }) => id)).toContain('lifecycle')

    await Room.close('lifecycle')
    expect(closed).toBe(1)
    expect(memberLeaves).toBe(1)
    expect((await driver.readHead('lifecycle'))?.head).toMatchObject({ state: 'closed', currentInc: null })
    expect(await driver.listGenerations('lifecycle')).toEqual([])
    expect((await driver.directoryList('lifecycle')).entries).toEqual([])
    await expect(room.join()).rejects.toThrow(/closed/i)
    await expect(Room.get('lifecycle')).rejects.toThrow('Room not found')

    const recreated = (await Room.create('lifecycle')) as ServerRoom
    expect(recreated._inc).not.toBe(firstInc)
    expect(await recreated.getParticipants()).toEqual([])
  })

  it('tears down a close observed by a separate Room runtime that cannot inherit the initiator hold', async () => {
    const authority = await Room.create('remote-close-teardown')
    authority.onAnnounce(() => {})
    await settle()

    // A fresh module graph has its own initiating-close registry and backend installation, like another
    // server process. It shares only the raw authority driver, so this observer receives the real close
    // emitted by Room.close() without being able to see the initiator's in-memory hold.
    vi.resetModules()
    const remoteInstall = await import('../backend/install.js')
    const remoteServer = await import('./server.js')
    const remoteBackend = remoteInstall.installBackend(() => driver)
    const unsubscribed: string[] = []
    const subscribeLane = remoteBackend.subscribeLane.bind(remoteBackend)
    const subscribe = vi.spyOn(remoteBackend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      const subscription = subscribeLane(roomId, inc, lane, receiver)
      return {
        ready: subscription.ready,
        state: () => subscription.state(),
        onStateChange: (callback) => subscription.onStateChange(callback),
        unsubscribe: async () => {
          unsubscribed.push(lane.kind)
          await subscription.unsubscribe()
        },
      }
    })
    try {
      expect(remoteServer.Room).not.toBe(Room)
      const observer = await remoteServer.Room.get('remote-close-teardown')
      observer.onAnnounce(() => {})
      await settle()

      await Room.close('remote-close-teardown')
      await settle()

      expect(observer.isClosed).toBe(true)
      expect(unsubscribed.sort()).toEqual(['control', 'semantic'])
    } finally {
      subscribe.mockRestore()
      await remoteInstall.disposeBackend()
    }
  })

  it('propagates presence/meta while hidden members stay addressable and admin removal carries its cause', async () => {
    const authority = await Room.create('presence')
    const observer = await Room.get('presence')
    const events: string[] = []
    observer.onJoin((member) => events.push(`join:${String(member.meta.name)}`))
    observer.onParticipantUpdate((member) => events.push(`update:${String(member.meta.name)}`))
    observer.onLeave((member) => events.push(`leave:${String(member.meta.name)}`))

    const hidden = await authority.join({ meta: { role: 'bot' }, hidden: true })
    const player = await authority.join({ meta: { name: 'Alice' }, identity: 'user-1' })
    await player.setAttributes({ name: 'Alicia', score: 1 })

    expect(observer.count).toBe(1)
    expect((await observer.getParticipants()).map((member) => member.id)).toEqual([player.id])
    expect((await observer.getParticipants({ hidden: true })).map((member) => member.id)).toEqual([hidden.id])
    expect(events).toEqual(['join:Alice', 'update:Alicia'])

    const causes: unknown[] = []
    player.onLeave((cause) => causes.push(cause))
    await Room.removeParticipant('presence', { identity: 'user-1', reason: 'moderated' })
    expect(causes).toEqual([{ type: 'removed', reason: 'moderated' }])
    expect(observer.count).toBe(0)
    expect(events.at(-1)).toBe('leave:Alicia')
    expect((await observer.getParticipants({ hidden: true })).map((member) => member.id)).toEqual([hidden.id])
  })

  it('keeps text self-delivery local and binary subscriptions selective across named tracks', async () => {
    const publisherRoom = await Room.create('media')
    const observer = await Room.get('media')
    const quiet = await publisherRoom.join({ meta: { name: 'Camera' }, selfDelivery: false })
    const localText: unknown[] = []
    const remoteText: unknown[] = []
    publisherRoom.subscribe((data) => localText.push(data))
    observer.subscribe((data) => remoteText.push(data))

    await quiet.publish('frame-ready')
    expect(localText).toEqual([])
    expect(remoteText).toEqual(['frame-ready'])

    await observer.getParticipants()
    const remote = (await observer.getParticipant(quiet.id))!
    const screen: Array<[number, unknown]> = []
    remote.subscribeBinary((bytes, info) => screen.push([bytes[0]!, info.meta]), { track: 'screen' })
    await quiet.publishBinary(new Uint8Array([1]))
    await quiet.publishBinary(new Uint8Array([2]), { track: 'screen', meta: { key: true } })
    expect(screen).toEqual([[2, { key: true }]])
  })

  it('keeps DMs private, supports acknowledgements, and preserves room-authored sends', async () => {
    const room = await Room.create('dm')
    const bot = await room.join({ meta: { role: 'bot' }, hidden: true })
    const player = await room.join({ meta: { name: 'Player' } })
    const roomText: unknown[] = []
    const inbox: unknown[] = []
    room.subscribe((data) => roomText.push(data))
    bot.listen((data, from) => {
      inbox.push([data, from?.id])
      return `handled:${String(data)}`
    })

    const ack = await player.send(bot.id, 'move', { ack: true })
    expect(ack.response).toBe('handled:move')
    expect(inbox).toEqual([['move', player.id]])
    expect(roomText).toEqual([])

    const fromRoom: unknown[] = []
    player.listen((data, from) => fromRoom.push([data, from]))
    await Room.send('dm', { id: player.id }, { notice: true })
    expect(fromRoom).toEqual([[{ notice: true }, null]])
  })

  it('applies before guards and after hooks around authoritative joins, publishes, and sends', async () => {
    await Room.create('guarded')
    const room = await Room.get('guarded')
    const after: string[] = []
    Room.guard(room, {
      onBeforeJoin: (member) => {
        if (member.meta.name === 'blocked') throw new Error('no entry')
      },
      onBeforePublish: (_from, data) => {
        if (data === 'blocked') throw new Error('no publish')
      },
      onBeforeSend: (_from, _to, data) => {
        if (data === 'blocked') throw new Error('no send')
      },
      onAfterJoin: (member) => void after.push(`join:${String(member.meta.name)}`),
      onAfterPublish: (_from, data) => void after.push(`publish:${String(data)}`),
      onAfterSend: (_from, _to, data) => void after.push(`send:${String(data)}`),
    })

    await expect(room.join({ meta: { name: 'blocked' } })).rejects.toThrow('no entry')
    const alice = await room.join({ meta: { name: 'Alice' } })
    const bob = await room.join({ meta: { name: 'Bob' } })
    const inbox: unknown[] = []
    bob.listen((data) => inbox.push(data))
    await expect(alice.publish('blocked')).rejects.toThrow('no publish')
    await alice.publish('ok')
    await expect(alice.send(bob.id, 'blocked')).rejects.toThrow('no send')
    await alice.send(bob.id, 'ok')

    expect(inbox).toEqual(['ok'])
    expect(after).toEqual(['join:Alice', 'join:Bob', 'publish:ok', 'send:ok'])
  })

  it('orders participant text and room announcements in one monotonic semantic domain', async () => {
    const room = await Room.create('semantic-order')
    const member = await room.join()
    const first = await member.publish('one')
    const second = await Room.announce('semantic-order', 'notice')
    const third = await member.publish('two')
    expect([first.seq, second.seq, third.seq]).toEqual([1, 2, 3])
    expect(first.timestamp).toBeLessThanOrEqual(second.timestamp)
    expect(second.timestamp).toBeLessThanOrEqual(third.timestamp)
  })

  it("retained owner cleanup is compare-delete, so a newer owner's racing frame survives", async () => {
    const room = (await Room.create('retained-owner')) as ServerRoom
    const departing = await room.join({ meta: { name: 'departing' } })
    const replacement = await room.join({ meta: { name: 'replacement' } })
    await departing.publish('old', { retain: true })

    const realDelete = driver.deleteRetained.bind(driver)
    let raced = false
    const deleting = vi.spyOn(driver, 'deleteRetained').mockImplementation(async (roomId, inc, lane, opts) => {
      if (!raced && opts?.ifSeq !== undefined) {
        raced = true
        await replacement.publish('new', { retain: true })
      }
      return realDelete(roomId, inc, lane, opts)
    })
    await Room.removeParticipant(room.id, { id: departing.id })
    deleting.mockRestore()

    const retained = await driver.readRetained(room.id, room._inc, semanticLane)
    expect(parse(decoder.decode(retained!.payload))).toMatchObject({ from: replacement.id, data: 'new' })
  })

  it('tail mode holds pre-attach text and flushes it in order on first client demand', async () => {
    await Room.create('tail')
    const source = await Room.get('tail')
    const member = await source.join()
    const tail = (await Room.get('tail', { tail: true })) as ServerRoom
    await member.publish('early')

    const { stub, peer } = serve(tail)
    await member.publish('held')
    expect(dataFrames(peer)).toEqual([])
    stub._onPeerBroadcastSubscribe(false)
    await settle()
    await member.publish('live')
    expect(dataFrames(peer)).toEqual(['early', 'held', 'live'])
  })

  it('onDemand reports named-track demand turning on and off', async () => {
    const room = await Room.create('demand')
    const camera = await room.join()
    const changes: Array<[string | null, boolean]> = []
    camera.onDemand((track, wanted) => changes.push([track, wanted]))
    const observer = await Room.get('demand')
    await observer.getParticipants()

    const unsubscribe = (await observer.getParticipant(camera.id))!.subscribeBinary(() => {}, { track: 'screen' })
    await delay(30)
    unsubscribe()
    await delay(30)
    expect(changes).toEqual([
      ['screen', true],
      ['screen', false],
    ])
  })

  it('keeps live and retained binary seq above 2^32 through server and public client decode', async () => {
    const live = await wideBinaryScenario('wide-live', false, 7)
    const retained = await wideBinaryScenario('wide-retained', true, 9)
    expect(live).toEqual({ receipt: 0x1_0000_0000, server: 0x1_0000_0000, client: 0x1_0000_0000 })
    expect(retained).toEqual({ receipt: 0x1_0000_0000, server: null, client: 0x1_0000_0000 })
  })

  it('keeps zero-configuration memory on the same supervised path as explicit drivers', async () => {
    await disposeBackend()
    const room = await Room.create('zero-config')
    const backend = getBackend()
    expect(backend).not.toBeInstanceOf(MemoryBackend)
    const member = await room.join()
    expect(await member.publish('works')).toMatchObject({ seq: 1 })
  })

  it('keeps snapshot references stable until a real state change', async () => {
    const room = await Room.create('snapshot')
    const first = room.snapshot()
    expect(room.snapshot()).toBe(first)
    let changes = 0
    room.onChange(() => changes++)
    await room.join({ meta: { name: 'Alice' } })
    expect(room.snapshot()).not.toBe(first)
    expect(changes).toBe(1)
  })
})

describe('memory Backend SPI contract', () => {
  it('covers head/cell/lane/directory/drop postconditions through the supervised consumer', async () => {
    const backend = getBackend()
    const created = await backend.compareExchangeHead(
      'spi',
      { expect: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    if (!('ok' in created) || !('head' in created)) throw new Error('head create failed')

    const cells = await backend.readCells('spi', 'inc-1', { keys: ['member'] })
    if (!('revision' in cells)) throw new Error('cell read fenced unexpectedly')
    expect(
      await backend.compareExchangeCells('spi', 'inc-1', cells.revision, [
        { key: 'member', set: { bytes: encoder.encode('Alice') } },
      ]),
    ).toBe('committed')
    expect(await backend.compareExchangeCells('spi', 'inc-1', cells.revision, [])).toBe('conflict')

    const received: string[] = []
    const subscription = backend.subscribeLane('spi', 'inc-1', semanticLane, (payload) =>
      received.push(decoder.decode(payload)),
    )
    await subscription.ready
    const commit = await backend.commitLane('spi', 'inc-1', semanticLane, encoder.encode('one'), { retain: true })
    if (!('accepted' in commit)) throw new Error('lane commit fenced unexpectedly')
    await commit.delivery
    expect({ seq: commit.seq, receivers: commit.receivers, received }).toEqual({
      seq: 1,
      receivers: 1,
      received: ['one'],
    })
    expect(decoder.decode((await backend.readRetained('spi', 'inc-1', semanticLane))!.payload)).toBe('one')

    await expect(backend.dropGeneration('spi', 'inc-1')).rejects.toThrow('refusing to drop the current')
    expect(subscription.state()).toBe('ready')

    const closing = await backend.compareExchangeHead(
      'spi',
      { expect: { rev: created.head.rev } },
      {
        head: {
          state: 'closing',
          currentInc: 'inc-1',
          config: created.head.config,
          closeLease: { id: 'lease-1', durationMs: 1_000 },
        },
      },
    )
    if (!('ok' in closing) || !('head' in closing) || closing.head.closeLease === undefined) {
      throw new Error('head close failed')
    }
    const closed = await backend.compareExchangeHead(
      'spi',
      { expect: { rev: closing.head.rev, closingLease: closing.head.closeLease.id } },
      { head: { state: 'closed', currentInc: null, config: closing.head.config }, ttlMs: 60_000 },
    )
    if (!('ok' in closed) || !('head' in closed)) throw new Error('head finalize failed')
    const reopened = await backend.compareExchangeHead(
      'spi',
      { expect: { rev: closed.head.rev } },
      { head: { state: 'open', currentInc: 'inc-2', config: closed.head.config } },
    )
    expect(reopened).toMatchObject({ ok: true, head: { state: 'open', currentInc: 'inc-2' } })
    expect(await backend.commitLane('spi', 'inc-1', semanticLane, encoder.encode('stale'))).toEqual({ stale: true })
    await backend.dropGeneration('spi', 'inc-1')
    expect(await backend.listGenerations('spi')).toEqual(['inc-2'])
    expect(subscription.state()).toBe('closed')

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
    driver = new MemoryBackend({ state: memoryState, authorityNow: () => now })
    setDefaultBackend(() => driver)
    const backend = getBackend()
    const created = await backend.compareExchangeHead(
      'order-survivor',
      { expect: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    if (!('ok' in created) || !('head' in created)) throw new Error('head create failed')

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
    const reconstructedDriver = new MemoryBackend({ state: memoryState, authorityNow: () => now })
    const reconstructed = await reconstructedDriver.commitLane(
      'order-survivor',
      'inc-1',
      semanticLane,
      encoder.encode('reconstructed'),
    )
    expect(reconstructed).toMatchObject({ accepted: true, seq: 3, timestamp: 3 })
    await reconstructedDriver.dispose()
  })

  it('interprets every legal head transition from the one exported data table', () => {
    const head = (state: 'open' | 'closing' | 'closed', inc: string | null, lease?: string) => ({
      rev: 'r1',
      state,
      currentInc: inc,
      config: new Uint8Array([1]),
      ...(lease === undefined ? {} : { closeLease: { id: lease, until: 10 } }),
    })
    const next = (state: 'open' | 'closing' | 'closed', inc: string | null, lease?: string) => ({
      head: {
        state,
        currentInc: inc,
        config: new Uint8Array([1]),
        ...(lease === undefined ? {} : { closeLease: { id: lease, durationMs: 1_000 } }),
      },
    })
    const cases = [
      [{ expect: 'absent' as const }, null, next('open', 'i1')],
      [{ expect: { rev: 'r1' } }, head('closed', null), next('open', 'i1')],
      [{ expect: { rev: 'r1' } }, head('open', 'i1'), next('open', 'i1')],
      [{ expect: { rev: 'r1' } }, head('open', 'i1'), next('closing', 'i1', 'l1')],
      [
        { expect: { rev: 'r1', closingLeaseExpired: true as const } },
        head('closing', 'i1', 'l1'),
        next('closing', 'i1', 'l2'),
      ],
      [{ expect: { rev: 'r1', closingLease: 'l1' } }, head('closing', 'i1', 'l1'), next('closed', null)],
    ] as const
    expect(HEAD_TRANSITIONS).toHaveLength(cases.length)
    for (const [cx, current, candidate] of cases) {
      expect(() => assertHeadTransition(cx, candidate, current, () => false)).not.toThrow()
    }
    let failure: unknown
    try {
      assertHeadTransition({ expect: { rev: 'r1' } }, next('closed', null), head('open', 'i1'), () => false)
    } catch (error) {
      failure = error
    }
    expect(failure).toBeInstanceOf(Error)
    expect(Object.getPrototypeOf(failure)).toBe(Error.prototype)
    expect(Object.keys(failure as object)).toEqual([])
    expect((failure as Error).message).toContain('not a legal head transition')
  })

  it('validates HeadNext shape before delegating to any raw driver', async () => {
    const backend = getBackend()
    const opened = await backend.compareExchangeHead(
      'head-shape',
      { expect: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    if (!('ok' in opened) || !('head' in opened)) throw new Error('head create failed')
    const delegated = vi.spyOn(driver, 'compareExchangeHead')

    await expect(
      backend.compareExchangeHead(
        'head-shape',
        { expect: { rev: opened.head.rev } },
        {
          head: {
            state: 'closing',
            currentInc: 'inc-1',
            config: opened.head.config,
            closeLease: { id: 'lease-1', durationMs: 999 },
          },
        },
      ),
    ).rejects.toThrow('close lease durationMs 999 outside [1000, 60000]')
    expect(delegated).not.toHaveBeenCalled()
  })

  it('publishes one immutable wide ordering layout and codec', () => {
    expect(ORDERING_FRAME_LAYOUT).toEqual({
      headerBytes: 16,
      wordBytes: 4,
      wordRange: 0x1_0000_0000,
      endianness: 'big',
      offsets: { seqHigh: 0, seqLow: 4, timestampHigh: 8, timestampLow: 12 },
    })
    const payload = new Uint8Array([1, 255])
    const info = { seq: 0x1_0000_0007, timestamp: 0x2_0000_0009 }
    expect(decodeOrderingFrame(encodeOrderingFrame(payload, info))).toEqual({ payload, info })
  })

  it('tracks raw-driver identity rather than wrapper identity during replacement', async () => {
    await disposeBackend()
    const raw = new MemoryBackend()
    const dispose = vi.spyOn(raw, 'dispose')
    const first = setDefaultBackend(() => raw, {})
    const second = setDefaultBackend(() => raw, {})
    await settle()
    await expect(first.readHead('same-driver')).resolves.toBe(null)
    expect(second).toBe(first)
    expect(dispose).not.toHaveBeenCalled()
  })
})

describe('shared subscription supervision', () => {
  it('owns fan-out, refcount, epochs, replacement, and cleanup-decoupled resubscription once', async () => {
    const firstCleanup = deferred<void>()
    const secondCleanup = deferred<void>()
    const raw = new ControlledDriver()
    raw.plan(() => ControlledAttempt.ready(firstCleanup.promise))
    raw.plan(() => ControlledAttempt.ready(secondCleanup.promise))
    const manager = new SubscriptionManager(raw)
    const received: string[] = []
    const first = manager.subscribe('source', (payload) => received.push(`a:${decoder.decode(payload)}`))
    const second = manager.subscribe('source', (payload) => received.push(`b:${decoder.decode(payload)}`))
    await first.ready
    expect(raw.opens).toHaveLength(1)
    expect(raw.opens[0]!.localReceiverCount()).toBe(2)

    raw.opens[0]!.attempt.close()
    await settleMicrotasks()
    expect(raw.opens).toHaveLength(2)
    await raw.deliver(0, 'stale')
    await raw.deliver(1, 'current')
    expect(received).toEqual(['a:current', 'b:current'])

    await first.unsubscribe()
    const stopping = second.unsubscribe()
    const replacement = manager.subscribe('source', () => {})
    await replacement.ready
    expect(raw.opens).toHaveLength(3)
    secondCleanup.resolve()
    firstCleanup.resolve()
    await stopping
    await replacement.unsubscribe()
  })

  it('normalizes initial readiness events while preserving failure and recovery transitions', async () => {
    const raw = new ControlledDriver()
    raw.plan(() => new ControlledAttempt())
    raw.plan(() => ControlledAttempt.ready())
    const manager = new SubscriptionManager(raw)
    const subscription = manager.subscribe('async-ready', () => {})
    const states: SubscriptionState[] = []
    subscription.onStateChange((state) => states.push(state))

    raw.opens[0]!.attempt.establish()
    await subscription.ready
    expect(states).toEqual([])

    raw.opens[0]!.attempt.close()
    await settleMicrotasks()
    expect(states).toEqual(['lost', 'ready'])
    await subscription.unsubscribe()
    expect(states).toEqual(['lost', 'ready', 'closed'])

    const failedRaw = new ControlledDriver()
    failedRaw.plan(() => new ControlledAttempt())
    failedRaw.plan(() => ControlledAttempt.ready())
    const failed = new SubscriptionManager(failedRaw).subscribe('initial-failure', () => {})
    const failedStates: SubscriptionState[] = []
    failed.onStateChange((state) => failedStates.push(state))
    failedRaw.opens[0]!.attempt.close()
    await settleMicrotasks()
    expect(failedStates).toEqual(['lost', 'ready'])
    await failed.unsubscribe()
  })

  it('includes the opaque driver partition in source identity', async () => {
    const raw = new ControlledDriver()
    raw.plan(() => ControlledAttempt.ready())
    raw.plan(() => ControlledAttempt.ready())
    const manager = new SubscriptionManager(raw)
    const received: string[] = []

    raw.partition = 'session-a'
    const first = manager.subscribe('same-source', (payload) => received.push(`a:${decoder.decode(payload)}`))
    raw.partition = 'session-b'
    const second = manager.subscribe('same-source', (payload) => received.push(`b:${decoder.decode(payload)}`))
    await Promise.all([first.ready, second.ready])

    expect(raw.opens).toHaveLength(2)
    await raw.deliver(0, 'one')
    await raw.deliver(1, 'two')
    expect(received).toEqual(['a:one', 'b:two'])
    await Promise.all([first.unsubscribe(), second.unsubscribe()])
  })

  it('maps raw ownership termination to public closed without replanning', async () => {
    const raw = new ControlledDriver()
    raw.plan(() => new ControlledAttempt())
    raw.plan(() => ControlledAttempt.ready())
    const manager = new SubscriptionManager(raw)
    const subscription = manager.subscribe('session', () => {})
    const states: SubscriptionState[] = []
    subscription.onStateChange((state) => states.push(state))
    raw.opens[0]!.attempt.establish()
    await subscription.ready

    raw.opens[0]!.attempt.terminate()
    await settleMicrotasks()
    expect(subscription.state()).toBe('closed')
    expect(states).toEqual(['closed'])
    expect(raw.openCalls).toBe(1)

    const replacement = manager.subscribe('session', () => {})
    await replacement.ready
    expect(raw.openCalls).toBe(2)
    await subscription.unsubscribe()
    const sibling = manager.subscribe('session', () => {})
    await sibling.ready
    expect(raw.openCalls).toBe(2)
    await Promise.all([replacement.unsubscribe(), sibling.unsubscribe()])

    const pendingRaw = new ControlledDriver()
    pendingRaw.plan(() => new ControlledAttempt())
    const pending = new SubscriptionManager(pendingRaw).subscribe('pending-session', () => {})
    const readiness = pending.ready
    pendingRaw.opens[0]!.attempt.terminate()
    await expect(readiness).rejects.toThrow('ownership terminated')
    expect(pending.state()).toBe('closed')
    expect(pendingRaw.openCalls).toBe(1)
    await pending.unsubscribe()
  })

  it('checks binding validity before scheduling and immediately before a queued open', async () => {
    const beforeSchedule = new ControlledDriver()
    beforeSchedule.plan(() => {
      beforeSchedule.bindingValid = false
      throw new Error('manager disposed during open')
    })
    const terminal = new SubscriptionManager(beforeSchedule).subscribe('before-schedule', () => {})
    const terminalReadiness = terminal.ready
    expect(terminal.state()).toBe('closed')
    expect(beforeSchedule.openCalls).toBe(1)
    await expect(terminalReadiness).rejects.toThrow('ownership terminated')
    await terminal.unsubscribe()

    const beforeOpen = new ControlledDriver()
    beforeOpen.plan(() => ControlledAttempt.ready())
    beforeOpen.plan(() => ControlledAttempt.ready())
    const queued = new SubscriptionManager(beforeOpen).subscribe('before-open', () => {})
    await queued.ready
    beforeOpen.opens[0]!.attempt.close()
    beforeOpen.bindingValid = false
    await settleMicrotasks()
    expect(queued.state()).toBe('closed')
    expect(beforeOpen.openCalls).toBe(1)
    await queued.unsubscribe()
  })

  it('bounds a hung establishment, replans, and terminally rejects after the exact replacement budget', async () => {
    vi.useFakeTimers()
    const raw = new ControlledDriver()
    for (let attempt = 0; attempt <= SUBSCRIPTION_REPLAN_LIMIT; attempt++) {
      raw.plan(() => new ControlledAttempt())
    }
    const report = vi.fn()
    const manager = new SubscriptionManager(raw, report)
    const subscription = manager.subscribe('hung', () => {})
    const readiness = subscription.ready

    for (let attempt = 0; attempt <= SUBSCRIPTION_REPLAN_LIMIT; attempt++) {
      await vi.advanceTimersByTimeAsync(SUBSCRIPTION_ESTABLISH_TIMEOUT_MS)
    }
    expect(raw.openCalls).toBe(1 + SUBSCRIPTION_REPLAN_LIMIT)
    expect(raw.opens.every(({ attempt }) => attempt.unsubscribeCalls === 1)).toBe(true)
    expect(report).toHaveBeenCalledTimes(1 + SUBSCRIPTION_REPLAN_LIMIT)
    await expect(readiness).rejects.toThrow(`failed after ${SUBSCRIPTION_REPLAN_LIMIT} replacement attempts`)
  })

  it('spends one replacement attempt on a transient deadline and then recovers', async () => {
    vi.useFakeTimers()
    const raw = new ControlledDriver()
    raw.plan(() => new ControlledAttempt())
    raw.plan(() => ControlledAttempt.ready())
    const report = vi.fn()
    const manager = new SubscriptionManager(raw, report)
    const subscription = manager.subscribe('transient', () => {})

    await vi.advanceTimersByTimeAsync(SUBSCRIPTION_ESTABLISH_TIMEOUT_MS)
    await expect(subscription.ready).resolves.toBeUndefined()
    expect(raw.openCalls).toBe(2)
    expect(report).toHaveBeenCalledTimes(1)
    expect(String(report.mock.calls[0]![0])).toContain('establishment did not settle within the deadline')
  })

  it('allocates the Room lane terminal horizon evenly across the bounded attempt budget', () => {
    const totalAttempts = 1 + SUBSCRIPTION_REPLAN_LIMIT
    expect(SUBSCRIPTION_ESTABLISH_TIMEOUT_MS * totalAttempts).toBe(ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS)
  })

  it('never fires the watchdog for the synchronous zero-config memory driver', async () => {
    vi.useFakeTimers()
    await disposeBackend()
    const raw = new MemoryBackend()
    const opened: BackendSubscriptionSource[] = []
    const bind = raw.subscriptions.bind.bind(raw.subscriptions)
    vi.spyOn(raw.subscriptions, 'bind').mockImplementation((source) => {
      const binding = bind(source)
      return {
        partition: binding.partition,
        valid: binding.valid,
        open: (receiver, localReceiverCount) => {
          opened.push(source)
          return binding.open(receiver, localReceiverCount)
        },
      }
    })
    setDefaultBackend(() => raw)
    const room = await Room.create('sync-memory')
    room.subscribe(() => {})
    await vi.advanceTimersByTimeAsync((1 + SUBSCRIPTION_REPLAN_LIMIT) * SUBSCRIPTION_ESTABLISH_TIMEOUT_MS + 1)
    const semanticOpens = opened.filter((source) => source.kind === 'durable' && source.lane.kind === 'semantic')
    expect(semanticOpens).toHaveLength(1)
    expect(await (await room.join()).publish('live')).toMatchObject({ seq: 1 })
  })
})

type Peer = ReturnType<typeof attachPeer>

function attachPeer(stub: RoomStubChannel) {
  const frames: Uint8Array[] = []
  stub._attachPeer(
    new IndexedPeer({ send: (frame) => frames.push(frame) }, 7, new ReplayBuffer(1024 * 1024, 60_000, 1024 * 1024)),
  )
  return { decoded: () => frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>)) }
}

function serve(room: ServerRoom): { stub: RoomStubChannel; peer: Peer } {
  const stub = new RoomStubChannel(room)
  stub._registerChannel()
  room._attachStub(stub)
  return { stub, peer: attachPeer(stub) }
}

function dataFrames(peer: Peer): unknown[] {
  return peer
    .decoded()
    .filter((frame) => frame.tag === TAG.PUBLISH)
    .map((frame) => JSON.parse(frame.text) as { __r: string; data?: unknown })
    .filter((frame) => frame.__r === 'data')
    .map((frame) => frame.data)
}

async function wideBinaryScenario(id: string, retain: boolean, byte: number) {
  const serverRoom = (await Room.create(id)) as ServerRoom
  const camera = await serverRoom.join()
  const { stub, peer } = serve(serverRoom)
  const serverSeqs: number[] = []
  if (!retain) serverRoom.subscribeBinary((_data, info) => serverSeqs.push(info.seq))

  const generation = memoryState.rooms.get(id)!.gens.get(serverRoom._inc)!
  generation.order.set(`binary:${encodeURIComponent(camera.id)}:${encodeURIComponent(DEFAULT_TRACK)}`, {
    seq: 0xffff_ffff,
    timestamp: 10,
  })
  if (!retain) stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', wants: allBinary }), 1)
  const receipt = await camera.publishBinary(new Uint8Array([byte]), retain ? { retain: true } : undefined)
  if (retain) stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', wants: allBinary }), 2)
  await settle()

  const frame = peer
    .decoded()
    .filter((candidate) => candidate.tag === TAG.PUBLISH_BINARY)
    .at(-1)!
  const fake = createFakeStub()
  const client = new ClientRoom(fake.stub, snapshot(id))
  const clientSeqs: number[] = []
  client.subscribeBinary((_data, info) => clientSeqs.push(info.seq))
  fake.emitBinary(frame.data, { key: id, ...frame.info })
  return { receipt: receipt.seq, server: serverSeqs[0] ?? null, client: clientSeqs[0] }
}

function createFakeStub(): {
  stub: ClientBroadcast
  emitBinary(data: Uint8Array, info: ChannelPublishInfo): void
} {
  const binary: Array<(data: Uint8Array, info: ChannelPublishInfo) => void> = []
  const stub = {
    _subscribeLocal: () => () => {},
    _subscribeBinaryLocal: (callback: (data: Uint8Array, info: ChannelPublishInfo) => void) => {
      binary.push(callback)
      return () => binary.splice(binary.indexOf(callback), 1)
    },
    _setWireTextSubscribed: () => {},
    send: async () => undefined,
    publish: async () => ({ key: 'fake', seq: 1, timestamp: 1 }),
    publishBinary: async () => ({ key: 'fake', seq: 1, timestamp: 1 }),
    onClose: () => {},
    _onReconnect: () => {},
  } as unknown as ClientBroadcast
  return {
    stub,
    emitBinary: (data, info) => binary.forEach((callback) => callback(data, info)),
  }
}

function snapshot(roomId: string): RoomSnapshotMetadata {
  return {
    channelId: 'channel',
    roomId,
    meta: {},
    closed: false,
    count: 0,
    stamp: { at: 0, by: '' },
  }
}

type OpenRecord = {
  receiver: BackendReceiver
  localReceiverCount: () => number
  attempt: ControlledAttempt
}

class ControlledDriver implements SubscriptionDriver<string> {
  readonly opens: OpenRecord[] = []
  private readonly _plans: Array<() => ControlledAttempt> = []
  partition = ''
  bindingValid = true
  openCalls = 0

  plan(plan: () => ControlledAttempt): void {
    this._plans.push(plan)
  }

  bind(_source: string) {
    const partition = this.partition
    return {
      partition,
      valid: () => this.bindingValid,
      open: (receiver: BackendReceiver, localReceiverCount: () => number): SubscriptionAttempt => {
        this.openCalls++
        const attempt = (this._plans.shift() ?? (() => ControlledAttempt.ready()))()
        this.opens.push({ receiver, localReceiverCount, attempt })
        return attempt
      },
    }
  }

  async deliver(index: number, value: string): Promise<void> {
    await (this.opens[index]!.receiver(encoder.encode(value), { seq: index + 1, timestamp: 1 }) as unknown)
  }
}

class ControlledAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  private readonly _readiness = deferred<void>()
  private readonly _listeners = new Set<(state: SubscriptionAttemptState) => void>()
  private readonly _cleanup: Promise<void>
  private _state: SubscriptionAttemptState = 'establishing'
  unsubscribeCalls = 0

  constructor(cleanup: Promise<void> = Promise.resolve()) {
    this.ready = this._readiness.promise
    this._cleanup = cleanup
    void this.ready.catch(() => {})
  }

  static ready(cleanup?: Promise<void>): ControlledAttempt {
    const attempt = new ControlledAttempt(cleanup)
    attempt.establish()
    return attempt
  }

  state(): SubscriptionAttemptState {
    return this._state
  }

  onStateChange(listener: (state: SubscriptionAttemptState) => void): () => void {
    this._listeners.add(listener)
    return () => this._listeners.delete(listener)
  }

  async unsubscribe(): Promise<void> {
    this.unsubscribeCalls++
    this._transition('closed')
    await this._cleanup
  }

  establish(): void {
    this._transition('ready')
    this._readiness.resolve()
  }

  close(): void {
    this._transition('closed')
  }

  terminate(): void {
    this._transition('terminated')
  }

  private _transition(state: SubscriptionAttemptState): void {
    this._state = state
    for (const listener of this._listeners) listener(state)
  }
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

async function settleMicrotasks(turns = 8): Promise<void> {
  for (let turn = 0; turn < turns; turn++) await Promise.resolve()
}

function settle(): Promise<void> {
  return delay(0)
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
