import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { IndexedPeer } from '../server/IndexedPeer.js'
import { ACK_STATUS, TAG, decode } from '../shared-ws.js'
import { waitForTelefunctionCallBarriers } from '../client/call-barrier.js'
import { ShieldValidationError, isShieldValidationError } from '../../shared/ShieldValidationError.js'
import { Abort, isAbort } from '../../shared/Abort.js'
import {
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_MEMBER_TTL_MS,
  ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
  ROOM_TAIL_HOLD_BYTES_MAX,
  ROOM_TAIL_HOLD_MAX,
} from '../constants.js'
import {
  DEFAULT_TRACK,
  RoomError,
  frameWithMemberId,
  isRoomError,
  mergeAttributes,
  normalizeJoinOptions,
  pushBoundedTail,
  roomAckError,
  roomCtrlKey,
  roomIdentityKvPrefix,
  roomMemberKvKey,
  sanitizeBinaryWants,
  unframeMemberId,
  type RoomSnapshotMetadata,
} from './protocol.js'
import { ClientRoom, RoomClientBroadcast } from './client.js'
import { RoomState, remoteBacking } from './state.js'
import { Room, ServerRoom } from './server.js'
import { SubSlot, configFromHead, decodeRoomText, encodeRoomConfig } from './server/lanes.js'
import { RoomStubChannel } from './stubs.js'
import { RoomDemand } from './demand.js'
import type { ChannelPublishInfo } from '../channel.js'
import { disposeBackend, getBackend, installBackend, setDefaultBackend } from '../backend/install.js'
import { HEAD_TRANSITIONS, assertHeadTransition } from '../backend/head-transitions.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import { SubscriptionManager } from '../backend/subscriptions.js'
import type {
  BackendReceiver,
  BackendSubscription,
  LaneId,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionDriver,
  SubscriptionState,
} from '../backend/spi.js'
import { ORDERING_FRAME_LAYOUT, decodeOrderingFrame, encodeOrderingFrame } from '../ordering-frame.js'
import { GcRegistry } from '../gcRegistry.js'
import { wrapProxy } from '../wrapProxy.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const semanticLane = { kind: 'semantic' } as const satisfies LaneId
const allBinary = { everyMember: { all: true, tracks: [] }, members: {} }
const FORMER_MEMBER_KV_TTL_MS = 180_000

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
  } finally {
    report.mockRestore()
  }
})

describe('Room public behavior', () => {
  it('opens semantic ingestion only when a semantic listener wants delivery', async () => {
    const room = (await Room.create('semantic-demand')) as ServerRoom
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    let semanticSubscriptions = 0
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind === 'semantic') semanticSubscriptions++
      return subscribeLane(roomId, inc, lane, receiver)
    })
    try {
      const member = await room.join()
      expect(semanticSubscriptions).toBe(0)

      const received: unknown[] = []
      room.subscribe((data) => received.push(data))
      await vi.waitFor(() => expect(semanticSubscriptions).toBe(1))
      await member.publish('wanted')
      expect(received).toEqual(['wanted'])
    } finally {
      subscribe.mockRestore()
    }
  })

  it('does not let a removed member publish or receive a DM when its leave frame is lost', async () => {
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'control') return subscribeLane(roomId, inc, lane, receiver)
      return subscribeLane(roomId, inc, lane, (payload, info) => {
        const envelope = parse(decoder.decode(payload)) as { __r?: string }
        if (envelope.__r === 'leave') return
        receiver(payload, info)
      })
    })
    try {
      const room = await Room.create('removed-authority-fence')
      const removed = await room.join({ meta: { name: 'removed' }, identity: 'removed-user' })
      const sender = await room.join({ meta: { name: 'sender' } })
      const inbox: unknown[] = []
      removed.listen((data) => inbox.push(data))

      await Room.removeParticipant(room.id, { id: removed.id, reason: 'moderated' })
      const publish = await removed.publish('forbidden').then(
        () => 'accepted',
        () => 'rejected',
      )
      const send = await sender.send(removed.id, 'after-removal').then(
        () => 'accepted',
        () => 'rejected',
      )

      expect({ publish, send, inbox }).toEqual({ publish: 'rejected', send: 'rejected', inbox: [] })
    } finally {
      subscribe.mockRestore()
    }
  })

  it('keeps a joining identity undiscoverable until its member inbox subscription is ready', async () => {
    const room = await Room.create('join-inbox-readiness')
    const sender = await room.join()

    const backend = getBackend()
    const commitLane = vi.spyOn(backend, 'commitLane')
    const delayed = delayLaneSubscription((lane) => lane.kind === 'inbox')
    try {
      let joinSettled = false
      const joining = room.join({ identity: 'waiting' }).then((participant) => {
        joinSettled = true
        return participant
      })
      await delayed.started
      expect(joinSettled).toBe(false)
      expect(await Room.getParticipants(room.id, { identity: 'waiting' })).toEqual([])
      await Room.send(room.id, { identity: 'waiting' }, 'premature')
      expect(commitLane).not.toHaveBeenCalledWith(
        room.id,
        expect.any(String),
        expect.objectContaining({ kind: 'inbox' }),
        expect.any(Uint8Array),
        expect.any(Object),
      )

      await delayed.release()

      const joined = await joining
      const inbox: unknown[] = []
      joined.listen((data) => inbox.push(data))
      await sender.send(joined.id, 'ready')
      expect(inbox).toEqual(['ready'])
    } finally {
      delayed.restore()
    }
  })

  it('rolls durable membership back when the join announcement fails', async () => {
    const room = await Room.create('join-readiness-rollback')
    const backend = getBackend()
    const commitLane = backend.commitLane.bind(backend)
    const publish = vi.spyOn(backend, 'commitLane').mockImplementation((roomId, inc, lane, payload, options) => {
      if (lane.kind === 'control' && (parse(decoder.decode(payload)) as { __r?: string }).__r === 'join') {
        throw new Error('join announcement failed')
      }
      return commitLane(roomId, inc, lane, payload, options)
    })
    try {
      await expect(room.join()).rejects.toThrow('join announcement failed')
      await expect(Room.getParticipants(room.id)).resolves.toEqual([])
    } finally {
      publish.mockRestore()
    }
  })

  it('keeps a server participant active so a transient durable-leave failure can be retried', async () => {
    const room = await Room.create('retry-local-leave')
    const member = await room.join()
    const failure = new Error('transient member delete failure')
    vi.spyOn(driver, 'compareExchangeCells').mockRejectedValueOnce(failure)

    await expect(member.leave()).rejects.toBe(failure)
    await expect(member.publish('still-present')).resolves.toMatchObject({ seq: 1 })
    await expect(member.leave()).resolves.toBeUndefined()
    expect(await Room.getParticipants(room.id)).toEqual([])
  })

  it('retains room-stub ownership so a transient durable-leave failure can be retried', async () => {
    const room = (await Room.create('retry-stub-leave')) as ServerRoom
    const stub = register(room)
    const joined = (await room._handleStubRequest(stub, {
      __r: 'req-join',
      meta: {},
      selfDelivery: true,
    })) as { id: string }
    const request = { __r: 'req-leave' as const, id: joined.id }
    const failure = new Error('transient member delete failure')
    vi.spyOn(driver, 'compareExchangeCells').mockRejectedValueOnce(failure)

    await expect(room._handleStubRequest(stub, request)).rejects.toBe(failure)
    expect(stub._stubMembers.has(joined.id)).toBe(true)
    await expect(room._handleStubRequest(stub, request)).resolves.toBeUndefined()
    expect(await Room.getParticipants(room.id)).toEqual([])
  })

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

  it('returns only a discoverable room and closes a head whose directory registration cannot finish', async () => {
    const put = driver.directoryPut.bind(driver)
    let transientAttempts = 0
    const directoryPut = vi.spyOn(driver, 'directoryPut').mockImplementation(async (roomId, inc) => {
      if (roomId === 'index-retry' && transientAttempts++ === 0) throw new Error('transient index failure')
      if (roomId === 'index-failure') throw new Error('persistent index failure')
      await put(roomId, inc)
    })
    try {
      const retried = await Room.create('index-retry')
      expect((await Room.list()).map(({ id }) => id)).toContain(retried.id)

      await expect(Room.create('index-failure')).rejects.toThrow('persistent index failure')
      expect((await driver.readHead('index-failure'))?.head).toMatchObject({ state: 'closed', currentInc: null })
    } finally {
      directoryPut.mockRestore()
    }
  })

  it('waits for an active close lease and takes over until the head is closed', async () => {
    vi.useFakeTimers()
    const room = (await Room.create('concurrent-close')) as ServerRoom
    const current = (await driver.readHead(room.id))!.head
    const leased = await driver.compareExchangeHead(
      room.id,
      { expect: { rev: current.rev } },
      {
        head: {
          currentInc: current.currentInc,
          state: 'closing',
          config: encodeRoomConfig({ ...configFromHead(current), status: 'closing' }),
          closeLease: { id: 'stalled-closer', durationMs: 1_000 },
        },
      },
    )
    expect(leased).toMatchObject({ ok: true, head: { state: 'closing' } })

    let settled = false
    const closing = Room.close(room.id).then(() => {
      settled = true
    })
    await Promise.resolve()
    expect(settled).toBe(false)

    await vi.advanceTimersByTimeAsync(1_100)
    await closing
    expect((await driver.readHead(room.id))?.head).toMatchObject({ state: 'closed', currentInc: null })
  })

  it('retries generation cleanup after the head was durably finalized', async () => {
    const room = (await Room.create('finalized-cleanup-retry')) as ServerRoom
    const inc = room._inc
    await room.join()
    const dropping = vi
      .spyOn(driver, 'dropGeneration')
      .mockRejectedValueOnce(new Error('transient generation cleanup failure'))

    await expect(Room.close(room.id)).rejects.toThrow('transient generation cleanup failure')
    expect((await driver.readHead(room.id))?.head).toMatchObject({ state: 'closed', currentInc: null })
    expect(await driver.listGenerations(room.id)).toEqual([inc])

    await expect(Room.close(room.id)).resolves.toBeUndefined()
    expect(await driver.listGenerations(room.id)).toEqual([])
    expect((await driver.directoryList(room.id)).entries).toEqual([])
    dropping.mockRestore()
  })

  it('waits out an active close lease in getOrCreate instead of reporting not-found', async () => {
    vi.useFakeTimers()
    const room = (await Room.create('get-or-create-closing')) as ServerRoom
    const firstInc = room._inc
    const current = (await driver.readHead(room.id))!.head
    await driver.compareExchangeHead(
      room.id,
      { expect: { rev: current.rev } },
      {
        head: {
          currentInc: current.currentInc,
          state: 'closing',
          config: encodeRoomConfig({ ...configFromHead(current), status: 'closing' }),
          closeLease: { id: 'active-get-or-create-close', durationMs: 1_000 },
        },
      },
    )

    let settled = false
    const resolving = Room.getOrCreate(room.id).finally(() => {
      settled = true
    })
    await Promise.resolve()
    expect(settled).toBe(false)

    await vi.advanceTimersByTimeAsync(1_100)
    const recreated = (await resolving) as ServerRoom
    expect(recreated._inc).not.toBe(firstInc)
    expect(recreated.isClosed).toBe(false)
  })

  it('tears down a close observed by a separate Room runtime that cannot inherit the initiator hold', async () => {
    const authority = await Room.create('remote-close-teardown')
    authority.onAnnounce(() => {})

    // A fresh module graph has its own initiating-close registry and backend installation, like another
    // server process. It shares only the raw authority driver, so this observer receives the real close
    // emitted by Room.close() without being able to see the initiator's in-memory hold.
    vi.resetModules()
    const remoteInstall = await import('../backend/install.js')
    const remoteServer = await import('./server.js')
    const remoteBackend = remoteInstall.installBackend(() => driver)
    const unsubscribed: string[] = []
    const observedLanes = new Set<string>()
    const observationReady = deferred<void>()
    const observationStopped = deferred<void>()
    const subscribeLane = remoteBackend.subscribeLane.bind(remoteBackend)
    const subscribe = vi.spyOn(remoteBackend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      const subscription = subscribeLane(roomId, inc, lane, receiver)
      void subscription.ready.then(() => {
        observedLanes.add(lane.kind)
        if (observedLanes.has('control') && observedLanes.has('semantic')) observationReady.resolve()
      })
      return {
        ready: subscription.ready,
        state: () => subscription.state(),
        onStateChange: (callback) => subscription.onStateChange(callback),
        unsubscribe: async () => {
          unsubscribed.push(lane.kind)
          if (unsubscribed.includes('control') && unsubscribed.includes('semantic')) observationStopped.resolve()
          await subscription.unsubscribe()
        },
      }
    })
    try {
      expect(remoteServer.Room).not.toBe(Room)
      const observer = await remoteServer.Room.get('remote-close-teardown')
      observer.onAnnounce(() => {})
      const closed = deferred<void>()
      observer.onClose(() => closed.resolve())
      await observationReady.promise

      await Room.close('remote-close-teardown')
      await Promise.all([closed.promise, observationStopped.promise])

      expect(observer.isClosed).toBe(true)
      expect(unsubscribed.sort()).toEqual(['control', 'semantic'])
    } finally {
      subscribe.mockRestore()
      await remoteInstall.disposeBackend()
    }
  })

  it('replaces a still-demanded Room subscription after its supervised source closes terminally', async () => {
    const authority = await Room.create('terminal-subscription-recovery')
    const publisher = await authority.join()
    const observer = await Room.get(authority.id)
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    let terminal: ReturnType<typeof terminalSubscription> | undefined
    const replacementReady = deferred<void>()
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'semantic') return subscribeLane(roomId, inc, lane, receiver)
      if (terminal === undefined) {
        terminal = terminalSubscription()
        return terminal.subscription
      }
      const replacement = subscribeLane(roomId, inc, lane, receiver)
      void replacement.ready.then(() => replacementReady.resolve())
      return replacement
    })
    try {
      const received: unknown[] = []
      observer.subscribe((data) => received.push(data))
      if (!terminal) throw new Error('semantic subscription did not start')
      await terminal.subscription.ready
      // Let the observe-transition roster refresh finish before the terminal event; otherwise its
      // unrelated trailing `_syncSubs()` would accidentally replace the closed slot.
      await observer.getParticipants()
      await terminal.close()
      await replacementReady.promise

      await publisher.publish('after-recovery')
      expect(received).toEqual(['after-recovery'])
    } finally {
      subscribe.mockRestore()
    }
  })

  it('reconciles a zombie Room when terminal control state follows a lost closed frame', async () => {
    const authority = await Room.create('terminal-control-reconcile')
    await authority.join()

    vi.resetModules()
    const remoteInstall = await import('../backend/install.js')
    const remoteServer = await import('./server.js')
    const remoteBackend = remoteInstall.installBackend(() => driver)
    const subscribeLane = remoteBackend.subscribeLane.bind(remoteBackend)
    let terminal: ReturnType<typeof terminalSubscription> | undefined
    const subscribe = vi.spyOn(remoteBackend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      const filtered =
        lane.kind === 'control'
          ? (payload: Uint8Array, info: { seq: number; timestamp: number }) => {
              const envelope = parse(decoder.decode(payload)) as { __r?: string }
              if (envelope.__r !== 'closed') receiver(payload, info)
            }
          : receiver
      const subscription = subscribeLane(roomId, inc, lane, filtered)
      if (lane.kind !== 'control' || terminal !== undefined) return subscription
      terminal = terminalSubscription(subscription)
      return terminal.subscription
    })
    try {
      const observer = await remoteServer.Room.get(authority.id)
      let closes = 0
      const closed = deferred<void>()
      observer.onClose(() => {
        closes++
        closed.resolve()
      })
      if (!terminal) throw new Error('control subscription did not start')
      await terminal.subscription.ready

      await Room.close(authority.id)
      await terminal.close()
      await closed.promise

      expect({ closed: observer.isClosed, count: observer.count, closes }).toEqual({
        closed: true,
        count: 0,
        closes: 1,
      })
    } finally {
      subscribe.mockRestore()
      await remoteInstall.disposeBackend()
    }
  })

  it('reconciles authority after a control gap or same-attempt recovery', async () => {
    const room = (await Room.create('control-reconcile')) as ServerRoom
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const controlSubscribed = deferred<void>()
    let transition!: (state: SubscriptionState) => void
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      const inner = subscribeLane(roomId, inc, lane, receiver)
      if (lane.kind !== 'control') return inner
      return {
        ready: inner.ready,
        state: () => inner.state(),
        onStateChange: (listener) => {
          transition = listener
          controlSubscribed.resolve()
          return inner.onStateChange(listener)
        },
        unsubscribe: () => inner.unsubscribe(),
      }
    })
    try {
      room.onAnnounce(() => {})
      await controlSubscribed.promise
      const member = await room.join()
      await room.getParticipants()
      room._state.applyLeave(member.id)
      expect(room.count).toBe(0)

      transition('lost')
      transition('ready')
      await vi.waitFor(() => expect(room.count).toBe(1))
      expect((await room.getParticipants()).map(({ id }) => id)).toEqual([member.id])

      room._state.applyLeave(member.id)
      expect(room.count).toBe(0)
      const onControl = (
        room as unknown as { _onCtrlMessage(message: string, info: { seq: number; timestamp: number }): void }
      )._onCtrlMessage.bind(room)
      onControl('{"__r":"update","meta":{},"at":1,"by":"a"}', { seq: 1, timestamp: 1 })
      onControl('{"__r":"update","meta":{},"at":2,"by":"a"}', { seq: 3, timestamp: 3 })
      await vi.waitFor(() => expect(room.count).toBe(1))
      expect((await room.getParticipants()).map(({ id }) => id)).toEqual([member.id])
    } finally {
      subscribe.mockRestore()
    }
  })

  it('uses the heartbeat roster snapshot to repair observer drift', async () => {
    const authority = await Room.create('heartbeat-reconcile')
    const member = await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    await observer.getParticipants()
    observer._state.applyLeave(member.id)
    expect(observer.count).toBe(0)

    await (observer as unknown as { _heartbeatTick(): Promise<void> })._heartbeatTick()

    expect(observer.count).toBe(1)
    expect((await observer.getParticipants()).map(({ id }) => id)).toEqual([member.id])
  })

  it('reads authority while the control subscription is establishing', async () => {
    const authority = await Room.create('establishing-roster')
    const observer = (await Room.get(authority.id)) as ServerRoom
    expect(await observer.getParticipants()).toEqual([])
    const readiness = deferred<void>()
    const slot = (observer as unknown as { _ctrlSub: SubSlot })._ctrlSub
    slot.sync(true, () => ({
      ready: readiness.promise,
      state: () => 'establishing',
      onStateChange: () => () => {},
      unsubscribe: async () => {},
    }))
    const member = await authority.join()

    expect((await observer.getParticipants()).map(({ id }) => id)).toEqual([member.id])
    readiness.resolve()
    slot.stop()
  })

  it('makes roster readers join an authoritative refresh already in flight', async () => {
    const authority = (await Room.create('roster-refresh-owner')) as ServerRoom
    await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    observer.onJoin(() => {})
    await observer.getParticipants()
    await vi.waitFor(() => expect((observer as unknown as { _ctrlSub: SubSlot })._ctrlSub.established).toBe(true))
    expect(observer._state.rosterKnown).toBe(true)

    const readCells = driver.readCells.bind(driver)
    const started = deferred<void>()
    const release = deferred<void>()
    let held = false
    const read = vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
      if (!held && 'prefix' in selector && selector.prefix.endsWith(':m:')) {
        held = true
        started.resolve()
        await release.promise
      }
      return readCells(roomId, inc, selector)
    })
    const refresh = (
      observer as unknown as {
        _refreshMembers(): Promise<void>
      }
    )._refreshMembers()
    await started.promise
    expect((observer as unknown as { _ctrlSub: SubSlot })._ctrlSub.established).toBe(true)
    const participants = observer.getParticipants()
    const status = await Promise.race([
      participants.then(() => 'settled' as const),
      new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), 0)),
    ])
    expect(status).toBe('pending')

    release.resolve()
    await refresh
    expect((await participants).map(({ id }) => id)).toHaveLength(1)
    read.mockRestore()
  })

  it('heartbeats pure control observers without owned members or binary demand', async () => {
    vi.useFakeTimers()
    const observer = (await Room.get((await Room.create('observer-heartbeat')).id)) as ServerRoom
    const heartbeat = vi
      .spyOn(observer as unknown as { _heartbeatTick(): Promise<void> }, '_heartbeatTick')
      .mockResolvedValue()
    observer.onJoin(() => {})

    await vi.advanceTimersByTimeAsync(ROOM_HEARTBEAT_INTERVAL_MS)

    expect(heartbeat).toHaveBeenCalledOnce()
  })

  it('renews every owned member in one heartbeat cell transaction', async () => {
    const room = (await Room.create('batched-heartbeat')) as ServerRoom
    await room.join()
    await room.join()
    const compare = vi.spyOn(driver, 'compareExchangeCells')

    await (room as unknown as { _heartbeatTick(): Promise<void> })._heartbeatTick()

    expect(compare).toHaveBeenCalledOnce()
  })

  it('reconciles authority even when member renewal fails first', async () => {
    const room = (await Room.create('heartbeat-renewal-failure')) as ServerRoom
    await room.join()
    const readCells = vi.spyOn(driver, 'readCells').mockRejectedValue(new Error('renewal failed'))
    const readHead = vi.spyOn(driver, 'readHead').mockResolvedValue(null)

    await expect((room as unknown as { _heartbeatTick(): Promise<void> })._heartbeatTick()).rejects.toThrow(
      'renewal failed',
    )

    expect(room.isClosed).toBe(true)
    readCells.mockRestore()
    readHead.mockRestore()
  })

  it('replaces a subscription that is already closed on initial establishment', async () => {
    const closed = terminalSubscription()
    await closed.close()
    const terminal = vi.fn()
    const slot = new SubSlot(terminal, () => {})

    slot.sync(true, () => closed.subscription)
    await Promise.resolve()

    expect(terminal).toHaveBeenCalledOnce()
    expect(terminal.mock.calls[0]?.[0]).toBe(slot)
    slot.stop()
  })

  it('rejects only an exhausted internal readiness generation and recovers holder readiness later', async () => {
    const slot = new SubSlot(
      () => {},
      () => {},
    )
    slot.sync(true, () => rejectedSubscription('first generation failed'))
    const holderReady = slot.ready
    const exhaustedGeneration = slot.generationReady
    const exhausted = new RoomError('generation exhausted')

    slot.markLost(exhausted)

    await expect(exhaustedGeneration).rejects.toBe(exhausted)
    expect(await Promise.race([holderReady.then(() => 'ready'), Promise.resolve('pending')])).toBe('pending')

    slot.sync(true, () => terminalSubscription().subscription)
    await expect(holderReady).resolves.toBeUndefined()
    await expect(slot.generationReady).resolves.toBeUndefined()
    expect(slot).toMatchObject({ wanted: true, active: true })
    slot.stop()
  })

  it('keeps retrying inside the horizon after more than six immediate failures', async () => {
    vi.useFakeTimers()
    const observer = await Room.get((await Room.create('single-recovery-horizon')).id)
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    let attempts = 0
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'semantic') return subscribeLane(roomId, inc, lane, receiver)
      attempts++
      return attempts < 8 ? rejectedSubscription(`attempt ${attempts}`) : subscribeLane(roomId, inc, lane, receiver)
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      observer.subscribe(() => {})
      await vi.advanceTimersByTimeAsync(2_000)
      expect(attempts).toBe(8)
      expect(observer.isClosed).toBe(false)
    } finally {
      subscribe.mockRestore()
      report.mockRestore()
    }
  })

  it('keeps an authoritative open Room open after subscription recovery exhausts', async () => {
    vi.useFakeTimers()
    const observer = (await Room.get((await Room.create('open-recovery-exhaustion')).id)) as ServerRoom
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      return lane.kind === 'semantic'
        ? rejectedSubscription('persistent subscription failure')
        : subscribeLane(roomId, inc, lane, receiver)
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const onClose = vi.fn()
    observer.onClose(onClose)
    try {
      observer.subscribe(() => {})
      await vi.advanceTimersByTimeAsync(ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS + 100)
      const textSlot = (observer as unknown as { _textSub: SubSlot })._textSub

      expect((await backend.readHead(observer.id))?.head.state).toBe('open')
      expect({ closed: observer.isClosed, onClose: onClose.mock.calls.length }).toEqual({ closed: false, onClose: 0 })
      expect(textSlot).toMatchObject({ wanted: true, active: false })
    } finally {
      subscribe.mockRestore()
      report.mockRestore()
    }
  })

  it('settles an in-flight join when its inbox recovery horizon exhausts', async () => {
    vi.useFakeTimers()
    const room = (await Room.create('join-recovery-exhaustion')) as ServerRoom
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const inboxStarted = deferred<void>()
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'inbox') return subscribeLane(roomId, inc, lane, receiver)
      inboxStarted.resolve()
      return rejectedSubscription('persistent inbox subscription failure')
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const pending = Symbol('pending')
    let outcome: unknown = pending
    void room.join().then(
      (participant) => {
        outcome = participant
      },
      (error: unknown) => {
        outcome = error
      },
    )
    try {
      await inboxStarted.promise
      await vi.advanceTimersByTimeAsync(ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS + 100)

      expect(outcome).toBeInstanceOf(RoomError)
      expect(room.isClosed).toBe(false)
      expect(room.count).toBe(0)
    } finally {
      subscribe.mockRestore()
      report.mockRestore()
    }
  })

  it('settles retained replay when its semantic recovery horizon exhausts', async () => {
    vi.useFakeTimers()
    const authority = (await Room.create('replay-recovery-exhaustion')) as ServerRoom
    const publisher = await authority.join()
    await publisher.publish('retained', { retain: true })
    const observer = (await Room.get(authority.id)) as ServerRoom
    const { stub } = serve(observer)
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const semanticStarted = deferred<void>()
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'semantic') return subscribeLane(roomId, inc, lane, receiver)
      semanticStarted.resolve()
      return rejectedSubscription('persistent semantic subscription failure')
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    stub._onPeerBroadcastSubscribe(false)
    const pending = Symbol('pending')
    let outcome: unknown = pending
    void observer._replayRetainedText(stub, false, new Set()).then(
      () => {
        outcome = undefined
      },
      (error: unknown) => {
        outcome = error
      },
    )
    try {
      await semanticStarted.promise
      await vi.advanceTimersByTimeAsync(ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS + 100)

      expect(outcome).toBeInstanceOf(RoomError)
      expect(observer.isClosed).toBe(false)
    } finally {
      subscribe.mockRestore()
      report.mockRestore()
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

  it('excludes an expired unobserved member from static get and list presence', async () => {
    const room = (await Room.create('expired-static-presence')) as ServerRoom
    const member = await room.join()
    const memberKey = roomMemberKvKey(room.id, member.id)
    const read = await driver.readCells(room.id, room._inc, { keys: [memberKey] })
    expect('staleInc' in read).toBe(false)
    if ('staleInc' in read) throw new Error('unexpected stale generation')
    const record = parse(decoder.decode(read.cells.get(memberKey)!))
    await expect(
      driver.compareExchangeCells(room.id, room._inc, read.revision, [
        {
          key: memberKey,
          set: { bytes: encoder.encode(stringify({ ...record, seenAt: Date.now() - ROOM_MEMBER_TTL_MS - 1 })) },
        },
      ]),
    ).resolves.toBe('committed')

    const loaded = await Room.get(room.id)
    expect(loaded.count).toBe(0)
    expect(loaded.isEmpty).toBe(true)
    expect((await Room.list()).find(({ id }) => id === room.id)).toMatchObject({ count: 0, isEmpty: true })
  })

  it('keeps local and verified sender metadata at the newest accepted sequence', async () => {
    const room = (await Room.create('participant-meta-order')) as ServerRoom
    const participant = await room.join()
    const observer = await Room.get(room.id)
    const senderMeta: unknown[] = []
    observer.subscribe((_data, _info, from) => senderMeta.push(from.meta))
    const commitLane = driver.commitLane.bind(driver)
    const firstCommit = deferred<void>()
    const releaseFirst = deferred<void>()
    const commit = vi.spyOn(driver, 'commitLane').mockImplementation(async (roomId, inc, lane, payload, options) => {
      if (lane.kind === 'control') {
        const event = parse(decodeRoomText(payload)) as { __r?: string; seq?: number }
        if (event.__r === 'p-meta' && event.seq === 1) {
          firstCommit.resolve()
          await releaseFirst.promise
        }
      }
      return commitLane(roomId, inc, lane, payload, options)
    })

    const first = participant.setMeta({ version: 1 })
    await firstCommit.promise
    await participant.setMeta({ version: 2 })
    releaseFirst.resolve()
    await first

    expect(participant.meta).toEqual({ version: 2 })
    await participant.publish('metadata-check')
    expect(senderMeta).toEqual([{ version: 2 }])
    commit.mockRestore()
  })

  it('resolves multiple identity members with one batched record read', async () => {
    const room = await Room.create('identity-batch')
    const first = await room.join({ identity: 'shared' })
    const second = await room.join({ identity: 'shared' })
    const reads = vi.spyOn(driver, 'readCells')

    const members = await Room.getParticipants(room.id, { identity: 'shared' })

    expect(members.map(({ id }) => id)).toEqual([first.id, second.id])
    expect(reads).toHaveBeenCalledTimes(3)
  })

  it('keeps a member whose heartbeat wins the expired-record reap race', async () => {
    const room = (await Room.create('reap-heartbeat-race')) as ServerRoom
    const member = await room.join()
    const memberKey = roomMemberKvKey(room.id, member.id)
    const compareExchange = driver.compareExchangeCells.bind(driver)
    const initial = await driver.readCells(room.id, room._inc, { keys: [memberKey] })
    expect('staleInc' in initial).toBe(false)
    if ('staleInc' in initial) throw new Error('unexpected stale generation')
    const initialRaw = initial.cells.get(memberKey)
    expect(initialRaw).toBeDefined()
    const initialRecord = parse(decoder.decode(initialRaw!)) as Record<string, unknown>
    await expect(
      compareExchange(room.id, room._inc, initial.revision, [
        {
          key: memberKey,
          set: {
            bytes: encoder.encode(stringify({ ...initialRecord, seenAt: Date.now() - ROOM_MEMBER_TTL_MS - 1 })),
          },
        },
      ]),
    ).resolves.toBe('committed')

    let raced = false
    const compare = vi
      .spyOn(driver, 'compareExchangeCells')
      .mockImplementation(async (roomId, inc, revision, mutations) => {
        if (!raced && mutations.some(({ key, set }) => key === memberKey && set === undefined)) {
          raced = true
          const fresh = await driver.readCells(roomId, inc, { keys: [memberKey] })
          expect('staleInc' in fresh).toBe(false)
          if ('staleInc' in fresh) throw new Error('unexpected stale generation')
          const raw = fresh.cells.get(memberKey)
          expect(raw).toBeDefined()
          const record = parse(decoder.decode(raw!)) as Record<string, unknown>
          await expect(
            compareExchange(roomId, inc, fresh.revision, [
              {
                key: memberKey,
                set: { bytes: encoder.encode(stringify({ ...record, seenAt: Date.now() })) },
              },
            ]),
          ).resolves.toBe('committed')
        }
        return compareExchange(roomId, inc, revision, mutations)
      })

    expect((await Room.getParticipants(room.id)).map(({ id }) => id)).toEqual([member.id])
    expect(raced).toBe(true)
    compare.mockRestore()
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

  it('expands binary subscriptions and demand from one canonical pair set', async () => {
    const room = (await Room.create('binary-pairs')) as ServerRoom
    const publisher = await room.join()
    const expand = (
      room as unknown as {
        _binaryPairs: (wants: typeof allBinary, memberIds: string[]) => Array<[string, string]>
      }
    )._binaryPairs.bind(room)

    expect(
      expand(
        {
          everyMember: { all: false, tracks: ['screen'] },
          members: { [publisher.id]: { all: false, tracks: ['camera'] } },
        },
        [publisher.id],
      ),
    ).toEqual([
      [publisher.id, 'screen'],
      [publisher.id, 'camera'],
    ])
  })

  it('announces binary demand only after the demanded route is ready', async () => {
    const room = await Room.create('binary-demand-readiness')
    const publisher = await room.join()
    const observer = await Room.get(room.id)
    await observer.getParticipants()
    const frames: number[] = []
    let demandStarted = false
    let publishing: Promise<unknown> | undefined
    const demandReady = deferred<void>()
    publisher.onDemand((track, wanted) => {
      if (track === 'screen' && wanted) {
        demandStarted = true
        publishing = publisher.publishBinary(new Uint8Array([8]), { track: 'screen' })
        demandReady.resolve()
      }
    })
    const delayed = delayLaneSubscription(
      (lane) => lane.kind === 'binary' && lane.member === publisher.id && lane.track === 'screen',
    )
    try {
      const remote = (await observer.getParticipant(publisher.id))!
      remote.subscribeBinary((data) => frames.push(data[0]!), {
        track: 'screen',
      })
      await delayed.started
      expect(demandStarted).toBe(false)
      await delayed.release()
      await demandReady.promise
      expect(demandStarted).toBe(true)
      await publishing
      expect(frames).toEqual([8])
    } finally {
      delayed.restore()
    }
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

  it('surfaces an ack timeout as the operational RoomError at the public send boundary', async () => {
    const room = await Room.create('dm-timeout-error-class')
    const sender = await room.join()
    const recipient = await room.join()
    vi.useFakeTimers()

    const sending = sender.send(recipient, 'unhandled', { ack: true }).catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(ROOM_DM_ACK_TIMEOUT_MS)
    const publicError = await sending

    expect(publicError).toBeInstanceOf(RoomError)
    expect(isRoomError(publicError)).toBe(true)
    expect(publicError).toMatchObject({
      name: 'RoomError',
      message: expect.stringContaining('timed out'),
    })
  })

  it('reports a participant-left race on a room-authored send without calling the room closed', async () => {
    const room = await Room.create('server-send-race')
    const target = await room.join()
    const commitLane = driver.commitLane.bind(driver)
    let raced = false
    const commit = vi.spyOn(driver, 'commitLane').mockImplementation(async (roomId, inc, lane, payload, options) => {
      if (!raced && lane.kind === 'inbox') {
        raced = true
        await Room.removeParticipant(room.id, { id: target.id })
      }
      return commitLane(roomId, inc, lane, payload, options)
    })
    try {
      await expect(Room.send(room.id, { id: target.id }, 'late')).rejects.toThrow(
        `Participant not found (left?): ${target.id}`,
      )
      expect((await driver.readHead(room.id))?.head.state).toBe('open')
    } finally {
      commit.mockRestore()
    }
  })

  it('drains DMs that arrived before a participant was bound to its client forwarder', async () => {
    const room = await Room.create('pre-bind-inbox')
    const target = await room.join()
    const sender = await room.join()
    const internal = target as unknown as {
      readonly _isBound: boolean
      _deliverMessage(message: { data: unknown }): void
      _deliverMessageAck(message: { data: unknown }): Promise<unknown>
      _setForwarder(forwarder: (message: { data: unknown }) => unknown): void
    }
    const plainArrived = deferred<void>()
    const ackArrived = deferred<void>()
    const deliverMessage = internal._deliverMessage.bind(target)
    const deliverMessageAck = internal._deliverMessageAck.bind(target)
    const plainDelivery = vi.spyOn(internal, '_deliverMessage').mockImplementation((message) => {
      plainArrived.resolve()
      deliverMessage(message)
    })
    const ackDelivery = vi.spyOn(internal, '_deliverMessageAck').mockImplementation((message) => {
      ackArrived.resolve()
      return deliverMessageAck(message)
    })
    try {
      await sender.send(target.id, 'plain-before-bind')
      const acknowledging = sender.send(target.id, 'ack-before-bind', { ack: true })
      await Promise.all([plainArrived.promise, ackArrived.promise])
      expect(internal._isBound).toBe(false)

      const forwarded: unknown[] = []
      internal._setForwarder((message) => {
        forwarded.push(message.data)
        return Promise.resolve({ ok: true, result: `handled:${String(message.data)}` })
      })

      expect(forwarded).toEqual(['plain-before-bind', 'ack-before-bind'])
      await expect(acknowledging).resolves.toMatchObject({ response: 'handled:ack-before-bind' })
    } finally {
      plainDelivery.mockRestore()
      ackDelivery.mockRestore()
    }
  })

  it('keeps every live ack correlation instead of silently dropping the oldest', async () => {
    const stub = register(await Room.create('ack-correlations'))
    for (let index = 0; index <= 1_024; index++) {
      stub._recordAckDm(`ack-${index}`, `sender-${index}`, 'recipient')
    }
    expect(stub._takeAckDm('ack-0', 'recipient')).toBe('sender-0')
  })

  it('applies before guards and after hooks around authoritative joins, publishes, and sends', async () => {
    await Room.create('guarded')
    const room = (await Room.get('guarded')) as ServerRoom
    const after: string[] = []
    const published: unknown[] = []
    room.subscribe((data) => published.push(data))
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
    expect(room.count).toBe(0)
    expect(await room.getParticipants()).toEqual([])
    const alice = await room.join({ meta: { name: 'Alice' } })
    const bob = await room.join({ meta: { name: 'Bob' } })
    const inbox: unknown[] = []
    bob.listen((data) => inbox.push(data))
    await expect(alice.publish('blocked', { retain: true })).rejects.toThrow('no publish')
    expect(published).toEqual([])
    expect(await driver.readRetained(room.id, room._inc, semanticLane)).toBeNull()
    await alice.publish('ok')
    await expect(alice.send(bob.id, 'blocked')).rejects.toThrow('no send')
    await alice.send(bob.id, 'ok')

    expect(inbox).toEqual(['ok'])
    expect(published).toEqual(['ok'])
    expect(after).toEqual(['join:Alice', 'join:Bob', 'publish:ok', 'send:ok'])
  })

  it('preserves committed join, publish, and send results when after hooks reject', async () => {
    const room = (await Room.create('after-hook-failure')) as ServerRoom
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    Room.guard(room, {
      onAfterJoin: async () => {
        throw new Error('after join failed')
      },
      onAfterPublish: async () => {
        throw new Error('after publish failed')
      },
      onAfterSend: async () => {
        throw new Error('after send failed')
      },
    })
    try {
      const alice = await room.join()
      const bob = await room.join()
      const inbox: unknown[] = []
      bob.listen((data) => inbox.push(data))

      await expect(alice.publish('published')).resolves.toMatchObject({ seq: expect.any(Number) })
      await expect(alice.send(bob.id, 'sent')).resolves.toMatchObject({ seq: expect.any(Number) })
      expect((await room.getParticipants()).map(({ id }) => id)).toEqual([alice.id, bob.id])
      expect(inbox).toEqual(['sent'])
      expect(report).toHaveBeenCalledTimes(4)
    } finally {
      report.mockRestore()
    }
  })

  it('orders participant text and room announcements in one semantic domain', async () => {
    const room = await Room.create('announce-semantic-order')
    const member = await room.join()
    const observer = await Room.get(room.id)
    const observed: Array<[unknown, number]> = []
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const controlReady = deferred<void>()
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      const subscription = subscribeLane(roomId, inc, lane, receiver)
      if (lane.kind === 'control') void subscription.ready.then(() => controlReady.resolve())
      return subscription
    })
    observer.onAnnounce((data, info) => observed.push([data, info.seq]))
    await controlReady.promise
    subscribe.mockRestore()

    const first = await member.publish('one')
    const second = await Room.announce(room.id, 'notice')
    const third = await member.publish('two')

    expect([first.seq, second.seq, third.seq]).toEqual([1, 2, 3])
    expect(first.timestamp).toBeLessThanOrEqual(second.timestamp)
    expect(second.timestamp).toBeLessThanOrEqual(third.timestamp)
    expect(observed).toEqual([['notice', 2]])
  })

  it('relays semantic announcements only to stubs that declared announce demand', async () => {
    const room = (await Room.create('announce-want-gate')) as ServerRoom
    const backend = getBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const semanticReady = deferred<void>()
    const subscribe = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      const subscription = subscribeLane(roomId, inc, lane, receiver)
      if (lane.kind === 'semantic') void subscription.ready.then(() => semanticReady.resolve())
      return subscription
    })
    const wanted = serve(room)
    const silent = serve(room)
    wanted.stub._onPeerMessage(JSON.stringify({ __r: 'sub-text', members: [], announce: true }), 1)
    await semanticReady.promise
    subscribe.mockRestore()

    await Room.announce(room.id, 'wanted')

    await vi.waitFor(() => expect(announceFrames(wanted.peer)).toEqual(['wanted']))
    expect(announceFrames(silent.peer)).toEqual([])
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

  it('retries retained cleanup after member deletion from durable eviction work', async () => {
    const room = (await Room.create('retained-cleanup-retry')) as ServerRoom
    const member = await room.join()
    await member.publish('text', { retain: true })
    await member.publishBinary(new Uint8Array([1]), { track: 'screen', retain: true })
    const deletion = vi
      .spyOn(driver, 'deleteRetained')
      .mockRejectedValueOnce(new Error('transient retained cleanup failure'))

    await expect(member.leave()).rejects.toThrow('transient retained cleanup failure')
    deletion.mockRestore()
    expect(await driver.listRetained(room.id, room._inc)).toHaveLength(2)

    await expect(Room.getParticipants(room.id)).resolves.toEqual([])
    expect(await driver.listRetained(room.id, room._inc)).toEqual([])
  })

  it('reaps legacy orphan-owned retained text instead of replaying it', async () => {
    const authority = (await Room.create('orphan-retained-replay')) as ServerRoom
    const member = await authority.join()
    await member.publish('orphan', { retain: true })
    const memberKey = roomMemberKvKey(authority.id, member.id)
    const cells = await driver.readCells(authority.id, authority._inc, { keys: [memberKey] })
    expect('staleInc' in cells).toBe(false)
    if ('staleInc' in cells) throw new Error('unexpected stale generation')
    await expect(
      driver.compareExchangeCells(authority.id, authority._inc, cells.revision, [{ key: memberKey }]),
    ).resolves.toBe('committed')

    const observer = (await Room.get(authority.id)) as ServerRoom
    const { stub, peer } = serve(observer)
    stub._wantsText = true
    await observer._replayRetainedText(stub, false, new Set())

    expect(dataFrames(peer)).toEqual([])
    await expect(driver.readRetained(authority.id, authority._inc, semanticLane)).resolves.toBeNull()
  })

  it('does not retain or expose mutable lane aliases', async () => {
    const room = (await Room.create('retained-lane-alias')) as ServerRoom
    const lane = { kind: 'binary', member: 'member', track: 'original' } as LaneId
    await driver.commitLane(room.id, room._inc, lane, new Uint8Array([1]), { retain: true })
    if (lane.kind !== 'binary') throw new Error('expected binary lane')
    lane.track = 'mutated-ingress'
    const listed = await driver.listRetained(room.id, room._inc)
    expect(listed).toEqual([{ kind: 'binary', member: 'member', track: 'original' }])
    const returned = listed[0]
    if (returned?.kind !== 'binary') throw new Error('expected retained binary lane')
    returned.track = 'mutated-egress'
    await expect(driver.listRetained(room.id, room._inc)).resolves.toEqual([
      { kind: 'binary', member: 'member', track: 'original' },
    ])
  })

  it('does not replay older retained text after a newer global semantic frame', async () => {
    const room = (await Room.create('global-retained-watermark')) as ServerRoom
    const stub = register(room)
    const relay = vi.spyOn(stub, '_relayPublishText').mockImplementation(() => {})

    stub._relayTextLive('newer-live', 'sender-b', { seq: 2, timestamp: 2 })
    stub._emitRetainedText('older-retained', 'sender-a', { seq: 1, timestamp: 1 })

    expect(relay.mock.calls.map(([wire]) => wire)).toEqual(['newer-live'])
  })

  it('waits for roster-derived binary routes before reading retained frames', async () => {
    const authority = await Room.create('retained-binary-roster-fence')
    const publisher = await authority.join()
    await publisher.publishBinary(new Uint8Array([1]), { track: 'screen', retain: true })
    const observer = (await Room.get(authority.id)) as ServerRoom
    const stub = register(observer)
    const readCells = driver.readCells.bind(driver)
    const rosterStarted = deferred<void>()
    const releaseRoster = deferred<void>()
    const reading = vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
      if (roomId === authority.id && 'prefix' in selector) {
        rosterStarted.resolve()
        await releaseRoster.promise
      }
      return readCells(roomId, inc, selector)
    })
    const listRetained = vi.spyOn(driver, 'listRetained')
    try {
      await observer._handleStubRequest(stub, {
        __r: 'sub-binary',
        wants: { everyMember: { all: true, tracks: [] }, members: {} },
      })
      await rosterStarted.promise
      expect(listRetained).not.toHaveBeenCalled()

      releaseRoster.resolve()
      await vi.waitFor(() => expect(listRetained).toHaveBeenCalled())
    } finally {
      releaseRoster.resolve()
      reading.mockRestore()
    }
  })

  it('delivers exact-member binary frames before the roster is known', async () => {
    const authority = await Room.create('exact-binary-without-roster')
    const publisher = await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    const stub = register(observer)
    const readCells = driver.readCells.bind(driver)
    const rosterStarted = deferred<void>()
    const releaseRoster = deferred<void>()
    const reading = vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
      if (roomId === authority.id && 'prefix' in selector) {
        rosterStarted.resolve()
        await releaseRoster.promise
      }
      return readCells(roomId, inc, selector)
    })
    expect(observer._state.rosterKnown).toBe(false)

    try {
      await observer._handleStubRequest(stub, {
        __r: 'sub-binary',
        wants: {
          everyMember: { all: false, tracks: [] },
          members: { [publisher.id]: { all: false, tracks: ['screen'] } },
        },
      })
      await rosterStarted.promise
      await (observer as unknown as { _binaryReady(): Promise<void> })._binaryReady()
      expect(observer._state.rosterKnown).toBe(false)

      await publisher.publishBinary(new Uint8Array([7]), { track: 'screen' })
      expect(observer._state.rosterKnown).toBe(false)
      const frame = attachPeer(stub)
        .decoded()
        .find((candidate) => candidate.tag === TAG.PUBLISH_BINARY)
      if (frame?.tag !== TAG.PUBLISH_BINARY) throw new Error('expected exact-member binary publish')
      expect(unframeMemberId(frame.data)).toMatchObject({
        from: publisher.id,
        track: 'screen',
        payload: new Uint8Array([7]),
      })
    } finally {
      releaseRoster.resolve()
      await observer.getParticipants()
      reading.mockRestore()
    }
  })

  it('drops retained text and binary when a crashed publisher is reaped', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(1_000_000)
    const room = (await Room.create('reaped-retained')) as ServerRoom
    const writes = vi.spyOn(driver, 'compareExchangeCells')
    const publisher = await room.join({ identity: 'expired-owner' })
    const leaves: string[] = []
    room.onLeave((member) => leaves.push(member.id))
    await publisher.publish('text', { retain: true })
    await publisher.publishBinary(new Uint8Array([1]), { track: 'screen', retain: true })
    expect(await driver.listRetained(room.id, room._inc)).toHaveLength(2)

    expect(writes.mock.calls.flatMap((call) => call[3]).every(({ set }) => set?.ttlMs === undefined)).toBe(true)
    vi.setSystemTime(1_000_000 + FORMER_MEMBER_KV_TTL_MS + 1)
    expect(await Room.getParticipants(room.id)).toEqual([])
    expect(await driver.listRetained(room.id, room._inc)).toEqual([])
    expect(leaves).toEqual([publisher.id])
    await expect(Room.removeParticipant(room.id, { identity: 'expired-owner' })).resolves.toBeUndefined()
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
    await vi.waitFor(() => expect(dataFrames(peer)).toEqual(['early', 'held']))
    await member.publish('live')
    expect(dataFrames(peer)).toEqual(['early', 'held', 'live'])
  })

  it('releases a tail that is not attached within its 60 second lease', async () => {
    vi.useFakeTimers()
    await Room.create('tail-pre-attach-expiry')
    const source = await Room.get('tail-pre-attach-expiry')
    const member = await source.join()
    const tail = (await Room.get('tail-pre-attach-expiry', { tail: true })) as ServerRoom
    await member.publish('expired')

    await vi.advanceTimersByTimeAsync(ROOM_TAIL_ATTACH_TIMEOUT_MS + 1)
    const { stub, peer } = serve(tail)
    stub._onPeerBroadcastSubscribe(false)

    expect(dataFrames(peer)).toEqual([])
  })

  it('releases an attached tail when first text demand misses its 60 second lease', async () => {
    vi.useFakeTimers()
    await Room.create('tail-post-attach-expiry')
    const source = await Room.get('tail-post-attach-expiry')
    const member = await source.join()
    const tail = (await Room.get('tail-post-attach-expiry', { tail: true })) as ServerRoom
    await member.publish('expired')
    const { stub, peer } = serve(tail)

    await vi.advanceTimersByTimeAsync(ROOM_TAIL_ATTACH_TIMEOUT_MS + 1)
    stub._onPeerBroadcastSubscribe(false)

    expect(dataFrames(peer)).toEqual([])
  })

  it('onDemand reports named-track demand turning on and off', async () => {
    const room = await Room.create('demand')
    const camera = await room.join()
    const changes: Array<[string | null, boolean]> = []
    camera.onDemand((track, wanted) => changes.push([track, wanted]))
    const observer = await Room.get('demand')
    await observer.getParticipants()

    const unsubscribe = (await observer.getParticipant(camera.id))!.subscribeBinary(() => {}, { track: 'screen' })
    await vi.waitFor(() => expect(changes).toEqual([['screen', true]]))
    unsubscribe()
    await vi.waitFor(() =>
      expect(changes).toEqual([
        ['screen', true],
        ['screen', false],
      ]),
    )
  })

  it('replays already-true demand when the publisher attaches its handler', async () => {
    const authority = await Room.create('late-demand-handler')
    const observer = await Room.get(authority.id)
    observer.subscribeBinary(() => {})

    const publisher = await authority.join()
    const changes: Array<[string | null, boolean]> = []
    publisher.onDemand((track, wanted) => changes.push([track, wanted]))

    expect(changes).toEqual([[null, true]])
  })

  it('uses the inbox hold only before the first listener attaches', async () => {
    const room = await Room.create('one-shot-inbox-hold')
    const member = await room.join()
    const internal = member as unknown as {
      _deliverMessage(message: {
        from: string
        fromMeta: Record<string, unknown> | null
        fromIdentity: string | null
        data: unknown
      }): void
      _deliverMessageAck(message: {
        from: string
        fromMeta: Record<string, unknown> | null
        fromIdentity: string | null
        data: unknown
      }): Promise<unknown>
    }
    const received: unknown[] = []
    const unlisten = member.listen((data) => received.push(data))
    unlisten()

    internal._deliverMessage({ from: '', fromMeta: null, fromIdentity: null, data: 'stale' })
    let reply: unknown
    void internal
      ._deliverMessageAck({ from: '', fromMeta: null, fromIdentity: null, data: 'ack' })
      .then((value) => (reply = value))
    await Promise.resolve()
    member.listen((data) => received.push(data))

    expect(received).toEqual([])
    expect(reply).toMatchObject({ ok: false })
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

    const lane = { key: 'zero-config-supervision', kind: 'text' } as const
    const received: string[] = []
    const firstReceived = deferred<void>()
    let secondReceived = deferred<void>()
    const first = backend.subscribe(lane, (payload) => {
      received.push(`first:${decoder.decode(payload)}`)
      firstReceived.resolve()
    })
    const second = backend.subscribe(lane, (payload) => {
      received.push(`second:${decoder.decode(payload)}`)
      secondReceived.resolve()
    })
    await Promise.all([first.ready, second.ready])
    await backend.publish(lane, encoder.encode('one'))
    await Promise.all([firstReceived.promise, secondReceived.promise])
    expect(received).toEqual(['first:one', 'second:one'])

    await first.unsubscribe()
    secondReceived = deferred<void>()
    await backend.publish(lane, encoder.encode('two'))
    await secondReceived.promise
    expect(received).toEqual(['first:one', 'second:one', 'second:two'])
    expect(second.state()).toBe('ready')
    await second.unsubscribe()
  })

  it('keeps snapshot references stable until a real state change', async () => {
    const room = await Room.create('snapshot')
    const first = room.snapshot()
    expect(room.snapshot()).toBe(first)
    expect(Object.isFrozen(first)).toBe(true)
    expect(Object.isFrozen(first.meta)).toBe(true)
    expect(Object.isFrozen(first.participants)).toBe(true)
    let changes = 0
    room.onChange(() => changes++)
    await room.join({ meta: { name: 'Alice' } })
    const changed = room.snapshot()
    expect(changed).not.toBe(first)
    expect(Object.isFrozen(changed.participants[0])).toBe(true)
    expect(Object.isFrozen(changed.participants[0]!.meta)).toBe(true)
    expect(changes).toBe(1)
  })
})

describe('client Room lifecycle', () => {
  it('keeps RoomError precedence when an error also matches ShieldValidationError', () => {
    const error = Object.assign(new ShieldValidationError('overlap'), {
      [Symbol.for('telefunc.RoomError')]: true,
    })
    expect(isRoomError(error)).toBe(true)
    expect(isShieldValidationError(error)).toBe(true)
    expect(roomAckError(error, vi.fn())).toEqual({ text: 'overlap', status: ACK_STATUS.ERROR })
  })

  it('classifies only own, real, non-hostile error brands', () => {
    const roomError = new RoomError('room')
    const abortError = Abort('abort')
    const shieldError = new ShieldValidationError('shield')
    const roomBrand = Object.getOwnPropertySymbols(roomError)[0]!
    const abortBrand = Object.getOwnPropertySymbols(abortError).find(
      (symbol) => (abortError as unknown as Record<symbol, unknown>)[symbol] === true,
    )!
    const shieldBrand = Object.getOwnPropertySymbols(shieldError)[0]!

    expect(isRoomError(Object.create(roomError))).toBe(false)
    expect(isAbort(Object.create(abortError))).toBe(false)
    expect(isShieldValidationError(Object.create(shieldError))).toBe(false)
    expect(isRoomError({ [roomBrand]: true, name: 'RoomError', message: 'forged' })).toBe(false)
    expect(isAbort({ [abortBrand]: true, name: 'Abort', message: 'forged', abortValue: null })).toBe(false)
    expect(isShieldValidationError({ [shieldBrand]: true, name: 'ShieldValidationError', message: 'forged' })).toBe(
      false,
    )

    const hostile = new Proxy(
      {},
      {
        has() {
          throw new Error('hostile brand probe')
        },
        getOwnPropertyDescriptor() {
          throw new Error('hostile brand descriptor')
        },
      },
    )
    const report = vi.fn()
    expect(() => isRoomError(hostile)).not.toThrow()
    expect(roomAckError(hostile, report)).toEqual({
      text: 'Internal Server Error — see server logs',
      status: ACK_STATUS.ERROR,
    })
    expect(report).toHaveBeenCalledOnce()
  })

  it('keeps remote serializer backing unforgeable and exact-keyed', async () => {
    const room = await Room.create('remote-backing')
    const joined = await room.join()
    const remote = await room.getParticipant(joined.id)
    expect(remoteBacking(remote)).not.toBeNull()
    expect(remoteBacking(Object.create(remote!))).toBeNull()
    expect(Object.getOwnPropertySymbols(remote!)).toEqual([])
    const hostile = new Proxy(
      {},
      {
        has() {
          throw new Error('hostile remote probe')
        },
      },
    )
    expect(() => remoteBacking(hostile)).not.toThrow()
    expect(remoteBacking(hostile)).toBeNull()
  })

  describe.skipIf(typeof globalThis.gc !== 'function')('Room-derived handle ownership (real GC)', () => {
    it('keeps the Room wrapper alive through a participant extracted from a roster array', async () => {
      const memberId = crypto.randomUUID()
      const fake = createFakeStub()
      const target = new ClientRoom(fake.stub, snapshot('gc-list-owner'))
      const registry = new GcRegistry(5)
      let closed = 0
      const retained = await retainOnlyListedRemote(target, fake, registry, () => closed++, memberId)

      await forceRoomGc()

      expect(retained.room.deref()).not.toBeUndefined()
      expect(retained.member.id).toBe(memberId)
      expect(closed).toBe(0)
    })

    it('keeps the Room wrapper alive through a joined participant', async () => {
      const memberId = crypto.randomUUID()
      const fake = createFakeStub({
        send: async (message: any) => (message.__r === 'req-join' ? { id: memberId, joinedAt: 1 } : undefined),
      })
      const target = new ClientRoom(fake.stub, snapshot('gc-join-owner'))
      const registry = new GcRegistry(5)
      let closed = 0
      const retained = await retainOnlyJoinedParticipant(target, registry, () => closed++)

      await forceRoomGc()

      expect(retained.room.deref()).not.toBeUndefined()
      expect(retained.member.id).toBe(memberId)
      expect(closed).toBe(0)
    })

    it('keeps the Room wrapper alive through a callback-delivered participant', async () => {
      const memberId = crypto.randomUUID()
      const fake = createFakeStub()
      const target = new ClientRoom(fake.stub, snapshot('gc-callback-owner'))
      const registry = new GcRegistry(5)
      let closed = 0
      const retained = await retainOnlyCallbackRemote(target, fake, registry, () => closed++, memberId)

      await forceRoomGc()

      expect(retained.room.deref()).not.toBeUndefined()
      expect(retained.member.id).toBe(memberId)
      expect(closed).toBe(0)
    })

    it('releases the Room wrapper from a departed participant handle', async () => {
      const memberId = crypto.randomUUID()
      const fake = createFakeStub({
        send: async (message: any) => (message.__r === 'req-join' ? { id: memberId, joinedAt: 1 } : undefined),
      })
      const target = new ClientRoom(fake.stub, snapshot('gc-departed-owner'))
      const registry = new GcRegistry(5)
      let closed = 0
      const retained = await retainOnlyDepartedParticipant(target, fake, registry, () => closed++, memberId)

      await forceRoomGc()

      expect(retained.member.id).toBe(memberId)
      expect(retained.room.deref()).toBeUndefined()
      expect(closed).toBe(1)
    })

    it('releases the Room wrapper after every application handle is dropped', async () => {
      const memberId = crypto.randomUUID()
      const fake = createFakeStub({
        send: async (message: any) => (message.__r === 'req-join' ? { id: memberId, joinedAt: 1 } : undefined),
      })
      const target = new ClientRoom(fake.stub, snapshot('gc-drop-owner'))
      const registry = new GcRegistry(5)
      let closed = 0
      const room = await dropJoinedRoomHandles(target, registry, () => closed++)

      await forceRoomGc()

      expect(room.deref()).toBeUndefined()
      expect(closed).toBe(1)
    })
  })

  it('does not emit onEmpty from a partial pre-roster member map', () => {
    const fake = createFakeStub()
    const client = new ClientRoom(fake.stub, { ...snapshot('pre-roster-empty'), count: 2 })
    const memberId = crypto.randomUUID()
    let empty = 0
    client.onEmpty(() => empty++)

    fake.emitText({ __r: 'join', id: memberId, meta: {}, joinedAt: 1 }, { key: client.id, seq: 1, timestamp: 1 })
    fake.emitText({ __r: 'leave', id: memberId }, { key: client.id, seq: 2, timestamp: 2 })

    expect(client.count).toBe(2)
    expect(empty).toBe(0)
  })

  it('dirties the roster epoch for unknown member-record events', () => {
    const state = new RoomState({
      roomId: 'unknown-member-epoch',
      meta: {},
      seed: { members: [] },
      updateStamp: { at: 0, by: '' },
      onListenersChanged: () => {},
      onCallbackError: () => {},
    })
    const id = crypto.randomUUID()

    state.applyTrack(id, 'screen')
    state.applyParticipantMeta(id, { step: 1 }, 1)
    state.applyLeave(id)

    expect(state.membershipVersion).toBe(3)
  })

  it('reports rejected async RoomState callbacks without awaiting delivery', async () => {
    const memberId = crypto.randomUUID()
    const errors: unknown[] = []
    const state = new RoomState({
      roomId: 'async-state-callbacks',
      meta: {},
      seed: { members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0 }] },
      updateStamp: { at: 0, by: '' },
      onListenersChanged: () => {},
      onCallbackError: (error) => errors.push(error),
    })
    const remote = state.getRemote(memberId)!
    const failures = Array.from({ length: 5 }, (_, index) => new Error(`async state callback ${index}`))
    const rejected = failures.map((failure) => {
      const promise = Promise.reject(failure)
      void promise.catch(() => {})
      return promise
    })
    state.subscribe(() => rejected[0])
    remote.subscribe(() => rejected[1])
    state.subscribeBinary(() => rejected[2])
    remote.subscribeBinary(() => rejected[3])
    state.onClose(() => rejected[4])

    state.applyData(memberId, {}, null, 'text', { key: state.roomId, seq: 1, timestamp: 1 })
    state.applyBinary(memberId, new Uint8Array(), null, null, { key: state.roomId, seq: 1, timestamp: 1 })
    state.applyClosed()
    await Promise.resolve()

    expect(errors).toEqual(failures)
  })

  it('reports rejected async participant inbox, demand, and leave callbacks', async () => {
    const room = await Room.create('async-participant-callbacks')
    const participant = await room.join()
    const errors: unknown[] = []
    const internal = participant as unknown as {
      _reportError(error: unknown): void
      _deliverMessage(message: {
        from: string
        fromMeta: Record<string, unknown> | null
        fromIdentity: string | null
        data: unknown
      }): void
      _onDemand(track: string | null, wanted: boolean): void
      _onLeft(cause: { type: 'left' }): void
    }
    internal._reportError = (error) => errors.push(error)
    const failures = Array.from({ length: 3 }, (_, index) => new Error(`async participant callback ${index}`))
    const rejected = failures.map((failure) => {
      const promise = Promise.reject(failure)
      void promise.catch(() => {})
      return promise
    })
    participant.listen(() => rejected[0])
    participant.onDemand(() => rejected[1])
    participant.onLeave(() => rejected[2])

    internal._deliverMessage({ from: '', fromMeta: null, fromIdentity: null, data: 'text' })
    internal._onDemand(null, true)
    internal._onLeft({ type: 'left' })
    await Promise.resolve()

    expect(errors).toEqual(failures)
  })

  it('keeps a client participant active so a rejected leave request can be retried', async () => {
    let leaveAttempts = 0
    const fake = createFakeStub({
      send: async (message) => {
        const request = message as { __r?: string }
        if (request.__r === 'req-join') return { id: crypto.randomUUID(), joinedAt: Date.now() }
        if (request.__r === 'req-leave' && leaveAttempts++ === 0) throw new Error('transient leave rejection')
        return undefined
      },
    })
    const client = new ClientRoom(fake.stub, snapshot('retry-client-leave'))
    const member = await client.join()

    await expect(member.leave()).rejects.toThrow('transient leave rejection')
    await expect(member.publish('still-active')).resolves.toMatchObject({ seq: 1 })
    await expect(member.leave()).resolves.toBeUndefined()
    expect(leaveAttempts).toBe(2)
  })

  it.each(['leave', 'closed'] as const)('settles a client join across a pre-ack %s event', async (event) => {
    const id = crypto.randomUUID()
    const ack = deferred<{ id: string; joinedAt: number }>()
    const fake = createFakeStub({
      send: async (message) => {
        if ((message as { __r?: string }).__r === 'req-join') return await ack.promise
        return undefined
      },
    })
    const client = new ClientRoom(fake.stub, snapshot(`pre-ack-${event}`))
    const joining = client.join()
    await Promise.resolve()
    if (event === 'leave') {
      fake.emitText({ __r: 'leave', id }, { key: client.id, seq: 1, timestamp: 1 })
    } else {
      fake.emitText({ __r: 'closed' }, { key: client.id, seq: 1, timestamp: 1 })
    }
    ack.resolve({ id, joinedAt: 1 })

    const participant = await joining
    await expect(participant.publish('zombie-check')).rejects.toThrow(/left/i)
    const causes: unknown[] = []
    participant.onLeave((cause) => causes.push(cause))
    expect(causes).toEqual([{ type: event === 'leave' ? 'left' : 'closed' }])
  })

  it('replays demand that arrives while a client join ack is pending', async () => {
    const id = crypto.randomUUID()
    const ack = deferred<{ id: string; joinedAt: number }>()
    const fake = createFakeStub({
      send: async (message) => {
        if ((message as { __r?: string }).__r === 'req-join') return await ack.promise
        return undefined
      },
    })
    const client = new ClientRoom(fake.stub, snapshot('pre-ack-demand'))
    const joining = client.join()
    await Promise.resolve()
    fake.emitText(
      { __r: 'demand', member: id, track: 'screen', wanted: true },
      { key: client.id, seq: 1, timestamp: 1 },
    )
    ack.resolve({ id, joinedAt: 1 })

    const participant = await joining
    const demand: unknown[] = []
    participant.onDemand((track, wanted) => demand.push([track, wanted]))
    expect(demand).toEqual([['screen', true]])
  })

  it("derives participant-update prev from the receiver's own applied state", async () => {
    const fake = createFakeStub()
    const client = new ClientRoom(fake.stub, snapshot('receiver-local-prev'))
    const memberId = crypto.randomUUID()
    fake.emitText(
      {
        __r: 'roster',
        members: [{ id: memberId, meta: { step: 0 }, joinedAt: 1, metaSeq: 0 }],
      },
      { key: 'receiver-local-prev', seq: 1, timestamp: 1 },
    )
    const member = await client.getParticipant(memberId)
    const updates: Array<[unknown, unknown]> = []
    member!.onUpdate((meta, prev) => updates.push([meta, prev]))

    // This receiver skipped the writer's step:1 revision. Its callback must report the value it
    // actually transitioned away from, not the writer-local value carried by the old wire shape.
    fake.emitText(
      { __r: 'p-meta', id: memberId, meta: { step: 2 }, prev: { step: 1 }, seq: 2 },
      { key: 'receiver-local-prev', seq: 2, timestamp: 2 },
    )
    expect(updates).toEqual([[{ step: 2 }, { step: 0 }]])
  })

  it('makes the closed-and-empty state visible before participant leave callbacks run', async () => {
    const fake = createFakeStub()
    const client = new ClientRoom(fake.stub, snapshot('atomic-close-state'))
    const memberId = crypto.randomUUID()
    fake.emitText(
      {
        __r: 'roster',
        members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0 }],
      },
      { key: 'atomic-close-state', seq: 1, timestamp: 1 },
    )
    const member = await client.getParticipant(memberId)
    const before = client.snapshot()
    let observed: unknown
    member!.onLeave(() => {
      const snap = client.snapshot()
      observed = {
        closed: client.isClosed,
        count: client.count,
        snapshotChanged: snap !== before,
        snapshotClosed: snap.isClosed,
        snapshotCount: snap.count,
        participants: snap.participants.length,
      }
    })

    fake.emitText({ __r: 'closed' }, { key: 'atomic-close-state', seq: 2, timestamp: 2 })
    expect(observed).toEqual({
      closed: true,
      count: 0,
      snapshotChanged: true,
      snapshotClosed: true,
      snapshotCount: 0,
      participants: 0,
    })
  })

  it('rejects hidden-roster enumeration in clients instead of returning a partial direct-handle list', async () => {
    const fake = createFakeStub()
    const client = new ClientRoom(fake.stub, snapshot('client-hidden-roster'))
    fake.emitText({ __r: 'roster', members: [] }, { key: 'client-hidden-roster', seq: 1, timestamp: 1 })
    client._reviveRemote({
      id: crypto.randomUUID(),
      meta: { role: 'moderator' },
      joinedAt: 1,
      metaSeq: 0,
      identity: null,
      hidden: true,
    })

    await expect(client.getParticipants({ hidden: true })).rejects.toThrow(
      'Hidden participants can only be enumerated on the server',
    )
  })

  it('declares a room-level default binary track without an earlier all-track listener', () => {
    const sent: unknown[] = []
    const fake = createFakeStub({
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    const client = new ClientRoom(fake.stub, snapshot('default-track-declaration'))

    client.subscribeBinary(() => {}, { track: null })

    expect(sent).toContainEqual({
      __r: 'sub-binary',
      wants: { everyMember: { all: false, tracks: [DEFAULT_TRACK] }, members: {} },
    })
  })

  it('declares and retires client announce demand on the semantic lane', () => {
    const sent: unknown[] = []
    const fake = createFakeStub({
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    const client = new ClientRoom(fake.stub, snapshot('announce-declaration'))

    const stop = client.onAnnounce(() => {})
    stop()

    expect(sent.filter((message: any) => message.__r === 'sub-text')).toEqual([
      { __r: 'sub-text', members: [], announce: true },
      { __r: 'sub-text', members: [], announce: false },
    ])
  })

  it('fences a causally subsequent telefunction call until announce demand is accepted', async () => {
    const accepted = deferred<void>()
    const fake = createFakeStub({
      send: async (_message, options) => {
        if (options?.ack) await accepted.promise
        return undefined
      },
    })
    const client = new ClientRoom(fake.stub, snapshot('announce-demand-fence'))

    client.onAnnounce(() => {})
    const fence = waitForTelefunctionCallBarriers()

    expect(fence).not.toBeNull()
    let ready = false
    void fence!.then(() => {
      ready = true
    })
    await Promise.resolve()
    expect(ready).toBe(false)
    accepted.resolve()
    await fence
    expect(ready).toBe(true)
  })

  it('redeclares a room-wide text subscription after reconnect even when the local latch already matches', () => {
    const wireDeclarations: boolean[] = []
    const fake = createFakeStub({ wireDeclarations })
    const client = new ClientRoom(fake.stub, snapshot('reconnect-text'))

    client.subscribe(() => {})
    expect(wireDeclarations).toEqual([true])

    // Model the transport dying after the client emitted BROADCAST_SUB but before the server
    // applied it: the client latch is true, while the peer's authoritative state is still false.
    fake.reconnect()

    expect(wireDeclarations).toEqual([true, true])
  })

  it('turns a server roster read rejection into an explicit client-settling event', async () => {
    const room = (await Room.create('roster-error-event')) as ServerRoom
    const stub = register(room)
    const failure = new Error('backend roster read failed')
    const ensureRoster = vi
      .spyOn(room as unknown as { _ensureRoster(): Promise<void> }, '_ensureRoster')
      .mockRejectedValue(failure)
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      const peer = attachPeer(stub)
      await vi.waitFor(() => expect(peer.decoded().some((frame) => frame.tag === TAG.PUBLISH)).toBe(true))
      const frame = peer.decoded().find((candidate) => candidate.tag === TAG.PUBLISH)!
      if (frame.tag !== TAG.PUBLISH) throw new Error('expected roster error publish')
      const fake = createFakeStub()
      const participants = new ClientRoom(fake.stub, snapshot('roster-error-event')).getParticipants()
      fake.emitText(parse(frame.text), { key: 'roster-error-event', seq: 1, timestamp: 1 })
      await expect(participants).rejects.toThrow('Failed to load room participants')
      expect(ensureRoster).toHaveBeenCalledOnce()
    } finally {
      report.mockRestore()
      ensureRoster.mockRestore()
    }
  })

  it('replays a committed server-pushed roster after reconnect without rerunning onOpen', async () => {
    const room = (await Room.create('roster-replay')) as ServerRoom
    const stub = register(room)
    const ensureRoster = vi.spyOn(room as unknown as { _ensureRoster(): Promise<void> }, '_ensureRoster')
    attachPeer(stub)
    await vi.waitFor(() => expect(ensureRoster).toHaveBeenCalledOnce())

    stub._onPeerDisconnect(1_000)
    const [frame] = attachPeer(stub, 0).decoded()
    if (frame?.tag !== TAG.PUBLISH) throw new Error('expected replayed roster publish')
    expect(parse(frame.text)).toMatchObject({ __r: 'roster', members: [] })

    expect(ensureRoster).toHaveBeenCalledOnce()
  })
})

describe('room demand lifecycle', () => {
  it('retires pushed demand when a member departs so a later owner gets a fresh transition', () => {
    let owns = true
    const delivered: Array<[string, string, boolean]> = []
    const demand = new RoomDemand(
      () => {},
      () => owns,
      (member, track, wanted) => delivered.push([member, track, wanted]),
    )
    demand.applyWant({ member: 'member', track: 'screen', node: 'remote-a', on: true })
    owns = false
    demand.forgetMember('member')
    owns = true
    demand.applyWant({ member: 'member', track: 'screen', node: 'remote-b', on: true })

    expect(delivered).toEqual([
      ['member', 'screen', true],
      ['member', 'screen', true],
    ])
  })
})

describe('room binary protocol validation', () => {
  it('rejects malformed key components as usage errors', () => {
    expect(() => roomCtrlKey('\ud800')).toThrow('well-formed')
    expect(() => roomIdentityKvPrefix('room', '\udc00')).toThrow('well-formed')
  })

  it('rejects non-boolean self-delivery options', () => {
    expect(() => normalizeJoinOptions({ selfDelivery: 'false' } as never)).toThrow('boolean')
    expect(() => normalizeJoinOptions({ selfDelivery: 0 } as never)).toThrow('boolean')
  })

  it('rejects invalid binary subscription option containers', () => {
    const state = new RoomState({
      roomId: 'strict-options',
      meta: {},
      seed: { members: [] },
      updateStamp: { at: 0, by: '' },
      onListenersChanged: () => {},
      onCallbackError: () => {},
    })
    for (const options of [null, [], 'screen']) {
      expect(() => state.subscribeBinary(() => {}, options as never)).toThrow('object')
    }
  })

  it('bounds tails by count and serialized code units at every cap edge', () => {
    const entry = (serialized: string, seq = 0) => ({
      serialized,
      ord: { seq, timestamp: 0 },
      from: '',
    })
    const byCount: ReturnType<typeof entry>[] = []
    for (let seq = 0; seq <= ROOM_TAIL_HOLD_MAX; seq++) pushBoundedTail(byCount, entry('x', seq))
    expect(byCount).toHaveLength(ROOM_TAIL_HOLD_MAX)
    expect(byCount[0]!.ord.seq).toBe(1)

    const bySize: ReturnType<typeof entry>[] = []
    const halfPlusOne = 'x'.repeat(ROOM_TAIL_HOLD_BYTES_MAX / 2 + 1)
    pushBoundedTail(bySize, entry(halfPlusOne, 1))
    pushBoundedTail(bySize, entry(halfPlusOne, 2))
    expect(bySize.map(({ ord }) => ord.seq)).toEqual([2])
    pushBoundedTail(bySize, entry('x'.repeat(ROOM_TAIL_HOLD_BYTES_MAX + 1), 3))
    expect(bySize.map(({ ord }) => ord.seq)).toEqual([2])

    const nonAscii: ReturnType<typeof entry>[] = []
    pushBoundedTail(nonAscii, entry('💥'.repeat(ROOM_TAIL_HOLD_BYTES_MAX / 2)))
    expect(nonAscii).toHaveLength(1)
  })

  it.each([
    [
      'member UUIDs',
      ['12345678-1234-1234-1234-123456789abc', '12345678-1234-1234-1234-123456789ABC'],
      (id: string) => frameWithMemberId(id, new Uint8Array()),
    ],
    [
      'single-unit tracks',
      Array.from({ length: 0x1_0000 }, (_, code) => String.fromCharCode(code)),
      (track: string) => frameWithMemberId('12345678-1234-1234-1234-123456789abc', new Uint8Array(), { track }),
    ],
  ] as const)('encodes every accepted %s input injectively', (_name, inputs, encode) => {
    const seen = new Set<string>()
    for (const input of inputs) {
      let frame: Uint8Array
      try {
        frame = encode(input)
      } catch {
        continue
      }
      const key = String.fromCharCode(...frame)
      expect(seen.has(key)).toBe(false)
      seen.add(key)
      if (_name === 'single-unit tracks') expect(unframeMemberId(frame)?.track).toBe(input)
    }
  })

  it('preserves __proto__ as data and builds prototype-safe binary wants', () => {
    const attrs = Object.create(null) as Record<string, unknown>
    Object.defineProperty(attrs, '__proto__', { value: { safe: true }, enumerable: true, configurable: true })
    const merged = mergeAttributes({}, attrs)
    expect(Object.getPrototypeOf(merged)).toBe(Object.prototype)
    expect(Object.hasOwn(merged, '__proto__')).toBe(true)
    expect(merged.__proto__).toEqual({ safe: true })
    Object.defineProperty(attrs, '__proto__', { value: undefined, enumerable: true, configurable: true })
    expect(Object.hasOwn(mergeAttributes(merged, attrs), '__proto__')).toBe(false)

    const memberId = crypto.randomUUID()
    const sanitized = sanitizeBinaryWants({
      everyMember: { all: false, tracks: [] },
      members: { [memberId]: { all: false, tracks: ['screen'] } },
    })
    expect(sanitized).not.toBeNull()
    expect(Object.getPrototypeOf(sanitized!.members)).toBeNull()
    const hostileMembers = Object.create(null) as Record<string, unknown>
    hostileMembers.__proto__ = { all: true, tracks: [] }
    expect(sanitizeBinaryWants({ everyMember: { all: false, tracks: [] }, members: hostileMembers })).toBeNull()
    expect(
      sanitizeBinaryWants({
        everyMember: { all: false, tracks: [] },
        members: { 'not-a-member-id': { all: false, tracks: [] } },
      }),
    ).toBeNull()
  })

  it('rejects array metadata and malformed UTF-8 instead of normalizing them', () => {
    const memberId = crypto.randomUUID()
    expect(() =>
      frameWithMemberId(memberId, new Uint8Array(), {
        meta: [] as unknown as Record<string, unknown>,
      }),
    ).toThrow('meta should be an object')

    const arrayMeta = frameWithMemberId(memberId, new Uint8Array(), { meta: {} })
    arrayMeta[19] = '['.charCodeAt(0)
    arrayMeta[20] = ']'.charCodeAt(0)
    expect(unframeMemberId(arrayMeta)).toBeNull()

    const malformedTrack = frameWithMemberId(memberId, new Uint8Array(), { track: 'x' })
    malformedTrack[18] = 0xff
    expect(unframeMemberId(malformedTrack)).toBeNull()
  })

  it('uses the full wire length fields instead of smaller policy caps', () => {
    const memberId = crypto.randomUUID()
    const track = 't'.repeat(255)
    const meta = { note: 'm'.repeat(5000) }
    const framed = frameWithMemberId(memberId, new Uint8Array([1]), { track, meta })

    expect(unframeMemberId(framed)).toMatchObject({ track, meta, payload: new Uint8Array([1]) })
    expect(() => frameWithMemberId(memberId, new Uint8Array(), { track: 't'.repeat(256) })).toThrow('255 bytes')
    expect(() => frameWithMemberId(memberId, new Uint8Array(), { meta: { note: 'm'.repeat(70_000) } })).toThrow(
      '65535 bytes',
    )
  })

  it('accepts legal binary declarations regardless of undocumented aggregate counts', () => {
    const members = Object.fromEntries(
      Array.from({ length: 4097 }, (_, index) => [
        `${index.toString(16).padStart(8, '0')}-0000-0000-0000-000000000000`,
        { all: false, tracks: [] },
      ]),
    )
    expect(sanitizeBinaryWants({ everyMember: { all: false, tracks: [] }, members })).not.toBeNull()
    expect(
      sanitizeBinaryWants({
        everyMember: { all: false, tracks: Array.from({ length: 65 }, (_, index) => `track-${index}`) },
        members: {},
      }),
    ).not.toBeNull()
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
    expect(await backend.compareExchangeCells('spi', 'inc-1', currentCells.revision, [{ key: 'member' }])).toBe(
      'committed',
    )
    expect(
      await backend.commitLane('spi', 'inc-1', semanticLane, encoder.encode('fenced'), {
        requiredCellKeys: ['member'],
      }),
    ).toEqual({ stale: true })

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

    for (const durationMs of [0, Number.POSITIVE_INFINITY]) {
      await expect(
        backend.compareExchangeHead(
          'head-shape',
          { expect: { rev: opened.head.rev } },
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

  it('publishes one immutable wide ordering layout and codec', () => {
    expect(Object.isFrozen(ORDERING_FRAME_LAYOUT)).toBe(true)
    expect(Object.isFrozen(ORDERING_FRAME_LAYOUT.offsets)).toBe(true)
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

  it('keeps the memory lane gated until every target settles after one rejects', async () => {
    await driver.compareExchangeHead(
      'memory-order',
      { expect: 'absent' },
      { head: { currentInc: 'inc', state: 'open', config: new Uint8Array() } },
    )
    const slowStarted = deferred<void>()
    const releaseSlow = deferred<void>()
    let secondStarted = false
    const source = { kind: 'durable' as const, roomId: 'memory-order', inc: 'inc', lane: semanticLane }
    const fast = driver.subscriptions.bind(source).open(
      () => {
        throw new Error('fast rejection')
      },
      () => 2,
    )
    const slow = driver.subscriptions.bind(source).open(
      async (_payload, { seq }) => {
        if (seq === 1) {
          slowStarted.resolve()
          await releaseSlow.promise
        } else secondStarted = true
      },
      () => 2,
    )
    await Promise.all([fast.ready, slow.ready])
    const first = await driver.commitLane('memory-order', 'inc', semanticLane, new Uint8Array([1]))
    const second = await driver.commitLane('memory-order', 'inc', semanticLane, new Uint8Array([2]))
    if (!('accepted' in first) || !('accepted' in second)) throw new Error('memory ordering commit was stale')
    await slowStarted.promise
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    expect(secondStarted).toBe(false)
    releaseSlow.resolve()
    await expect(first.delivery).rejects.toThrow('fast rejection')
    await expect(second.delivery).rejects.toThrow('fast rejection')
  })

  it('tracks raw-driver identity rather than wrapper identity during replacement', async () => {
    await disposeBackend()
    const raw = new MemoryBackend()
    const dispose = vi.spyOn(raw, 'dispose')
    const first = setDefaultBackend(() => raw, {})
    const second = setDefaultBackend(() => raw, {})
    await expect(first.readHead('same-driver')).resolves.toBe(null)
    expect(second).toBe(first)
    expect(dispose).not.toHaveBeenCalled()
  })
})

describe('shared subscription supervision', () => {
  it('owns fan-out, refcount, epochs, and raw terminal signalling once', async () => {
    const firstCleanup = deferred<void>()
    const secondCleanup = deferred<void>()
    const raw = new ControlledDriver()
    raw.plan(() => ControlledAttempt.ready(firstCleanup.promise))
    raw.plan(() => ControlledAttempt.ready(secondCleanup.promise))
    const manager = new SubscriptionManager(raw, vi.fn())
    const received: string[] = []
    const first = manager.subscribe('source', (payload) => received.push(`a:${decoder.decode(payload)}`))
    const second = manager.subscribe('source', (payload) => received.push(`b:${decoder.decode(payload)}`))
    await first.ready
    expect(raw.opens).toHaveLength(1)
    expect(raw.opens[0]!.localReceiverCount()).toBe(2)

    raw.opens[0]!.attempt.close()
    expect(first.state()).toBe('closed')
    expect(second.state()).toBe('closed')
    expect(raw.opens).toHaveLength(1)
    await raw.deliver(0, 'stale')
    expect(received).toEqual([])

    const replacement = manager.subscribe('source', (payload) => received.push(`c:${decoder.decode(payload)}`))
    await replacement.ready
    expect(raw.opens).toHaveLength(2)
    await raw.deliver(1, 'current')
    expect(received).toEqual(['c:current'])

    await Promise.all([first.unsubscribe(), second.unsubscribe()])
    const stopping = replacement.unsubscribe()
    secondCleanup.resolve()
    firstCleanup.resolve()
    await stopping
  })

  it('does not convert pending raw cleanup into successful settlement', async () => {
    const liveCleanup = deferred<void>()
    const terminalCleanup = deferred<void>()
    const raw = new ControlledDriver()
    raw.plan(() => ControlledAttempt.ready(liveCleanup.promise))
    raw.plan(() => ControlledAttempt.ready(liveCleanup.promise))
    raw.plan(() => ControlledAttempt.ready(terminalCleanup.promise))
    const manager = new SubscriptionManager(raw)
    const first = manager.subscribe('unsubscribe', () => {})
    const second = manager.subscribe('dispose', () => {})
    const terminal = manager.subscribe('already-terminal', () => {})
    await Promise.all([first.ready, second.ready, terminal.ready])
    raw.opens[2]!.attempt.close()

    let unsubscribeSettled = false
    let disposeSettled = false
    const unsubscribing = first.unsubscribe().then(() => (unsubscribeSettled = true))
    const disposing = manager.dispose().then(() => (disposeSettled = true))
    expect({ unsubscribeSettled, disposeSettled }).toEqual({ unsubscribeSettled: false, disposeSettled: false })
    liveCleanup.resolve()
    await unsubscribing
    expect({ unsubscribeSettled, disposeSettled }).toEqual({ unsubscribeSettled: true, disposeSettled: false })
    terminalCleanup.resolve()
    await disposing
    expect({ unsubscribeSettled, disposeSettled }).toEqual({ unsubscribeSettled: true, disposeSettled: true })
  })

  it('normalizes initial readiness and surfaces raw recovery or terminal failure', async () => {
    const raw = new ControlledDriver()
    raw.plan(() => new ControlledAttempt())
    const manager = new SubscriptionManager(raw, vi.fn())
    const subscription = manager.subscribe('async-ready', () => {})
    const states: SubscriptionState[] = []
    subscription.onStateChange((state) => states.push(state))

    raw.opens[0]!.attempt.establish()
    await subscription.ready
    expect(states).toEqual([])

    raw.opens[0]!.attempt.lose()
    const recovered = subscription.ready
    raw.opens[0]!.attempt.establish()
    await recovered
    expect(states).toEqual(['lost', 'ready'])
    expect(raw.openCalls).toBe(1)

    raw.opens[0]!.attempt.close()
    expect(states).toEqual(['lost', 'ready', 'lost', 'closed'])
    expect(raw.openCalls).toBe(1)
    await subscription.unsubscribe()
    expect(states).toEqual(['lost', 'ready', 'lost', 'closed'])

    const failedRaw = new ControlledDriver()
    failedRaw.plan(() => new ControlledAttempt())
    const failed = new SubscriptionManager(failedRaw).subscribe('initial-failure', () => {})
    const failedStates: SubscriptionState[] = []
    failed.onStateChange((state) => failedStates.push(state))
    const failedReadiness = failed.ready
    failedRaw.opens[0]!.attempt.close()
    await expect(failedReadiness).rejects.toThrow('Backend subscription closed')
    expect(failedStates).toEqual(['lost', 'closed'])
    expect(failedRaw.openCalls).toBe(1)
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
    await vi.waitFor(() => expect(subscription.state()).toBe('closed'))
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

  it('checks binding ownership before opening a raw attempt', async () => {
    const raw = new ControlledDriver()
    raw.bindingValid = false
    const terminal = new SubscriptionManager(raw).subscribe('invalid-owner', () => {})
    const terminalReadiness = terminal.ready
    expect(terminal.state()).toBe('closed')
    expect(raw.openCalls).toBe(0)
    await expect(terminalReadiness).rejects.toThrow('ownership terminated')
    await terminal.unsubscribe()
  })
})

type Peer = ReturnType<typeof attachPeer>

function attachPeer(stub: RoomStubChannel, lastSeq?: number) {
  const frames: Uint8Array[] = []
  const replay = stub._replayBuffer!
  if (lastSeq !== undefined) frames.push(...replay.getAfter(lastSeq))
  stub._attachPeer(
    new IndexedPeer(
      {
        send: (frame, onCommit) => {
          frames.push(frame)
          onCommit?.()
        },
      },
      7,
      replay,
    ),
  )
  return { decoded: () => frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>)) }
}

function register(room: ServerRoom): RoomStubChannel {
  const stub = new RoomStubChannel(room)
  stub._registerChannel()
  room._attachStub(stub)
  return stub
}

function serve(room: ServerRoom): { stub: RoomStubChannel; peer: Peer } {
  const stub = register(room)
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

function announceFrames(peer: Peer): unknown[] {
  return peer
    .decoded()
    .filter((frame) => frame.tag === TAG.PUBLISH)
    .map((frame) => JSON.parse(frame.text) as { __r: string; data?: unknown })
    .filter((frame) => frame.__r === 'announce')
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
  if (!retain) {
    await serverRoom._handleStubRequest(stub, { __r: 'sub-binary', wants: allBinary })
    await (serverRoom as unknown as { _binaryReady(): Promise<void> })._binaryReady()
  }
  const receipt = await camera.publishBinary(new Uint8Array([byte]), retain ? { retain: true } : undefined)
  if (retain) {
    await serverRoom._handleStubRequest(stub, { __r: 'sub-binary', wants: allBinary })
    await (serverRoom as unknown as { _binaryReady(): Promise<void> })._binaryReady()
  }
  await vi.waitFor(() => expect(peer.decoded().some((candidate) => candidate.tag === TAG.PUBLISH_BINARY)).toBe(true))

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

function createFakeStub(options?: {
  send?: (message: unknown, options?: { ack?: boolean }) => Promise<unknown>
  wireDeclarations?: boolean[]
}): {
  stub: RoomClientBroadcast
  emitText(data: unknown, info: ChannelPublishInfo): void
  emitBinary(data: Uint8Array, info: ChannelPublishInfo): void
  reconnect(): void
} {
  const text: Array<(data: unknown, info: ChannelPublishInfo) => void> = []
  const binary: Array<(data: Uint8Array, info: ChannelPublishInfo) => void> = []
  let reconnect = () => {}
  const stub = {
    _wireTextSubscribed: false,
    _isClosed: false,
    _connection: {
      sendBroadcastSubscribe: () => options?.wireDeclarations?.push(true),
      sendBroadcastUnsubscribe: () => options?.wireDeclarations?.push(false),
    },
    _subscribeLocal: (callback: (data: unknown, info: ChannelPublishInfo) => void) => {
      text.push(callback)
      return () => text.splice(text.indexOf(callback), 1)
    },
    _subscribeBinaryLocal: (callback: (data: Uint8Array, info: ChannelPublishInfo) => void) => {
      binary.push(callback)
      return () => binary.splice(binary.indexOf(callback), 1)
    },
    _setWireTextSubscribed: RoomClientBroadcast.prototype._setWireTextSubscribed,
    send: options?.send ?? (async () => undefined),
    publish: async () => ({ key: 'fake', seq: 1, timestamp: 1 }),
    publishBinary: async () => ({ key: 'fake', seq: 1, timestamp: 1 }),
    onClose: () => {},
    _onReconnect: (callback: () => void) => {
      reconnect = callback
    },
  } as unknown as RoomClientBroadcast
  return {
    stub,
    emitText: (data, info) => text.forEach((callback) => callback(data, info)),
    emitBinary: (data, info) => binary.forEach((callback) => callback(data, info)),
    reconnect: () => reconnect(),
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

async function forceRoomGc(): Promise<void> {
  for (let cycle = 0; cycle < 8; cycle++) {
    ;(globalThis as { gc(): void }).gc()
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

async function retainOnlyListedRemote(
  target: ClientRoom,
  fake: ReturnType<typeof createFakeStub>,
  registry: GcRegistry,
  onClose: () => void,
  memberId: string,
) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  fake.emitText(
    {
      __r: 'roster',
      members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0, identity: null }],
    },
    { key: target.id, seq: 1, timestamp: 1 },
  )
  const [member] = await room.getParticipants()
  return { member: member!, room: new WeakRef(room) }
}

async function retainOnlyJoinedParticipant(target: ClientRoom, registry: GcRegistry, onClose: () => void) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  return { member: await room.join(), room: new WeakRef(room) }
}

async function retainOnlyCallbackRemote(
  target: ClientRoom,
  fake: ReturnType<typeof createFakeStub>,
  registry: GcRegistry,
  onClose: () => void,
  memberId: string,
) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  let member: ReturnType<ClientRoom['getParticipant']> extends Promise<infer T> ? NonNullable<T> : never
  const stop = room.onJoin((joined) => {
    member = joined
  })
  fake.emitText({ __r: 'join', id: memberId, meta: {}, joinedAt: 1 }, { key: target.id, seq: 1, timestamp: 1 })
  stop()
  return { member: member!, room: new WeakRef(room) }
}

async function retainOnlyDepartedParticipant(
  target: ClientRoom,
  fake: ReturnType<typeof createFakeStub>,
  registry: GcRegistry,
  onClose: () => void,
  memberId: string,
) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  const member = await room.join()
  fake.emitText({ __r: 'leave', id: memberId }, { key: target.id, seq: 1, timestamp: 1 })
  return { member, room: new WeakRef(room) }
}

async function dropJoinedRoomHandles(target: ClientRoom, registry: GcRegistry, onClose: () => void) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  await room.join()
  return new WeakRef(room)
}

type OpenRecord = {
  receiver: BackendReceiver
  localReceiverCount: () => number
  attempt: ControlledAttempt
}

class ControlledDriver implements SubscriptionDriver<string> {
  readonly opens: OpenRecord[] = []
  readonly #plans: Array<() => ControlledAttempt> = []
  partition = ''
  bindingValid = true
  openCalls = 0

  plan(plan: () => ControlledAttempt): void {
    this.#plans.push(plan)
  }

  bind(_source: string) {
    const partition = this.partition
    return {
      partition,
      valid: () => this.bindingValid,
      open: (receiver: BackendReceiver, localReceiverCount: () => number): SubscriptionAttempt => {
        this.openCalls++
        const attempt = (this.#plans.shift() ?? (() => ControlledAttempt.ready()))()
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
  readonly #readiness = deferred<void>()
  readonly #listeners = new Set<(state: SubscriptionAttemptState) => void>()
  readonly #cleanup: Promise<void>
  #state: SubscriptionAttemptState = 'establishing'

  constructor(cleanup: Promise<void> = Promise.resolve()) {
    this.ready = this.#readiness.promise
    this.#cleanup = cleanup
    void this.ready.catch(() => {})
  }

  static ready(cleanup?: Promise<void>): ControlledAttempt {
    const attempt = new ControlledAttempt(cleanup)
    attempt.establish()
    return attempt
  }

  state(): SubscriptionAttemptState {
    return this.#state
  }

  onStateChange(listener: (state: SubscriptionAttemptState) => void): () => void {
    this.#listeners.add(listener)
    return () => this.#listeners.delete(listener)
  }

  async unsubscribe(): Promise<void> {
    this.#transition('closed')
    await this.#cleanup
  }

  establish(): void {
    this.#transition('ready')
    this.#readiness.resolve()
  }

  lose(): void {
    this.#transition('lost')
  }

  close(): void {
    this.#transition('closed')
  }

  terminate(): void {
    this.#transition('terminated')
  }

  #transition(state: SubscriptionAttemptState): void {
    this.#state = state
    for (const listener of this.#listeners) listener(state)
  }
}

function terminalSubscription(inner?: BackendSubscription): {
  subscription: BackendSubscription
  close(): Promise<void>
} {
  let state = inner?.state() ?? 'ready'
  const listeners = new Set<(next: SubscriptionState) => void>()
  let closed = false
  const subscription: BackendSubscription = {
    ready: inner?.ready ?? Promise.resolve(),
    state: () => state,
    onStateChange: (listener) => {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    unsubscribe: async () => {
      if (closed) return
      closed = true
      state = 'closed'
      for (const listener of listeners) listener(state)
      listeners.clear()
      await inner?.unsubscribe()
    },
  }
  return { subscription, close: subscription.unsubscribe }
}

function rejectedSubscription(diagnostic: string): BackendSubscription {
  const ready = Promise.reject(new Error(diagnostic))
  void ready.catch(() => {})
  return {
    ready,
    state: () => 'establishing',
    onStateChange: () => () => {},
    unsubscribe: async () => {},
  }
}

function deferred<T>() {
  let resolve!: (value?: T) => void
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise as (value?: T) => void
  })
  return { promise, resolve }
}

function delayLaneSubscription(matches: (lane: LaneId) => boolean) {
  const backend = getBackend()
  const subscribeLane = backend.subscribeLane.bind(backend)
  const started = deferred<void>()
  let release!: () => Promise<void>
  const spy = vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
    if (!matches(lane)) return subscribeLane(roomId, inc, lane, receiver)
    let inner: BackendSubscription | null = null
    let state: SubscriptionState = 'establishing'
    const readiness = deferred<void>()
    const listeners = new Set<(next: SubscriptionState) => void>()
    started.resolve()
    release = async () => {
      inner = subscribeLane(roomId, inc, lane, receiver)
      await inner.ready
      state = 'ready'
      readiness.resolve()
      for (const listener of listeners) listener(state)
    }
    return {
      ready: readiness.promise,
      state: () => state,
      onStateChange: (listener) => {
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
      unsubscribe: async () => {
        state = 'closed'
        readiness.resolve()
        for (const listener of listeners) listener(state)
        await inner?.unsubscribe()
      },
    }
  })
  return { started: started.promise, release: () => release(), restore: () => spy.mockRestore() }
}
