import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ChannelClosedError } from '../channel-errors.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { IndexedPeer } from '../server/IndexedPeer.js'
import { CHANNEL_CLOSE_TIMEOUT_MS, CHANNEL_TRANSPORT } from '../constants.js'
import { ACK_STATUS, ProtocolViolationError, TAG, decode, type BroadcastSubscriptions } from '../shared-ws.js'
import { ShieldValidationError, isShieldValidationError } from '../../shared/ShieldValidationError.js'
import { Abort } from '../../shared/Abort.js'
import {
  ROOM_DEMAND_TTL_MS,
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_MEMBER_TTL_MS,
  ROOM_HORIZON_MS,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
  ROOM_TAIL_HOLD_CODE_UNITS_MAX,
  ROOM_TAIL_HOLD_MAX,
  ROOM_WANTED_TRACKS_MAX,
} from './constants.js'
import { DEFAULT_TRACK, decodeBinaryFrame, emptyTrackWants, encodeBinaryFrame, sanitizeBinaryWants } from './binary.js'
import { RoomError, isRoomError, roomAckError, toRoomFailure } from './errors.js'
import { leaveCauseFromWire, leaveCauseToWire, mergeAttributes } from './model.js'
import { hasRoomTag, type RoomSnapshotMetadata } from './protocol.js'
import { MEMBER_CELL_PREFIX, memberCellKey } from './server/cells.js'
import type { LeaveCause, Sender } from './types.js'
import { ClientRoom, ClientStandaloneParticipant } from './client.js'
import { ClientBroadcast, type ClientChannel } from '../client/channel.js'
import { RoomState, remoteBacking } from './state.js'
import { Room } from './server/statics.js'
import { ServerRoom, type ServerLocalParticipant } from './server/room.js'
import { configFromHead, decodeRoomText, encodeRoomRecord } from './server/lanes.js'
import { config } from '../../node/server/serverConfig.js'
import { config as clientConfig } from '../../client/clientConfig.js'
import type { LaneSubscription } from './server/lane-subscription.js'
import { reportRoomError } from './server/errors.js'
import { RoomParticipantStubChannel, RoomStubChannel } from './server/stub.js'
import { TailHold } from './server/tail.js'
import { RoomDemand } from './demand.js'
import { roomParticipantReplacer, roomRemoteReplacer, roomReplacer } from './response-server.js'
import { roomRemoteReviver } from './response-client.js'
import type { InternalClientReviverContext, InternalServerReplacerContext } from '../types.js'
import type { ServerChannel } from '../server/channel.js'
import type { ChannelPublishInfo } from '../channel.js'
import { disposeBackend, getBroadcastBackend, getRoomBackend, installBackend } from '../backend/install.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import { superviseRoomDriver } from '../backend/room/supervise.js'
import { DriverAttempt } from '../backend/attempt.js'
import type { RoomDriver } from '../backend/room/contract.js'
import type { LaneId } from '../backend/room/contract.js'
import type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionState,
} from '../backend/subscription.js'
import { onBug } from '../../node/server/runTelefunc/onBug.js'
import { GcRegistry } from '../gcRegistry.js'
import { wrapProxy } from '../wrapProxy.js'
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
  vi.restoreAllMocks()
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
    const backend = getRoomBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    let semanticSubscriptions = 0
    vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind === 'semantic') semanticSubscriptions++
      return subscribeLane(roomId, inc, lane, receiver)
    })
    const member = await room.join()
    expect(semanticSubscriptions).toBe(0)
    const received: unknown[] = []
    room.subscribe((data) => received.push(data))
    await vi.waitFor(() => expect(semanticSubscriptions).toBe(1))
    await member.publish('wanted')
    expect(received).toEqual(['wanted'])
  })
  it('does not let a removed member publish or receive a DM when its leave frame is lost', async () => {
    const backend = getRoomBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'control') return subscribeLane(roomId, inc, lane, receiver)
      return subscribeLane(roomId, inc, lane, (payload, info) => {
        const envelope = parse(decoder.decode(payload)) as { __r?: string }
        if (envelope.__r === 'leave') return
        receiver(payload, info)
      })
    })
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
  })
  it('keeps a joining identity undiscoverable until its member inbox subscription is ready', async () => {
    const room = await Room.create('join-inbox-readiness')
    const sender = await room.join()
    const backend = getRoomBackend()
    const commitLane = vi.spyOn(backend, 'commitLane')
    const delayed = delayLaneSubscription((lane) => lane.kind === 'inbox')
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
  })
  it('rolls durable membership back when the join announcement fails', async () => {
    const room = await Room.create('join-readiness-rollback')
    const backend = getRoomBackend()
    const commitLane = backend.commitLane.bind(backend)
    vi.spyOn(backend, 'commitLane').mockImplementation((roomId, inc, lane, payload, options) => {
      if (lane.kind === 'control' && (parse(decoder.decode(payload)) as { __r?: string }).__r === 'join') {
        throw new Error('join announcement failed')
      }
      return commitLane(roomId, inc, lane, payload, options)
    })
    await expect(room.join()).rejects.toThrow('join announcement failed')
    await expect(Room.getParticipants(room.id)).resolves.toEqual([])
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
    const joined = (await stub._handleRequest({
      __r: 'req-join',
      meta: {},
      selfDelivery: true,
    })) as { id: string }
    const request = { __r: 'req-leave' as const, id: joined.id }
    const failure = new Error('transient member delete failure')
    vi.spyOn(driver, 'compareExchangeCells').mockRejectedValueOnce(failure)
    await expect(stub._handleRequest(request)).rejects.toBe(failure)
    expect(stub._holds(joined.id)).toBe(true)
    await expect(stub._handleRequest(request)).resolves.toBeUndefined()
    expect(await Room.getParticipants(room.id)).toEqual([])
  })
  it("routes a client member's meta, attribute and DM requests through its Room stub", async () => {
    const room = (await Room.create('stub-requests')) as ServerRoom
    const stub = register(room)
    const { id } = (await stub._handleRequest({ __r: 'req-join', meta: { name: 'a' }, selfDelivery: true })) as {
      id: string
    }
    const other = await room.join()
    const inbox: unknown[] = []
    other.listen((data) => void inbox.push(data))
    await stub._handleRequest({ __r: 'req-set-meta', id, meta: { name: 'b' } })
    await stub._handleRequest({ __r: 'req-set-attrs', id, attrs: { score: 1 } })
    await stub._handleRequest({ __r: 'req-dm', id, to: other.id, data: 'hi' })
    expect((await room.getParticipants()).find((member) => member.id === id)?.meta).toEqual({ name: 'b', score: 1 })
    await vi.waitFor(() => expect(inbox).toEqual(['hi']))
  })
  it("ends a lane the driver refuses at subscribe with the driver's reason", async () => {
    const subscription = getRoomBackend().subscribeLane('refused-room', 'refused-inc', semanticLane, () => {})
    await expect(subscription.ready).rejects.toThrow("has no open incarnation 'refused-inc'")
  })
  it('relays a leave that reached this instance with no event, from a reconciled roster or a vanished record', async () => {
    const control = loseLaneFrames((lane) => lane.kind === 'control')
    const room = (await Room.create('lost-leave-owner')) as ServerRoom
    const { stub, peer } = serve(room)
    const join = async () =>
      ((await stub._handleRequest({ __r: 'req-join', meta: {}, selfDelivery: true })) as { id: string }).id
    const reconciled = await join()
    const vanished = await join()
    await vi.waitFor(() => expect(memberEvents(peer, vanished).map((event) => event.__r)).toEqual(['join']))
    const causes: unknown[] = []
    room.onLeave((_, cause) => causes.push(cause))
    const leaves = (id: string) => memberEvents(peer, id).filter((event) => event.__r === 'leave')
    control.next()
    await Room.removeParticipant(room.id, { id: reconciled })
    await subsOf(room).reconcileAuthority()
    expect(leaves(reconciled)).toEqual([{ __r: 'leave', id: reconciled, cause: 'removed' }])
    expect(stub._holds(reconciled)).toBe(false)
    control.next()
    await Room.removeParticipant(room.id, { id: vanished })
    await subsOf(room)._heartbeatTick()
    expect(leaves(vanished)).toEqual([{ __r: 'leave', id: vanished, cause: 'removed' }])
    expect(causes).toEqual([{ type: 'removed' }, { type: 'removed' }])
  })
  it("relays a room meta update that reached this instance only through the authority's reconcile", async () => {
    const control = loseLaneFrames((lane) => lane.kind === 'control')
    const room = (await Room.create('lost-update', { meta: { topic: 'old' } })) as unknown as ServerRoom
    const { peer } = serve(room)
    await new Promise((resolve) => setTimeout(resolve, 0))
    control.next()
    await Room.setMeta(room.id, { topic: 'new' })
    await subsOf(room).reconcileAuthority()
    const updates = peer
      .decoded()
      .filter((frame) => frame.tag === TAG.PUBLISH)
      .map((frame) => JSON.parse(frame.text) as { __r: string; meta?: unknown })
      .filter((event) => event.__r === 'update')
    expect(updates.map((event) => event.meta)).toEqual([{ topic: 'new' }])
  })
  it('tells its clients about its own join, meta change and leave even when the lane loses their echoes', async () => {
    const control = loseLaneFrames((lane) => lane.kind === 'control')
    const room = (await Room.create('own-events-lost-echo')) as ServerRoom
    const { peer } = serve(room)
    await subsOf(room).reconcileAuthority()
    control.where(() => true)
    const member = await room.join({ meta: { v: 1 } })
    expect(clientView(peer, room.id)._getRemote(member.id)?.meta).toEqual({ v: 1 })
    await member.setMeta({ v: 2 })
    expect(clientView(peer, room.id)._getRemote(member.id)?.meta).toEqual({ v: 2 })
    await member.leave()
    expect(clientView(peer, room.id)._getRemote(member.id)).toBeNull()
  })
  it('tells its clients about a meta change it committed but failed to announce', async () => {
    const room = (await Room.create('unannounced-meta')) as ServerRoom
    const { peer } = serve(room)
    const member = await room.join({ meta: { v: 1 } })
    vi.spyOn(driver, 'commitLane').mockRejectedValueOnce(new Error('publish lost'))
    await expect(member.setMeta({ v: 2 })).rejects.toThrow('publish lost')
    expect(clientView(peer, room.id)._getRemote(member.id)?.meta).toEqual({ v: 2 })
  })
  it("relays a hidden member's reconciled meta to the clients handed it", async () => {
    const room = (await Room.create('lost-hidden-meta')) as ServerRoom
    const holder = await Room.get(room.id)
    const bot = await holder.join({ hidden: true, meta: { mood: 'old' } })
    const control = loseLaneFrames((lane) => lane.kind === 'control')
    const handed = new RoomStubChannel(room, { grants: { selfSuppressed: new Set(), hidden: new Set([bot.id]) } })
    handed._registerChannel()
    room._attachStub(handed)
    const peer = attachPeer(handed)
    await subsOf(room).reconcileAuthority()
    control.next()
    await bot.setMeta({ mood: 'new' })
    await subsOf(room).reconcileAuthority()
    const metas = peer
      .decoded()
      .filter((frame) => frame.tag === TAG.PUBLISH)
      .map((frame) => JSON.parse(frame.text) as { __r: string; members?: Array<{ id: string; meta: unknown }> })
      .flatMap((event) => (event.__r === 'roster' ? event.members! : []))
      .filter((member) => member.id === bot.id)
      .map((member) => member.meta)
    expect(metas.at(-1)).toEqual({ mood: 'new' })
  })
  it("relays a hidden member's event-less leave to no client it wasn't handed to", async () => {
    const control = loseLaneFrames((lane) => lane.kind === 'control')
    const room = (await Room.create('lost-hidden-leave')) as ServerRoom
    const bot = await room.join({ hidden: true })
    const { peer } = serve(room)
    const causes: unknown[] = []
    bot.onLeave((cause) => causes.push(cause?.type))
    await new Promise((resolve) => setTimeout(resolve, 0))
    control.next()
    await Room.removeParticipant(room.id, { id: bot.id })
    await subsOf(room)._heartbeatTick()
    expect(causes).toEqual(['removed'])
    expect(memberEvents(peer, bot.id)).toEqual([])
  })
  it("reports a leave as 'left' when the owner's heartbeat lands while the eviction finishes", async () => {
    const room = (await Room.create('leave-during-heartbeat')) as ServerRoom
    const member = await room.join()
    const causes: unknown[] = []
    member.onLeave((cause) => causes.push(cause))
    room.onLeave((_, cause) => causes.push(cause))
    const listRetained = driver.listRetained.bind(driver)
    // The eviction's first retained read: its member delete committed, its leave isn't published.
    vi.spyOn(driver, 'listRetained').mockImplementationOnce(async (roomId, inc) => {
      await subsOf(room)._heartbeatTick()
      return listRetained(roomId, inc)
    })
    await member.leave()
    expect(causes).toEqual([{ type: 'left' }, { type: 'left' }])
  })
  it("reports a leave as 'left' on another instance whose roster read overlaps the eviction", async () => {
    const room = (await Room.create('leave-during-roster-read')) as ServerRoom
    const member = await room.join()
    const observer = (await Room.get(room.id)) as ServerRoom
    await observer.getParticipants()
    // The leave reaches this instance after its roster read, as over a networked backend.
    const observed = holdLaneDelivery((lane) => lane.kind === 'control')
    const causes: unknown[] = []
    observer.onLeave((_, cause) => causes.push(cause))
    await subsOf(observer).reconcileAuthority()
    const readCells = driver.readCells.bind(driver)
    const memberRead = { started: deferred<void>(), release: deferred<void>(), held: false }
    vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
      if (!memberRead.held && 'prefix' in selector && selector.prefix === MEMBER_CELL_PREFIX) {
        memberRead.held = true
        memberRead.started.resolve()
        await memberRead.release.promise
      }
      return readCells(roomId, inc, selector)
    })
    const listRetained = driver.listRetained.bind(driver)
    const committed = deferred<void>()
    const publish = deferred<void>()
    vi.spyOn(driver, 'listRetained').mockImplementationOnce(async (roomId, inc) => {
      committed.resolve()
      await publish.promise
      return listRetained(roomId, inc)
    })
    const refresh = subsOf(observer).reconcileAuthority()
    await memberRead.started.promise
    const leaving = member.leave()
    await committed.promise
    memberRead.release.resolve()
    await refresh
    await observed.release()
    publish.resolve()
    await leaving
    expect(causes).toEqual([{ type: 'left' }])
  })
  it('lists without a room that closes between its head read and its roster read', async () => {
    await Room.create('list-open')
    const closing = (await Room.create('list-closing')) as ServerRoom
    const readCells = driver.readCells.bind(driver)
    let closed = false
    vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
      if (roomId === closing.id && !closed) {
        closed = true
        await Room.close(closing.id)
      }
      return readCells(roomId, inc, selector)
    })
    expect((await Room.list()).map(({ id }) => id)).toEqual(['list-open'])
  })
  it('creates a room whose head went away between its read and the create, as a lapsing tombstone does', async () => {
    const lapsed = { conflict: true, current: null } as unknown as Awaited<
      ReturnType<MemoryBackend['compareExchangeHead']>
    >
    vi.spyOn(driver, 'compareExchangeHead').mockResolvedValueOnce(lapsed)
    expect((await Room.getOrCreate('lapsed-get-or-create')).isClosed).toBe(false)
    vi.spyOn(driver, 'compareExchangeHead').mockResolvedValueOnce(lapsed)
    expect((await Room.create('lapsed-create')).isClosed).toBe(false)
  })
  it('creates, lists, updates, closes fully, and recreates a genuinely fresh domain', async () => {
    const room = (await Room.create('lifecycle', { meta: { topic: 'one' } })) as unknown as ServerRoom
    const firstInc = room._inc
    const me = await room.join({ meta: { name: 'Alice' } })
    const observer = await Room.get('lifecycle')
    let closed = 0
    let memberLeaves = 0
    observer.onClose(() => closed++)
    me.onLeave(() => memberLeaves++)
    await Room.setMeta('lifecycle', { topic: 'two', draft: true })
    await Room.setAttributes('lifecycle', { draft: undefined, pinned: true })
    expect(observer.meta).toEqual({ topic: 'two', pinned: true })
    expect((await Room.list()).map(({ id }) => id)).toContain('lifecycle')
    await Room.close('lifecycle')
    expect(closed).toBe(1)
    expect(memberLeaves).toBe(1)
    expect(await driver.readHead('lifecycle')).toMatchObject({ state: 'closed', currentInc: null })
    expect([...memoryState.rooms.get('lifecycle')!.gens.keys()]).toEqual([])
    expect((await driver.directoryList('lifecycle')).entries).toEqual([])
    await expect(room.join()).rejects.toThrow(/closed/i)
    await expect(Room.get('lifecycle')).rejects.toThrow('Room not found')
    const recreated = (await Room.create('lifecycle')) as ServerRoom
    expect(recreated._inc).not.toBe(firstInc)
    expect(await recreated.getParticipants()).toEqual([])
  })
  it('reports directory registration failure and repairs the open head on getOrCreate', async () => {
    const put = driver.directoryPut.bind(driver)
    let attempts = 0
    vi.spyOn(driver, 'directoryPut').mockImplementation(async (roomId, inc) => {
      if (roomId === 'index-repair' && attempts++ === 0) throw new Error('index registration failure')
      await put(roomId, inc)
    })
    await expect(Room.create('index-repair')).rejects.toThrow('index registration failure')
    const open = (await driver.readHead('index-repair'))!
    expect(open).toMatchObject({ state: 'open', currentInc: expect.any(String) })
    expect(((await Room.getOrCreate('index-repair')) as ServerRoom)._inc).toBe(open.currentInc)
    expect((await Room.list()).map(({ id }) => id)).toContain('index-repair')
  })
  it('waits for an active close lease and takes over until the head is closed', async () => {
    vi.useFakeTimers()
    const room = (await Room.create('concurrent-close')) as ServerRoom
    const current = (await driver.readHead(room.id))!
    const leased = await driver.compareExchangeHead(
      room.id,
      { form: 'rev', rev: current.rev },
      {
        head: {
          currentInc: current.currentInc,
          state: 'closing',
          config: encodeRoomRecord(configFromHead(current)),
          closeLease: { id: 'stalled-closer', durationMs: 1_000 },
        },
      },
    )
    expect(leased).toMatchObject({ head: { state: 'closing' } })
    if (!('head' in leased)) throw new Error('expected an active close lease')
    let settled = false
    const closing = Room.close(room.id).then(() => {
      settled = true
    })
    await vi.advanceTimersByTimeAsync(900)
    expect(settled).toBe(false)
    expect(await driver.readHead(room.id)).toMatchObject({
      currentInc: room._inc,
      state: 'closing',
      closeLease: { id: leased.head.closeLease?.id },
    })
    await vi.advanceTimersByTimeAsync(200)
    await closing
    expect(await driver.readHead(room.id)).toMatchObject({ state: 'closed', currentInc: null })
  })
  it('retries generation cleanup after the head was durably finalized', async () => {
    const room = (await Room.create('finalized-cleanup-retry')) as ServerRoom
    const inc = room._inc
    await room.join()
    vi.spyOn(driver, 'dropGeneration').mockRejectedValueOnce(new Error('transient generation cleanup failure'))
    await expect(Room.close(room.id)).rejects.toThrow('transient generation cleanup failure')
    expect(await driver.readHead(room.id)).toMatchObject({ state: 'closed', currentInc: null })
    expect([...memoryState.rooms.get(room.id)!.gens.keys()]).toEqual([inc])
    await expect(Room.close(room.id)).resolves.toBeUndefined()
    expect([...memoryState.rooms.get(room.id)!.gens.keys()]).toEqual([])
    expect((await driver.directoryList(room.id)).entries).toEqual([])
  })
  it('reports an active close lease and lets the caller retry after its deadline', async () => {
    vi.useFakeTimers()
    const room = (await Room.create('get-or-create-closing')) as ServerRoom
    const firstInc = room._inc
    const current = (await driver.readHead(room.id))!
    await driver.compareExchangeHead(
      room.id,
      { form: 'rev', rev: current.rev },
      {
        head: {
          currentInc: current.currentInc,
          state: 'closing',
          config: encodeRoomRecord(configFromHead(current)),
          closeLease: { id: 'active-get-or-create-close', durationMs: 1_000 },
        },
      },
    )
    await expect(Room.getOrCreate(room.id)).rejects.toThrow(`Room is closing: ${room.id}`)
    expect(await driver.readHead(room.id)).toMatchObject({
      currentInc: firstInc,
      state: 'closing',
      closeLease: { id: 'active-get-or-create-close' },
    })
    await vi.advanceTimersByTimeAsync(1_100)
    const recreated = (await Room.getOrCreate(room.id)) as ServerRoom
    expect(recreated._inc).not.toBe(firstInc)
    expect(recreated.isClosed).toBe(false)
  })
  it('tears down an observing instance when another instance closes the room', async () => {
    const authority = await Room.create('remote-close-teardown')
    authority.onAnnounce(() => {})
    const remoteBackend = getRoomBackend()
    const unsubscribed: string[] = []
    const observedLanes = new Set<string>()
    const observationReady = deferred<void>()
    const observationStopped = deferred<void>()
    const subscribeLane = remoteBackend.subscribeLane.bind(remoteBackend)
    vi.spyOn(remoteBackend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
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
    const observer = await Room.get('remote-close-teardown')
    observer.onAnnounce(() => {})
    const closed = deferred<void>()
    observer.onClose(() => closed.resolve())
    await observationReady.promise
    await Room.close('remote-close-teardown')
    await Promise.all([closed.promise, observationStopped.promise])
    expect(observer.isClosed).toBe(true)
    expect(unsubscribed.sort()).toEqual(['control', 'semantic'])
  })
  it('replaces a still-demanded Room subscription after its supervised source closes terminally', async () => {
    const authority = await Room.create('terminal-subscription-recovery')
    const publisher = await authority.join()
    const observer = await Room.get(authority.id)
    const backend = getRoomBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    let terminal: ReturnType<typeof terminalSubscription> | undefined
    const replacementReady = deferred<void>()
    vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'semantic') return subscribeLane(roomId, inc, lane, receiver)
      if (terminal === undefined) {
        terminal = terminalSubscription()
        return terminal.subscription
      }
      const replacement = subscribeLane(roomId, inc, lane, receiver)
      void replacement.ready.then(() => replacementReady.resolve())
      return replacement
    })
    const received: unknown[] = []
    observer.subscribe((data) => received.push(data))
    if (!terminal) throw new Error('semantic subscription did not start')
    await terminal.subscription.ready
    await terminal.close()
    await replacementReady.promise
    await publisher.publish('after-recovery')
    expect(received).toEqual(['after-recovery'])
  })
  it("reports a lane that ends after it was ready, with the driver's reason as the cause", async () => {
    const room = (await Room.create('terminal-reason')) as ServerRoom
    const reason = new Error('generation invalidated')
    let end: ((reason: Error) => void) | undefined
    const bind = driver.subscriptions.bind.bind(driver.subscriptions)
    vi.spyOn(driver.subscriptions, 'bind').mockImplementation((source) => {
      const binding = bind(source)
      if (!('lane' in source) || source.lane.kind !== 'semantic' || end) return binding
      return {
        ...binding,
        open: (receiver, localReceiverCount) => {
          const inner = binding.open(receiver, localReceiverCount)
          const listeners = new Set<(state: SubscriptionState, reason?: Error) => void>()
          let ended = false
          end = (error) => {
            ended = true
            for (const listener of listeners) listener('closed', error)
          }
          return {
            state: () => (ended ? 'closed' : inner.state()),
            onStateChange: (listener) => {
              listeners.add(listener)
              return inner.onStateChange(listener)
            },
            unsubscribe: () => inner.unsubscribe(),
          }
        },
      }
    })
    const bugs: unknown[] = []
    onBug((err) => bugs.push(err))
    vi.spyOn(console, 'error').mockImplementation(() => {})
    room.subscribe(() => {})
    await subsOf(room)._semantic.ready
    end!(reason)
    await vi.waitFor(() => expect(bugs).toContainEqual(expect.objectContaining({ cause: reason })))
  })
  it('does not re-subscribe a recovered lane when its catch-up reconcile fails', async () => {
    const authority = await Room.create('recovered-reconcile-failure')
    const observer = (await Room.get(authority.id)) as ServerRoom
    const backend = getRoomBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    let terminal: ReturnType<typeof terminalSubscription> | undefined
    let semanticOpens = 0
    vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'semantic') return subscribeLane(roomId, inc, lane, receiver)
      semanticOpens++
      if (terminal === undefined) return (terminal = terminalSubscription()).subscription
      return subscribeLane(roomId, inc, lane, receiver)
    })
    observer.subscribe(() => {})
    if (!terminal) throw new Error('semantic subscription did not start')
    await terminal.subscription.ready
    await observer.getParticipants()
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const reconcile = vi.spyOn(subsOf(observer), 'reconcileAuthority').mockRejectedValue(new Error('contention'))
    await terminal.close()
    await vi.waitFor(() => expect(reconcile).toHaveBeenCalled())
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(semanticOpens).toBe(2)
  })
  it('replaces a recovered lane again when it ends while the recovery catches up', async () => {
    const authority = await Room.create('recovered-lane-ends-again')
    const observer = (await Room.get(authority.id)) as ServerRoom
    const backend = getRoomBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const terminals: Array<ReturnType<typeof terminalSubscription>> = []
    let semanticOpens = 0
    vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'semantic') return subscribeLane(roomId, inc, lane, receiver)
      semanticOpens++
      if (terminals.length === 2) return subscribeLane(roomId, inc, lane, receiver)
      const terminal = terminalSubscription()
      terminals.push(terminal)
      return terminal.subscription
    })
    observer.subscribe(() => {})
    await observer.getParticipants()
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const subs = subsOf(observer)
    const reconcile = subs.reconcileAuthority.bind(subs)
    const catchingUp = deferred<void>()
    const release = deferred<void>()
    vi.spyOn(subs, 'reconcileAuthority').mockImplementationOnce(async () => {
      catchingUp.resolve()
      await release.promise
      await reconcile()
    })
    await terminals[0]!.close()
    await catchingUp.promise
    await terminals[1]!.close()
    release.resolve()
    await vi.waitFor(() => expect(semanticOpens).toBe(3))
  })
  it('closes a view and tells its clients when a terminal control lane lost the closed frame', async () => {
    const authority = await Room.create('terminal-close-relay')
    await authority.join()
    const backend = getRoomBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    let terminal: ReturnType<typeof terminalSubscription> | undefined
    vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      if (lane.kind !== 'control') return subscribeLane(roomId, inc, lane, receiver)
      const withoutClosed = (payload: Uint8Array, info: { seq: number; timestamp: number }) => {
        if ((parse(decoder.decode(payload)) as { __r?: string }).__r !== 'closed') receiver(payload, info)
      }
      terminal ??= terminalSubscription(subscribeLane(roomId, inc, lane, withoutClosed))
      return terminal.subscription
    })
    const observer = (await Room.get(authority.id)) as ServerRoom
    let closes = 0
    observer.onClose(() => closes++)
    const { peer } = serve(observer)
    if (!terminal) throw new Error('control subscription did not start')
    await terminal.subscription.ready
    await Room.close(authority.id)
    await terminal.close()
    await vi.waitFor(() => expect(observer.isClosed).toBe(true))
    expect({ count: observer.count, closes }).toEqual({ count: 0, closes: 1 })
    const relayed = peer
      .decoded()
      .filter((frame) => frame.tag === TAG.PUBLISH)
      .map((frame) => (JSON.parse(frame.text) as { __r: string }).__r)
    expect(relayed).toContain('closed')
  })
  it('applies a control frame whose seq restarted, as after a Redis restart without its data', async () => {
    let deliver!: BackendReceiver
    mockLaneSubscription('control', (subscribeLane, roomId, inc, lane, receiver) => {
      deliver = receiver
      return subscribeLane(roomId, inc, lane, receiver)
    })
    const room = await Room.create('control-seq-restart')
    room.onUpdate(() => {})
    const at = Date.now() + 1000
    deliver(encodeRoomRecord({ __r: 'update', meta: { step: 1 }, at, by: 'a' }), { seq: 5, timestamp: at })
    deliver(encodeRoomRecord({ __r: 'update', meta: { step: 2 }, at: at + 1, by: 'a' }), { seq: 1, timestamp: at + 1 })
    expect(room.meta).toEqual({ step: 2 })
  })
  it('reconciles authority after a same-attempt recovery', async () => {
    const room = (await Room.create('control-reconcile')) as ServerRoom
    const backend = getRoomBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const controlSubscribed = deferred<void>()
    let transition!: (state: SubscriptionState) => void
    vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
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
    room.onAnnounce(() => {})
    await controlSubscribed.promise
    const member = await room.join()
    await room.getParticipants()
    room._state.applyLeave(member.id, { type: 'left' })
    expect(room.count).toBe(0)
    transition('lost')
    transition('ready')
    await vi.waitFor(() => expect(room.count).toBe(1))
    expect((await room.getParticipants()).map(({ id }) => id)).toEqual([member.id])
  })
  it('sends commits held for an establishing lane before any commit that arrives once it is ready', async () => {
    class ManualAttempt extends DriverAttempt {
      async unsubscribe() {
        this.transition('closed')
      }
      ready() {
        this.transition('ready')
      }
    }
    // Whatever the microtask distance of the later commit from the lane's readiness.
    for (let distance = 0; distance < 12; distance++) {
      const order: string[] = []
      let attempt!: ManualAttempt
      const driver = {
        subscriptions: { bind: () => ({ partition: '', open: () => (attempt = new ManualAttempt()) }) },
        commitLane: async (_roomId: string, _inc: string, _lane: LaneId, payload: Uint8Array) => {
          order.push(decoder.decode(payload))
          return { accepted: true, seq: order.length, timestamp: 1, delivery: Promise.resolve() }
        },
      } as unknown as RoomDriver
      const backend = superviseRoomDriver(driver)
      const subscription = backend.subscribeLane('room', 'inc', semanticLane, () => {})
      const held = ['a', 'b'].map((text) => backend.commitLane('room', 'inc', semanticLane, encoder.encode(text)))
      attempt.ready()
      let later: Promise<unknown> = Promise.resolve()
      for (let hop = 0; hop < distance; hop++) later = later.then(() => {})
      const overtaking = later.then(() => backend.commitLane('room', 'inc', semanticLane, encoder.encode('c')))
      await Promise.all([...held, overtaking])
      expect(order).toEqual(['a', 'b', 'c'])
      await subscription.unsubscribe()
      await backend.dispose()
    }
  })
  it('a publish right after a subscribe on this instance reaches it while the lane is still establishing', async () => {
    const room = (await Room.create('establishing-hold')) as ServerRoom
    const member = await room.join()
    const release = delayDriverLane((lane) => lane.kind === 'semantic')
    const received: unknown[] = []
    room.subscribe((data) => received.push(data))
    const publishing = member.publish('first')
    await new Promise((resolve) => setTimeout(resolve, 0))
    release()
    await publishing
    await vi.waitFor(() => expect(received).toEqual(['first']))
  })
  it('reads authority while the control subscription is establishing', async () => {
    const authority = await Room.create('establishing-roster')
    const observer = (await Room.get(authority.id)) as ServerRoom
    expect(await observer.getParticipants()).toEqual([])
    const readiness = deferred<void>()
    const slot = subsOf(observer)._control
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
  it("loads a fresh view's first roster while members' meta changes keep arriving", async () => {
    const authority = await Room.create('first-roster-meta-traffic')
    const player = await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    const readCells = driver.readCells.bind(driver)
    let score = 0
    vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
      const result = await readCells(roomId, inc, selector)
      // A member's meta change reaches this view during every roster read.
      if ('prefix' in selector && selector.prefix === MEMBER_CELL_PREFIX && score < 100)
        await player.setAttributes({ score: ++score })
      return result
    })
    vi.spyOn(console, 'error').mockImplementation(() => {})
    observer.onJoin(() => {})
    // The last change reached this view after the read that loaded it, and is newer than what the read saw.
    expect((await observer.getParticipants()).map(({ id, meta }) => ({ id, meta }))).toEqual([
      { id: player.id, meta: { score } },
    ])
  })
  it('makes roster readers join one bounded authoritative refresh', async () => {
    const authority = (await Room.create('roster-refresh-owner')) as ServerRoom
    await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    observer.onJoin(() => {})
    await observer.getParticipants()
    await vi.waitFor(() => expect(subsOf(observer)._control.established).toBe(true))
    expect(observer._state.rosterKnown).toBe(true)
    const readCells = driver.readCells.bind(driver)
    const started = deferred<void>()
    const release = deferred<void>()
    let held = false
    let churn = 0
    vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
      if (!held && 'prefix' in selector && selector.prefix === MEMBER_CELL_PREFIX) {
        held = true
        started.resolve()
        await release.promise
      }
      const result = await readCells(roomId, inc, selector)
      if (churn-- > 0 && 'prefix' in selector) observer._state.membershipVersion++
      return result
    })
    const refresh = subsOf(observer)._refreshMembers()
    await started.promise
    expect(subsOf(observer)._control.established).toBe(true)
    const participants = observer.getParticipants()
    const status = await Promise.race([
      participants.then(() => 'settled' as const),
      new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), 0)),
    ])
    expect(status).toBe('pending')
    release.resolve()
    await refresh
    expect((await participants).map(({ id }) => id)).toHaveLength(1)
    churn = 20
    await expect(subsOf(observer)._refreshMembers()).rejects.toThrow('Room roster refresh contention')
  })
  it('heartbeats pure control observers without owned members or binary demand', async () => {
    vi.useFakeTimers()
    const observer = (await Room.get((await Room.create('observer-heartbeat')).id)) as ServerRoom
    const heartbeat = vi.spyOn(subsOf(observer), '_heartbeatTick').mockResolvedValue()
    observer.onJoin(() => {})
    await vi.advanceTimersByTimeAsync(ROOM_HEARTBEAT_INTERVAL_MS)
    expect(heartbeat).toHaveBeenCalledOnce()
  })
  it("resubscribes a pure observer's control lane on the heartbeat when its recovery fails after a replan", async () => {
    vi.useFakeTimers()
    const observer = (await Room.get((await Room.create('observer-recovery-heartbeat')).id)) as ServerRoom
    const first = terminalSubscription()
    let opens = 0
    mockLaneSubscription('control', (subscribeLane, roomId, inc, lane, receiver) => {
      opens++
      if (opens === 1) return first.subscription
      if (opens === 2) return rejectedSubscription('replacement refused')
      return subscribeLane(roomId, inc, lane, receiver)
    })
    vi.spyOn(console, 'error').mockImplementation(() => {})
    observer.onJoin(() => {})
    const headRead = deferred<void>()
    const readOpenConfig = observer._readOpenConfig.bind(observer)
    vi.spyOn(observer, '_readOpenConfig').mockImplementationOnce(async () => {
      await headRead.promise
      return readOpenConfig()
    })
    await first.close()
    // A replan while the recovery reads the head.
    observer.onLeave(() => {})
    headRead.resolve()
    await vi.advanceTimersByTimeAsync(100)
    expect(opens).toBe(2)
    await vi.advanceTimersByTimeAsync(ROOM_HEARTBEAT_INTERVAL_MS)
    expect(opens).toBe(3)
    expect(subsOf(observer)._control.established).toBe(true)
  })
  it("reports a lane's replacement that ends before it is ready once", async () => {
    vi.useFakeTimers()
    const observer = await Room.get((await Room.create('replacement-reported-once')).id)
    let attempts = 0
    mockLaneSubscription('semantic', (subscribeLane, roomId, inc, lane, receiver) => {
      attempts++
      return attempts < 3 ? rejectedSubscription(`attempt ${attempts}`) : subscribeLane(roomId, inc, lane, receiver)
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    observer.subscribe(() => {})
    await vi.advanceTimersByTimeAsync(100)
    expect(report.mock.calls.map(([logged]) => String(logged).split('\n')[0])).toEqual([
      'Error: attempt 1',
      'Error: attempt 2',
    ])
  })
  it('retries a still-wanted lost subscription on the next planning pass', async () => {
    vi.useFakeTimers()
    const observer = await Room.get((await Room.create('single-recovery-horizon')).id)
    let attempts = 0
    mockLaneSubscription('semantic', (subscribeLane, roomId, inc, lane, receiver) => {
      attempts++
      return attempts < 3 ? rejectedSubscription(`attempt ${attempts}`) : subscribeLane(roomId, inc, lane, receiver)
    })
    vi.spyOn(console, 'error').mockImplementation(() => {})
    observer.subscribe(() => {})
    await vi.advanceTimersByTimeAsync(100)
    // The lane and its one replacement; the next attempt waits for the heartbeat.
    expect(attempts).toBe(2)
    await vi.advanceTimersByTimeAsync(ROOM_HEARTBEAT_INTERVAL_MS)
    expect(attempts).toBe(3)
    expect(subsOf(observer as ServerRoom)._semantic.established).toBe(true)
    expect(observer.isClosed).toBe(false)
  })
  it('keeps an authoritative open Room open after subscription recovery exhausts', async () => {
    const observer = (await Room.get((await Room.create('open-recovery-exhaustion')).id)) as ServerRoom
    const { backend } = rejectLaneSubscriptions('semantic', 'persistent subscription failure')
    const onClose = vi.fn()
    observer.onClose(onClose)
    observer.subscribe(() => {})
    await vi.advanceTimersByTimeAsync(ROOM_HORIZON_MS + 100)
    const textSlot = subsOf(observer)._semantic
    expect((await backend.readHead(observer.id))?.state).toBe('open')
    expect({ closed: observer.isClosed, onClose: onClose.mock.calls.length }).toEqual({ closed: false, onClose: 0 })
    expect(textSlot).toMatchObject({ wanted: true, active: false })
  })
  it('settles an in-flight join when its inbox recovery horizon exhausts', async () => {
    const room = (await Room.create('join-recovery-exhaustion')) as ServerRoom
    const { started } = rejectLaneSubscriptions('inbox', 'persistent inbox subscription failure')
    const outcome = captureOutcome(room.join())
    await started
    await vi.advanceTimersByTimeAsync(ROOM_HORIZON_MS + 100)
    expect(outcome.value).toBeInstanceOf(RoomError)
    expect(room.isClosed).toBe(false)
    expect(room.count).toBe(0)
  })
  it('settles retained replay when its semantic recovery horizon exhausts', async () => {
    const authority = (await Room.create('replay-recovery-exhaustion')) as ServerRoom
    const publisher = await authority.join()
    await publisher.publish('retained', { retain: true })
    const observer = (await Room.get(authority.id)) as ServerRoom
    const { stub } = serve(observer)
    const { started } = rejectLaneSubscriptions('semantic', 'persistent semantic subscription failure')
    stub._onPeerSubscription('text', true)
    const outcome = captureOutcome(observer._replayRetainedText(stub, { all: false, members: [] }))
    await started
    await vi.advanceTimersByTimeAsync(ROOM_HORIZON_MS + 100)
    expect(outcome.value).toBeInstanceOf(RoomError)
    expect(observer.isClosed).toBe(false)
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
    authority.onLeave((_member, cause) => Reflect.set(cause!, 'type', 'closed'))
    await player.setAttributes({ name: 'Alicia', score: 1 })
    expect(observer.count).toBe(1)
    expect((await observer.getParticipants()).map((member) => member.id)).toEqual([player.id])
    expect((await observer.getParticipants({ hidden: true })).map((member) => member.id)).toEqual([hidden.id])
    expect(events).toEqual(['join:Alice', 'update:Alicia'])
    const causes: unknown[] = []
    player.onLeave((cause) => Reflect.set(cause, 'type', 'disconnected'))
    player.onLeave((cause) => causes.push(cause))
    await Room.removeParticipant('presence', { identity: 'user-1', reason: 'moderated' })
    player.onLeave((cause) => causes.push(cause))
    expect(causes).toEqual(Array(2).fill({ type: 'removed', reason: 'moderated' }))
    expect(causes.every(Object.isFrozen)).toBe(true)
    expect(observer.count).toBe(0)
    expect(events.at(-1)).toBe('leave:Alicia')
    expect((await observer.getParticipants({ hidden: true })).map((member) => member.id)).toEqual([hidden.id])
  })
  it('rejects exact sends to an expired member and excludes it from static presence', async () => {
    const room = (await Room.create('expired-static-presence')) as ServerRoom
    const member = await room.join()
    const memberKey = memberCellKey(member.id)
    const read = await driver.readCells(room.id, room._inc, { keys: [memberKey] })
    expect('staleInc' in read).toBe(false)
    if ('staleInc' in read) throw new Error('unexpected stale generation')
    const record = parse(decoder.decode(read.cells.get(memberKey)!)) as Record<string, unknown>
    await expect(
      driver.compareExchangeCells(room.id, room._inc, read.revision, [
        {
          key: memberKey,
          bytes: encoder.encode(stringify({ ...record, seenAt: Date.now() - ROOM_MEMBER_TTL_MS - 1 })),
        },
      ]),
    ).resolves.toBe('committed')
    const commitLane = vi.spyOn(driver, 'commitLane')
    await expect(Room.send(room.id, { id: member.id }, 'post-crash')).rejects.toThrow('Participant not found')
    expect(commitLane.mock.calls.some(([, , lane]) => lane.kind === 'inbox')).toBe(false)
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
    vi.spyOn(driver, 'commitLane').mockImplementation(async (roomId, inc, lane, payload, options) => {
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
  })
  it('keeps a member whose heartbeat wins the expired-record reap race', async () => {
    const room = (await Room.create('reap-heartbeat-race')) as ServerRoom
    const member = await room.join()
    const memberKey = memberCellKey(member.id)
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
          bytes: encoder.encode(stringify({ ...initialRecord, seenAt: Date.now() - ROOM_MEMBER_TTL_MS - 1 })),
        },
      ]),
    ).resolves.toBe('committed')
    let raced = false
    vi.spyOn(driver, 'compareExchangeCells').mockImplementation(async (roomId, inc, revision, mutations) => {
      if (!raced && mutations.some(({ key, bytes }) => key === memberKey && bytes === null)) {
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
              bytes: encoder.encode(stringify({ ...record, seenAt: Date.now() })),
            },
          ]),
        ).resolves.toBe('committed')
      }
      return compareExchange(roomId, inc, revision, mutations)
    })
    expect((await Room.getParticipants(room.id)).map(({ id }) => id)).toEqual([member.id])
    expect(raced).toBe(true)
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
  it("drops a co-returned participant's echo on the room stub even across Room instances", async () => {
    await Room.create('self-suppress-by-id')
    const me = (await Room.join('self-suppress-by-id', { selfDelivery: false })) as ServerLocalParticipant
    const room = (await Room.get('self-suppress-by-id')) as ServerRoom
    const channels: ServerChannel[] = []
    const context = replacerContext(channels)
    roomReplacer.replace(room, context)
    roomParticipantReplacer.replace(me, context)
    const stub = channels.find((channel) => channel instanceof RoomStubChannel) as RoomStubChannel
    const peer = attachPeer(stub)
    stub._onPeerSubscription('text', true)
    const other = await room.join()
    await me.publish('echo')
    await other.publish('marker')
    await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toContain('marker'))
    expect(semanticFrames(peer, 'data')).not.toContain('echo')
  })
  it("replays no self-suppressed member's own retained frames to its client", async () => {
    await Room.create('self-suppress-retained')
    const me = (await Room.join('self-suppress-retained', { selfDelivery: false })) as ServerLocalParticipant
    await me.publish('mine', { retain: true })
    await me.publishBinary(new Uint8Array([1]), { track: 'screen', retain: true })
    const room = (await Room.get('self-suppress-retained')) as ServerRoom
    const channels: ServerChannel[] = []
    const context = replacerContext(channels)
    roomReplacer.replace(room, context)
    roomParticipantReplacer.replace(me, context)
    const stub = channels.find((channel) => channel instanceof RoomStubChannel) as RoomStubChannel
    const peer = attachPeer(stub)
    const replayText = vi.spyOn(room, '_replayRetainedText')
    const replayBinary = vi.spyOn(room, '_replayRetainedBinary')
    stub._onPeerSubscription('text', true)
    declare(stub, {
      __r: 'sub-binary',
      wants: { everyMember: { all: true, tracks: [] }, members: {} },
    })
    await Promise.all([...replayText.mock.results, ...replayBinary.mock.results].map(({ value }) => value))
    expect(semanticFrames(peer, 'data')).toEqual([])
    expect(peer.decoded().filter((frame) => frame.tag === TAG.PUBLISH_BINARY)).toEqual([])
  })
  it("relays none of a hidden member's events from an instance that has not loaded the roster", async () => {
    const authority = await Room.create('hidden-pre-roster')
    const bot = await authority.join({ hidden: true })
    const observer = (await Room.get(authority.id)) as ServerRoom
    const roster = delayRosterRead(authority.id)
    try {
      const { peer } = serve(observer)
      const before = await authority.join()
      await vi.waitFor(() =>
        expect(memberEvents(peer, before.id)).toContainEqual(expect.objectContaining({ __r: 'join' })),
      )
      expect(observer._state.rosterKnown).toBe(false)
      await bot.setMeta({ secret: true })
      await bot.publishBinary(new Uint8Array([1]), { track: 'hidden-track' })
      await bot.leave()
      const after = await authority.join()
      await vi.waitFor(() =>
        expect(memberEvents(peer, after.id)).toContainEqual(expect.objectContaining({ __r: 'join' })),
      )
      expect(memberEvents(peer, bot.id)).toEqual([])
    } finally {
      roster.release()
    }
  })
  it("relays a returned hidden participant's updates and leave to that response's client only", async () => {
    const room = (await Room.create('hidden-grant')) as ServerRoom
    const bot = await room.join({ hidden: true })
    const [remote] = await room.getParticipants({ hidden: true })
    const channels: ServerChannel[] = []
    const context = replacerContext(channels)
    roomRemoteReplacer.replace(remote!, context)
    roomReplacer.replace(room, context)
    const granted = attachPeer(channels.find((channel) => channel instanceof RoomStubChannel) as RoomStubChannel)
    const { peer: other } = serve(room)
    await bot.setMeta({ mood: 'busy' })
    await bot.leave()
    await vi.waitFor(() => expect(memberEvents(granted, bot.id).map((event) => event.__r)).toEqual(['p-meta', 'leave']))
    const marker = await room.join()
    await vi.waitFor(() =>
      expect(memberEvents(other, marker.id)).toContainEqual(expect.objectContaining({ __r: 'join' })),
    )
    expect(memberEvents(other, bot.id)).toEqual([])
  })
  it("keeps a client participant's own meta on the value every observer converged to", async () => {
    const acks = { A: deferred<unknown>(), B: deferred<unknown>() }
    const { client, emit } = fakeClient('own-meta', {
      send: async (message) => {
        const request = message as { __r: string; meta?: { v: 'A' | 'B' } }
        if (request.__r === 'req-join') return { id: 'me', joinedAt: 1 }
        if (request.__r === 'req-set-meta') return await acks[request.meta!.v].promise
        return undefined
      },
    })
    const me = await client.join()
    emit({ __r: 'join', id: 'me', meta: {}, joinedAt: 1 }, 1)
    const settingA = me.setMeta({ v: 'A' })
    const settingB = me.setMeta({ v: 'B' })
    emit({ __r: 'p-meta', id: 'me', meta: { v: 'B' }, seq: 1 }, 2)
    emit({ __r: 'p-meta', id: 'me', meta: { v: 'A' }, seq: 2 }, 3)
    acks.A.resolve({ meta: { v: 'A' }, seq: 2 })
    await settingA
    acks.B.resolve({ meta: { v: 'B' }, seq: 1 })
    await settingB
    expect(me.meta).toEqual({ v: 'A' })
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
  it('drops an incarnation an interrupted close left, once listing finds its tombstone lapsed', async () => {
    vi.useFakeTimers()
    const room = (await Room.create('interrupted-close')) as ServerRoom
    vi.spyOn(driver, 'dropGeneration').mockRejectedValueOnce(new Error('connection lost'))
    await expect(Room.close(room.id)).rejects.toThrow('connection lost')
    await vi.advanceTimersByTimeAsync(60_000)
    await Room.list()
    expect(memoryState.rooms.get(room.id)?.gens.has(room._inc) ?? false).toBe(false)
  })
  it('lists a closing room without dropping the incarnation its close still owns', async () => {
    const room = (await Room.create('closing-listed')) as ServerRoom
    const finalizing = deferred<void>()
    const compareExchangeHead = driver.compareExchangeHead.bind(driver)
    const cx = vi.spyOn(driver, 'compareExchangeHead').mockImplementation(async (id, expected, next) => {
      if (expected.form === 'finalize') await finalizing.promise
      return compareExchangeHead(id, expected, next)
    })
    const closing = Room.close(room.id)
    await vi.waitFor(() =>
      expect(cx).toHaveBeenCalledWith(room.id, expect.objectContaining({ form: 'finalize' }), expect.anything()),
    )
    await Room.list()
    expect(memoryState.rooms.get(room.id)?.gens.has(room._inc)).toBe(true)
    finalizing.resolve()
    await closing
  })
  it("treats a delivery handoff that never settles or rejects as lost, not as the publisher's failure", async () => {
    vi.useFakeTimers()
    const room = await Room.create('lost-delivery')
    const member = await room.join()
    const commitLane = driver.commitLane.bind(driver)
    const handoffs = [() => new Promise<void>(() => {}), () => Promise.reject(new Error('fence cancelled'))]
    vi.spyOn(driver, 'commitLane').mockImplementation(async (...args) => {
      const result = await commitLane(...args)
      return 'stale' in result ? result : { ...result, delivery: handoffs.shift()!() }
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const publishing = member.publish('lost')
    // The member's next publish commits without waiting on the handoff still pending.
    await expect(member.publish('rejected')).resolves.toMatchObject({ seq: expect.any(Number) })
    await vi.advanceTimersByTimeAsync(ROOM_HORIZON_MS)
    await expect(publishing).resolves.toMatchObject({ seq: expect.any(Number) })
    expect(report).toHaveBeenCalled()
  })
  it('applies room-wide and member-specific binary wants to both subscription and demand', async () => {
    const room = await Room.create('binary-pairs')
    const publisher = await room.join()
    const observer = await Room.get(room.id)
    const demand = new Set<string | null>()
    const received: Array<[string, number]> = []
    publisher.onDemand((track, wanted) => {
      if (wanted) demand.add(track)
    })
    observer.subscribeBinary((data, info) => received.push([info.track, data[0]!] as [string, number]), {
      track: 'screen',
    })
    const remote = (await observer.getParticipant(publisher.id))!
    remote.subscribeBinary((data, info) => received.push([info.track, data[0]!] as [string, number]), {
      track: 'camera',
    })
    await vi.waitFor(() => expect([...demand].sort()).toEqual(['camera', 'screen']))
    await publisher.publishBinary(new Uint8Array([1]), { track: 'screen' })
    await publisher.publishBinary(new Uint8Array([2]), { track: 'camera' })
    expect(received).toEqual([
      ['screen', 1],
      ['camera', 2],
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
  it('never runs a guard with a stand-in for a member that left while its publish queued', async () => {
    const room = await Room.create('queued-publish-kick')
    const identities: unknown[] = []
    const held = deferred<void>()
    Room.guard(room, {
      onBeforePublish: async (from) => {
        identities.push(from.identity)
        if (identities.length === 1) await held.promise
      },
    })
    const member = await room.join({ identity: 'alice' })
    const first = member.publish('one').catch((error: unknown) => error)
    const queued = member.publish('two').catch((error: unknown) => error)
    await vi.waitFor(() => expect(identities).toEqual(['alice']))
    await Room.removeParticipant(room.id, { id: member.id })
    held.resolve()
    expect(isRoomError(await queued)).toBe(true)
    await first
    expect(identities).toEqual(['alice'])
  })
  it('takes meta as it was at the call, nested values included', async () => {
    const created = { topic: { name: 'a' } }
    const creating = Room.create('nested-meta', { meta: created })
    created.topic.name = 'changed'
    const room = await creating
    const joined = { pos: { x: 0 } }
    const joining = room.join({ meta: joined })
    joined.pos.x = 9
    const me = await joining
    expect({ room: room.meta, me: me.meta }).toEqual({ room: { topic: { name: 'a' } }, me: { pos: { x: 0 } } })
    const attrs = { pos: { x: 1 } }
    const setting = me.setAttributes(attrs)
    attrs.pos.x = 9
    await setting
    const roomMeta = { topic: { name: 'b' } }
    const settingRoom = Room.setMeta(room.id, roomMeta)
    roomMeta.topic.name = 'changed'
    await settingRoom
    await vi.waitFor(() => expect(room.meta).toEqual({ topic: { name: 'b' } }))
    // A change after the call settled reaches nothing either.
    attrs.pos.x = 8
    roomMeta.topic.name = 'later'
    const fresh = await Room.get(room.id)
    expect({
      view: [room.meta, me.meta],
      stored: [fresh.meta, (await fresh.getParticipants()).map(({ meta }) => meta)],
    }).toEqual({
      view: [{ topic: { name: 'b' } }, { pos: { x: 1 } }],
      stored: [{ topic: { name: 'b' } }, [{ pos: { x: 1 } }]],
    })
  })
  it('sends a server message as it was at the call, however the caller reuses its object', async () => {
    const room = await Room.create('reused-message')
    const n = (data: unknown) => (data as { n: number }).n
    const stored: number[] = []
    Room.guard(room, { onAfterPublish: (_from, data) => void stored.push(n(data)) })
    const bot = await room.join()
    const bob = await room.join()
    const published: number[] = []
    room.subscribe((data) => published.push(n(data)))
    const announced: number[] = []
    room.onAnnounce((data) => announced.push(n(data)))
    const dms: number[] = []
    bob.listen((data) => void dms.push(n(data)))
    const message = { n: 1 }
    const sends: Promise<unknown>[] = [bot.publish(message)]
    message.n = 2
    sends.push(bot.publish(message))
    message.n = 3
    sends.push(Room.announce(room.id, message))
    message.n = 4
    sends.push(bot.send(bob.id, message))
    message.n = 5
    sends.push(Room.send(room.id, { id: bob.id }, message))
    message.n = 6
    await Promise.all(sends)
    await vi.waitFor(() => expect(dms).toHaveLength(2))
    expect({ published, stored, announced, dms }).toEqual({
      published: [1, 2],
      stored: [1, 2],
      announced: [3],
      dms: [4, 5],
    })
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
  it("drops a reply that arrives after its sender's ack timeout", async () => {
    const room = await Room.create('late-ack-reply')
    const sender = await room.join()
    const recipient = await room.join()
    vi.useFakeTimers()
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const sending = sender.send(recipient, 'late', { ack: true }).catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(ROOM_DM_ACK_TIMEOUT_MS)
    expect(isRoomError(await sending)).toBe(true)
    const handler = vi.fn(() => 'too late')
    recipient.listen(handler)
    await vi.advanceTimersByTimeAsync(0)
    expect(handler).toHaveBeenCalled()
    expect(report).not.toHaveBeenCalled()
  })
  it('isolates an identity send from a departed member but preserves exact-ID diagnosis', async () => {
    const room = await Room.create('server-send-race')
    const departed = await room.join({ identity: 'fanout' })
    const healthy = await room.join({ identity: 'fanout' })
    const inbox: unknown[] = []
    healthy.listen((data) => inbox.push(data))
    const commitLane = driver.commitLane.bind(driver)
    let raceId = departed.id
    vi.spyOn(driver, 'commitLane').mockImplementation(async (roomId, inc, lane, payload, options) => {
      if (lane.kind === 'inbox' && lane.member === raceId) {
        raceId = ''
        await Room.removeParticipant(room.id, { id: lane.member })
      }
      return commitLane(roomId, inc, lane, payload, options)
    })
    await expect(Room.send(room.id, { identity: 'fanout' }, 'fanout')).resolves.toBeUndefined()
    expect(inbox).toEqual(['fanout'])
    const exact = await room.join()
    raceId = exact.id
    await expect(Room.send(room.id, { id: exact.id }, 'late')).rejects.toThrow(
      `Participant not found (left?): ${exact.id}`,
    )
    expect((await driver.readHead(room.id))?.state).toBe('open')
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
    vi.spyOn(internal, '_deliverMessage').mockImplementation((message) => {
      plainArrived.resolve()
      deliverMessage(message)
    })
    vi.spyOn(internal, '_deliverMessageAck').mockImplementation((message) => {
      ackArrived.resolve()
      return deliverMessageAck(message)
    })
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
  })
  it("rebuilds a client-held participant's ack reply so it cannot forge an inbox envelope", async () => {
    const room = await Room.create('forged-ack-reply')
    const holder = (await room.join()) as ServerLocalParticipant
    const victim = await room.join()
    const sender = await room.join()
    const victimInbox: unknown[] = []
    victim.listen((data) => victimInbox.push(data))
    const channel = new RoomParticipantStubChannel(holder)
    const forged = { ok: true, result: 'handled', __r: 'dm', to: victim.id, from: '', fromMeta: null, data: 'forged' }
    vi.spyOn(channel, 'send').mockResolvedValue(forged as never)
    await expect(sender.send(holder.id, 'ping', { ack: true })).resolves.toMatchObject({ response: 'handled' })
    expect(victimInbox).toEqual([])
  })
  it('rejects an option the call does not have', async () => {
    const room = (await Room.create('unknown-options')) as ServerRoom
    const me = await room.join()
    const calls: Array<[() => unknown, string]> = [
      [() => Room.create('unknown-options-2', { size: 4 } as never), 'Unknown Room option: size'],
      [() => Room.get(room.id, { lazy: true } as never), 'Unknown Room.get() option: lazy'],
      [() => Room.list({ limit: 10 } as never), 'Unknown Room.list() option: limit'],
      [() => room.join({ echo: false } as never), 'Unknown join() option: echo'],
      [() => Room.guard(room, { onBeforeLeave: () => {} } as never), 'Unknown Room.guard() option: onBeforeLeave'],
      [() => me.publish('x', { persist: true } as never), 'Unknown publish() option: persist'],
      [() => me.publishBinary(new Uint8Array([1]), { layer: 'mic' } as never), 'Unknown publishBinary() option: layer'],
      [() => room.subscribeBinary(() => {}, { layer: 'mic' } as never), 'Unknown subscribeBinary() option: layer'],
      [() => me.send(me.id, 'x', { confirm: true } as never), 'Unknown send() option: confirm'],
      [() => room.getParticipants({ all: true } as never), 'Unknown getParticipants() option: all'],
    ]
    for (const [call, message] of calls) await expect(Promise.resolve().then(call)).rejects.toThrow(message)
  })
  it('bounds the named tracks a subscriber can want, on the API and on the wire', async () => {
    const room = (await Room.create('track-cap')) as ServerRoom
    const member = await room.join()
    const remote = (await room.getParticipant(member.id))!
    const tracks = Array.from({ length: 17 }, (_, i) => `t${i}`)
    for (const track of tracks.slice(0, 16)) remote.subscribeBinary(() => {}, { track })
    expect(() => remote.subscribeBinary(() => {}, { track: tracks[16] })).toThrow('at most 16 tracks per participant')
    remote.subscribeBinary(() => {}, { track: tracks[0] })
    for (const track of tracks.slice(0, 16)) room.subscribeBinary(() => {}, { track })
    expect(() => room.subscribeBinary(() => {}, { track: tracks[16] })).toThrow('at most 16 tracks per participant')
    const stub = register(room)
    let seq = 0
    const declare = (wanted: string[]) =>
      stub._dispatchFrame({
        tag: TAG.TEXT,
        index: 7,
        seq: ++seq,
        text: stringify({
          __r: 'sub-binary',
          wants: { everyMember: emptyTrackWants(), members: { [member.id]: { all: false, tracks: wanted } } },
        }),
        bytes: 1,
      })
    expect(() => declare(tracks.slice(0, 16))).not.toThrow()
    expect(() => declare(tracks)).toThrow(ProtocolViolationError)
  })
  it('treats a request the client library never sends, unparsable or misshapen, as a protocol violation', async () => {
    const stub = register((await Room.create('malformed-stub-request')) as ServerRoom)
    const holder = (await Room.join('malformed-stub-request')) as ServerLocalParticipant
    const participant = new RoomParticipantStubChannel(holder)
    const member = crypto.randomUUID()
    const text = (value: unknown) => stringify(value)
    const frames = [
      [stub, { tag: TAG.TEXT, index: 7, seq: 1, text: text({ __r: 'sub-binary', wants: 5 }), bytes: 1 }],
      [
        stub,
        { tag: TAG.TEXT, index: 7, seq: 2, text: text({ __r: 'sub-text', members: [1], announce: false }), bytes: 1 },
      ],
      [
        stub,
        {
          tag: TAG.TEXT,
          index: 7,
          seq: 3,
          text: text({ __r: 'dm-reply', id: member, ackId: 'a', reply: {} }),
          bytes: 1,
        },
      ],
      [
        stub,
        { tag: TAG.TEXT, index: 7, seq: 4, text: text({ __r: 'req-join', meta: {}, selfDelivery: true }), bytes: 1 },
      ],
      [
        stub,
        { tag: TAG.TEXT_ACK_REQ, index: 7, seq: 5, text: text({ __r: 'req-join', meta: [], selfDelivery: true }) },
      ],
      [stub, { tag: TAG.TEXT_ACK_REQ, index: 7, seq: 6, text: text({ __r: 'req-set-meta', id: 'nope', meta: {} }) }],
      [
        stub,
        { tag: TAG.TEXT_ACK_REQ, index: 7, seq: 7, text: text({ __r: 'sub-text', members: [], announce: false }) },
      ],
      [stub, { tag: TAG.PUBLISH_ACK_REQ, index: 7, seq: 8, text: text({ __r: 'data', from: member, retain: 1 }) }],
      [stub, { tag: TAG.PUBLISH_BINARY_ACK_REQ, index: 7, seq: 9, data: new Uint8Array([1, 2]) }],
      [participant, { tag: TAG.TEXT_ACK_REQ, index: 7, seq: 1, text: text({ __r: 'req-set-attrs', attrs: 'x' }) }],
      [
        participant,
        { tag: TAG.BINARY_ACK_REQ, index: 7, seq: 2, data: encodeBinaryFrame(member, new Uint8Array([1])) },
      ],
      [stub, { tag: TAG.TEXT, index: 7, seq: 10, text: '{', bytes: 1 }],
      [stub, { tag: TAG.TEXT_ACK_REQ, index: 7, seq: 11, text: '{' }],
      [participant, { tag: TAG.TEXT_ACK_REQ, index: 7, seq: 3, text: '{' }],
    ] as const
    for (const [channel, frame] of frames) expect(() => channel._dispatchFrame(frame)).toThrow(ProtocolViolationError)
  })
  it("hands a client join's guard a frozen meta, as a server join does", async () => {
    const room = (await Room.create('stub-join-guard')) as ServerRoom
    const frozen: boolean[] = []
    Room.guard(room, { onBeforeJoin: ({ meta }) => void frozen.push(Object.isFrozen(meta)) })
    const { stub } = serve(room)
    await stub._handleRequest({ __r: 'req-join', meta: { name: 'a' }, selfDelivery: true })
    await room.join({ meta: { name: 'b' } })
    expect(frozen).toEqual([true, true])
  })
  it('hands send guards a detached sender snapshot for both ends', async () => {
    const room = await Room.create('guard-snapshots')
    const seen: Sender[] = []
    Room.guard(room, {
      onBeforeSend: (from, to) => {
        seen.push(from, to)
      },
    })
    const alice = await room.join({ meta: { name: 'Alice' } })
    const bob = await room.join({ meta: { name: 'Bob' } })
    await alice.send(bob.id, 'hi')
    expect(seen).toEqual([
      { id: alice.id, meta: { name: 'Alice' }, identity: null },
      { id: bob.id, meta: { name: 'Bob' }, identity: null },
    ])
    for (const sender of seen) {
      expect(Object.isFrozen(sender)).toBe(true)
      expect(Object.keys(sender)).toEqual(['id', 'meta', 'identity'])
    }
  })
  it('rejects a malformed argument at the API edge as a usage error, before any guard runs', async () => {
    const room = await Room.create('api-edge')
    const me = await room.join()
    const onBeforeJoin = vi.fn()
    Room.guard(room, { onBeforeJoin })
    const calls: Array<[() => unknown, string]> = [
      [() => Room.create('\ud800'), 'well-formed'],
      [() => room.join({ identity: '\udc00' }), 'join() options.identity should be a non-empty well-formed string'],
      [() => Room.getParticipants(room.id, { identity: '\udc00' }), 'well-formed'],
      [() => Room.removeParticipant(room.id, { identity: '\udc00' }), 'well-formed'],
      [() => room.join({ selfDelivery: 'false' } as never), 'join() options.selfDelivery should be a boolean'],
      [() => room.join({ selfDelivery: 0 } as never), 'join() options.selfDelivery should be a boolean'],
      [
        () => Room.removeParticipant(room.id, 'member-id' as never),
        'The participant ref should be { id } or { identity }',
      ],
      [() => Room.send(room.id, null as never, 'hi'), 'The participant ref should be { id } or { identity }'],
      [() => me.setMeta([] as never), 'setMeta() meta should be an object'],
      [() => me.setAttributes('x' as never), 'setAttributes() attributes should be an object'],
      [() => Room.setMeta(room.id, [] as never), 'Room.setMeta() meta should be an object'],
      [() => Room.setAttributes(room.id, [] as never), 'Room.setAttributes() attributes should be an object'],
      [() => Room.create('room-meta-array', { meta: [] as never }), 'options.meta should be an object'],
      ...[null, [], 'screen'].map((options): [() => unknown, string] => [
        () => room.subscribeBinary(() => {}, options as never),
        'subscribeBinary() options should be an object',
      ]),
    ]
    for (const [call, message] of calls) await expect(Promise.resolve().then(call)).rejects.toThrow(message)
    expect(() => me.send(null as never, 'hi')).toThrow('send() recipient should be a participant or its id')
    expect(onBeforeJoin).not.toHaveBeenCalled()
  })
  it("round-trips an ack DM through a room stub and keeps only the reply's own fields", async () => {
    const room = (await Room.create('stub-ack-dm')) as ServerRoom
    const { stub, peer } = serve(room)
    const { id } = (await stub._handleRequest({ __r: 'req-join', meta: {}, selfDelivery: true })) as {
      id: string
    }
    const victim = await room.join()
    const victimInbox: unknown[] = []
    victim.listen((data) => victimInbox.push(data))
    const sender = await room.join()
    const acking = sender.send(id, 'ping', { ack: true })
    let ackId = ''
    await vi.waitFor(() => {
      const dm = peer
        .decoded()
        .filter((frame) => frame.tag === TAG.PUBLISH)
        .map((frame) => JSON.parse(frame.text) as { __r: string; ackId?: string })
        .find((envelope) => envelope.__r === 'dm')
      expect(dm?.ackId).toBeTypeOf('string')
      ackId = dm!.ackId!
    })
    const reply = { ok: true, result: 'handled', __r: 'dm', to: victim.id, from: '', data: 'forged' }
    stub._onPeerMessage(stringify({ __r: 'dm-reply', id, ackId, reply }), 0)
    await expect(acking).resolves.toMatchObject({ response: 'handled' })
    expect(victimInbox).toEqual([])
  })
  it('reports a client-held participant whose channel closed as disconnected', async () => {
    const room = (await Room.create('standalone-disconnect')) as ServerRoom
    const holder = (await room.join()) as ServerLocalParticipant
    const stub = register(room)
    const { id } = (await stub._handleRequest({ __r: 'req-join', meta: {}, selfDelivery: true })) as { id: string }
    const causes = new Map<string, unknown>()
    room.onLeave((member, cause) => causes.set(member.id, cause?.type))
    new RoomParticipantStubChannel(holder).abort()
    stub.abort()
    await vi.waitFor(() =>
      expect(Object.fromEntries(causes)).toEqual({ [holder.id]: 'disconnected', [id]: 'disconnected' }),
    )
  })
  it('answers a DM to a client-held participant whose client closed while it leaves, and reports no bug', async () => {
    await disposeBackend()
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    installBackend(() => driver)
    const room = await Room.create('closed-holder-dm')
    const holder = (await room.join()) as ServerLocalParticipant
    const sender = await room.join()
    const channel = new RoomParticipantStubChannel(holder)
    const compareExchange = driver.compareExchangeCells.bind(driver)
    const evicting = deferred<void>()
    vi.spyOn(driver, 'compareExchangeCells').mockImplementationOnce(async (...args) => {
      await evicting.promise
      return await compareExchange(...args)
    })
    channel.abort()
    let outcome = 'pending'
    void sender.send(holder.id, 'ack?', { ack: true }).then(
      () => (outcome = 'answered'),
      (error: unknown) => (outcome = isRoomError(error) ? 'left' : 'bug'),
    )
    await sender.send(holder.id, 'plain')
    await vi.waitFor(() => expect(outcome).toBe('left'))
    expect(report).not.toHaveBeenCalled()
    evicting.resolve()
  })
  it('stops renewing a client-held participant whose removal failed after its client went away', async () => {
    vi.useFakeTimers()
    const room = await Room.create('standalone-expire')
    const holder = (await room.join()) as ServerLocalParticipant
    const channel = new RoomParticipantStubChannel(holder)
    vi.spyOn(console, 'error').mockImplementation(() => {})
    vi.spyOn(driver, 'compareExchangeCells').mockRejectedValueOnce(new Error('backend unavailable'))
    channel.abort()
    await vi.advanceTimersByTimeAsync(ROOM_MEMBER_TTL_MS + 2 * ROOM_HEARTBEAT_INTERVAL_MS)
    const observer = await Room.get('standalone-expire')
    expect((await observer.getParticipants()).map((member) => member.id)).not.toContain(holder.id)
  })
  it("rejects a client-held participant's binary publish with the guard's Abort, not the whole response", async () => {
    const room = await Room.create('standalone-binary-abort')
    Room.guard(room, {
      onBeforePublish: () => {
        throw Abort('blocked')
      },
    })
    const holder = (await room.join()) as ServerLocalParticipant
    const channel = new RoomParticipantStubChannel(holder)
    channel._registerChannel()
    const responseAbort = vi.fn()
    channel._setResponseAbort(responseAbort)
    const peer = attachPeer(channel as unknown as RoomStubChannel)
    const data = encodeBinaryFrame(holder.id, new Uint8Array([1]))
    channel._dispatchFrame({ tag: TAG.BINARY_ACK_REQ, index: 7, seq: 1, data })
    await vi.waitFor(() =>
      expect(peer.decoded().find((frame) => frame.tag === TAG.ACK_RES)).toMatchObject({ status: ACK_STATUS.ABORT }),
    )
    expect(responseAbort).not.toHaveBeenCalled()
  })
  it("answers, not throws, a client-held participant's binary frame that arrives after it left", async () => {
    const room = await Room.create('standalone-binary-after-leave')
    const holder = (await room.join()) as ServerLocalParticipant
    const channel = new RoomParticipantStubChannel(holder)
    channel._registerChannel()
    const peer = attachPeer(channel as unknown as RoomStubChannel)
    await holder.leave()
    const data = encodeBinaryFrame(holder.id, new Uint8Array([1]))
    channel._dispatchFrame({ tag: TAG.BINARY_ACK_REQ, index: 7, seq: 1, data })
    await vi.waitFor(() =>
      expect(peer.decoded().find((frame) => frame.tag === TAG.ACK_RES)).toMatchObject({
        status: ACK_STATUS.ERROR,
        text: 'Participant left the room',
      }),
    )
  })
  it('leaves no member behind a join whose member write committed but whose reply was lost', async () => {
    const room = await Room.create('join-reply-lost')
    const compareExchange = driver.compareExchangeCells.bind(driver)
    vi.spyOn(driver, 'compareExchangeCells').mockImplementationOnce(async (...args) => {
      await compareExchange(...args)
      throw new Error('Connection is closed.')
    })
    await expect(room.join({ identity: 'user-1' })).rejects.toThrow('Connection is closed.')
    expect(await Room.getParticipants(room.id)).toEqual([])
  })
  it('rejects a join whose member was removed before its join event committed, leaving no member behind', async () => {
    const room = (await Room.create('join-kicked')) as ServerRoom
    const observer = (await Room.get(room.id)) as ServerRoom
    await observer.getParticipants()
    const commitLane = driver.commitLane.bind(driver)
    const committing = deferred<void>()
    const kicked = deferred<void>()
    vi.spyOn(driver, 'commitLane').mockImplementationOnce(async (...args) => {
      committing.resolve()
      await kicked.promise
      return await commitLane(...args)
    })
    const joining = room.join({ identity: 'user-1' }).catch((error: unknown) => error)
    await committing.promise
    await Room.removeParticipant(room.id, { identity: 'user-1' })
    kicked.resolve()
    expect(isRoomError(await joining)).toBe(true)
    await subsOf(observer).reconcileAuthority()
    expect([room.count, observer.count]).toEqual([0, 0])
  })
  it("leaves no unhandled rejection behind a participant's un-awaited publish, binary publish or DM that fails", async () => {
    const room = await Room.create('fire-and-forget-participant')
    const authority = await room.join({ identity: 'authority', hidden: true })
    const peer = await room.join()
    await Room.close(room.id)
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => void unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      authority.publish('tick')
      authority.publishBinary(new Uint8Array([1]))
      authority.send(peer.id, 'dm')
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(unhandled).toEqual([])
      await expect(authority.publishBinary(new Uint8Array([1]))).rejects.toThrow('Participant left the room')
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
  })
  it('rejects, not as a bug, a join whose client stub closed during the guard', async () => {
    const room = (await Room.create('stub-close-during-guard')) as ServerRoom
    const stub = register(room)
    const entered = deferred<void>()
    const release = deferred<void>()
    Room.guard(room, {
      onBeforeJoin: async () => {
        entered.resolve()
        await release.promise
      },
    })
    const joining = stub
      ._handleRequest({ __r: 'req-join', meta: {}, selfDelivery: true })
      .catch((error: unknown) => error)
    await entered.promise
    stub.abort()
    release.resolve()
    expect(isRoomError(await joining)).toBe(true)
    expect(await Room.getParticipants(room.id)).toEqual([])
  })
  it('does not admit a member whose client stub closed while the join was committing', async () => {
    const room = (await Room.create('stub-close-during-join')) as ServerRoom
    const stub = register(room)
    const compareExchange = driver.compareExchangeCells.bind(driver)
    const writing = deferred<void>()
    const release = deferred<void>()
    vi.spyOn(driver, 'compareExchangeCells').mockImplementationOnce(async (...args) => {
      writing.resolve()
      await release.promise
      return compareExchange(...args)
    })
    const joining = stub
      ._handleRequest({ __r: 'req-join', meta: {}, selfDelivery: true })
      .catch((error: unknown) => error)
    await writing.promise
    stub.abort()
    release.resolve()
    expect(isRoomError(await joining)).toBe(true)
    expect(await Room.getParticipants(room.id)).toEqual([])
  })
  it('does not evict a client-held participant again once it left', async () => {
    vi.useFakeTimers()
    const room = (await Room.create('standalone-leave-once')) as ServerRoom
    const holder = (await room.join()) as ServerLocalParticipant
    const channel = new RoomParticipantStubChannel(holder)
    let closed = false
    channel.onClose(() => {
      closed = true
    })
    const departed = vi.spyOn(room, '_removeDepartedMember')
    await holder.leave()
    await vi.advanceTimersByTimeAsync(CHANNEL_CLOSE_TIMEOUT_MS + 1)
    expect(closed).toBe(true)
    expect(departed).not.toHaveBeenCalled()
  })
  it('does not hold the process open for a pending ack DM', async () => {
    const room = await Room.create('ack-timer-unref')
    const sender = await room.join()
    const recipient = await room.join()
    const ackTimers: ReturnType<typeof setTimeout>[] = []
    const realSetTimeout = globalThis.setTimeout
    vi.spyOn(globalThis, 'setTimeout').mockImplementation(((callback: () => void, ms?: number) => {
      const timer = realSetTimeout(callback, ms)
      if (ms === ROOM_DM_ACK_TIMEOUT_MS) ackTimers.push(timer)
      return timer
    }) as typeof setTimeout)
    void sender.send(recipient.id, 'unanswered', { ack: true }).catch(() => {})
    await vi.waitFor(() => expect(ackTimers).toHaveLength(1))
    expect(ackTimers[0]!.hasRef()).toBe(false)
  })
  it("removes a crashed node's member from a quiet observer's view", async () => {
    vi.useFakeTimers()
    const owner = await Room.create('crash-reap')
    const member = await owner.join()
    const observer = await Room.get('crash-reap')
    const left: string[] = []
    observer.onLeave((participant) => left.push(participant.id))
    await observer.getParticipants()
    const memberKey = memberCellKey(member.id)
    const compareExchange = driver.compareExchangeCells.bind(driver)
    vi.spyOn(driver, 'compareExchangeCells').mockImplementation(async (roomId, inc, revision, mutations) => {
      if (mutations.some((mutation) => mutation.key === memberKey && mutation.bytes !== null))
        throw new Error('crashed')
      return compareExchange(roomId, inc, revision, mutations)
    })
    vi.spyOn(console, 'error').mockImplementation(() => {})
    await vi.advanceTimersByTimeAsync(ROOM_MEMBER_TTL_MS + 2 * ROOM_HEARTBEAT_INTERVAL_MS)
    expect(left).toEqual([member.id])
  })
  it('settles closure on the next heartbeat for an owning instance that missed `closed`', async () => {
    vi.useFakeTimers()
    loseLaneFrames((lane) => lane.kind === 'control').where((text) => text.includes('"__r":"closed"'))
    const room = (await Room.create('missed-close')) as ServerRoom
    const member = await room.join()
    const causes: string[] = []
    member.onLeave((cause) => causes.push(cause.type))
    vi.spyOn(driver, 'dropGeneration').mockRejectedValue(new Error('transient drop failure'))
    await expect(Room.close('missed-close')).rejects.toThrow('transient drop failure')
    expect(room.isClosed).toBe(false)
    await vi.advanceTimersByTimeAsync(ROOM_HEARTBEAT_INTERVAL_MS)
    expect(room.isClosed).toBe(true)
    expect(causes).toEqual(['closed'])
  })
  it('reports a RoomError escaping application code as a bug', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const room = await Room.create('listener-room-error')
    room.onJoin(() => {
      throw new RoomError('Room is closed: from a listener')
    })
    const member = await room.join()
    member.listen(() => {
      throw new RoomError('Participant left the room')
    })
    await (await Room.get('listener-room-error')).join().then((sender) => sender.send(member.id, 'hi'))
    await vi.waitFor(() => {
      const reported = report.mock.calls.flat().map(String).join('\n')
      expect(reported).toContain('Room is closed: from a listener')
      expect(reported).toContain('Participant left the room')
    })
  })
  it('reports bugs, not expected RoomErrors, from background Room work', () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    reportRoomError(new RoomError('Room is closed: background'))
    expect(report).not.toHaveBeenCalled()
    reportRoomError(new Error('a real bug'))
    expect(report).toHaveBeenCalled()
  })
  it('announces a named track on the next publish after its first announcement failed', async () => {
    const room = await Room.create('track-announce-retry')
    const publisher = await room.join()
    const observer = await Room.get('track-announce-retry')
    const frames: number[] = []
    observer.subscribeBinary((data) => frames.push(data[0]!))
    await observer.getParticipants()
    const commitLane = driver.commitLane.bind(driver)
    let failed = false
    vi.spyOn(driver, 'commitLane').mockImplementation(async (roomId, inc, lane, payload, options) => {
      if (!failed && lane.kind === 'control' && decoder.decode(payload).includes('"__r":"track"')) {
        failed = true
        throw new Error('transient control-lane failure')
      }
      return commitLane(roomId, inc, lane, payload, options)
    })
    await expect(publisher.publishBinary(new Uint8Array([1]), { track: 'camera' })).rejects.toThrow()
    await publisher.publishBinary(new Uint8Array([2]), { track: 'camera' })
    await vi.waitFor(() => expect(frames).toContain(2))
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
    const alice = await room.join()
    const bob = await room.join()
    const inbox: unknown[] = []
    bob.listen((data) => inbox.push(data))
    await expect(alice.publish('published')).resolves.toMatchObject({ seq: expect.any(Number) })
    await expect(alice.send(bob.id, 'sent')).resolves.toMatchObject({ seq: expect.any(Number) })
    expect((await room.getParticipants()).map(({ id }) => id)).toEqual([alice.id, bob.id])
    expect(inbox).toEqual(['sent'])
    expect(report).toHaveBeenCalledTimes(4)
  })
  it('orders participant text and room announcements in one semantic domain', async () => {
    const room = await Room.create('announce-semantic-order')
    const member = await room.join()
    const observer = await Room.get(room.id)
    const observed: Array<[unknown, number]> = []
    const backend = getRoomBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const controlReady = deferred<void>()
    vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      const subscription = subscribeLane(roomId, inc, lane, receiver)
      if (lane.kind === 'control') void subscription.ready.then(() => controlReady.resolve())
      return subscription
    })
    observer.onAnnounce((data, info) => observed.push([data, info.seq]))
    await controlReady.promise
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
    const backend = getRoomBackend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    const semanticReady = deferred<void>()
    vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
      const subscription = subscribeLane(roomId, inc, lane, receiver)
      if (lane.kind === 'semantic') void subscription.ready.then(() => semanticReady.resolve())
      return subscription
    })
    const wanted = serve(room)
    const silent = serve(room)
    wanted.stub._onPeerMessage(JSON.stringify({ __r: 'sub-text', members: [], announce: true }), 1)
    await semanticReady.promise
    await Room.announce(room.id, 'wanted')
    await vi.waitFor(() => expect(semanticFrames(wanted.peer, 'announce')).toEqual(['wanted']))
    expect(semanticFrames(silent.peer, 'announce')).toEqual([])
  })
  it("retained owner cleanup is compare-delete, so a newer owner's racing frame survives", async () => {
    const room = (await Room.create('retained-owner')) as ServerRoom
    const departing = await room.join({ meta: { name: 'departing' } })
    const replacement = await room.join({ meta: { name: 'replacement' } })
    await departing.publish('old', { retain: true })
    const realDelete = driver.deleteRetained.bind(driver)
    let raced = false
    vi.spyOn(driver, 'deleteRetained').mockImplementation(async (roomId, inc, lane, opts) => {
      if (!raced && opts?.ifSeq !== undefined) {
        raced = true
        await replacement.publish('new', { retain: true })
      }
      return realDelete(roomId, inc, lane, opts)
    })
    await Room.removeParticipant(room.id, { id: departing.id })
    const retained = await driver.readRetained(room.id, room._inc, semanticLane)
    expect(parse(decoder.decode(retained!.payload))).toMatchObject({ from: replacement.id, data: 'new' })
  })
  it('retries retained cleanup after member deletion from durable eviction work', async () => {
    const room = (await Room.create('retained-cleanup-retry')) as ServerRoom
    const member = await room.join()
    await member.publish('text', { retain: true })
    await member.publishBinary(new Uint8Array([1]), { track: 'screen', retain: true })
    vi.spyOn(driver, 'deleteRetained').mockRejectedValueOnce(new Error('transient retained cleanup failure'))
    await expect(member.leave()).rejects.toThrow('transient retained cleanup failure')
    expect(await driver.listRetained(room.id, room._inc)).toHaveLength(2)
    await expect(Room.getParticipants(room.id)).resolves.toEqual([])
    expect(await driver.listRetained(room.id, room._inc)).toEqual([])
  })
  it('names why a commit was stale, so callers need no diagnosis reads', async () => {
    const room = (await Room.create('stale-reason')) as ServerRoom
    const member = await room.join()
    const semantic = { kind: 'semantic' } as const
    const payload = new Uint8Array([1])
    expect(await driver.commitLane(room.id, room._inc, semantic, payload, { requiredCellKeys: ['m:gone'] })).toEqual({
      stale: 'cell',
      key: 'm:gone',
    })
    expect(await driver.commitLane(room.id, 'other-inc', semantic, payload)).toEqual({ stale: 'incarnation' })
    const current = await driver.readCells(room.id, room._inc, { keys: [memberCellKey(member.id)] })
    if ('staleInc' in current) throw new Error('room went stale')
    await driver.compareExchangeCells(room.id, room._inc, current.revision, [
      { key: memberCellKey(member.id), bytes: null },
    ])
    const target = await room.join()
    const readCells = vi.spyOn(driver, 'readCells')
    const readHead = vi.spyOn(driver, 'readHead')
    await expect(member.send(target.id, 'hi')).rejects.toThrow(`Participant not found (left?): ${member.id}`)
    expect([readCells.mock.calls.length, readHead.mock.calls.length]).toEqual([0, 0])
  })
  it('replays retained text and binary once to a late server-side subscriber', async () => {
    const authority = await Room.create('late-server-subscriber')
    const publisher = await authority.join()
    await publisher.publish('state', { retain: true })
    await publisher.publishBinary(new Uint8Array([7]), { track: 'camera', retain: true })
    const observer = await Room.get(authority.id)
    const texts: unknown[] = []
    const frames: number[][] = []
    observer.subscribe((data) => void texts.push(data))
    observer.subscribeBinary((data) => void frames.push([...data]), { track: 'camera' })
    await vi.waitFor(() => expect({ texts, frames }).toEqual({ texts: ['state'], frames: [[7]] }))
    await publisher.publish('live')
    await publisher.publishBinary(new Uint8Array([8]), { track: 'camera' })
    await vi.waitFor(() => expect({ texts, frames }).toEqual({ texts: ['state', 'live'], frames: [[7], [8]] }))
  })
  it('drops a live frame older than a retained frame that reached the subscriber first', async () => {
    const authority = await Room.create('retained-before-older-live')
    const publisher = await authority.join()
    const observer = await Room.get(authority.id)
    const readRetained = driver.readRetained.bind(driver)
    const published = deferred<void>()
    vi.spyOn(driver, 'readRetained').mockImplementationOnce(async (...args) => {
      await published.promise
      return readRetained(...args)
    })
    const live = holdLaneDelivery((lane) => lane.kind === 'semantic')
    const texts: unknown[] = []
    observer.subscribe((data) => void texts.push(data))
    await publisher.publish('older')
    await publisher.publish('newest', { retain: true })
    // The retained read returns before the live path delivers the two frames it raced.
    published.resolve()
    await vi.waitFor(() => expect(texts).toEqual(['newest']))
    await live.release()
    await publisher.publish('after')
    await vi.waitFor(() => expect(texts).toEqual(['newest', 'after']))
  })
  it("keeps another member's in-flight text when a retained frame replays", async () => {
    const authority = await Room.create('retained-other-sender')
    const a = await authority.join()
    const b = await authority.join()
    const observer = await Room.get(authority.id)
    const live = holdLaneDelivery((lane) => lane.kind === 'semantic')
    const fromA: unknown[] = []
    const fromB: unknown[] = []
    ;(await observer.getParticipant(a.id))!.subscribe((data) => void fromA.push(data))
    await a.publish('a-1')
    await b.publish('b', { retain: true })
    ;(await observer.getParticipant(b.id))!.subscribe((data) => void fromB.push(data))
    await vi.waitFor(() => expect(fromB).toEqual(['b']))
    await live.release()
    expect(fromA).toEqual(['a-1'])
    expect(fromB).toEqual(['b'])
  })
  it('waits for roster-derived binary routes before reading retained frames', async () => {
    const authority = await Room.create('retained-binary-roster-fence')
    const publisher = await authority.join()
    await publisher.publishBinary(new Uint8Array([1]), { track: 'screen', retain: true })
    const observer = (await Room.get(authority.id)) as ServerRoom
    const stub = register(observer)
    const roster = delayRosterRead(authority.id)
    const listRetained = vi.spyOn(driver, 'listRetained')
    try {
      declare(stub, {
        __r: 'sub-binary',
        wants: { everyMember: { all: true, tracks: [] }, members: {} },
      })
      await roster.started
      expect(listRetained).not.toHaveBeenCalled()
      roster.release()
      await vi.waitFor(() => expect(listRetained).toHaveBeenCalled())
    } finally {
      roster.release()
    }
  })
  it('opens no backend lane for a declared want naming no member', async () => {
    const authority = await Room.create('declared-strangers')
    await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    const stub = register(observer)
    const subscribeLane = vi.spyOn(getRoomBackend(), 'subscribeLane')
    const members = Object.fromEntries(
      Array.from({ length: 100 }, () => [crypto.randomUUID(), { all: false, tracks: ['screen'] }]),
    )
    declare(stub, {
      __r: 'sub-binary',
      wants: { everyMember: { all: false, tracks: [] }, members },
    })
    await observer.getParticipants()
    expect(subscribeLane.mock.calls.filter(([, , lane]) => lane.kind === 'binary')).toEqual([])
  })
  it('opens an exact-member binary lane once the roster names the member', async () => {
    const authority = await Room.create('exact-binary-after-roster')
    const publisher = await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    const stub = register(observer)
    const roster = delayRosterRead(authority.id)
    const subscribeLane = vi.spyOn(getRoomBackend(), 'subscribeLane')
    const binaryLanes = () => subscribeLane.mock.calls.filter(([, , lane]) => lane.kind === 'binary').length
    expect(observer._state.rosterKnown).toBe(false)
    try {
      declare(stub, {
        __r: 'sub-binary',
        wants: {
          everyMember: { all: false, tracks: [] },
          members: { [publisher.id]: { all: false, tracks: ['screen'] } },
        },
      })
      await roster.started
      expect(binaryLanes()).toBe(0)
      roster.release()
      await vi.waitFor(() => expect(binaryLanes()).toBe(1))
      await subsOf(observer).binaryReady()
      await publisher.publishBinary(new Uint8Array([7]), { track: 'screen' })
      const frame = attachPeer(stub)
        .decoded()
        .find((candidate) => candidate.tag === TAG.PUBLISH_BINARY)
      if (frame?.tag !== TAG.PUBLISH_BINARY) throw new Error('expected exact-member binary publish')
      expect(decodeBinaryFrame(frame.data)).toMatchObject({
        from: publisher.id,
        track: 'screen',
        payload: new Uint8Array([7]),
      })
    } finally {
      roster.release()
    }
  })
  it('drops retained text and binary when a crashed publisher is reaped', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(1_000_000)
    const room = (await Room.create('reaped-retained')) as ServerRoom
    const publisher = await room.join({ identity: 'expired-owner' })
    const leaves: string[] = []
    room.onLeave((member) => leaves.push(member.id))
    await publisher.publish('text', { retain: true })
    await publisher.publishBinary(new Uint8Array([1]), { track: 'screen', retain: true })
    expect(await driver.listRetained(room.id, room._inc)).toHaveLength(2)
    vi.setSystemTime(1_000_000 + ROOM_MEMBER_TTL_MS + 1)
    expect(await Room.getParticipants(room.id)).toEqual([])
    expect(await driver.listRetained(room.id, room._inc)).toEqual([])
    expect(leaves).toEqual([publisher.id])
    await expect(Room.removeParticipant(room.id, { identity: 'expired-owner' })).resolves.toBeUndefined()
  })
  it('tail mode holds pre-attach text and flushes it in order on first client demand', async () => {
    const { member, tail } = await createTail('tail')
    await member.publish('early')
    const { stub, peer } = serve(tail)
    await member.publish('held')
    expect(semanticFrames(peer, 'data')).toEqual([])
    stub._onPeerSubscription('text', true)
    await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toEqual(['early', 'held']))
    await member.publish('live')
    expect(semanticFrames(peer, 'data')).toEqual(['early', 'held', 'live'])
  })
  it('Room.get with tail resolves once the tail receives, so it holds what commits after', async () => {
    await Room.create('tail-ready')
    const member = await (await Room.get('tail-ready')).join()
    const semantic = delayLaneSubscription((lane) => lane.kind === 'semantic')
    const getting = Room.get('tail-ready', { tail: true })
    await semantic.started
    setTimeout(() => void semantic.release(), 10)
    const tail = (await getting) as ServerRoom
    await member.publish('after-get')
    const { stub, peer } = serve(tail)
    stub._onPeerSubscription('text', true)
    await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toEqual(['after-get']))
  })
  it('flushes held tail text after an announcement relayed before the first text subscription', async () => {
    const { member, tail } = await createTail('tail-announce')
    const { stub, peer } = serve(tail)
    declare(stub, { __r: 'sub-text', members: [], announce: true })
    await member.publish('held')
    await Room.announce(tail.id, 'notice')
    await vi.waitFor(() => expect(semanticFrames(peer, 'announce')).toEqual(['notice']))
    stub._onPeerSubscription('text', true)
    await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toEqual(['held']))
  })
  it('subscribes the lanes of a member that joins through this instance for its listeners here', async () => {
    const room = (await Room.create('own-join-replan')) as ServerRoom
    const frames: number[] = []
    room.subscribeBinary((data) => frames.push(data[0]!))
    const me = await room.join()
    const demand: Array<[string | null, boolean]> = []
    me.onDemand((track, wanted) => demand.push([track, wanted]))
    await vi.waitFor(() => expect(demand).toEqual([[null, true]]))
    await me.publishBinary(new Uint8Array([7]))
    await vi.waitFor(() => expect(frames).toEqual([7]))
  })
  it("tells this instance's clients a member's new track whose echo arrives after it", async () => {
    const room = (await Room.create('own-track-relay')) as ServerRoom
    // The echo reaches this instance later, as over a networked backend.
    const echo = holdLaneDelivery((lane) => lane.kind === 'control')
    const member = await room.join()
    const peer = attachPeer(register(room))
    const frames: number[] = []
    room.subscribeBinary((data) => frames.push(data[0]!))
    await member.publishBinary(new Uint8Array([1]), { track: 'cam' })
    await echo.release()
    await member.publishBinary(new Uint8Array([2]), { track: 'cam' })
    expect(controlEvents(peer).filter(({ __r }) => __r === 'track')).toEqual([
      expect.objectContaining({ id: member.id, track: 'cam' }),
    ])
    // The all-track listener here subscribes the new track's lane before its first frame.
    await vi.waitFor(() => expect(frames).toEqual([1, 2]))
  })
  it('sends a reattached client the room state its offline buffer dropped', async () => {
    const room = (await Room.create('reattach-resync')) as ServerRoom
    const leaver = await room.join()
    config.channel = { bufferLimit: 256 }
    try {
      const stub = register(room)
      const first = attachPeer(stub)
      await vi.waitFor(() => expect(controlEvents(first).map(({ __r }) => __r)).toContain('roster'))
      stub._onPeerDisconnect(60_000)
      await leaver.leave()
      // Larger than the offline buffer: it clears the buffered leave, and is dropped too.
      const meta = { pad: 'x'.repeat(300) }
      await Room.setMeta(room.id, meta)
      await vi.waitFor(() => expect(room.meta).toEqual(meta))
      const peer = attachPeer(stub)
      await vi.waitFor(() =>
        expect(controlEvents(peer).map(({ __r, members, meta }) => ({ __r, members, meta }))).toEqual([
          { __r: 'update', members: undefined, meta },
          { __r: 'roster', members: [], meta: undefined },
        ]),
      )
    } finally {
      config.channel = {}
    }
  })
  it("sends a reattached client its member's demand its offline buffer dropped", async () => {
    const room = (await Room.create('reattach-demand')) as ServerRoom
    config.channel = { bufferLimit: 256 }
    try {
      const stub = register(room)
      const first = attachPeer(stub)
      const { id } = (await stub._handleRequest({ __r: 'req-join', meta: {}, selfDelivery: true })) as { id: string }
      const idle = (await stub._handleRequest({ __r: 'req-join', meta: {}, selfDelivery: true })) as { id: string }
      await vi.waitFor(() => expect(controlEvents(first).map(({ __r }) => __r)).toContain('roster'))
      stub._onPeerDisconnect(60_000)
      const observer = await Room.get(room.id)
      ;(await observer.getParticipant(id))!.subscribeBinary(() => {})
      await new Promise((resolve) => setTimeout(resolve, 20))
      // Larger than the offline buffer: it clears the buffered demand, and is dropped too.
      await Room.setMeta(room.id, { pad: 'x'.repeat(300) })
      const peer = attachPeer(stub)
      await vi.waitFor(() =>
        expect(controlEvents(peer).filter(({ __r }) => __r === 'demand-state')).toEqual([
          { __r: 'demand-state', member: id, tracks: [null] },
          { __r: 'demand-state', member: idle.id, tracks: [] },
        ]),
      )
    } finally {
      config.channel = {}
    }
  })
  it('sends a reattached client of a handed-out participant the demand its offline buffer dropped', async () => {
    const room = (await Room.create('reattach-participant-demand')) as ServerRoom
    config.channel = { bufferLimit: 256 }
    try {
      const me = (await room.join()) as ServerLocalParticipant
      const other = await room.join()
      const wanted: Array<string | null> = []
      me.onDemand((track, on) => void (on && wanted.push(track)))
      const channel = new RoomParticipantStubChannel(me)
      channel._registerChannel()
      attachPeer(channel)
      channel._onPeerDisconnect(60_000)
      const observer = await Room.get(room.id)
      ;(await observer.getParticipant(me.id))!.subscribeBinary(() => {})
      await vi.waitFor(() => expect(wanted).toEqual([null]))
      // Larger than the offline buffer: it clears the buffered demand notice.
      await other.send(me.id, 'x'.repeat(300))
      const peer = attachPeer(channel)
      await vi.waitFor(() =>
        expect(
          peer
            .decoded()
            .filter((frame) => frame.tag === TAG.TEXT)
            .map((frame) => parse(frame.text) as { __r: string })
            .filter(({ __r }) => __r === 'demand-state'),
        ).toEqual([{ __r: 'demand-state', tracks: [null] }]),
      )
    } finally {
      config.channel = {}
    }
  })
  it("applies a reattach entry's text subscription as the Room stub's want, not as a Broadcast route", async () => {
    const room = (await Room.create('reattach-text')) as ServerRoom
    const member = await room.join()
    const stub = register(room)
    attachPeer(stub)
    const peer = attachPeer(stub, undefined, { text: true, binary: false })
    await member.publish('after-reattach')
    await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toEqual(['after-reattach']))
    expect(memoryState.broadcastSubs.size).toBe(0)
  })
  it('releases a tail that is not attached within its 60 second lease', async () => {
    vi.useFakeTimers()
    const { member, tail } = await createTail('tail-pre-attach-expiry')
    await member.publish('expired')
    await vi.advanceTimersByTimeAsync(ROOM_TAIL_ATTACH_TIMEOUT_MS + 1)
    const { stub, peer } = serve(tail)
    stub._onPeerSubscription('text', true)
    expect(semanticFrames(peer, 'data')).toEqual([])
  })
  it('releases an attached tail when first text demand misses its 60 second lease', async () => {
    vi.useFakeTimers()
    const { member, tail } = await createTail('tail-post-attach-expiry')
    await member.publish('expired')
    const { stub, peer } = serve(tail)
    await vi.advanceTimersByTimeAsync(ROOM_TAIL_ATTACH_TIMEOUT_MS + 1)
    stub._onPeerSubscription('text', true)
    expect(semanticFrames(peer, 'data')).toEqual([])
  })
  it('holds no listener added to a participant after it left', async () => {
    const authority = await Room.create('dead-entry-listener')
    const member = await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    const off = observer.onChange(() => {})
    const remote = (await observer.getParticipant(member.id))!
    await member.leave()
    await vi.waitFor(() => expect(observer.count).toBe(0))
    off()
    remote.subscribe(() => {})
    remote.subscribeBinary(() => {})
    remote.onUpdate(() => {})
    expect((observer as unknown as { _state: { listenerCount: number } })._state.listenerCount).toBe(0)
  })
  it("commits a participant's publishes in call order, however long its guard takes for each", async () => {
    const room = await Room.create('publish-call-order')
    const slow = deferred<void>()
    Room.guard(room, {
      onBeforePublish: async (_from, data) => {
        if (data === 'first' || (data instanceof Uint8Array && data[0] === 1)) await slow.promise
      },
    })
    const me = await room.join()
    const texts: unknown[] = []
    const frames: number[] = []
    room.subscribe((data) => void texts.push(data))
    room.subscribeBinary((data) => void frames.push(data[0]!))
    const published = [
      me.publish('first'),
      me.publish('second'),
      me.publishBinary(new Uint8Array([1])),
      me.publishBinary(new Uint8Array([2])),
    ]
    await new Promise((resolve) => setTimeout(resolve, 0))
    slow.resolve()
    await Promise.all(published)
    await vi.waitFor(() => expect({ texts, frames }).toEqual({ texts: ['first', 'second'], frames: [1, 2] }))
  })
  it("sends a participant's next publish before the backend has answered the one before it", async () => {
    const room = await Room.create('publish-pipeline')
    const me = await room.join()
    const commitLane = driver.commitLane.bind(driver)
    const firstAnswer = deferred<void>()
    let sent = 0
    vi.spyOn(driver, 'commitLane').mockImplementation(async (...args) => {
      const first = args[2].kind === 'semantic' && ++sent === 1
      const result = await commitLane(...args)
      if (first) await firstAnswer.promise
      return result
    })
    const published = [me.publish('first'), me.publish('second')]
    await vi.waitFor(() => expect(sent).toBe(2))
    firstAnswer.resolve()
    await Promise.all(published)
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
  it("counts no demand from a selfDelivery: false publisher's own side", async () => {
    const room = (await Room.create('self-demand')) as ServerRoom
    const me = (await room.join({ selfDelivery: false })) as ServerLocalParticipant
    // `me` is held by its client, which is told its demand.
    const toClient = vi.spyOn(new RoomParticipantStubChannel(me), 'send').mockResolvedValue(undefined as never)
    const demand = () =>
      toClient.mock.calls.map(([message]) => message as { __r: string }).filter((message) => message.__r === 'demand')
    room.subscribeBinary(() => {}, { track: 'mic' })
    await subsOf(room).binaryReady()
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(demand()).toEqual([])
    const { stub } = serve(room)
    declare(stub, { __r: 'sub-binary', wants: { everyMember: { all: false, tracks: ['mic'] }, members: {} } })
    await vi.waitFor(() => expect(demand()).toEqual([{ __r: 'demand', track: 'mic', wanted: true }]))
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
    const backend = getRoomBackend()
    const broadcast = getBroadcastBackend()
    expect(backend).not.toBeInstanceOf(MemoryBackend)
    const member = await room.join()
    expect(await member.publish('works')).toMatchObject({ seq: 1 })
    const route = { key: 'zero-config-supervision', kind: 'text' } as const
    const received: string[] = []
    const firstReceived = deferred<void>()
    let secondReceived = deferred<void>()
    const first = broadcast.subscribe(route, (payload) => {
      received.push(`first:${decoder.decode(payload)}`)
      firstReceived.resolve()
    })
    const second = broadcast.subscribe(route, (payload) => {
      received.push(`second:${decoder.decode(payload)}`)
      secondReceived.resolve()
    })
    await Promise.all([first.ready, second.ready])
    await broadcast.publish(route, encoder.encode('one'))
    await Promise.all([firstReceived.promise, secondReceived.promise])
    expect(received).toEqual(['first:one', 'second:one'])
    await first.unsubscribe()
    secondReceived = deferred<void>()
    await broadcast.publish(route, encoder.encode('two'))
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
  it('copies metadata into state and freezes every public metadata view', async () => {
    const roomMeta = { topic: 'original' }
    const room = await Room.create('owned-meta', { meta: roomMeta })
    roomMeta.topic = 'caller mutation'
    expect(room.meta).toEqual({ topic: 'original' })
    expect(Object.isFrozen(room.meta)).toBe(true)
    const joinMeta = { name: 'Alice' }
    const participant = await room.join({ meta: joinMeta })
    joinMeta.name = 'caller mutation'
    expect(participant.meta).toEqual({ name: 'Alice' })
    expect(Object.isFrozen(participant.meta)).toBe(true)
    const replacement = { name: 'Bob' }
    await participant.setMeta(replacement)
    replacement.name = 'caller mutation'
    expect(participant.meta).toEqual({ name: 'Bob' })
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
  it('renders a shield failure to the caller on both failure carriers, and reports no bug', () => {
    const report = vi.fn()
    const error = new ShieldValidationError('data.text should be a string')
    expect(roomAckError(error, report)).toEqual({ text: error.message, status: ACK_STATUS.SHIELD_ERROR })
    expect(toRoomFailure(error, report)).toEqual({ ok: false, err: error.message })
    expect(report).not.toHaveBeenCalled()
  })
  it("keeps a client-held participant's meta in accepted revision order", async () => {
    let notify!: (notice: unknown) => unknown
    const acks: Array<(accepted: unknown) => void> = []
    const channel = {
      listen: (cb: (notice: unknown) => unknown) => {
        notify = cb
      },
      onClose: () => {},
      send: () => new Promise((resolve) => acks.push(resolve)),
    } as unknown as ClientChannel
    const participant = new ClientStandaloneParticipant(channel, {
      channelId: 'channel',
      id: 'me',
      meta: { v: 0 },
      selfDelivery: true,
      identity: null,
    })
    const first = participant.setMeta({ v: 'A' })
    const second = participant.setMeta({ v: 'B' })
    notify({ __r: 'p-meta', meta: { v: 'B' }, seq: 2 })
    acks[1]!({ meta: { v: 'B' }, seq: 2 })
    acks[0]!({ meta: { v: 'A' }, seq: 1 }) // the older write's ack arrives last
    await Promise.all([first, second])
    expect(participant.meta).toEqual({ v: 'B' })
  })
  it("applies a handed-out participant's whole demand set as the changes from what it had", () => {
    let notify!: (notice: unknown) => void
    const channel = {
      listen: (cb: (notice: unknown) => unknown) => {
        notify = cb
      },
      onClose: () => {},
    } as unknown as ClientChannel
    const participant = new ClientStandaloneParticipant(channel, {
      channelId: 'channel',
      id: 'me',
      meta: {},
      selfDelivery: true,
      identity: null,
    })
    notify({ __r: 'demand', track: 'screen', wanted: true })
    const demand: unknown[] = []
    participant.onDemand((track, wanted) => demand.push([track, wanted]))
    demand.length = 0
    notify({ __r: 'demand-state', tracks: [null] })
    expect(demand).toEqual([
      ['screen', false],
      [null, true],
    ])
  })
  it('keeps remote serializer backing unforgeable and exact-keyed', async () => {
    const room = await Room.create('remote-backing')
    const joined = await room.join()
    const remote = await room.getParticipant(joined.id)
    expect(remoteBacking(remote)).not.toBeNull()
    expect(remoteBacking(Object.create(remote!))).toBeNull()
    expect(Object.getOwnPropertySymbols(remote!)).toEqual([])
  })
  describe('Room-derived handle ownership (real GC)', () => {
    it('does not make a roster participant the owner of its Room wrapper', async () => {
      const gc = gcFixture('gc-list-owner')
      const retained = await retainOnlyListedRemote(gc)
      await forceRoomGc()
      expect(retained.room.deref()).toBeUndefined()
      expect(retained.member.id).toBe(gc.memberId)
      expect(gc.closed()).toBe(1)
    })
    it('does not make a participant revived with its Room the owner of the Room wrapper', async () => {
      const gc = gcFixture('gc-revived-owner')
      const retained = retainOnlyRevivedRemote(gc)
      await forceRoomGc()
      expect(retained.room.deref()).toBeUndefined()
      expect(retained.member.id).toBe(gc.memberId)
      expect(gc.closed()).toBe(1)
    })
    it('does not make a joined participant the owner of its Room wrapper', async () => {
      const gc = gcFixture('gc-join-owner', true)
      const retained = await retainOnlyJoinedParticipant(gc)
      await forceRoomGc()
      expect(retained.room.deref()).toBeUndefined()
      expect(retained.member.id).toBe(gc.memberId)
      expect(gc.closed()).toBe(1)
    })
    it('does not make a callback participant the owner of its Room wrapper', async () => {
      const gc = gcFixture('gc-callback-owner')
      const retained = await retainOnlyCallbackRemote(gc)
      await forceRoomGc()
      expect(retained.room.deref()).toBeUndefined()
      expect(retained.member.id).toBe(gc.memberId)
      expect(gc.closed()).toBe(1)
    })
    it('releases the Room wrapper from a departed participant handle', async () => {
      const gc = gcFixture('gc-departed-owner', true)
      const retained = await retainOnlyDepartedParticipant(gc)
      await forceRoomGc()
      expect(retained.member.id).toBe(gc.memberId)
      expect(retained.room.deref()).toBeUndefined()
      expect(gc.closed()).toBe(1)
    })
  })
  it('does not emit onEmpty from a partial pre-roster member map', () => {
    const { client, emit } = fakeClient('pre-roster-empty', undefined, { count: 2 })
    const memberId = crypto.randomUUID()
    let empty = 0
    client.onEmpty(() => empty++)
    emit({ __r: 'join', id: memberId, meta: {}, joinedAt: 1 })
    emit({ __r: 'leave', id: memberId }, 2)
    expect(client.count).toBe(2)
    expect(empty).toBe(0)
  })
  it('dirties unknown events and keeps reconcile outcomes distinct', () => {
    const state = new RoomState({
      roomId: 'unknown-member-epoch',
      meta: {},
      seed: { members: [] },
      updateStamp: { at: 0, by: '' },
      onListenersChanged: () => {},
      onCallbackError: () => {},
      onLeave: () => {},
    })
    const id = crypto.randomUUID()
    state.applyTrack(id, 'screen')
    state.applyParticipantMeta(id, { step: 1 }, 1)
    state.applyLeave(id, { type: 'left' })
    expect(state.membershipVersion).toBe(3)
    const member = { id, meta: {}, joinedAt: 1, metaSeq: 0 }
    expect(state.reconcileRoster([member])).toBe(true)
    const version = state.membershipVersion
    const reconcile = (tracks: string[]) => state.reconcileRoster([{ ...member, metaSeq: 1, tracks }])
    expect(reconcile(['screen'])).toBe(true)
    expect(state.membershipVersion).toBe(version)
    expect(reconcile(['screen', 'camera'])).toBe(false)
    expect(state.membershipVersion).toBe(version + 1)
  })
  it('reconciles every member to the final state and preserves semantic joins', () => {
    const alice = { id: crypto.randomUUID(), meta: {}, joinedAt: 1, metaSeq: 0 }
    const bob = { id: crypto.randomUUID(), meta: {}, joinedAt: 2, metaSeq: 0 }
    const carol = { id: crypto.randomUUID(), meta: {}, joinedAt: 3, metaSeq: 0 }
    const state = new RoomState({
      roomId: 'reconcile-change-count',
      meta: {},
      seed: { members: [alice] },
      updateStamp: { at: 0, by: '' },
      onListenersChanged: () => {},
      onCallbackError: () => {},
      onLeave: () => {},
    })
    const observed: string[][] = []
    const joins: string[] = []
    state.onChange(() => observed.push(state.snapshotMembers().map((member) => member.id)))
    state.onJoin((member) => joins.push(member.id))
    state.reconcileRoster([alice, bob, carol])
    expect(observed.at(-1)).toEqual([alice.id, bob.id, carol.id])
    expect(joins).toEqual([bob.id, carol.id])
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
      onLeave: () => {},
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
    const info = { key: state.roomId, seq: 1, timestamp: 1 }
    state.applyData({ __r: 'data', from: memberId, fromMeta: {}, data: 'text' }, info)
    state.applyBinary({ from: memberId, payload: new Uint8Array(), track: null, meta: null, retain: false }, info)
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
  it('owns fallback sender snapshots before listener fan-out', async () => {
    const participant = await (await Room.create('owned-fallback-sender')).join()
    const seen: Sender[] = []
    participant.listen((_data, from) => Reflect.set(from!.meta, 'name', 'listener mutation'))
    participant.listen((_data, from) => seen.push(from!))
    const meta = { name: 'owned' }
    const raw = participant as unknown as { _deliverMessage(message: unknown): void }
    raw._deliverMessage({ from: 'unknown', fromMeta: meta, fromIdentity: null, data: null })
    meta.name = 'wire mutation'
    expect(seen.map(({ meta }) => meta)).toEqual([{ name: 'owned' }])
    expect(Object.isFrozen(seen[0]) && Object.isFrozen(seen[0]!.meta)).toBe(true)
  })
  it('keeps a client participant active so a rejected leave request can be retried', async () => {
    let leaveAttempts = 0
    const { client } = fakeClient('retry-client-leave', {
      send: async (message) => {
        const request = message as { __r?: string }
        if (request.__r === 'req-join') return { id: crypto.randomUUID(), joinedAt: Date.now() }
        if (request.__r === 'req-leave' && leaveAttempts++ === 0) throw new Error('transient leave rejection')
        return undefined
      },
    })
    const member = await client.join()
    const causes: unknown[] = []
    client.onLeave((_, cause) => causes.push(cause))
    await expect(member.leave()).rejects.toThrow('transient leave rejection')
    await expect(member.publish('still-active')).resolves.toMatchObject({ seq: 1 })
    await expect(member.leave()).resolves.toBeUndefined()
    expect(leaveAttempts).toBe(2)
    expect(causes).toEqual([{ type: 'left' }])
  })
  it('settles a client join across a pre-ack closed event', async () => {
    const { id, ack, emit, joining } = await pendingClientJoin('pre-ack-closed')
    emit({ __r: 'closed' })
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    await expect(participant.publish('zombie-check')).rejects.toThrow(/left/i)
    const causes: unknown[] = []
    participant.onLeave((cause) => causes.push(cause))
    expect(causes).toEqual([{ type: 'closed' }])
  })
  it('drops an ack DM reply that settles after the client stub closed', async () => {
    const { id, ack, fake, emit, joining } = await pendingClientJoin('dm-reply-after-close')
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    const answer = deferred<string>()
    participant.listen(() => answer.promise)
    emit({ __r: 'dm', to: id, from: crypto.randomUUID(), fromMeta: {}, data: 'hi', ackId: 'ack-late' }, 1)
    Object.defineProperty(fake.stub, 'isClosed', { value: true })
    const send = vi.spyOn(fake.stub, 'send').mockImplementation(() => {
      throw new ChannelClosedError()
    })
    answer.resolve('late')
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(send).not.toHaveBeenCalled()
  })
  it("applies a member's whole demand set as the changes from what it had", async () => {
    const { id, ack, emit, joining } = await pendingClientJoin('demand-state')
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 1)
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    emit({ __r: 'demand', member: id, track: 'screen', wanted: true }, 2)
    emit({ __r: 'demand', member: id, track: 'mic', wanted: true }, 3)
    const demand: unknown[] = []
    participant.onDemand((track, wanted) => demand.push([track, wanted]))
    demand.length = 0
    emit({ __r: 'demand-state', member: id, tracks: ['mic', null] }, 4)
    expect(demand).toEqual([
      ['screen', false],
      [null, true],
    ])
  })
  it("delivers member-addressed events that arrive before the participant's join ack", async () => {
    const replies: unknown[] = []
    const { id, ack, emit, joining, client } = await pendingClientJoin('pre-ack-events', (message) => {
      if ((message as { __r?: string }).__r === 'dm-reply') replies.push(message)
    })
    emit({ __r: 'dm', to: id, from: crypto.randomUUID(), fromMeta: {}, data: 'welcome', ackId: 'ack-welcome' }, 1)
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 2)
    emit({ __r: 'dm', to: id, from: crypto.randomUUID(), fromMeta: {}, data: 'again', ackId: 'ack-again' }, 3)
    emit({ __r: 'demand', member: id, track: 'screen', wanted: true }, 4)
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    const demand: unknown[] = []
    participant.onDemand((track, wanted) => demand.push([track, wanted]))
    participant.listen((data) => `got ${String(data)}`)
    await vi.waitFor(() =>
      expect(replies).toEqual([
        expect.objectContaining({ ackId: 'ack-welcome', reply: { ok: true, result: 'got welcome' } }),
        expect.objectContaining({ ackId: 'ack-again', reply: { ok: true, result: 'got again' } }),
      ]),
    )
    expect(demand).toEqual([['screen', true]])
    expect(client.count).toBe(1)
  })
  it('leaves, without a ghost, a participant whose leave beat its join ack', async () => {
    const { id, ack, emit, joining, client } = await pendingClientJoin('pre-ack-leave')
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 1)
    emit({ __r: 'leave', id, cause: 'removed' }, 2)
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    const causes: unknown[] = []
    participant.onLeave((cause) => causes.push(cause.type))
    expect(causes).toEqual(['removed'])
    expect(client.count).toBe(0)
  })
  it('sends DMs from a client participant, and rejects an option a client call does not have', async () => {
    const requests: unknown[] = []
    const { id, ack, emit, joining, client } = await pendingClientJoin('client-unknown-options', (message) => {
      requests.push(message)
      return { seq: requests.length, timestamp: 1 }
    })
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 1)
    ack.resolve({ id, joinedAt: 1 })
    const me = await joining
    const calls: Array<[() => unknown, string]> = [
      [() => client.getParticipants({ all: true } as never), 'Unknown getParticipants() option: all'],
      [() => me.publish('x', { persist: true } as never), 'Unknown publish() option: persist'],
      [() => me.send(id, 'x', { confirm: true } as never), 'Unknown send() option: confirm'],
    ]
    for (const [call, message] of calls) await expect(Promise.resolve().then(call)).rejects.toThrow(message)
    await me.send(id, 'fire and forget')
    await me.send(id, 'confirmed', { ack: true })
    expect(requests).toEqual([
      expect.objectContaining({ __r: 'req-dm', data: 'fire and forget' }),
      expect.objectContaining({ __r: 'req-dm', data: 'confirmed', ack: true }),
    ])
    expect(requests[0]).not.toHaveProperty('ack')
  })
  it('ends a local participant that a roster no longer lists', async () => {
    const { id, ack, emit, joining, client } = await pendingClientJoin('roster-drops-local')
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 1)
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    const causes: unknown[] = []
    participant.onLeave((cause) => causes.push(cause.type))
    client.onLeave((_, cause) => causes.push(cause?.type))
    emit({ __r: 'roster', members: [] }, 2)
    expect(causes).toEqual(['removed', 'removed'])
  })
  it('applies the newest meta change that arrived before the first roster, unless the roster has a newer one', async () => {
    const { client, emit } = fakeClient('pre-roster-meta')
    const [newest, rosterNewer, gone] = [crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID()]
    emit({ __r: 'p-meta', id: newest, meta: { v: 2 }, seq: 2 }, 1)
    emit({ __r: 'p-meta', id: newest, meta: { v: 1 }, seq: 1 }, 2)
    emit({ __r: 'p-meta', id: rosterNewer, meta: { v: 1 }, seq: 1 }, 3)
    emit({ __r: 'p-meta', id: gone, meta: { v: 1 }, seq: 1 }, 4)
    emit(
      {
        __r: 'roster',
        members: [
          { id: newest, meta: { v: 0 }, joinedAt: 1, metaSeq: 0 },
          { id: rosterNewer, meta: { v: 3 }, joinedAt: 1, metaSeq: 3 },
        ],
      },
      5,
    )
    expect((await client.getParticipants()).map(({ id, meta }) => [id, meta])).toEqual([
      [newest, { v: 2 }],
      [rosterNewer, { v: 3 }],
    ])
  })
  it("derives participant-update prev from the receiver's own applied state", async () => {
    const { client, emit } = fakeClient('receiver-local-prev')
    const memberId = crypto.randomUUID()
    emit({ __r: 'roster', members: [{ id: memberId, meta: { step: 0 }, joinedAt: 1, metaSeq: 0 }] })
    const member = await client.getParticipant(memberId)
    const updates: Array<[unknown, unknown]> = []
    member!.onUpdate((meta, prev) => updates.push([meta, prev]))
    // This receiver skipped the writer's step:1 revision; prev is the value it actually transitioned away from.
    emit({ __r: 'p-meta', id: memberId, meta: { step: 2 }, seq: 2 }, 2)
    expect(updates).toEqual([[{ step: 2 }, { step: 0 }]])
  })
  it('makes the closed-and-empty state visible before participant leave callbacks run', async () => {
    const { client, emit } = fakeClient('atomic-close-state')
    const memberId = crypto.randomUUID()
    emit({ __r: 'roster', members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0 }] })
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
    emit({ __r: 'closed' }, 2)
    expect(observed).toEqual({
      closed: true,
      count: 0,
      snapshotChanged: true,
      snapshotClosed: true,
      snapshotCount: 0,
      participants: 0,
    })
  })
  it('gives a participant revived with its Room no lifecycle of its own', async () => {
    const { client } = fakeClient('revived-no-lifecycle')
    const context = { shareLifecycle: () => {} } as unknown as InternalClientReviverContext
    const metadata = { room: client, id: crypto.randomUUID(), meta: {}, joinedAt: 1, metaSeq: 0, identity: null }
    const revived = roomRemoteReviver.revive(metadata, context)
    await revived.close()
    revived.abort(new Error('aborted') as never)
    expect(client.isClosed).toBe(false)
    expect(client._getRemote(metadata.id)).toBe(revived.value)
  })
  it('fires onClose for a listener registered after the room closed', () => {
    const { client } = fakeClient('closed-before-listener', undefined, { closed: true, count: 0 })
    let closes = 0
    client.onClose(() => closes++)
    expect(closes).toBe(1)
  })
  it('hands out a participant revived into a closed room as having left with it', () => {
    const { client } = fakeClient('revived-into-closed', undefined, { closed: true, count: 0 })
    const member = client._reviveRemote({ id: crypto.randomUUID(), meta: {}, joinedAt: 1, metaSeq: 0, identity: null })
    const causes: unknown[] = []
    member.onLeave((cause) => causes.push(cause))
    expect(causes).toEqual([{ type: 'closed' }])
    expect(client.snapshot().participants).toEqual([])
  })
  it('fires onLeave on a directly held hidden member when its leave is relayed', () => {
    const { client, emit } = fakeClient('client-hidden-leave')
    emit({ __r: 'roster', members: [] })
    const hidden = client._reviveRemote({
      id: crypto.randomUUID(),
      meta: {},
      joinedAt: 1,
      metaSeq: 0,
      identity: null,
      hidden: true,
    })
    const causes: unknown[] = []
    hidden.onLeave((cause) => causes.push(cause?.type))
    emit({ __r: 'leave', id: hidden.id, cause: 'removed', hidden: true }, 2)
    expect(causes).toEqual(['removed'])
  })
  it('keeps a directly held hidden member while its roster carries it, and rejects client enumeration', async () => {
    const { client, emit } = fakeClient('client-hidden-roster')
    emit({ __r: 'roster', members: [] })
    const member = { id: crypto.randomUUID(), meta: { role: 'moderator' }, joinedAt: 1, metaSeq: 0, identity: null }
    const hidden = client._reviveRemote({ ...member, hidden: true })
    let left = 0
    hidden.onLeave(() => left++)
    emit({ __r: 'roster', members: [{ ...member, hidden: true }] }, 2)
    expect(client._getRemote(hidden.id)).toBe(hidden)
    await expect(client.getParticipants({ hidden: true })).rejects.toThrow(
      'Hidden participants can only be enumerated on the server',
    )
    // A roster without it: the member left before this client's stub could hear its leave.
    emit({ __r: 'roster', members: [] }, 3)
    expect(client._getRemote(hidden.id)).toBeNull()
    expect(left).toBe(1)
  })
  it("rejects a member's publish the server refused as expected, without reporting a client bug", async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    // A real ClientBroadcast whose wire never opens: the test answers its requests.
    clientConfig.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
    const stub = new ClientBroadcast({
      channelId: crypto.randomUUID(),
      key: 'publish-refused',
      transports: [CHANNEL_TRANSPORT.SSE],
      telefuncUrl: 'http://publish-refused.test/_telefunc',
      connectionKey: crypto.randomUUID(),
    })
    vi.spyOn(stub, 'send').mockImplementation((async (message: { __r?: string }) =>
      message.__r === 'req-join' ? { id: crypto.randomUUID(), joinedAt: 1 } : undefined) as never)
    try {
      const me = await new ClientRoom(stub, snapshot('publish-refused')).join()
      const publishing = me.publish('hi')
      const text = 'Participant not found (left?)'
      stub._dispatchFrame({ tag: TAG.ACK_RES, index: 0, seq: 1, ackedSeq: 1, status: ACK_STATUS.ERROR, text })
      await expect(publishing).rejects.toThrow(text)
      expect(report).not.toHaveBeenCalled()
    } finally {
      stub.abort()
      delete clientConfig.fetch
    }
  })
  it("leaves no unhandled rejection behind a client participant's un-awaited publish, binary publish or DM that fails", async () => {
    const memberId = crypto.randomUUID()
    const { client } = fakeClient('client-fire-and-forget', {
      send: async (message: any) => (message.__r === 'req-join' ? { id: memberId, joinedAt: 1 } : undefined),
    })
    const me = await client.join()
    await me.leave()
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => void unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      me.publish({ cursor: 1 })
      me.publish({ cursor: 2 }, { coalesce: 'cursor' })
      me.publishBinary(new Uint8Array([1]))
      me.send(memberId, 'dm')
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(unhandled).toEqual([])
      await expect(me.publish({ cursor: 3 })).rejects.toThrow('Participant left the room')
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
  })
  it('declares its member subscriptions while a room-wide one covers them, so the server never wants none when it stops', () => {
    const log: unknown[] = []
    const { client, emit } = fakeClient('text-want-order', {
      wireDeclarations: log as boolean[],
      send: async (message: any) => {
        if (message.__r === 'sub-text') log.push(message.members)
      },
    })
    const member = { id: crypto.randomUUID(), meta: {}, joinedAt: 1, metaSeq: 0, identity: null }
    emit({ __r: 'roster', members: [member] })
    const stopRoomWide = client.subscribe(() => {})
    log.length = 0
    client._getRemote(member.id)!.subscribe(() => {})
    // Declared at once, so a stream stopped live or in a reattach's RECONCILE finds the member set in place.
    expect(log).toEqual([[member.id]])
    stopRoomWide()
    expect(log).toEqual([[member.id], false])
  })
  it('declares nothing while its stub is closing, so an unsubscribe during the close returns normally', () => {
    let closing = false
    const { client, fake } = fakeClient('declare-while-closing', {
      // Like ClientChannel.send on a closed channel: it throws synchronously.
      send: () => {
        if (closing) throw new ChannelClosedError()
        return Promise.resolve(undefined)
      },
    })
    Object.defineProperty(fake.stub, 'isClosed', { get: () => closing })
    const unsubscribe = client.onAnnounce(() => {})
    closing = true
    expect(() => unsubscribe()).not.toThrow()
  })
  it('caps named binary tracks at the call site even while an all-track listener exists', () => {
    const { client } = fakeClient('track-cap-with-all')
    client.subscribeBinary(() => {})
    for (let track = 0; track < ROOM_WANTED_TRACKS_MAX; track++)
      client.subscribeBinary(() => {}, { track: `t${track}` })
    expect(() => client.subscribeBinary(() => {}, { track: 'one-too-many' })).toThrow(
      'subscribeBinary() can name at most',
    )
  })
  it('declares a room-level default binary track without an earlier all-track listener', () => {
    const sent: unknown[] = []
    const { client } = fakeClient('default-track-declaration', {
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    client.subscribeBinary(() => {}, { track: null })
    expect(sent).toContainEqual({
      __r: 'sub-binary',
      wants: { everyMember: { all: false, tracks: [DEFAULT_TRACK] }, members: {} },
    })
  })
  it('declares wants only when they change, so listener churn sends nothing', () => {
    const sent: unknown[] = []
    const wireDeclarations: boolean[] = []
    const { client } = fakeClient('wants-on-change', {
      wireDeclarations,
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    for (let i = 0; i < 3; i++) client.onChange(() => {})()
    expect(sent).toEqual([])
    const stop = client.onAnnounce(() => {})
    client.onChange(() => {})()
    stop()
    expect(sent).toEqual([
      { __r: 'sub-text', members: [], announce: true },
      { __r: 'sub-text', members: [], announce: false },
    ])
    expect(wireDeclarations).toEqual([])
  })
  it('turns a server roster read rejection into an explicit client-settling event', async () => {
    const room = (await Room.create('roster-error-event')) as ServerRoom
    const stub = register(room)
    const failure = new Error('backend roster read failed')
    const ensureRoster = vi.spyOn(subsOf(room), 'ensureRoster').mockRejectedValue(failure)
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const peer = attachPeer(stub)
    await vi.waitFor(() => expect(peer.decoded().some((frame) => frame.tag === TAG.PUBLISH)).toBe(true))
    const frame = peer.decoded().find((candidate) => candidate.tag === TAG.PUBLISH)!
    if (frame.tag !== TAG.PUBLISH) throw new Error('expected roster error publish')
    const { client, emit } = fakeClient('roster-error-event')
    const participants = client.getParticipants()
    emit(parse(frame.text))
    await expect(participants).rejects.toThrow('Failed to load room participants')
    expect(ensureRoster).toHaveBeenCalledOnce()
  })
  it('sends a stub its roster after the next successful refresh once its first roster read failed', async () => {
    const room = (await Room.create('roster-error-recovery')) as ServerRoom
    const stub = register(room)
    vi.spyOn(subsOf(room), 'ensureRoster').mockRejectedValueOnce(new Error('backend roster read failed'))
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const peer = attachPeer(stub)
    const events = () =>
      peer.decoded().flatMap((frame) => (frame.tag === TAG.PUBLISH ? [(parse(frame.text) as { __r: string }).__r] : []))
    await vi.waitFor(() => expect(events()).toContain('roster-error'))
    await subsOf(room)._refreshMembers()
    expect(events()).toEqual(['roster-error', 'roster'])
  })
  it('replays a committed server-pushed roster after reconnect, then sends a fresh one', async () => {
    const room = (await Room.create('roster-replay')) as ServerRoom
    const stub = register(room)
    const ensureRoster = vi.spyOn(subsOf(room), 'ensureRoster')
    const peer = attachPeer(stub)
    await vi.waitFor(() => expect(peer.decoded().some((frame) => frame.tag === TAG.PUBLISH)).toBe(true))
    stub._onPeerDisconnect(1_000)
    const [frame] = attachPeer(stub, 0).decoded()
    if (frame?.tag !== TAG.PUBLISH) throw new Error('expected replayed roster publish')
    expect(parse(frame.text)).toMatchObject({ __r: 'roster', members: [] })
    expect(ensureRoster).toHaveBeenCalledTimes(2)
  })
})
describe('room demand lifecycle', () => {
  it("aggregates remote demand only for the members this instance owns, and forgets a departed member's", () => {
    const owned = new Set(['member'])
    const delivered: Array<[string, string, boolean]> = []
    const demand = new RoomDemand(
      () => {},
      (id) => owned.has(id),
      (member, track, wanted) => delivered.push([member, track, wanted]),
    )
    demand.applyWant({ member: 'elsewhere', track: 'screen', instance: 'remote-a', on: true })
    expect(demand.isActive()).toBe(false)
    demand.applyWant({ member: 'member', track: 'screen', instance: 'remote-a', on: true })
    owned.delete('member')
    demand.forgetMember('member')
    demand.applyWant({ member: 'member', track: 'screen', instance: 'remote-b', on: true })
    expect(delivered).toEqual([['member', 'screen', true]])
    expect(demand.isActive()).toBe(false)
  })
  it("drops a reporter's demand once its lease lapses unrenewed, as when its instance crashed", () => {
    vi.useFakeTimers()
    const delivered: Array<[string, string, boolean]> = []
    const demand = new RoomDemand(
      () => {},
      () => true,
      (member, track, wanted) => delivered.push([member, track, wanted]),
    )
    demand.applyWant({ member: 'member', track: 'screen', instance: 'crashed', on: true })
    vi.advanceTimersByTime(ROOM_DEMAND_TTL_MS - 1)
    demand.heartbeat()
    expect(demand.isActive()).toBe(true)
    vi.advanceTimersByTime(1)
    demand.heartbeat()
    expect(delivered).toEqual([
      ['member', 'screen', true],
      ['member', 'screen', false],
    ])
    expect(demand.isActive()).toBe(false)
  })
})
describe('room protocol validation', () => {
  it('bounds tails by count and serialized code units at every cap edge', () => {
    const entry = (serialized: string, seq = 0) => ({ serialized, ord: { seq, timestamp: 0 }, from: '' })
    const byCount = new TailHold(() => {})
    for (let seq = 0; seq <= ROOM_TAIL_HOLD_MAX; seq++) byCount.push(entry('x', seq))
    const counted = byCount.take()
    expect(counted).toHaveLength(ROOM_TAIL_HOLD_MAX)
    expect(counted[0]!.ord.seq).toBe(1)
    const bySize = new TailHold(() => {})
    const halfPlusOne = 'x'.repeat(ROOM_TAIL_HOLD_CODE_UNITS_MAX / 2 + 1)
    bySize.push(entry(halfPlusOne, 1))
    bySize.push(entry(halfPlusOne, 2))
    bySize.push(entry('x'.repeat(ROOM_TAIL_HOLD_CODE_UNITS_MAX + 1), 3))
    expect(bySize.take().map(({ ord }) => ord.seq)).toEqual([2])
    const nonAscii = new TailHold(() => {})
    nonAscii.push(entry('💥'.repeat(ROOM_TAIL_HOLD_CODE_UNITS_MAX / 2)))
    expect(nonAscii.take()).toHaveLength(1)
  })
  it.each([
    [
      'member UUIDs',
      ['12345678-1234-1234-1234-123456789abc', '12345678-1234-1234-1234-123456789ABC'],
      (id: string) => encodeBinaryFrame(id, new Uint8Array()),
    ],
    [
      'single-unit tracks',
      Array.from({ length: 0x1_0000 }, (_, code) => String.fromCharCode(code)),
      (track: string) => encodeBinaryFrame('12345678-1234-1234-1234-123456789abc', new Uint8Array(), { track }),
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
      if (_name === 'single-unit tracks') expect(decodeBinaryFrame(frame)?.track).toBe(input)
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
    const state = new RoomState({
      roomId: 'state-wants',
      meta: {},
      seed: { members: [{ id: '__proto__', meta: {}, joinedAt: 1, metaSeq: 0 }] },
      updateStamp: { at: 0, by: '' },
      onListenersChanged: () => {},
      onCallbackError: () => {},
      onLeave: () => {},
    })
    state.getRemote('__proto__')!.subscribeBinary(() => {})
    expect(Object.getPrototypeOf(state.binaryWants().members)).toBeNull()
    expect(Object.hasOwn(state.binaryWants().members, '__proto__')).toBe(true)
  })
  it('rejects non-record metadata and malformed UTF-8 instead of normalizing them', () => {
    const memberId = crypto.randomUUID()
    for (const meta of [[], new Date(), new Map(), new Set()].map((value) => parse(stringify(value)))) {
      expect(() => encodeBinaryFrame(memberId, new Uint8Array(), { meta: meta as never })).toThrow(
        'meta should be an object',
      )
      expect(sanitizeBinaryWants({ everyMember: { all: false, tracks: [] }, members: meta })).toBeNull()
    }
    const arrayMeta = encodeBinaryFrame(memberId, new Uint8Array(), { meta: {} })
    arrayMeta[19] = '['.charCodeAt(0)
    arrayMeta[20] = ']'.charCodeAt(0)
    expect(decodeBinaryFrame(arrayMeta)).toBeNull()
    const malformedTrack = encodeBinaryFrame(memberId, new Uint8Array(), { track: 'x' })
    malformedTrack[18] = 0xff
    expect(decodeBinaryFrame(malformedTrack)).toBeNull()
  })
  it('takes a Room tag only from a plain record', () => {
    expect(hasRoomTag(Object.create({ __r: 'closed' }))).toBe(false)
  })
  it('round-trips every admitted leave cause injectively', () => {
    const causes = [
      { type: 'left' },
      { type: 'removed' },
      { type: 'removed', reason: null },
      { type: 'disconnected' },
      { type: 'closed' },
    ] satisfies LeaveCause[]
    const encoded = causes.map((cause) => stringify(leaveCauseToWire(cause)))
    expect(new Set(encoded)).toHaveLength(causes.length)
    expect(causes.map((cause) => leaveCauseFromWire(leaveCauseToWire(cause)))).toEqual(causes)
  })
  it('uses the full wire length fields instead of smaller policy caps', () => {
    const memberId = crypto.randomUUID()
    const track = 't'.repeat(255)
    const meta = { note: 'm'.repeat(5000) }
    const framed = encodeBinaryFrame(memberId, new Uint8Array([1]), { track, meta })
    expect(decodeBinaryFrame(framed)).toMatchObject({ track, meta, payload: new Uint8Array([1]) })
    expect(() => encodeBinaryFrame(memberId, new Uint8Array(), { track: 't'.repeat(256) })).toThrow('255 bytes')
    expect(() => encodeBinaryFrame(memberId, new Uint8Array(), { meta: { note: 'm'.repeat(70_000) } })).toThrow(
      '65535 bytes',
    )
    const wantsTrack = (track: string) =>
      sanitizeBinaryWants({ everyMember: { all: false, tracks: [track] }, members: {} })
    expect(wantsTrack(`${'é'.repeat(127)}t`)).not.toBeNull()
    expect(wantsTrack('é'.repeat(128))).toBeNull()
  })
})
type Peer = ReturnType<typeof attachPeer>
function attachPeer(stub: ServerChannel, lastSeq?: number, broadcast?: BroadcastSubscriptions) {
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
    { broadcast },
  )
  return { decoded: () => frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>)) }
}
function subsOf(room: Room | ServerRoom): {
  _control: LaneSubscription
  _semantic: LaneSubscription
  _refreshMembers(): Promise<void>
  _heartbeatTick(): Promise<void>
  binaryReady(): Promise<void>
  ensureRoster(): Promise<void>
  reconcileAuthority(): Promise<void>
} {
  return (room as unknown as { _subs: ReturnType<typeof subsOf> })._subs
}
function replacerContext(channels: ServerChannel[]): InternalServerReplacerContext {
  const states = new Map<symbol, unknown>()
  return {
    registerChannel: (channel: ServerChannel) => {
      channel._registerChannel()
      channels.push(channel)
    },
    validators: new Map(),
    responseState<T>(key: symbol, init: () => T): T {
      if (!states.has(key)) states.set(key, init())
      return states.get(key) as T
    },
  } as unknown as InternalServerReplacerContext
}
function declare(stub: RoomStubChannel, declaration: unknown): void {
  stub._onPeerMessage(stringify(declaration), 0)
}
function register(room: ServerRoom): RoomStubChannel {
  const stub = new RoomStubChannel(room, { grants: { selfSuppressed: new Set(), hidden: new Set() } })
  stub._registerChannel()
  room._attachStub(stub)
  return stub
}
function serve(room: ServerRoom): { stub: RoomStubChannel; peer: Peer } {
  const stub = register(room)
  return { stub, peer: attachPeer(stub) }
}
async function createTail(id: string) {
  await Room.create(id)
  const source = await Room.get(id)
  return {
    member: await source.join(),
    tail: (await Room.get(id, { tail: true })) as ServerRoom,
  }
}
function memberEvents(peer: Peer, id: string): Array<{ __r: string }> {
  return peer
    .decoded()
    .filter((frame) => frame.tag === TAG.PUBLISH)
    .map((frame) => JSON.parse(frame.text) as { __r: string; id?: string })
    .filter((event) => event.id === id)
}
/** A client Room fed every event its stub relayed, in order. */
function clientView(peer: Peer, roomId: string): ClientRoom {
  const { client, emit } = fakeClient(roomId)
  for (const frame of peer.decoded()) if (frame.tag === TAG.PUBLISH) emit(parse(frame.text), frame.info.seq)
  return client
}
function controlEvents(peer: Peer): Array<{ __r: string; members?: unknown[]; meta?: unknown }> {
  return peer
    .decoded()
    .filter((frame) => frame.tag === TAG.PUBLISH)
    .map((frame) => JSON.parse(frame.text) as { __r: string; members?: unknown[]; meta?: unknown })
}
function semanticFrames(peer: Peer, kind: 'data' | 'announce'): unknown[] {
  return peer
    .decoded()
    .filter((frame) => frame.tag === TAG.PUBLISH)
    .map((frame) => JSON.parse(frame.text) as { __r: string; data?: unknown })
    .filter((frame) => frame.__r === kind)
    .map((frame) => frame.data)
}

function delayRosterRead(roomId: string): { started: Promise<void>; release: () => void } {
  const readCells = driver.readCells.bind(driver)
  const roster = { started: deferred<void>(), release: deferred<void>() }
  vi.spyOn(driver, 'readCells').mockImplementation(async (candidateRoomId, inc, selector) => {
    if (candidateRoomId === roomId && 'prefix' in selector) {
      roster.started.resolve()
      await roster.release.promise
    }
    return readCells(candidateRoomId, inc, selector)
  })
  return { started: roster.started.promise, release: roster.release.resolve }
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
    declare(stub, { __r: 'sub-binary', wants: allBinary })
    await subsOf(serverRoom).binaryReady()
  }
  const receipt = await camera.publishBinary(new Uint8Array([byte]), retain ? { retain: true } : undefined)
  if (retain) {
    declare(stub, { __r: 'sub-binary', wants: allBinary })
    await subsOf(serverRoom).binaryReady()
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
  stub: ClientBroadcast
  emitText(data: unknown, info: ChannelPublishInfo): void
  emitBinary(data: Uint8Array, info: ChannelPublishInfo): void
} {
  const text: Array<(data: unknown, info: ChannelPublishInfo) => void> = []
  const binary: Array<(data: Uint8Array, info: ChannelPublishInfo) => void> = []
  const stub = {
    _wire: { text: false, binary: false },
    _isClosed: false,
    _connection: {
      sendBroadcastSubscribe: () => options?.wireDeclarations?.push(true),
      sendBroadcastUnsubscribe: () => options?.wireDeclarations?.push(false),
    },
    _subscribeLocal: (kind: 'text' | 'binary', callback: never) => {
      const listeners: unknown[] = kind === 'text' ? text : binary
      listeners.push(callback)
      return () => listeners.splice(listeners.indexOf(callback), 1)
    },
    _setWireSubscribed: ClientBroadcast.prototype._setWireSubscribed,
    send: options?.send ?? (async () => undefined),
    _publishUnreported: async () => ({ key: 'fake', seq: 1, timestamp: 1 }),
    _publishBinaryUnreported: async () => ({ key: 'fake', seq: 1, timestamp: 1 }),
    onClose: () => {},
  } as unknown as ClientBroadcast
  return {
    stub,
    emitText: (data, info) => text.forEach((callback) => callback(data, info)),
    emitBinary: (data, info) => binary.forEach((callback) => callback(data, info)),
  }
}
function fakeClient(
  roomId: string,
  options?: Parameters<typeof createFakeStub>[0],
  snapshotOverride?: Partial<RoomSnapshotMetadata>,
) {
  const fake = createFakeStub(options)
  return {
    fake,
    client: new ClientRoom(fake.stub, { ...snapshot(roomId), ...snapshotOverride }),
    emit: (data: unknown, seq = 1) => fake.emitText(data, { key: roomId, seq, timestamp: seq }),
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
async function pendingClientJoin(roomId: string, onSend?: (message: unknown) => unknown) {
  const id = crypto.randomUUID()
  const ack = deferred<{ id: string; joinedAt: number }>()
  const { fake, client, emit } = fakeClient(roomId, {
    send: async (message) => {
      if ((message as { __r?: string }).__r === 'req-join') return await ack.promise
      return await onSend?.(message)
    },
  })
  const joining = client.join()
  await Promise.resolve()
  return { id, ack, fake, client, emit, joining }
}
async function forceRoomGc(): Promise<void> {
  for (let cycle = 0; cycle < 8; cycle++) {
    ;(globalThis as { gc(): void }).gc()
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}
function gcFixture(roomId: string, join = false) {
  const memberId = crypto.randomUUID()
  const fake = createFakeStub(
    join
      ? { send: async (message: any) => (message.__r === 'req-join' ? { id: memberId, joinedAt: 1 } : undefined) }
      : {},
  )
  let closeCount = 0
  return {
    memberId,
    fake,
    target: new ClientRoom(fake.stub, snapshot(roomId)),
    registry: new GcRegistry(5),
    onClose: () => {
      closeCount++
    },
    closed: () => closeCount,
  }
}
type GcFixture = ReturnType<typeof gcFixture>
async function retainOnlyListedRemote({ target, fake, registry, onClose, memberId }: GcFixture) {
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
function retainOnlyRevivedRemote({ target, registry, onClose, memberId }: GcFixture) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  const context = { shareLifecycle: () => {} } as unknown as InternalClientReviverContext
  const metadata = { room, id: memberId, meta: {}, joinedAt: 1, metaSeq: 0, identity: null }
  return { member: roomRemoteReviver.revive(metadata, context).value, room: new WeakRef(room) }
}
async function retainOnlyJoinedParticipant({ target, registry, onClose }: GcFixture) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  return { member: await room.join(), room: new WeakRef(room) }
}
async function retainOnlyCallbackRemote({ target, fake, registry, onClose, memberId }: GcFixture) {
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
async function retainOnlyDepartedParticipant({ target, fake, registry, onClose, memberId }: GcFixture) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  const member = await room.join()
  fake.emitText({ __r: 'leave', id: memberId }, { key: target.id, seq: 1, timestamp: 1 })
  return { member, room: new WeakRef(room) }
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
    state: () => 'closed',
    onStateChange: () => () => {},
    unsubscribe: async () => {},
  }
}
function captureOutcome<T>(promise: Promise<T>) {
  const outcome: { value: unknown } = { value: Symbol('pending') }
  void promise.then(
    (value) => {
      outcome.value = value
    },
    (error: unknown) => {
      outcome.value = error
    },
  )
  return outcome
}
function deferred<T>() {
  let resolve!: (value?: T) => void
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise as (value?: T) => void
  })
  return { promise, resolve }
}
function mockLaneSubscription(
  kind: LaneId['kind'],
  replacement: (
    subscribeLane: ReturnType<typeof getRoomBackend>['subscribeLane'],
    roomId: string,
    inc: string,
    lane: LaneId,
    receiver: BackendReceiver,
  ) => BackendSubscription,
) {
  const backend = getRoomBackend()
  const subscribeLane = backend.subscribeLane.bind(backend)
  vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) =>
    lane.kind === kind
      ? replacement(subscribeLane, roomId, inc, lane, receiver)
      : subscribeLane(roomId, inc, lane, receiver),
  )
  return backend
}
function rejectLaneSubscriptions(kind: LaneId['kind'], diagnostic: string) {
  vi.useFakeTimers()
  const started = deferred<void>()
  const backend = mockLaneSubscription(kind, () => {
    started.resolve()
    return rejectedSubscription(diagnostic)
  })
  vi.spyOn(console, 'error').mockImplementation(() => {})
  return { backend, started: started.promise }
}
/** Loses the frames picked on matching lanes opened from now on, as a lane that stays ready. */
function loseLaneFrames(matches: (lane: LaneId) => boolean) {
  const backend = getRoomBackend()
  const subscribeLane = backend.subscribeLane.bind(backend)
  let pick: (text: string) => boolean = () => false
  vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
    if (!matches(lane)) return subscribeLane(roomId, inc, lane, receiver)
    return subscribeLane(roomId, inc, lane, (payload, info) => {
      if (!pick(decoder.decode(payload))) receiver(payload, info)
    })
  })
  return {
    next() {
      pick = () => ((pick = () => false), true)
    },
    where(picks: (text: string) => boolean) {
      pick = picks
    },
  }
}
/** Holds the frames delivered on matching lanes opened from now on, until released in delivery order. */
function holdLaneDelivery(matches: (lane: LaneId) => boolean) {
  const backend = getRoomBackend()
  const subscribeLane = backend.subscribeLane.bind(backend)
  const held: Array<() => void | Promise<void>> = []
  let holding = true
  vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
    if (!matches(lane)) return subscribeLane(roomId, inc, lane, receiver)
    return subscribeLane(roomId, inc, lane, (payload, info) => {
      if (!holding) return receiver(payload, info)
      held.push(() => receiver(payload, info))
    })
  })
  return {
    async release() {
      holding = false
      for (const deliver of held.splice(0)) await deliver()
    },
  }
}
/** A matching lane's driver attempt receives only once released, as a Redis SUBSCRIBE still in flight. */
function delayDriverLane(matches: (lane: LaneId) => boolean): () => void {
  const bind = driver.subscriptions.bind.bind(driver.subscriptions)
  let release: () => void = () => {}
  vi.spyOn(driver.subscriptions, 'bind').mockImplementation((source) => {
    const binding = bind(source)
    if (!('lane' in source) || !matches(source.lane)) return binding
    return {
      ...binding,
      open: (receiver, localReceiverCount): SubscriptionAttempt => {
        let inner: SubscriptionAttempt | null = null
        let state: SubscriptionState = 'establishing'
        const listeners = new Set<(next: SubscriptionState) => void>()
        release = () => {
          inner = binding.open(receiver, localReceiverCount)
          state = inner.state()
          for (const listener of listeners) listener(state)
        }
        return {
          state: () => state,
          onStateChange: (listener) => {
            listeners.add(listener)
            return () => listeners.delete(listener)
          },
          unsubscribe: async () => {
            state = 'closed'
            await inner?.unsubscribe()
          },
        }
      },
    }
  })
  return () => release()
}
function delayLaneSubscription(matches: (lane: LaneId) => boolean) {
  const backend = getRoomBackend()
  const subscribeLane = backend.subscribeLane.bind(backend)
  const started = deferred<void>()
  let release!: () => Promise<void>
  vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) => {
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
  return { started: started.promise, release: () => release() }
}
