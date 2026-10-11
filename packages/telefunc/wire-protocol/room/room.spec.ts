import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ChannelClosedError, ChannelOverflowError } from '../channel-errors.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { IndexedPeer } from '../server/IndexedPeer.js'
import { CHANNEL_PING_INTERVAL_MS, CHANNEL_RECONNECT_TIMEOUT_MS, CHANNEL_TRANSPORT } from '../constants.js'
import { ACK_STATUS, ProtocolViolationError, TAG, decode, type BroadcastSubscriptions } from '../shared-ws.js'
import { ShieldValidationError, isShieldValidationError } from '../../shared/ShieldValidationError.js'
import { Abort } from '../../shared/Abort.js'
import { createDeferred } from '../../utils/createDeferred.js'
import {
  ROOM_DEMAND_TTL_MS,
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_MEMBER_TTL_MS,
  ROOM_HORIZON_MS,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
  ROOM_TAIL_HOLD_CODE_UNITS_MAX,
  ROOM_TAIL_HOLD_MAX,
  ROOM_TRACK_WANTS_MAX,
  ROOM_MEMBER_NAMED_LANES_MAX,
  ROOM_EMPTY_TIMEOUT_MS,
  ROOM_DEPARTURE_TIMEOUT_MS,
  ROOM_GOSSIP_IN_FLIGHT_MAX,
} from './constants.js'
import { DEFAULT_TRACK, decodeBinaryFrame, encodeBinaryFrame } from './binary.js'
import { RoomError, isRoomError, roomAckError, toRoomFailure } from './errors.js'
import { mergeAttributes } from './model.js'
import {
  hasRoomTag,
  leaveCauseFromWire,
  leaveCauseToWire,
  type InboxMessage,
  type RoomSnapshotMetadata,
} from './protocol.js'
import { HOLDS_CELL_KEY, JOINS_CELL_KEY, MEMBER_CELL_PREFIX, memberCellKey } from './server/cells.js'
import { holdViews } from './server/membership.js'
import type { LeaveCause, ParticipantMeta, Sender } from './types.js'
import { ClientRoom, ClientStandaloneParticipant } from './client.js'
import { ClientBroadcast, type ClientChannel } from '../client/channel.js'
import { RoomState, type RoomStateView, remoteBacking } from './state.js'
import { Room } from './server/statics.js'
import { ServerRoom, type ServerLocalParticipant } from './server/room.js'
import { configFromHead, decodeRoomText, encodeRoomRecord } from './server/lanes.js'
import { decodeBinaryWantChange } from './server/requests.js'
import { config, getServerConfig } from '../../node/server/serverConfig.js'
import { config as clientConfig } from '../../client/clientConfig.js'
import type { LaneSubscription } from './server/lane-subscription.js'
import { reportRoomError } from './server/errors.js'
import { heldSendWeight } from '../backend/held-send-weight.js'
import { MemberMessageTexts, ResponseRoomGrants, RoomParticipantStubChannel, RoomStubChannel } from './server/stub.js'
import { TailHold } from './server/tail.js'
import { WantsIndex } from './server/wants-index.js'
import { RoomDemand } from './demand.js'
import { roomParticipantReplacer, roomRemoteReplacer, roomReplacer } from './response-server.js'
import { roomRemoteReviver } from './response-client.js'
import type { InternalClientReviverContext, InternalServerReplacerContext } from '../types.js'
import { reconnectWindow, type ServerChannel } from '../server/channel.js'
import type { ChannelPublishInfo } from '../channel.js'
import { disposeBackend, getBroadcastBackend, getRoomBackend, installBackend } from '../backend/install.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import type { LaneId, RoomSubscriptionSource } from '../backend/room/contract.js'
import { encodeLaneKey, everyTrackLaneKey } from '../backend/room/lane-key.js'
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
type SubscribedLane = RoomSubscriptionSource['lane']
const HOLD_RECORD_KEYS = [HOLDS_CELL_KEY, JOINS_CELL_KEY].join()
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
  it('a pending acknowledged send fails as its client-held recipient leaves, not at the ack timeout', async () => {
    const room = (await Room.create('recipient-leaves-ack')) as ServerRoom
    const sender = await room.join()
    const stub = register(room)
    const recipient = await joinThrough(stub) // its client never answers
    let outcome: unknown = 'pending'
    void sender.send(recipient, 'ping', { ack: true }).then(
      () => (outcome = 'answered'),
      (error: unknown) => (outcome = isRoomError(error) ? error.message : error),
    )
    await new Promise((resolve) => setTimeout(resolve, 10))
    await stub._handleRequest({ __r: 'req-leave', id: recipient })
    await vi.waitFor(() => expect(outcome).toBe('Participant left the room'), { timeout: 2000 })
  })

  it('opens semantic ingestion only when a semantic listener wants delivery', async () => {
    const room = (await Room.create('semantic-demand')) as ServerRoom
    let semanticSubscriptions = 0
    mockLaneSubscription('semantic', (subscribeLane, roomId, inc, lane, receiver) => {
      semanticSubscriptions++
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
    loseLaneFrames((lane) => lane.kind === 'control').where((text) => (parse(text) as { __r?: string }).__r === 'leave')
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
  it('admits a thousand simultaneous joins, each read racing the others’ writes, with no conflict, and counts each as a hold', async () => {
    const room = (await Room.create('join-burst')) as ServerRoom
    const readCells = driver.readCells.bind(driver)
    // Every read yields, so the other joins' writes land between a join's read and its write.
    vi.spyOn(driver, 'readCells').mockImplementation(async (...args) => {
      const read = await readCells(...args)
      await new Promise((resolve) => setTimeout(resolve, 0))
      return read
    })
    const compareExchange = driver.compareExchangeCells.bind(driver)
    let conflicts = 0
    vi.spyOn(driver, 'compareExchangeCells').mockImplementation(async (...args) => {
      const result = await compareExchange(...args)
      if (result === 'conflict') conflicts++
      return result
    })
    await Promise.all(Array.from({ length: 1_000 }, (_, i) => room.join({ identity: `user-${i % 50}` })))
    expect(conflicts).toBe(0)
    expect(await Room.getParticipants(room.id)).toHaveLength(1_000)
    const read = await readCells(room.id, room._inc, { keys: [HOLDS_CELL_KEY] })
    if ('staleInc' in read) throw new Error('the room is gone')
    expect(JSON.parse(decoder.decode(read.cells.get(HOLDS_CELL_KEY)!))).toMatchObject({ value: 1_000 })
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
    const id = await joinThrough(stub)
    const request = { __r: 'req-leave' as const, id }
    const failure = new Error('transient member delete failure')
    vi.spyOn(driver, 'compareExchangeCells').mockRejectedValueOnce(failure)
    await expect(stub._handleRequest(request)).rejects.toBe(failure)
    expect(stub._holds(id)).toBe(true)
    await expect(stub._handleRequest(request)).resolves.toBeUndefined()
    expect(await Room.getParticipants(room.id)).toEqual([])
  })
  it("routes a client member's meta, attribute and DM requests through its Room stub", async () => {
    const room = (await Room.create('stub-requests')) as ServerRoom
    const stub = register(room)
    const id = await joinThrough(stub, { name: 'a' })
    const other = await room.join()
    const inbox: unknown[] = []
    other.listen((data) => void inbox.push(data))
    await stub._handleRequest({ __r: 'req-set-meta', id, meta: { name: 'b' } })
    await stub._handleRequest({ __r: 'req-set-attrs', id, attrs: { score: 1 } })
    await stub._handleRequest({ __r: 'req-dm', id, to: other.id, data: 'hi' })
    expect((await room.getParticipants()).find((member) => member.id === id)?.meta).toEqual({ name: 'b', score: 1 })
    await vi.waitFor(() => expect(inbox).toEqual(['hi']))
  })
  it('relays a leave that reached this instance with no event, from a reconciled roster or a vanished record', async () => {
    const control = loseLaneFrames((lane) => lane.kind === 'control')
    const room = (await Room.create('lost-leave-owner')) as ServerRoom
    const { stub, peer } = serve(room)
    const reconciled = await joinThrough(stub)
    const vanished = await joinThrough(stub)
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
    const updates = relayed(peer).filter((event) => event.__r === 'update')
    // The first is the attach's own resend.
    expect(updates.map((event) => event.meta)).toEqual([[['topic', 'old', 0, 0]], [['topic', 'new', 0, 0]]])
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
    const grants = new ResponseRoomGrants()
    grants.hidden.add(bot.id)
    const handed = new RoomStubChannel(room, { grants })
    handed._registerChannel()
    room._attachStub(handed)
    const peer = attachPeer(handed)
    await subsOf(room).reconcileAuthority()
    control.next()
    await bot.setMeta({ mood: 'new' })
    await subsOf(room).reconcileAuthority()
    const metas = relayed(peer)
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
    const ticked = createDeferred()
    // The eviction's retained listing: its member delete committed, its leave isn't published.
    vi.spyOn(driver, 'listRetained').mockImplementationOnce(async (...args) => {
      await subsOf(room)._heartbeatTick()
      ticked.resolve()
      return listRetained(...args)
    })
    await member.leave()
    await ticked.promise
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
    const readCellPages = driver.readCellPages.bind(driver)
    const memberRead = { started: createDeferred(), release: createDeferred(), held: false }
    vi.spyOn(driver, 'readCellPages').mockImplementation(async function* (roomId, inc, prefix) {
      if (!memberRead.held && prefix === MEMBER_CELL_PREFIX) {
        memberRead.held = true
        memberRead.started.resolve()
        await memberRead.release.promise
      }
      yield* readCellPages(roomId, inc, prefix)
    })
    const listRetained = driver.listRetained.bind(driver)
    const committed = createDeferred()
    const publish = createDeferred()
    vi.spyOn(driver, 'listRetained').mockImplementationOnce(async (...args) => {
      committed.resolve()
      await publish.promise
      return listRetained(...args)
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
  it('lists rooms in code point order, as the Redis and Cloudflare backends do', async () => {
    for (const id of ['order:a', 'order:\u{1f600}', 'order:é', 'order:\uff01', 'order:B', 'order:f'])
      await Room.create(id)
    expect((await Room.list({ prefix: 'order:' })).map(({ id }) => id)).toEqual([
      'order:B',
      'order:a',
      'order:f',
      'order:é',
      'order:\uff01',
      'order:\u{1f600}',
    ])
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
    await expect(Room.close('lifecycle-never-created')).resolves.toBeUndefined()
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
    const observationReady = createDeferred()
    const observationStopped = createDeferred()
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
    const closed = createDeferred()
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
    let terminal: ReturnType<typeof terminalSubscription> | undefined
    const replacementReady = createDeferred()
    mockLaneSubscription('semantic', (subscribeLane, roomId, inc, lane, receiver) => {
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
  it("reports a shared lane's terminal end once, however many Room instances observe it", async () => {
    const room = await Room.create('terminal-shared')
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
    const observers = [room, await Room.get(room.id), await Room.get(room.id)] as ServerRoom[]
    for (const observer of observers) observer.subscribe(() => {})
    for (const observer of observers) await subsOf(observer)._semantic.ready
    end!(reason)
    await vi.waitFor(() => expect(bugs).toContainEqual(expect.objectContaining({ cause: reason })))
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(bugs.filter((bug) => (bug as { cause?: unknown }).cause === reason)).toHaveLength(1)
  })
  it('does not re-subscribe a recovered lane when its catch-up reconcile fails', async () => {
    const authority = await Room.create('recovered-reconcile-failure')
    const observer = (await Room.get(authority.id)) as ServerRoom
    let terminal: ReturnType<typeof terminalSubscription> | undefined
    let semanticOpens = 0
    mockLaneSubscription('semantic', (subscribeLane, roomId, inc, lane, receiver) => {
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
    const terminals: Array<ReturnType<typeof terminalSubscription>> = []
    let semanticOpens = 0
    mockLaneSubscription('semantic', (subscribeLane, roomId, inc, lane, receiver) => {
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
    const catchingUp = createDeferred()
    const release = createDeferred()
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
    let terminal: ReturnType<typeof terminalSubscription> | undefined
    mockLaneSubscription('control', (subscribeLane, roomId, inc, lane, receiver) => {
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
    expect(relayed(peer).map(({ __r }) => __r)).toContain('closed')
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
    deliver(encodeRoomRecord({ __r: 'update', meta: [['step', 1, 0, 0]], seq: 1 }), { seq: 5, timestamp: at })
    deliver(encodeRoomRecord({ __r: 'update', meta: [['step', 2, 1, 0]], seq: 2 }), { seq: 1, timestamp: at + 1 })
    expect(room.meta).toEqual({ step: 2 })
  })
  it('keeps the newest room meta when an older update arrives after it', async () => {
    let deliver!: BackendReceiver
    mockLaneSubscription('control', (subscribeLane, roomId, inc, lane, receiver) => {
      deliver = receiver
      return subscribeLane(roomId, inc, lane, receiver)
    })
    const room = await Room.create('control-late-update')
    const updates: unknown[] = []
    room.onUpdate((meta) => updates.push(meta))
    deliver(encodeRoomRecord({ __r: 'update', meta: [['step', 2, 1, 0]], seq: 2 }), { seq: 1, timestamp: 1 })
    deliver(encodeRoomRecord({ __r: 'update', meta: [['step', 1, 0, 0]], seq: 1 }), { seq: 2, timestamp: 2 })
    expect({ meta: room.meta, updates }).toEqual({ meta: { step: 2 }, updates: [{ step: 2 }] })
  })
  it('reconciles authority after a same-attempt recovery', async () => {
    const room = (await Room.create('control-reconcile')) as ServerRoom
    const controlSubscribed = createDeferred()
    let transition!: (state: SubscriptionState) => void
    mockLaneSubscription('control', (subscribeLane, roomId, inc, lane, receiver) => {
      const inner = subscribeLane(roomId, inc, lane, receiver)
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
  it("holds publishes behind this instance's establishing lane while their bytes, each with its overhead, fit bufferLimit, refusing one past it with ChannelOverflowError", async () => {
    const room = (await Room.create('establishing-bound')) as ServerRoom
    const member = await room.join()
    const received: unknown[] = []
    const release = delayDriverLane((lane) => lane.kind === 'semantic')
    room.subscribe((data) => received.push(data))
    const held = Array.from({ length: 1_200 }, (_, n) => member.publish(n))
    await new Promise((resolve) => setTimeout(resolve, 0))
    release()
    await Promise.all(held)
    await vi.waitFor(() => expect(received).toHaveLength(1_200))
    const flooded = (await Room.create('establishing-flood')) as ServerRoom
    const flooding = await flooded.join()
    const release1 = delayDriverLane((lane) => lane.kind === 'semantic')
    flooded.subscribe(() => {})
    // Their payloads alone fit bufferLimit; with each one's overhead they don't.
    const flood = Array.from({ length: 4_000 }, (_, n) => captureOutcome(flooding.publish(n)))
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(flood.at(-1)!.value).toBeInstanceOf(ChannelOverflowError)
    release1()
    const release2 = delayDriverLane((lane) => lane.kind === 'binary')
    room.subscribeBinary(() => {}, { track: 'camera' })
    const pastBytes = captureOutcome(
      member.publishBinary(new Uint8Array(getServerConfig().channel.bufferLimitBinary + 1), { track: 'camera' }),
    )
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(pastBytes.value).toBeInstanceOf(ChannelOverflowError)
    release2()
  })
  it("holds a member's binary publishes behind this instance's establishing every-track lane, refusing one past bufferLimitBinary", async () => {
    const room = (await Room.create('establishing-every-track')) as ServerRoom
    const member = await room.join()
    const received: string[] = []
    const release = delayDriverLane((lane) => lane.kind === 'binary-all')
    room.subscribeBinary((data, info) => received.push(`${info.track}:${data[0]}`))
    const held = [
      member.publishBinary(new Uint8Array([1]), { track: 'camera' }),
      member.publishBinary(new Uint8Array([2])),
    ]
    const pastBytes = captureOutcome(
      member.publishBinary(new Uint8Array(getServerConfig().channel.bufferLimitBinary), { track: 'screen' }),
    )
    await vi.waitFor(() => expect(pastBytes.value).toBeInstanceOf(ChannelOverflowError))
    release()
    await Promise.all(held)
    // Each track is its own order.
    await vi.waitFor(() => expect([...received].sort()).toEqual(['camera:1', 'null:2']))
  })
  it.each([
    ['its every-track lane', 'binary-all', (room: ServerRoom) => void room.subscribeBinary(() => {})],
    ['its named lanes', 'binary', (room: ServerRoom, track: string) => void room.subscribeBinary(() => {}, { track })],
  ] as const)(
    'holds at most bufferLimitBinary of one member across its tracks behind %s',
    async (_, kind, subscribe) => {
      const room = (await Room.create(`establishing-member-budget-${kind}`)) as ServerRoom
      const member = await room.join()
      const tracks = trackNames(8)
      const release = delayDriverLane((lane) => lane.kind === kind)
      for (const track of tracks) subscribe(room, track)
      // One frame of half the bound fits it; the other seven are past it.
      const half = new Uint8Array(getServerConfig().channel.bufferLimitBinary / 2)
      const outcomes = tracks.map((track) => captureOutcome(member.publishBinary(half, { track })))
      await new Promise((resolve) => setTimeout(resolve, 0))
      expect(outcomes.filter(({ value }) => value instanceof ChannelOverflowError)).toHaveLength(7)
      release()
      // The one held frame is sent.
      await vi.waitFor(() => expect(outcomes.filter(({ value }) => typeof value === 'symbol')).toEqual([]))
    },
  )
  it('reads authority while the control subscription is establishing', async () => {
    const authority = await Room.create('establishing-roster')
    const observer = (await Room.get(authority.id)) as ServerRoom
    expect(await observer.getParticipants()).toEqual([])
    const readiness = createDeferred()
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
    const readCellPages = driver.readCellPages.bind(driver)
    let score = 0
    vi.spyOn(driver, 'readCellPages').mockImplementation(async function* (roomId, inc, prefix) {
      yield* readCellPages(roomId, inc, prefix)
      // A member's meta change reaches this view during every roster read.
      if (prefix === MEMBER_CELL_PREFIX && score < 100) await player.setAttributes({ score: ++score })
    })
    vi.spyOn(console, 'error').mockImplementation(() => {})
    observer.onJoin(() => {})
    // The last change reached this view after the read that loaded it, and is newer than what the read saw.
    expect((await observer.getParticipants()).map(({ id, meta }) => ({ id, meta }))).toEqual([
      { id: player.id, meta: { score } },
    ])
  })
  it('makes roster readers join one authoritative refresh', async () => {
    const authority = (await Room.create('roster-refresh-owner')) as ServerRoom
    await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    observer.onJoin(() => {})
    await observer.getParticipants()
    await vi.waitFor(() => expect(subsOf(observer)._control.established).toBe(true))
    expect(observer._state.rosterKnown).toBe(true)
    const readCellPages = driver.readCellPages.bind(driver)
    const started = createDeferred()
    const release = createDeferred()
    let held = false
    vi.spyOn(driver, 'readCellPages').mockImplementation(async function* (roomId, inc, prefix) {
      if (!held && prefix === MEMBER_CELL_PREFIX) {
        held = true
        started.resolve()
        await release.promise
      }
      yield* readCellPages(roomId, inc, prefix)
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
  })
  it('keeps the joins and leaves that land between two slices of applying a roster of 100,000 members, each narrated once', async () => {
    const authority = (await Room.create('roster-apply-races')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 100_000 }, () => authority.join()))
    const observer = (await Room.get(authority.id)) as ServerRoom
    const narrated: string[] = []
    observer.onJoin((member) => narrated.push(`join ${member.id}`))
    observer.onLeave((member) => narrated.push(`leave ${member.id}`))
    await observer.getParticipants()
    await vi.waitFor(() => expect(subsOf(observer)._control.established).toBe(true))
    const expected: string[] = []
    const race = async (leaving: (typeof members)[number]) => {
      const joined = await authority.join()
      await leaving.leave()
      expected.push(`join ${joined.id}`, `leave ${leaving.id}`)
    }
    const state = observer._state
    const reconciling = state.reconcilingRoster.bind(state)
    vi.spyOn(state, 'reconcilingRoster').mockImplementation(function* (roster, events) {
      const steps = reconciling(roster, events)
      for (let step = 1; ; step++) {
        const next = steps.next()
        if (next.done) return next.value
        // Midway through merging the roster, a member it has not merged yet leaves; midway through removing what it
        // lacks, one it already passed does. The apply's slices run on while each race lands.
        const leaving = step === 150_000 ? members[99_999] : step === 250_000 ? members[1] : undefined
        let landed = leaving === undefined
        if (leaving !== undefined) void race(leaving).then(() => (landed = true))
        do yield
        while (!landed)
      }
    })
    await subsOf(observer)._refreshMembers()
    expect(narrated).toEqual(expected)
    expect((await observer.getParticipants()).length).toBe(100_000)
  }, 60_000)
  it('applies the membership events that race its roster read, each once, without reading again', async () => {
    const authority = (await Room.create('roster-refresh-races')) as ServerRoom
    const first = await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    const narrated: string[] = []
    observer.onJoin((member) => narrated.push(`join ${member.id}`))
    observer.onLeave((member) => narrated.push(`leave ${member.id}`))
    observer.onParticipantUpdate((member, meta) => narrated.push(`meta ${member.id} ${JSON.stringify(meta)}`))
    await observer.getParticipants()
    await vi.waitFor(() => expect(subsOf(observer)._control.established).toBe(true))
    const readCellPages = driver.readCellPages.bind(driver)
    let rosterReads = 0
    const expected: string[] = []
    const kept: string[] = []
    vi.spyOn(driver, 'readCellPages').mockImplementation(async function* (roomId, inc, prefix) {
      yield* readCellPages(roomId, inc, prefix)
      if (prefix !== MEMBER_CELL_PREFIX || ++rosterReads > 6) return
      // Six membership events reach this view after the read: three joins, a meta change and two leaves.
      const [left, updated, gone] = [await authority.join(), await authority.join(), await authority.join()]
      await updated.setAttributes({ read: rosterReads })
      await left.leave()
      await gone.leave()
      expected.push(
        `join ${left.id}`,
        `join ${updated.id}`,
        `join ${gone.id}`,
        `meta ${updated.id} {"read":${rosterReads}}`,
        `leave ${left.id}`,
        `leave ${gone.id}`,
      )
      kept.push(updated.id)
      await vi.waitFor(() => expect(narrated).toEqual(expected))
    })
    await subsOf(observer)._refreshMembers()
    expect(rosterReads).toBe(1)
    expect(narrated).toEqual(expected)
    expect((await observer.getParticipants()).map(({ id, meta }) => ({ id, meta }))).toEqual([
      { id: first.id, meta: {} },
      ...kept.map((id) => ({ id, meta: { read: 1 } })),
    ])
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
    const headRead = createDeferred()
    const readOpenConfig = observer._readOpenConfig.bind(observer)
    vi.spyOn(observer, '_readOpenConfig').mockImplementationOnce(async () => {
      await headRead.promise
      return readOpenConfig()
    })
    await first.close()
    // A replan, and the roster refresh it starts, while the recovery reads the head.
    observer.onLeave(() => {})
    await subsOf(observer).ensureRoster()
    headRead.resolve()
    await vi.advanceTimersByTimeAsync(100)
    expect(opens).toBe(2)
    await vi.advanceTimersByTimeAsync(ROOM_HEARTBEAT_INTERVAL_MS)
    expect(opens).toBe(3)
    expect(subsOf(observer)._control.established).toBe(true)
  })
  it('retries a still-wanted lost subscription on the next planning pass', async () => {
    vi.useFakeTimers()
    const observer = await Room.get((await Room.create('single-recovery-horizon')).id)
    let attempts = 0
    mockLaneSubscription('semantic', (subscribeLane, roomId, inc, lane, receiver) => {
      attempts++
      return attempts < 3 ? rejectedSubscription(`attempt ${attempts}`) : subscribeLane(roomId, inc, lane, receiver)
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    // The roster is read first, so the lane's own pass is the only one before the heartbeat.
    observer.onJoin(() => {})
    await subsOf(observer).ensureRoster()
    observer.subscribe(() => {})
    await vi.advanceTimersByTimeAsync(100)
    // The lane and its one replacement; the next attempt waits for the heartbeat.
    expect(attempts).toBe(2)
    expect(report.mock.calls.map(([logged]) => String(logged).split('\n')[0])).toEqual([
      'Error: attempt 1',
      'Error: attempt 2',
    ])
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
    const outcome = captureOutcome(observer._replayRetainedText(stub, { all: false, members: new Map() }))
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
  it('fires onEmpty when the last player leaves while a hidden participant stays seated', async () => {
    const room = await Room.create('empty-beside-hidden')
    const authority = await room.join({ identity: 'authority', hidden: true })
    const player = await room.join()
    let empty = 0
    room.onEmpty(() => empty++)
    await player.leave()
    expect({ empty, count: room.count }).toEqual({ empty: 1, count: 0 })
    await expect(authority.publish('tick')).resolves.toMatchObject({ seq: expect.any(Number) })
  })
  it('calls a callback once per registration in registration order, and each unlisten removes only its own', async () => {
    const room = await Room.create('listener-registrations')
    const calls: string[] = []
    const a = () => void calls.push('a')
    const offFirst = room.onJoin(a)
    room.onJoin(() => void calls.push('b'))
    const offSecond = room.onJoin(a)
    await room.join()
    offFirst()
    await room.join()
    offSecond()
    await room.join()
    expect(calls).toEqual(['a', 'b', 'a', 'b', 'a', 'b'])
  })
  it('counts only visible members as hidden ones leave and when the room closes', async () => {
    const room = await Room.create('count-beside-hidden')
    const bot = await room.join({ hidden: true })
    await room.join({ hidden: true })
    await room.join()
    expect(room.count).toBe(1)
    await bot.leave()
    expect(room.count).toBe(1)
    await Room.close(room.id)
    await vi.waitFor(() => expect(room.isClosed).toBe(true))
    expect(room.count).toBe(0)
  })
  it("resolves the last member's leave() and kick when onEmpty closes the room under them", async () => {
    const room = await Room.create('closed-on-empty')
    room.onEmpty(() => Room.close(room.id))
    await expect((await room.join({ identity: 'leaver' })).leave()).resolves.toBeUndefined()
    const reopened = await Room.create('closed-on-empty')
    reopened.onEmpty(() => Room.close(reopened.id))
    await reopened.join({ identity: 'kicked' })
    await expect(Room.removeParticipant(reopened.id, { identity: 'kicked' })).resolves.toBeUndefined()
    expect(await Room.list({ prefix: 'closed-on-empty' })).toEqual([])
  })
  it('removes nobody by { identity } from a room that is missing or closed, and still throws for an exact { id }', async () => {
    await expect(
      Room.removeParticipant('never-created', { identity: 'alice', reason: 'revoked' }),
    ).resolves.toBeUndefined()
    await Room.create('closed-revoke')
    await Room.close('closed-revoke')
    await expect(Room.removeParticipant('closed-revoke', { identity: 'alice' })).resolves.toBeUndefined()
    await expect(Room.removeParticipant('never-created', { id: 'member' })).rejects.toThrow('Room not found')
    await expect(Room.removeParticipant('never-created', { identity: '\udc00' })).rejects.toThrow('well-formed')
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
      driver.compareExchangeCells(room.id, room._inc, {
        expect: [{ key: memberKey, version: read.versions.get(memberKey)! }],
        mutations: [
          {
            key: memberKey,
            bytes: encoder.encode(stringify({ ...record, seenAt: Date.now() - ROOM_MEMBER_TTL_MS - 1 })),
          },
        ],
      }),
    ).resolves.toEqual({ committed: new Map() })
    const commitLane = vi.spyOn(driver, 'commitLane')
    await expect(Room.send(room.id, { id: member.id }, 'post-crash')).rejects.toThrow('Participant not found')
    expect(commitLane.mock.calls.some(([, , lane]) => lane.kind === 'inbox')).toBe(false)
    const loaded = await Room.get(room.id)
    expect(loaded.count).toBe(0)
    expect(loaded.isEmpty).toBe(true)
    expect((await Room.list()).find(({ id }) => id === room.id)).toMatchObject({ count: 0, isEmpty: true })
  })
  it("relays a member's message without its meta to a client that names the member, so its size is the same at any meta size", async () => {
    const room = (await Room.create('stamp-named')) as ServerRoom
    const large = Object.fromEntries(Array.from({ length: 1000 }, (_, i) => [`k${i}`, 'x'.repeat(90)]))
    const sender = await room.join({ meta: large })
    const other = await room.join({ meta: { n: 1 } })
    const stub = register(room)
    const peer = attachPeer(stub, undefined, { text: true, binary: false })
    await vi.waitFor(() => expect(rosterEnded(peer)).toBe(true))
    const { client, emit } = fakeClient(room.id)
    const named: unknown[] = []
    client.subscribe((_data, _info, from) => named.push([from.id, from.meta]))
    await sender.publish('hi')
    await other.publish('hi')
    await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toHaveLength(2))
    const frames = peer.decoded().filter((frame) => frame.tag === TAG.PUBLISH)
    for (const frame of frames) emit(parse(frame.text), frame.info.seq)
    const data = frames.filter((frame) => (parse(frame.text) as { __r: string }).__r === 'data')
    expect(data.map((frame) => frame.text.length)).toEqual([data[1]!.text.length, data[1]!.text.length])
    expect(named).toEqual([
      [sender.id, large],
      [other.id, { n: 1 }],
    ])
  })
  it('names a member to a client whose roster has not arrived as the member was when it sent', async () => {
    const authority = await Room.create('stamp-roster-pending')
    const sender = await authority.join({ meta: { v: 1 } })
    const observer = (await Room.get(authority.id)) as ServerRoom
    const roster = delayRosterRead(authority.id)
    try {
      const stub = register(observer)
      const peer = attachPeer(stub, undefined, { text: true, binary: false })
      await roster.started
      await sender.publish('first')
      await sender.setMeta({ v: 2 })
      await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toEqual(['first']))
      roster.release()
      await vi.waitFor(() => expect(rosterEnded(peer)).toBe(true))
      await sender.publish('second')
      await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toEqual(['first', 'second']))
      const { client, emit } = fakeClient(authority.id)
      const named: unknown[] = []
      client.subscribe((data, _info, from) => named.push([data, from.meta]))
      for (const frame of peer.decoded()) if (frame.tag === TAG.PUBLISH) emit(parse(frame.text), frame.info.seq)
      expect(named).toEqual([
        ['first', { v: 1 }],
        ['second', { v: 2 }],
      ])
    } finally {
      roster.release()
    }
  })
  it("applies a stamp newer than a view's meta of its sender before the message, so every client names the sender as it sent", async () => {
    const room = (await Room.create('stamp-ahead')) as ServerRoom
    const sender = await room.join({ meta: { v: 1 } })
    const stub = register(room)
    const peer = attachPeer(stub, undefined, { text: true, binary: false })
    await vi.waitFor(() => expect(rosterEnded(peer)).toBe(true))
    const updates: unknown[] = []
    room.onParticipantUpdate((member, meta) => updates.push([member.id, meta]))
    // A send whose meta write this view hasn't got yet, as from an instance ahead of it on the control lane.
    const envelope = { __r: 'data', from: sender.id, data: 'hi', fromMeta: { v: 2 }, fromMetaSeq: 1 }
    room._onTextData(stringify(envelope), { seq: 1, timestamp: 1 })
    expect(updates).toEqual([[sender.id, { v: 2 }]])
    const { client, emit } = fakeClient(room.id)
    const named: unknown[] = []
    client.subscribe((_data, _info, from) => named.push(from.meta))
    for (const frame of peer.decoded()) if (frame.tag === TAG.PUBLISH) emit(parse(frame.text), frame.info.seq)
    expect(named).toEqual([{ v: 2 }])
    expect(relayed(peer).find((event) => event.__r === 'data')).not.toHaveProperty('fromMeta')
  })
  it('keeps local and verified sender metadata at the newest accepted sequence', async () => {
    const room = (await Room.create('participant-meta-order')) as ServerRoom
    const participant = await room.join()
    const observer = await Room.get(room.id)
    const senderMeta: unknown[] = []
    observer.subscribe((_data, _info, from) => senderMeta.push(from.meta))
    const commitLane = driver.commitLane.bind(driver)
    const firstCommit = createDeferred()
    const releaseFirst = createDeferred()
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
      compareExchange(room.id, room._inc, {
        expect: [{ key: memberKey, version: initial.versions.get(memberKey)! }],
        mutations: [
          {
            key: memberKey,
            bytes: encoder.encode(stringify({ ...initialRecord, seenAt: Date.now() - ROOM_MEMBER_TTL_MS - 1 })),
          },
        ],
      }),
    ).resolves.toEqual({ committed: new Map() })
    let raced = false
    vi.spyOn(driver, 'compareExchangeCells').mockImplementation(async (roomId, inc, cx) => {
      if (!raced && cx.mutations.some(({ key, bytes }) => key === memberKey && bytes === null)) {
        raced = true
        const fresh = await driver.readCells(roomId, inc, { keys: [memberKey] })
        expect('staleInc' in fresh).toBe(false)
        if ('staleInc' in fresh) throw new Error('unexpected stale generation')
        const raw = fresh.cells.get(memberKey)
        expect(raw).toBeDefined()
        const record = parse(decoder.decode(raw!)) as Record<string, unknown>
        await expect(
          compareExchange(roomId, inc, {
            expect: [{ key: memberKey, version: fresh.versions.get(memberKey)! }],
            mutations: [{ key: memberKey, bytes: encoder.encode(stringify({ ...record, seenAt: Date.now() })) }],
          }),
        ).resolves.toEqual({ committed: new Map() })
      }
      return compareExchange(roomId, inc, cx)
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
    declare(stub, { __r: 'sub-binary', ...EVERY_TRACK })
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
    const acks = { A: createDeferred<unknown>(), B: createDeferred<unknown>() }
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
    const finalizing = createDeferred()
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
    const demandReady = createDeferred()
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
    const held = createDeferred()
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
  it('applies two hundred concurrent Room.setAttributes() calls on one room, each in about one write, and every view ends with all of them', async () => {
    const room = await Room.create('meta-burst', { meta: { topic: 'start' } })
    const observer = await Room.get(room.id)
    observer.onUpdate(() => {})
    const writes = yieldAfterEachReadAndCountWrites()
    const calls = 200
    await Promise.all(Array.from({ length: calls }, (_, i) => Room.setAttributes(room.id, { [`k${i}`]: i })))
    expect(writes()).toBeLessThanOrEqual(2 * calls)
    const expected = { topic: 'start', ...Object.fromEntries(Array.from({ length: calls }, (_, i) => [`k${i}`, i])) }
    expect((await Room.get(room.id)).meta).toEqual(expected)
    await vi.waitFor(() => expect(observer.meta).toEqual(expected))
  })
  it('replaces the meta wholesale under concurrent Room.setMeta() calls, each in about one write, every view ending on the one stored', async () => {
    const room = await Room.create('meta-replace-burst', { meta: { topic: 'start' } })
    const observer = await Room.get(room.id)
    observer.onUpdate(() => {})
    const writes = yieldAfterEachReadAndCountWrites()
    const metas = Array.from({ length: 50 }, (_, i) => ({ [`only${i}`]: i }))
    await Promise.all(metas.map((meta) => Room.setMeta(room.id, meta)))
    expect(writes()).toBeLessThanOrEqual(2 * metas.length)
    const stored = (await Room.get(room.id)).meta
    expect(metas).toContainEqual(stored)
    await vi.waitFor(() => expect(observer.meta).toEqual(stored))
  })
  it('reads a room meta that a Room.setMeta() lands in the middle of as the meta before it or after it, never a mix', async () => {
    const room = await Room.create('meta-paged-replace')
    const before = Object.fromEntries(Array.from({ length: 1_100 }, (_, i) => [`k${i}`, i]))
    await Room.setMeta(room.id, before)
    const readCellPages = driver.readCellPages.bind(driver)
    let landed = false
    vi.spyOn(driver, 'readCellPages').mockImplementation(async function* (roomId, inc, prefix) {
      for await (const page of readCellPages(roomId, inc, prefix)) {
        yield page
        if (!landed && prefix.startsWith('meta:')) {
          landed = true
          await Room.setMeta(room.id, { after: true })
        }
      }
    })
    const read = (await Room.get(room.id)).meta
    expect(landed).toBe(true)
    expect([before, { after: true }]).toContainEqual(read)
  })
  it('reads a room meta while a Room.setAttributes() lands in every read of its pages, as it stands after the last', async () => {
    const room = await Room.create('meta-paged-merges')
    const before = Object.fromEntries(Array.from({ length: 1_100 }, (_, i) => [`k${i}`, i]))
    await Room.setMeta(room.id, before)
    const readCellPages = driver.readCellPages.bind(driver)
    let merges = 0
    let merging = false
    vi.spyOn(driver, 'readCellPages').mockImplementation(async function* (roomId, inc, prefix) {
      let first = true
      for await (const page of readCellPages(roomId, inc, prefix)) {
        yield page
        if (!first || merging || !prefix.startsWith('meta:')) continue
        first = false
        merging = true
        await Room.setAttributes(room.id, { k0: 'merged', merges: ++merges })
        merging = false
      }
    })
    const read = (await Room.get(room.id)).meta
    expect(merges).toBeGreaterThanOrEqual(2)
    expect(read).toEqual({ ...before, k0: 'merged', merges })
  })
  it('lists room meta keys in the order its writes gave them: Room.setMeta() its own, Room.setAttributes() after the keys already there', async () => {
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
  it('lists the keys concurrent Room.setAttributes() calls add in one order, whichever call commits first', async () => {
    yieldAfterEachReadAndCountWrites()
    const orders: string[][] = []
    for (const [id, calls] of [
      ['meta-ties-zy', [{ z: 1 }, { y: 1 }]],
      ['meta-ties-yz', [{ y: 1 }, { z: 1 }]],
    ] as const) {
      await Room.create(id, { meta: { topic: 'start' } })
      await Room.setAttributes(id, { first: 1 })
      await Promise.all(calls.map((attrs) => Room.setAttributes(id, attrs)))
      orders.push(Object.keys((await Room.get(id)).meta))
    }
    expect(orders).toEqual([
      ['topic', 'first', 'y', 'z'],
      ['topic', 'first', 'y', 'z'],
    ])
  })
  it('grows a room meta to 20,000 keys one Room.setAttributes() at a time, each write taking about as long at 20,000 keys as at 1,000', async () => {
    const room = await Room.create('meta-growth')
    const observer = await Room.get(room.id)
    observer.onJoin(() => {}) // a live view: it applies every write, and builds its meta only when read
    const windows: number[] = []
    let started = 0
    for (let i = 1; i <= 20_000; i++) {
      if (i % 1_000 === 501) started = performance.now()
      await Room.setAttributes(room.id, { [`k${i}`]: i })
      if (i === 1_000 || i === 10_000 || i === 20_000) windows.push(performance.now() - started)
    }
    await vi.waitFor(() => expect(Object.keys(observer.meta)).toHaveLength(20_000))
    // The last 500 writes before each size.
    const [at1k, at10k, at20k] = windows as [number, number, number]
    expect(Math.max(at10k, at20k)).toBeLessThanOrEqual(3 * at1k)
  }, 60_000)
  it('ends every view, a client too, on the stored meta, its key order and its seq under concurrent Room.setAttributes() and Room.setMeta() calls', async () => {
    const room = (await Room.create('meta-converge', { meta: { topic: 'start' } })) as unknown as ServerRoom
    const other = (await Room.get(room.id)) as ServerRoom
    for (const view of [room, other]) view.onUpdate(() => {})
    const { peer } = serve(room)
    await new Promise((resolve) => setTimeout(resolve, 0))
    yieldAfterEachReadAndCountWrites()
    await Promise.all(
      Array.from({ length: 120 }, (_, i) =>
        i % 12 === 11
          ? Room.setMeta(room.id, { [`r${i}`]: i, topic: i })
          : Room.setAttributes(room.id, { [`k${i % 7}`]: i, [`n${i}`]: i, [`n${i - 3}`]: undefined }),
      ),
    )
    const stored = (await Room.get(room.id)) as ServerRoom
    const viewOf = (view: ServerRoom | ClientRoom) => ({
      keys: Object.keys(view.meta),
      meta: view.meta,
      seq: (view as unknown as { _state: RoomState })._state.metaSeq,
    })
    const expected = viewOf(stored)
    expect(expected.seq).toBeGreaterThan(100)
    await vi.waitFor(() => expect([room, other].map(viewOf)).toEqual([expected, expected]))
    expect(viewOf(clientView(peer, room.id))).toEqual(expected)
  })
  it("grows a member's meta to 20,000 keys one setAttributes() at a time, each write taking about as long at 20,000 keys as at 1,000", async () => {
    const room = await Room.create('member-meta-growth')
    const member = await room.join()
    const observer = await Room.get(room.id)
    observer.onJoin(() => {}) // a live view: it applies every write, and builds the meta only when read
    const windows: number[] = []
    let started = 0
    for (let i = 1; i <= 20_000; i++) {
      if (i % 1_000 === 501) started = performance.now()
      await member.setAttributes({ [`k${i}`]: i })
      if (i === 1_000 || i === 10_000 || i === 20_000) windows.push(performance.now() - started)
    }
    await vi.waitFor(async () =>
      expect(Object.keys((await observer.getParticipant(member.id))!.meta)).toHaveLength(20_000),
    )
    // The last 500 writes before each size.
    const [at1k, at10k, at20k] = windows as [number, number, number]
    expect(Math.max(at10k, at20k)).toBeLessThanOrEqual(3 * at1k)
  }, 60_000)
  it("ends every view, a client too, on a member's stored meta, its key order and its seq under concurrent setAttributes() and setMeta() calls", async () => {
    const room = (await Room.create('member-meta-converge')) as ServerRoom
    const member = await room.join({ meta: { topic: 'start' } })
    const other = (await Room.get(room.id)) as ServerRoom
    for (const view of [room, other]) view.onParticipantUpdate(() => {})
    await other.getParticipants()
    const { peer } = serve(room)
    await new Promise((resolve) => setTimeout(resolve, 0))
    yieldAfterEachReadAndCountWrites()
    await Promise.all(
      Array.from({ length: 120 }, (_, i) =>
        i % 12 === 11
          ? member.setMeta({ [`r${i}`]: i, topic: i })
          : member.setAttributes({ [`k${i % 7}`]: i, [`n${i}`]: i, [`n${i - 3}`]: undefined }),
      ),
    )
    const viewOf = (view: ServerRoom | ClientRoom) => {
      const accepted = (view as unknown as { _state: RoomState })._state.acceptedMeta(member.id)!
      return { keys: Object.keys(accepted.meta), meta: accepted.meta, seq: accepted.seq }
    }
    const stored = (await Room.get(room.id)) as ServerRoom
    await stored.getParticipants()
    const expected = viewOf(stored)
    expect(expected.seq).toBe(120)
    await vi.waitFor(() => expect([room, other].map(viewOf)).toEqual([expected, expected]))
    expect(viewOf(clientView(peer, room.id))).toEqual(expected)
    expect({ keys: Object.keys(member.meta), meta: member.meta }).toEqual({ keys: expected.keys, meta: expected.meta })
  })
  it('repairs a member meta update a view never got from the next one it gets, well within a heartbeat', async () => {
    const control = loseLaneFrames((lane) => lane.kind === 'control')
    const room = await Room.create('member-meta-gap')
    const member = await room.join()
    const observer = await Room.get(room.id)
    observer.onParticipantUpdate(() => {})
    await vi.waitFor(async () => expect(await observer.getParticipant(member.id)).not.toBeNull())
    control.next()
    await member.setAttributes({ a: 1 })
    await member.setAttributes({ b: 2 })
    await vi.waitFor(async () => expect((await observer.getParticipant(member.id))!.meta).toEqual({ a: 1, b: 2 }), {
      timeout: 5_000,
    })
  })
  it('repairs a room meta update a view never got from the next one it gets, well within a heartbeat', async () => {
    const control = loseLaneFrames((lane) => lane.kind === 'control')
    const room = await Room.create('meta-gap')
    const observer = await Room.get(room.id)
    observer.onUpdate(() => {})
    await new Promise((resolve) => setTimeout(resolve, 0))
    control.next()
    await Room.setAttributes(room.id, { a: 1 })
    await Room.setAttributes(room.id, { b: 2 })
    await vi.waitFor(() => expect(observer.meta).toEqual({ a: 1, b: 2 }), { timeout: 5_000 })
  })
  it('repairs a room meta write whose writer crashed between its exchange and its publish', async () => {
    const room = await Room.create('meta-crash')
    const observer = await Room.get(room.id)
    observer.onUpdate(() => {})
    await new Promise((resolve) => setTimeout(resolve, 0))
    await Room.setAttributes(room.id, { a: 1 })
    await vi.waitFor(() => expect(observer.meta).toEqual({ a: 1 }))
    vi.spyOn(driver, 'commitLane').mockImplementationOnce(() => Promise.reject(new Error('crashed')))
    await expect(Room.setAttributes(room.id, { b: 2 })).rejects.toThrow('crashed')
    await Room.setAttributes(room.id, { c: 3 })
    await vi.waitFor(() => expect(observer.meta).toEqual({ a: 1, b: 2, c: 3 }), { timeout: 5_000 })
  })
  it('drops the keys of a meta generation whose replace crashed before dropping them, at the next heartbeat or the next replace', async () => {
    const room = (await Room.create('meta-leftover')) as ServerRoom
    const observer = (await Room.get(room.id)) as ServerRoom
    observer.onUpdate(() => {})
    const generations = async () => {
      const read = await driver.readCells(room.id, room._inc, { prefix: 'meta:k:' })
      if ('staleInc' in read) throw new Error('the room is gone')
      return new Set([...read.cells.keys()].map((key) => key.split(':')[2]))
    }
    const exchange = driver.compareExchangeCells.bind(driver)
    const crashDrops = () =>
      vi.spyOn(driver, 'compareExchangeCells').mockImplementation(async (roomId, inc, cx) => {
        const drop = cx.mutations.some((m) => m.bytes === null && m.key.startsWith('meta:k:'))
        if (drop) throw new Error('crashed')
        return exchange(roomId, inc, cx)
      })
    await Room.setMeta(room.id, { a: 1 })
    let crashing = crashDrops()
    await expect(Room.setMeta(room.id, { b: 1 })).rejects.toThrow('crashed')
    crashing.mockRestore()
    expect((await generations()).size).toBe(2)
    await subsOf(observer).reconcileAuthority() // the heartbeat's
    expect((await generations()).size).toBe(1)
    crashing = crashDrops()
    await expect(Room.setMeta(room.id, { c: 1 })).rejects.toThrow('crashed')
    crashing.mockRestore()
    expect((await generations()).size).toBe(2)
    await Room.setMeta(room.id, { d: 1 })
    expect((await generations()).size).toBe(1)
    expect((await Room.get(room.id)).meta).toEqual({ d: 1 })
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
    expect([Object.isFrozen(room.meta), Object.isFrozen(me.meta)]).toEqual([true, true])
    const replaced = { pos: { x: 0 }, tag: { v: 'a' } }
    const replacing = me.setMeta(replaced)
    replaced.tag.v = 'changed'
    await replacing
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
    replaced.tag.v = 'later'
    attrs.pos.x = 8
    roomMeta.topic.name = 'later'
    const fresh = await Room.get(room.id)
    expect({
      view: [room.meta, me.meta],
      stored: [fresh.meta, (await fresh.getParticipants()).map(({ meta }) => meta)],
    }).toEqual({
      view: [{ topic: { name: 'b' } }, { pos: { x: 1 }, tag: { v: 'a' } }],
      stored: [{ topic: { name: 'b' } }, [{ pos: { x: 1 }, tag: { v: 'a' } }]],
    })
  })
  it.each([false, true])(
    'lets a 0 ms timer fire while a listener answers every message it gets (after an await: %s)',
    async (afterAwait) => {
      const room = await Room.create(`echo-${afterAwait}`)
      const member = await room.join()
      let timerFired = false
      setTimeout(() => (timerFired = true), 0)
      const stopped = Promise.withResolvers<number>()
      // Bounded, so the spec ends where the answers starve the timer.
      const answer = (depth: number) => {
        if (timerFired || depth === 20_000) return stopped.resolve(depth)
        member.publish(depth + 1).catch(stopped.reject)
      }
      room.subscribe(
        afterAwait
          ? async (data) => {
              await null
              answer(data as number)
            }
          : (data) => answer(data as number),
      )
      await member.publish(0)
      expect(await stopped.promise).toBeLessThan(20_000)
    },
  )
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
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const sending = sender.send(recipient, 'unhandled', { ack: true }).catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(ROOM_DM_ACK_TIMEOUT_MS)
    const publicError = await sending
    expect(publicError).toBeInstanceOf(RoomError)
    expect(isRoomError(publicError)).toBe(true)
    expect(publicError).toMatchObject({
      name: 'RoomError',
      message: expect.stringContaining('timed out'),
    })
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
      _deliverMessage(message: InboxMessage): void
      _deliverMessageAck(message: InboxMessage): Promise<unknown>
      _setForwarder(forwarder: {
        deliver(message: InboxMessage): void
        deliverAck(message: InboxMessage): Promise<unknown>
      }): void
    }
    const plainArrived = createDeferred()
    const ackArrived = createDeferred()
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
    internal._setForwarder({
      deliver: (message) => void forwarded.push(message.data),
      deliverAck: (message) => {
        forwarded.push(message.data)
        return Promise.resolve({ ok: true, result: `handled:${String(message.data)}` })
      },
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
  it('treats a request the client library never sends, unparsable or misshapen, as a protocol violation', async () => {
    const stub = register((await Room.create('malformed-stub-request')) as ServerRoom)
    const holder = (await Room.join('malformed-stub-request')) as ServerLocalParticipant
    const participant = new RoomParticipantStubChannel(holder)
    const member = crypto.randomUUID()
    const text = (value: unknown) => stringify(value)
    const frames = [
      [
        stub,
        {
          tag: TAG.TEXT,
          index: 7,
          seq: 1,
          text: text({ __r: 'sub-binary', member: null, track: 5, on: true }),
          bytes: 1,
        },
      ],
      [
        stub,
        {
          tag: TAG.TEXT,
          index: 7,
          seq: 2,
          text: text({ __r: 'sub-text', members: { 'not-a-member-id': true }, announce: false }),
          bytes: 1,
        },
      ],
      [
        stub,
        {
          tag: TAG.TEXT,
          index: 7,
          seq: 3,
          text: text({ __r: 'dm-reply', ackId: 'a', reply: {} }),
          bytes: 1,
        },
      ],
      [
        stub,
        { tag: TAG.TEXT, index: 7, seq: 4, text: text({ __r: 'req-join', meta: {}, selfDelivery: true }), bytes: 1 },
      ],
      [
        stub,
        {
          tag: TAG.TEXT_ACK_REQ,
          index: 7,
          seq: 5,
          text: text({ __r: 'req-join', meta: [], selfDelivery: true }),
          bytes: 1,
        },
      ],
      [
        stub,
        {
          tag: TAG.TEXT_ACK_REQ,
          index: 7,
          seq: 6,
          text: text({ __r: 'req-set-meta', id: 'nope', meta: {} }),
          bytes: 1,
        },
      ],
      [
        stub,
        {
          tag: TAG.TEXT_ACK_REQ,
          index: 7,
          seq: 7,
          text: text({ __r: 'sub-text', members: {}, announce: false }),
          bytes: 1,
        },
      ],
      [
        stub,
        { tag: TAG.PUBLISH_ACK_REQ, index: 7, seq: 8, text: text({ __r: 'data', from: member, retain: 1 }), bytes: 1 },
      ],
      [stub, { tag: TAG.PUBLISH_BINARY_ACK_REQ, index: 7, seq: 9, data: new Uint8Array([1, 2]), bytes: 2 }],
      [
        participant,
        { tag: TAG.TEXT_ACK_REQ, index: 7, seq: 1, text: text({ __r: 'req-set-attrs', attrs: 'x' }), bytes: 1 },
      ],
      [
        participant,
        { tag: TAG.BINARY_ACK_REQ, index: 7, seq: 2, data: encodeBinaryFrame(member, new Uint8Array([1])), bytes: 1 },
      ],
      [stub, { tag: TAG.TEXT, index: 7, seq: 10, text: '{', bytes: 1 }],
      [stub, { tag: TAG.TEXT_ACK_REQ, index: 7, seq: 11, text: '{', bytes: 1 }],
      [participant, { tag: TAG.TEXT_ACK_REQ, index: 7, seq: 3, text: '{', bytes: 1 }],
    ] as const
    for (const [channel, frame] of frames) expect(() => channel._dispatchFrame(frame)).toThrow(ProtocolViolationError)
  })
  it("hands a client join's guard a frozen meta, as a server join does", async () => {
    const room = (await Room.create('stub-join-guard')) as ServerRoom
    const frozen: boolean[] = []
    Room.guard(room, { onBeforeJoin: ({ meta }) => void frozen.push(Object.isFrozen(meta)) })
    const { stub } = serve(room)
    await joinThrough(stub, { name: 'a' })
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
      [() => me.publish('kept', { retain: 1 } as never), 'publish() options.retain should be a boolean'],
      [() => me.publish('kept', { coalesce: 1 } as never), 'publish() options.coalesce should be a string'],
      [() => me.send(me.id, 'hi', { ack: 'yes' } as never), 'send() options.ack should be a boolean'],
      [
        () => me.publishBinary(new Uint8Array([1]), { retain: 'yes' } as never),
        'publishBinary() options.retain should be a boolean',
      ],
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
    const id = await joinThrough(stub)
    const victim = await room.join()
    const victimInbox: unknown[] = []
    victim.listen((data) => victimInbox.push(data))
    const sender = await room.join()
    const acking = sender.send(id, 'ping', { ack: true })
    let ackId = ''
    await vi.waitFor(() => {
      const dm = relayed(peer).find((event) => event.__r === 'dm')
      expect(dm?.ackId).toBeTypeOf('string')
      ackId = dm!.ackId!
    })
    const reply = { ok: true, result: 'handled', __r: 'dm', to: victim.id, from: '', data: 'forged' }
    declare(stub, { __r: 'dm-reply', ackId, reply })
    await expect(acking).resolves.toMatchObject({ response: 'handled' })
    expect(victimInbox).toEqual([])
  })
  it("forgets a stub's ack DM once its sender timed out, at the next ack DM, so a reply after that publishes nothing", async () => {
    const room = (await Room.create('stub-ack-dm-expiry')) as ServerRoom
    const { stub } = serve(room)
    const publishDmAck = vi.spyOn(room, '_publishDmAck').mockResolvedValue()
    vi.useFakeTimers()
    const dm = (ackId: string) => ({
      __r: 'dm' as const,
      to: 'member',
      from: 'sender',
      fromMeta: null,
      data: 'ping',
      ackId,
    })
    const texts = () => new MemberMessageTexts('{}', { seq: 0, timestamp: 0 }, () => ({}))
    stub._relayDm(texts(), dm('a'))
    await vi.advanceTimersByTimeAsync(ROOM_DM_ACK_TIMEOUT_MS + 1)
    stub._relayDm(texts(), dm('b'))
    declare(stub, { __r: 'dm-reply', ackId: 'a', reply: { ok: true, result: 'late' } })
    declare(stub, { __r: 'dm-reply', ackId: 'b', reply: { ok: true, result: 'in time' } })
    expect(publishDmAck.mock.calls.map(([, ackId]) => ackId)).toEqual(['b'])
  })
  it('reports a client-held participant whose channel closed as disconnected', async () => {
    const room = (await Room.create('standalone-disconnect')) as ServerRoom
    const holder = (await room.join()) as ServerLocalParticipant
    const stub = register(room)
    const id = await joinThrough(stub)
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
    const evicting = createDeferred()
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
  it("fails a member's pending ack send when that member leaves, not at the ack timeout", async () => {
    const room = await Room.create('sender-leaves-ack')
    const sender = await room.join()
    const recipient = await room.join() // never listens, so never answers
    let outcome: unknown = 'pending'
    void sender.send(recipient.id, 'ping', { ack: true }).then(
      () => (outcome = 'answered'),
      (error: unknown) => (outcome = isRoomError(error) ? error.message : error),
    )
    await new Promise((resolve) => setTimeout(resolve, 10))
    await sender.leave()
    await vi.waitFor(() => expect(outcome).toBe('Participant left the room'))
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
    channel._dispatchFrame({ tag: TAG.BINARY_ACK_REQ, index: 7, seq: 1, data, bytes: data.byteLength })
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
    channel._dispatchFrame({ tag: TAG.BINARY_ACK_REQ, index: 7, seq: 1, data, bytes: data.byteLength })
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
  it('lets a join whose reply was lost and whose eviction failed lapse from its join, though a heartbeat landed meanwhile', async () => {
    vi.useFakeTimers()
    const room = (await Room.create('join-reply-lost-eviction-failed')) as ServerRoom
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const compareExchange = driver.compareExchangeCells.bind(driver)
    vi.spyOn(driver, 'compareExchangeCells')
      .mockImplementationOnce(async (...args) => {
        await compareExchange(...args)
        throw new Error('Connection is closed.')
      })
      .mockImplementationOnce(async () => {
        vi.setSystemTime(Date.now() + ROOM_HEARTBEAT_INTERVAL_MS)
        await subsOf(room)._heartbeatTick()
        throw new Error('backend unavailable')
      })
    await expect(room.join()).rejects.toThrow('Connection is closed.')
    await vi.advanceTimersByTimeAsync(ROOM_MEMBER_TTL_MS - ROOM_HEARTBEAT_INTERVAL_MS + 1)
    expect(await Room.getParticipants(room.id)).toEqual([])
  })
  it('rejects a join whose member was removed before its join event committed, leaving no member behind', async () => {
    const room = (await Room.create('join-kicked')) as ServerRoom
    const observer = (await Room.get(room.id)) as ServerRoom
    await observer.getParticipants()
    const commitLane = driver.commitLane.bind(driver)
    const committing = createDeferred()
    const kicked = createDeferred()
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
    const entered = createDeferred()
    const release = createDeferred()
    Room.guard(room, {
      onBeforeJoin: async () => {
        entered.resolve()
        await release.promise
      },
    })
    const joining = joinThrough(stub).catch((error: unknown) => error)
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
    const writing = createDeferred()
    const release = createDeferred()
    vi.spyOn(driver, 'compareExchangeCells').mockImplementationOnce(async (...args) => {
      writing.resolve()
      await release.promise
      return compareExchange(...args)
    })
    const departed = vi.spyOn(room, '_removeDepartedMember')
    const joining = joinThrough(stub).catch((error: unknown) => error)
    await writing.promise
    stub.abort()
    release.resolve()
    expect(isRoomError(await joining)).toBe(true)
    expect(await Room.getParticipants(room.id)).toEqual([])
    expect(departed).not.toHaveBeenCalled() // the admission rolls itself back
  })
  it('reports a member whose client stub closed after its member write committed as disconnected, once', async () => {
    const room = (await Room.create('stub-close-after-member-write')) as ServerRoom
    const observer = await Room.get(room.id)
    const stub = register(room)
    const compareExchange = driver.compareExchangeCells.bind(driver)
    const written = createDeferred()
    const release = createDeferred()
    vi.spyOn(driver, 'compareExchangeCells').mockImplementation(async (...args) => {
      const result = await compareExchange(...args)
      if (args[2].mutations.some(({ key }) => key.startsWith(MEMBER_CELL_PREFIX))) {
        written.resolve()
        await release.promise
      }
      return result
    })
    const joining = joinThrough(stub).catch((error: unknown) => error)
    await written.promise
    const causes: unknown[] = []
    observer.onLeave((_member, cause) => causes.push(cause?.type))
    expect(await observer.getParticipants()).toHaveLength(1)
    stub.abort()
    release.resolve()
    expect(isRoomError(await joining)).toBe(true)
    await vi.waitFor(() => expect(causes).toEqual(['disconnected']))
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(causes).toEqual(['disconnected'])
  })
  it('keeps no stub index entry for a member that left, or whose stub closed', async () => {
    const room = (await Room.create('stub-index-cleanup')) as ServerRoom
    const stub = register(room)
    for (let i = 0; i < 3; i++) await stub._handleRequest({ __r: 'req-leave', id: await joinThrough(stub) })
    expect(stubIndexOf(room).size).toBe(0)
    await joinThrough(stub)
    stub.abort()
    expect(stubIndexOf(room).size).toBe(0) // before the departed member's eviction settles
  })
  it("keeps no order for a sender that left in a stub that didn't hold it", async () => {
    const room = (await Room.create('stub-replay-forget')) as ServerRoom
    const stub = register(room)
    const peer = attachPeer(stub, undefined, { text: true, binary: false })
    await vi.waitFor(() => expect(rosterEnded(peer)).toBe(true))
    const senders = await Promise.all(Array.from({ length: 3 }, () => room.join()))
    for (const sender of senders) await sender.publish('hi')
    await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toHaveLength(3))
    for (const sender of senders) await sender.leave()
    const replay = (stub as unknown as { _replay: { _high: Map<string, number> } })._replay
    expect([...replay._high.keys()]).toEqual([])
  })
  it('never indexes a member under a stub that closed before its join held it', async () => {
    const room = (await Room.create('stub-index-closed-before-hold')) as ServerRoom
    const stub = register(room)
    const entered = createDeferred()
    const release = createDeferred()
    Room.guard(room, {
      onBeforeJoin: async () => {
        entered.resolve()
        await release.promise
      },
    })
    const sizes: number[] = []
    const abandon = (room as unknown as { _abandonAdmission(id: string): void })._abandonAdmission.bind(room)
    vi.spyOn(room as unknown as { _abandonAdmission(id: string): void }, '_abandonAdmission').mockImplementation(
      (id) => {
        sizes.push(stubIndexOf(room).size)
        abandon(id)
      },
    )
    const joining = joinThrough(stub).catch((error: unknown) => error)
    await entered.promise
    stub.abort()
    release.resolve()
    expect(isRoomError(await joining)).toBe(true)
    expect(sizes).toEqual([0])
  })
  it("keeps a member's binary lane while an attached stub wants it, through toggles, attaches and closes", async () => {
    const room = (await Room.create('incremental-binary-wants')) as ServerRoom
    const me = await room.join()
    const demand: Array<[string | null, boolean]> = []
    me.onDemand((track, wanted) => demand.push([track, wanted]))
    const a = register(room)
    const b = register(room)
    declare(a, { __r: 'sub-binary', ...EVERY_TRACK })
    await vi.waitFor(() => expect(demand).toEqual([[null, true]]))
    declare(a, { __r: 'sub-binary', ...NO_TRACK })
    await subsOf(room).binaryReady()
    expect(demand).toEqual([
      [null, true],
      [null, false],
    ])
    declare(a, { __r: 'sub-binary', ...EVERY_TRACK })
    declare(b, { __r: 'sub-binary', member: me.id, track: DEFAULT_TRACK, on: true })
    a.abort()
    await vi.waitFor(() =>
      expect(demand).toEqual([
        [null, true],
        [null, false],
        [null, true],
      ]),
    )
    b.abort()
    await subsOf(room).binaryReady()
    expect(demand.at(-1)).toEqual([null, false])
    declare(b, { __r: 'sub-binary', ...EVERY_TRACK }) // a closed stub's late declaration
    await subsOf(room).binaryReady()
    expect(binaryLanes(room)).toEqual([])
  })
  it("keeps a member's lanes while one of its listeners remains, and indexes only what each listener change changed", async () => {
    const room = (await Room.create('counted-member-listeners')) as ServerRoom
    const me = await room.join()
    const other = await room.join()
    ;(await room.getParticipant(other.id))!.subscribeBinary(() => {})
    const remote = (await room.getParticipant(me.id))!
    const update = vi.spyOn(WantsIndex.prototype, 'update')
    const updateText = vi.spyOn(WantsIndex.prototype, 'updateText')
    const applyBinary = vi.spyOn(WantsIndex.prototype, 'applyBinary')
    const text = [remote.subscribe(() => {}), remote.subscribe(() => {})]
    const screen = [
      remote.subscribeBinary(() => {}, { track: 'screen' }),
      remote.subscribeBinary(() => {}, { track: 'screen' }),
    ]
    expect(update).not.toHaveBeenCalled()
    expect(updateText.mock.calls.map(([, , members]) => [...members!])).toEqual([[me.id], [], [], []])
    expect(applyBinary.mock.calls.map(([, change]) => change)).toEqual([{ member: me.id, track: 'screen', on: true }])
    const lanes = () => ({
      text: subsOf(room)._semantic.wanted,
      binary: binaryLanes(room).filter((key) => key.includes(me.id)),
    })
    await subsOf(room).binaryReady()
    expect(lanes()).toEqual({ text: true, binary: [`binary:${me.id}:screen`] })
    text[0]!()
    screen[0]!()
    await subsOf(room).binaryReady()
    expect(lanes()).toEqual({ text: true, binary: [`binary:${me.id}:screen`] })
    text[1]!()
    screen[1]!()
    await subsOf(room).binaryReady()
    expect(lanes()).toEqual({ text: false, binary: [] })
  })
  it('waits for a binary lane that is still establishing before it reports binary readiness', async () => {
    const room = (await Room.create('binary-ready-establishing')) as ServerRoom
    await room.join()
    const binary = delayLaneSubscription((lane) => lane.kind === 'binary-all')
    room.subscribeBinary(() => {})
    await binary.started
    let ready = false
    const waiting = subsOf(room)
      .binaryReady()
      .then(() => {
        ready = true
      })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(ready).toBe(false)
    await binary.release()
    await waiting
  })
  it("drops a member's binary lane when it leaves", async () => {
    const room = (await Room.create('incremental-binary-leave')) as ServerRoom
    const me = await room.join()
    declare(register(room), { __r: 'sub-binary', ...EVERY_TRACK })
    await subsOf(room).binaryReady()
    expect(binaryLanes(room)).toEqual([`binary-all:${me.id}`])
    await me.leave()
    expect(binaryLanes(room)).toEqual([])
  })
  it("plans a selfDelivery: false member's lane from the holders that get its frames back", async () => {
    const room = (await Room.create('incremental-binary-suppressed')) as ServerRoom
    const me = await room.join({ selfDelivery: false })
    const demand: Array<[string | null, boolean]> = []
    me.onDemand((track, wanted) => demand.push([track, wanted]))
    room.subscribeBinary(() => {}) // this instance's own listener suppresses `me`
    const stub = register(room)
    declare(stub, { __r: 'sub-binary', ...EVERY_TRACK })
    await vi.waitFor(() => expect(demand).toEqual([[null, true]]))
    declare(stub, { __r: 'sub-binary', ...NO_TRACK })
    await subsOf(room).binaryReady()
    expect(demand).toEqual([
      [null, true],
      [null, false],
    ])
  })
  it('wants no lane for a co-returned participant serialized after its room stub attached', async () => {
    await Room.create('suppressed-after-attach')
    const me = (await Room.join('suppressed-after-attach', { selfDelivery: false })) as ServerLocalParticipant
    const demand: Array<[string | null, boolean]> = []
    me.onDemand((track, wanted) => demand.push([track, wanted]))
    const room = (await Room.get('suppressed-after-attach')) as ServerRoom
    await room.getParticipants()
    const channels: ServerChannel[] = []
    const context = replacerContext(channels)
    roomReplacer.replace(room, context)
    roomParticipantReplacer.replace(me, context)
    const stub = channels.find((channel) => channel instanceof RoomStubChannel) as RoomStubChannel
    declare(stub, { __r: 'sub-binary', ...EVERY_TRACK })
    await subsOf(room).binaryReady()
    expect(binaryLanes(room)).toEqual([])
    declare(register(room), { __r: 'sub-binary', ...EVERY_TRACK })
    await vi.waitFor(() => expect(demand).toEqual([[null, true]]))
  })
  it("drops the lane of a co-returned participant a streamed response serializes after its room's stub declared", async () => {
    await Room.create('suppressed-mid-stream')
    const me = (await Room.join('suppressed-mid-stream', { selfDelivery: false })) as ServerLocalParticipant
    const demand: Array<[string | null, boolean]> = []
    me.onDemand((track, wanted) => demand.push([track, wanted]))
    const room = (await Room.get('suppressed-mid-stream')) as ServerRoom
    await room.getParticipants()
    const channels: ServerChannel[] = []
    const context = replacerContext(channels)
    roomReplacer.replace(room, context)
    const stub = channels.find((channel) => channel instanceof RoomStubChannel) as RoomStubChannel
    declare(stub, { __r: 'sub-binary', ...EVERY_TRACK })
    await vi.waitFor(() => expect(demand).toEqual([[null, true]]))
    roomParticipantReplacer.replace(me, context)
    // The planning pass it starts drops the lane, not the member's next event or the next heartbeat.
    await subsOf(room).binaryReady()
    expect(binaryLanes(room)).toEqual([])
    await vi.waitFor(() =>
      expect(demand).toEqual([
        [null, true],
        [null, false],
      ]),
    )
  })
  it("closes a member's inbox when it leaves, when its stub closes, and opens none under a closed stub", async () => {
    const room = (await Room.create('incremental-inboxes')) as ServerRoom
    const stub = register(room)
    await stub._handleRequest({ __r: 'req-leave', id: await joinThrough(stub) })
    expect(subsOf(room)._inbox.size).toBe(0)
    await joinThrough(stub)
    stub.abort()
    expect(subsOf(room)._inbox.size).toBe(0) // before the departed member's eviction settles
    const closing = register(room)
    const entered = createDeferred()
    const release = createDeferred()
    Room.guard(room, {
      onBeforeJoin: async () => {
        entered.resolve()
        await release.promise
      },
    })
    const sizes: number[] = []
    const abandon = (room as unknown as { _abandonAdmission(id: string): void })._abandonAdmission.bind(room)
    vi.spyOn(room as unknown as { _abandonAdmission(id: string): void }, '_abandonAdmission').mockImplementation(
      (id) => {
        sizes.push(subsOf(room)._inbox.size)
        abandon(id)
      },
    )
    // Checked as the admission takes ownership, before a roster refresh's full replan could close a stray inbox.
    const subs = subsOf(room)
    const memberOwned = subs.memberOwned.bind(subs)
    const opened: boolean[] = []
    vi.spyOn(subs, 'memberOwned').mockImplementation((id) => {
      memberOwned(id)
      opened.push(subs._inbox.has(id))
    })
    const joining = joinThrough(closing).catch((error: unknown) => error)
    await entered.promise
    closing.abort()
    release.resolve()
    expect(isRoomError(await joining)).toBe(true)
    expect({ opened, sizes }).toEqual({ opened: [false], sizes: [0] })
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
    await vi.advanceTimersByTimeAsync(reconnectWindow() + 1)
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
    vi.spyOn(driver, 'compareExchangeCells').mockImplementation(async (roomId, inc, cx) => {
      if (cx.mutations.some((mutation) => mutation.key === memberKey && mutation.bytes !== null))
        throw new Error('crashed')
      return compareExchange(roomId, inc, cx)
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
  it('reports a background Room write that a held-send bound refused loudly, not as a bug', () => {
    const bugs: unknown[] = []
    onBug((err) => bugs.push(err))
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const refused = heldSendWeight('text', 'semantic', () => 0, 'Room').overflow()
    reportRoomError(refused)
    expect({ bugs, report: report.mock.calls }).toEqual({ bugs: [], report: [[refused]] })
  })
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
    const controlReady = createDeferred()
    mockLaneSubscription('control', (subscribeLane, roomId, inc, lane, receiver) => {
      const subscription = subscribeLane(roomId, inc, lane, receiver)
      void subscription.ready.then(() => controlReady.resolve())
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
    const semanticReady = createDeferred()
    mockLaneSubscription('semantic', (subscribeLane, roomId, inc, lane, receiver) => {
      const subscription = subscribeLane(roomId, inc, lane, receiver)
      void subscription.ready.then(() => semanticReady.resolve())
      return subscription
    })
    const wanted = serve(room)
    const silent = serve(room)
    declare(wanted.stub, { __r: 'sub-text', members: {}, announce: true })
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
    const current = await driver.readCells(room.id, room._inc, { keys: [memberCellKey(member.id)] })
    if ('staleInc' in current) throw new Error('room went stale')
    await driver.compareExchangeCells(room.id, room._inc, {
      expect: [{ key: memberCellKey(member.id), version: current.versions.get(memberCellKey(member.id))! }],
      mutations: [{ key: memberCellKey(member.id), bytes: null }],
    })
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
  it("replays a member's retained frames on each of its tracks to a want for that member, listing only that member's lanes", async () => {
    const authority = await Room.create('retained-member-want')
    const publisher = await authority.join()
    const other = await authority.join()
    await publisher.publishBinary(new Uint8Array([1]), { retain: true })
    await publisher.publishBinary(new Uint8Array([2]), { track: 'screen', retain: true })
    await other.publishBinary(new Uint8Array([3]), { retain: true })
    const observer = await Room.get(authority.id)
    const listRetained = vi.spyOn(driver, 'listRetained')
    const frames: number[][] = []
    ;(await observer.getParticipant(publisher.id))!.subscribeBinary((data) => void frames.push([...data]))
    await vi.waitFor(() => expect(frames.sort()).toEqual([[1], [2]]))
    expect(listRetained.mock.calls.map(([, , of]) => of)).toEqual([{ member: publisher.id }])
  })
  it("replays a track's retained frames once each to a room-wide want for that track, reading only those frames, among 100 members", async () => {
    const authority = (await Room.create('retained-track-want')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 100 }, () => authority.join()))
    const retaining = members.slice(0, 3)
    for (const [i, member] of retaining.entries())
      await member.publishBinary(new Uint8Array([i]), { track: 'screen', retain: true })
    await members[3]!.publishBinary(new Uint8Array([9]), { track: 'camera', retain: true })
    const observer = await Room.get(authority.id)
    const listRetained = vi.spyOn(driver, 'listRetained')
    const readRetained = vi.spyOn(driver, 'readRetained')
    const frames: number[] = []
    observer.subscribeBinary((data) => void frames.push(data[0]!), { track: 'screen' })
    await vi.waitFor(() => expect(frames.length).toBe(3))
    for (const member of retaining) await member.publishBinary(new Uint8Array([10]), { track: 'screen' })
    await vi.waitFor(() => expect(frames.length).toBe(6))
    expect(frames.sort((a, b) => a - b)).toEqual([0, 1, 2, 10, 10, 10])
    expect(readRetained).toHaveBeenCalledTimes(3)
    expect(listRetained.mock.calls.map(([, , of]) => of)).toEqual([{ track: 'screen' }])
    await retaining[0]!.leave()
    expect((await driver.listRetained(authority.id, authority._inc, { track: 'screen' })).length).toBe(2)
  })
  it.each([
    ['one track', { track: 'screen' }],
    ['every track', undefined],
  ] as const)(
    'replays no retained frame of a member that leaves while a room-wide want of %s reads it',
    async (_, options) => {
      const authority = (await Room.create(`retained-leaver-${options?.track ?? 'all'}`)) as ServerRoom
      const leaver = await authority.join()
      const stayer = await authority.join()
      await leaver.publishBinary(new Uint8Array([1]), { track: 'screen', retain: true })
      await stayer.publishBinary(new Uint8Array([2]), { track: 'screen', retain: true })
      const readRetained = driver.readRetained.bind(driver)
      vi.spyOn(driver, 'readRetained').mockImplementation(async (...args) => {
        const stored = await readRetained(...args)
        // The leave, its cleanup first, lands between the replay's read and its delivery.
        if (args[2].kind === 'binary' && args[2].member === leaver.id) await leaver.leave()
        return stored
      })
      const replays = vi.spyOn(authority, '_replayRetainedBinary')
      const frames: number[] = []
      authority.subscribeBinary((data) => void frames.push(data[0]!), options)
      await vi.waitFor(() => expect(replays).toHaveBeenCalled())
      await Promise.all(replays.mock.results.map(({ value }) => value))
      expect(frames).toEqual([2])
    },
  )
  it('replays no retained message of a member that leaves while the replay reads it', async () => {
    const authority = (await Room.create('retained-text-leaver')) as ServerRoom
    const leaver = await authority.join()
    await leaver.publish('pinned', { retain: true })
    const readRetained = driver.readRetained.bind(driver)
    let left = false
    vi.spyOn(driver, 'readRetained').mockImplementation(async (...args) => {
      const stored = await readRetained(...args)
      // The leave's cleanup reads the message too, and deletes it before the leave lands.
      if (args[2].kind === 'semantic' && !left) (left = true), await leaver.leave()
      return stored
    })
    const replays = vi.spyOn(authority, '_replayRetainedText')
    const texts: unknown[] = []
    authority.subscribe((data) => void texts.push(data))
    await vi.waitFor(() => expect(replays).toHaveBeenCalled())
    await Promise.all(replays.mock.results.map(({ value }) => value))
    expect({ left, texts }).toEqual({ left: true, texts: [] })
  })
  it('drops a live frame older than a retained frame that reached the subscriber first', async () => {
    const authority = await Room.create('retained-before-older-live')
    const publisher = await authority.join()
    const observer = await Room.get(authority.id)
    const readRetained = driver.readRetained.bind(driver)
    const published = createDeferred()
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
      declare(stub, { __r: 'sub-binary', ...EVERY_TRACK })
      await roster.started
      expect(listRetained).not.toHaveBeenCalled()
      roster.release()
      await vi.waitFor(() => expect(listRetained).toHaveBeenCalled())
    } finally {
      roster.release()
    }
  })
  it("keeps each member's declared binary want until a declaration names that member", async () => {
    const room = (await Room.create('declared-changes')) as ServerRoom
    const a = await room.join()
    const b = await room.join()
    const stub = register(room)
    declare(stub, { __r: 'sub-binary', member: a.id, track: null, on: true })
    declare(stub, { __r: 'sub-binary', member: b.id, track: null, on: true })
    await subsOf(room).binaryReady()
    expect(binaryLanes(room)).toEqual([`binary-all:${a.id}`, `binary-all:${b.id}`].sort())
    declare(stub, { __r: 'sub-binary', member: a.id, track: null, on: false })
    await subsOf(room).binaryReady()
    expect(binaryLanes(room)).toEqual([`binary-all:${b.id}`])
  })
  it("reopens a member's binary lane wanted again after its want stopped", async () => {
    const room = (await Room.create('declared-rewant')) as ServerRoom
    const member = await room.join()
    const stub = register(room)
    const lanes: number[] = []
    for (const on of [true, false, true]) {
      declare(stub, { __r: 'sub-binary', member: member.id, track: null, on })
      await subsOf(room).binaryReady()
      lanes.push(binaryLanes(room).length)
    }
    expect(lanes).toEqual([1, 0, 1])
  })
  it('opens no backend lane for a declared want naming no member', async () => {
    const authority = await Room.create('declared-strangers')
    await authority.join()
    const observer = (await Room.get(authority.id)) as ServerRoom
    const stub = register(observer)
    const subscribeLane = vi.spyOn(getRoomBackend(), 'subscribeLane')
    for (let i = 0; i < 100; i++)
      declare(stub, { __r: 'sub-binary', member: crypto.randomUUID(), track: 'screen', on: true })
    await observer.getParticipants()
    expect(subscribeLane.mock.calls.filter(([, , lane]) => lane.kind === 'binary')).toEqual([])
  })
  it('holds no want naming an id that is no member', async () => {
    const room = (await Room.create('declared-non-member')) as ServerRoom
    const member = await room.join()
    const stub = register(room)
    declareEach(stub, [crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID(), member.id])
    const only = [member.id]
    expect(wantsHeld(room, stub)).toEqual({ text: only, binary: only, indexedText: only, indexedBinary: only })
  })
  it('drops the wants naming a member as it leaves', async () => {
    const room = (await Room.create('declared-leave')) as ServerRoom
    const member = await room.join()
    const binaryStub = register(room)
    const textStub = register(room)
    declare(binaryStub, { __r: 'sub-binary', member: member.id, track: null, on: true })
    declare(textStub, { __r: 'sub-text', announce: false, members: { [member.id]: true } })
    await member.leave()
    const none = { text: [], binary: [], indexedText: [], indexedBinary: [] }
    expect(wantsHeld(room, binaryStub)).toEqual(none)
    expect(wantsHeld(room, textStub)).toEqual(none)
  })
  it('holds no want naming an id the view does not hold while every roster read fails', async () => {
    const authority = await Room.create('declared-before-roster')
    const observer = (await Room.get(authority.id)) as ServerRoom
    const readCellPages = driver.readCellPages.bind(driver)
    vi.spyOn(driver, 'readCellPages').mockImplementation(async function* (roomId, inc, prefix) {
      if (roomId === authority.id) throw new Error('backend roster read failed')
      yield* readCellPages(roomId, inc, prefix)
    })
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const member = await observer.join()
    const stub = register(observer)
    const peer = attachPeer(stub)
    await vi.waitFor(() => expect(relayed(peer).map(({ __r }) => __r)).toContain('roster-error'))
    declareEach(stub, [crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID(), member.id])
    expect(observer._state.rosterKnown).toBe(false)
    const only = [member.id]
    expect(wantsHeld(observer, stub)).toEqual({ text: only, binary: only, indexedText: only, indexedBinary: only })
  })
  it('opens an exact-member binary lane for a member the view holds before its roster loads', async () => {
    const authority = await Room.create('exact-binary-before-roster')
    const observer = (await Room.get(authority.id)) as ServerRoom
    const publisher = await observer.join()
    const stub = register(observer)
    const roster = delayRosterRead(authority.id)
    const subscribeLane = vi.spyOn(getRoomBackend(), 'subscribeLane')
    const binaryLanes = () => subscribeLane.mock.calls.filter(([, , lane]) => lane.kind === 'binary').length
    try {
      declare(stub, { __r: 'sub-binary', member: publisher.id, track: 'screen', on: true })
      await roster.started
      expect(observer._state.rosterKnown).toBe(false)
      expect(binaryLanes()).toBe(1)
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
  it("drops a leaving member's retained binary lanes without listing the room's", async () => {
    const room = (await Room.create('retained-leave-unlisted')) as ServerRoom
    const member = await room.join()
    const other = await room.join()
    await member.publishBinary(new Uint8Array([1]), { retain: true })
    await member.publishBinary(new Uint8Array([2]), { track: 'screen', retain: true })
    await other.publishBinary(new Uint8Array([3]), { retain: true })
    const listRetained = vi.spyOn(driver, 'listRetained')
    await member.leave()
    expect(listRetained.mock.calls.map(([, , of]) => of)).toEqual([{ member: member.id }])
    expect(await driver.listRetained(room.id, room._inc)).toEqual([{ kind: 'binary', member: other.id, track: '' }])
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
    declare(stub, { __r: 'sub-text', members: {}, announce: true })
    await member.publish('held')
    await Room.announce(tail.id, 'notice')
    await vi.waitFor(() => expect(semanticFrames(peer, 'announce')).toEqual(['notice']))
    stub._onPeerSubscription('text', true)
    await vi.waitFor(() => expect(semanticFrames(peer, 'data')).toEqual(['held']))
  })
  it('never shows a join that an identity kick overtook while it was announced', async () => {
    const room = (await Room.create('join-kick-overtake')) as ServerRoom
    // The kick's leave reaches this instance after the failed join settles, as over a networked backend.
    const held = holdLaneDelivery((lane) => lane.kind === 'control')
    const seen: string[] = []
    room.onJoin(() => seen.push('join'))
    room.onLeave((_member, cause) => seen.push(cause.type))
    await vi.waitFor(() => expect(subsOf(room)._control.established).toBe(true))
    const backend = getRoomBackend()
    const commitLane = backend.commitLane.bind(backend)
    let kicked = false
    vi.spyOn(backend, 'commitLane').mockImplementation(async (roomId, inc, lane, payload, options) => {
      if (!kicked && lane.kind === 'control' && (parse(decoder.decode(payload)) as { __r?: string }).__r === 'join') {
        kicked = true
        // The user's other tab is banned while this join's announcement is in flight.
        await Room.removeParticipant(room.id, { identity: 'user-1', reason: 'banned' })
      }
      return commitLane(roomId, inc, lane, payload, options)
    })
    await expect(room.join({ identity: 'user-1' })).rejects.toThrow()
    await held.release()
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(seen).toEqual([])
  })
  it('does not show a member again that a kick removed while its join announcement was confirming', async () => {
    const room = (await Room.create('join-kick-confirming')) as ServerRoom
    const seen: string[] = []
    room.onJoin(() => seen.push('join'))
    room.onLeave((_member, cause) => seen.push(cause.type))
    const peer = attachPeer(register(room))
    const backend = getRoomBackend()
    const commitLane = backend.commitLane.bind(backend)
    const committed = createDeferred()
    const confirm = createDeferred()
    vi.spyOn(backend, 'commitLane').mockImplementation(async (roomId, inc, lane, payload, options) => {
      const result = await commitLane(roomId, inc, lane, payload, options)
      const isJoin = lane.kind === 'control' && (parse(decoder.decode(payload)) as { __r?: string }).__r === 'join'
      if (!isJoin || !('accepted' in result)) return result
      committed.resolve()
      // Its delivery is confirmed late, as when one of a Cloudflare room's session DOs is slow.
      return { ...result, delivery: result.delivery.then(() => confirm.promise) }
    })
    const joining = room.join({ identity: 'user-1' }).catch((error: unknown) => error)
    await committed.promise
    await Room.removeParticipant(room.id, { identity: 'user-1', reason: 'banned' })
    await vi.waitFor(() => expect(seen).toEqual(['join', 'removed']))
    confirm.resolve()
    expect(isRoomError(await joining)).toBe(true)
    expect(seen).toEqual(['join', 'removed'])
    expect(room.count).toBe(0)
    expect(relayed(peer).flatMap(({ __r }) => (__r === 'join' || __r === 'leave' ? [__r] : []))).toEqual([
      'join',
      'leave',
    ])
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
  it('gets an every-track subscriber on another instance the unretained first frame of a track started after it subscribed, and tells the publisher it is wanted', async () => {
    const room = await Room.create('first-frame-of-new-track')
    const publisher = await room.join()
    const demand: Array<[string | null, boolean]> = []
    publisher.onDemand((track, wanted) => demand.push([track, wanted]))
    // Control events reach the observer only later, as over a networked backend: the new track needs none.
    const control = holdLaneDelivery((lane) => lane.kind === 'control')
    const observer = (await Room.get(room.id)) as ServerRoom
    const frames: string[] = []
    observer.subscribeBinary((data, info) => frames.push(`${info.track}:${data[0]}`))
    await vi.waitFor(() => expect(demand).toEqual([[null, true]]))
    await subsOf(observer).binaryReady()
    await publisher.publishBinary(new Uint8Array([1]), { track: 'screen' })
    await vi.waitFor(() => expect(frames).toEqual(['screen:1']))
    expect(demand).toEqual([
      [null, true],
      ['screen', true],
    ])
    await control.release()
  })
  it('gets a subscriber of one of 100 named tracks only that track', async () => {
    const room = await Room.create('hundred-tracks')
    const publisher = await room.join()
    const frames: string[] = []
    ;(await room.getParticipant(publisher.id))!.subscribeBinary(
      (data, info) => frames.push(`${info.track}:${data[0]}`),
      { track: 't73' },
    )
    for (const track of trackNames(100)) await publisher.publishBinary(new Uint8Array([1]), { track })
    await publisher.publishBinary(new Uint8Array([2]), { track: 't73' })
    await vi.waitFor(() => expect(frames).toEqual(['t73:1', 't73:2']))
  })
  it('gets an every-track subscriber on another instance one frame from each of 1,000 tracks, over one backend subscription', async () => {
    const room = await Room.create('thousand-tracks')
    const publisher = await room.join()
    const observer = (await Room.get(room.id)) as ServerRoom
    const subscribed = countBinarySubscriptions()
    const frames: string[] = []
    ;(await observer.getParticipant(publisher.id))!.subscribeBinary((_data, info) => frames.push(info.track!))
    await subsOf(observer).binaryReady()
    const tracks = trackNames(1_000)
    for (const track of tracks) await publisher.publishBinary(new Uint8Array([1]), { track })
    await vi.waitFor(() => expect(frames.length).toBe(1_000))
    expect(frames).toEqual(tracks)
    expect(subscribed).toEqual([{ kind: 'binary-all', member: publisher.id }])
  })
  it("lets a subscriber name 1,000 tracks of one member, receiving each track's frames in that track's order", async () => {
    const room = await Room.create('thousand-named')
    const publisher = await room.join()
    const observer = (await Room.get(room.id)) as ServerRoom
    const remote = (await observer.getParticipant(publisher.id))!
    const received = trackNames(1_000).map((track) => {
      const frames: number[] = []
      remote.subscribeBinary((data) => frames.push(data[0]!), { track })
      return { track, frames }
    })
    await subsOf(observer).binaryReady()
    for (const round of [0, 1, 2])
      for (const { track } of received) await publisher.publishBinary(new Uint8Array([round]), { track })
    await vi.waitFor(() => expect(received.every(({ frames }) => frames.length === 3)).toBe(true))
    expect(received.filter(({ frames }) => frames.join() !== '0,1,2')).toEqual([])
  })
  it("relays to a client naming 500 tracks of one member each track's frames", async () => {
    const room = (await Room.create('client-named-tracks')) as ServerRoom
    const publisher = await room.join()
    const { stub, peer } = serve(room)
    const tracks = trackNames(500)
    for (const track of tracks) declare(stub, { __r: 'sub-binary', member: publisher.id, track, on: true })
    await subsOf(room).binaryReady()
    for (const track of tracks) await publisher.publishBinary(new Uint8Array([1]), { track })
    await vi.waitFor(() =>
      expect(
        peer
          .decoded()
          .filter((frame) => frame.tag === TAG.PUBLISH_BINARY)
          .map((frame) => decodeBinaryFrame(frame.data)!.track),
      ).toEqual(tracks),
    )
  })
  it('gets a room-wide subscriber of one track that track from each member publishing on 1,000, and no other', async () => {
    const room = await Room.create('room-wide-track')
    const a = await room.join()
    const b = await room.join()
    const observer = (await Room.get(room.id)) as ServerRoom
    const frames: string[] = []
    observer.subscribeBinary(
      (data, info, from) => frames.push(`${from.id === a.id ? 'a' : 'b'}:${info.track}:${data[0]}`),
      {
        track: 't500',
      },
    )
    await observer.getParticipants()
    await subsOf(observer).binaryReady()
    for (const track of trackNames(1_000))
      for (const member of [a, b]) await member.publishBinary(new Uint8Array([1]), { track })
    for (const member of [a, b]) await member.publishBinary(new Uint8Array([2]), { track: 't500' })
    await vi.waitFor(() => expect([...frames].sort()).toEqual(['a:t500:1', 'a:t500:2', 'b:t500:1', 'b:t500:2']))
  })
  it('tells a publisher on 1,000 tracks that t900 is wanted while another instance receives every track, with no flap as it narrows to t900', async () => {
    const room = await Room.create('demand-thousand')
    const publisher = await room.join()
    for (const track of trackNames(1_000)) await publisher.publishBinary(new Uint8Array([1]), { track })
    const demand: boolean[] = []
    publisher.onDemand((track, wanted) => {
      if (track === 't900') demand.push(wanted)
    })
    const observer = (await Room.get(room.id)) as ServerRoom
    const remote = (await observer.getParticipant(publisher.id))!
    const offAll = remote.subscribeBinary(() => {})
    await vi.waitFor(() => expect(demand).toEqual([true]))
    const offNamed = remote.subscribeBinary(() => {}, { track: 't900' })
    offAll()
    const t900 = encodeLaneKey({ kind: 'binary', member: publisher.id, track: 't900' })
    await vi.waitFor(() => expect(binaryLanes(observer)).toEqual([t900]))
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(demand).toEqual([true])
    offNamed()
    await vi.waitFor(() => expect(demand).toEqual([true, false]))
  })
  it.each([
    ['widens to every track', 'binary-all', false],
    ['narrows back to it', 'binary', true],
  ] as const)(
    "keeps a named track's frames while the want %s and the replacing lane establishes",
    async (_, replacing, wide) => {
      const room = (await Room.create(`make-before-break-${replacing}`)) as ServerRoom
      const publisher = await room.join()
      const observer = (await Room.get(room.id)) as ServerRoom
      const remote = (await observer.getParticipant(publisher.id))!
      const offAll = wide ? remote.subscribeBinary(() => {}) : null
      const named: number[] = []
      remote.subscribeBinary((data) => named.push(data[0]!), { track: 't1' })
      const t1 = encodeLaneKey({ kind: 'binary', member: publisher.id, track: 't1' })
      const all = everyTrackLaneKey(publisher.id)
      await vi.waitFor(() => expect(binaryLanes(observer)).toEqual([wide ? all : t1]))
      await subsOf(observer).binaryReady()
      // Committed as at another instance, so not held behind this one's establishing lane.
      const commit = (n: number) =>
        driver.commitLane(
          room.id,
          room._inc,
          { kind: 'binary', member: publisher.id, track: 't1' },
          encodeBinaryFrame(publisher.id, new Uint8Array([n]), { track: 't1' }),
        )
      const release = delayDriverLane((lane) => lane.kind === replacing)
      if (offAll) offAll()
      else remote.subscribeBinary(() => {})
      await commit(1)
      release()
      await vi.waitFor(() => expect(binaryLanes(observer)).toEqual([wide ? t1 : all]))
      await subsOf(observer).binaryReady()
      await commit(2)
      await vi.waitFor(() => expect(named).toEqual([1, 2]))
    },
  )
  it("serves a member past ROOM_MEMBER_NAMED_LANES_MAX named tracks from its every-track lane and back, each track's frames once and in order", async () => {
    const room = (await Room.create('named-lanes-max')) as ServerRoom
    const publisher = await room.join()
    const observer = (await Room.get(room.id)) as ServerRoom
    const remote = (await observer.getParticipant(publisher.id))!
    const tracks = trackNames(ROOM_MEMBER_NAMED_LANES_MAX + 1)
    const extra = tracks.pop()!
    const got = new Map<string, number[]>()
    const record = (track: string) => (data: Uint8Array) => got.set(track, [...(got.get(track) ?? []), data[0]!])
    for (const track of tracks) remote.subscribeBinary(record(track), { track })
    const named = tracks.map((track) => encodeLaneKey({ kind: 'binary', member: publisher.id, track })).sort()
    await vi.waitFor(() => expect(binaryLanes(observer)).toEqual(named))
    await subsOf(observer).binaryReady()
    // Committed as at another instance, so not held behind this one's establishing lane.
    const commit = async (n: number, on: string[]) => {
      for (const track of on)
        await driver.commitLane(
          room.id,
          room._inc,
          { kind: 'binary', member: publisher.id, track },
          encodeBinaryFrame(publisher.id, new Uint8Array([n]), { track }),
        )
    }
    let release = delayDriverLane((lane) => lane.kind === 'binary-all')
    const offExtra = remote.subscribeBinary(record(extra), { track: extra })
    await commit(1, tracks)
    release()
    await vi.waitFor(() => expect(binaryLanes(observer)).toEqual([everyTrackLaneKey(publisher.id)]))
    await subsOf(observer).binaryReady()
    await commit(2, [...tracks, extra, 'unnamed'])
    release = delayDriverLane((lane) => lane.kind === 'binary')
    offExtra()
    await commit(3, [...tracks, extra])
    release()
    await vi.waitFor(() => expect(binaryLanes(observer)).toEqual(named))
    await subsOf(observer).binaryReady()
    await commit(4, [...tracks, extra])
    await vi.waitFor(() => expect(got.get(tracks[0]!)).toEqual([1, 2, 3, 4]))
    expect(Object.fromEntries(got)).toEqual({
      ...Object.fromEntries(tracks.map((track) => [track, [1, 2, 3, 4]])),
      [extra]: [2],
    })
  })
  it('keeps named lanes for a few tracks wanted of each of 100 members', async () => {
    const room = (await Room.create('few-tracks-many-members')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 100 }, () => room.join()))
    const observer = (await Room.get(room.id)) as ServerRoom
    await observer.getParticipants()
    const subscribed = countBinarySubscriptions()
    const tracks = ['mic', 'camera', 'screen', 'screen-audio']
    for (const track of tracks) observer.subscribeBinary(() => {}, { track })
    await vi.waitFor(() => expect(binaryLanes(observer)).toHaveLength(members.length * tracks.length))
    await subsOf(observer).binaryReady()
    expect(subscribed.filter((lane) => lane.kind === 'binary-all')).toEqual([])
    expect(subscribed).toHaveLength(members.length * tracks.length)
  })
  it("replays a retained frame on one of a member's 1,000 tracks once to a late every-track subscriber, and drops it when the member leaves", async () => {
    const room = await Room.create('retained-of-thousand')
    const publisher = await room.join()
    for (const track of trackNames(1_000))
      await publisher.publishBinary(new Uint8Array([1]), { track, retain: track === 't700' })
    const retained = () => [...memoryState.rooms.get(room.id)!.gens.values()].flatMap((gen) => [...gen.retained.keys()])
    expect(retained()).toEqual([encodeLaneKey({ kind: 'binary', member: publisher.id, track: 't700' })])
    const observer = await Room.get(room.id)
    const frames: string[] = []
    ;(await observer.getParticipant(publisher.id))!.subscribeBinary((data, info) =>
      frames.push(`${info.track}:${data[0]}`),
    )
    await vi.waitFor(() => expect(frames).toEqual(['t700:1']))
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(frames).toEqual(['t700:1'])
    await publisher.leave()
    await vi.waitFor(() => expect(retained()).toEqual([]))
  })
  it.each([1, 1_000, 100_000])(
    'opens one backend subscription for an every-track subscriber of a member publishing on %i tracks',
    async (count) => {
      const room = await Room.create(`every-track-${count}`)
      const publisher = await room.join()
      for (const track of trackNames(count)) await publisher.publishBinary(new Uint8Array([1]), { track })
      const observer = (await Room.get(room.id)) as ServerRoom
      const subscribed = countBinarySubscriptions()
      let frames = 0
      ;(await observer.getParticipant(publisher.id))!.subscribeBinary(() => void frames++)
      await subsOf(observer).binaryReady()
      await publisher.publishBinary(new Uint8Array([2]), { track: `t${count - 1}` })
      await vi.waitFor(() => expect(frames).toBe(1))
      expect(subscribed).toEqual([{ kind: 'binary-all', member: publisher.id }])
    },
    60_000,
  )
  it.each(['listeners on this instance', "clients' declarations"])(
    'names tracks of one member one by one, through %s, in time linear in their count from 50,000 to 200,000',
    async (through) => {
      const name = async (count: number) => {
        const room = await Room.create(`named-one-by-one-${count}-${crypto.randomUUID()}`)
        const publisher = await room.join()
        const observer = (await Room.get(room.id)) as ServerRoom
        const remote = (await observer.getParticipant(publisher.id))!
        // A client names at most ROOM_TRACK_WANTS_MAX tracks of a room.
        const stubs = Array.from({ length: Math.ceil(count / ROOM_TRACK_WANTS_MAX) }, () => register(observer))
        const elapsed = await timeOutsideGc(async () => {
          for (const [n, track] of trackNames(count).entries())
            if (through === "clients' declarations")
              declare(stubs[Math.floor(n / ROOM_TRACK_WANTS_MAX)]!, {
                __r: 'sub-binary',
                member: publisher.id,
                track,
                on: true,
              })
            else remote.subscribeBinary(() => {}, { track })
          await subsOf(observer).binaryReady()
        })
        // Its clients never connect: they end here, not when the close's wait for them ends, in a later spec.
        for (const stub of stubs) stub.abort()
        await Room.close(room.id)
        // Its last planning pass runs in slices, which stay out of the next run's time.
        await subsOf(observer).binaryReady()
        return elapsed
      }
      // The faster of two runs at each count, so a scheduling hiccup in one run is not read as growth.
      const fastest = async (count: number) => Math.min(await name(count), await name(count))
      await name(50_000)
      // An insert into a hash table holding every track named so far costs more as the table outgrows the CPU's caches,
      // which from 5,000 to 50,000 tracks reads as growth (14 to 26 here). From 50,000 to 200,000 a linear path measures
      // about 4 to 6, the marking that grows with the live heap included; a path whose every change costs the tracks
      // already named, about 16.
      expect((await fastest(200_000)) / (await fastest(50_000))).toBeLessThanOrEqual(8)
    },
    60_000,
  )
  // The server notices a drop at once (the socket closed) or only at its ping deadline (a silent drop).
  const dropNoticed = [0, 2 * CHANNEL_PING_INTERVAL_MS]
  it.each(dropNoticed)(
    "keeps a kicked participant's leave, with its reason, for a client back before its hold ends (drop noticed after %i ms)",
    async (noticedAfter) => {
      vi.useFakeTimers()
      const room = (await Room.create('kick-while-offline')) as ServerRoom
      const me = (await room.join({ identity: 'user-1' })) as ServerLocalParticipant
      const channel = new RoomParticipantStubChannel(me)
      channel._registerChannel()
      const first = attachPeer(channel)
      if (noticedAfter === 0) channel._onPeerDisconnect(first.peer, CHANNEL_RECONNECT_TIMEOUT_MS)
      await Room.removeParticipant(room.id, { identity: 'user-1', reason: 'banned' })
      await vi.advanceTimersByTimeAsync(noticedAfter)
      if (noticedAfter > 0) channel._onPeerDisconnect(first.peer, CHANNEL_RECONNECT_TIMEOUT_MS)
      await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_TIMEOUT_MS - 1_000)
      expect(channel._replayBuffer).not.toBeNull()
      expect(notices(attachPeer(channel, 0))).toContainEqual({ __r: 'left', cause: 'removed', reason: 'banned' })
    },
  )
  it.each(dropNoticed)(
    "keeps a room's closed event for a client back before its hold ends (drop noticed after %i ms)",
    async (noticedAfter) => {
      vi.useFakeTimers()
      const room = (await Room.create('close-while-offline')) as ServerRoom
      const stub = register(room)
      const first = attachPeer(stub)
      await vi.waitFor(() => expect(relayed(first).map(({ __r }) => __r)).toContain('roster'))
      if (noticedAfter === 0) stub._onPeerDisconnect(first.peer, CHANNEL_RECONNECT_TIMEOUT_MS)
      await Room.close(room.id)
      await vi.advanceTimersByTimeAsync(noticedAfter)
      if (noticedAfter > 0) stub._onPeerDisconnect(first.peer, CHANNEL_RECONNECT_TIMEOUT_MS)
      await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_TIMEOUT_MS - 1_000)
      expect(stub._replayBuffer).not.toBeNull()
      expect(relayed(attachPeer(stub, 0)).map(({ __r }) => __r)).toContain('closed')
    },
  )
  it("sends a reattached client the room state and its members' demand its offline buffer dropped", async () => {
    const room = (await Room.create('reattach-resync')) as ServerRoom
    const leaver = await room.join()
    config.channel = { bufferLimit: 256 }
    try {
      const stub = register(room)
      const first = attachPeer(stub)
      const wanted = await joinThrough(stub)
      const idle = await joinThrough(stub)
      await vi.waitFor(() => expect(relayed(first).map(({ __r }) => __r)).toContain('roster'))
      stub._onPeerDisconnect(first.peer, 60_000)
      await leaver.leave()
      const observer = await Room.get(room.id)
      ;(await observer.getParticipant(wanted))!.subscribeBinary(() => {})
      await new Promise((resolve) => setTimeout(resolve, 20))
      // Larger than the offline buffer: it clears the buffered leave and demand, and is dropped too.
      const meta = { pad: 'x'.repeat(300) }
      await Room.setMeta(room.id, meta)
      await vi.waitFor(() => expect(room.meta).toEqual(meta))
      const peer = attachPeer(stub)
      await vi.waitFor(() =>
        expect(relayed(peer)).toEqual([
          expect.objectContaining({ __r: 'update', meta: [['pad', meta.pad, expect.any(Number), 0]] }),
          { __r: 'demand-state', member: wanted, tracks: [null] },
          { __r: 'demand-state', member: idle, tracks: [] },
          expect.objectContaining({
            __r: 'roster',
            members: [expect.objectContaining({ id: wanted }), expect.objectContaining({ id: idle })],
          }),
        ]),
      )
      expect(stub.isClosed).toBe(false)
    } finally {
      config.channel = {}
    }
  })
  it('turns a server roster read rejection into an explicit client-settling event', async () => {
    const room = (await Room.create('roster-error-event')) as ServerRoom
    const stub = register(room)
    const failure = new Error('backend roster read failed')
    const ensureRoster = vi.spyOn(subsOf(room), 'ensureRoster').mockRejectedValue(failure)
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const peer = attachPeer(stub)
    const rosterError = () => relayed(peer).find(({ __r }) => __r === 'roster-error')
    await vi.waitFor(() => expect(rosterError()).toBeDefined())
    const { client, emit } = fakeClient('roster-error-event')
    const participants = client.getParticipants()
    emit(rosterError())
    await expect(participants).rejects.toThrow('Failed to load room participants')
    expect(ensureRoster).toHaveBeenCalledOnce()
  })
  it('sends a stub its roster after the next successful refresh once its first roster read failed', async () => {
    const room = (await Room.create('roster-error-recovery')) as ServerRoom
    const stub = register(room)
    vi.spyOn(subsOf(room), 'ensureRoster').mockRejectedValueOnce(new Error('backend roster read failed'))
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const peer = attachPeer(stub)
    const events = () => relayed(peer).map(({ __r }) => __r)
    await vi.waitFor(() => expect(events()).toContain('roster-error'))
    await subsOf(room)._refreshMembers()
    expect(events()).toEqual(['update', 'roster-error', 'roster'])
  })
  it('replays a committed server-pushed roster after reconnect, then sends a fresh one', async () => {
    const room = (await Room.create('roster-replay')) as ServerRoom
    const stub = register(room)
    const ensureRoster = vi.spyOn(subsOf(room), 'ensureRoster')
    const first = attachPeer(stub)
    await vi.waitFor(() => expect(relayed(first).map(({ __r }) => __r)).toContain('roster'))
    stub._onPeerDisconnect(first.peer, 1_000)
    const replayed = relayed(attachPeer(stub, 0))
    expect(replayed.slice(0, 2)).toMatchObject([{ __r: 'update' }, { __r: 'roster', members: [] }])
    expect(ensureRoster).toHaveBeenCalledTimes(2)
  })
  it('tells the client of a handed-out participant the demand it already had when it was returned', async () => {
    const room = (await Room.create('handed-out-demand')) as ServerRoom
    room.subscribeBinary(() => {})
    const me = (await room.join()) as ServerLocalParticipant
    const wanted: Array<string | null> = []
    me.onDemand((track, on) => void (on && wanted.push(track)))
    // Known before the telefunction returns it, as after an onAfterJoin that awaits I/O.
    await vi.waitFor(() => expect(wanted).toEqual([null]))
    const channel = new RoomParticipantStubChannel(me)
    channel._registerChannel()
    const peer = attachPeer(channel)
    await vi.waitFor(() =>
      expect(notices(peer).filter(({ __r }) => __r === 'demand-state')).toEqual([
        { __r: 'demand-state', tracks: [null] },
      ]),
    )
  })
  it("closes its clients' stubs as it closes, at the longest reconnect window config.channel accepts", async () => {
    config.channel = { reconnectTimeout: 2 ** 31 - 1, pingInterval: 2 ** 30 - 1 }
    try {
      const room = (await Room.create('longest-reconnect-window')) as ServerRoom
      const stub = register(room)
      const held = new RoomParticipantStubChannel((await room.join()) as ServerLocalParticipant)
      held._registerChannel()
      const report = vi.spyOn(console, 'error')
      await Room.close(room.id)
      await vi.waitFor(() => expect([stub.isClosed, held.isClosed]).toEqual([true, true]))
      expect(report).not.toHaveBeenCalled()
      stub.abort()
      held.abort()
    } finally {
      config.channel = {}
    }
  })
  it('sends a reattached client of a handed-out participant the meta and demand its offline buffer dropped', async () => {
    const room = (await Room.create('reattach-participant')) as ServerRoom
    config.channel = { bufferLimit: 256 }
    try {
      const me = (await room.join({ meta: { score: 0 } })) as ServerLocalParticipant
      const wanted: Array<string | null> = []
      me.onDemand((track, on) => void (on && wanted.push(track)))
      const channel = new RoomParticipantStubChannel(me)
      channel._registerChannel()
      const first = attachPeer(channel)
      channel._onPeerDisconnect(first.peer, 60_000)
      await me.setAttributes({ score: 1 })
      const observer = await Room.get(room.id)
      ;(await observer.getParticipant(me.id))!.subscribeBinary(() => {})
      await vi.waitFor(() => expect(wanted).toEqual([null]))
      // Larger than the offline buffer: it clears the buffered meta and demand notices, and is dropped too.
      const pad = 'x'.repeat(300)
      await me.setAttributes({ pad })
      const peer = attachPeer(channel)
      await vi.waitFor(() =>
        expect(notices(peer).filter(({ __r }) => __r === 'p-meta' || __r === 'demand-state')).toEqual([
          { __r: 'p-meta', meta: { score: 1, pad }, seq: expect.any(Number) },
          { __r: 'demand-state', tracks: [null] },
        ]),
      )
      expect(channel.isClosed).toBe(false)
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
    expect([memoryState.broadcastSubs.text.size, memoryState.broadcastSubs.binary.size]).toEqual([0, 0])
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
    expect(subsOf(tail)._semantic.wanted).toBe(false)
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
    expect(observer._state.listenerCount).toBe(0)
  })
  it("commits a participant's publishes in call order, however long its guard takes for each", async () => {
    const room = await Room.create('publish-call-order')
    const slow = createDeferred()
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
    const firstAnswer = createDeferred()
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
  it('tells each member, joined before or after, of the tracks another instance names of every member, beside wants of one member without a flap', async () => {
    const room = await Room.create('every-member-demand')
    const tracks = trackNames(ROOM_MEMBER_NAMED_LANES_MAX + 1)
    const demanded = new Map<string, Array<[string | null, boolean]>>()
    const join = async () => {
      const member = await room.join()
      const changes: Array<[string | null, boolean]> = []
      member.onDemand((track, wanted) => changes.push([track, wanted]))
      demanded.set(member.id, changes)
      return member
    }
    const changesOf = (id: string) => [...demanded.get(id)!].sort()
    const all = (wanted: boolean) => tracks.map((track) => [track, wanted])
    const sorted = (changes: unknown[]) => [...changes].sort()
    const a = await join()
    const observer = (await Room.get(room.id)) as ServerRoom
    await observer.getParticipants()
    const { demand, published } = spyGossip(observer)
    const unsubscribe = tracks.map((track) => observer.subscribeBinary(() => {}, { track }))
    await vi.waitFor(() => expect(changesOf(a.id)).toEqual(sorted(all(true))))
    const b = await join()
    await vi.waitFor(() => expect(changesOf(b.id)).toEqual(sorted(all(true))))
    // One entry per track and one per member, not one per pair.
    published.mockClear()
    demand.heartbeat()
    expect(published).toHaveBeenCalledTimes(tracks.length + 2)
    // A want of `a` alone holds t0 for it while the want of every member comes and goes.
    const alone = (await observer.getParticipant(a.id))!.subscribeBinary(() => {}, { track: 't0' })
    await subsOf(observer).binaryReady()
    unsubscribe[0]!()
    await vi.waitFor(() => expect(demanded.get(b.id)!.at(-1)).toEqual(['t0', false]))
    unsubscribe[0] = observer.subscribeBinary(() => {}, { track: 't0' })
    await vi.waitFor(() => expect(demanded.get(b.id)!.at(-1)).toEqual(['t0', true]))
    alone()
    for (const off of unsubscribe) off()
    await vi.waitFor(() => expect(changesOf(a.id)).toEqual(sorted([...all(false), ...all(true)])))
    await vi.waitFor(() =>
      expect(changesOf(b.id)).toEqual(sorted([...all(false), ['t0', false], ...all(true), ['t0', true]])),
    )
  })
  it("counts no demand from a selfDelivery: false member's own side among the tracks named of every member, as it names one and drops it", async () => {
    const room = (await Room.create('self-demand-every-member')) as ServerRoom
    const me = await room.join({ selfDelivery: false })
    const other = await room.join()
    const changes = (member: typeof me) => {
      const changed: Array<[string | null, boolean]> = []
      member.onDemand((track, wanted) => changed.push([track, wanted]))
      return changed
    }
    const mine = changes(me)
    const others = changes(other)
    const tracks = trackNames(ROOM_MEMBER_NAMED_LANES_MAX + 1)
    const stub = register(room)
    for (const track of tracks) declare(stub, { __r: 'sub-binary', member: null, track, on: true })
    // The room's own listeners, which `me`'s frames don't reach, name one more track of every member.
    const unsubscribe = room.subscribeBinary(() => {}, { track: 'own' })
    const on = tracks.map((track) => [track, true]).sort()
    await vi.waitFor(() => expect([...others].sort()).toEqual([['own', true], ...on].sort()))
    await vi.waitFor(() => expect([...mine].sort()).toEqual(on))
    unsubscribe()
    await vi.waitFor(() => expect(others.at(-1)).toEqual(['own', false]))
    await subsOf(room).binaryReady()
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect([...mine].sort()).toEqual(on)
  })
  it("counts no demand from a selfDelivery: false publisher's own side", async () => {
    const room = (await Room.create('self-demand')) as ServerRoom
    const me = (await room.join({ selfDelivery: false })) as ServerLocalParticipant
    // `me` is held by its client, which is told its demand.
    const toClient = vi.spyOn(new RoomParticipantStubChannel(me), '_send').mockReturnValue(undefined)
    const demand = () =>
      toClient.mock.calls.map(([message]) => message as { __r: string }).filter((message) => message.__r === 'demand')
    room.subscribeBinary(() => {}, { track: 'mic' })
    await subsOf(room).binaryReady()
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(demand()).toEqual([])
    const { stub } = serve(room)
    declare(stub, { __r: 'sub-binary', member: null, track: 'mic', on: true })
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
      _deliverMessage(message: InboxMessage): void
      _deliverMessageAck(message: InboxMessage): Promise<unknown>
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
  it('holds DMs before the first listen() while their bytes, each with its overhead, fit bufferLimit, failing the ack sender of the oldest it drops', async () => {
    const room = await Room.create('inbox-overflow')
    const recipient = await room.join()
    const sender = await room.join()
    for (let n = 0; n < 200; n++) await sender.send(recipient.id, n)
    const received: unknown[] = []
    recipient.listen((data) => void received.push(data))
    expect(received).toEqual(Array.from({ length: 200 }, (_, n) => n))
    const late = await room.join()
    const oldestHeld = createDeferred()
    Room.guard(room, {
      onAfterSend: (_from, _to, data) => {
        if (data === 0) oldestHeld.resolve()
      },
    })
    const oldest = sender.send(late.id, 0, { ack: true })
    await oldestHeld.promise
    const half = getServerConfig().channel.bufferLimit / 2
    await sender.send(late.id, 'a'.repeat(half))
    await sender.send(late.id, 'b'.repeat(half))
    const kept: string[] = []
    late.listen((data) => void kept.push((data as string)[0]!))
    expect(kept).toEqual(['b'])
    await expect(oldest).rejects.toThrow('Inbox overflowed before the message was handled')
  })
  it('reports rejected async participant inbox, demand, and leave callbacks', async () => {
    const room = await Room.create('async-participant-callbacks')
    const participant = await room.join()
    const errors: unknown[] = []
    const internal = participant as unknown as {
      _reportError(error: unknown): void
      _deliverMessage(message: InboxMessage): void
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
    const firstReceived = createDeferred()
    let secondReceived = createDeferred()
    const first = broadcast.subscribe(route, (payload) => {
      received.push(`first:${payload}`)
      firstReceived.resolve()
    })
    const second = broadcast.subscribe(route, (payload) => {
      received.push(`second:${payload}`)
      secondReceived.resolve()
    })
    await Promise.all([first.ready, second.ready])
    await broadcast.publish(route, 'one')
    await Promise.all([firstReceived.promise, secondReceived.promise])
    expect(received).toEqual(['first:one', 'second:one'])
    await first.unsubscribe()
    secondReceived = createDeferred()
    await broadcast.publish(route, 'two')
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
  it('keeps remote serializer backing unforgeable and exact-keyed', async () => {
    const room = await Room.create('remote-backing')
    const joined = await room.join()
    const remote = await room.getParticipant(joined.id)
    expect(remoteBacking(remote)).not.toBeNull()
    expect(remoteBacking(Object.create(remote!))).toBeNull()
    expect(Object.getOwnPropertySymbols(remote!)).toEqual([])
  })
})
describe('client Room lifecycle', () => {
  it('refuses a subscribeBinary naming more than ROOM_TRACK_WANTS_MAX tracks of one room handle, declaring nothing for it', () => {
    const declared: unknown[] = []
    const { client } = fakeClient('client-track-wants', { send: async (notice) => void declared.push(notice) })
    const offs = trackNames(ROOM_TRACK_WANTS_MAX).map((track) => client.subscribeBinary(() => {}, { track }))
    expect(() => client.subscribeBinary(() => {}, { track: 'past' })).toThrow(
      `Too many binary tracks subscribed on one room (${ROOM_TRACK_WANTS_MAX} max)`,
    )
    // A track already named, and every track, take nothing more.
    client.subscribeBinary(() => {}, { track: 't0' })
    client.subscribeBinary(() => {})
    offs[1]!()
    client.subscribeBinary(() => {}, { track: 'past' })
    expect(declared).toHaveLength(ROOM_TRACK_WANTS_MAX + 3)
    expect(declared.at(-1)).toEqual({ __r: 'sub-binary', member: null, track: 'past', on: true })
  })
  it("keeps a client-held participant's meta in accepted revision order", async () => {
    const acks: Array<(accepted: unknown) => void> = []
    const { participant, notify } = standaloneParticipant({ v: 0 }, () => new Promise((resolve) => acks.push(resolve)))
    const first = participant.setMeta({ v: 'A' })
    const second = participant.setMeta({ v: 'B' })
    notify({ __r: 'p-meta', meta: { v: 'B' }, seq: 2 })
    acks[1]!({ meta: { v: 'B' }, seq: 2 })
    acks[0]!({ meta: { v: 'A' }, seq: 1 }) // the older write's ack arrives last
    await Promise.all([first, second])
    expect(participant.meta).toEqual({ v: 'B' })
  })
  it('holds DMs before a client-held participant’s first listen() within the bufferLimit its server sent', () => {
    const dm = (data: number) => ({ __r: 'dm', from: '', fromMeta: null, data })
    const roomy = standaloneParticipant({})
    for (let n = 0; n < 200; n++) roomy.notify(dm(n))
    const received: unknown[] = []
    roomy.participant.listen((data) => void received.push(data))
    expect(received).toEqual(Array.from({ length: 200 }, (_, n) => n))
    // Their serialized bytes alone fit 20,000; with each one's overhead they don't.
    const tight = standaloneParticipant({}, undefined, 20_000)
    for (let n = 0; n < 200; n++) tight.notify(dm(n))
    const kept: number[] = []
    tight.participant.listen((data) => void kept.push(data as number))
    expect(kept.length).toBeGreaterThan(0)
    expect(kept.length).toBeLessThan(200)
    expect(kept).toEqual(Array.from({ length: kept.length }, (_, i) => 200 - kept.length + i))
  })
  it("applies a handed-out participant's whole demand set as the changes from what it had", () => {
    const { participant, notify } = standaloneParticipant({})
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
  describe('Room-derived handle ownership (real GC)', () => {
    it.each([
      ['a roster participant', 'gc-list-owner', false, retainOnlyListedRemote],
      ['a participant revived with its Room', 'gc-revived-owner', false, retainOnlyRevivedRemote],
      ['a joined participant', 'gc-join-owner', true, retainOnlyJoinedParticipant],
      ['a callback participant', 'gc-callback-owner', false, retainOnlyCallbackRemote],
      ['a departed participant', 'gc-departed-owner', true, retainOnlyDepartedParticipant],
    ] as const)('does not make %s the owner of its Room wrapper', async (_handle, roomId, join, retainOnly) => {
      const gc = gcFixture(roomId, join)
      const retained = await retainOnly(gc)
      await forceRoomGc()
      expect(retained.room.deref()).toBeUndefined()
      expect(retained.member.id).toBe(gc.memberId)
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
    const answer = createDeferred<string>()
    participant.listen(() => answer.promise)
    emit(
      { __r: 'dm', to: id, from: crypto.randomUUID(), fromMeta: {}, fromMetaSeq: 0, data: 'hi', ackId: 'ack-late' },
      1,
    )
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
    emit(
      {
        __r: 'dm',
        to: id,
        from: crypto.randomUUID(),
        fromMeta: {},
        fromMetaSeq: 0,
        data: 'welcome',
        ackId: 'ack-welcome',
      },
      1,
    )
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 2)
    emit(
      { __r: 'dm', to: id, from: crypto.randomUUID(), fromMeta: {}, fromMetaSeq: 0, data: 'again', ackId: 'ack-again' },
      3,
    )
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
      [() => me.publish('x', { retain: 1 } as never), 'publish() options.retain should be a boolean'],
      [() => me.publish('x', { coalesce: 1 } as never), 'publish() options.coalesce should be a string'],
      [() => me.send(id, 'x', { ack: 'yes' } as never), 'send() options.ack should be a boolean'],
      [
        () => me.publishBinary(new Uint8Array([1]), { retain: 1 } as never),
        'publishBinary() options.retain should be a boolean',
      ],
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
    emit({ __r: 'roster', members: [], part: 0 }, 2)
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
        part: 0,
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
    emit({ __r: 'roster', members: [{ id: memberId, meta: { step: 0 }, joinedAt: 1, metaSeq: 0 }], part: 0 })
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
    emit({ __r: 'roster', members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0 }], part: 0 })
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
    emit({ __r: 'roster', members: [], part: 0 })
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
    emit({ __r: 'roster', members: [], part: 0 })
    const member = { id: crypto.randomUUID(), meta: { role: 'moderator' }, joinedAt: 1, metaSeq: 0, identity: null }
    const hidden = client._reviveRemote({ ...member, hidden: true })
    let left = 0
    hidden.onLeave(() => left++)
    emit({ __r: 'roster', members: [{ ...member, hidden: true }], part: 0 }, 2)
    expect(client._getRemote(hidden.id)).toBe(hidden)
    await expect(client.getParticipants({ hidden: true })).rejects.toThrow(
      'Hidden participants can only be enumerated on the server',
    )
    // A roster without it: the member left before this client's stub could hear its leave.
    emit({ __r: 'roster', members: [], part: 0 }, 3)
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
      stub._dispatchFrame({ tag: TAG.ACK_RES, index: 0, seq: 1, bytes: 0, ackedSeq: 1, status: ACK_STATUS.ERROR, text })
      await expect(publishing).rejects.toThrow(text)
      expect(report).not.toHaveBeenCalled()
    } finally {
      stub.abort()
      delete clientConfig.fetch
    }
  })
  it('takes a coalesced publish that waits behind its key as it was at the call', async () => {
    const memberId = crypto.randomUUID()
    const { fake, client } = fakeClient('client-coalesce-owned', {
      send: async (message: any) => (message.__r === 'req-join' ? { id: memberId, joinedAt: 1 } : undefined),
    })
    const me = await client.join()
    const sent: string[] = []
    let releaseFirst!: () => void
    vi.spyOn(fake.stub as any, '_publishUnreported').mockImplementation(async (...args: unknown[]) => {
      sent.push(JSON.stringify(args))
      if (sent.length === 1) await new Promise<void>((resolve) => (releaseFirst = resolve))
      return { key: 'fake', seq: sent.length, timestamp: 1 }
    })
    void me.publish({ draft: 'first' }, { coalesce: 'draft' })
    const draft = { draft: 'hello' }
    const queued = me.publish(draft, { coalesce: 'draft' }) // waits behind the first
    draft.draft = '' // the page clears its draft once submitted
    releaseFirst()
    await queued
    expect(sent[1]).toContain('hello')
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
    emit({ __r: 'roster', members: [member], part: 0 })
    const stopRoomWide = client.subscribe(() => {})
    log.length = 0
    client._getRemote(member.id)!.subscribe(() => {})
    // Declared at once, so a stream stopped live or in a reattach's RECONCILE finds the member set in place.
    expect(log).toEqual([{ [member.id]: true }])
    stopRoomWide()
    expect(log).toEqual([{ [member.id]: true }, false])
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
  it('declares a room-level default binary track without an earlier all-track listener', () => {
    const sent: unknown[] = []
    const { client } = fakeClient('default-track-declaration', {
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    client.subscribeBinary(() => {}, { track: null })
    expect(sent).toContainEqual({ __r: 'sub-binary', member: null, track: DEFAULT_TRACK, on: true })
  })
  it('declares only the binary want that changed, and the text wants of the member whose listeners changed', () => {
    const sent: unknown[] = []
    const { client, emit } = fakeClient('declared-member-changes', {
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    const members = [0, 1, 2].map(() => ({
      id: crypto.randomUUID(),
      meta: {},
      joinedAt: 1,
      metaSeq: 0,
      identity: null,
    }))
    emit({ __r: 'roster', members, part: 0 })
    for (const { id } of members) client._getRemote(id)!.subscribeBinary(() => {})
    client._getRemote(members[1]!.id)!.subscribe(() => {})()
    expect(sent).toEqual([
      ...members.map(({ id }) => ({ __r: 'sub-binary', member: id, track: null, on: true })),
      { __r: 'sub-text', announce: false, members: { [members[1]!.id]: true } },
      { __r: 'sub-text', announce: false, members: { [members[1]!.id]: false } },
    ])
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
      { __r: 'sub-text', members: {}, announce: true },
      { __r: 'sub-text', members: {}, announce: false },
    ])
    expect(wireDeclarations).toEqual([])
  })
})
describe('RoomState', () => {
  it('records the events of members it does not know for a roster read, and narrates only what a roster changes', async () => {
    const state = newState({ roomId: 'unknown-member-epoch' })
    const id = crypto.randomUUID()
    const events = await state.recordingMembership(
      async () => {
        state.applyParticipantMeta(id, { seq: 1, meta: { step: 1 } })
        state.applyLeave(id, { type: 'left' })
      },
      (_, events) => events,
    )
    expect(events).toEqual([{ meta: { id, update: { seq: 1, meta: { step: 1 } } } }, { leave: id }])
    const member = { id, meta: {}, joinedAt: 1, metaSeq: 0 }
    expect(await state.reconcileRoster({ members: [member], departing: new Set() }, [])).toBe(true)
    let changes = 0
    state.onChange(() => changes++)
    const reconcile = () => state.reconcileRoster({ members: [{ ...member, metaSeq: 1 }], departing: new Set() }, [])
    expect(await reconcile()).toBe(true)
    expect(changes).toBe(1)
    expect(await reconcile()).toBe(false)
    expect(changes).toBe(1)
  })
  it('reconciles every member to the final state and preserves semantic joins', async () => {
    const alice = { id: crypto.randomUUID(), meta: {}, joinedAt: 1, metaSeq: 0 }
    const bob = { id: crypto.randomUUID(), meta: {}, joinedAt: 2, metaSeq: 0 }
    const carol = { id: crypto.randomUUID(), meta: {}, joinedAt: 3, metaSeq: 0 }
    const state = newState({ roomId: 'reconcile-change-count', seed: { members: [alice] } })
    const observed: string[][] = []
    const joins: string[] = []
    state.onChange(() => observed.push([...state.memberIds()]))
    state.onJoin((member) => joins.push(member.id))
    await state.reconcileRoster({ members: [alice, bob, carol], departing: new Set() }, [])
    expect(observed.at(-1)).toEqual([alice.id, bob.id, carol.id])
    expect(joins).toEqual([bob.id, carol.id])
  })
  it('reports rejected async RoomState callbacks without awaiting delivery', async () => {
    const memberId = crypto.randomUUID()
    const errors: unknown[] = []
    const state = newState({
      roomId: 'async-state-callbacks',
      seed: { members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0 }] },
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
    const info = { key: state.roomId, seq: 1, timestamp: 1 }
    state.applyData({ __r: 'data', from: memberId, fromMeta: {}, data: 'text' }, info)
    state.applyBinary({ from: memberId, payload: new Uint8Array(), track: null, meta: null, retain: false }, info)
    state.applyClosed()
    await Promise.resolve()
    expect(errors).toEqual(failures)
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
    demand.applyWant({ __r: 'want', member: 'elsewhere', track: 'screen', instance: 'remote-a', on: true })
    expect(demand.isActive()).toBe(false)
    demand.applyWant({ __r: 'want', member: 'member', track: 'screen', instance: 'remote-a', on: true })
    owned.delete('member')
    demand.forgetMember('member')
    demand.applyWant({ __r: 'want', member: 'member', track: 'screen', instance: 'remote-b', on: true })
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
    demand.applyWant({ __r: 'want', member: 'member', track: 'screen', instance: 'crashed', on: true })
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
  it("pairs a reporter's tracks named of every member only with the members it covers, until their leases lapse", () => {
    vi.useFakeTimers()
    const delivered: Array<[string, string, boolean]> = []
    const demand = new RoomDemand(
      () => {},
      () => true,
      (member, track, wanted) => delivered.push([member, track, wanted]),
    )
    demand.applyWant({ __r: 'want', member: null, track: 'screen', instance: 'crashed', on: true })
    demand.applyWant({ __r: 'covers', member: 'a', instance: 'other', on: true })
    expect(delivered).toEqual([])
    demand.applyWant({ __r: 'covers', member: 'a', instance: 'crashed', on: true })
    demand.applyWant({ __r: 'covers', member: 'b', instance: 'crashed', on: true })
    expect(demand.wanted('a')).toEqual(['screen'])
    vi.advanceTimersByTime(ROOM_DEMAND_TTL_MS - 1)
    demand.applyWant({ __r: 'covers', member: 'a', instance: 'other', on: true })
    demand.heartbeat()
    vi.advanceTimersByTime(1)
    demand.heartbeat()
    expect(delivered).toEqual([
      ['a', 'screen', true],
      ['b', 'screen', true],
      ['a', 'screen', false],
      ['b', 'screen', false],
    ])
    expect(demand.isActive()).toBe(true)
    vi.advanceTimersByTime(ROOM_DEMAND_TTL_MS)
    demand.heartbeat()
    expect(demand.isActive()).toBe(false)
  })
})
describe('room protocol validation', () => {
  it('refuses as a protocol violation a client naming more than ROOM_TRACK_WANTS_MAX tracks, wanting nothing for it', async () => {
    const room = (await Room.create('track-wants-bound')) as ServerRoom
    const publisher = await room.join()
    const demanded = new Set<string | null>()
    publisher.onDemand((track, wanted) => (wanted ? demanded.add(track) : demanded.delete(track)))
    const stub = register(room)
    const want = (track: string, member: string | null, on = true) =>
      declare(stub, { __r: 'sub-binary', member, track, on })
    // Room-wide and one member's tracks count alike.
    trackNames(ROOM_TRACK_WANTS_MAX).forEach((track, n) => want(track, n % 2 === 0 ? null : publisher.id))
    want('t0', null)
    expect(() => want('past', publisher.id)).toThrow(ProtocolViolationError)
    expect(() => want('past', null)).toThrow(ProtocolViolationError)
    await vi.waitFor(() => expect(demanded.size).toBe(ROOM_TRACK_WANTS_MAX))
    expect(demanded.has('past')).toBe(false)
    want('t0', null, false)
    want('past', null)
    await vi.waitFor(() => expect(demanded.has('past')).toBe(true))
    expect(demanded.has('t0')).toBe(false)
  })
  it('opens at most ROOM_MEMBER_NAMED_LANES_MAX + 1 lanes per member for a client naming ROOM_TRACK_WANTS_MAX tracks room-wide', async () => {
    const room = (await Room.create('room-wide-track-wants')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 50 }, () => room.join()))
    const bound = members.length * (ROOM_MEMBER_NAMED_LANES_MAX + 1)
    const stub = register(room)
    const subscribed = countBinarySubscriptions()
    const subs = subsOf(room)
    for (const track of trackNames(ROOM_TRACK_WANTS_MAX)) {
      declare(stub, { __r: 'sub-binary', member: null, track, on: true })
      expect(subs._binary.size + subs._everyTrack.size).toBeLessThanOrEqual(bound)
    }
    await vi.waitFor(() => expect(binaryLanes(room)).toEqual(members.map(({ id }) => everyTrackLaneKey(id)).sort()))
    expect(subscribed.length).toBeLessThanOrEqual(bound)
  })
  it.each([10, 50, 100])(
    'gossips ROOM_TRACK_WANTS_MAX tracks a client names of every member once each, and one entry per member, among %i members',
    async (count) => {
      const room = (await Room.create(`every-member-gossip-${count}`)) as ServerRoom
      const members = await Promise.all(Array.from({ length: count }, () => room.join()))
      const demanded = members.map((member) => {
        const tracks = new Set<string | null>()
        member.onDemand((track, wanted) => (wanted ? tracks.add(track) : tracks.delete(track)))
        return tracks
      })
      const { demand, published } = spyGossip(room)
      const stub = register(room)
      for (const track of trackNames(ROOM_TRACK_WANTS_MAX))
        declare(stub, { __r: 'sub-binary', member: null, track, on: true })
      await vi.waitFor(() => expect(demanded.every((tracks) => tracks.size === ROOM_TRACK_WANTS_MAX)).toBe(true), {
        timeout: 10_000,
      })
      // Beside the tracks, a member's named lanes may each report its pair until the every-track lane covers it.
      expect(published.mock.calls.length).toBeLessThanOrEqual(
        ROOM_TRACK_WANTS_MAX + count * (2 * ROOM_MEMBER_NAMED_LANES_MAX + 1),
      )
      published.mockClear()
      demand.heartbeat()
      expect(published).toHaveBeenCalledTimes(ROOM_TRACK_WANTS_MAX + count)
    },
  )
  it.each([1_000, 10_000])(
    'opens no named lane for a client naming 64 tracks room-wide one by one among %i members, and gossips each track and each member once',
    async (count) => {
      const room = (await Room.create(`room-wide-burst-${count}`)) as ServerRoom
      const members = await Promise.all(Array.from({ length: count }, () => room.join()))
      const demanded = members.map((member) => {
        const tracks = new Set<string | null>()
        member.onDemand((track, wanted) => (wanted ? tracks.add(track) : tracks.delete(track)))
        return tracks
      })
      const stub = register(room)
      const subscribed = countBinarySubscriptions()
      const { published } = spyGossip(room)
      const tracks = trackNames(64)
      const before = process.memoryUsage().heapUsed
      let peak = before
      const sampler = setInterval(() => (peak = Math.max(peak, process.memoryUsage().heapUsed)), 1)
      for (const track of tracks) declare(stub, { __r: 'sub-binary', member: null, track, on: true })
      await vi.waitFor(() => expect(demanded.every((wanted) => wanted.size === tracks.length)).toBe(true), {
        timeout: 15_000,
      })
      clearInterval(sampler)
      expect(subscribed.filter((lane) => lane.kind === 'binary')).toEqual([])
      expect(subscribed).toHaveLength(count)
      expect(published.mock.calls.length).toBeLessThanOrEqual(tracks.length + count)
      // About 10 KB per member at 10,000 members, garbage included; a named lane per pair costs about 5 KB.
      expect((peak - before) / count).toBeLessThanOrEqual(64 * 1024)
    },
  )
  it('keeps the event loop turning while a client names 64 tracks room-wide one by one among 10,000 members, and as it goes', async () => {
    const longestGap = async () => {
      const room = (await Room.create(`room-wide-turns-${crypto.randomUUID()}`)) as ServerRoom
      const members = await Promise.all(Array.from({ length: 10_000 }, () => room.join()))
      let demanded = 0
      for (const member of members) member.onDemand((_, wanted) => (demanded += wanted ? 1 : -1))
      const stub = register(room)
      let last = performance.now()
      let longest = 0
      const timer = setInterval(() => {
        const now = performance.now()
        longest = Math.max(longest, now - last)
        last = now
      }, 1)
      for (const track of trackNames(64)) declare(stub, { __r: 'sub-binary', member: null, track, on: true })
      await vi.waitFor(() => expect(demanded).toBe(64 * members.length), { timeout: 15_000 })
      stub.abort()
      await vi.waitFor(() => expect(demanded).toBe(0), { timeout: 15_000 })
      clearInterval(timer)
      await Room.close(room.id)
      return longest
    }
    // The shorter of two runs, so a scheduling hiccup in one run is not read as a stall.
    expect(Math.min(await longestGap(), await longestGap())).toBeLessThanOrEqual(100)
  })
  it('renews 10,000 owned members in a heartbeat tick whose every cell read and write takes a round trip, well within the interval', async () => {
    const room = (await Room.create('heartbeat-round-trips')) as ServerRoom
    await Promise.all(Array.from({ length: 10_000 }, () => room.join()))
    const roundTrip = () => new Promise((resolve) => setTimeout(resolve, 1))
    const readCells = driver.readCells.bind(driver)
    vi.spyOn(driver, 'readCells').mockImplementation(async (...args) => (await roundTrip(), readCells(...args)))
    const exchange = driver.compareExchangeCells.bind(driver)
    vi.spyOn(driver, 'compareExchangeCells').mockImplementation(
      async (...args) => (await roundTrip(), exchange(...args)),
    )
    const started = performance.now()
    await subsOf(room)._heartbeatTick()
    // One renewal after another, the renewals alone take 20,000 round trips.
    expect(performance.now() - started).toBeLessThanOrEqual(2_000)
    expect(await Room.getParticipants(room.id)).toHaveLength(10_000)
  }, 60_000)
  it('tells a watched track of each of 20,000 members wanted once while each member is replanned between two slices of telling it', async () => {
    const room = (await Room.create('demand-told-whole')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 20_000 }, () => room.join()))
    const told = new Map<string, boolean[]>()
    for (const member of members)
      member.onDemand((track, wanted) => {
        if (track === 'cam') told.set(member.id, [...(told.get(member.id) ?? []), wanted])
      })
    room.subscribeBinary(() => {})
    await subsOf(room).binaryReady()
    const stubs = Array.from({ length: Math.ceil(members.length / ROOM_TRACK_WANTS_MAX) }, () => register(room))
    const demand = (room as unknown as { _demand: { tellUntil(deadline: number): boolean } })._demand
    const tellUntil = demand.tellUntil.bind(demand)
    let replanned = false
    vi.spyOn(demand, 'tellUntil').mockImplementation((deadline) => {
      const done = tellUntil(deadline)
      if (done || replanned) return done
      // Between two slices: the client names cam of every member, and a replan of each member moves it to that want.
      replanned = true
      declare(stubs[0]!, { __r: 'sub-binary', member: null, track: 'cam', on: true })
      for (const member of members) subsOf(room).memberChanged(member.id)
      return false
    })
    members.forEach((member, i) =>
      declare(stubs[i % stubs.length]!, { __r: 'sub-binary', member: member.id, track: 'cam', on: true }),
    )
    await vi.waitFor(() => expect(replanned).toBe(true), { timeout: 15_000 })
    await subsOf(room).binaryReady()
    const sequences = new Set(members.map((member) => JSON.stringify(told.get(member.id))))
    expect([...sequences]).toEqual(['[true]'])
  })
  it('keeps ROOM_GOSSIP_IN_FLIGHT_MAX gossip commits in flight at most while a client names tracks of 200 members', async () => {
    const room = (await Room.create('gossip-in-flight')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 200 }, () => room.join()))
    const demanded = members.map((member) => {
      const tracks = new Set<string | null>()
      member.onDemand((track, wanted) => (wanted ? tracks.add(track) : tracks.delete(track)))
      return tracks
    })
    const backend = getRoomBackend()
    const commitLane = backend.commitLane.bind(backend)
    let inFlight = 0
    let most = 0
    vi.spyOn(backend, 'commitLane').mockImplementation((...args) => {
      const committed = commitLane(...args)
      if (args[0] !== room.id || args[2].kind !== 'control') return committed
      most = Math.max(most, ++inFlight)
      void committed
        .then((result) => ('stale' in result ? undefined : result.delivery))
        .catch(() => {})
        .finally(() => inFlight--)
      return committed
    })
    const { published } = spyGossip(room)
    const stub = register(room)
    const tracks = trackNames(2 * ROOM_MEMBER_NAMED_LANES_MAX)
    for (const track of tracks) declare(stub, { __r: 'sub-binary', member: null, track, on: true })
    await vi.waitFor(() => expect(demanded.every((wanted) => wanted.size === tracks.length)).toBe(true))
    await vi.waitFor(() => expect(inFlight).toBe(0))
    expect(published.mock.calls.length).toBeGreaterThan(ROOM_GOSSIP_IN_FLIGHT_MAX)
    expect(most).toBeLessThanOrEqual(ROOM_GOSSIP_IN_FLIGHT_MAX)
  })
  it('commits none of the demand gossip still waiting once its room closed', async () => {
    const room = (await Room.create('gossip-after-close')) as ServerRoom
    const member = await room.join()
    const backend = getRoomBackend()
    const commitLane = backend.commitLane.bind(backend)
    let afterClose = 0
    vi.spyOn(backend, 'commitLane').mockImplementation((...args) => {
      if (room.isClosed && args[0] === room.id && args[2].kind === 'control') afterClose++
      return commitLane(...args)
    })
    const { demand } = spyGossip(room)
    for (const track of trackNames(100 * ROOM_GOSSIP_IN_FLIGHT_MAX))
      demand._publishWant({ __r: 'want', member: member.id, track, instance: 'elsewhere', on: true })
    await Room.close(room.id)
    await new Promise((resolve) => setTimeout(resolve, 100))
    expect(afterClose).toBe(0)
  })
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
  ] as const)('encodes every accepted %s input injectively', (name, inputs, encode) => {
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
      if (name === 'single-unit tracks') expect(decodeBinaryFrame(frame)?.track).toBe(input)
    }
  })
  it('preserves __proto__ as data and takes a binary want only for a member id', () => {
    const attrs = Object.create(null) as Record<string, unknown>
    Object.defineProperty(attrs, '__proto__', { value: { safe: true }, enumerable: true, configurable: true })
    const merged = mergeAttributes({}, attrs)
    expect(Object.getPrototypeOf(merged)).toBe(Object.prototype)
    expect(Object.hasOwn(merged, '__proto__')).toBe(true)
    expect(merged.__proto__).toEqual({ safe: true })
    Object.defineProperty(attrs, '__proto__', { value: undefined, enumerable: true, configurable: true })
    expect(Object.hasOwn(mergeAttributes(merged, attrs), '__proto__')).toBe(false)
    const member = crypto.randomUUID()
    expect(decodeBinaryWantChange({ member, track: 'screen', on: true })).toEqual({ member, track: 'screen', on: true })
    for (const id of ['__proto__', 'not-a-member-id'])
      expect(() => decodeBinaryWantChange({ member: id, track: null, on: true })).toThrow(ProtocolViolationError)
  })
  it('rejects non-record metadata and malformed UTF-8 instead of normalizing them', () => {
    const memberId = crypto.randomUUID()
    for (const meta of [[], new Date(), new Map(), new Set()].map((value) => parse(stringify(value)))) {
      expect(() => encodeBinaryFrame(memberId, new Uint8Array(), { meta: meta as never })).toThrow(
        'meta should be an object',
      )
      expect(() => decodeBinaryWantChange({ member: meta, track: null, on: true })).toThrow(ProtocolViolationError)
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
    const wantsTrack = (track: string) => decodeBinaryWantChange({ member: null, track, on: true })
    expect(() => wantsTrack(`${'é'.repeat(127)}t`)).not.toThrow()
    expect(() => wantsTrack('é'.repeat(128))).toThrow(ProtocolViolationError)
  })
})
type Peer = ReturnType<typeof attachPeer>
describe('a room nothing holds closes on its own', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })
  afterEach(() => {
    config.room = {}
  })
  const isListed = async (id: string) => (await Room.list()).some((room) => room.id === id)
  /** Watches the room from a server handle, which doesn't hold it. */
  async function observe(id: string) {
    const observer = await Room.get(id)
    const closed = vi.fn()
    observer.onClose(closed)
    return closed
  }
  /** As `Room.close()` leaves it: a tombstone, its holders told, and no longer listed. */
  async function expectClosed(id: string, closed: () => void) {
    await vi.waitFor(async () => expect((await getRoomBackend().readHead(id))?.state).toBe('closed'))
    await vi.waitFor(() => expect(closed).toHaveBeenCalledOnce())
    expect(await isListed(id)).toBe(false)
  }
  /** One member joins and leaves. */
  async function visit(room: Room) {
    await (await room.join()).leave()
  }

  it('keeps a room a page was returned open for 10 minutes, and its join then works, while one nobody holds closes at emptyTimeout', async () => {
    const held = (await Room.create('page-holds')) as ServerRoom
    const unheld = await Room.create('nobody-holds')
    const unheldClosed = await observe(unheld.id)
    const { stub } = serve(held)
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS)
    await expectClosed(unheld.id, unheldClosed)
    await vi.advanceTimersByTimeAsync(10 * 60_000 - ROOM_EMPTY_TIMEOUT_MS)
    const id = await joinThrough(stub)
    expect((await Room.getParticipants(held.id)).map((member) => member.id)).toEqual([id])
  })
  it('closes a room emptyTimeout after the page it was returned to closes, when nobody joined it', async () => {
    const room = (await Room.create('page-closed-unjoined')) as ServerRoom
    const closed = await observe(room.id)
    const { stub } = serve(room)
    await vi.advanceTimersByTimeAsync(60_000)
    stub._onPeerClose()
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS - 1_000)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(1_000)
    await expectClosed(room.id, closed)
  })
  it('closes a room departureTimeout after its last member left with its page', async () => {
    const room = (await Room.create('page-closed-joined')) as ServerRoom
    const closed = await observe(room.id)
    const { stub } = serve(room)
    await joinThrough(stub)
    await vi.advanceTimersByTimeAsync(60_000)
    stub._onPeerClose()
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS - 1_000)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(1_000)
    await expectClosed(room.id, closed)
  })
  it("restores a page's view whose first write failed at the next heartbeat", async () => {
    const room = (await Room.create('view-write-failed')) as ServerRoom
    vi.spyOn(console, 'error').mockImplementation(() => {})
    vi.spyOn(driver, 'compareExchangeCells').mockRejectedValueOnce(new Error('backend unavailable'))
    serve(room)
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS + ROOM_HEARTBEAT_INTERVAL_MS)
    expect(await isListed(room.id)).toBe(true)
  })
  it("renews the views of hundreds of pages on one server in one write, with the room's other writes alongside", async () => {
    const room = (await Room.create('many-views')) as ServerRoom
    const views = (room as unknown as { _views: Map<RoomStubChannel, Promise<void>> })._views
    for (let page = 0; page < 300; page++) await views.get(register(room))
    const writes = vi.spyOn(driver, 'compareExchangeCells')
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const tick = captureOutcome(subsOf(room)._heartbeatTick())
    const join = captureOutcome(room.join())
    await vi.advanceTimersByTimeAsync(5_000)
    const viewWrites = writes.mock.calls.filter(([, , cx]) => cx.mutations.some(({ key }) => key.startsWith('v:')))
    expect({
      tick: tick.value,
      joined: (join.value as { id?: string }).id !== undefined,
      report: report.mock.calls,
    }).toEqual({
      tick: undefined,
      joined: true,
      report: [],
    })
    expect(viewWrites.length).toBeLessThanOrEqual(2)
  })
  it("lets a page's view lapse once its server stops renewing it, as a member does, and closes the room emptyTimeout after", async () => {
    const room = (await Room.create('view-lapses')) as ServerRoom
    const closed = await observe(room.id)
    // A view the server holding the page wrote before it died, renewed by nothing since.
    await holdViews(room._incarnation, ['gone-page'])
    await vi.advanceTimersByTimeAsync(ROOM_MEMBER_TTL_MS + ROOM_HEARTBEAT_INTERVAL_MS)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS)
    await expectClosed(room.id, closed)
  })
  it("keeps a room whose creation's hold count failed to write open under its members", async () => {
    vi.spyOn(driver, 'compareExchangeCells').mockRejectedValueOnce(new Error('transient'))
    await expect(Room.create('hold-write-failed')).rejects.toThrow('transient')
    const room = await Room.getOrCreate('hold-write-failed')
    const member = await room.join()
    member.onLeave(() => {})
    await vi.advanceTimersByTimeAsync(10 * 60_000)
    expect((await getRoomBackend().readHead(room.id))?.state).toBe('open')
    expect((await Room.getParticipants(room.id)).map(({ id }) => id)).toEqual([member.id])
  })
  it("counts a member who joins from another server before the new room's first cell write, closing it only after they leave", async () => {
    const exchange = driver.compareExchangeCells.bind(driver)
    let joined: ReturnType<typeof Room.join> | undefined
    vi.spyOn(driver, 'compareExchangeCells').mockImplementation(async (roomId, inc, cx) => {
      if (joined === undefined) {
        joined = Room.join(roomId)
        await joined
      }
      return await exchange(roomId, inc, cx)
    })
    const room = await Room.create('join-before-hold', { emptyTimeout: 1_000, departureTimeout: 5_000 })
    const closed = await observe(room.id)
    const member = await joined!
    await vi.advanceTimersByTimeAsync(60_000)
    expect((await Room.getParticipants(room.id)).map(({ id }) => id)).toEqual([member.id])
    await member.leave()
    await vi.advanceTimersByTimeAsync(5_000 - 1)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(1)
    await expectClosed(room.id, closed)
  })
  it("keeps a room's lapse through a meta write", async () => {
    const room = await Room.create('meta-keeps-lapse', { emptyTimeout: 60_000 })
    const { expiresAt } = (await getRoomBackend().readHead(room.id))!
    expect(expiresAt).toBeGreaterThan(Date.now())
    await vi.advanceTimersByTimeAsync(10_000)
    await Room.setMeta(room.id, { topic: 'kept' })
    expect((await getRoomBackend().readHead(room.id))?.expiresAt).toBe(expiresAt)
  })
  it("closes a room departureTimeout after the leave that left it unheld, though another server's member left it last", async () => {
    const room = (await Room.create('last-leave-elsewhere', { departureTimeout: 60_000 })) as ServerRoom
    const closed = await observe(room.id)
    const member = await room.join()
    // Another server's member joins, and leaves 10 s after this server's; that server is gone before its timer fires.
    const count = (counts: { key: string; delta: number }[]) =>
      driver.compareExchangeCells(room.id, room._inc, { expect: [], mutations: [], counts })
    await count([
      { key: HOLDS_CELL_KEY, delta: 1 },
      { key: JOINS_CELL_KEY, delta: 1 },
    ])
    await member.leave()
    await vi.advanceTimersByTimeAsync(10_000)
    await count([{ key: HOLDS_CELL_KEY, delta: -1 }])
    // This server's heartbeat finds the room unheld, closing 60 s after that last leave.
    await vi.advanceTimersByTimeAsync(60_000 - 1)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(1)
    await expectClosed(room.id, closed)
  })
  it('waits on when its close timer fires early: another server left the room unheld later than this one armed it for', async () => {
    const room = (await Room.create('close-fires-early', { emptyTimeout: 10_000 })) as ServerRoom
    const closed = await observe(room.id)
    // Another server's writes, 4 s in: held, then unheld again, closing 4 s after this server's timer fires.
    await vi.advanceTimersByTimeAsync(4_000)
    for (const delta of [1, -1])
      await driver.compareExchangeCells(room.id, room._inc, {
        expect: [],
        mutations: [],
        counts: [{ key: HOLDS_CELL_KEY, delta }],
      })
    await vi.advanceTimersByTimeAsync(6_000)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(4_000)
    await expectClosed(room.id, closed)
  })
  it('keeps a room that never closes once joined open under a first join that lands while its new expiry is set', async () => {
    const readCells = driver.readCells.bind(driver)
    let holdReads = 0
    let joined: Promise<unknown> = Promise.resolve()
    vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
      const read = await readCells(roomId, inc, selector)
      // The first read of the hold record is the one that sets the new room's expiry: a join commits after it.
      if ('keys' in selector && selector.keys.join() === HOLD_RECORD_KEYS && ++holdReads === 1) {
        joined = Room.join(roomId)
        await joined
        await vi.advanceTimersByTimeAsync(0) // the join's own expiry write
      }
      return read
    })
    const creating = Room.create('first-join-races-expiry', { emptyTimeout: 1_000, departureTimeout: Infinity })
    await vi.advanceTimersByTimeAsync(100) // a compare-exchange the join's write raced waits out its backoff
    await creating
    const { id } = (await joined) as { id: string }
    await vi.advanceTimersByTimeAsync(60_000)
    expect((await getRoomBackend().readHead('first-join-races-expiry'))?.state).toBe('open')
    expect((await Room.getParticipants('first-join-races-expiry')).map((member) => member.id)).toEqual([id])
  })
  it('lets no hidden participant hold a room: alone it closes at emptyTimeout, after players at departureTimeout', async () => {
    const alone = await Room.create('hidden-alone')
    const aloneClosed = await observe(alone.id)
    await alone.join({ hidden: true, identity: 'authority' })
    const played = await Room.create('hidden-after-players')
    const playedClosed = await observe(played.id)
    const authority = await played.join({ hidden: true, identity: 'authority' })
    const authorityLeft = vi.fn()
    authority.onLeave(authorityLeft)
    await visit(played)
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS)
    await expectClosed(played.id, playedClosed)
    expect(authorityLeft).toHaveBeenCalledWith({ type: 'closed' })
    expect(await isListed(alone.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS - ROOM_DEPARTURE_TIMEOUT_MS)
    await expectClosed(alone.id, aloneClosed)
  })
  it('keeps a room held again before its timeout, and closes it once nothing holds it again', async () => {
    const room = await Room.create('held-again')
    const closed = await observe(room.id)
    await visit(room)
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS - 5_000)
    const back = await room.join()
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS)
    expect(await isListed(room.id)).toBe(true)
    await back.leave()
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS)
    await expectClosed(room.id, closed)
  })
  it('keeps a room whose timeout is Infinity open until Room.close()', async () => {
    const left = await Room.create('never-after-leave', { departureTimeout: Infinity })
    const unjoined = await Room.getOrCreate('never-unjoined', { emptyTimeout: Infinity })
    await visit(left)
    await vi.advanceTimersByTimeAsync(60 * 60_000)
    expect(await isListed(left.id)).toBe(true)
    expect(await isListed(unjoined.id)).toBe(true)
  })
  it("takes its timeouts from config.room, which a room's own options override", async () => {
    config.room = { departureTimeout: 1_000 }
    const byConfig = await Room.create('config-timeout')
    const byConfigClosed = await observe(byConfig.id)
    const own = await Room.create('own-timeout', { departureTimeout: 5_000 })
    const ownClosed = await observe(own.id)
    await visit(byConfig)
    await visit(own)
    await vi.advanceTimersByTimeAsync(1_000)
    await expectClosed(byConfig.id, byConfigClosed)
    expect(await isListed(own.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(4_000)
    await expectClosed(own.id, ownClosed)
  })
  it('accepts a timeout of 0 to 2^31 - 1 ms or Infinity, and refuses anything else as a usage error', async () => {
    for (const valid of [0, 2 ** 31 - 1, Infinity]) {
      await Room.create(`valid-${valid}`, { emptyTimeout: valid, departureTimeout: valid })
    }
    for (const invalid of [-1, 1.5, 2 ** 31, Number.NaN, '1000']) {
      const message = 'should be a non-negative safe integer of milliseconds, at most 2147483647'
      await expect(Room.create('invalid', { emptyTimeout: invalid as number })).rejects.toThrow(
        `\`options.emptyTimeout\` ${message}`,
      )
      await expect(Room.getOrCreate('invalid', { departureTimeout: invalid as number })).rejects.toThrow(
        `\`options.departureTimeout\` ${message}`,
      )
    }
  })
})
function attachPeer(stub: ServerChannel, lastSeq?: number, broadcast?: BroadcastSubscriptions) {
  const frames: Uint8Array[] = []
  const replay = stub._replayBuffer!
  if (lastSeq !== undefined) {
    const missed = replay.getAfter(lastSeq)
    if (missed === null) throw new Error('The replay dropped a frame the peer lacks')
    frames.push(...missed)
  }
  const peer = new IndexedPeer(
    {
      send: (frame, onCommit) => {
        frames.push(frame)
        onCommit?.()
      },
      bufferedAmount: () => 0,
    },
    7,
    replay,
  )
  stub._attachPeer(peer, { broadcast })
  return { peer, decoded: () => frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>)) }
}
const EVERY_TRACK = { member: null, track: null, on: true }
const NO_TRACK = { member: null, track: null, on: false }
/** Every read of the driver yields once, so other calls' writes land between a read and the write after it; the
 *  returned function counts the head and cell writes since. */
function yieldAfterEachReadAndCountWrites(): () => number {
  for (const method of ['readHead', 'readCells'] as const) {
    const read = (driver[method] as (...args: unknown[]) => Promise<unknown>).bind(driver)
    vi.spyOn(driver, method).mockImplementation((async (...args: unknown[]) => {
      const result = await read(...args)
      await new Promise((resolve) => setTimeout(resolve, 0))
      return result
    }) as never)
  }
  const writes = [vi.spyOn(driver, 'compareExchangeHead'), vi.spyOn(driver, 'compareExchangeCells')]
  return () => writes.reduce((total, write) => total + write.mock.calls.length, 0)
}
function subsOf(room: Room | ServerRoom): {
  _control: LaneSubscription
  _semantic: LaneSubscription
  _binary: Map<string, LaneSubscription>
  _everyTrack: Map<string, LaneSubscription>
  _inbox: Map<string, LaneSubscription>
  memberOwned(member: string): void
  memberChanged(member: string): void
  _refreshMembers(): Promise<void>
  _heartbeatTick(): Promise<void>
  replan(): Promise<void>
  binaryReady(): Promise<void>
  ensureRoster(): Promise<void>
  reconcileAuthority(): Promise<void>
} {
  return (room as unknown as { _subs: ReturnType<typeof subsOf> })._subs
}
/** The binary lanes a room subscribes, by their backend keys. */
function binaryLanes(room: ServerRoom): string[] {
  const subs = subsOf(room)
  const named = [...subs._binary.keys()].map((key) => {
    const [member, track] = key.split('\0') as [string, string]
    return encodeLaneKey({ kind: 'binary', member, track })
  })
  return [...[...subs._everyTrack.keys()].map(everyTrackLaneKey), ...named].sort()
}
/** How long `run` took, less the garbage collections during it: their pauses grow with the live heap, not with what
 *  `run` does. */
async function timeOutsideGc(run: () => Promise<void>): Promise<number> {
  const collections: PerformanceEntry[] = []
  const observer = new PerformanceObserver((entries) => void collections.push(...entries.getEntries()))
  observer.observe({ entryTypes: ['gc'] })
  const started = performance.now()
  await run()
  const ended = performance.now()
  // A collection's entry arrives after it.
  await new Promise((resolve) => setTimeout(resolve, 0))
  observer.disconnect()
  const collecting = collections
    .filter((entry) => entry.startTime >= started && entry.startTime < ended)
    .reduce((total, entry) => total + entry.duration, 0)
  return ended - started - collecting
}
function trackNames(count: number): string[] {
  return Array.from({ length: count }, (_, i) => `t${i}`)
}
/** The room's demand, and the gossip its instance publishes from now on. */
function spyGossip(room: Room | ServerRoom) {
  const demand = (room as unknown as { _demand: { _publishWant(event: unknown): void; heartbeat(): void } })._demand
  return { demand, published: vi.spyOn(demand, '_publishWant') }
}
/** The binary lanes opened on the backend from now on. */
function countBinarySubscriptions(): SubscribedLane[] {
  const subscribed: SubscribedLane[] = []
  mockLaneSubscription(
    (lane) => lane.kind === 'binary' || lane.kind === 'binary-all',
    (subscribeLane, roomId, inc, lane, receiver) => {
      subscribed.push(lane)
      return subscribeLane(roomId, inc, lane, receiver)
    },
  )
  return subscribed
}
/** The member ids a stub's wants name, as it holds them and as the room's index does. */
function wantsHeld(room: ServerRoom, stub: RoomStubChannel) {
  const held = stub as unknown as { _textMemberWants: Set<string>; _binary: { members: Map<string, unknown> } }
  const index = (
    subsOf(room) as unknown as { _wants: { _textMembers: Map<string, unknown>; _members: Map<string, unknown> } }
  )._wants
  return {
    text: [...held._textMemberWants],
    binary: [...held._binary.members.keys()],
    indexedText: [...index._textMembers.keys()],
    indexedBinary: [...index._members.keys()],
  }
}
/** Declares a binary want for every track of each id, and a text want for each, in order. */
function declareEach(stub: RoomStubChannel, ids: string[]): void {
  for (const id of ids) declare(stub, { __r: 'sub-binary', member: id, track: null, on: true })
  declare(stub, { __r: 'sub-text', announce: false, members: Object.fromEntries(ids.map((id) => [id, true])) })
}
function stubIndexOf(room: ServerRoom): Map<string, RoomStubChannel> {
  return (room as unknown as { _stubOf: Map<string, RoomStubChannel> })._stubOf
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
  const stub = new RoomStubChannel(room, { grants: new ResponseRoomGrants() })
  stub._registerChannel()
  room._attachStub(stub)
  return stub
}
function serve(room: ServerRoom): { stub: RoomStubChannel; peer: Peer } {
  const stub = register(room)
  return { stub, peer: attachPeer(stub) }
}
/** A client's join through its Room stub; returns the member id. */
async function joinThrough(stub: RoomStubChannel, meta: ParticipantMeta = {}): Promise<string> {
  return ((await stub._handleRequest({ __r: 'req-join', meta, selfDelivery: true })) as { id: string }).id
}
async function createTail(id: string) {
  await Room.create(id)
  const source = await Room.get(id)
  return {
    member: await source.join(),
    tail: (await Room.get(id, { tail: true })) as ServerRoom,
  }
}
type RelayedEvent = {
  __r: string
  id?: string
  members?: Array<{ id: string; meta: unknown }>
  meta?: unknown
  data?: unknown
  ackId?: string
}
/** Every event a Room stub relayed to its client, in order. */
function relayed(peer: Peer): RelayedEvent[] {
  return peer.decoded().flatMap((frame) => (frame.tag === TAG.PUBLISH ? [parse(frame.text) as RelayedEvent] : []))
}
/** Whether the stub relayed a roster's last part. */
function rosterEnded(peer: Peer): boolean {
  return relayed(peer).some((event) => event.__r === 'roster' && !('more' in event))
}
function memberEvents(peer: Peer, id: string): RelayedEvent[] {
  return relayed(peer).filter((event) => event.id === id)
}
/** A client Room fed every event its stub relayed, in order. */
function clientView(peer: Peer, roomId: string): ClientRoom {
  const { client, emit } = fakeClient(roomId)
  for (const frame of peer.decoded()) if (frame.tag === TAG.PUBLISH) emit(parse(frame.text), frame.info.seq)
  return client
}
/** A participant stub's notices to its client. */
function notices(peer: Peer): Array<{ __r: string }> {
  return peer.decoded().flatMap((frame) => (frame.tag === TAG.TEXT ? [parse(frame.text) as { __r: string }] : []))
}
function semanticFrames(peer: Peer, kind: 'data' | 'announce'): unknown[] {
  return relayed(peer)
    .filter((event) => event.__r === kind)
    .map((event) => event.data)
}

function delayRosterRead(roomId: string): { started: Promise<void>; release: () => void } {
  const readCellPages = driver.readCellPages.bind(driver)
  const roster = { started: createDeferred(), release: createDeferred() }
  vi.spyOn(driver, 'readCellPages').mockImplementation(async function* (candidateRoomId, inc, prefix) {
    if (candidateRoomId === roomId && prefix === MEMBER_CELL_PREFIX) {
      roster.started.resolve()
      await roster.release.promise
    }
    yield* readCellPages(candidateRoomId, inc, prefix)
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
    declare(stub, { __r: 'sub-binary', ...EVERY_TRACK })
    await subsOf(serverRoom).binaryReady()
  }
  const receipt = await camera.publishBinary(new Uint8Array([byte]), retain ? { retain: true } : undefined)
  if (retain) {
    declare(stub, { __r: 'sub-binary', ...EVERY_TRACK })
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
/** A client-held participant whose channel the test drives: `notify` delivers a stub notice. */
function standaloneParticipant(
  meta: ParticipantMeta,
  send?: () => Promise<unknown>,
  bufferLimit = getServerConfig().channel.bufferLimit,
) {
  let notify!: (notice: unknown) => unknown
  const channel = {
    listen: (cb: (notice: unknown) => unknown) => {
      notify = cb
    },
    onClose: () => {},
    send,
  } as unknown as ClientChannel
  const participant = new ClientStandaloneParticipant(channel, {
    channelId: 'channel',
    id: 'me',
    meta,
    selfDelivery: true,
    identity: null,
    bufferLimit,
  })
  return { participant, notify: (notice: unknown) => notify(notice) }
}
function newState(options: Partial<ConstructorParameters<typeof RoomState>[0]>): RoomState {
  return new RoomState({
    owner: {} as RoomStateView, // read only by the serializer
    roomId: 'state',
    meta: [],
    seed: { members: [] },
    metaSeq: 0,
    onListenersChanged: () => {},
    onCallbackError: () => {},
    onLeave: () => {},
    ...options,
  })
}
function snapshot(roomId: string): RoomSnapshotMetadata {
  return {
    channelId: 'channel',
    roomId,
    meta: [],
    closed: false,
    count: 0,
    metaSeq: 0,
    bufferLimit: getServerConfig().channel.bufferLimit,
  }
}
async function pendingClientJoin(roomId: string, onSend?: (message: unknown) => unknown) {
  const id = crypto.randomUUID()
  const ack = createDeferred<{ id: string; joinedAt: number }>()
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
      part: 0,
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
function mockLaneSubscription(
  match: SubscribedLane['kind'] | ((lane: SubscribedLane) => boolean),
  replacement: (
    subscribeLane: ReturnType<typeof getRoomBackend>['subscribeLane'],
    roomId: string,
    inc: string,
    lane: SubscribedLane,
    receiver: BackendReceiver,
  ) => BackendSubscription,
) {
  const backend = getRoomBackend()
  const subscribeLane = backend.subscribeLane.bind(backend)
  const matches = typeof match === 'function' ? match : (lane: SubscribedLane) => lane.kind === match
  vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) =>
    matches(lane)
      ? replacement(subscribeLane, roomId, inc, lane, receiver)
      : subscribeLane(roomId, inc, lane, receiver),
  )
  return backend
}
function rejectLaneSubscriptions(kind: SubscribedLane['kind'], diagnostic: string) {
  vi.useFakeTimers()
  const started = createDeferred()
  const backend = mockLaneSubscription(kind, () => {
    started.resolve()
    return rejectedSubscription(diagnostic)
  })
  vi.spyOn(console, 'error').mockImplementation(() => {})
  return { backend, started: started.promise }
}
/** Loses the frames picked on matching lanes opened from now on, as a lane that stays ready. */
function loseLaneFrames(matches: (lane: SubscribedLane) => boolean) {
  let pick: (text: string) => boolean = () => false
  mockLaneSubscription(matches, (subscribeLane, roomId, inc, lane, receiver) =>
    subscribeLane(roomId, inc, lane, (payload, info) => {
      if (!pick(decoder.decode(payload))) receiver(payload, info)
    }),
  )
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
function holdLaneDelivery(matches: (lane: SubscribedLane) => boolean) {
  const held: Array<() => void | Promise<void>> = []
  let holding = true
  mockLaneSubscription(matches, (subscribeLane, roomId, inc, lane, receiver) =>
    subscribeLane(roomId, inc, lane, (payload, info) => {
      if (!holding) return receiver(payload, info)
      held.push(() => receiver(payload, info))
    }),
  )
  return {
    async release() {
      holding = false
      for (const deliver of held.splice(0)) await deliver()
    },
  }
}
/** Matching lanes' driver attempts receive only once released, as a Redis SUBSCRIBE still in flight. */
function delayDriverLane(matches: (lane: SubscribedLane) => boolean): () => void {
  const bind = driver.subscriptions.bind.bind(driver.subscriptions)
  const releases: Array<() => void> = []
  vi.spyOn(driver.subscriptions, 'bind').mockImplementation((source) => {
    const binding = bind(source)
    if (!('lane' in source) || !matches(source.lane)) return binding
    return {
      ...binding,
      open: (receiver, localReceiverCount): SubscriptionAttempt => {
        let inner: SubscriptionAttempt | null = null
        let state: SubscriptionState = 'establishing'
        const listeners = new Set<(next: SubscriptionState) => void>()
        releases.push(() => {
          inner = binding.open(receiver, localReceiverCount)
          state = inner.state()
          for (const listener of listeners) listener(state)
        })
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
  return () => {
    for (const release of releases.splice(0)) release()
  }
}
function delayLaneSubscription(matches: (lane: SubscribedLane) => boolean) {
  const started = createDeferred()
  let release!: () => Promise<void>
  mockLaneSubscription(matches, (subscribeLane, roomId, inc, lane, receiver) => {
    let inner: BackendSubscription | null = null
    let state: SubscriptionState = 'establishing'
    const readiness = createDeferred()
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
