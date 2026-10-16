import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { ProtocolViolationError, decode } from '../../shared-ws.js'
import { ROOM_TRACK_WANTS_MAX } from '../constants.js'
import {
  ROOM_TAIL_HOLD_CODE_UNITS_MAX,
  ROOM_TAIL_HOLD_MAX,
  ROOM_GOSSIP_IN_FLIGHT_MAX,
  ROOM_DEMAND_PAGE_ENTRIES,
} from './constants.js'
import { decodeBinaryFrame, encodeBinaryFrame } from '../binary.js'
import { mergeAttributes } from '../model.js'
import { hasRoomTag, leaveCauseFromWire, leaveCauseToWire } from '../protocol.js'
import type { LeaveCause } from '../types.js'
import { Room } from './statics.js'
import { ServerRoom } from './room.js'
import { decodeBinaryWantChange } from './requests.js'
import { TailHold } from './tail.js'
import { getRoomBackend } from '../../backend/install.js'
import { everyTrackLaneKey } from '../../backend/room/lane-key.js'
import {
  anotherServer,
  binaryLanes,
  captureHeartbeats,
  countBinarySubscriptions,
  countDemandLane,
  declare,
  decoder,
  disposeRoomBackend,
  driver,
  installRoomBackend,
  loseLaneFrames,
  register,
  serve,
  spyGossip,
  subsOf,
  syncOf,
  tellAgain,
  trackNames,
} from '../../test/room.js'
import { longestGapWhile } from '../../test/event-loop.js'

beforeEach(() => installRoomBackend())
afterEach(disposeRoomBackend)
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
  it('opens one lane per member and none per track for a client naming ROOM_TRACK_WANTS_MAX tracks room-wide among 50 members', async () => {
    const room = (await Room.create('room-wide-track-wants')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 50 }, () => room.join()))
    const stub = register(room)
    const subscribed = countBinarySubscriptions()
    const subs = subsOf(room)
    for (const track of trackNames(ROOM_TRACK_WANTS_MAX)) {
      declare(stub, { __r: 'sub-binary', member: null, track, on: true })
    }
    await subs.binaryReady()
    expect(subs._binary.size + subs._everyMember.size).toBe(0)
    expect(binaryLanes(room)).toEqual(members.map(({ id }) => everyTrackLaneKey(id)).sort())
    expect(subscribed).toHaveLength(50)
  })
  it.each([10, 50, 100])(
    'gossips ROOM_TRACK_WANTS_MAX tracks a client names of every member once each, and each member they are fetched by once, among %i members',
    async (count) => {
      const room = (await Room.create(`every-member-gossip-${count}`)) as ServerRoom
      const members = await Promise.all(Array.from({ length: count }, () => room.join()))
      const demanded = members.map((member) => {
        const tracks = new Set<string | null>()
        member.onDemand((track, wanted) => (wanted ? tracks.add(track) : tracks.delete(track)))
        return tracks
      })
      const { report, published } = spyGossip(room)
      const stub = register(room)
      for (const track of trackNames(ROOM_TRACK_WANTS_MAX)) {
        declare(stub, { __r: 'sub-binary', member: null, track, on: true })
      }
      await vi.waitFor(() => expect(demanded.every((tracks) => tracks.size === ROOM_TRACK_WANTS_MAX)).toBe(true), {
        timeout: 10_000,
      })
      expect(published).toHaveBeenCalledTimes(ROOM_TRACK_WANTS_MAX + count)
      published.mockClear()
      tellAgain(report)
      expect(published).toHaveBeenCalledTimes(ROOM_TRACK_WANTS_MAX + count)
    },
  )
  it.each([1_000, 10_000])(
    'opens one lane per track and none per member for a client naming 64 tracks room-wide one by one among %i members, and gossips each track once',
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
      expect(subscribed).toEqual(tracks.map((track) => ({ kind: 'binary-track', track })))
      expect(published).toHaveBeenCalledTimes(tracks.length)
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
      const { gap } = await longestGapWhile(async () => {
        for (const track of trackNames(64)) declare(stub, { __r: 'sub-binary', member: null, track, on: true })
        await vi.waitFor(() => expect(demanded).toBe(64 * members.length), { timeout: 15_000 })
        stub.abort()
        await vi.waitFor(() => expect(demanded).toBe(0), { timeout: 15_000 })
      })
      await Room.close(room.id)
      return gap
    }
    // The shorter of two runs, so a scheduling hiccup in one run is not read as a stall.
    expect(Math.min(await longestGap(), await longestGap())).toBeLessThanOrEqual(100)
  }, 60_000)
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
    await syncOf(room)._heartbeatTick()
    // One renewal after another, the renewals alone take 20,000 round trips.
    expect(performance.now() - started).toBeLessThanOrEqual(2_000)
    expect(await Room.getParticipants(room.id)).toHaveLength(10_000)
  }, 60_000)
  it('tells a watched track of each of 20,000 members wanted once while each member is replanned between two slices of telling it', async () => {
    const room = (await Room.create('demand-told-whole')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 20_000 }, () => room.join()))
    const told = new Map<string, boolean[]>()
    for (const member of members) {
      member.onDemand((track, wanted) => {
        if (track === 'cam') told.set(member.id, [...(told.get(member.id) ?? []), wanted])
      })
    }
    room.subscribeBinary(() => {})
    await subsOf(room).binaryReady()
    const stubs = Array.from({ length: Math.ceil(members.length / ROOM_TRACK_WANTS_MAX) }, () => register(room))
    const demand = (room as ServerRoom)._replica.report
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
  it('keeps ROOM_GOSSIP_IN_FLIGHT_MAX demand pages in flight at most while a burst of entries fills more than that', async () => {
    const room = (await Room.create('gossip-in-flight')) as ServerRoom
    const member = await room.join()
    const backend = getRoomBackend()
    const commitLane = backend.commitLane.bind(backend)
    let pages = 0
    let inFlight = 0
    let most = 0
    vi.spyOn(backend, 'commitLane').mockImplementation((...args) => {
      const committed = commitLane(...args)
      if (args[0] !== room.id || args[2].kind !== 'demand') return committed
      pages++
      most = Math.max(most, ++inFlight)
      void committed
        .then((result) => ('stale' in result ? undefined : result.delivery))
        .catch(() => {})
        .finally(() => inFlight--)
      return committed
    })
    const { report } = spyGossip(room)
    const entries = (ROOM_GOSSIP_IN_FLIGHT_MAX + 1) * ROOM_DEMAND_PAGE_ENTRIES
    for (const track of trackNames(entries)) report['_tell']({ member: member.id, track }, true)
    await vi.waitFor(
      () => expect({ pages: pages >= ROOM_GOSSIP_IN_FLIGHT_MAX + 1, inFlight }).toEqual({ pages: true, inFlight: 0 }),
      {
        timeout: 10_000,
      },
    )
    expect(most).toBeLessThanOrEqual(ROOM_GOSSIP_IN_FLIGHT_MAX)
  })
  it.each([10, 30, 100])(
    "renews the demand of %i Room.get() handles hearing every member's mic once per server each heartbeat, and parses each demand page once",
    async (handles) => {
      const heartbeats = captureHeartbeats()
      const counted = countDemandLane(`gossip-handles-${handles}`)
      const room = (await Room.create(`gossip-handles-${handles}`)) as ServerRoom
      const members = await Promise.all(Array.from({ length: 100 }, () => room.join()))
      const hearing = members.map((member) => {
        const tracks = new Set<string | null>()
        member.onDemand((track, wanted) => (wanted ? tracks.add(track) : tracks.delete(track)))
        return tracks
      })
      for (let i = 0; i < handles; i++) {
        const view = (await Room.get(room.id)) as ServerRoom
        declare(serve(view).stub, { __r: 'sub-binary', member: null, track: 'mic', on: true })
      }
      await vi.waitFor(() => expect(hearing.every((tracks) => tracks.has('mic'))).toBe(true), { timeout: 30_000 })
      await counted.quiet()
      counted.reset()
      heartbeats.fire()
      await counted.quiet()
      // One entry for the track, whatever the number of handles and members.
      expect(counted.commits).toBe(1)
      expect(counted.parsed).toBe(counted.commits)
    },
    120_000,
  )
  it('reads no demand on a server that only views the room, while the server owning its members takes it', async () => {
    const room = (await Room.create('demand-pure-viewer')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 3 }, () => room.join()))
    const hearing = members.map((member) => {
      const tracks = new Set<string | null>()
      member.onDemand((track, wanted) => (wanted ? tracks.add(track) : tracks.delete(track)))
      return tracks
    })
    const other = await anotherServer()
    const backend = other.backend()
    const subscribeLane = backend.subscribeLane.bind(backend)
    let demandFrames = 0
    vi.spyOn(backend, 'subscribeLane').mockImplementation((id, inc, lane, receiver) =>
      subscribeLane(id, inc, lane, (payload, info) => {
        if (/"__r":"(want|covers|demand)"/.test(decoder.decode(payload))) demandFrames++
        return receiver(payload, info)
      }),
    )
    const viewer = (await other.Room.get(room.id)) as ServerRoom
    viewer.subscribeBinary(() => {}, { track: 'mic' })
    ;(await viewer.getParticipant(members[0]!.id))!.subscribeBinary(() => {}, { track: 'camera' })
    ;(await viewer.getParticipant(members[1]!.id))!.subscribeBinary(() => {})
    // The owning server hears a track of every member too, so it tells demand of its own.
    room.subscribeBinary(() => {}, { track: 'screen' })
    const heard = () => hearing.map((tracks) => [...tracks].sort())
    await vi.waitFor(() =>
      expect(heard()).toEqual([
        ['camera', 'mic', 'screen'],
        ['mic', null, 'screen'],
        ['mic', 'screen'],
      ]),
    )
    await Promise.all([viewer._replica._renewDemand(), room._replica._renewDemand()])
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(demandFrames).toBe(0)
  })
  it('tells a server that starts owning a member the demand another server told before, without a heartbeat', async () => {
    const room = (await Room.create('demand-late-owner')) as ServerRoom
    const other = await anotherServer()
    const viewer = (await other.Room.get(room.id)) as ServerRoom
    viewer.subscribeBinary(() => {}, { track: 'mic' })
    await vi.waitFor(() => expect(viewer._replica.report.reporting).toBe(true))
    await new Promise((resolve) => setTimeout(resolve, 20))
    const member = await room.join()
    const tracks = new Set<string | null>()
    member.onDemand((track, wanted) => (wanted ? tracks.add(track) : tracks.delete(track)))
    await vi.waitFor(() => expect([...tracks]).toEqual(['mic']))
  })
  it("tells a server that missed another server's demand page that demand by the other server's next heartbeat", async () => {
    const room = (await Room.create('demand-missed-page')) as ServerRoom
    let missed = false
    loseLaneFrames((lane) => lane.kind === 'demand').where((text) => !missed && (missed = text.includes('"mic"')))
    const member = await room.join()
    const tracks = new Set<string | null>()
    member.onDemand((track, wanted) => (wanted ? tracks.add(track) : tracks.delete(track)))
    const viewer = (await (await anotherServer()).Room.get(room.id)) as ServerRoom
    viewer.subscribeBinary(() => {}, { track: 'mic' })
    await vi.waitFor(() => expect(missed).toBe(true))
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect([...tracks]).toEqual([])
    await viewer._replica._renewDemand()
    await vi.waitFor(() => expect([...tracks]).toEqual(['mic']))
  })
  it("drops the demand a server heard while it owned a member once it owns none, as it can't hear that demand stop", async () => {
    const room = (await Room.create('demand-owner-again')) as ServerRoom
    const first = await room.join()
    const firstTracks = new Set<string | null>()
    first.onDemand((track, wanted) => (wanted ? firstTracks.add(track) : firstTracks.delete(track)))
    const other = await anotherServer()
    const viewer = (await other.Room.get(room.id)) as ServerRoom
    const stop = viewer.subscribeBinary(() => {}, { track: 'mic' })
    await vi.waitFor(() => expect([...firstTracks]).toEqual(['mic']))
    await first.leave()
    await vi.waitFor(() => expect(subsOf(room)._demandLane.wanted).toBe(false))
    stop()
    await vi.waitFor(() => expect(viewer._replica.report.reporting).toBe(false))
    await new Promise((resolve) => setTimeout(resolve, 20))
    const second = await room.join()
    const secondTracks = new Set<string | null>()
    second.onDemand((track, wanted) => (wanted ? secondTracks.add(track) : secondTracks.delete(track)))
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect([...secondTracks]).toEqual([])
  })
  it('commits none of the demand gossip still waiting once its room closed', async () => {
    const room = (await Room.create('gossip-after-close')) as ServerRoom
    const member = await room.join()
    const backend = getRoomBackend()
    const commitLane = backend.commitLane.bind(backend)
    let afterClose = 0
    vi.spyOn(backend, 'commitLane').mockImplementation((...args) => {
      if (room.isClosed && args[0] === room.id && args[2].kind === 'demand') afterClose++
      return commitLane(...args)
    })
    const { report } = spyGossip(room)
    for (const track of trackNames(100 * ROOM_GOSSIP_IN_FLIGHT_MAX)) report['_tell']({ member: member.id, track }, true)
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
    for (const id of ['__proto__', 'not-a-member-id']) {
      expect(() => decodeBinaryWantChange({ member: id, track: null, on: true })).toThrow(ProtocolViolationError)
    }
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
