import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ROOM_DEMAND_TTL_MS, ROOM_HEARTBEAT_INTERVAL_MS } from './constants.js'
import { Room } from './statics.js'
import { ServerRoom } from './room.js'
import { decodeDemandPage, decodeRoomText } from './lanes.js'
import { RoomDemand } from './demand.js'
import { getRoomBackend } from '../../backend/install.js'
import {
  anotherServer,
  applyPage,
  disposeRoomBackend,
  expire,
  installRoomBackend,
  leasesHeld,
  trackNames,
} from '../../test/room.js'

beforeEach(() => installRoomBackend())
afterEach(disposeRoomBackend)
describe('room demand lifecycle', () => {
  it("aggregates remote demand only for the members this instance owns, and forgets a departed member's", () => {
    const owned = new Set(['member'])
    const delivered: Array<[string, string, boolean]> = []
    const demand = new RoomDemand({
      ownsMember: (id) => owned.has(id),
      ownedMembers: () => owned,
      deliver: (member, track, wanted) => delivered.push([member, track, wanted]),
      ask: () => {},
    })
    applyPage(demand, 'remote-a', [{ on: true, member: 'elsewhere', track: 'screen' }])
    expect(leasesHeld(demand)).toBe(0)
    applyPage(demand, 'remote-a', [{ on: true, member: 'member', track: 'screen' }])
    owned.delete('member')
    demand.forgetMember('member')
    applyPage(demand, 'remote-b', [{ on: true, member: 'member', track: 'screen' }])
    expect(delivered).toEqual([['member', 'screen', true]])
    expect(leasesHeld(demand)).toBe(0)
  })
  it('drops the demand of a server that crashed once its lease lapses unrenewed', async () => {
    const other = await anotherServer()
    vi.useFakeTimers()
    const room = await Room.create('crashed-reporter')
    const publisher = await room.join()
    const demand: Array<[string | null, boolean]> = []
    publisher.onDemand((track, wanted) => demand.push([track, wanted]))
    ;(await (await other.Room.get(room.id)).getParticipant(publisher.id))!.subscribeBinary(() => {})
    await vi.waitFor(() => expect(demand).toEqual([[null, true]]))
    await other.crash()
    // Its last renewal was at most a heartbeat before it crashed.
    await vi.advanceTimersByTimeAsync(ROOM_DEMAND_TTL_MS - ROOM_HEARTBEAT_INTERVAL_MS - 1)
    expect(demand).toEqual([[null, true]])
    await vi.advanceTimersByTimeAsync(2 * ROOM_HEARTBEAT_INTERVAL_MS)
    expect(demand).toEqual([
      [null, true],
      [null, false],
    ])
  })
  it("drops a reporter's demand once its lease lapses unrenewed, as when its instance crashed", () => {
    vi.useFakeTimers()
    const delivered: Array<[string, string, boolean]> = []
    const demand = new RoomDemand({
      ownsMember: () => true,
      ownedMembers: () => ['member'],
      deliver: (member, track, wanted) => delivered.push([member, track, wanted]),
      ask: () => {},
    })
    applyPage(demand, 'crashed', [{ on: true, member: 'member', track: 'screen' }])
    vi.advanceTimersByTime(ROOM_DEMAND_TTL_MS - 1)
    expire(demand)
    expect(leasesHeld(demand)).toBeGreaterThan(0)
    vi.advanceTimersByTime(1)
    expire(demand)
    expect(delivered).toEqual([
      ['member', 'screen', true],
      ['member', 'screen', false],
    ])
    expect(leasesHeld(demand)).toBe(0)
  })
  it("wants a reporter's track of every member of each owned member but those it receives it from none of, until its lease lapses", () => {
    vi.useFakeTimers()
    const owned = ['a', 'b', 'c']
    const delivered: Array<[string, string, boolean]> = []
    const demand = new RoomDemand({
      ownsMember: (id) => owned.includes(id),
      ownedMembers: () => owned,
      deliver: (member, track, wanted) => delivered.push([member, track, wanted]),
      ask: () => {},
    })
    applyPage(demand, 'crashed', [{ on: true, track: 'screen', except: ['c'] }])
    expect(delivered).toEqual([
      ['a', 'screen', true],
      ['b', 'screen', true],
    ])
    expect(demand.wanted('a')).toEqual(['screen'])
    applyPage(demand, 'other', [{ on: true, track: 'screen', except: ['a', 'b'] }])
    vi.advanceTimersByTime(ROOM_DEMAND_TTL_MS - 1)
    // Renewed, now receiving it from every member but `a`.
    applyPage(demand, 'other', [{ on: true, track: 'screen', except: ['a'] }])
    expire(demand)
    vi.advanceTimersByTime(1)
    expire(demand)
    expect(delivered).toEqual([
      ['a', 'screen', true],
      ['b', 'screen', true],
      ['c', 'screen', true],
      ['a', 'screen', false],
    ])
    expect(leasesHeld(demand)).toBeGreaterThan(0)
    vi.advanceTimersByTime(ROOM_DEMAND_TTL_MS)
    expire(demand)
    expect(leasesHeld(demand)).toBe(0)
    expect(delivered.slice(4)).toEqual([
      ['b', 'screen', false],
      ['c', 'screen', false],
    ])
  })
  it("evaluates for a change of a reporter's lease of a track of every member only the members another reporter's lease leaves out, among 1,000 owned members", async () => {
    const owned = Array.from({ length: 1_000 }, (_, i) => `m${i}`)
    let evaluated = 0
    const delivered: Array<[string, string, boolean]> = []
    const demand = new RoomDemand({
      ownsMember: () => (evaluated++, true),
      ownedMembers: () => owned,
      deliver: (member, track, wanted) => delivered.push([member, track, wanted]),
      ask: () => {},
    })
    const tracks = trackNames(100)
    const tell = (instance: string, on: boolean, except: string[] = []) =>
      applyPage(
        demand,
        instance,
        tracks.map((track) => ({ on, track, except })),
      )
    tell('a', true)
    await vi.waitFor(() => expect(delivered).toHaveLength(100_000))
    evaluated = 0
    delivered.length = 0
    tell('b', true, ['m0'])
    tell('a', true, ['m1'])
    tell('a', false)
    await vi.waitFor(() => expect(delivered).toEqual(tracks.map((track) => ['m0', track, false])))
    // At most one member a track per change.
    expect(evaluated).toBeLessThanOrEqual(3 * tracks.length)
  })
  it('gossips each member a track of every member is not received from once, however many were before it', async () => {
    const room = (await Room.create('except-each-once')) as ServerRoom
    room.subscribeBinary(() => {}, { track: 'mic' })
    const backend = getRoomBackend()
    const commitLane = backend.commitLane.bind(backend)
    let excepted = 0
    vi.spyOn(backend, 'commitLane').mockImplementation((...args) => {
      if (args[2].kind === 'demand') {
        for (const want of decodeDemandPage(decodeRoomText(args[3])).wants) {
          if ('except' in want) excepted += Array.isArray(want.except) ? want.except.length : 1
        }
      }
      return commitLane(...args)
    })
    // Each joins unheard by the handle's own listener.
    for (let i = 0; i < 200; i++) await room.join({ selfDelivery: false })
    await room._replica.plan.replan()
    await vi.waitFor(() => expect(room._replica.report.changed).toBe(false))
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(excepted).toBe(200)
  })
})
