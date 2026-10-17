import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
import { Room } from './statics.js'
import { ServerRoom } from './room.js'
import { decodeRoomText } from './lanes.js'
import { getRoomBackend } from '../../backend/install.js'
import { MemoryBackendState } from '../../backend/memory/backend.js'
import {
  anotherServer,
  captureHeartbeats,
  disposeRoomBackend,
  driver,
  installRoomBackend,
  syncOf,
} from '../../test/room.js'

beforeEach(() => installRoomBackend())
afterEach(disposeRoomBackend)
describe("a server's session in a room", () => {
  it('lets each member of a server that crashed holding 10,000 across two rooms leave once, within three minutes', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    const crashing = await anotherServer()
    const watching = await anotherServer()
    const leaves = new Map<string, number>()
    const joined: string[] = []
    for (const id of ['crash-a', 'crash-b']) {
      const room = (await Room.create(id)) as ServerRoom
      const held = await crashing.Room.get(id)
      joined.push(...(await Promise.all(Array.from({ length: 5_000 }, () => held.join()))).map(({ id }) => id))
      // Two servers watch the room, each heartbeat of each able to claim the crashed server's session.
      for (const view of [room, await watching.Room.get(id)]) {
        view.onLeave(() => {})
        await view.getParticipants()
      }
      getRoomBackend().subscribeLane(id, room._incarnation.inc, { kind: 'control' }, (payload) => {
        const event = parse(decodeRoomText(payload as Uint8Array)) as { __r: string; id?: string }
        if (event.__r === 'leave') leaves.set(event.id!, (leaves.get(event.id!) ?? 0) + 1)
      })
    }
    await crashing.crash()
    await vi.advanceTimersByTimeAsync(180_000)
    expect([...new Set(leaves.values())]).toEqual([1])
    expect(joined.filter((id) => !leaves.has(id))).toEqual([])
  }, 120_000)
  it('writes the store once per server in an idle heartbeat, whatever its members', async () => {
    const heartbeats = captureHeartbeats()
    const other = await anotherServer()
    const room = (await Room.create('idle-writes')) as ServerRoom
    const views = [room, await other.Room.get(room.id)]
    for (const view of views) {
      await Promise.all(Array.from({ length: 1_000 }, () => view.join()))
      view.onLeave(() => {})
    }
    const writes = [driver, other.driver].map((store) => ({
      cells: vi.spyOn(store, 'compareExchangeCells'),
      head: vi.spyOn(store, 'compareExchangeHead'),
      lanes: vi.spyOn(store, 'commitLane'),
    }))
    const calls = () => writes.flatMap((spies) => Object.values(spies).map((spy) => spy.mock.calls.length))
    /** No store write for five timer turns in a row. */
    const quiet = async () => {
      for (let still = 0, last = ''; still < 5; ) {
        await new Promise((resolve) => setTimeout(resolve, 20))
        const now = calls().join()
        still = now === last ? still + 1 : 0
        last = now
      }
    }
    await quiet()
    for (const spies of writes) for (const spy of Object.values(spies)) spy.mockClear()
    heartbeats.fire()
    await quiet()
    const cellWrites = writes.map(({ cells }) =>
      cells.mock.calls.flatMap(([, , { mutations }]) => mutations.map(({ key }) => key.split(':')[0])),
    )
    expect(cellWrites).toEqual([['session'], ['session']])
    expect(writes.map(({ head, lanes }) => head.mock.calls.length + lanes.mock.calls.length)).toEqual([0, 0])
  }, 60_000)
  it("evicts nobody when the app servers' clocks run 120 seconds ahead of the store's", async () => {
    const storeNow = Date.now
    await installRoomBackend(new MemoryBackendState({ now: () => storeNow() }))
    const other = await anotherServer()
    const room = (await Room.create('skewed-clocks')) as ServerRoom
    const members = await Promise.all(Array.from({ length: 100 }, () => room.join()))
    const observer = (await other.Room.get(room.id)) as ServerRoom
    const left: string[] = []
    observer.onLeave((member) => left.push(member.id))
    await observer.getParticipants()
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(storeNow() + 120_000)
    await syncOf(observer)._heartbeatTick()
    await syncOf(room)._heartbeatTick()
    expect(left).toEqual([])
    expect((await Room.getParticipants(room.id)).map(({ id }) => id).sort()).toEqual(members.map(({ id }) => id).sort())
  })
})
