import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getRoomPlane } from '../../backend/plane.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from '../constants.js'
import { encodeRoomRecord } from '../lanes/lanes.js'
import type { RoomIncarnation } from '../protocol.js'
import { RoomLease, holdViews, renewLease } from '../store/lease.js'
import { AuthoritySync } from './AuthoritySync.js'
import { newRoomHold } from './hold.js'
import { RoomHeartbeat } from './RoomHeartbeat.js'

/** A room in the memory store that a page's view holds, and the heartbeat of the server whose lease holds it. */
async function heldRoom() {
  const room: RoomIncarnation = {
    id: `lobby-${crypto.randomUUID()}`,
    inc: crypto.randomUUID(),
    hold: { emptyTimeout: 20_000, departureTimeout: 20_000 },
  }
  const plane = getRoomPlane()
  const config = encodeRoomRecord({ meta: {}, inc: room.inc, hold: room.hold })
  await plane.compareExchangeHead(
    room.id,
    { form: 'absent' },
    { head: { currentInc: room.inc, state: 'open', config }, ...newRoomHold(room.hold) },
  )
  const lease = new RoomLease()
  await holdViews(room, lease, ['page'])
  const model = { closed: false }
  let renewedAt = 0
  const hosting = {
    maintain: vi.fn(async () => {
      renewedAt = Date.now()
      await renewLease(room, lease)
    }),
  }
  const authority = {
    reconcileMeta: () => plane.driver.readHead(room.id),
    checkCoverage: vi.fn<() => Promise<void>>(),
  }
  const heartbeat = new RoomHeartbeat({
    hosting,
    incarnation: room,
    model,
    lanePlan: { observing: true, ownsInbox: false },
    outgoingDemand: { reporting: false },
    demandExchange: { renew: async () => {} },
    authority,
  })
  heartbeat.update()
  const stop = () => {
    model.closed = true
    heartbeat.update()
  }
  const lapse = async () => (await plane.driver.readHead(room.id))?.expiresAt ?? 0
  return { room, hosting, authority, stop, lapse, renewedAt: () => renewedAt }
}

/** Runs heartbeats until `ran` holds, as a tick still running skips the next. */
async function tickUntil(ran: () => void): Promise<void> {
  await vi.waitFor(async () => {
    await vi.advanceTimersByTimeAsync(ROOM_HEARTBEAT_INTERVAL_MS)
    ran()
  })
}

describe('RoomHeartbeat', () => {
  let error: ReturnType<typeof vi.spyOn>
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'Date'] })
    error = vi.spyOn(console, 'error').mockImplementation(() => {})
  })
  afterEach(() => {
    vi.useRealTimers()
    error.mockRestore()
  })

  it("renews the lease and the room's lapse while a coverage check doesn't end", async () => {
    const { hosting, authority, stop, lapse, renewedAt } = await heldRoom()
    authority.checkCoverage.mockReturnValue(new Promise(() => {}))
    await tickUntil(() => expect(authority.checkCoverage).toHaveBeenCalledOnce())
    await tickUntil(() => expect(hosting.maintain).toHaveBeenCalledTimes(2))
    // The lease's 120 s, the room's 20 s timeout and two heartbeats past the tick.
    await vi.waitFor(async () => expect(await lapse()).toBeGreaterThanOrEqual(renewedAt() + 200_000))
    expect(authority.checkCoverage).toHaveBeenCalledOnce()
    stop()
  })

  it('reports a coverage check that rejects once, and checks again at a later tick', async () => {
    const { authority, stop } = await heldRoom()
    authority.checkCoverage.mockRejectedValueOnce(new Error('coverage read failed')).mockResolvedValue(undefined)
    await tickUntil(() => expect(authority.checkCoverage).toHaveBeenCalledTimes(2))
    expect(error).toHaveBeenCalledOnce()
    expect(error).toHaveBeenCalledWith(expect.stringContaining('coverage read failed'))
    stop()
  })

  it('leaves a view as it closed when the room closes during a coverage check', async () => {
    const { room, stop } = await heldRoom()
    const model = {
      closed: false,
      metaWaiting: false,
      metaSeq: 0,
      rosterKnown: true,
      memberIds: () => [].values(),
      recordMembership: () => [],
      stopRecordingMembership: () => {},
    }
    const owner = { applyControl: vi.fn() }
    const authority = new AuthoritySync({
      owner,
      incarnation: room,
      roster: { reconcile: async () => true },
      model,
      lanePlan: { replan: async () => {}, observing: true, controlEstablished: true },
    })
    authority.controlDelivered(1)
    // The close dropped the room's lanes, and its closed frame reaches the view while the check reads the control lane.
    await getRoomPlane().driver.dropIncarnation(room.id, room.inc)
    const check = authority.checkCoverage()
    model.closed = true
    await check
    expect(owner.applyControl).not.toHaveBeenCalled()
    stop()
  })
})
