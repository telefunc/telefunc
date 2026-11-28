import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getRoomPlane } from '../../backend/plane.js'
import { replicaParts } from '../../test/replicaParts.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from '../constants.js'
import { LanePlan } from '../lanes/LanePlan.js'
import { encodeRoomRecord } from '../lanes/lanes.js'
import type { RoomIncarnation } from '../protocol.js'
import { RoomLease, holdViews, renewLease } from '../store/lease.js'
import { AuthoritySync } from './AuthoritySync.js'
import { newRoomHold } from './hold.js'
import { RoomHeartbeat } from './RoomHeartbeat.js'
import { RoomHosting } from './RoomHosting.js'
import { RosterStream } from './RosterStream.js'

/** A room in the memory store that a page's view holds, and the heartbeat of the server whose lease holds it. */
async function heldRoom() {
  const room: RoomIncarnation = {
    id: `lobby-${crypto.randomUUID()}`,
    inc: crypto.randomUUID(),
    hold: { emptyTimeout: 20_000, departureTimeout: 20_000 },
  }
  const plane = getRoomPlane()
  const config = encodeRoomRecord({ meta: {}, inc: room.inc, hold: room.hold })
  await plane.direct.compareExchangeHead(
    room.id,
    { form: 'absent' },
    { head: { rev: crypto.randomUUID(), currentInc: room.inc, state: 'open', config }, ...newRoomHold(room.hold) },
  )
  const lease = new RoomLease()
  await holdViews(plane.direct, room, lease, ['page'])
  const { model, lanePlan, outgoingDemand, demandExchange, authority, hosting } = replicaParts(room)
  // The view's lanes follow the room, and its hosting renews the lease that holds the page's view.
  vi.spyOn(LanePlan.prototype, 'observing', 'get').mockReturnValue(true)
  let renewedAt = 0
  const maintain = vi.spyOn(RoomHosting.prototype, 'maintain').mockImplementation(async () => {
    renewedAt = Date.now()
    await renewLease(plane.direct, room, lease)
  })
  vi.spyOn(AuthoritySync.prototype, 'reconcileMeta').mockImplementation(() => plane.direct.reads.readHead(room.id))
  const checkCoverage = vi.spyOn(AuthoritySync.prototype, 'checkCoverage').mockResolvedValue()
  const heartbeat = new RoomHeartbeat({
    hosting,
    incarnation: room,
    model,
    lanePlan,
    outgoingDemand,
    demandExchange,
    authority,
  })
  heartbeat.update()
  const stop = () => {
    model.closed = true
    heartbeat.update()
  }
  const lapse = async () => (await plane.direct.reads.readHead(room.id))?.expiresAt ?? 0
  return { room, maintain, checkCoverage, stop, lapse, renewedAt: () => renewedAt }
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
    vi.restoreAllMocks()
  })

  it("renews the lease and the room's lapse while a coverage check doesn't end", async () => {
    const { maintain, checkCoverage, stop, lapse, renewedAt } = await heldRoom()
    checkCoverage.mockReturnValue(new Promise(() => {}))
    await tickUntil(() => expect(checkCoverage).toHaveBeenCalledOnce())
    await tickUntil(() => expect(maintain).toHaveBeenCalledTimes(2))
    // The lease's 120 s, the room's 20 s timeout and two heartbeats past the tick.
    await vi.waitFor(async () => expect(await lapse()).toBeGreaterThanOrEqual(renewedAt() + 200_000))
    expect(checkCoverage).toHaveBeenCalledOnce()
    stop()
  })

  it('reports a coverage check that rejects once, and checks again at a later tick', async () => {
    const { checkCoverage, stop } = await heldRoom()
    checkCoverage.mockRejectedValueOnce(new Error('coverage read failed')).mockResolvedValue(undefined)
    await tickUntil(() => expect(checkCoverage).toHaveBeenCalledTimes(2))
    expect(error).toHaveBeenCalledOnce()
    expect(error).toHaveBeenCalledWith(expect.stringContaining('coverage read failed'))
    stop()
  })

  it('leaves a view as it closed when the room closes during a coverage check', async () => {
    const { room, stop, checkCoverage } = await heldRoom()
    // This test's own sync, which checks coverage itself; its lanes hold the control subscription.
    checkCoverage.mockRestore()
    const applyControl = vi.fn()
    const { model, authority } = replicaParts(room, applyControl)
    vi.spyOn(LanePlan.prototype, 'controlEstablished', 'get').mockReturnValue(true)
    vi.spyOn(LanePlan.prototype, 'replan').mockResolvedValue()
    vi.spyOn(RosterStream.prototype, 'reconcile').mockResolvedValue(true)
    authority.controlDelivered({ seq: 1, timestamp: 0 })
    // The close dropped the room's lanes, and its closed frame reaches the view while the check reads the control lane.
    await getRoomPlane().direct.dropIncarnation(room.id, room.inc)
    const check = authority.checkCoverage()
    model.closed = true
    await check
    expect(applyControl).not.toHaveBeenCalled()
    stop()
  })
})
