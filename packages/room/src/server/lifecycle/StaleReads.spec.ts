import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { replicaParts } from '../../test/replicaParts.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from '../constants.js'
import { LanePlan } from '../lanes/LanePlan.js'
import { StaleReads } from './StaleReads.js'

/** The stale reads of room `lobby`'s view, whose lanes follow it while `lanePlan.observing` is set. */
function staleReads() {
  const { model, lanePlan: plan } = replicaParts({ id: 'lobby', inc: crypto.randomUUID(), hold: null })
  const lanePlan = { observing: false }
  vi.spyOn(LanePlan.prototype, 'observing', 'get').mockImplementation(() => lanePlan.observing)
  return { model, lanePlan, reads: new StaleReads({ roomId: 'lobby', model, lanePlan: plan }) }
}

describe('StaleReads', () => {
  let warn: ReturnType<typeof vi.spyOn>
  beforeEach(() => {
    vi.useFakeTimers()
    warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  })
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('warns once, naming the room, at a read a heartbeat past when it was last current', () => {
    const { reads } = staleReads()
    vi.advanceTimersByTime(ROOM_HEARTBEAT_INTERVAL_MS)
    reads.read()
    expect(warn).not.toHaveBeenCalled()
    vi.advanceTimersByTime(1)
    reads.read()
    reads.read()
    expect(warn).toHaveBeenCalledOnce()
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining("Room 'lobby' was read 30 s after it was last current on this server"),
    )
  })

  it('stays quiet while followed, on a closed room, and after a Room.get() caught it up', () => {
    const { reads, model, lanePlan } = staleReads()
    vi.advanceTimersByTime(2 * ROOM_HEARTBEAT_INTERVAL_MS)
    lanePlan.observing = true
    reads.read()
    lanePlan.observing = false
    model.closed = true
    reads.read()
    model.closed = false
    reads.caughtUp()
    reads.read()
    expect(warn).not.toHaveBeenCalled()
  })

  it("counts from a follow's end", () => {
    const { reads, lanePlan } = staleReads()
    lanePlan.observing = true
    reads.lanesChanged()
    vi.advanceTimersByTime(2 * ROOM_HEARTBEAT_INTERVAL_MS)
    lanePlan.observing = false
    reads.lanesChanged()
    vi.advanceTimersByTime(ROOM_HEARTBEAT_INTERVAL_MS)
    reads.read()
    expect(warn).not.toHaveBeenCalled()
    vi.advanceTimersByTime(1)
    reads.read()
    expect(warn).toHaveBeenCalledOnce()
  })
})
