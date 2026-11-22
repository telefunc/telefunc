// What the memory store lists, and when an ended room's data goes: the room's head decides both, in its own turn.
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { BackendReceiver, DriverAttempt } from 'telefunc/__internal'
import { getRoomPlane } from '../backend/plane.js'
import { MemoryRoomDriver } from '../backend/MemoryRoomDriver.js'
import { DIRECTORY_PAGE_SIZE, type DirectoryPage, type HeadNext, type LaneId } from '../backend/contract.js'
import { Room } from '../index.js'
import { TIMER_DELAY_MAX_MS } from '../shared/constants.js'
import {
  ROOM_CLOSE_LEASE_MS,
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_LEASE_TTL_MS,
  ROOM_TOMBSTONE_TTL_MS,
} from './constants.js'
import { configFromHead, encodeRoomRecord } from './lanes/lanes.js'
import { ServerRoom, handleOf } from './room.js'
import { sweepLapsedRoomsSoon } from './store/directory.js'

afterEach(() => {
  vi.restoreAllMocks()
  vi.useRealTimers()
})

const noHold = { emptyTimeout: Infinity, departureTimeout: Infinity }
const SEMANTIC: LaneId = { kind: 'semantic' }
const CONTROL: LaneId = { kind: 'control' }
const CONFIG = new Uint8Array([7])
const fresh = (what: string) => `${what}-${crypto.randomUUID()}`
/** Fakes the store's own timers with its clock; its delivery turns stay real. */
const useStoreClock = () => vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] })

function deferred() {
  let open = () => {}
  const done = new Promise<void>((resolve) => {
    open = resolve
  })
  return { open, done }
}

async function settled(work: Promise<unknown>): Promise<string> {
  try {
    await work
    return 'resolved'
  } catch (error: unknown) {
    return error instanceof Error ? error.message : String(error)
  }
}

/** The current incarnation of room `id`'s head, if it names one. */
async function currentInc(id: string): Promise<string | null> {
  return (await getRoomPlane().direct.reads.readHead(id))?.currentInc ?? null
}

/** Whether the installed store still holds incarnation `inc` of room `id`: its semantic lane's order, which goes with
 *  it. */
async function holdsData(id: string, inc: string): Promise<boolean> {
  return (await getRoomPlane().direct.reads.readLaneOrder(id, inc, SEMANTIC)) !== null
}

/** An open head for room `id` on `driver`, lapsing `ttlMs` from now, with a commit on its semantic lane. */
async function openHead(driver: MemoryRoomDriver, id: string, ttlMs?: number, rev?: string) {
  const inc = crypto.randomUUID()
  const next: HeadNext = { head: { rev: crypto.randomUUID(), currentInc: inc, state: 'open', config: CONFIG } }
  if (ttlMs !== undefined) next.ttlMs = ttlMs
  const result = await driver.compareExchangeHead(
    id,
    rev === undefined ? { form: 'absent' } : { form: 'rev', rev },
    next,
  )
  if (result.kind !== 'committed') throw new Error(`the head of ${id} was not written`)
  await driver.commitLane({ roomId: id, inc, lane: SEMANTIC }, new Uint8Array([1]))
  return { inc, rev: result.head.rev }
}

/** A close at the store alone: the closing lease, the closed frame, the closed tombstone. With `sameTurn`, the
 *  tombstone is written in the turn the frame is accepted, before the frame is delivered. */
async function closeHead(
  driver: MemoryRoomDriver,
  id: string,
  { inc, rev }: { inc: string; rev: string },
  { sameTurn = false } = {},
) {
  const lease = crypto.randomUUID()
  const closing = await driver.compareExchangeHead(
    id,
    { form: 'rev', rev },
    {
      head: {
        rev: crypto.randomUUID(),
        currentInc: inc,
        state: 'closing',
        config: CONFIG,
        closeLease: { id: lease, durationMs: ROOM_CLOSE_LEASE_MS },
      },
    },
  )
  if (closing.kind !== 'committed') throw new Error(`room ${id} didn't start closing`)
  const committing = driver.commitLane({ roomId: id, inc, lane: CONTROL }, new Uint8Array([9]), { closingLease: lease })
  const finalize = () =>
    driver.compareExchangeHead(
      id,
      { form: 'finalize', rev: closing.head.rev, lease },
      {
        head: { rev: crypto.randomUUID(), currentInc: null, state: 'closed', config: CONFIG },
        ttlMs: ROOM_TOMBSTONE_TTL_MS,
      },
    )
  const finalizing = sameTurn ? finalize() : null
  const frame = await committing
  if (frame.kind !== 'accepted') throw new Error(`room ${id}'s closed frame was refused`)
  const closed = await (finalizing ?? finalize())
  if (closed.kind !== 'committed') throw new Error(`room ${id} didn't close`)
  return { delivery: frame.delivery }
}

/** What the store's turns queued so far has run: its deliveries and its lanes' ends. */
async function turns(): Promise<void> {
  for (let turn = 0; turn < 3; turn++) await new Promise((resolve) => setImmediate(resolve))
}

/** A ready subscription to `lane` of incarnation `inc`, and what it saw since, in order: each frame's seq, then its
 *  states. */
async function readyLane(driver: MemoryRoomDriver, id: string, inc: string, lane: LaneId) {
  const seen: string[] = []
  const receiver: BackendReceiver = { deliver: (_payload, info) => void seen.push(`frame ${info.seq}`) }
  const attempt: DriverAttempt = driver.subscribe({ roomId: id, inc, lane }, receiver)
  attempt.onStateChange((state) => void seen.push(state))
  await turns()
  if (attempt.state() !== 'ready') throw new Error(`the lane of ${id} isn't ready`)
  seen.length = 0
  return { attempt, seen }
}

/** The timers armed and cleared from here on, under the faked store clock. */
function timerCalls() {
  const armed = vi.spyOn(globalThis, 'setTimeout')
  const cleared = vi.spyOn(globalThis, 'clearTimeout')
  return {
    /** Each armed timer's delay, in order. */
    delays: () => armed.mock.calls.map((call) => call[1]),
    cleared: () => cleared.mock.calls.length,
    /** Runs the `at`th armed timer's callback now, by hand, as a host that runs it late or early would. */
    runArmed(at: number) {
      const callback = armed.mock.calls[at]?.[0]
      if (typeof callback !== 'function') throw new Error(`no timer ${at} was armed`)
      callback()
    },
    handle: (at: number) => armed.mock.results[at]?.value,
    clearedHandles: () => cleared.mock.calls.map((call) => call[0]),
  }
}

describe("the memory store's listing", () => {
  it('pages rooms in code point order under a prefix, past a room closed between two pages', async () => {
    const prefix = fresh('paged')
    const ids = Array.from({ length: DIRECTORY_PAGE_SIZE + 3 }, (_, at) => `${prefix}:${String(at).padStart(4, '0')}`)
    for (const id of ids) await Room.create(id, noHold)
    await Room.create(`${prefix}~outside`, noHold)
    const { reads } = getRoomPlane().direct
    const first = await reads.directoryList(`${prefix}:`)
    if (first.cursor === undefined) throw new Error('Expected a second page')
    await Room.close(first.cursor)
    const second = await reads.directoryList(`${prefix}:`, first.cursor)
    const listed = [...first.entries, ...second.entries].map((entry) => entry.roomId)
    expect({ listed, more: second.cursor }).toEqual({ listed: ids, more: undefined })
  })
})

describe("an ended room's data", () => {
  it('goes with its close, though the call that drops it is refused', async () => {
    const id = fresh('close-drop-refused')
    await Room.create(id, noHold)
    const inc = await currentInc(id)
    if (inc === null) throw new Error(`room ${id} has no head`)
    await getRoomPlane().direct.commitLane({ roomId: id, inc, lane: SEMANTIC }, new Uint8Array([1]))
    vi.spyOn(MemoryRoomDriver.prototype, 'dropIncarnation').mockRejectedValue(new Error('refused before dispatch'))
    await settled(Room.close(id))
    vi.restoreAllMocks()
    expect({
      state: (await getRoomPlane().direct.reads.readHead(id))?.state,
      data: await holdsData(id, inc),
    }).toEqual({ state: 'closed', data: false })
  })

  it('of an earlier incarnation goes in the turn the head names a new one', async () => {
    const driver = new MemoryRoomDriver()
    const id = fresh('replaced')
    const first = await openHead(driver, id, 10_000)
    const second = await openHead(driver, id, 60_000, first.rev)
    expect({
      first: await driver.readLaneOrder(id, first.inc, SEMANTIC),
      second: (await driver.readLaneOrder(id, second.inc, SEMANTIC))?.seq,
    }).toEqual({ first: null, second: 1 })
  })

  it("leaves no room record past its tombstone's lapse, after a close and no later call", async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('tombstone')
    await closeHead(driver, id, await openHead(driver, id))
    const atClose = driver['rooms'].has(id)
    await vi.advanceTimersByTimeAsync(ROOM_TOMBSTONE_TTL_MS + 1_000)
    expect({ atClose, pastTombstone: driver['rooms'].has(id) }).toEqual({ atClose: true, pastTombstone: false })
  })
})

describe('an incarnation its head names', () => {
  it('keeps its data and its lanes through a drop that acted on an earlier read', async () => {
    const driver = new MemoryRoomDriver()
    const id = fresh('named-drop')
    const opened = await openHead(driver, id)
    const { attempt } = await readyLane(driver, id, opened.inc, SEMANTIC)
    // A repair that read no head yet, before the create's exchange, drops what it would have found unnamed.
    await driver.dropIncarnation(id, opened.inc)
    await new Promise((resolve) => setImmediate(resolve))
    expect({
      data: (await driver.readLaneOrder(id, opened.inc, SEMANTIC))?.seq,
      lane: attempt.state(),
    }).toEqual({ data: 1, lane: 'ready' })
  })
})

describe("an ended incarnation's lanes", () => {
  it('end after the close frame, with its delivery settled first, though the close ends in the turn it was accepted', async () => {
    const driver = new MemoryRoomDriver()
    const id = fresh('finalized-lane')
    const opened = await openHead(driver, id)
    const { attempt, seen } = await readyLane(driver, id, opened.inc, CONTROL)
    const { delivery } = await closeHead(driver, id, opened, { sameTurn: true })
    await delivery
    await expect.poll(() => attempt.state()).toBe('closed')
    expect(seen).toEqual(['frame 1', 'closed'])
  })
})

describe('a closing head whose closer stopped', () => {
  it('stays closing, due past its open lapse and a lease after each sweep, until a later close finishes it and its data goes', async () => {
    const id = fresh('stopped-closer')
    await Room.create(id, { emptyTimeout: 60_000, departureTimeout: 60_000 })
    const inc = await currentInc(id)
    if (inc === null) throw new Error(`room ${id} has no head`)
    const commit = MemoryRoomDriver.prototype.commitLane
    vi.spyOn(MemoryRoomDriver.prototype, 'commitLane').mockImplementation(async function (
      this: MemoryRoomDriver,
      target,
      payload,
      opts,
    ) {
      if (target.roomId === id && target.lane.kind === 'control') throw new Error('refused before dispatch')
      return await commit.call(this, target, payload, opts)
    })
    await settled(Room.close(id))
    vi.restoreAllMocks()
    const { reads } = getRoomPlane().direct
    const names = (entries: DirectoryPage['entries']) => entries.some((entry) => entry.roomId === id)
    // The clock the sweep's own pacing reads moves too, so its next sweep may start.
    vi.useFakeTimers({ toFake: ['Date', 'performance'] })
    vi.advanceTimersByTime(70_000)
    const pastLapse = names(await reads.directoryDue())
    // The sweep's due reads as the store answers them. From its third on it is told nothing is due, so it ends.
    const sweepReads: DirectoryPage['entries'][] = []
    const ended = deferred()
    const due = MemoryRoomDriver.prototype.directoryDue
    vi.spyOn(MemoryRoomDriver.prototype, 'directoryDue').mockImplementation(async function (this: MemoryRoomDriver) {
      if (sweepReads.length === 2) {
        ended.open()
        return []
      }
      const entries = await due.call(this)
      sweepReads.push(entries)
      if (entries.length === 0) ended.open()
      return entries
    })
    sweepLapsedRoomsSoon()
    await ended.done
    vi.restoreAllMocks()
    const [visited = [], afterVisit = []] = sweepReads
    vi.advanceTimersByTime(ROOM_LEASE_TTL_MS + 1_000)
    const aLeaseLater = names(await reads.directoryDue())
    const stillClosing = (await reads.readHead(id))?.state
    // Its close lease ran out long ago, so a close takes it over at once.
    const closed = await settled(Room.close(id))
    const head = await reads.readHead(id)
    expect({
      pastLapse,
      visited: names(visited),
      afterVisit: names(afterVisit),
      aLeaseLater,
      stillClosing,
      closed,
      state: head?.state,
      endedInc: head === null ? null : configFromHead(head).inc,
      data: await holdsData(id, inc),
    }).toEqual({
      pastLapse: true,
      visited: true,
      afterVisit: false,
      aLeaseLater: true,
      stillClosing: 'closing',
      closed: 'resolved',
      state: 'closed',
      endedInc: inc,
      data: false,
    })
  })
})

describe("a head's lapse", () => {
  it('removes the room with its data on its timer, with no read', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('timer-first')
    const timers = timerCalls()
    const opened = await openHead(driver, id, 10_000)
    const { attempt } = await readyLane(driver, id, opened.inc, SEMANTIC)
    await vi.advanceTimersByTimeAsync(9_999)
    const before = attempt.state()
    await vi.advanceTimersByTimeAsync(1)
    await turns()
    expect({ delays: timers.delays(), before, after: attempt.state() }).toEqual({
      delays: [10_000],
      before: 'ready',
      after: 'closed',
    })
  })

  it('removes the room at the first read past it, for good, and its timer then does nothing', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('read-first')
    const timers = timerCalls()
    const opened = await openHead(driver, id, 10_000)
    const { attempt } = await readyLane(driver, id, opened.inc, SEMANTIC)
    // The host's clock steps forward; no timer runs.
    vi.setSystemTime(Date.now() + 11_000)
    const read = await driver.readHead(id)
    await turns()
    const lane = attempt.state()
    timers.runArmed(0)
    // And back: what the store saw lapse stays gone.
    vi.setSystemTime(Date.now() - 6_000)
    expect({
      read,
      lane,
      armedAfter: timers.delays(),
      head: await driver.readHead(id),
      data: await driver.readLaneOrder(id, opened.inc, SEMANTIC),
    }).toEqual({ read: null, lane: 'closed', armedAfter: [10_000], head: null, data: null })
  })

  it('leaves nothing to a history read that comes first past it', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('history-first')
    const opened = await openHead(driver, id, 10_000)
    await driver.commitLane({ roomId: id, inc: opened.inc, lane: SEMANTIC }, new Uint8Array([2]), { retain: true })
    vi.setSystemTime(Date.now() + 11_000)
    expect({
      order: await driver.readLaneOrder(id, opened.inc, SEMANTIC),
      retained: await driver.readRetained(id, opened.inc, SEMANTIC),
      listed: await driver.listRetained(id, opened.inc),
    }).toEqual({ order: null, retained: null, listed: [] })
  })

  it('removes the room when its timer runs after a forward step of the clock, before its own delay', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('stepped-timer')
    const timers = timerCalls()
    const opened = await openHead(driver, id, 60_000)
    const { attempt } = await readyLane(driver, id, opened.inc, SEMANTIC)
    vi.setSystemTime(Date.now() + 61_000)
    timers.runArmed(0)
    await turns()
    expect({ lane: attempt.state(), data: await driver.readLaneOrder(id, opened.inc, SEMANTIC) }).toEqual({
      lane: 'closed',
      data: null,
    })
  })

  it('keeps the room when its timer runs after a backward step, and goes at the lapse', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('stepped-back')
    const timers = timerCalls()
    const opened = await openHead(driver, id, 10_000)
    const { attempt } = await readyLane(driver, id, opened.inc, SEMANTIC)
    vi.setSystemTime(Date.now() - 5_000)
    await vi.advanceTimersByTimeAsync(10_000)
    await turns()
    const atTimer = { lane: attempt.state(), delays: timers.delays() }
    await vi.advanceTimersByTimeAsync(5_000)
    await turns()
    expect({ atTimer, atLapse: attempt.state() }).toEqual({
      atTimer: { lane: 'ready', delays: [10_000, 5_000] },
      atLapse: 'closed',
    })
  })

  it('is pushed out by a renewal that makes no timer call, and the timer re-arms for the rest', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('renewed')
    const timers = timerCalls()
    const opened = await openHead(driver, id, 10_000)
    const { attempt } = await readyLane(driver, id, opened.inc, SEMANTIC)
    await vi.advanceTimersByTimeAsync(8_000)
    const renewed = await driver.compareExchangeHead(
      id,
      { form: 'rev', rev: opened.rev },
      { head: { rev: crypto.randomUUID(), currentInc: opened.inc, state: 'open', config: CONFIG }, ttlMs: 10_000 },
    )
    const byRenewal = { armed: timers.delays().length, cleared: timers.cleared() }
    await vi.advanceTimersByTimeAsync(2_000)
    await turns()
    const atOldLapse = attempt.state()
    await vi.advanceTimersByTimeAsync(8_000)
    await turns()
    expect({
      renewed: renewed.kind,
      byRenewal,
      atOldLapse,
      delays: timers.delays(),
      atNewLapse: attempt.state(),
    }).toEqual({
      renewed: 'committed',
      byRenewal: { armed: 1, cleared: 0 },
      atOldLapse: 'ready',
      delays: [10_000, 8_000],
      atNewLapse: 'closed',
    })
  })

  it('of a replaced head, run by hand, leaves its successor at an equal deadline to its own', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('equal-deadline')
    const timers = timerCalls()
    const first = await openHead(driver, id, 10_000)
    const second = await openHead(driver, id, 10_000, first.rev)
    const { attempt } = await readyLane(driver, id, second.inc, SEMANTIC)
    const byReplacement = { armed: timers.delays().length, cleared: timers.cleared() }
    await vi.advanceTimersByTimeAsync(5_000)
    timers.runArmed(0)
    await turns()
    const early = {
      lane: attempt.state(),
      data: (await driver.readLaneOrder(id, second.inc, SEMANTIC))?.seq,
      delays: timers.delays(),
    }
    await vi.advanceTimersByTimeAsync(5_000)
    await turns()
    expect({ byReplacement, early, atDeadline: attempt.state() }).toEqual({
      byReplacement: { armed: 1, cleared: 0 },
      early: { lane: 'ready', data: 1, delays: [10_000, 5_000] },
      atDeadline: 'closed',
    })
  })

  it('of a head replaced by one lapsing sooner, run by hand, arms nothing', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('earlier-deadline')
    const timers = timerCalls()
    const first = await openHead(driver, id, 60_000)
    const second = await openHead(driver, id, 10_000, first.rev)
    const { attempt } = await readyLane(driver, id, second.inc, SEMANTIC)
    const byReplacement = { delays: timers.delays(), cleared: timers.clearedHandles() }
    timers.runArmed(0)
    const afterOld = { delays: timers.delays().length, lane: attempt.state() }
    await vi.advanceTimersByTimeAsync(10_000)
    await turns()
    expect({ byReplacement, afterOld, atDeadline: attempt.state() }).toEqual({
      byReplacement: { delays: [60_000, 10_000], cleared: [timers.handle(0)] },
      afterOld: { delays: 2, lane: 'ready' },
      atDeadline: 'closed',
    })
  })

  it("past one timer's range arms that range, then the rest, and goes at the lapse, not before", async () => {
    useStoreClock()
    const warnings: string[] = []
    const onWarning = (warning: Error) => warnings.push(warning.name)
    process.on('warning', onWarning)
    try {
      const driver = new MemoryRoomDriver()
      const id = fresh('long-lapse')
      const timers = timerCalls()
      const opened = await openHead(driver, id, TIMER_DELAY_MAX_MS + 5_000)
      const { attempt } = await readyLane(driver, id, opened.inc, SEMANTIC)
      await vi.advanceTimersByTimeAsync(TIMER_DELAY_MAX_MS)
      await turns()
      const atRange = attempt.state()
      await vi.advanceTimersByTimeAsync(5_000)
      await turns()
      expect({ delays: timers.delays(), atRange, atLapse: attempt.state(), warnings }).toEqual({
        delays: [TIMER_DELAY_MAX_MS, 5_000],
        atRange: 'ready',
        atLapse: 'closed',
        warnings: [],
      })
    } finally {
      process.off('warning', onWarning)
    }
  })

  it('arms, re-arms or clears the timer only as the lapse appears, moves earlier or goes', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('phases')
    const timers = timerCalls()
    let { inc, rev } = await openHead(driver, id, 3_605_000)
    const calls: { armed: (number | undefined)[]; cleared: number }[] = []
    const exchange = async (ttlMs: number | undefined) => {
      const armed = timers.delays().length
      const cleared = timers.cleared()
      const next: HeadNext = { head: { rev: crypto.randomUUID(), currentInc: inc, state: 'open', config: CONFIG } }
      if (ttlMs !== undefined) next.ttlMs = ttlMs
      const result = await driver.compareExchangeHead(id, { form: 'rev', rev }, next)
      if (result.kind !== 'committed') throw new Error(`the head of ${id} was not written`)
      rev = result.head.rev
      calls.push({ armed: timers.delays().slice(armed), cleared: timers.cleared() - cleared })
    }
    // A first join under a one-second departure timeout: the held horizon is sooner than the empty one.
    await exchange(ROOM_LEASE_TTL_MS + 1_000 + 2 * ROOM_HEARTBEAT_INTERVAL_MS)
    // An infinite departure timeout once joined: no lapse.
    await exchange(undefined)
    // A finite one again, with none armed.
    await exchange(181_000)
    // The same phase, pushed out.
    await exchange(200_000)
    expect(calls).toEqual([
      { armed: [181_000], cleared: 1 },
      { armed: [], cleared: 1 },
      { armed: [181_000], cleared: 0 },
      { armed: [], cleared: 0 },
    ])
  })

  it('of a room a participant first joins under a short departure timeout moves its timer earlier', async () => {
    useStoreClock()
    const timers = timerCalls()
    const id = fresh('first-join')
    const room = await Room.create(id, { emptyTimeout: 3_600_000, departureTimeout: 1_000 })
    const emptyLapse = timers.delays().indexOf(3_605_000)
    await room.join({ meta: {} })
    const heldHorizon = ROOM_LEASE_TTL_MS + 1_000 + 2 * ROOM_HEARTBEAT_INTERVAL_MS
    for (let turn = 0; turn < 50 && !timers.delays().includes(heldHorizon); turn++) await turns()
    expect({
      emptyArmed: emptyLapse !== -1,
      heldArmed: timers.delays().includes(heldHorizon),
      emptyCleared: timers.clearedHandles().includes(timers.handle(emptyLapse)),
    }).toEqual({ emptyArmed: true, heldArmed: true, emptyCleared: true })
    await Room.close(id)
  })
})

describe("a lapsed incarnation's lanes", () => {
  it('end after what was accepted on them, which is delivered with its delivery settled', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('lapsed-lane')
    const opened = await openHead(driver, id, 10_000)
    const { seen } = await readyLane(driver, id, opened.inc, CONTROL)
    const committing = driver.commitLane({ roomId: id, inc: opened.inc, lane: CONTROL }, new Uint8Array([3]))
    // In the same turn: past the lapse, and read.
    vi.setSystemTime(Date.now() + 11_000)
    const read = driver.readHead(id)
    const accepted = await committing
    if (accepted.kind !== 'accepted') throw new Error('the commit before the lapse was refused')
    await accepted.delivery
    await turns()
    const after = await driver.commitLane({ roomId: id, inc: opened.inc, lane: CONTROL }, new Uint8Array([4]))
    expect({ read: await read, seen, after: after.kind }).toEqual({
      read: null,
      seen: ['frame 1', 'closed'],
      after: 'stale-incarnation',
    })
  })

  it("end a tail-only view's, which is torn down", async () => {
    useStoreClock()
    const id = fresh('tail-only')
    const { direct } = getRoomPlane()
    const inc = crypto.randomUUID()
    const result = await direct.compareExchangeHead(
      id,
      { form: 'absent' },
      {
        head: {
          rev: crypto.randomUUID(),
          currentInc: inc,
          state: 'open',
          config: encodeRoomRecord({ meta: {}, inc, hold: null }),
        },
        ttlMs: 10_000,
      },
    )
    if (result.kind !== 'committed') throw new Error(`the head of ${id} was not written`)
    const room = await Room.get(id, { tail: true })
    if (!(room instanceof ServerRoom)) throw new Error('Room.get() gave no server room')
    const handle = handleOf(room)
    const tailed = handle.holdsTail()
    await vi.advanceTimersByTimeAsync(10_000)
    for (let turn = 0; turn < 50 && handle.holdsTail(); turn++) await turns()
    expect({ tailed, afterLapse: handle.holdsTail() }).toEqual({ tailed: true, afterLapse: false })
  })
})
