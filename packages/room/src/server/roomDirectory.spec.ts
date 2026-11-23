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

/** The listing a create writes once its head is open, for a room without hold timeouts. */
async function listOpened(driver: MemoryRoomDriver, id: string, { inc }: { inc: string }): Promise<void> {
  await driver.directoryPut(id, inc, Infinity, 0)
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

/** Starts a sweep, past the pacing a sweep before it left (one per heartbeat interval), under faked `performance`: a
 *  heartbeat at a time until `started()` says it read its due rooms. */
async function startSweep(started: () => boolean): Promise<void> {
  for (let tries = 0; !started() && tries < 100; tries++) {
    sweepLapsedRoomsSoon()
    await turns()
    if (!started()) vi.advanceTimersByTime(ROOM_HEARTBEAT_INTERVAL_MS)
  }
  if (!started()) throw new Error('no sweep started')
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

/** Pauses `Room.create(id)`'s exchange of the head until `release` opens. */
function pauseCreates(id: string) {
  const exchange = MemoryRoomDriver.prototype.compareExchangeHead
  const paused = deferred()
  const release = deferred()
  vi.spyOn(MemoryRoomDriver.prototype, 'compareExchangeHead').mockImplementation(async function (
    this: MemoryRoomDriver,
    roomId,
    expected,
    next,
  ) {
    if (roomId === id && expected.form === 'absent') {
      paused.open()
      await release.done
    }
    return await exchange.call(this, roomId, expected, next)
  })
  return { paused, release }
}

describe("the memory store's listing", () => {
  it('lists a room created while a `Room.list()` read its listing, after both', async () => {
    const id = fresh('list-race')
    const read = MemoryRoomDriver.prototype.readHead
    const create = pauseCreates(id)
    const created = deferred()
    const listRead = deferred()
    let pauseList = false
    vi.spyOn(MemoryRoomDriver.prototype, 'readHead').mockImplementation(async function (
      this: MemoryRoomDriver,
      roomId,
    ) {
      const head = await read.call(this, roomId)
      if (roomId === id && pauseList) {
        // The list read the head before the create committed it, and acts on that read after the create resolved.
        pauseList = false
        listRead.open()
        await created.done
      }
      return head
    })
    // The create has read no head and pauses before its exchange.
    const creating = Room.create(id, noHold)
    await create.paused.done
    pauseList = true
    const listing = Room.list({ prefix: id })
    await Promise.race([listRead.done, listing])
    create.release.open()
    await creating
    created.open()
    await listing
    vi.restoreAllMocks()
    const listed = (await Room.list({ prefix: id })).map((room) => room.id)
    expect({ listed, getFinds: (await Room.get(id)).id }).toEqual({ listed: [id], getFinds: id })
  })

  it('names the winner of two creates, as the loser of their head exchange rejects', async () => {
    const id = fresh('two-creates')
    const put = MemoryRoomDriver.prototype.directoryPut
    const firstPut = deferred()
    const releaseFirst = deferred()
    let held = false
    vi.spyOn(MemoryRoomDriver.prototype, 'directoryPut').mockImplementation(async function (
      this: MemoryRoomDriver,
      roomId,
      incTag,
      checkAt,
      minDelayMs,
    ) {
      if (roomId === id && !held) {
        held = true
        firstPut.open()
        await releaseFirst.done
      }
      return await put.call(this, roomId, incTag, checkAt, minDelayMs)
    })
    // A reads no head and no listing, then pauses before it lists its proposed incarnation.
    const first = settled(Room.create(id, noHold))
    await firstPut.done
    const second = await settled(Room.create(id, noHold))
    releaseFirst.open()
    const loser = await first
    vi.restoreAllMocks()
    const winner = await currentInc(id)
    expect({ second, loser, listed: await getRoomPlane().direct.reads.directoryGet(id) }).toEqual({
      second: 'resolved',
      loser: `Room already exists: ${id}`,
      listed: winner,
    })
  })

  it('names only the winner of two timed creates that a list ran between, and only its check is due', async () => {
    const id = fresh('creates-and-list')
    vi.useFakeTimers({ toFake: ['Date'] })
    const timed = { emptyTimeout: 60_000, departureTimeout: 60_000 }
    const create = pauseCreates(id)
    const first = settled(Room.create(id, timed))
    await create.paused.done
    const second = settled(Room.create(id, timed))
    // Both have written their checks and wait at their exchange.
    await new Promise((resolve) => setImmediate(resolve))
    const between = (await Room.list({ prefix: id })).map((room) => room.id)
    create.release.open()
    const outcomes = [await first, await second].sort()
    vi.restoreAllMocks()
    const winner = await currentInc(id)
    const listed = (await Room.list({ prefix: id })).map((room) => room.id)
    vi.setSystemTime(Date.now() + ROOM_LEASE_TTL_MS + 1_000)
    const due = (await getRoomPlane().direct.reads.directoryDue()).filter((entry) => entry.roomId === id)
    expect({ between, outcomes, listed, due }).toEqual({
      between: [],
      outcomes: [`Room already exists: ${id}`, 'resolved'],
      listed: [id],
      due: [{ roomId: id, incTag: winner }],
    })
  })

  it('pages past rooms that lapse before it reads and between its pages, each page full of live ids', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const prefix = fresh('lapsing-pages')
    const ids = Array.from({ length: DIRECTORY_PAGE_SIZE + 5 }, (_, at) => `${prefix}:${String(at).padStart(4, '0')}`)
    // Two rooms in the first page's range lapse in 5 s, the last two in 10 s, the others never.
    const early = new Set([ids[10], ids[11]])
    const late = new Set([ids[DIRECTORY_PAGE_SIZE + 3], ids[DIRECTORY_PAGE_SIZE + 4]])
    for (const id of ids) {
      await listOpened(
        driver,
        id,
        await openHead(driver, id, early.has(id) ? 5_000 : late.has(id) ? 10_000 : undefined),
      )
    }
    // A first list builds the index; each lapse then comes with no timer run.
    await driver.directoryList(prefix)
    vi.setSystemTime(Date.now() + 6_000)
    const first = await driver.directoryList(`${prefix}:`)
    vi.setSystemTime(Date.now() + 5_000)
    // A get that comes first past a lapse finds it too.
    const lateGet = await driver.directoryGet(ids[DIRECTORY_PAGE_SIZE + 3] ?? '')
    const second = await driver.directoryList(`${prefix}:`, first.cursor)
    const live = ids.filter((id) => !early.has(id) && !late.has(id))
    expect({
      first: first.entries.map((entry) => entry.roomId),
      cursor: first.cursor,
      second: second.entries.map((entry) => entry.roomId),
      more: second.cursor,
      lapsedGet: await driver.directoryGet(`${prefix}:0010`),
      lateGet,
    }).toEqual({
      first: live.slice(0, DIRECTORY_PAGE_SIZE),
      cursor: live[DIRECTORY_PAGE_SIZE - 1],
      second: live.slice(DIRECTORY_PAGE_SIZE),
      more: undefined,
      lapsedGet: undefined,
      lateGet: undefined,
    })
  })

  it('pages in code point order, not UTF-16 order', async () => {
    const driver = new MemoryRoomDriver()
    const prefix = fresh('code-points')
    // U+FF61 sorts before U+1F600 by code point; its UTF-16 unit 0xFF61 sorts after 0xD83D.
    const ids = [`${prefix}:a\u{1F600}`, `${prefix}:a\u{FF61}`]
    for (const id of ids) await listOpened(driver, id, await openHead(driver, id))
    const page = await driver.directoryList(`${prefix}:`)
    expect(page.entries.map((entry) => entry.roomId)).toEqual([`${prefix}:a\u{FF61}`, `${prefix}:a\u{1F600}`])
  })

  it("keeps a successor listed through the repair a list makes from its page's earlier read", async () => {
    const driver = new MemoryRoomDriver()
    const id = fresh('stale-page')
    const first = await openHead(driver, id)
    await listOpened(driver, id, first)
    const page = await driver.directoryList(id)
    await closeHead(driver, id, first)
    const tombstone = await driver.readHead(id)
    if (tombstone === null) throw new Error(`room ${id} has no tombstone`)
    const second = await openHead(driver, id, undefined, tombstone.rev)
    await listOpened(driver, id, second)
    // The core's repair from its page's entry: drop the listed incarnation, re-list or delete under its tag.
    await driver.dropIncarnation(id, first.inc)
    await driver.directoryPut(id, first.inc, 0, ROOM_LEASE_TTL_MS)
    await driver.directoryDelete(id, first.inc)
    expect({
      listedFirst: page.entries.map((entry) => entry.incTag),
      get: await driver.directoryGet(id),
      listed: (await driver.directoryList(id)).entries.map((entry) => entry.incTag),
      data: (await driver.readLaneOrder(id, second.inc, SEMANTIC))?.seq,
    }).toEqual({ listedFirst: [first.inc], get: second.inc, listed: [second.inc], data: 1 })
  })
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
      listed: await getRoomPlane().direct.reads.directoryGet(id),
    }).toEqual({ state: 'closed', data: false, listed: undefined })
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
  /** Closes room `id` with the store refusing its close's `step` once: the closed frame's commit, or, the frame
   *  committed, the head's finalize. */
  async function stoppedClose(id: string, step: 'commit' | 'finalize'): Promise<string> {
    if (step === 'commit') {
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
    } else {
      const exchange = MemoryRoomDriver.prototype.compareExchangeHead
      vi.spyOn(MemoryRoomDriver.prototype, 'compareExchangeHead').mockImplementation(async function (
        this: MemoryRoomDriver,
        roomId,
        expected,
        next,
      ) {
        if (roomId === id && expected.form === 'finalize') throw new Error('refused before dispatch')
        return await exchange.call(this, roomId, expected, next)
      })
    }
    const outcome = await settled(Room.close(id))
    vi.restoreAllMocks()
    return outcome
  }

  /** Sweeps once the close lease ran out, with no call about the room, until its head no longer names `inc`. */
  async function sweepPastLease(id: string, inc: string): Promise<void> {
    vi.useFakeTimers({ toFake: ['Date', 'performance'] })
    vi.advanceTimersByTime(ROOM_CLOSE_LEASE_MS + 1_000)
    const due = MemoryRoomDriver.prototype.directoryDue
    let dueReads = 0
    vi.spyOn(MemoryRoomDriver.prototype, 'directoryDue').mockImplementation(async function (this: MemoryRoomDriver) {
      dueReads++
      return await due.call(this)
    })
    await startSweep(() => dueReads > 0)
    for (let tries = 0; (await currentInc(id)) === inc && tries < 100; tries++) await turns()
    vi.restoreAllMocks()
  }

  for (const [label, options] of [
    ['a room without hold timeouts', noHold],
    ['a timed room', { emptyTimeout: 60_000, departureTimeout: 60_000 }],
  ] as const) {
    it(`is closed by a sweep once its close lease ran out, with no call about it: ${label}`, async () => {
      const id = fresh('stopped-closer')
      await Room.create(id, options)
      const inc = await currentInc(id)
      if (inc === null) throw new Error(`room ${id} has no head`)
      const close = await stoppedClose(id, 'commit')
      const { reads } = getRoomPlane().direct
      const whileLeased = (await reads.readHead(id))?.state
      await sweepPastLease(id, inc)
      const head = await reads.readHead(id)
      expect({
        close,
        whileLeased,
        state: head?.state,
        endedInc: head === null ? null : configFromHead(head).inc,
        data: await holdsData(id, inc),
        listed: (await reads.directoryList(id)).entries.some((entry) => entry.roomId === id),
      }).toEqual({
        close: 'refused before dispatch',
        whileLeased: 'closing',
        state: 'closed',
        endedInc: inc,
        data: false,
        listed: false,
      })
    })
  }

  it("closes a view once, though the sweep's takeover commits a second closed frame after the stopped closer's", async () => {
    const id = fresh('stopped-finalize')
    await Room.create(id, noHold)
    const inc = await currentInc(id)
    if (inc === null) throw new Error(`room ${id} has no head`)
    const view = await Room.get(id)
    const closes: string[] = []
    view.onClose((cause) => void closes.push(cause.type))
    const close = await stoppedClose(id, 'finalize')
    await turns()
    const afterClose = [...closes]
    await sweepPastLease(id, inc)
    await turns()
    expect({ close, afterClose, closes, state: (await getRoomPlane().direct.reads.readHead(id))?.state }).toEqual({
      close: 'refused before dispatch',
      afterClose: ['closed'],
      closes: ['closed'],
      state: 'closed',
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

describe("a room's due check", () => {
  it('of a timed create whose opening reply was refused is set by its head, which lists it', async () => {
    const id = fresh('reply-refused')
    vi.useFakeTimers({ toFake: ['Date'] })
    const exchange = MemoryRoomDriver.prototype.compareExchangeHead
    vi.spyOn(MemoryRoomDriver.prototype, 'compareExchangeHead').mockImplementation(async function (
      this: MemoryRoomDriver,
      roomId,
      expected,
      next,
    ) {
      const result = await exchange.call(this, roomId, expected, next)
      if (roomId === id && expected.form === 'absent') throw new Error('the reply was refused')
      return result
    })
    const created = await settled(Room.create(id, { emptyTimeout: 60_000, departureTimeout: 60_000 }))
    vi.restoreAllMocks()
    const { reads } = getRoomPlane().direct
    const listed = await reads.directoryGet(id)
    const inc = await currentInc(id)
    vi.setSystemTime(Date.now() + ROOM_LEASE_TTL_MS + 1_000)
    const due = (await reads.directoryDue()).filter((entry) => entry.roomId === id)
    expect({ created, listed: listed === inc, due }).toEqual({
      created: 'the reply was refused',
      listed: true,
      due: [{ roomId: id, incTag: inc }],
    })
  })

  for (const sweep of ['before', 'across'] as const) {
    it(`of a timed create stays, though a sweep deletes its pre-put ${sweep} its opening turn, and its reply and its close's commit are refused`, async () => {
      const id = fresh(`swept-${sweep}`)
      vi.useFakeTimers({ toFake: ['Date', 'performance'] })
      const exchange = MemoryRoomDriver.prototype.compareExchangeHead
      const atExchange = deferred()
      const releaseExchange = deferred()
      vi.spyOn(MemoryRoomDriver.prototype, 'compareExchangeHead').mockImplementation(async function (
        this: MemoryRoomDriver,
        roomId,
        expected,
        next,
      ) {
        if (roomId !== id || expected.form !== 'absent') return await exchange.call(this, roomId, expected, next)
        atExchange.open()
        await releaseExchange.done
        await exchange.call(this, roomId, expected, next)
        throw new Error('the reply was refused')
      })
      const drop = MemoryRoomDriver.prototype.dropIncarnation
      const atDrop = deferred()
      let dropping = false
      const releaseDrop = deferred()
      vi.spyOn(MemoryRoomDriver.prototype, 'dropIncarnation').mockImplementation(async function (
        this: MemoryRoomDriver,
        roomId,
        inc,
      ) {
        if (roomId === id && sweep === 'across') {
          dropping = true
          atDrop.open()
          await releaseDrop.done
        }
        return await drop.call(this, roomId, inc)
      })
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
      // The sweep's due reads, bounded: from its third on it is told nothing is due.
      const due = MemoryRoomDriver.prototype.directoryDue
      let dueReads = 0
      const swept = deferred()
      vi.spyOn(MemoryRoomDriver.prototype, 'directoryDue').mockImplementation(async function (this: MemoryRoomDriver) {
        if (++dueReads > 2) {
          swept.open()
          return []
        }
        const entries = await due.call(this)
        if (entries.length === 0) swept.open()
        return entries
      })
      const created = settled(Room.create(id, { emptyTimeout: 60_000, departureTimeout: 60_000 }))
      await atExchange.done
      // A lease on, the sweep finds the pre-put with no head and repairs it: drop, then delete.
      vi.advanceTimersByTime(ROOM_LEASE_TTL_MS + 1_000)
      await startSweep(() => dueReads > 0)
      if (sweep === 'across') {
        await Promise.race([atDrop.done, swept.done])
        if (!dropping) throw new Error('the sweep repaired nothing')
        releaseExchange.open()
        await created
        releaseDrop.open()
      }
      await swept.done
      releaseExchange.open()
      const reply = await created
      const closed = await settled(Room.close(id))
      vi.restoreAllMocks()
      const { reads } = getRoomPlane().direct
      const state = (await reads.readHead(id))?.state
      vi.advanceTimersByTime(70_000)
      expect({
        reply,
        closed: closed !== 'resolved',
        state,
        due: (await reads.directoryDue()).some((entry) => entry.roomId === id),
      }).toEqual({ reply: 'the reply was refused', closed: true, state: 'closing', due: true })
    })
  }

  it('handed out goes with the head that ends before the sweep reads it, and leaves no node', async () => {
    const driver = new MemoryRoomDriver()
    const id = fresh('due-then-closed')
    const opened = await openHead(driver, id)
    await driver.directoryPut(id, opened.inc, 0, 0)
    const handed = await driver.directoryDue()
    await closeHead(driver, id, opened)
    expect({
      handed: handed.map((entry) => entry.roomId),
      after: (await driver.directoryDue()).map((entry) => entry.roomId),
      nodes: driver['dueChecks'].size,
    }).toEqual({ handed: [id], after: [], nodes: 0 })
  })

  it('moves only by a put of the tag its live head names, however late the put', async () => {
    useStoreClock()
    const driver = new MemoryRoomDriver()
    const id = fresh('late-puts')
    const opened = await openHead(driver, id, 10_000)
    const openLapse = Date.now() + 10_000
    // The closing turn keeps the check; a put that holds the open head's lapse moves it, and it stays finite.
    const closing = await driver.compareExchangeHead(
      id,
      { form: 'rev', rev: opened.rev },
      {
        head: {
          rev: crypto.randomUUID(),
          currentInc: opened.inc,
          state: 'closing',
          config: CONFIG,
          closeLease: { id: 'lease', durationMs: ROOM_CLOSE_LEASE_MS },
        },
      },
    )
    if (closing.kind !== 'committed') throw new Error(`room ${id} didn't start closing`)
    await driver.directoryPut(id, opened.inc, openLapse, ROOM_HEARTBEAT_INTERVAL_MS)
    vi.setSystemTime(Date.now() + ROOM_HEARTBEAT_INTERVAL_MS + 1_000)
    const whileClosing = (await driver.directoryDue()).map((entry) => entry.incTag)
    await driver.compareExchangeHead(
      id,
      { form: 'finalize', rev: closing.head.rev, lease: 'lease' },
      {
        head: { rev: crypto.randomUUID(), currentInc: null, state: 'closed', config: CONFIG },
        ttlMs: ROOM_TOMBSTONE_TTL_MS,
      },
    )
    const tombstone = await driver.readHead(id)
    if (tombstone === null) throw new Error(`room ${id} has no tombstone`)
    const afterClose = { get: await driver.directoryGet(id), nodes: driver['dueChecks'].size }
    const successor = await openHead(driver, id, undefined, tombstone.rev)
    await driver.directoryPut(id, successor.inc, Date.now() + 1_000, 0)
    // Late puts of the ended tag: neither its check nor its listing comes back, and the successor's check stays.
    await driver.directoryPut(id, opened.inc, Infinity, 0)
    await driver.directoryPut(id, opened.inc, 0, 0)
    vi.setSystemTime(Date.now() + 2_000)
    expect({
      whileClosing,
      afterClose,
      get: await driver.directoryGet(id),
      due: (await driver.directoryDue()).map((entry) => entry.incTag),
      nodes: driver['dueChecks'].size,
    }).toEqual({
      whileClosing: [opened.inc],
      afterClose: { get: undefined, nodes: 0 },
      get: successor.inc,
      due: [successor.inc],
      nodes: 1,
    })
  })
})
