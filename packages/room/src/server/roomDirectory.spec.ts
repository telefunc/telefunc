// What the memory store lists, and when an ended room's data goes: the room's head decides both, in its own turn.
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { BackendReceiver, DriverAttempt } from 'telefunc/__internal'
import { getRoomPlane } from '../backend/plane.js'
import { MemoryRoomDriver } from '../backend/MemoryRoomDriver.js'
import { DIRECTORY_PAGE_SIZE, type DirectoryPage, type HeadNext, type LaneId } from '../backend/contract.js'
import { Room } from '../index.js'
import { ROOM_CLOSE_LEASE_MS, ROOM_LEASE_TTL_MS, ROOM_TOMBSTONE_TTL_MS } from './constants.js'
import { configFromHead } from './lanes/lanes.js'
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

/** A ready subscription to `lane` of incarnation `inc`, and what it saw since, in order: each frame's seq, then its
 *  states. */
async function readyLane(driver: MemoryRoomDriver, id: string, inc: string, lane: LaneId) {
  const seen: string[] = []
  const receiver: BackendReceiver = { deliver: (_payload, info) => void seen.push(`frame ${info.seq}`) }
  const attempt: DriverAttempt = driver.subscribe({ roomId: id, inc, lane }, receiver)
  attempt.onStateChange((state) => void seen.push(state))
  await expect.poll(() => attempt.state()).toBe('ready')
  seen.length = 0
  return { attempt, seen }
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
