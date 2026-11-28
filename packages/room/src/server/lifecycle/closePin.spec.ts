// A close by id acts only on the incarnation its first read saw, on the memory store. Once that incarnation is gone, it
// resolves as a close of a room already closed does, and writes nothing to the room that took the id:
// - a close paused after its first read while the room is closed and created again, then let go;
// - a close that lost its lease to another closer, and then finds a new room under the id.
// And, apart: a room closed by two closers, one taking over the other's lapsed lease, commits two close events, and a
// room handle observes one close.
import { afterEach, expect, it, vi } from 'vitest'
import { MemoryRoomDriver } from '../../backend/MemoryRoomDriver.js'
import type {
  CommitOptions,
  CommitResult,
  CommitTarget,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
} from '../../backend/contract.js'
import { getRoomPlane } from '../../backend/plane.js'
import { Room } from '../../index.js'
import { ROOM_CLOSE_LEASE_MS } from '../constants.js'

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

/** A room's lease lapses on the memory store's clock, which is `Date`. */
function lapseTheCloseLease(): void {
  vi.useFakeTimers({ toFake: ['Date'], now: Date.now() })
  vi.setSystemTime(Date.now() + ROOM_CLOSE_LEASE_MS + 1_000)
}

/** A new room Y under `id`, with a member and meta of its own: what a close of the room before must not touch. */
async function recreated(id: string) {
  await Room.create(id, { meta: { name: 'Y' } })
  const member = await Room.join(id, { meta: { name: 'y1' } })
  const head = await getRoomPlane().direct.reads.readHead(id)
  return { inc: head?.currentInc ?? null, member: member.id }
}

/** What is under `id` once the row's close settled: Y as it was, or not. */
async function after(id: string) {
  const head = await getRoomPlane().direct.reads.readHead(id)
  const room = await Room.get(id).then(
    (got) => ({ name: got.meta.name, closed: got.isClosed }),
    (error: unknown) => String(error),
  )
  const participants = await Room.getParticipants(id).then(
    (listed) => listed.map((each) => each.id),
    (error: unknown) => String(error),
  )
  return { state: head?.state ?? 'none', inc: head?.currentInc ?? null, room, participants }
}

it('a close paused after its first read, while the room is closed and created again, leaves the new room as it is', async () => {
  const id = `close-pin-read-${crypto.randomUUID()}`
  await Room.create(id, { meta: { name: 'X' } })
  const { reads } = getRoomPlane().direct
  const readHead = reads.readHead.bind(reads)
  const firstRead = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  let held = false
  vi.spyOn(reads, 'readHead').mockImplementation(async (roomId) => {
    const head = await readHead(roomId)
    if (roomId === id && !held) {
      held = true
      firstRead.resolve()
      await release.promise
    }
    return head
  })
  const closing = Room.close(id).then(
    () => 'resolved',
    (error: unknown) => String(error),
  )
  await firstRead.promise
  await Room.close(id)
  const y = await recreated(id)
  release.resolve()
  const answer = await closing

  expect({ answer, ...(await after(id)) }).toEqual({
    answer: 'resolved',
    state: 'open',
    inc: y.inc,
    room: { name: 'Y', closed: false },
    participants: [y.member],
  })
  await Room.close(id)
})

it('a close that lost its lease to another closer, then finds a new room under the id, leaves the new room as it is', async () => {
  const id = `close-pin-lease-${crypto.randomUUID()}`
  await Room.create(id, { meta: { name: 'X' } })
  const commit = MemoryRoomDriver.prototype.commitLane
  const held = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  let holding = false
  vi.spyOn(MemoryRoomDriver.prototype, 'commitLane').mockImplementation(async function (
    this: MemoryRoomDriver,
    target: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    // The first closer's close event, which it commits under its lease, waits until its lease is another's.
    if (target.roomId === id && opts?.closingLease !== undefined && !holding) {
      holding = true
      held.resolve()
      await release.promise
    }
    return commit.call(this, target, payload, opts)
  })
  const closing = Room.close(id).then(
    () => 'resolved',
    (error: unknown) => String(error),
  )
  await held.promise
  lapseTheCloseLease()
  await Room.close(id)
  const y = await recreated(id)
  release.resolve()
  const answer = await closing

  expect({ answer, ...(await after(id)) }).toEqual({
    answer: 'resolved',
    state: 'open',
    inc: y.inc,
    room: { name: 'Y', closed: false },
    participants: [y.member],
  })
  await Room.close(id)
})

it('a room closed by two closers, one taking over the lapsed lease of the other, is observed closed once', async () => {
  const id = `close-observed-${crypto.randomUUID()}`
  const room = await Room.create(id)
  const observed: string[] = []
  room.onClose((cause) => void observed.push(cause.type))
  const commit = MemoryRoomDriver.prototype.commitLane
  let closeEvents = 0
  vi.spyOn(MemoryRoomDriver.prototype, 'commitLane').mockImplementation(async function (
    this: MemoryRoomDriver,
    target: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const committed = await commit.call(this, target, payload, opts)
    if (target.roomId === id && opts?.closingLease !== undefined && committed.kind === 'accepted') closeEvents++
    return committed
  })
  const exchangeHead = MemoryRoomDriver.prototype.compareExchangeHead
  const held = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  let holding = false
  vi.spyOn(MemoryRoomDriver.prototype, 'compareExchangeHead').mockImplementation(async function (
    this: MemoryRoomDriver,
    roomId: string,
    expected: HeadExpectation,
    next: HeadNext,
  ): Promise<HeadExchangeResult> {
    // The first closer's finalize waits until another closer took its lapsed lease and finished.
    if (roomId === id && expected.form === 'finalize' && !holding) {
      holding = true
      held.resolve()
      await release.promise
    }
    return exchangeHead.call(this, roomId, expected, next)
  })
  const first = Room.close(id)
  await held.promise
  lapseTheCloseLease()
  await Room.close(id)
  release.resolve()
  await first
  await vi.waitFor(() => expect(room.isClosed).toBe(true))

  expect({ closeEvents, observed }).toEqual({ closeEvents: 2, observed: ['closed'] })
})
