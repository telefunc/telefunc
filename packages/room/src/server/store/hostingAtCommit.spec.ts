// A message reaches a participant only while its lease hosts it at the commit that delivers it, in the store's time:
// whatever a lookup or a server's view saw before that commit.
import { afterEach, describe, expect, it, vi } from 'vitest'
import { getRoomPlane } from '../../backend/plane.js'
import { MemoryRoomDriver } from '../../backend/MemoryRoomDriver.js'
import { BackendError } from 'telefunc/__internal'
import type { OperationOptions, OperationResult, RoomOperation } from '../../backend/contract.js'
import { Room } from '../../index.js'
import { configFromHead } from '../lanes/lanes.js'
import type { RoomIncarnation } from '../protocol.js'
import { ROOM_LEASE_TTL_MS } from '../constants.js'
import { LEASE_CELL_PREFIX } from './cells.js'
import { RENEW_LEASE } from './lease.js'
import { runOperation } from './operations.js'

afterEach(() => vi.restoreAllMocks())

async function incarnationOf(id: string): Promise<RoomIncarnation> {
  const head = await getRoomPlane().direct.reads.readHead(id)
  if (head === null) throw new Error(`room ${id} has no head`)
  return { id, ...configFromHead(head) }
}

/** The one lease room `id` holds, by its id. */
async function onlyLease(room: RoomIncarnation): Promise<string> {
  const keys: string[] = []
  for await (const page of getRoomPlane().direct.reads.readCellPages(room.id, room.inc, LEASE_CELL_PREFIX)) {
    if (page.kind === 'read') keys.push(...page.cells.keys())
  }
  const [key] = keys
  if (keys.length !== 1 || key === undefined) throw new Error(`expected one lease, read ${keys.length}`)
  return key.slice(LEASE_CELL_PREFIX.length)
}

/** Each commit to `member`'s inbox in room `id` runs `step` first, then commits as it was asked. */
function beforeInboxCommit(id: string, member: string, step: () => Promise<void>): void {
  const { commitLane } = MemoryRoomDriver.prototype
  vi.spyOn(MemoryRoomDriver.prototype, 'commitLane').mockImplementation(async function (
    this: MemoryRoomDriver,
    target,
    payload,
    options,
  ) {
    if (target.roomId === id && target.lane.kind === 'inbox' && target.lane.member === member) await step()
    return await commitLane.call(this, target, payload, options)
  })
}

/** The store's clock from here on reads `at`. */
function clockAt(at: number): void {
  vi.spyOn(Date, 'now').mockReturnValue(at)
}

/** From here on, the store refuses every leave in room `id` before it runs, as a reap's eviction can be. */
function refusingLeaves(id: string): void {
  const { runOperation: run } = MemoryRoomDriver.prototype
  async function refusing<Args, Value>(
    this: MemoryRoomDriver,
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    if (roomId === id && operation.name === 'leave') {
      throw new BackendError({ code: 'denied', outcome: 'not-run', message: 'refused before dispatch' })
    }
    return await run.bind(this)(roomId, inc, operation, args, options)
  }
  vi.spyOn(MemoryRoomDriver.prototype, 'runOperation').mockImplementation(refusing)
}

const told = (error: unknown) => (error instanceof Error ? error.message : typeof error)

describe('a send whose recipient was looked up while its lease hosted it', () => {
  it('delivers nothing once the lease lapsed before the inbox commit', async () => {
    const id = `hosting-at-commit-lapsed-${crypto.randomUUID()}`
    const handle = await Room.create(id)
    const tab = await handle.join({ identity: 'person' })
    const received: unknown[] = []
    tab.listen((message) => {
      received.push(message)
    })
    // The lookup read the lease live; the commit lands past its deadline.
    const joined = Date.now()
    beforeInboxCommit(id, tab.id, async () => clockAt(joined + ROOM_LEASE_TTL_MS + 1_000))
    const receipts = await Room.send(id, { identity: 'person' }, 'private message')
    vi.restoreAllMocks()

    expect({ receipts: receipts.length, received }).toEqual({ receipts: 0, received: [] })
    await Room.close(id)
  })

  it('delivers once the lease was renewed before the inbox commit, past the deadline it was looked up with', async () => {
    const id = `hosting-at-commit-renewed-${crypto.randomUUID()}`
    const handle = await Room.create(id)
    const tab = await handle.join({ identity: 'person' })
    const received: unknown[] = []
    tab.listen((message) => {
      received.push(message)
    })
    const room = await incarnationOf(id)
    const lease = await onlyLease(room)
    let renewed: boolean | undefined
    // The lookup read the lease near its deadline; it is renewed, and the commit lands past the deadline it had.
    const joined = Date.now()
    beforeInboxCommit(id, tab.id, async () => {
      clockAt(joined + ROOM_LEASE_TTL_MS - 1_000)
      renewed = (await runOperation(getRoomPlane().direct, room, RENEW_LEASE, { id: lease })).value
      clockAt(joined + ROOM_LEASE_TTL_MS + 1_000)
    })
    const receipts = await Room.send(id, { identity: 'person' }, 'private message')
    vi.restoreAllMocks()

    expect({ renewed, receipts: receipts.length, received }).toEqual({
      renewed: true,
      receipts: 1,
      received: ['private message'],
    })
    await Room.close(id)
  })
})

describe("a participant's send to a tab its server's view lists", () => {
  it('is refused once a reap claimed the tab’s lease, and tells the tab no leave of its own', async () => {
    const id = `hosting-at-commit-claimed-${crypto.randomUUID()}`
    const handle = await Room.create(id)
    const old = await handle.join({ identity: 'old' })
    const received: unknown[] = []
    old.listen((message) => {
      received.push(message)
    })
    const left: unknown[] = []
    old.onLeave((cause) => left.push(cause))
    // The lease lapsed and a reap claimed it; the store refused the reap's eviction, so no leave reached the view.
    refusingLeaves(id)
    clockAt(Date.now() + ROOM_LEASE_TTL_MS + 1_000)
    await Room.getParticipants(id).catch(() => {})
    vi.restoreAllMocks()
    const sender = await handle.join({ identity: 'live' })
    const listed = (await Room.getParticipants(id)).map((member) => member.id)
    const sent = await sender.send(old.id, 'hello').then(() => 'delivered', told)

    expect({ listed, sent, received, left }).toEqual({
      listed: [sender.id],
      sent: `Participant not found (left?): ${old.id}`,
      received: [],
      left: [],
    })
    await Room.close(id)
  })
})
