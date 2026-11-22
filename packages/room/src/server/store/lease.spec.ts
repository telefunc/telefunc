// A reap's claim on a lapsed lease keeps other reapers off for as long as a close's lease, not a lease's: so a reap
// the store refuses leaves the lease to the next reader within 15 s.
import { afterEach, expect, it, vi } from 'vitest'
import { BackendError } from 'telefunc/__internal'
import { getRoomPlane } from '../../backend/plane.js'
import { MemoryRoomDriver } from '../../backend/MemoryRoomDriver.js'
import type { OperationOptions, OperationResult, RoomOperation } from '../../backend/contract.js'
import { Room } from '../../index.js'
import { ROOM_CLOSE_LEASE_MS } from '../constants.js'
import { configFromHead } from '../lanes/lanes.js'
import { LEASE_CELL_PREFIX } from './cells.js'
import { RoomLease } from './lease.js'
import { joinMember } from './members.js'

afterEach(() => vi.restoreAllMocks())

it('a reap whose eviction the store refuses leaves a claim that lapses within a close lease', async () => {
  const id = `refused-reap-${crypto.randomUUID()}`
  await Room.create(id, { emptyTimeout: Infinity, departureTimeout: Infinity })
  const plane = getRoomPlane()
  const head = await plane.direct.reads.readHead(id)
  if (head === null) throw new Error(`room ${id} has no head`)
  const room = { id, ...configFromHead(head) }
  // A member of another server's lease, which then lapses, as once that server stopped.
  const lease = new RoomLease()
  const member = crypto.randomUUID()
  const joinedAt = Date.now()
  await joinMember(
    plane.direct,
    room,
    lease,
    {
      id: member,
      record: { joinedAt, metaSeq: 0, metaGen: 0 },
      meta: {},
      join: { __r: 'join', id: member, meta: {}, joinedAt },
    },
    {},
  )
  const key = `${LEASE_CELL_PREFIX}${lease.id}`
  await plane.direct.compareExchangeCells(id, room.inc, {
    expect: [],
    mutations: [{ key, bytes: new TextEncoder().encode('{"until":0}') }],
  })
  const { runOperation } = MemoryRoomDriver.prototype
  async function refusingLeaves<Args, Value>(
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
    return await runOperation.bind(this)(roomId, inc, operation, args, options)
  }
  vi.spyOn(MemoryRoomDriver.prototype, 'runOperation').mockImplementation(refusingLeaves)
  const refused = await Room.getParticipants(id).then(
    () => 'resolved',
    (error: unknown) => error,
  )
  vi.restoreAllMocks()
  const read = await plane.direct.reads.readCells(id, room.inc, { keys: [key] })
  const raw = read.kind === 'stale-incarnation' ? undefined : read.cells.get(key)
  const until = Number(/"until":(\d+)/.exec(raw === undefined ? '' : new TextDecoder().decode(raw))?.[1])
  const claimMs = until - Date.now()

  expect({ refused, claimed: claimMs > 0 && claimMs <= ROOM_CLOSE_LEASE_MS }).toMatchObject({
    refused: { name: 'BackendError', code: 'denied', outcome: 'may-have-run' },
    claimed: true,
  })
})
