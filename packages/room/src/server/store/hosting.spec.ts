// A member counts as a participant only while its own lease can host, as observed beside its member cell: a lease
// claimed by a reap, lapsed or removed hosts no one, whatever the order of the reads and the reaps around it.
import { afterEach, describe, expect, it, vi } from 'vitest'
import { BackendError, ChannelOverflowError } from 'telefunc/__internal'
import { getRoomPlane } from '../../backend/plane.js'
import { MemoryRoomDriver } from '../../backend/MemoryRoomDriver.js'
import { RoomStore } from '../../backend/RoomStore.js'
import type { CellsRead, OperationOptions, OperationResult, RoomOperation } from '../../backend/contract.js'
import { Room } from '../../index.js'
import { RoomModel, type RosterRead } from '../../shared/RoomModel.js'
import { configFromHead, encodeRoomRecord } from '../lanes/lanes.js'
import type { RoomIncarnation } from '../protocol.js'
import { ROOM_CLOSE_LEASE_MS } from '../constants.js'
import { MEMBER_CELL_PREFIX, leaseCellKey, leaseClaimCellKey, memberCellKey } from './cells.js'
import { CLAIM_LEASE, RoomLease, type LeaseState } from './lease.js'
import { JOIN, LEAVE, joinMember } from './members.js'
import { runOperation } from './operations.js'

afterEach(() => vi.restoreAllMocks())

async function openRoom(prefix: string): Promise<RoomIncarnation> {
  const id = `${prefix}-${crypto.randomUUID()}`
  await Room.create(id, { emptyTimeout: Infinity, departureTimeout: Infinity })
  const head = await getRoomPlane().direct.reads.readHead(id)
  if (head === null) throw new Error(`room ${id} has no head`)
  return { id, ...configFromHead(head) }
}

/** A member joined under `lease`, another server's, as that server joins one. */
async function joinUnder(
  room: RoomIncarnation,
  lease: RoomLease,
  options: { hidden?: boolean; identity?: string } = {},
) {
  const member = crypto.randomUUID()
  const joinedAt = Date.now()
  const record = { joinedAt, metaSeq: 0, metaGen: 0, ...options }
  await joinMember(
    getRoomPlane().direct,
    room,
    lease,
    { id: member, record, meta: {}, join: { __r: 'join', id: member, meta: {}, joinedAt } },
    {},
  )
  return member
}

/** A member joined by JOIN under `lease` as its server holds it, its reply read. */
async function joinAs(room: RoomIncarnation, lease: LeaseState): Promise<string> {
  const member = crypto.randomUUID()
  const joinedAt = Date.now()
  const joined = await runOperation(getRoomPlane().direct, room, JOIN, {
    lease,
    hold: room.hold,
    id: member,
    record: { joinedAt, metaSeq: 0, metaGen: 0 },
    meta: {},
    join: { __r: 'join', id: member, meta: {}, joinedAt },
  })
  if (joined.value !== 'joined') throw new Error(`join under ${lease.id}: ${joined.value}`)
  return member
}

/** Which of `ids` the store holds a member record of, hidden ones too. */
async function storedMembers(room: RoomIncarnation, ids: string[]): Promise<string[]> {
  const read = await getRoomPlane().direct.reads.readCells(room.id, room.inc, { keys: ids.map(memberCellKey) })
  return read.kind === 'read' ? ids.filter((id) => read.cells.has(memberCellKey(id))) : []
}

/** Lets `lease` lapse, as once its server stopped renewing it. */
async function lapseLease(room: RoomIncarnation, lease: string) {
  await getRoomPlane().direct.compareExchangeCells(room.id, room.inc, {
    expect: [],
    mutations: [{ key: leaseCellKey(lease), bytes: new Uint8Array(), lapsesInMs: -1 }],
  })
}

/** Moves `lease` to `cleaner`'s claim, as a reap's claim does. */
async function claimLease(room: RoomIncarnation, lease: string, cleaner: string) {
  await getRoomPlane().direct.compareExchangeCells(room.id, room.inc, {
    expect: [],
    mutations: [
      { key: leaseCellKey(lease), bytes: null },
      { key: leaseClaimCellKey(lease), bytes: new TextEncoder().encode(cleaner), lapsesInMs: ROOM_CLOSE_LEASE_MS },
    ],
  })
}

/** From here on, the store refuses every leave in `room` before it runs, as a reap's eviction can be. */
function refusingLeaves(room: RoomIncarnation): void {
  const { runOperation: run } = MemoryRoomDriver.prototype
  async function refusing<Args, Value>(
    this: MemoryRoomDriver,
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    if (roomId === room.id && operation.name === 'leave') {
      throw new BackendError({ code: 'denied', outcome: 'not-run', message: 'refused before dispatch' })
    }
    return await run.bind(this)(roomId, inc, operation, args, options)
  }
  vi.spyOn(MemoryRoomDriver.prototype, 'runOperation').mockImplementation(refusing)
}

/** The participants a read lists, the room's count, and what a send to `member` by id is told. */
async function observed(room: RoomIncarnation, member: string) {
  const listed = (await Room.getParticipants(room.id)).map((each) => each.id).includes(member)
  const [info] = (await Room.list({ prefix: room.id })).filter((each) => each.id === room.id)
  const sent = await Room.send(room.id, { id: member }, 'hello').then(
    () => 'delivered',
    (error: unknown) => (error instanceof Error ? error.message : typeof error),
  )
  return { listed, count: info?.count, sent: sent === 'delivered' ? sent : sent.startsWith('Participant not found') }
}

describe('a member whose lease a reap claimed, while the reap is refused', () => {
  it('is no participant: not listed, not counted, and a send to it is told it is gone (#436 line 76)', async () => {
    const room = await openRoom('hosting-claimed')
    const lease = new RoomLease()
    const member = await joinUnder(room, lease)
    await lapseLease(room, lease.id)
    refusingLeaves(room)
    await Room.getParticipants(room.id).catch(() => {})
    vi.restoreAllMocks()

    expect(await observed(room, member)).toEqual({ listed: false, count: 0, sent: true })
  })
})

describe('a member joined under a lease the reader’s lease scan missed', () => {
  it('is listed only while its lease can host, whatever the scan saw (A.2)', async () => {
    const room = await openRoom('hosting-missed')
    // The reader's member read waits until the room changed under it.
    let release = () => {}
    const held = new Promise<void>((resolve) => {
      release = resolve
    })
    let holding = true
    const { readCellPages } = MemoryRoomDriver.prototype
    async function* holdingMembers(
      this: MemoryRoomDriver,
      ...read: Parameters<MemoryRoomDriver['readCellPages']>
    ): AsyncGenerator<CellsRead> {
      const [roomId, , prefix] = read
      if (roomId === room.id && prefix === MEMBER_CELL_PREFIX && holding) {
        holding = false
        await held
      }
      yield* readCellPages.apply(this, read)
    }
    vi.spyOn(MemoryRoomDriver.prototype, 'readCellPages').mockImplementation(holdingMembers)
    const reading = Room.getParticipants(room.id)
    // P joins under a new lease, which lapses and is claimed by a reap that the store then refuses.
    const leaseP = new RoomLease()
    const p = await joinUnder(room, leaseP)
    await claimLease(room, leaseP.id, 'another reap')
    // Q joins live after P's claim.
    const q = await joinUnder(room, new RoomLease())
    release()
    const listed = (await reading).map((each) => each.id)

    expect({ p: listed.includes(p), q: listed.includes(q) }).toEqual({ p: false, q: true })
  })
})

describe('a reader whose claim on a lapsed lease another reap took first', () => {
  it('rereads the members and leaves out the lease’s member (A.2, the degraded order)', async () => {
    const room = await openRoom('hosting-lost-claim')
    const lease = new RoomLease()
    const member = await joinUnder(room, lease)
    await lapseLease(room, lease.id)
    // Just before this reader's claim, another reap claims the lease, and its eviction is refused.
    const { runOperation: run } = MemoryRoomDriver.prototype
    let first = true
    async function claimedFirst<Args, Value>(
      this: MemoryRoomDriver,
      roomId: string,
      inc: string,
      operation: RoomOperation<Args, Value>,
      args: Args,
      options?: OperationOptions,
    ): Promise<OperationResult<Value>> {
      if (roomId === room.id && operation.name === 'claimLease' && first) {
        first = false
        await run.bind(this)(roomId, inc, CLAIM_LEASE, { lease: lease.id, cleaner: 'another' })
      }
      return await run.bind(this)(roomId, inc, operation, args, options)
    }
    vi.spyOn(MemoryRoomDriver.prototype, 'runOperation').mockImplementation(claimedFirst)
    const listed = (await Room.getParticipants(room.id)).map((each) => each.id)

    expect(listed.includes(member)).toBe(false)
  })
})

describe('a lease id that its server reused after the lease was removed', () => {
  it('hosts the member joined under it again (A.1)', async () => {
    const room = await openRoom('hosting-reused')
    // A lease whose first join's reply was lost: its server still holds it as never created, and joins under it so.
    const lease = { id: crypto.randomUUID(), created: false }
    const first = await joinAs(room, lease)
    // A reap claims it, evicts its member and removes it.
    await claimLease(room, lease.id, 'a reap')
    const { direct } = getRoomPlane()
    await runOperation(direct, room, LEAVE, { id: first, cause: { cause: 'disconnected' }, hold: room.hold })
    await direct.compareExchangeCells(room.id, room.inc, {
      expect: [],
      mutations: [{ key: leaseClaimCellKey(lease.id), bytes: null }],
    })
    // The server joins another member under the same id, which writes the lease's cell anew.
    const member = await joinAs(room, lease)
    const [listed] = (await Room.getParticipants(room.id)).filter((each) => each.id === member)

    expect(listed?.isConnected).toBe(true)
  })
})

describe('a member record without its lease reference', () => {
  it('is a bug a read names', async () => {
    const room = await openRoom('hosting-no-reference')
    const member = await joinUnder(room, new RoomLease())
    await getRoomPlane().direct.compareExchangeCells(room.id, room.inc, {
      expect: [],
      mutations: [{ key: `ml:${member}`, bytes: null }],
    })
    const told = await Room.getParticipants(room.id).then(
      () => 'listed',
      (error: unknown) => (error instanceof Error ? error.message : typeof error),
    )

    expect(told).toContain(`the member record ${MEMBER_CELL_PREFIX}${member} was read without its lease reference`)
  })
})

describe('a removal by identity whose reason fits a visible tab’s leave and not a hidden tab’s', () => {
  it('goes through where the hidden tab is on a lapsed lease: it hosts no one, so it is not checked', async () => {
    const room = await openRoom('hosting-hidden-tab')
    const visible = await Room.join(room.id, { identity: 'u1' })
    const lapsed = new RoomLease()
    const hidden = await joinUnder(room, lapsed, { hidden: true, identity: 'u1' })
    await lapseLease(room, lapsed.id)
    // A store whose size rule takes a visible tab's leave with this reason, and refuses a hidden tab's, 14 bytes
    // longer (members.spec.ts).
    const reason = 'r'.repeat(64)
    const limit = encodeRoomRecord({ __r: 'leave', id: visible.id, cause: 'removed', reason }).byteLength
    vi.spyOn(RoomStore.prototype, 'assertCommittable').mockImplementation((_lane, payload) => {
      if (payload.byteLength > limit) throw new ChannelOverflowError(`a commit of ${payload.byteLength} bytes`)
    })
    const removed = await Room.removeParticipant(room.id, { identity: 'u1', reason }).then(
      () => 'removed',
      (error: unknown) => (error instanceof Error ? error.message : typeof error),
    )
    vi.restoreAllMocks()

    expect({ removed, stored: await storedMembers(room, [visible.id, hidden]) }).toEqual({
      removed: 'removed',
      stored: [],
    })
  })
})

/** A store whose size rule takes `LIMIT` bytes at most, from here on. */
const LIMIT = 64 * 1024
function withSizeRule(): void {
  vi.spyOn(RoomStore.prototype, 'assertCommittable').mockImplementation((_lane, payload) => {
    if (payload.byteLength > LIMIT) throw new ChannelOverflowError(`a commit of ${payload.byteLength} bytes`)
  })
}

describe('a call over the size whose participant is on a lapsed lease', () => {
  it('Room.send() by id to it is told the participant is gone, and the lease is reaped', async () => {
    const room = await openRoom('hosting-lapsed-target')
    const lease = new RoomLease()
    const member = await joinUnder(room, lease)
    await lapseLease(room, lease.id)
    withSizeRule()
    const told = await Room.send(room.id, { id: member }, 'x'.repeat(2 * LIMIT)).then(
      () => 'resolved',
      (error: unknown) => (error instanceof Error ? error.message : typeof error),
    )
    vi.restoreAllMocks()

    expect({ told, stored: await storedMembers(room, [member]) }).toEqual({
      told: `Participant not found (left?): ${member}`,
      stored: [],
    })
  })

  it('Room.removeParticipant() by an identity whose tabs are all on lapsed leases goes through, and the reap removes them', async () => {
    const room = await openRoom('hosting-lapsed-identity')
    const lease = new RoomLease()
    const tabs = [
      await joinUnder(room, lease, { identity: 'u1' }),
      await joinUnder(room, lease, { identity: 'u1', hidden: true }),
    ]
    await lapseLease(room, lease.id)
    withSizeRule()
    const removed = await Room.removeParticipant(room.id, { identity: 'u1', reason: 'r'.repeat(2 * LIMIT) }).then(
      () => 'removed',
      (error: unknown) => (error instanceof Error ? error.name : typeof error),
    )
    vi.restoreAllMocks()

    expect({ removed, stored: await storedMembers(room, tabs) }).toEqual({ removed: 'removed', stored: [] })
  })
})

/** From here on, the store loses one control frame of room `id` at its delivery to this process each time
 *  `loseNext()` was called, as a store that drops a frame does: the view finds the hole by the next one. */
function losingControlFrames(id: string): { loseNext: () => void; lost: () => number } {
  let losing = 0
  let lost = 0
  const { subscribe } = MemoryRoomDriver.prototype
  vi.spyOn(MemoryRoomDriver.prototype, 'subscribe').mockImplementation(function (
    this: MemoryRoomDriver,
    source,
    receiver,
  ) {
    if (source.roomId !== id || source.lane.kind !== 'control') return subscribe.bind(this)(source, receiver)
    return subscribe.bind(this)(source, {
      deliver(payload, ordering) {
        if (losing > 0) {
          losing--
          lost++
          return
        }
        receiver.deliver(payload, ordering)
      },
    })
  })
  return { loseNext: () => void losing++, lost: () => lost }
}

describe('a view whose roster read leaves out a member whose reap is stalled', () => {
  it('reads its roster on a lost control frame, keeps the member for its leave, and ends it once with the reap’s own cause (A.3)', async () => {
    const id = `hosting-stalled-${crypto.randomUUID()}`
    const frames = losingControlFrames(id)
    const handle = await Room.create(id, { emptyTimeout: Infinity, departureTimeout: Infinity })
    const head = await getRoomPlane().direct.reads.readHead(id)
    if (head === null) throw new Error(`room ${id} has no head`)
    const room = { id, ...configFromHead(head) }
    const lease = new RoomLease()
    const member = await joinUnder(room, lease)
    const causes: unknown[] = []
    const left = Promise.withResolvers<void>()
    handle.onLeave((leaving, cause) => {
      if (leaving.id !== member) return
      causes.push(cause)
      left.resolve()
    })
    // The view knows the member.
    await Room.get(id)
    await lapseLease(room, lease.id)
    // A reap claims the lease, and its eviction of the member stalls in the store.
    const evicting = Promise.withResolvers<void>()
    const stalled = Promise.withResolvers<void>()
    const { runOperation: run } = MemoryRoomDriver.prototype
    async function stalling<Args, Value>(
      this: MemoryRoomDriver,
      roomId: string,
      inc: string,
      operation: RoomOperation<Args, Value>,
      args: Args,
      options?: OperationOptions,
    ): Promise<OperationResult<Value>> {
      if (roomId === id && operation.name === 'leave') {
        evicting.resolve()
        await stalled.promise
      }
      return await run.bind(this)(roomId, inc, operation, args, options)
    }
    vi.spyOn(MemoryRoomDriver.prototype, 'runOperation').mockImplementation(stalling)
    const reaping = Room.getParticipants(id)
    await evicting.promise
    // The view's next roster read, and the roster it reconciles.
    const reconciled = Promise.withResolvers<RosterRead>()
    const { reconcileRoster } = RoomModel.prototype
    vi.spyOn(RoomModel.prototype, 'reconcileRoster').mockImplementation(async function (
      this: RoomModel,
      roster,
      events,
    ) {
      const result = await reconcileRoster.bind(this)(roster, events)
      reconciled.resolve(roster)
      return result
    })
    // The store loses one join's frame to the view, which finds the hole by the next join's.
    frames.loseNext()
    await joinUnder(room, new RoomLease())
    await joinUnder(room, new RoomLease())
    const roster = await reconciled.promise
    const causesWhileStalled = [...causes]
    stalled.resolve()
    await reaping
    await left.promise

    expect({
      lost: frames.lost(),
      leaving: [...(roster.leaving ?? [])],
      causesWhileStalled,
      causes,
    }).toEqual({ lost: 1, leaving: [member], causesWhileStalled: [], causes: [{ type: 'disconnected' }] })
  })
})
