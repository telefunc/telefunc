// A `Room.send()` of data, a participant's `send()` of data, or a `Room.removeParticipant()` with a reason, that its
// commit would refuse for its size is refused before the call's first write, also where finding the participant would
// reap a lapsed lease. The memory
// store has no size rule, so these rows give it one: the commit refuses what `assertCommittable()` refuses, as the
// Redis store's does (packages/redis/src/publish-send.store.spec.ts runs the same calls there).
// A send's check is given its first member's message, which that member commits; every other member's is encoded at
// its own commit, with the checked one's length.
import { afterEach, expect, it, vi } from 'vitest'
import { Abort } from 'telefunc'
import { ChannelOverflowError } from 'telefunc/__internal'
import { getRoomPlane } from '../backend/plane.js'
import { MemoryRoomDriver } from '../backend/MemoryRoomDriver.js'
import { RoomStore } from '../backend/RoomStore.js'
import type { CommitOptions, CommitResult, CommitTarget, LaneId } from '../backend/contract.js'
import { Room } from '../index.js'
import { configFromHead, encodeRoomRecord } from './lanes/lanes.js'
import { LEASE_CELL_PREFIX, leaseCellKey, memberCellKey } from './store/cells.js'
import { RoomLease } from './store/lease.js'
import { joinMember } from './store/members.js'

afterEach(() => vi.restoreAllMocks())

const LIMIT = 64 * 1024
const LARGE = 'x'.repeat(2 * LIMIT)

function told(error: unknown) {
  return error instanceof Error ? error.name : typeof error
}

/** The size rule of a store that commits `LIMIT` bytes at most. */
function sizeRule(_lane: LaneId, payload: Uint8Array): void {
  if (payload.byteLength > LIMIT) throw new ChannelOverflowError(`a commit of ${payload.byteLength} bytes`)
}

/** From here on, the memory store commits only what its size rule takes. */
function withSizeRule(): void {
  const { commitLane } = RoomStore.prototype
  async function refusingCommit(
    this: RoomStore,
    target: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    sizeRule(target.lane, payload)
    return await commitLane.bind(this)(target, payload, opts)
  }
  vi.spyOn(RoomStore.prototype, 'assertCommittable').mockImplementation(sizeRule)
  vi.spyOn(RoomStore.prototype, 'commitLane').mockImplementation(refusingCommit)
}

/** A lease of another server in room `id` that lapsed, hosting nothing: a call that finds a participant there reaps
 *  it. Reads its cell back with `read`. */
async function lapsedLease(id: string): Promise<{ read: () => Promise<string | undefined> }> {
  const { direct } = getRoomPlane()
  const head = await direct.reads.readHead(id)
  if (head === null) throw new Error(`room ${id} has no head`)
  const { inc } = configFromHead(head)
  const key = `${LEASE_CELL_PREFIX}lapsed`
  await direct.compareExchangeCells(id, inc, {
    expect: [],
    mutations: [{ key, bytes: new TextEncoder().encode('{"until":0}') }],
  })
  return {
    read: async () => {
      const read = await direct.reads.readCells(id, inc, { keys: [key] })
      const raw = read.kind === 'stale-incarnation' ? undefined : read.cells.get(key)
      return raw === undefined ? undefined : new TextDecoder().decode(raw)
    },
  }
}

for (const by of ['id', 'identity'] as const) {
  it(`Room.send() by ${by} of data over the size, where finding the participant would reap a lapsed lease, is refused before its first write`, async () => {
    const id = `send-large-${by}-${crypto.randomUUID()}`
    await Room.create(id)
    const participant = await Room.join(id, { identity: 'u1' })
    const lease = await lapsedLease(id)
    withSizeRule()
    const sending =
      by === 'id' ? Room.send(id, { id: participant.id }, LARGE) : Room.send(id, { identity: 'u1' }, LARGE)
    const refused = await sending.then(() => 'resolved', told)
    vi.restoreAllMocks()

    expect({ refused, lease: await lease.read() }).toEqual({ refused: 'ChannelOverflowError', lease: '{"until":0}' })
    await Room.close(id)
  })

  it(`Room.removeParticipant() by ${by} with a reason over the size, where finding the participant would reap a lapsed lease, is refused before its first write`, async () => {
    const id = `remove-large-${by}-${crypto.randomUUID()}`
    await Room.create(id)
    const participant = await Room.join(id, { identity: 'u1' })
    const lease = await lapsedLease(id)
    withSizeRule()
    const target = by === 'id' ? { id: participant.id } : { identity: 'u1' }
    const refused = await Room.removeParticipant(id, { ...target, reason: LARGE }).then(() => 'resolved', told)
    vi.restoreAllMocks()

    expect({
      refused,
      lease: await lease.read(),
      listed: (await Room.getParticipants(id)).map((each) => each.id),
    }).toEqual({ refused: 'ChannelOverflowError', lease: '{"until":0}', listed: [participant.id] })
    await Room.close(id)
  })
}

/** From here on, counts the reads of `member`'s record by id: the lookup in the store of a send that finds it there. */
function countLookups(member: string): { count: number } {
  const lookups = { count: 0 }
  const { readCells } = MemoryRoomDriver.prototype
  vi.spyOn(MemoryRoomDriver.prototype, 'readCells').mockImplementation(async function (
    this: MemoryRoomDriver,
    roomId,
    inc,
    sel,
  ) {
    if ('keys' in sel && sel.keys.includes(memberCellKey(member))) lookups.count++
    return await readCells.bind(this)(roomId, inc, sel)
  })
  return lookups
}

/** A participant that another server joined to room `id` with `identity` under its own lease, as its join writes it:
 *  no view this process opened before lists it. `lapse()` lets that lease lapse, as a server that stopped does. */
async function joinedElsewhere(id: string, identity?: string): Promise<{ member: string; lapse: () => Promise<void> }> {
  const { direct } = getRoomPlane()
  const head = await direct.reads.readHead(id)
  if (head === null) throw new Error(`room ${id} has no head`)
  const room = { id, ...configFromHead(head) }
  const lease = new RoomLease()
  const member = crypto.randomUUID()
  const joinedAt = Date.now()
  const identified = identity === undefined ? {} : { identity }
  await joinMember(
    direct,
    room,
    lease,
    {
      id: member,
      record: { joinedAt, metaSeq: 0, metaGen: 0, ...identified },
      meta: {},
      join: { __r: 'join', id: member, meta: {}, joinedAt, ...identified },
    },
    {},
  )
  const lapse = async () => {
    const mutations = [{ key: leaseCellKey(lease.id), bytes: new TextEncoder().encode('{"until":0}') }]
    await direct.compareExchangeCells(id, room.inc, { expect: [], mutations })
  }
  return { member, lapse }
}

it("a participant's send() of data over the size, to a participant this server holds no view of, found by its lookup in the store, where that lookup would reap a lapsed lease, is refused before its first write", async () => {
  const id = `participant-send-large-${crypto.randomUUID()}`
  await Room.create(id)
  const { member: recipient } = await joinedElsewhere(id)
  const sender = await Room.join(id)
  const lease = await lapsedLease(id)
  withSizeRule()
  const lookups = countLookups(recipient)
  const refused = await sender.send(recipient, LARGE).then(() => 'resolved', told)
  vi.restoreAllMocks()

  expect({ refused, lookups: lookups.count, lease: await lease.read() }).toEqual({
    refused: 'ChannelOverflowError',
    lookups: 1,
    lease: '{"until":0}',
  })
  await Room.close(id)
})

for (const by of ['id', 'identity'] as const) {
  it(`Room.send() by ${by} encodes each member's message once, and commits the bytes its size check was given or bytes of their length`, async () => {
    const id = `send-encodes-${by}-${crypto.randomUUID()}`
    await Room.create(id)
    const first = await Room.join(id, { identity: 'u1' })
    await Room.join(id, { identity: 'u1' })
    const marker = crypto.randomUUID()
    const { encode } = TextEncoder.prototype
    let encodes = 0
    function countingEncode(this: TextEncoder, input?: string): Uint8Array<ArrayBuffer> {
      if (input?.includes(marker)) encodes++
      return encode.bind(this)(input)
    }
    vi.spyOn(TextEncoder.prototype, 'encode').mockImplementation(countingEncode)
    const checked: Uint8Array[] = []
    const committed: Uint8Array[] = []
    vi.spyOn(RoomStore.prototype, 'assertCommittable').mockImplementation((lane, payload) => {
      if (lane.kind === 'inbox') checked.push(payload)
    })
    const { commitLane } = RoomStore.prototype
    async function recordingCommit(
      this: RoomStore,
      target: CommitTarget,
      payload: Uint8Array,
      opts?: CommitOptions,
    ): Promise<CommitResult> {
      if (target.lane.kind === 'inbox') committed.push(payload)
      return await commitLane.bind(this)(target, payload, opts)
    }
    vi.spyOn(RoomStore.prototype, 'commitLane').mockImplementation(recordingCommit)
    const data = { marker }
    if (by === 'id') await Room.send(id, { id: first.id }, data)
    else await Room.send(id, { identity: 'u1' }, data)
    vi.restoreAllMocks()

    const members = by === 'id' ? 1 : 2
    const [checkedPayload] = checked
    expect({
      encodes,
      checked: checked.length,
      commits: committed.length,
      committedAsChecked: committed.filter((payload) => payload === checkedPayload).length,
      ofTheCheckedLength: committed.every((payload) => payload.byteLength === checkedPayload?.byteLength),
    }).toEqual({ encodes: members, checked: 1, commits: members, committedAsChecked: 1, ofTheCheckedLength: true })
    await Room.close(id)
  })
}

/** Which of `ids` room `id`'s store holds a member record of, hidden ones too. */
async function storedMembers(id: string, ids: string[]): Promise<string[]> {
  const { direct } = getRoomPlane()
  const head = await direct.reads.readHead(id)
  if (head === null) throw new Error(`room ${id} has no head`)
  const read = await direct.reads.readCells(id, configFromHead(head).inc, { keys: ids.map(memberCellKey) })
  return read.kind === 'read' ? ids.filter((member) => read.cells.has(memberCellKey(member))) : []
}

/** The length of `reason` at which a hidden tab's leave with it is `bytes` long: the leave Room commits for it. */
function reasonForHiddenLeave(bytes: number): string {
  const leave = (reason: string) =>
    encodeRoomRecord({ __r: 'leave', id: crypto.randomUUID(), cause: 'removed', reason, hidden: true }).byteLength
  return 'x'.repeat(bytes - leave(''))
}

for (const over of [0, 1]) {
  it(`Room.removeParticipant() by identity of a visible and a hidden tab, with a reason whose hidden leave is ${over === 0 ? 'the size exactly: removes both' : 'one byte over the size: is refused, and removes neither'}`, async () => {
    const id = `remove-threshold-${over}-${crypto.randomUUID()}`
    await Room.create(id)
    const visible = await Room.join(id, { identity: 'u1' })
    const hidden = await Room.join(id, { identity: 'u1', hidden: true })
    withSizeRule()
    const reason = reasonForHiddenLeave(LIMIT + over)
    const answer = await Room.removeParticipant(id, { identity: 'u1', reason }).then(() => 'resolved', told)
    vi.restoreAllMocks()
    const kept = await storedMembers(id, [visible.id, hidden.id])

    expect({ answer, visible: kept.includes(visible.id), hidden: kept.includes(hidden.id) }).toEqual(
      over === 0
        ? { answer: 'resolved', visible: false, hidden: false }
        : { answer: 'ChannelOverflowError', visible: true, hidden: true },
    )
    await Room.close(id)
  })
}

it('Room.send() by identity whose first tab, in join order as the memory store reads them, is on a lapsed lease checks the tab that hosts, and delivers to it', async () => {
  const id = `send-checked-reaped-${crypto.randomUUID()}`
  await Room.create(id)
  const first = await joinedElsewhere(id, 'u1')
  const left = await Room.join(id, { identity: 'u1' })
  await first.lapse()
  const checked: string[] = []
  vi.spyOn(RoomStore.prototype, 'assertCommittable').mockImplementation((lane) => {
    if (lane.kind === 'inbox') checked.push(lane.member)
  })
  const receipts = await Room.send(id, { identity: 'u1' }, 'small')
  vi.restoreAllMocks()
  const listed = (await Room.getParticipants(id)).map((each) => each.id)

  // A tab on a lapsed lease hosts no one, so the lookup leaves it out: the check runs on the first read and on the reread
  // after the reap, each time on the tab that hosts.
  expect({ checked, delivered: receipts.length, listed }).toEqual({
    checked: [left.id, left.id],
    delivered: 1,
    listed: [left.id],
  })
  await Room.close(id)
})

for (const listed of ['in the view', 'only in the store'] as const) {
  it(`a participant's send() over the size, to a participant ${listed === 'in the view' ? 'in the view, found with no store lookup' : 'only in the store, found by its lookup there'}, is refused for its size before its send guard runs`, async () => {
    const id = `participant-send-guarded-${listed === 'in the view' ? 'view' : 'store'}-${crypto.randomUUID()}`
    const room = await Room.create(id)
    let guarded = 0
    Room.guard(room, {
      anyoneCanJoin: true,
      onBeforeSend: () => {
        guarded++
        throw Abort('Test: the guard refuses every send')
      },
    })
    const elsewhere = listed === 'only in the store' ? (await joinedElsewhere(id)).member : null
    const sender = await room.join()
    const recipient = elsewhere ?? (await room.join()).id
    withSizeRule()
    const lookups = countLookups(recipient)
    const oversized = await sender.send(recipient, LARGE).then(() => 'resolved', told)
    vi.restoreAllMocks()
    const guardedBefore = guarded
    const fitting = await sender.send(recipient, 'small').then(() => 'resolved', told)

    expect({ oversized, lookups: lookups.count, guardedBefore, fitting, guarded }).toEqual({
      oversized: 'ChannelOverflowError',
      lookups: listed === 'in the view' ? 0 : 1,
      guardedBefore: 0,
      fitting: 'Abort',
      guarded: 1,
    })
    await Room.close(id)
  })
}
