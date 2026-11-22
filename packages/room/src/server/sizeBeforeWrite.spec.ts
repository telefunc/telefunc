// A `Room.send()` of data, or a `Room.removeParticipant()` with a reason, that its commit would refuse for its size is
// refused before the call's first write, also where finding the participant would reap a lapsed lease. The memory
// store has no size rule, so these rows give it one: the commit refuses what `assertCommittable()` refuses, as the
// Redis store's does (packages/redis/src/publish-send.store.spec.ts runs the same calls there).
// A send's check is given its first member's message, which that member commits; every other member's is encoded at
// its own commit, with the checked one's length.
import { afterEach, expect, it, vi } from 'vitest'
import { ChannelOverflowError } from 'telefunc/__internal'
import { getRoomPlane } from '../backend/plane.js'
import { RoomStore } from '../backend/RoomStore.js'
import type { CommitOptions, CommitResult, CommitTarget, LaneId } from '../backend/contract.js'
import { Room } from '../index.js'
import { configFromHead } from './lanes/lanes.js'
import { LEASE_CELL_PREFIX } from './store/cells.js'

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
