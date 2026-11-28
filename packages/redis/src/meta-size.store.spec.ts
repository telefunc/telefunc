// A room meta update whose event the store's commit would refuse, on the oldest and the newest Redis and Valkey
// Telefunc supports: its operation, the meta and its event in one write, is refused before its first write, with the
// error that commit throws, and the store keeps every key and value as before, its revision and index included. The
// event's `seq` is the one the operation writes, planned again from a new read where another write committed first,
// so the refusal and its twin one byte shorter, which commits, sit at a digit boundary:
// - a first write, at seq 1;
// - a merge whose read predicts seq 9, 99999999999999 or 999999999999999 while another merge commits first, so it
//   writes 10, 100000000000000 or 1000000000000000;
// - a replacement that a merge overtook, at the seq it writes.
// The value that numbers the event is always there: the first write creates `meta:seq`, and a later one counts it
// while `meta:gen`, written with it, is as read. A room closed and opened again before a replacement's operation tells
// the replacement it closed, and a first write another first write overtook writes as a replacement.
import { isDeepStrictEqual } from 'node:util'
import type { Redis } from 'ioredis'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { Room } from '@telefunc/room'
import { getRoomPlane } from '@telefunc/room/backend'
import { endReached, reached, runs, snapshot, STORE_START_MS, STORES, type Store } from '../test/stores.js'
import { REDIS_COMMANDS } from './commands.js'
import { RedisConnection } from './RedisConnection.js'

/** The most a commit takes on a room's control lane: `PUBLISH_MAX_BYTES` in commands.ts. */
const MAX = 28 * 1024 * 1024
afterEach(() => vi.restoreAllMocks())
afterAll(() => endReached())

/** Room `id`'s `meta:seq` counter at `value`, as a merge after the first write would have left it. */
async function seqAt(admin: Redis, id: string, value: number): Promise<void> {
  const [values] = await admin.keys(`*{${id}}*:values`)
  if (values === undefined) throw new Error(`room ${id} has no cells`)
  await admin.hset(values, 'meta:seq', `{"value":${value},"changedAt":${Date.now()}}`)
}

/** The control commit that the operation `input` would commit in room `id`, if it is one. */
function controlPayload(input: unknown, id: string): Uint8Array | undefined {
  if (typeof input !== 'object' || input === null || !('roomId' in input) || input.roomId !== id) return undefined
  if (!('commit' in input) || typeof input.commit !== 'object' || input.commit === null) return undefined
  const { commit } = input
  if (!('lane' in commit) || typeof commit.lane !== 'object' || commit.lane === null) return undefined
  if (!('kind' in commit.lane) || commit.lane.kind !== 'control') return undefined
  return 'payload' in commit && commit.payload instanceof Uint8Array ? commit.payload : undefined
}

/** Whether an operation's `outcome` committed its commit. */
function committed(outcome: unknown): boolean {
  if (typeof outcome !== 'object' || outcome === null || !('kind' in outcome) || outcome.kind !== 'done') return false
  if (!('commit' in outcome) || typeof outcome.commit !== 'object' || outcome.commit === null) return false
  return 'kind' in outcome.commit && outcome.commit.kind === 'accepted'
}

/** Room `id`'s meta writes as the store gets them, each an operation: the control commits they committed, each its
 *  length and `seq`; and, where `hold` is true, the first one whose commit writes the meta key `big`, held until
 *  released once it read and planned, before it reaches the store. */
function metaWrites(id: string, hold: boolean) {
  const commits: { bytes: number; seq: string }[] = []
  const held = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  let holding = hold
  const { run } = RedisConnection.prototype
  vi.spyOn(RedisConnection.prototype, 'run').mockImplementation(async function (this: RedisConnection, command, input) {
    const payload = command.name === REDIS_COMMANDS.operation.name ? controlPayload(input, id) : undefined
    const json = payload === undefined ? '' : new TextDecoder().decode(payload.subarray(0, 64))
    if (holding && payload !== undefined && new TextDecoder().decode(payload).includes('"big"')) {
      holding = false
      held.resolve()
      await release.promise
    }
    const outcome = await run.call(this, command, input)
    if (payload !== undefined && committed(outcome)) {
      commits.push({ bytes: payload.byteLength, seq: /"seq":(\d+)/.exec(json)?.[1] ?? 'none' })
    }
    return outcome
  })
  return { commits, held: held.promise, release: () => release.resolve() }
}

/** The error a commit of a record of `bytes` bytes on room `id`'s control lane throws, which writes nothing. */
async function commitRefusal(id: string, bytes: number): Promise<unknown> {
  const { direct } = getRoomPlane()
  const head = await direct.reads.readHead(id)
  if (head === null || head.currentInc === null) throw new Error(`room ${id} isn't open`)
  return await direct
    .commitLane({ roomId: id, inc: head.currentInc, lane: { kind: 'control' } }, new Uint8Array(bytes))
    .then(
      () => 'committed',
      (error: unknown) => error,
    )
}

/** What tells an error apart from another, field by field. */
function fields(error: unknown) {
  if (!(error instanceof Error)) return { value: String(error) }
  const own = Object.getOwnPropertyNames(error).filter((name) => name !== 'stack')
  return {
    class: error.constructor.name,
    name: error.name,
    message: error.message,
    own: Object.fromEntries(own.map((name) => [name, Reflect.get(error, name)])),
  }
}

const told = (call: Promise<void>) =>
  call.then(
    () => 'resolved',
    (error: unknown) => fields(error),
  )

/** A merge of `big` at a read of `meta:seq` `read`, which another merge overtakes, run first with `{ big: 'y' }` in a
 *  room of its own: the length of its event besides the value, at the seq it writes. */
async function overtakenMerge(admin: Redis, id: string, read: number, value: string) {
  vi.restoreAllMocks()
  await Room.create(id)
  await Room.updateMeta(id, { a: 'x' })
  await seqAt(admin, id, read)
  const write = metaWrites(id, true)
  const merging = told(Room.updateMeta(id, { big: value }))
  await write.held
  await Room.updateMeta(id, { b: '1' })
  const before = await snapshot(admin)
  write.release()
  const answer = await merging
  const unchanged = isDeepStrictEqual(await snapshot(admin), before)
  return { answer, unchanged, commits: write.commits }
}

describe.runIf(runs)('a room meta update whose event the commit would refuse', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a first write one byte over is refused before any write, and one byte shorter commits`, async () => {
      const admin = await reached(store)
      const probe = `meta-first-probe-${i}`
      await Room.create(probe)
      const probed = metaWrites(probe, false).commits
      await Room.updateMeta(probe, { big: 'y' })
      const besides = (probed[0]?.bytes ?? Number.NaN) - 1
      vi.restoreAllMocks()
      const over = `meta-first-over-${i}`
      await Room.create(over)
      const before = await snapshot(admin)
      const answer = await told(Room.updateMeta(over, { big: 'y'.repeat(MAX + 1 - besides) }))
      const unchanged = isDeepStrictEqual(await snapshot(admin), before)
      const refusal = fields(await commitRefusal(over, MAX + 1))
      const fits = `meta-first-fits-${i}`
      await Room.create(fits)
      const { commits } = metaWrites(fits, false)
      const fitting = await told(Room.updateMeta(fits, { big: 'y'.repeat(MAX - besides) }))
      console.log(JSON.stringify({ store: store.name, besides, answer, unchanged, fitting, commits }))
      admin.disconnect()

      expect({ answer, unchanged, fitting, commits }).toEqual({
        answer: refusal,
        unchanged: true,
        fitting: 'resolved',
        commits: [{ bytes: MAX, seq: '1' }],
      })
    }, 180_000)

    for (const written of [10, 100000000000000, 1000000000000000]) {
      it(`${store.name}: a merge that predicts seq ${written - 1} and writes ${written}, one byte over there, is refused before any write; one byte shorter commits`, async () => {
        const admin = await reached(store)
        const probe = await overtakenMerge(admin, `meta-merge-probe-${written}-${i}`, written - 2, 'y')
        const commit = probe.commits.find(({ seq }) => seq === String(written))
        const besides = (commit?.bytes ?? Number.NaN) - 1
        const over = await overtakenMerge(
          admin,
          `meta-merge-over-${written}-${i}`,
          written - 2,
          'y'.repeat(MAX + 1 - besides),
        )
        const refusal = fields(await commitRefusal(`meta-merge-over-${written}-${i}`, MAX + 1))
        const fits = await overtakenMerge(
          admin,
          `meta-merge-fits-${written}-${i}`,
          written - 2,
          'y'.repeat(MAX - besides),
        )
        const seen = {
          over: { answer: over.answer, unchanged: over.unchanged },
          fits: { answer: fits.answer, written: fits.commits.at(-1) },
        }
        console.log(JSON.stringify({ store: store.name, written, besides, ...seen }))
        admin.disconnect()

        expect(seen).toEqual({
          over: { answer: refusal, unchanged: true },
          fits: { answer: 'resolved', written: { bytes: MAX, seq: String(written) } },
        })
      }, 300_000)
    }

    it(`${store.name}: a replacement a merge overtook, one byte over at the seq it writes, is refused before any write; one byte shorter commits`, async () => {
      const admin = await reached(store)
      const replaced = async (id: string, value: string) => {
        vi.restoreAllMocks()
        await Room.create(id)
        await Room.updateMeta(id, { a: 'x' })
        await seqAt(admin, id, 8)
        const write = metaWrites(id, true)
        const replacing = told(Room.setMeta(id, { big: value }))
        await write.held
        await Room.updateMeta(id, { b: '1' })
        const before = await snapshot(admin)
        write.release()
        const answer = await replacing
        const unchanged = isDeepStrictEqual(await snapshot(admin), before)
        return { answer, unchanged, commits: write.commits, meta: Object.keys((await Room.get(id)).meta) }
      }
      const probe = await replaced(`meta-replace-probe-${i}`, 'y')
      const besides = (probe.commits.find(({ seq }) => seq === '10')?.bytes ?? Number.NaN) - 1
      const over = await replaced(`meta-replace-over-${i}`, 'y'.repeat(MAX + 1 - besides))
      const refusal = fields(await commitRefusal(`meta-replace-over-${i}`, MAX + 1))
      const fits = await replaced(`meta-replace-fits-${i}`, 'y'.repeat(MAX - besides))
      console.log(JSON.stringify({ store: store.name, besides, over, fits }))
      admin.disconnect()

      expect({ over, fits }).toEqual({
        over: {
          answer: refusal,
          unchanged: true,
          commits: [{ bytes: expect.any(Number), seq: '9' }],
          meta: ['a', 'b'],
        },
        fits: {
          answer: 'resolved',
          unchanged: false,
          commits: [
            { bytes: expect.any(Number), seq: '9' },
            { bytes: MAX, seq: '10' },
          ],
          meta: ['big'],
        },
      })
    }, 300_000)
  }
})

describe.runIf(runs)('the value that numbers a room meta update', () => {
  for (const store of STORES) {
    describe(store.name, () => {
      // The store starts once, on a budget of its own: no row's 5 s takes a store start.
      let admin: Redis
      beforeAll(async () => {
        admin = await reached(store)
      }, STORE_START_MS)
      afterAll(() => admin.disconnect())

      it(`a replacement whose room closed and opened again before its exchange is told it closed, and writes nothing`, async () => {
        const id = 'meta-reopened'
        await Room.create(id)
        await Room.updateMeta(id, { a: 'x' })
        const write = metaWrites(id, true)
        const replacing = told(Room.setMeta(id, { big: 'y' }))
        await write.held
        await Room.close(id)
        await Room.create(id)
        await Room.updateMeta(id, { seed: 's' })
        const seeded = write.commits.length
        const before = await snapshot(admin)
        write.release()
        const answer = await replacing
        const unchanged = isDeepStrictEqual(await snapshot(admin), before)
        const commits = write.commits.slice(seeded)
        const meta = Object.keys((await Room.get(id)).meta)

        expect({ answer, unchanged, commits, meta }).toEqual({
          answer: expect.objectContaining({ class: 'RoomError', message: `Room is closed: ${id}` }),
          unchanged: true,
          commits: [],
          meta: ['seed'],
        })
      })

      it(`a first write another first write overtook writes as a replacement, at the next seq`, async () => {
        const id = 'meta-first-overtaken'
        await Room.create(id)
        const write = metaWrites(id, true)
        const replacing = told(Room.setMeta(id, { big: 'y' }))
        await write.held
        await Room.setMeta(id, { seed: 's' })
        write.release()
        const answer = await replacing
        const meta = Object.keys((await Room.get(id)).meta)

        expect({ answer, seqs: write.commits.map(({ seq }) => seq), meta }).toEqual({
          answer: 'resolved',
          seqs: ['1', '2'],
          meta: ['big'],
        })
      })
    })
  }
})
