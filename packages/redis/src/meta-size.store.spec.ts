// A room meta update whose event the store's commit would refuse, on the oldest and the newest Redis and Valkey
// Telefunc supports: the cells exchange refuses it before its first write, with the error that commit throws, and the
// store keeps every key and value as before, its revision and index included. The event's `seq` is the one the
// exchange writes, so the refusal and its twin one byte shorter, which commits, sit at a digit boundary:
// - a first write, at seq 1;
// - a merge whose read predicts seq 9, 99999999999999 or 999999999999999 while another merge commits first, so it
//   writes 10, 100000000000000 or 1000000000000000;
// - a replacement that a merge overtook, at the seq it writes.
// The value that numbers the event is always there: the first write creates `meta:seq`, and a later one counts it
// while `meta:gen`, written with it, is as read. A room closed and opened again before a replacement's exchange tells
// the replacement it closed, and a first write another first write overtook writes as a replacement. An exchange whose
// control record is numbered by a counter it leaves absent is refused as a bug, before its first write.
import { isDeepStrictEqual } from 'node:util'
import type { Redis } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { Room } from '@telefunc/room'
import { getRoomPlane, type CellsExchange, type ControlRecord } from '@telefunc/room/backend'
import { endReached, reached, runs, snapshot, STORES, type Store } from '../test/stores.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'

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

/** Each control-lane commit of room `id`: its length and its `seq`. */
function controlCommits(id: string): { bytes: number; seq: string }[] {
  const commits: { bytes: number; seq: string }[] = []
  const { commitLane } = RedisRoomDriver.prototype
  vi.spyOn(RedisRoomDriver.prototype, 'commitLane').mockImplementation(async function (
    this: RedisRoomDriver,
    target,
    payload,
    opts,
  ) {
    const committed = await commitLane.call(this, target, payload, opts)
    if (target.roomId === id && target.lane.kind === 'control' && committed.kind === 'accepted') {
      const head = new TextDecoder().decode(payload.subarray(0, 64))
      commits.push({ bytes: payload.byteLength, seq: /"seq":(\d+)/.exec(head)?.[1] ?? 'none' })
    }
    return committed
  })
  return commits
}

/** Until released, the first cells exchange of room `id` that writes the meta key `big` waits; the length its control
 *  record carries, once it reached the store. */
function holdBigWrite(id: string) {
  const { compareExchangeCells } = RedisRoomDriver.prototype
  const held = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  const carried = { bytes: Number.NaN }
  let holding = true
  vi.spyOn(RedisRoomDriver.prototype, 'compareExchangeCells').mockImplementation(async function (
    this: RedisRoomDriver,
    roomId: string,
    inc: string,
    exchange: CellsExchange,
    record?: ControlRecord,
  ) {
    if (holding && roomId === id && exchange.mutations.some(({ key }) => key.endsWith('"big"'))) {
      holding = false
      if (record !== undefined) carried.bytes = record.bytes
      held.resolve()
      await release.promise
    }
    return await compareExchangeCells.call(this, roomId, inc, exchange, record)
  })
  return { held: held.promise, release: () => release.resolve(), carried }
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
  const commits = controlCommits(id)
  const write = holdBigWrite(id)
  const merging = told(Room.updateMeta(id, { big: value }))
  await write.held
  await Room.updateMeta(id, { b: '1' })
  const before = await snapshot(admin)
  write.release()
  const answer = await merging
  const unchanged = isDeepStrictEqual(await snapshot(admin), before)
  return { answer, unchanged, commits, carried: write.carried.bytes }
}

describe.runIf(runs)('a room meta update whose event the commit would refuse', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a first write one byte over is refused before any write, and one byte shorter commits`, async () => {
      const admin = await reached(store)
      const probe = `meta-first-probe-${i}`
      await Room.create(probe)
      const probed = controlCommits(probe)
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
      const commits = controlCommits(fits)
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
        console.log(JSON.stringify({ store: store.name, written, besides, carried: over.carried, ...seen }))
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
        const commits = controlCommits(id)
        const write = holdBigWrite(id)
        const replacing = told(Room.setMeta(id, { big: value }))
        await write.held
        await Room.updateMeta(id, { b: '1' })
        const before = await snapshot(admin)
        write.release()
        const answer = await replacing
        const unchanged = isDeepStrictEqual(await snapshot(admin), before)
        return { answer, unchanged, commits, meta: Object.keys((await Room.get(id)).meta) }
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
    it(`${store.name}: an exchange whose record is numbered by a counter it leaves absent is a bug, and writes nothing`, async () => {
      const admin = await reached(store)
      const id = 'meta-unnumbered'
      await Room.create(id)
      const head = await getRoomPlane().direct.reads.readHead(id)
      if (head === null || head.currentInc === null) throw new Error(`room ${id} isn't open`)
      const inc = head.currentInc
      const before = await snapshot(admin)
      const exchange = {
        expect: [],
        mutations: [{ key: 'k', bytes: new Uint8Array([1]) }],
        counts: [{ key: 'absent', delta: 1 }],
      }
      const answer = await Promise.resolve()
        .then(() =>
          getRoomPlane().direct.compareExchangeCells(id, inc, exchange, {
            counter: 'absent',
            bytes: 1,
          }),
        )
        .then(
          () => 'exchanged',
          (error: unknown) => String(error),
        )
      const unchanged = isDeepStrictEqual(await snapshot(admin), before)
      admin.disconnect()

      expect({ answer, unchanged }).toEqual({
        answer: expect.stringMatching(
          /\[Bug\].*cells exchange: its control record is numbered by a counter it leaves absent/,
        ),
        unchanged: true,
      })
    })

    it(`${store.name}: a replacement whose room closed and opened again before its exchange is told it closed, and writes nothing`, async () => {
      const admin = await reached(store)
      const id = 'meta-reopened'
      await Room.create(id)
      await Room.updateMeta(id, { a: 'x' })
      const write = holdBigWrite(id)
      const replacing = told(Room.setMeta(id, { big: 'y' }))
      await write.held
      await Room.close(id)
      await Room.create(id)
      await Room.updateMeta(id, { seed: 's' })
      const commits = controlCommits(id)
      const before = await snapshot(admin)
      write.release()
      const answer = await replacing
      const unchanged = isDeepStrictEqual(await snapshot(admin), before)
      const meta = Object.keys((await Room.get(id)).meta)
      admin.disconnect()

      expect({ answer, unchanged, commits, meta }).toEqual({
        answer: expect.objectContaining({ class: 'RoomError', message: `Room is closed: ${id}` }),
        unchanged: true,
        commits: [],
        meta: ['seed'],
      })
    })

    it(`${store.name}: a first write another first write overtook writes as a replacement, at the next seq`, async () => {
      const admin = await reached(store)
      const id = 'meta-first-overtaken'
      await Room.create(id)
      const commits = controlCommits(id)
      const write = holdBigWrite(id)
      const replacing = told(Room.setMeta(id, { big: 'y' }))
      await write.held
      await Room.setMeta(id, { seed: 's' })
      write.release()
      const answer = await replacing
      const meta = Object.keys((await Room.get(id)).meta)
      admin.disconnect()

      expect({ answer, seqs: commits.map(({ seq }) => seq), meta }).toEqual({
        answer: 'resolved',
        seqs: ['1', '2'],
        meta: ['big'],
      })
    })
  }
})
