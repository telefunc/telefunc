// A DM's acknowledged reply from a server recipient, over the size the store commits, on the oldest and the newest
// Redis and Valkey Telefunc supports: a path where a refused size could leave a Room call's own effect half applied.
// The stored reply and the frame are the same bytes, the small overflow in the reply's place, and the sender is told
// that, by the frame, or by the stored reply once its wait times out with the frame lost. A timeout read before the
// reply is stored stays a timeout. A reply whose value changes between reads is stored as the frame carries it, and one
// that throws at a read after its first encode is stored and sent as that encode took it. A reply, a listener's failure
// and an overflow whose frame was lost are settled from the store by the next frame in the sender's inbox, which shows
// the skip. A timeout's read that finds nothing doesn't settle the sender ahead of a frame that settles it first. A
// sender removed, or a room closed, before the reply is stored is told that, with no reply left stored. A reply changed
// after its encode is stored, sent and told as encoded. The DM's own send failing after its reply settled the wait is
// what the sender is told.
import { setTimeout as sleep } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { Room } from '@telefunc/room'
import {
  getRoomPlane,
  type CommitOptions,
  type CommitResult,
  type CommitTarget,
  type OperationOptions,
  type OperationResult,
  type RoomOperation,
} from '@telefunc/room/backend'
import { forwarder, removeStores, runs, STORES, startStore, type Store } from '../test/stores.js'
import { installRedis } from './index.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'

let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
let app: Redis | undefined
afterAll(() => {
  app?.disconnect()
  proxy?.close()
  removeStores()
})
afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

/** A new store of the row's, which the backend reaches through the forwarder; reached once a read of a room that isn't
 *  there gets past the store. */
async function reached(store: Store): Promise<Redis> {
  if (proxy === undefined) {
    proxy = await forwarder()
    app = new Redis({ port: proxy.port, username: 'telefunc', password: 'pw', lazyConnect: true })
    app.on('error', () => {})
    installRedis(app)
  }
  removeStores()
  const { host, admin } = await startStore(store)
  await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
  proxy.to(host)
  const answers = () =>
    Room.get('r-none').then(
      () => true,
      (error: unknown) => !/ECONNRE|Connection is closed|ETIMEDOUT|EPIPE|connect/i.test(String(error)),
    )
  for (let tries = 0; !(await answers()); tries++) {
    if (tries > 200) throw new Error(`the backend never reached ${store.name}`)
    await sleep(25)
  }
  return admin
}

/** What the Redis backend commits at most, in bytes (packages/redis/src/commands.ts). */
const CEILING = 28 * 1024 * 1024
const LARGE = 'x'.repeat(29 * 1024 * 1024)

function told(error: unknown) {
  if (!(error instanceof Error)) return { value: typeof error }
  return { name: error.name, outcome: 'outcome' in error ? String(error.outcome) : 'none' }
}

async function valuesKey(admin: Redis, id: string): Promise<string> {
  const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
  if (values === undefined) throw new Error(`room ${id} has no cells`)
  return values
}

async function until(check: () => Promise<boolean>, ms: number): Promise<boolean> {
  for (const end = Date.now() + ms; Date.now() < end; await sleep(20)) if (await check()) return true
  return check()
}

/** When a DM's reply goes out. */
type Schedule = {
  /** Held until the stored reply was read, or lost. */
  frame: 'lost' | 'held'
  /** When the reply is stored: at once; once the sender's wait timed out; or while the timeout's read, which finds
   *  nothing, is on its way, and that read then returns only once the frame settled the sender. */
  store?: 'at once' | 'after the timeout' | 'during the timeout read'
  /** Runs as the recipient starts storing its reply, its encode done. */
  beforeStore?: (room: { id: string; sender: string }) => Promise<unknown> | void
  /** The DM's own send fails once its reply settled the sender's wait. */
  sendFails?: boolean
}

function sent(value: unknown) {
  return { resolved: typeof value === 'string' ? `${value.length} characters` : value }
}

/** A DM `bob` sends to `alice` with `{ ack: true }`, which alice's listener answers with `reply()`, in a new room on
 *  `admin`'s store, its reply sent as `schedule` says. What is stored, what the frame carried and how it settled, how
 *  long the sender waited on its clock, in steps of 5 s, and what it was told. */
async function acknowledged(
  admin: Redis,
  id: string,
  reply: () => unknown,
  { frame, store = 'at once', beforeStore, sendFails = false }: Schedule,
) {
  const room = await Room.create(id)
  const alice = await room.join({ meta: { name: 'alice' } })
  const bob = await room.join({ meta: { name: 'bob' } })
  alice.listen(reply)
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const { direct } = getRoomPlane()
  const commit = direct.commitLane.bind(direct)
  const frameHeld = Promise.withResolvers<void>()
  const carried = Promise.withResolvers<{ bytes: string; settled: unknown }>()
  let sender: unknown = 'pending'
  const settledSender = () => until(async () => sender !== 'pending', 30_000)
  vi.spyOn(direct, 'commitLane').mockImplementation(
    async (target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> => {
      if (target.lane.kind !== 'inbox' || target.lane.member !== bob.id) return commit(target, payload, opts)
      const bytes = Buffer.from(payload).toString()
      if (frame === 'lost') {
        carried.resolve({ bytes, settled: 'lost' })
        return { kind: 'stale-cell', key: 'the row lost the frame' }
      }
      await frameHeld.promise
      return commit(target, payload, opts).then(
        (result) => {
          carried.resolve({ bytes, settled: result.kind === 'accepted' ? 'committed' : result.kind })
          return result
        },
        (error: unknown) => {
          carried.resolve({ bytes, settled: told(error) })
          throw error
        },
      )
    },
  )
  if (sendFails) {
    // The DM's own commit goes through the call's store, which reaches the driver.
    const driverCommit = RedisRoomDriver.prototype.commitLane
    vi.spyOn(RedisRoomDriver.prototype, 'commitLane').mockImplementation(async function (
      this: RedisRoomDriver,
      target: CommitTarget,
      payload: Uint8Array,
      opts?: CommitOptions,
    ): Promise<CommitResult> {
      const committed = await driverCommit.call(this, target, payload, opts)
      if (target.lane.kind !== 'inbox' || target.lane.member !== alice.id || committed.kind !== 'accepted')
        return committed
      // Fails once the reply's frame settled the sender's wait.
      await carried.promise
      await sleep(200)
      throw new Error('Test: the send fails after its reply')
    })
  }
  const run = direct.runOperation.bind(direct)
  const storeHeld = Promise.withResolvers<void>()
  vi.spyOn(direct, 'runOperation').mockImplementation(async function <Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    if (operation.name === 'storeDmReply') {
      await beforeStore?.({ id, sender: bob.id })
      if (store !== 'at once') await storeHeld.promise
    }
    return run(roomId, inc, operation, args, options)
  })
  const { reads } = direct
  const read = reads.readCells.bind(reads)
  vi.spyOn(reads, 'readCells').mockImplementation(async (roomId, inc, sel) => {
    const cells = await read(roomId, inc, sel)
    const isTimeoutRead = 'keys' in sel && sel.keys.some((key) => key.startsWith(`ack:${bob.id}:`))
    if (isTimeoutRead && store === 'during the timeout read') {
      storeHeld.resolve()
      await settledSender()
    }
    return cells
  })
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
  const answer = bob.send(alice.id, 'question', { ack: true }).then(
    (value: unknown) => {
      sender = sent(value)
    },
    (error: unknown) => {
      sender = { ...told(error), message: error instanceof Error ? error.message : '' }
    },
  )
  const values = await valuesKey(admin, id)
  const replyField = async () => (await admin.hkeys(values)).find((field) => field.startsWith(`ack:${bob.id}:`))
  let stored: string | null = null
  if (store === 'at once') {
    await until(async () => (await replyField()) !== undefined || sender !== 'pending', 30_000)
    const field = await replyField()
    stored = field === undefined ? null : await admin.hget(values, field)
  }
  frameHeld.resolve()
  let waited = 0
  for (await sleep(200); sender === 'pending' && waited < 120_000; await sleep(50)) {
    await vi.advanceTimersByTimeAsync(5_000)
    waited += 5_000
  }
  vi.useRealTimers()
  storeHeld.resolve()
  await answer
  const framed = await Promise.race([carried.promise, sleep(30_000).then(() => ({ bytes: '', settled: 'none' }))])
  // What a reply that settled no one leaves stored.
  await sleep(500)
  const left = (await admin.exists(values)) === 1 && (await replyField()) !== undefined
  return { stored, frame: framed, waited, sender, left }
}

/** A DM `bob` sends to `alice` with `{ ack: true }` in a new room on `admin`'s store, whose reply's frame is lost:
 *  `answer` is alice's listener. Once the reply is stored, the row takes the seq the frame would have had in bob's inbox,
 *  as a frame lost on its way would, and alice sends bob a DM, whose frame shows bob's inbox skipped one. How the sender
 *  settled, how long it waited on its clock, in steps of 5 s, whether bob's stored replies were read, and whether the
 *  reply is still stored. */
async function acknowledgedThroughGap(admin: Redis, id: string, answer: (data: unknown) => unknown) {
  const room = await Room.create(id)
  const alice = await room.join({ meta: { name: 'alice' } })
  const bob = await room.join({ meta: { name: 'bob' } })
  alice.listen(answer)
  vi.spyOn(console, 'error').mockImplementation(() => {})
  // A first DM, so bob's inbox has a seq of its own to skip past.
  await alice.send(bob.id, 'before')
  const { direct } = getRoomPlane()
  const commit = direct.commitLane.bind(direct)
  vi.spyOn(direct, 'commitLane').mockImplementation(
    async (target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> => {
      const isReply =
        target.lane.kind === 'inbox' &&
        target.lane.member === bob.id &&
        Buffer.from(payload.subarray(0, 16)).toString() === '{"__r":"dm-ack",'
      return isReply ? { kind: 'stale-cell', key: 'the row lost the frame' } : commit(target, payload, opts)
    },
  )
  const { reads } = direct
  const pages = reads.readCellPages.bind(reads)
  let repliesRead = false
  vi.spyOn(reads, 'readCellPages').mockImplementation((roomId, inc, prefix) => {
    if (prefix.startsWith(`ack:${bob.id}:`)) repliesRead = true
    return pages(roomId, inc, prefix)
  })
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
  let sender: unknown = 'pending'
  const settled = bob.send(alice.id, 'question', { ack: true }).then(
    (value: unknown) => {
      sender = sent(value)
    },
    (error: unknown) => {
      sender = { ...told(error), message: error instanceof Error ? error.message : '' }
    },
  )
  const values = await valuesKey(admin, id)
  const replyField = async () => (await admin.hkeys(values)).find((field) => field.startsWith(`ack:${bob.id}:`))
  await until(async () => (await replyField()) !== undefined, 30_000)
  const [order] = await admin.keys(`tf:0:room:{${id}}:g:*:om:${bob.id}`)
  const marks = order === undefined ? {} : await admin.hgetall(order)
  const [[lane, mark] = ['', '']] = Object.entries(marks)
  const [seq, ts] = mark.split(':')
  if (order !== undefined) await admin.hset(order, lane, `${Number(seq) + 1}:${ts}`)
  const after = alice.send(bob.id, 'after').catch((error: unknown) => told(error))
  let waited = 0
  for (await sleep(200); sender === 'pending' && waited < 120_000; await sleep(50)) {
    await vi.advanceTimersByTimeAsync(5_000)
    waited += 5_000
  }
  vi.useRealTimers()
  await settled
  await after
  const stillStored = (await replyField()) !== undefined
  return { skipped: Object.keys(marks).length === 1, sender, waited, repliesRead, stillStored }
}

describe.runIf(runs)("a DM's acknowledged reply from a server recipient, over the size the store commits", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: the frame first: the overflow is stored and sent in the reply's place, and the sender told it before its timeout`, async () => {
      const admin = await reached(store)
      const { stored, frame, waited, sender } = await acknowledged(admin, `reply-frame-${i}`, () => LARGE, {
        frame: 'held',
      })
      console.log(
        JSON.stringify({ store: store.name, storedBytes: stored?.length, frame: frame.settled, waited, sender }),
      )
      admin.disconnect()

      expect({
        storedIsFrame: stored === frame.bytes,
        storedBelowCeiling: (stored?.length ?? Infinity) < CEILING,
        frame: frame.settled,
        beforeTimeout: waited < 60_000,
        sender,
      }).toEqual({
        storedIsFrame: true,
        storedBelowCeiling: true,
        frame: 'committed',
        beforeTimeout: true,
        sender: { name: 'ChannelOverflowError', outcome: 'none', message: expect.any(String) },
      })
    }, 180_000)

    it(`${store.name}: the frame lost: the sender's timeout reads the stored overflow`, async () => {
      const admin = await reached(store)
      const { stored, frame, waited, sender } = await acknowledged(admin, `reply-lost-${i}`, () => LARGE, {
        frame: 'lost',
      })
      console.log(JSON.stringify({ store: store.name, storedBytes: stored?.length, waited, sender }))
      admin.disconnect()

      expect({ storedIsFrame: stored === frame.bytes, timedOut: waited >= 60_000, sender }).toEqual({
        storedIsFrame: true,
        timedOut: true,
        sender: { name: 'ChannelOverflowError', outcome: 'none', message: expect.any(String) },
      })
    }, 180_000)

    it(`${store.name}: the timeout's read first, before the reply is stored: the sender is told it timed out`, async () => {
      const admin = await reached(store)
      const { waited, sender } = await acknowledged(admin, `reply-late-${i}`, () => LARGE, {
        frame: 'held',
        store: 'after the timeout',
      })
      console.log(JSON.stringify({ store: store.name, waited, sender }))
      admin.disconnect()

      expect({ timedOut: waited >= 60_000, sender }).toEqual({
        timedOut: true,
        sender: { name: 'RoomError', outcome: 'none', message: expect.stringContaining('timed out') },
      })
    }, 180_000)

    it(`${store.name}: a reply small at its first reads and large after: what is stored is what the frame carries`, async () => {
      const admin = await reached(store)
      let reads = 0
      const changing = {
        get data() {
          reads++
          return reads <= 2 ? 'small' : LARGE
        },
      }
      const { stored, frame, waited, sender } = await acknowledged(admin, `reply-getter-${i}`, () => changing, {
        frame: 'held',
      })
      console.log(
        JSON.stringify({ store: store.name, reads, storedBytes: stored?.length, frame: frame.settled, waited, sender }),
      )
      admin.disconnect()

      expect({
        storedIsFrame: stored === frame.bytes,
        frame: frame.settled,
        beforeTimeout: waited < 60_000,
        sender,
      }).toEqual({
        storedIsFrame: true,
        frame: 'committed',
        beforeTimeout: true,
        sender: { resolved: { data: 'small' } },
      })
    }, 180_000)

    it(`${store.name}: the timeout's read on its way and finding nothing, the frame settling the sender before it returns: the sender is told what the frame carries`, async () => {
      const admin = await reached(store)
      const { frame, waited, sender } = await acknowledged(admin, `reply-race-${i}`, () => LARGE, {
        frame: 'held',
        store: 'during the timeout read',
      })
      console.log(JSON.stringify({ store: store.name, frame: frame.settled, waited, sender }))
      admin.disconnect()

      expect({ frame: frame.settled, timedOut: waited >= 60_000, sender }).toEqual({
        frame: 'committed',
        timedOut: true,
        sender: { name: 'ChannelOverflowError', outcome: 'none', message: expect.any(String) },
      })
    }, 180_000)

    for (const [j, [arm, before, message]] of (
      [
        [
          'the sender removed',
          ({ id, sender }) => Room.removeParticipant(id, { id: sender }),
          'Participant left the room',
        ],
        ['the room closed', ({ id }) => Room.close(id), 'Room is closed'],
      ] satisfies [string, NonNullable<Schedule['beforeStore']>, string][]
    ).entries()) {
      it(`${store.name}: ${arm} before the reply is stored: the sender is told that, and no reply is left stored`, async () => {
        const admin = await reached(store)
        const { sender, left } = await acknowledged(admin, `reply-gone-${j}-${i}`, () => LARGE, {
          frame: 'held',
          beforeStore: before,
        })
        console.log(JSON.stringify({ store: store.name, arm, sender, left }))
        admin.disconnect()

        expect({ sender, left }).toEqual({ sender: { name: 'RoomError', outcome: 'none', message }, left: false })
      }, 180_000)
    }

    it(`${store.name}: a reply changed after its encode: what is stored, sent and told is the reply as encoded`, async () => {
      const admin = await reached(store)
      const answer = { v: 'as encoded' }
      const { stored, frame, sender } = await acknowledged(admin, `reply-changed-${i}`, () => answer, {
        frame: 'held',
        beforeStore: () => {
          answer.v = 'changed'
        },
      })
      console.log(JSON.stringify({ store: store.name, stored, frame, sender }))
      admin.disconnect()

      expect({ storedIsFrame: stored === frame.bytes, frame: frame.settled, sender }).toEqual({
        storedIsFrame: true,
        frame: 'committed',
        sender: { resolved: { v: 'as encoded' } },
      })
    }, 180_000)

    it(`${store.name}: the DM's own send failing after its reply settled the wait: the sender is told the send's failure`, async () => {
      const admin = await reached(store)
      const { frame, sender, left } = await acknowledged(admin, `reply-send-fails-${i}`, () => 'small', {
        frame: 'held',
        sendFails: true,
      })
      console.log(JSON.stringify({ store: store.name, frame: frame.settled, sender, left }))
      admin.disconnect()

      expect({ frame: frame.settled, sender, left }).toEqual({
        frame: 'committed',
        sender: { name: 'Error', outcome: 'none', message: 'Test: the send fails after its reply' },
        left: false,
      })
    }, 180_000)

    for (const [j, [arm, answer, expected]] of (
      [
        ['a reply', () => 'small', { resolved: '5 characters' }],
        [
          'a listener that throws',
          () => {
            throw new Error('Test: the listener refuses')
          },
          { name: 'RoomError', outcome: 'none', message: expect.not.stringContaining('timed out') },
        ],
        ['an overflow', () => LARGE, { name: 'ChannelOverflowError', outcome: 'none', message: expect.any(String) }],
      ] as const
    ).entries()) {
      it(`${store.name}: ${arm}, its frame lost: a later frame in the sender's inbox settles it from the store`, async () => {
        const admin = await reached(store)
        const result = await acknowledgedThroughGap(admin, `reply-gap-${j}-${i}`, answer)
        console.log(JSON.stringify({ store: store.name, arm, ...result }))
        admin.disconnect()

        const { waited, ...settled } = result
        expect({ ...settled, beforeTimeout: waited < 60_000 }).toEqual({
          skipped: true,
          sender: expected,
          repliesRead: true,
          stillStored: false,
          beforeTimeout: true,
        })
      }, 180_000)
    }

    it(`${store.name}: a reply that throws at its third read: it is stored and sent as its first encode took it`, async () => {
      const admin = await reached(store)
      let reads = 0
      const throwing = {
        get data() {
          reads++
          if (reads === 3) throw new Error('Test: a reply read a third time')
          return 'small'
        },
      }
      const { stored, frame, waited, sender } = await acknowledged(admin, `reply-throw-${i}`, () => throwing, {
        frame: 'held',
      })
      console.log(JSON.stringify({ store: store.name, reads, frame: frame.settled, waited, sender }))
      admin.disconnect()

      expect({
        reads,
        storedIsFrame: stored === frame.bytes,
        frame: frame.settled,
        beforeTimeout: waited < 60_000,
        sender,
      }).toEqual({
        reads: 2,
        storedIsFrame: true,
        frame: 'committed',
        beforeTimeout: true,
        sender: { resolved: { data: 'small' } },
      })
    }, 180_000)
  }
})
