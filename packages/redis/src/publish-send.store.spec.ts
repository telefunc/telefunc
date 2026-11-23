// What a `Room.publish()` or `Room.send()` that the store refuses leaves in it, on the oldest and the newest Redis and
// Valkey Telefunc supports.
// - A `Room.publish()` refused at a call its commit makes past its first write: nothing is written, the call is told it
//   didn't run, and the room's next message takes the next seq. So is one whose commit finds a key of another type.
// - A `Room.publish()`, a `Room.send()` or a participant's `send()` of data, or a `Room.removeParticipant()` with a
//   reason, over the size the store commits: refused before the call's first write, also where finding the participant
//   would reap a lapsed lease.
// - A participant's `send()` delivers the message its send guard let through; one the guard made over the size is
//   refused before its commit's first write.
import type { Redis } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { Abort } from 'telefunc'
import { Room } from '@telefunc/room'
import { atAnotherServer, hostingLease, joinedByAnotherServer } from '../test/servers.js'
import { changedKeys, endReached, reached, runs, snapshot, STORES } from '../test/stores.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'

afterAll(() => endReached())
afterEach(() => vi.restoreAllMocks())

/** What the Redis backend commits at most, 28 MiB (packages/redis/src/commands.ts). */
const CEILING = 28 * 1024 * 1024
/** Over the ceiling. */
const LARGE = 'x'.repeat(29 * 1024 * 1024)

/** From here on, counts the reads of `member`'s record by id: the store lookup `Room.send()` makes, and a participant's
 *  `send()` to a participant outside its view (packages/room/src/server/sizeBeforeWrite.spec.ts counts it there). */
function countLookups(member: string): { count: number } {
  const lookups = { count: 0 }
  const { readCells } = RedisRoomDriver.prototype
  vi.spyOn(RedisRoomDriver.prototype, 'readCells').mockImplementation(async function (
    this: RedisRoomDriver,
    roomId,
    inc,
    sel,
  ) {
    if ('keys' in sel && sel.keys.includes(`m:${member}`)) lookups.count++
    return await readCells.bind(this)(roomId, inc, sel)
  })
  return lookups
}

/** The reason at which a hidden tab's leave with it is `bytes` long: the leave Room commits for it, a JSON record. */
function reasonForHiddenLeave(bytes: number): string {
  const leave = (reason: string) =>
    JSON.stringify({ __r: 'leave', id: crypto.randomUUID(), cause: 'removed', reason, hidden: true }).length
  return 'x'.repeat(bytes - leave(''))
}

function told(error: unknown) {
  if (!(error instanceof Error)) return { value: typeof error }
  return {
    name: error.name,
    code: 'code' in error ? String(error.code) : 'none',
    outcome: 'outcome' in error ? String(error.outcome) : 'none',
  }
}

/** A lease of another server in room `id` that lapsed, hosting nothing, written as the backend writes a cell: a call
 *  that finds a participant there reaps it. */
async function lapsedLease(admin: Redis, id: string): Promise<void> {
  const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
  if (values === undefined) throw new Error(`room ${id} has no cells`)
  const incarnation = values.slice(0, -':values'.length)
  const key = 'session:lapsed'
  await admin.zadd(`${incarnation}:cells`, 0, key)
  await admin.hset(values, key, '')
  await admin.hset(`${incarnation}:versions`, key, '1')
  await admin.hset(`${incarnation}:lapses`, key, '0')
}

// The ACL rules that refuse one call of the commit past its first write (its lane's order), and the rules that allow it
// again.
const REFUSALS = [
  { call: "PUBLISH on the room's channel", refuse: ['resetchannels'], allow: ['&*'] },
  { call: "SADD to the incarnation's keys", refuse: ['-sadd'], allow: ['+sadd'] },
] as const

describe.runIf(runs)('Room.publish() refused at a call past its first write', () => {
  for (const [i, store] of STORES.entries()) {
    for (const [r, { call, refuse, allow }] of REFUSALS.entries()) {
      for (const first of [true, false]) {
        it(`${store.name}: ${call}, on ${first ? 'the lane’s first message' : 'a lane with a message'}: nothing written, told it didn't run, the next message takes the next seq`, async () => {
          const admin = await reached(store)
          const id = `publish-refused-${i}-${r}-${first ? 'first' : 'later'}`
          await Room.create(id)
          const before = first ? 0 : (await Room.publish(id, 'before')).seq
          const held = await snapshot(admin)
          await admin.call('ACL', 'SETUSER', 'telefunc', ...refuse)
          const refused = await Room.publish(id, 'refused').then(() => 'resolved', told)
          const changed = changedKeys(held, await snapshot(admin))
          await admin.call('ACL', 'SETUSER', 'telefunc', ...allow)
          const next = (await Room.publish(id, 'after')).seq
          admin.disconnect()

          expect({ refused, changed, next }).toEqual({
            refused: { name: 'BackendError', code: 'denied', outcome: 'not-run' },
            changed: [],
            next: before + 1,
          })
        }, 60_000)
      }
    }
  }
})

describe.runIf(runs)('Room.publish() whose commit finds a key of another type than Telefunc writes', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: the incarnation's key set held as a string: nothing written, told it didn't run`, async () => {
      const admin = await reached(store)
      const id = `publish-type-${i}`
      await Room.create(id)
      await Room.publish(id, 'before')
      const [keys] = await admin.keys(`tf:0:room:{${id}}:g:*:keys`)
      if (keys === undefined) throw new Error(`room ${id} has no key set`)
      await admin.del(keys)
      await admin.set(keys, 'not a set')
      const held = await snapshot(admin)
      const refused = await Room.publish(id, 'refused').then(() => 'resolved', told)
      const changed = changedKeys(held, await snapshot(admin))
      admin.disconnect()

      expect({ refused, changed }).toMatchObject({
        refused: { name: 'BackendError', outcome: 'not-run' },
        changed: [],
      })
    }, 60_000)
  }
})

describe.runIf(runs)('Room.publish() and Room.send() of data over the size the store commits', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: Room.publish() is refused before its first write`, async () => {
      const admin = await reached(store)
      const id = `publish-large-${i}`
      await Room.create(id)
      const held = await snapshot(admin)
      const refused = await Room.publish(id, LARGE).then(() => 'resolved', told)
      const changed = changedKeys(held, await snapshot(admin))
      admin.disconnect()

      expect({ refused, changed }).toEqual({
        refused: { name: 'ChannelOverflowError', code: 'none', outcome: 'none' },
        changed: [],
      })
    }, 60_000)

    const metaCalls = [
      ['Room.setMeta()', (id: string) => Room.setMeta(id, { big: LARGE })],
      ['Room.updateMeta()', (id: string) => Room.updateMeta(id, { big: LARGE })],
      ['Room.join()', (id: string) => Room.join(id, { meta: { big: LARGE } })],
    ] as const
    for (const [c, [name, call]] of metaCalls.entries()) {
      it(`${store.name}: ${name} with meta over the size, in a room with a lapsed lease, is refused before its first write`, async () => {
        const admin = await reached(store)
        const id = `meta-large-${i}-${c}`
        await Room.create(id)
        const participant = await Room.join(id)
        await lapsedLease(admin, id)
        const held = await snapshot(admin)
        const refused = await call(id).then(() => 'resolved', told)
        const changed = changedKeys(held, await snapshot(admin))
        await participant.leave()
        admin.disconnect()

        expect({ refused, changed }).toEqual({
          refused: { name: 'ChannelOverflowError', code: 'none', outcome: 'none' },
          changed: [],
        })
      }, 60_000)
    }

    for (const by of ['id', 'identity'] as const) {
      it(`${store.name}: Room.send() by ${by}, found by the store lookup a participant's send() to a participant outside its view takes too, where that lookup would reap a lapsed lease, is refused before its first write`, async () => {
        const admin = await reached(store)
        const id = `send-large-${i}-${by}`
        await Room.create(id)
        const participant = await Room.join(id, { identity: 'u1' })
        await lapsedLease(admin, id)
        const held = await snapshot(admin)
        const lookups = countLookups(participant.id)
        const sending =
          by === 'id' ? Room.send(id, { id: participant.id }, LARGE) : Room.send(id, { identity: 'u1' }, LARGE)
        const refused = await sending.then(() => 'resolved', told)
        vi.restoreAllMocks()
        const changed = changedKeys(held, await snapshot(admin))
        await participant.leave()
        admin.disconnect()

        expect({ refused, lookups: lookups.count, changed }).toEqual({
          refused: { name: 'ChannelOverflowError', code: 'none', outcome: 'none' },
          lookups: 1,
          changed: [],
        })
      }, 60_000)
    }

    it(`${store.name}: a participant's send() of data over the size, to a participant of a room another server created and joined it to, found by its lookup in the store, where that lookup would reap a lapsed lease, is refused before its first write`, async () => {
      const admin = await reached(store)
      const id = `participant-send-large-${i}`
      const printed = await atAnotherServer(
        String(admin.options.host),
        id,
        'await Room.create(id); console.log((await Room.join(id)).id)',
      )
      const recipient = printed.at(-1)
      if (recipient === undefined || recipient === '') throw new Error('the other server joined no one')
      const sender = await Room.join(id)
      await lapsedLease(admin, id)
      const held = await snapshot(admin)
      const lookups = countLookups(recipient)
      const refused = await sender.send(recipient, LARGE).then(() => 'resolved', told)
      vi.restoreAllMocks()
      const changed = changedKeys(held, await snapshot(admin))
      await sender.leave()
      admin.disconnect()

      expect({ refused, lookups: lookups.count, changed }).toEqual({
        refused: { name: 'ChannelOverflowError', code: 'none', outcome: 'none' },
        lookups: 1,
        changed: [],
      })
    }, 60_000)

    for (const by of ['id', 'identity'] as const) {
      it(`${store.name}: Room.removeParticipant() by ${by} with a reason over the size, where finding the participant would reap a lapsed lease, is refused before its first write`, async () => {
        const admin = await reached(store)
        const id = `remove-large-${i}-${by}`
        await Room.create(id)
        const participant = await Room.join(id, { identity: 'u1' })
        await lapsedLease(admin, id)
        const held = await snapshot(admin)
        const target = by === 'id' ? { id: participant.id } : { identity: 'u1' }
        const refused = await Room.removeParticipant(id, { ...target, reason: LARGE }).then(() => 'resolved', told)
        const changed = changedKeys(held, await snapshot(admin))
        await participant.leave()
        admin.disconnect()

        expect({ refused, changed }).toEqual({
          refused: { name: 'ChannelOverflowError', code: 'none', outcome: 'none' },
          changed: [],
        })
      }, 60_000)
    }
  }
})

describe.runIf(runs)(
  'Room.removeParticipant() by identity of a visible and a hidden tab at the size the store commits',
  () => {
    for (const [i, store] of STORES.entries()) {
      for (const over of [0, 1]) {
        it(`${store.name}: with a reason whose hidden leave is ${over === 0 ? 'the size exactly: removes both' : 'one byte over the size: is refused, and writes nothing'}`, async () => {
          const admin = await reached(store)
          const id = `remove-threshold-${i}-${over}`
          await Room.create(id)
          const visible = await Room.join(id, { identity: 'u1' })
          const hidden = await Room.join(id, { identity: 'u1', hidden: true })
          const held = await snapshot(admin)
          const reason = reasonForHiddenLeave(CEILING + over)
          const answer = await Room.removeParticipant(id, { identity: 'u1', reason }).then(() => 'resolved', told)
          const changed = changedKeys(held, await snapshot(admin))
          const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
          const kept = values === undefined ? [] : await admin.hkeys(values)
          if (over === 1) await Promise.all([visible.leave(), hidden.leave()])
          admin.disconnect()

          expect({
            answer,
            wrote: changed.length > 0,
            visible: kept.includes(`m:${visible.id}`),
            hidden: kept.includes(`m:${hidden.id}`),
          }).toEqual(
            over === 0
              ? { answer: 'resolved', wrote: true, visible: false, hidden: false }
              : {
                  answer: { name: 'ChannelOverflowError', code: 'none', outcome: 'none' },
                  wrote: false,
                  visible: true,
                  hidden: true,
                },
          )
        }, 60_000)
      }
    }
  },
)

describe.runIf(runs)('Room.send() by identity of a tab that hosts and one on a lapsed lease', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: checks the tab that hosts, as the tab on the lapsed lease hosts no one, before and after the reap, and delivers to it`, async () => {
      const admin = await reached(store)
      const id = `send-checked-reaped-${i}`
      await Room.create(id)
      const first = await joinedByAnotherServer(String(admin.options.host), id, { identity: 'u1' })
      const left = await Room.join(id, { identity: 'u1' })
      const { values, lease } = await hostingLease(admin, id, first)
      await admin.hset(`${values.slice(0, -':values'.length)}:lapses`, `session:${lease}`, '0')
      const { assertCommittable } = RedisRoomDriver.prototype
      const checked: string[] = []
      vi.spyOn(RedisRoomDriver.prototype, 'assertCommittable').mockImplementation(function (
        this: RedisRoomDriver,
        ...sized
      ) {
        const [lane] = sized
        if (lane.kind === 'inbox') checked.push(lane.member)
        assertCommittable.apply(this, sized)
      })
      const receipts = await Room.send(id, { identity: 'u1' }, 'small')
      vi.restoreAllMocks()
      const listed = (await Room.getParticipants(id)).map((each) => each.id)
      await left.leave()
      admin.disconnect()

      // A tab on a lapsed lease hosts no one, so the lookup leaves it out: the check runs on the first read and on the
      // reread after the reap, each time on the tab that hosts.
      expect({ checked, delivered: receipts.length, listed }).toEqual({
        checked: [left.id, left.id],
        delivered: 1,
        listed: [left.id],
      })
    }, 60_000)
  }
})

describe.runIf(runs)("a participant's send() over the size, refused by its send guard too", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: to a participant in the view, found with no store lookup, is refused for its size before its guard runs`, async () => {
      const admin = await reached(store)
      const id = `participant-send-guarded-${i}`
      const room = await Room.create(id)
      let guarded = 0
      Room.guard(room, {
        anyoneCanJoin: true,
        onBeforeSend: () => {
          guarded++
          throw Abort('Test: the guard refuses every send')
        },
      })
      const sender = await room.join()
      const recipient = (await room.join()).id
      const lookups = countLookups(recipient)
      const oversized = await sender.send(recipient, LARGE).then(() => 'resolved', told)
      vi.restoreAllMocks()
      const guardedBefore = guarded
      const fitting = await sender.send(recipient, 'small').then(() => 'resolved', told)
      await Room.close(id)
      admin.disconnect()

      expect({ oversized, lookups: lookups.count, guardedBefore, fitting, guarded }).toEqual({
        oversized: { name: 'ChannelOverflowError', code: 'none', outcome: 'none' },
        lookups: 0,
        guardedBefore: 0,
        fitting: { name: 'Abort', code: 'none', outcome: 'none' },
        guarded: 1,
      })
    }, 60_000)
  }
})

describe.runIf(runs)("a participant's send() whose send guard changes the message", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: delivers the message the guard let through`, async () => {
      const admin = await reached(store)
      const id = `participant-send-redacted-${i}`
      const room = await Room.create(id)
      Room.guard(room, {
        anyoneCanJoin: true,
        onBeforeSend: (_from, _to, data) => {
          if (typeof data === 'object' && data !== null && 'text' in data) data.text = 'redacted'
        },
      })
      const sender = await room.join()
      const recipient = await room.join()
      recipient.listen((data) => data)
      const read = await sender.send(recipient.id, { text: 'private' }, { ack: true })
      await Room.close(id)
      admin.disconnect()

      expect(read).toEqual({ text: 'redacted' })
    }, 60_000)

    it(`${store.name}: one the guard made over the size is refused before its commit's first write, and delivers nothing`, async () => {
      const admin = await reached(store)
      const id = `participant-send-grown-${i}`
      const room = await Room.create(id)
      Room.guard(room, {
        anyoneCanJoin: true,
        onBeforeSend: (_from, _to, data) => {
          if (typeof data === 'object' && data !== null && 'text' in data) data.text = LARGE
        },
      })
      const sender = await room.join()
      const recipient = await room.join()
      const delivered: unknown[] = []
      recipient.listen((data) => void delivered.push(data))
      const held = await snapshot(admin)
      const grown = await sender.send(recipient.id, { text: 'small' }).then(() => 'resolved', told)
      const changed = changedKeys(held, await snapshot(admin))
      // The recipient answers a later send once it read it, after the refused one had it been delivered.
      await sender.send(recipient.id, 'later', { ack: true })
      await Room.close(id)
      admin.disconnect()

      expect({ grown, changed, delivered }).toEqual({
        grown: { name: 'ChannelOverflowError', code: 'none', outcome: 'none' },
        changed: [],
        delivered: ['later'],
      })
    }, 60_000)
  }
})
