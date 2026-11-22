// What a `Room.publish()` or `Room.send()` that the store refuses leaves in it, on the oldest and the newest Redis and
// Valkey Telefunc supports.
// - A `Room.publish()` refused at a call its commit makes past its first write: nothing is written, the call is told it
//   didn't run, and the room's next message takes the next seq. So is one whose commit finds a key of another type.
// - A `Room.publish()` or `Room.send()` of data, or a `Room.removeParticipant()` with a reason, over the size the store
//   commits: refused before the call's first write, also where finding the participant would reap a lapsed lease.
import type { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { Room } from '@telefunc/room'
import { changedKeys, endReached, reached, runs, snapshot, STORES } from '../test/stores.js'

afterAll(() => endReached())

/** Over what the Redis backend commits at most, 28 MiB (packages/redis/src/commands.ts). */
const LARGE = 'x'.repeat(29 * 1024 * 1024)

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
  await admin.hset(values, key, JSON.stringify({ until: 0 }))
  await admin.hset(`${incarnation}:versions`, key, '1')
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
      it(`${store.name}: Room.send() by ${by}, where finding the participant would reap a lapsed lease, is refused before its first write`, async () => {
        const admin = await reached(store)
        const id = `send-large-${i}-${by}`
        await Room.create(id)
        const participant = await Room.join(id, { identity: 'u1' })
        await lapsedLease(admin, id)
        const held = await snapshot(admin)
        const sending =
          by === 'id' ? Room.send(id, { id: participant.id }, LARGE) : Room.send(id, { identity: 'u1' }, LARGE)
        const refused = await sending.then(() => 'resolved', told)
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
