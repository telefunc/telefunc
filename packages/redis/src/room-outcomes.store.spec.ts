// What a Room call that the store refuses, or that finds nothing to act on, tells the app, as the Redis guide's table
// says, on the oldest and the newest Redis and Valkey Telefunc supports.
// - `Room.list()`: a room listed as open needs no repair of its listing, so the call wrote nothing, and it says it
//   didn't run; called again once the store allows the read, it lists the room. One that cleaned up a lapsed room on
//   its first page and fails on its second may have run: the call's writes are one account. So may one that cleaned
//   up a lapsed room beside the room on the same page whose read fails: the call settles once every step it started
//   did.
// - `Room.removeParticipant()` by id: a store's refusal is a `BackendError`, which says nothing of the participant;
//   another error means the participant is no longer in the room.
// - `Room.removeParticipant()` and `Room.send()` that reaped a lapsed lease while finding the participant, then were
//   refused: they may have run, as the reap wrote.
import { setTimeout as sleep } from 'node:timers/promises'
import { isDeepStrictEqual } from 'node:util'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { BackendError } from 'telefunc'
import { Room } from '@telefunc/room'
import { forwarder, removeStores, runs, STORES, startStore, type Store } from '../test/stores.js'
import { installRedis } from './index.js'

/** Every key of Telefunc's in the store, with its value as DUMP gives it. */
async function snapshot(admin: Redis): Promise<Map<string, Buffer | null>> {
  const keys = (await admin.keys('tf:*')).sort()
  return new Map(await Promise.all(keys.map(async (key) => [key, await admin.dumpBuffer(key)] as const)))
}

let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
let app: Redis | undefined
afterAll(() => {
  app?.disconnect()
  proxy?.close()
  removeStores()
})

/** A new store of the row's, which the backend reaches through the forwarder, logging in as `telefunc`. */
async function reached(store: Store): Promise<Redis> {
  if (proxy === undefined) {
    proxy = await forwarder()
    app = new Redis({ port: proxy.port, username: 'telefunc', password: 'pw', lazyConnect: true })
    app.on('error', () => {})
    installRedis(app)
  }
  const { host, admin } = await startStore(store)
  await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
  proxy.to(host)
  // Pointing the forwarder at this store dropped the backend's connections to the last one: a read gets past the
  // store, to a room that isn't there, once they are back.
  const answers = () =>
    Room.get('r-none').then(
      () => true,
      (error: unknown) => !(error instanceof BackendError),
    )
  for (let tries = 0; !(await answers()); tries++) {
    if (tries > 200) throw new Error(`the backend never reached ${store.name}`)
    await sleep(25)
  }
  return admin
}

describe.runIf(runs)('Room.list() whose read the store refuses', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: is told 'not-run' where nothing needed repair, and lists the room once allowed`, async () => {
      const admin = await reached(store)
      const id = `r-${i}`
      await Room.create(id)

      await admin.call('ACL', 'SETUSER', 'telefunc', '-hget')
      const before = await snapshot(admin)
      const refused = await Room.list().then(
        () => 'resolved',
        (error: unknown) => error,
      )
      const unchanged = isDeepStrictEqual(await snapshot(admin), before)
      await admin.call('ACL', 'SETUSER', 'telefunc', '+hget')
      const listed = (await Room.list()).map((room) => room.id)
      admin.disconnect()

      expect({ refused, unchanged, listed }).toMatchObject({
        refused: { name: 'BackendError', code: 'denied', outcome: 'not-run' },
        unchanged: true,
        listed: [id],
      })
    }, 60_000)

    it(`${store.name}: may have run where its first page cleaned up a lapsed room and its second page's read fails`, async () => {
      const admin = await reached(store)
      // Sorted by id: the lapsed room and 99 open ones fill the first page of 100, the last open one is the second.
      const lapsed = `a-${i}-lapsed`
      const open = Array.from({ length: 100 }, (_, n) => `b-${i}-${String(n).padStart(3, '0')}`)
      for (const id of [lapsed, ...open]) await Room.create(id)
      // Its head is gone, as once it lapsed: its listing and its incarnation's keys are left for a list to clean up.
      await admin.del(`tf:0:room:{${lapsed}}:head`)
      const lapsedKeys = () => admin.keys(`tf:0:room:{${lapsed}}:*`)
      const left = (await lapsedKeys()).length
      // HGET is refused on the second page's room only: a selector allows it on the directory and every other room.
      await admin.call(
        'ACL',
        'SETUSER',
        'telefunc',
        '-hget',
        `(+hget ~*room-dir:* ~*room:{a-${i}-* ~*room:{b-${i}-0[0-8]?}* ~*room:{b-${i}-09[0-8]}*)`,
      )
      const refused = await Room.list().then(
        () => 'resolved',
        (error: unknown) => error,
      )
      const cleaned = {
        keys: (await lapsedKeys()).length,
        listed: await admin.zscore('tf:0:room-dir:{tf:0:dir}:index', lapsed),
      }
      admin.disconnect()

      expect({ left: left > 0, refused, cleaned }).toMatchObject({
        left: true,
        refused: { name: 'BackendError', code: 'denied', outcome: 'may-have-run' },
        cleaned: { keys: 0, listed: null },
      })
    }, 60_000)

    it(`${store.name}: may have run where a room on the page whose read fails was cleaned up beside it`, async () => {
      const admin = await reached(store)
      // One page, in id order: the room whose head read the store refuses, then a lapsed one.
      const unread = `c-${i}-unread`
      const lapsed = `c-${i}-z-lapsed`
      for (const id of [unread, lapsed]) await Room.create(id)
      await admin.del(`tf:0:room:{${lapsed}}:head`)
      const lapsedKeys = () => admin.keys(`tf:0:room:{${lapsed}}:*`)
      const left = (await lapsedKeys()).length
      // GET is refused but on the lapsed room's keys: the first room's head read fails, the lapsed room's goes through.
      await admin.call('ACL', 'SETUSER', 'telefunc', '-get', `(+get ~*room:{${lapsed}}:*)`)
      const refused = await Room.list().then(
        () => 'resolved',
        (error: unknown) => error,
      )
      // Read once the call settled, which it does only after every step it started: the cleanup is done by then.
      const cleaned = {
        keys: (await lapsedKeys()).length,
        listed: await admin.zscore('tf:0:room-dir:{tf:0:dir}:index', lapsed),
      }
      admin.disconnect()

      expect({ left: left > 0, refused, cleaned }).toMatchObject({
        left: true,
        refused: { name: 'BackendError', code: 'denied', outcome: 'may-have-run' },
        cleaned: { keys: 0, listed: null },
      })
    }, 60_000)
  }
})

describe.runIf(runs)('Room.removeParticipant() by id, called again', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a store's refusal is a BackendError while the participant stays; after they left, another error`, async () => {
      const admin = await reached(store)
      const id = `p-${i}`
      await Room.create(id)
      const participant = await Room.join(id, { identity: 'u1' })
      const remove = () =>
        Room.removeParticipant(id, { id: participant.id }).then(
          () => 'resolved',
          (error: unknown) => (error instanceof BackendError ? { backendError: error.code } : { other: String(error) }),
        )

      await admin.call('ACL', 'SETUSER', 'telefunc', '-hget')
      const refused = await remove()
      await admin.call('ACL', 'SETUSER', 'telefunc', '+hget')
      const stays = (await Room.getParticipants(id)).some((info) => info.id === participant.id)
      // The participant leaves by themselves: the room stays open.
      await participant.leave()
      const afterLeaving = await remove()
      const open = (await Room.get(id)) !== null
      admin.disconnect()

      expect({ refused, stays, afterLeaving, open }).toEqual({
        refused: { backendError: 'denied' },
        stays: true,
        afterLeaving: { other: expect.stringContaining('Participant not found') },
        open: true,
      })
    }, 60_000)
  }
})

/** A lease of another server in room `id` that lapsed, hosting nothing, written as the backend writes a cell: a call
 *  that finds a participant there reaps it. Whether it is still there. */
async function lapsedLease(admin: Redis, id: string): Promise<() => Promise<boolean>> {
  const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
  if (values === undefined) throw new Error(`room ${id} has no cells`)
  const incarnation = values.slice(0, -':values'.length)
  const key = 'session:lapsed'
  await admin.zadd(`${incarnation}:cells`, 0, key)
  await admin.hset(values, key, JSON.stringify({ until: 0 }))
  await admin.hset(`${incarnation}:versions`, key, '1')
  return async () => (await admin.hexists(values, key)) === 1
}

describe.runIf(runs)('a Room call refused after it reaped a lapsed lease', () => {
  for (const [i, store] of STORES.entries()) {
    for (const call of ['Room.removeParticipant()', 'Room.send()'] as const) {
      it(`${store.name}: ${call} by id, refused at its own write, may have run`, async () => {
        const admin = await reached(store)
        const id = `reaped-${i}-${call === 'Room.send()' ? 'send' : 'remove'}`
        await Room.create(id)
        const participant = await Room.join(id)
        const leaseStays = await lapsedLease(admin, id)
        // The removal and the send each publish on a channel of the room; the reap publishes on none.
        await admin.call('ACL', 'SETUSER', 'telefunc', 'resetchannels')
        const refused = await (call === 'Room.send()'
          ? Room.send(id, { id: participant.id }, 'hi')
          : Room.removeParticipant(id, { id: participant.id })
        ).then(
          () => 'resolved',
          (error: unknown) => error,
        )
        const reaped = !(await leaseStays())
        await admin.call('ACL', 'SETUSER', 'telefunc', '&*')
        admin.disconnect()

        expect({ refused, reaped }).toMatchObject({
          refused: { name: 'BackendError', code: 'denied', outcome: 'may-have-run' },
          reaped: true,
        })
      }, 60_000)
    }
  }
})
