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
// - `Room.get()` of a room this process holds a view of, whose catch-up reaped a lapsed lease of another server and
//   was then refused: it may have run, as the reap wrote.
// - A call whose reap of a lapsed lease is refused while it still removes the lease's views rejects only once those
//   removals ended: the store doesn't change after the rejection.
// - `Room.removeParticipant()` whose room closed after it planned the leave: the leave reaches the store, which
//   changes nothing, and the call resolves, as the close ended the participant.
// - `Room.removeParticipant()` by identity whose two leaves the store refuses, the second still on its way when the
//   first tab's eviction reads the head after its refusal: it didn't run, and the store keeps everything.
import { execFile } from 'node:child_process'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { isDeepStrictEqual, promisify } from 'node:util'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it, vi } from 'vitest'
import { BackendError } from 'telefunc'
import { Room } from '@telefunc/room'
import { endReached, reached, runs, snapshot, STORES, type Store } from '../test/stores.js'
import { REDIS_COMMANDS, type RedisCommand } from './commands.js'
import { installRedis } from './index.js'
import { RedisConnection } from './RedisConnection.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'
import type { OperationOptions, OperationResult, RoomOperation } from '@telefunc/room/backend'

afterAll(() => endReached())

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
        listed: await admin.zscore('tf:0:room-dir:{tf%3A0%3Adir}:index', lapsed),
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
        listed: await admin.zscore('tf:0:room-dir:{tf%3A0%3Adir}:index', lapsed),
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

/** What `work` prints, run on room `id` by another server: a process of its own, on the built packages, which then
 *  stops. */
async function atAnotherServer(host: string, id: string, work: string): Promise<string[]> {
  const server = `
import { Redis } from 'ioredis'
import { Room } from '@telefunc/room'
import { installRedis } from '@telefunc/redis'
installRedis(new Redis({ host: process.argv[1], username: 'telefunc', password: 'pw' }))
const id = process.argv[2]
${work}
process.exit(0)
`
  const cwd = fileURLToPath(new URL('..', import.meta.url))
  const { stdout } = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', server, host, id], {
    cwd,
  })
  return stdout.trim().split('\n')
}

/** A participant that another server joined to room `id` under its own lease, which it then left behind, as a server
 *  that stopped does: the participant's id. */
async function joinedByAnotherServer(host: string, id: string): Promise<string> {
  const printed = await atAnotherServer(host, id, 'console.log((await Room.join(id)).id)')
  const participant = printed.at(-1)
  if (participant === undefined || participant === '') throw new Error(`the other server joined no one: ${printed}`)
  return participant
}

/** The ids `Room.getParticipants()` of room `id` lists at another server. */
async function listedByAnotherServer(host: string, id: string): Promise<string[]> {
  const printed = await atAnotherServer(
    host,
    id,
    "console.log('listed:' + (await Room.getParticipants(id)).map((participant) => participant.id).join(','))",
  )
  const listed = printed.find((line) => line.startsWith('listed:'))
  if (listed === undefined) throw new Error(`the other server listed nothing: ${printed}`)
  return listed.slice('listed:'.length).split(',').filter(Boolean)
}

describe.runIf(runs)('Room.get() of a view this process holds, whose catch-up reaped a lapsed lease', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: refused after the reap, may have run`, async () => {
      const admin = await reached(store)
      const id = `held-${i}`
      // The view this process holds: no participant or page of its own observes it, so a get catches it up.
      const room = await Room.create(id)
      const member = await joinedByAnotherServer(String(admin.options.host), room.id)
      expect((await Room.getParticipants(room.id)).map((participant) => participant.id)).toEqual([member])
      await Room.get(room.id)
      const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
      if (values === undefined) throw new Error(`room ${id} has no cells`)
      const incarnation = values.slice(0, -':values'.length)
      const marks = (await admin.hkeys(values)).filter((key) => key.endsWith(`:${member}`) && key.startsWith('sm:'))
      const lease = marks[0]?.split(':')[1]
      if (lease === undefined) throw new Error(`no lease hosts ${member}`)
      // The other server's lease lapses.
      await admin.hset(values, `session:${lease}`, JSON.stringify({ until: 0 }))
      // GET is refused on the departing records alone: the refresh reads the member's after its reap removed it.
      await admin.call(
        'ACL',
        'SETUSER',
        'telefunc',
        '-get',
        `(+get ~tf:0:room:{${id}}:[^g]* ~${incarnation}:[^x]* ~${incarnation}:x:[^d]* ~tf:0:room-dir:*)`,
      )
      const refused = await Room.get(room.id).then(
        () => 'resolved',
        (error: unknown) => error,
      )
      const reaped = {
        lease: await admin.hexists(values, `session:${lease}`),
        member: await admin.hexists(values, `m:${member}`),
      }
      admin.disconnect()

      expect({ refused, reaped }).toMatchObject({
        refused: { name: 'BackendError', code: 'denied', outcome: 'may-have-run' },
        reaped: { lease: 0, member: 0 },
      })
    }, 60_000)
  }
})

/** The incarnation's cells of room `id` and the lease that hosts `member`, as the store holds them. */
async function hostingLease(admin: Redis, id: string, member: string): Promise<{ values: string; lease: string }> {
  const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
  if (values === undefined) throw new Error(`room ${id} has no cells`)
  const mark = (await admin.hkeys(values)).find((key) => key.startsWith('sm:') && key.endsWith(`:${member}`))
  const lease = mark?.split(':')[1]
  if (lease === undefined) throw new Error(`no lease hosts ${member}`)
  return { values, lease }
}

describe.runIf(runs)('a call whose reap of a lapsed lease is refused while the reap still writes', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: rejects once every write it started ended, handing over no more; the next reap removes the rest once`, async () => {
      const admin = await reached(store)
      const room = await Room.create(`drained-${i}`)
      const member = await joinedByAnotherServer(String(admin.options.host), room.id)
      const { values, lease } = await hostingLease(admin, room.id, member)
      const incarnation = values.slice(0, -':values'.length)
      // The other server's pages held more views than a reap removes at once, each counted in the room's holds.
      const views = Array.from({ length: 300 }, (_, n) => `v:${lease}:view-${n}`)
      for (const view of views) {
        await admin.zadd(`${incarnation}:cells`, 0, view)
        await admin.hset(values, view, '')
        await admin.hset(`${incarnation}:versions`, view, '1')
      }
      const holds = (await admin.hget(values, 'holds')) ?? ''
      const held = /"value":(\d+)/.exec(holds)?.[1]
      if (held === undefined) throw new Error(`room ${room.id} counts no holds`)
      await admin.hset(values, 'holds', holds.replace(/"value":\d+/, `"value":${Number(held) + views.length}`))
      await admin.hset(values, `session:${lease}`, JSON.stringify({ until: 0 }))
      // The store refuses the read that plans the member's removal; the views' removals go on beside it.
      await admin.call('ACL', 'SETUSER', 'telefunc', '-hkeys')
      // The removals the views' sibling still hands over after the close are refused inside it, never unhandled.
      const unhandled: unknown[] = []
      const onUnhandled = (reason: unknown) => {
        unhandled.push(reason)
      }
      process.on('unhandledRejection', onUnhandled)
      const refused = await Room.getParticipants(room.id).then(
        () => 'resolved',
        (error: unknown) => error,
      )
      const atRejection = await snapshot(admin)
      await sleep(2_000)
      const unchanged = isDeepStrictEqual(await snapshot(admin), atRejection)
      process.off('unhandledRejection', onUnhandled)
      const viewsLeft = async () => (await admin.hkeys(values)).filter((key) => key.startsWith(`v:${lease}:`)).length
      // The failed call handed over no removal after its close: the views not yet removed stay, the lease claimed.
      const leftAtRejection = await viewsLeft()
      // How long the claim keeps other reapers off, by the store's clock: a lease's length.
      const claim = (await admin.hget(values, `session:${lease}`)) ?? ''
      const [seconds, micros] = await admin.time()
      const claimMs =
        Number(/"until":(\d+)/.exec(claim)?.[1]) - (Number(seconds) * 1000 + Math.floor(Number(micros) / 1000))
      // What the apps see while the claim lasts, once the store allows the read: another server's list of the room's
      // participants, `Room.list()`'s count, and the roster of a member that joins then.
      await admin.call('ACL', 'SETUSER', 'telefunc', '+hkeys')
      const anotherServerLists = (await listedByAnotherServer(String(admin.options.host), room.id)).includes(member)
      const listedCount = (await Room.list({ prefix: room.id })).find((info) => info.id === room.id)?.count
      const watcher = await room.join()
      const rosterLists = room.getParticipants().some(({ id }) => id === member)
      await watcher.leave()
      const whileClaimed = { anotherServerLists, listedCount, rosterLists }
      console.log(`${store.name}: while the failed call's claim lasts: ${JSON.stringify(whileClaimed)}`)
      // Once the claim lapses, two reapers come at once: one removes the rest.
      await admin.hset(values, `session:${lease}`, claim.replace(/"until":\d+/, '"until":0'))
      await Promise.all([Room.getParticipants(room.id), Room.getParticipants(room.id)])
      const afterNextReap = {
        viewsLeft: await viewsLeft(),
        leaseLeft: await admin.hexists(values, `session:${lease}`),
        memberListed: (await Room.getParticipants(room.id)).some(({ id }) => id === member),
        // Each view's hold and the member's left the count once: it is the count before the views, less the member.
        holds: /"value":(\d+)/.exec((await admin.hget(values, 'holds')) ?? '')?.[1],
      }
      admin.disconnect()
      console.log(`${store.name}: the failed call's claim on the lease lasts ${claimMs} ms`)

      expect({
        refused,
        unchanged,
        unhandled,
        someLeftAtRejection: leftAtRejection > 0,
        claimedForALease: claimMs > 110_000 && claimMs <= 120_000,
        afterNextReap,
      }).toMatchObject({
        refused: { name: 'BackendError', code: 'denied', outcome: 'may-have-run' },
        unchanged: true,
        unhandled: [],
        someLeftAtRejection: true,
        claimedForALease: true,
        afterNextReap: { viewsLeft: 0, leaseLeft: 0, memberListed: false, holds: String(Number(held) - 1) },
      })
    }, 60_000)
  }
})

describe.runIf(runs)('Room.removeParticipant() whose room closed after it planned the leave', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: the leave reaches the store, which changes nothing, and the call resolves`, async () => {
      const admin = await reached(store)
      const room = await Room.create(`closed-${i}`)
      const member = await joinedByAnotherServer(String(admin.options.host), room.id)
      const run = RedisConnection.prototype.run
      let armed = true
      let closed: Map<string, unknown> | undefined
      let leaveAnswer: unknown
      vi.spyOn(RedisConnection.prototype, 'run').mockImplementation(async function <Input, Output>(
        this: RedisConnection,
        command: RedisCommand<Input, Output>,
        input: Input,
      ): Promise<Output> {
        if (!armed || command.name !== REDIS_COMMANDS.operation.name) return await run.bind(this)(command, input)
        armed = false
        // The leave is planned: the room closes before its script runs.
        await Room.close(room.id)
        closed = await snapshot(admin)
        const answer = await run.bind(this)(command, input)
        leaveAnswer = answer
        return answer
      })
      const removed = await Room.removeParticipant(room.id, { id: member }).then(
        () => 'resolved',
        (error: unknown) => error,
      )
      vi.restoreAllMocks()
      const unchanged = closed !== undefined && isDeepStrictEqual(await snapshot(admin), closed)
      admin.disconnect()

      expect({ removed, leaveAnswer, unchanged }).toMatchObject({
        removed: 'resolved',
        leaveAnswer: { kind: 'stale-incarnation' },
        unchanged: true,
      })
    }, 60_000)
  }
})

describe.runIf(runs)('a Room call whose leaves the store refuses, one still on its way after the other', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: Room.removeParticipant() by identity is told 'not-run' where every leave was refused, and the store keeps everything`, async () => {
      const admin = await reached(store)
      const room = await Room.create(`evicted-${i}`)
      const tabs = (
        await atAnotherServer(
          String(admin.options.host),
          room.id,
          "for (let n = 0; n < 2; n++) console.log('tab:' + (await Room.join(id, { identity: 'u1' })).id)",
        )
      )
        .filter((line) => line.startsWith('tab:'))
        .map((line) => line.slice('tab:'.length))
        .sort()
      const before = await snapshot(admin)
      // The store refuses each leave itself, before its first write: the leave's script deletes with HDEL, which the
      // app's user may no longer run.
      await admin.call('ACL', 'SETUSER', 'telefunc', '-hdel')
      // The first leave reaches the store once both are in the driver. The second reaches it only once the first
      // tab's eviction, after that refusal, has read the head of the room, which is still open: the eviction is
      // answered while the other leave is still on its way.
      const { runOperation, readHead } = RedisRoomDriver.prototype
      const bothIn = Promise.withResolvers<void>()
      const firstEvictionRead = Promise.withResolvers<void>()
      let leavesIn = 0
      const refusals: unknown[] = []
      async function leavesInOrder<Args, Value>(
        this: RedisRoomDriver,
        roomId: string,
        inc: string,
        operation: RoomOperation<Args, Value>,
        args: Args,
        options?: OperationOptions,
      ): Promise<OperationResult<Value>> {
        if (roomId !== room.id || operation.name !== 'leave') {
          return await runOperation.bind(this)(roomId, inc, operation, args, options)
        }
        if (++leavesIn === 1) await bothIn.promise
        else {
          bothIn.resolve()
          await firstEvictionRead.promise
        }
        try {
          return await runOperation.bind(this)(roomId, inc, operation, args, options)
        } catch (error) {
          refusals.push(error instanceof BackendError ? { code: error.code, outcome: error.outcome } : error)
          throw error
        }
      }
      vi.spyOn(RedisRoomDriver.prototype, 'runOperation').mockImplementation(leavesInOrder)
      vi.spyOn(RedisRoomDriver.prototype, 'readHead').mockImplementation(async function (
        this: RedisRoomDriver,
        roomId,
      ) {
        const head = await readHead.call(this, roomId)
        if (roomId === room.id && refusals.length === 1) firstEvictionRead.resolve()
        return head
      })
      const removed = await Room.removeParticipant(room.id, { identity: 'u1' }).then(
        () => 'resolved',
        (error: unknown) => (error instanceof BackendError ? { code: error.code, outcome: error.outcome } : error),
      )
      vi.restoreAllMocks()
      const unchanged = isDeepStrictEqual(await snapshot(admin), before)
      const listed = (await Room.getParticipants(room.id)).map(({ id }) => id).sort()
      admin.disconnect()

      expect({ removed, refusals, unchanged, listed }).toEqual({
        removed: { code: 'denied', outcome: 'not-run' },
        refusals: [
          { code: 'denied', outcome: 'not-run' },
          { code: 'denied', outcome: 'not-run' },
        ],
        unchanged: true,
        listed: tabs,
      })
      expect(tabs).toHaveLength(2)
    }, 60_000)
  }
})
