// A member counts as a participant only while its own lease can host, as the store observes it beside the member's
// cell, on the oldest and the newest Redis and Valkey Telefunc supports: a lease claimed by a reap, lapsed or removed
// hosts no one, whatever the order of the reads and the reaps around it. A view that keeps such a member for its leave
// is a row of packages/room/src/server/store/hosting.spec.ts alone: it waits for the view's own roster reconcile.
import type { Redis } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { BackendError } from 'telefunc'
import { Room, type LeaveCause } from '@telefunc/room'
import type { CellsRead, OperationOptions, OperationResult, RoomOperation } from '@telefunc/room/backend'
import { hostingLease, joinedByAnotherServer } from '../test/servers.js'
import { endReached, reached, runs, STORES } from '../test/stores.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'

afterAll(() => endReached())
afterEach(() => vi.restoreAllMocks())

/** What the Redis backend commits at most (packages/redis/src/commands.ts). */
const CEILING = 28 * 1024 * 1024

/** Writes the cell of `lease` in the incarnation whose cells are `values`, as the backend writes a cell. */
async function writeLease(admin: Redis, values: string, lease: string, record: { until: number; cleaner?: string }) {
  await admin.hset(values, `session:${lease}`, JSON.stringify(record))
}

/** Removes the cell `key` of the incarnation whose cells are `values`, as the backend removes one. */
async function removeCell(admin: Redis, values: string, key: string) {
  const incarnation = values.slice(0, -':values'.length)
  await admin.hdel(values, key)
  await admin.hdel(`${incarnation}:versions`, key)
  await admin.zrem(`${incarnation}:cells`, key)
}

/** From here on, each operation named `name` in room `id` first runs `before`, then runs as it would. */
function beforeOperation(id: string, name: string, before: () => Promise<void>): void {
  const { runOperation } = RedisRoomDriver.prototype
  async function first<Args, Value>(
    this: RedisRoomDriver,
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    if (roomId === id && operation.name === name) await before()
    return await runOperation.bind(this)(roomId, inc, operation, args, options)
  }
  vi.spyOn(RedisRoomDriver.prototype, 'runOperation').mockImplementation(first)
}

function refused(): Promise<void> {
  return Promise.reject(new BackendError({ code: 'denied', outcome: 'not-run', message: 'refused before dispatch' }))
}

/** The participants a read lists, the room's count, and whether a send to `member` by id is told it is gone. */
async function observed(id: string, member: string) {
  const listed = (await Room.getParticipants(id)).map((each) => each.id).includes(member)
  const [info] = (await Room.list({ prefix: id })).filter((each) => each.id === id)
  const sent = await Room.send(id, { id: member }, 'hello').then(
    () => 'delivered',
    (error: unknown) => (error instanceof Error && error.message.startsWith('Participant not found') ? true : error),
  )
  return { listed, count: info?.count, sent }
}

describe.runIf(runs)('a member whose lease a reap claimed, while the reap is refused', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: is no participant: not listed, not counted, and a send to it is told it is gone (#436 line 76)`, async () => {
      const admin = await reached(store)
      const id = `hosting-claimed-${i}`
      await Room.create(id)
      const member = await joinedByAnotherServer(String(admin.options.host), id)
      const { values, lease } = await hostingLease(admin, id, member)
      await writeLease(admin, values, lease, { until: 0 })
      beforeOperation(id, 'leave', refused)
      await Room.getParticipants(id).catch(() => {})
      vi.restoreAllMocks()
      const seen = await observed(id, member)
      admin.disconnect()

      expect(seen).toEqual({ listed: false, count: 0, sent: true })
    }, 60_000)
  }
})

describe.runIf(runs)('a member joined under a lease the reader’s lease scan missed', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: is listed only while its lease can host, whatever the scan saw (A.2)`, async () => {
      const admin = await reached(store)
      const host = String(admin.options.host)
      const id = `hosting-missed-${i}`
      await Room.create(id)
      // The reader's member read waits until the room changed under it.
      const held = Promise.withResolvers<void>()
      let holding = true
      const { readCellPages } = RedisRoomDriver.prototype
      async function* holdingMembers(
        this: RedisRoomDriver,
        ...read: Parameters<RedisRoomDriver['readCellPages']>
      ): AsyncGenerator<CellsRead> {
        const [roomId, , prefix] = read
        if (roomId === id && prefix === 'm:' && holding) {
          holding = false
          await held.promise
        }
        yield* readCellPages.apply(this, read)
      }
      vi.spyOn(RedisRoomDriver.prototype, 'readCellPages').mockImplementation(holdingMembers)
      const reading = Room.getParticipants(id)
      // P joins under a new lease, which a reap claims and the store then refuses.
      const p = await joinedByAnotherServer(host, id)
      const { values, lease } = await hostingLease(admin, id, p)
      await writeLease(admin, values, lease, { until: Date.now() + 15_000, cleaner: 'another reap' })
      // Q joins live after P's claim.
      const q = await joinedByAnotherServer(host, id)
      held.resolve()
      const listed = (await reading).map((each) => each.id)
      admin.disconnect()

      expect({ p: listed.includes(p), q: listed.includes(q) }).toEqual({ p: false, q: true })
    }, 60_000)
  }
})

describe.runIf(runs)('a reader whose claim on a lapsed lease another reap took first', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: rereads the members and leaves out the lease’s member (A.2, the degraded order)`, async () => {
      const admin = await reached(store)
      const id = `hosting-lost-claim-${i}`
      await Room.create(id)
      const member = await joinedByAnotherServer(String(admin.options.host), id)
      const { values, lease } = await hostingLease(admin, id, member)
      await writeLease(admin, values, lease, { until: 0 })
      // Just before this reader's claim, another reap claims the lease.
      let first = true
      beforeOperation(id, 'claimLease', async () => {
        if (!first) return
        first = false
        await writeLease(admin, values, lease, { until: Date.now() + 15_000, cleaner: 'another reap' })
      })
      const listed = (await Room.getParticipants(id)).map((each) => each.id)
      admin.disconnect()

      expect(listed.includes(member)).toBe(false)
    }, 60_000)
  }
})

describe.runIf(runs)('a lease id that its server reused after the lease was removed', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: hosts the member joined under it again (A.1)`, async () => {
      const admin = await reached(store)
      const id = `hosting-reused-${i}`
      await Room.create(id)
      // The first join commits and its reply is lost, so this server still holds its lease as never created.
      let lost = true
      const { runOperation } = RedisRoomDriver.prototype
      async function losingReply<Args, Value>(
        this: RedisRoomDriver,
        roomId: string,
        inc: string,
        operation: RoomOperation<Args, Value>,
        args: Args,
        options?: OperationOptions,
      ): Promise<OperationResult<Value>> {
        const result = await runOperation.bind(this)(roomId, inc, operation, args, options)
        if (roomId === id && operation.name === 'join' && lost) {
          lost = false
          throw new BackendError({ code: 'unreachable', outcome: 'may-have-run', message: 'the reply was lost' })
        }
        return result
      }
      vi.spyOn(RedisRoomDriver.prototype, 'runOperation').mockImplementation(losingReply)
      await Room.join(id).catch(() => {})
      vi.restoreAllMocks()
      // A reap claims the lease, and the lease is removed.
      const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
      if (values === undefined) throw new Error(`room ${id} has no cells`)
      const leases = (await admin.hkeys(values)).filter((key) => key.startsWith('session:'))
      for (const key of leases) await removeCell(admin, values, key)
      // This server joins again under the same lease id, which writes its cell anew.
      const member = await Room.join(id)
      const [listed] = (await Room.getParticipants(id)).filter((each) => each.id === member.id)
      admin.disconnect()

      expect(listed?.isConnected).toBe(true)
    }, 60_000)
  }
})

describe.runIf(runs)('a member record without its lease reference', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: is a bug a read names`, async () => {
      const admin = await reached(store)
      const id = `hosting-no-reference-${i}`
      await Room.create(id)
      const member = await joinedByAnotherServer(String(admin.options.host), id)
      const { values } = await hostingLease(admin, id, member)
      await removeCell(admin, values, `ml:${member}`)
      const told = await Room.getParticipants(id).then(
        () => 'listed',
        (error: unknown) => (error instanceof Error ? error.message : typeof error),
      )
      admin.disconnect()

      expect(told).toContain(`the member record m:${member} was read without its lease reference`)
    }, 60_000)
  }
})

describe.runIf(runs)('a removal by identity whose reason fits a visible tab’s leave and not a hidden tab’s', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: goes through where the hidden tab is on a lapsed lease: it hosts no one, so it is not checked`, async () => {
      const admin = await reached(store)
      const id = `hosting-hidden-tab-${i}`
      await Room.create(id)
      const visible = await Room.join(id, { identity: 'u1' })
      const hidden = await joinedByAnotherServer(String(admin.options.host), id, { identity: 'u1', hidden: true })
      const { values, lease } = await hostingLease(admin, id, hidden)
      await writeLease(admin, values, lease, { until: 0 })
      // The visible tab's leave is exactly the ceiling; the hidden tab's is 14 bytes over it.
      const reason = 'r'.repeat(CEILING - leaveRecordBytes(visible.id, '', false))
      const removed = await Room.removeParticipant(id, { identity: 'u1', reason }).then(
        () => 'removed',
        (error: unknown) => (error instanceof Error ? error.name : typeof error),
      )
      const kept = {
        visible: await admin.hexists(values, `m:${visible.id}`),
        hidden: await admin.hexists(values, `m:${hidden}`),
      }
      admin.disconnect()

      expect({ removed, kept }).toEqual({ removed: 'removed', kept: { visible: 0, hidden: 0 } })
    }, 180_000)
  }
})

describe.runIf(runs)('a call over the size whose participant is on a lapsed lease', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: Room.send() by id to it is told the participant is gone, and the lease is reaped`, async () => {
      const admin = await reached(store)
      const id = `hosting-lapsed-target-${i}`
      await Room.create(id)
      const member = await joinedByAnotherServer(String(admin.options.host), id)
      const { values, lease } = await hostingLease(admin, id, member)
      await writeLease(admin, values, lease, { until: 0 })
      const told = await Room.send(id, { id: member }, 'x'.repeat(CEILING + 1)).then(
        () => 'resolved',
        (error: unknown) => (error instanceof Error ? error.message : typeof error),
      )
      const stored = await admin.hexists(values, `m:${member}`)
      admin.disconnect()

      expect({ told, stored }).toEqual({ told: `Participant not found (left?): ${member}`, stored: 0 })
    }, 60_000)

    it(`${store.name}: Room.removeParticipant() by an identity whose tabs are all on lapsed leases goes through, and the reap removes them`, async () => {
      const admin = await reached(store)
      const id = `hosting-lapsed-identity-${i}`
      await Room.create(id)
      const host = String(admin.options.host)
      const tabs = [
        await joinedByAnotherServer(host, id, { identity: 'u1' }),
        await joinedByAnotherServer(host, id, { identity: 'u1', hidden: true }),
      ]
      for (const tab of tabs) {
        const { values, lease } = await hostingLease(admin, id, tab)
        await writeLease(admin, values, lease, { until: 0 })
      }
      const removed = await Room.removeParticipant(id, { identity: 'u1', reason: 'r'.repeat(CEILING + 1) }).then(
        () => 'removed',
        (error: unknown) => (error instanceof Error ? error.name : typeof error),
      )
      const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
      const stored =
        values === undefined ? [] : (await admin.hkeys(values)).filter((key) => tabs.some((tab) => key === `m:${tab}`))
      admin.disconnect()

      expect({ removed, stored }).toEqual({ removed: 'removed', stored: [] })
    }, 60_000)
  }
})

describe.runIf(runs)('a healthy Room.send() by id', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: makes five requests, and reads the member's lease beside it in the same one`, async () => {
      const admin = await reached(store)
      const id = `hosting-count-${i}`
      await Room.create(id)
      const member = await Room.join(id)
      await Room.send(id, { id: member.id }, 'warm')
      await admin.config('RESETSTAT')
      await Room.send(id, { id: member.id }, 'counted')
      const stats = await admin.info('commandstats')
      const calls = (command: string) => Number(new RegExp(`^cmdstat_${command}:calls=(\\d+)`, 'm').exec(stats)?.[1])
      admin.disconnect()

      // At the parent: 5 scripts, which read 5 cells by HGET; the lease beside the member adds its reference's and
      // the lease's.
      expect({ evalsha: calls('evalsha'), hget: calls('hget') }).toEqual({ evalsha: 5, hget: 7 })
    }, 60_000)
  }
})

function leaveRecordBytes(id: string, reason: string, hidden: boolean): number {
  const leave: Record<string, unknown> = { __r: 'leave', id, cause: 'removed', reason }
  if (hidden) leave.hidden = true
  return Buffer.byteLength(JSON.stringify(leave))
}
