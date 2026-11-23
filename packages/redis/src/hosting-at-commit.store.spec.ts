// A message reaches a participant only while its lease hosts it at the commit that delivers it, by the store's clock,
// on the oldest and the newest Redis and Valkey Telefunc supports: whatever a lookup or a server's view saw before. A
// tab's own publishes, sends and answers commit only while its lease hosts it.
import type { Redis } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { Room } from '@telefunc/room'
import { hostingLease, joinedByAnotherServer } from '../test/servers.js'
import { endReached, reached, runs, STORES } from '../test/stores.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'

afterAll(() => endReached())
afterEach(() => vi.restoreAllMocks())

/** What a lease lives for once written, from packages/room/src/server/constants.ts. */
const ROOM_LEASE_TTL_MS = 120_000

/** The incarnation whose cells are `values`. */
function incarnationOf(values: string): string {
  return values.slice(0, -':values'.length)
}

/** The store's clock, in ms. */
async function storeNow(admin: Redis): Promise<number> {
  const [seconds, micros] = await admin.time()
  return Number(seconds) * 1000 + Math.floor(Number(micros) / 1000)
}

/** Lets `lease` lapse, as once its server stopped renewing it. */
async function lapseLease(admin: Redis, values: string, lease: string) {
  await admin.hset(`${incarnationOf(values)}:lapses`, `session:${lease}`, '0')
}

/** Writes `lease` again, as its server renews it. */
async function renewLease(admin: Redis, values: string, lease: string) {
  const incarnation = incarnationOf(values)
  await admin.hset(`${incarnation}:versions`, `session:${lease}`, crypto.randomUUID())
  await admin.hset(`${incarnation}:lapses`, `session:${lease}`, String((await storeNow(admin)) + ROOM_LEASE_TTL_MS))
}

/** Moves `lease` to `cleaner`'s claim, a cell lapsing 15 s from now, as a reap's claim does. */
async function claimLease(admin: Redis, values: string, lease: string, cleaner: string) {
  const incarnation = incarnationOf(values)
  const claim = `session:claim:${lease}`
  for (const key of [values, `${incarnation}:versions`, `${incarnation}:lapses`])
    await admin.hdel(key, `session:${lease}`)
  await admin.zrem(`${incarnation}:cells`, `session:${lease}`)
  await admin.hset(values, claim, cleaner)
  await admin.hset(`${incarnation}:versions`, claim, crypto.randomUUID())
  await admin.zadd(`${incarnation}:cells`, 0, claim)
  await admin.hset(`${incarnation}:lapses`, claim, String((await storeNow(admin)) + 15_000))
}

/** How a lease stops hosting: it lapses, as once its server stopped renewing it, or a reap claims it. */
type LeaseEnd = 'lapsed' | 'claimed'

/** Ends the lease that hosts `member` in room `id` as `end` says. */
async function endLeaseOf(admin: Redis, id: string, member: string, end: LeaseEnd): Promise<void> {
  const { values, lease } = await hostingLease(admin, id, member)
  if (end === 'lapsed') await lapseLease(admin, values, lease)
  else await claimLease(admin, values, lease, 'a reap')
}

/** Each commit to `member`'s inbox in room `id` runs `step` first, then commits as it was asked. */
function beforeInboxCommit(id: string, member: string, step: () => Promise<void>): void {
  const { commitLane } = RedisRoomDriver.prototype
  vi.spyOn(RedisRoomDriver.prototype, 'commitLane').mockImplementation(async function (
    this: RedisRoomDriver,
    target,
    payload,
    options,
  ) {
    if (target.roomId === id && target.lane.kind === 'inbox' && target.lane.member === member) await step()
    return await commitLane.call(this, target, payload, options)
  })
}

const told = (error: unknown) => (error instanceof Error ? error.message : typeof error)

describe.runIf(runs)('a Room.send() by identity whose tab was looked up while its lease hosted it', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: delivers nothing once the lease lapsed before the inbox commit`, async () => {
      const admin = await reached(store)
      const id = `hosting-at-commit-lapsed-${i}`
      await Room.create(id)
      const tab = await Room.join(id, { identity: 'person' })
      const received: unknown[] = []
      tab.listen((message) => {
        received.push(message)
      })
      const { values, lease } = await hostingLease(admin, id, tab.id)
      beforeInboxCommit(id, tab.id, () => lapseLease(admin, values, lease))
      const receipts = await Room.send(id, { identity: 'person' }, 'private message')
      vi.restoreAllMocks()
      await Room.close(id)
      admin.disconnect()

      expect({ receipts: receipts.length, received }).toEqual({ receipts: 0, received: [] })
    }, 60_000)

    it(`${store.name}: delivers once the lease was renewed before the inbox commit, in the requests a send makes`, async () => {
      const admin = await reached(store)
      const id = `hosting-at-commit-renewed-${i}`
      await Room.create(id)
      const tab = await Room.join(id, { identity: 'person' })
      const received: unknown[] = []
      tab.listen((message) => {
        received.push(message)
      })
      const { values, lease } = await hostingLease(admin, id, tab.id)
      const requests = async (message: string) => {
        await admin.config('RESETSTAT')
        const receipts = await Room.send(id, { identity: 'person' }, message)
        const stats = await admin.info('commandstats')
        return { receipts: receipts.length, requests: Number(/^cmdstat_evalsha:calls=(\d+)/m.exec(stats)?.[1]) }
      }
      await Room.send(id, { identity: 'person' }, 'warm')
      const unrenewed = await requests('unrenewed')
      beforeInboxCommit(id, tab.id, () => renewLease(admin, values, lease))
      const renewed = await requests('renewed')
      vi.restoreAllMocks()
      await Room.close(id)
      admin.disconnect()

      expect({ renewed, received }).toEqual({ renewed: unrenewed, received: ['warm', 'unrenewed', 'renewed'] })
      expect(unrenewed).toEqual({ receipts: 1, requests: 6 })
    }, 60_000)
  }
})

describe.runIf(runs)("a participant's send() to a tab its server's view lists", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: is refused once a reap claimed the tab's lease, and tells the tab no leave of its own`, async () => {
      const admin = await reached(store)
      const id = `hosting-at-commit-claimed-${i}`
      await Room.create(id)
      const old = await Room.join(id, { identity: 'old' })
      const received: unknown[] = []
      old.listen((message) => {
        received.push(message)
      })
      const left: unknown[] = []
      old.onLeave((cause) => left.push(cause))
      const { values, lease } = await hostingLease(admin, id, old.id)
      await claimLease(admin, values, lease, 'another reap')
      const sender = await Room.join(id, { identity: 'live' })
      const listed = (await Room.getParticipants(id)).map((member) => member.id)
      const sent = await sender.send(old.id, 'hello').then(() => 'delivered', told)
      const leftBeforeClose = [...left]
      await Room.close(id)
      admin.disconnect()

      expect({ listed, sent, received, left: leftBeforeClose }).toEqual({
        listed: [sender.id],
        sent: `Participant not found (left?): ${old.id}`,
        received: [],
        left: [],
      })
    }, 60_000)

    it(`${store.name}: makes one request while the tab's lease hosts it`, async () => {
      const admin = await reached(store)
      const id = `hosting-at-commit-count-${i}`
      await Room.create(id)
      const tab = await Room.join(id)
      const sender = await Room.join(id)
      await sender.send(tab.id, 'warm')
      await admin.config('RESETSTAT')
      await sender.send(tab.id, 'counted')
      const stats = await admin.info('commandstats')
      await Room.close(id)
      admin.disconnect()

      expect(Number(/^cmdstat_evalsha:calls=(\d+)/m.exec(stats)?.[1])).toBe(1)
    }, 60_000)
  }
})

describe.runIf(runs)('a tab whose own lease no longer hosts it', () => {
  for (const [i, store] of STORES.entries()) {
    for (const end of ['lapsed', 'claimed'] as const) {
      it(`${store.name}: publishes nothing once its lease ${end}`, async () => {
        const admin = await reached(store)
        const id = `hosting-at-commit-publisher-${end}-${i}`
        await Room.create(id)
        const tab = await Room.join(id)
        const other = await Room.join(id)
        const received: unknown[] = []
        other.listen((message) => {
          received.push(message)
        })
        await endLeaseOf(admin, id, tab.id, end)
        const published = await tab.publish('hello').then(() => 'published', told)
        const onTrack = await tab.publish('hello', { track: 'cursor' }).then(() => 'published', told)
        await Room.close(id)
        admin.disconnect()

        expect({ published, onTrack, received }).toEqual({
          published: `Participant not found (left?): ${tab.id}`,
          onTrack: `Participant not found (left?): ${tab.id}`,
          received: [],
        })
      }, 60_000)

      it(`${store.name}: sends nothing to a tab another server hosts once its lease ${end}`, async () => {
        const admin = await reached(store)
        const id = `hosting-at-commit-sender-${end}-${i}`
        const handle = await Room.create(id)
        const sender = await Room.join(id)
        // The sender's view lists the recipient, so the send looks no one up.
        const listed = Promise.withResolvers<void>()
        handle.onJoin((member) => {
          if (member.id !== sender.id) listed.resolve()
        })
        const recipient = await joinedByAnotherServer(String(admin.options.host), id)
        await listed.promise
        await endLeaseOf(admin, id, sender.id, end)
        const sent = await sender.send(recipient, 'hello').then(() => 'delivered', told)
        await Room.close(id)
        admin.disconnect()

        expect(sent).toBe(`Participant not found (left?): ${sender.id}`)
      }, 60_000)

      it(`${store.name}: is answered nothing once its lease ${end} after it asked`, async () => {
        const admin = await reached(store)
        const id = `hosting-at-commit-asker-${end}-${i}`
        await Room.create(id)
        const asker = await Room.join(id)
        const answerer = await Room.join(id)
        answerer.listen(async () => {
          await endLeaseOf(admin, id, asker.id, end)
          return 'answer'
        })
        const answer = Promise.withResolvers<string>()
        const { commitLane } = RedisRoomDriver.prototype
        vi.spyOn(RedisRoomDriver.prototype, 'commitLane').mockImplementation(async function (
          this: RedisRoomDriver,
          target,
          payload,
          options,
        ) {
          const committed = await commitLane.call(this, target, payload, options)
          if (target.roomId === id && target.lane.kind === 'inbox' && target.lane.member === asker.id) {
            answer.resolve(committed.kind)
          }
          return committed
        })
        const asked = asker.send(answerer.id, 'question', { ack: true }).catch(told)
        const answerCommit = await answer.promise
        vi.restoreAllMocks()
        await Room.close(id)
        await asked
        admin.disconnect()

        expect(answerCommit).toBe('stale-cell')
      }, 60_000)
    }
  }
})
