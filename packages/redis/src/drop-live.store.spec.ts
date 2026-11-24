// A drop of an incarnation whose caller read another head, as a directory repair that read the head before a create
// and found the created room listed, leaves the incarnation the live head names as it is: on the oldest and the
// newest Redis and Valkey Telefunc supports.
import { afterAll, describe, expect, it } from 'vitest'
import { Room } from '@telefunc/room'
import { getRoomPlane } from '@telefunc/room/backend'
import { endReached, reached, runs, STORES } from '../test/stores.js'
import { DEFAULT_PREFIX, incarnationKeysKey } from './keys.js'

afterAll(() => endReached())

describe.runIf(runs)("a drop of the incarnation a room's live head names", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: drops none of it`, async () => {
      const admin = await reached(store)
      const id = `drop-live-${i}`
      await Room.create(id, { meta: { name: 'X' } })
      const member = await Room.join(id, { meta: { name: 'x1' } })
      const { direct } = getRoomPlane()
      const inc = (await direct.reads.readHead(id))?.currentInc
      if (inc === undefined || inc === null) throw new Error(`${id} has no incarnation`)
      const manifest = incarnationKeysKey(`${DEFAULT_PREFIX}0:`, id, inc)
      const keys = await admin.smembers(manifest)
      await direct.dropIncarnation(id, inc)
      const seen = {
        manifest: await admin.scard(manifest),
        kept: keys.length === 0 ? 0 : await admin.exists(...keys),
        participants: (await Room.getParticipants(id)).map((each) => each.id),
      }
      console.log(JSON.stringify({ store: store.name, inc, keys: keys.length, ...seen }))
      admin.disconnect()

      expect(keys.length).toBeGreaterThan(0)
      expect(seen).toEqual({ manifest: keys.length, kept: keys.length, participants: [member.id] })
    }, 120_000)
  }
})
