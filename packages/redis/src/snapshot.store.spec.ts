// The store specs' snapshot on the oldest and the newest Redis and Valkey Telefunc supports: it holds a hash's fields
// and values as the store's bytes, so a cell that changed by a byte that isn't UTF-8 on its own is a changed key.
import type { Redis } from 'ioredis'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { changedKeys, removeStores, runs, snapshot, startStore, STORE_START_MS, STORES } from '../test/stores.js'

describe.runIf(runs)("the store specs' snapshot", () => {
  for (const store of STORES) {
    describe(store.name, () => {
      // The store starts once, on a budget of its own: the row's 5 s takes no store start.
      let admin: Redis
      beforeAll(async () => {
        removeStores()
        ;({ admin } = await startStore(store))
      }, STORE_START_MS)
      afterAll(() => admin.disconnect())

      it(`a hash's value or field changed from byte 0x80 to 0x81 is a changed key`, async () => {
        await admin.hset('tf:value', 'cell', Buffer.from([0x80]))
        await admin.hset('tf:field', Buffer.from([0x80]), 'v')
        const before = await snapshot(admin)
        await admin.hset('tf:value', 'cell', Buffer.from([0x81]))
        await admin.hdel('tf:field', Buffer.from([0x80]))
        await admin.hset('tf:field', Buffer.from([0x81]), 'v')
        expect(changedKeys(before, await snapshot(admin))).toEqual(['tf:field', 'tf:value'])
      })
    })
  }
})
