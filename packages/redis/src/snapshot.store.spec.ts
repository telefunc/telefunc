// The store specs' snapshot on the oldest and the newest Redis and Valkey Telefunc supports: it holds a hash's fields
// and values as the store's bytes, so a cell that changed by a byte that isn't UTF-8 on its own is a changed key.
import { describe, expect, it } from 'vitest'
import { changedKeys, removeStores, runs, snapshot, startStore, STORES } from '../test/stores.js'

describe.runIf(runs)("the store specs' snapshot", () => {
  for (const store of STORES) {
    it(`${store.name}: a hash's value or field changed from byte 0x80 to 0x81 is a changed key`, async () => {
      removeStores()
      const { admin } = await startStore(store)
      await admin.hset('tf:value', 'cell', Buffer.from([0x80]))
      await admin.hset('tf:field', Buffer.from([0x80]), 'v')
      const before = await snapshot(admin)
      await admin.hset('tf:value', 'cell', Buffer.from([0x81]))
      await admin.hdel('tf:field', Buffer.from([0x80]))
      await admin.hset('tf:field', Buffer.from([0x81]), 'v')
      expect(changedKeys(before, await snapshot(admin))).toEqual(['tf:field', 'tf:value'])
      admin.disconnect()
    })
  }
})
