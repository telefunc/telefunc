// A store that lost the backend's scripts (SCRIPT FLUSH), on the oldest and the newest Redis and Valkey Telefunc
// supports: the backend's next request of a script on that socket is answered NOSCRIPT, which ran nothing, and goes
// once more as EVAL.
import { afterAll, describe, expect, it } from 'vitest'
import { Broadcast } from 'telefunc'
import { standaloneRows } from '../test/endpoints.js'
import { runs, STORES } from '../test/stores.js'

const rows = standaloneRows()
afterAll(() => rows.end())

describe.runIf(runs)("a store that lost the backend's scripts", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a publish after SCRIPT FLUSH goes as EVALSHA, is answered NOSCRIPT, and goes again as EVAL`, async () => {
      const { admin, command } = await rows.reached(store)
      const channel = `noscript-${i}`
      const got: unknown[] = []
      const both = Promise.withResolvers<void>()
      const subscription = Broadcast.subscribe(channel, (message) => {
        if (got.push(message) === 2) both.resolve()
      })
      await subscription.ready
      // The socket runs the publish's script once, so its next publish goes as EVALSHA.
      await Broadcast.publish(channel, 'one')
      await admin.call('SCRIPT', 'FLUSH')
      const [passage] = command.passages().filter(({ ended }) => ended === undefined)
      if (passage === undefined) throw new Error('the command client has no open connection')
      const before = passage.toStore.chunks.length
      await Broadcast.publish(channel, 'two')
      const sent = passage.toStore.chunks.slice(before).map(({ head }) => /\r\n(eval(?:sha)?)\r\n/i.exec(head)?.[1])
      await both.promise
      subscription()

      expect(sent).toEqual(['evalsha', 'eval'])
      expect(got).toEqual(['one', 'two'])
    }, 60_000)
  }
})
