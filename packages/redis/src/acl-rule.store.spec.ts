// The ACL rule the Redis page gives Telefunc's own user, read from the page itself, on the oldest and the newest Redis
// and Valkey Telefunc supports: the store takes it as it is written, and a user of it makes every call Telefunc makes
// of the store. The store refuses that user only the client info ioredis sends on each connection it opens, on the
// stores that know `CLIENT SETINFO`, and ioredis drops the refusal; with the page's optional grant it refuses nothing.
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import {
  CLIENT_INFO_GRANT,
  calls,
  documentedRule,
  EVERY_CALL_RUNS,
  knowsClientInfo,
  reaches,
  refused,
  setUser,
} from '../test/acl.js'
import { forwarder, removeStores, runs, startStore, STORES, type Store } from '../test/stores.js'
import { installRedis } from './index.js'

/** The app's client, logging in as `telefunc`, and the forwarder it reaches each row's store through: the backend is
 *  installed once per process. */
let reach: { proxy: Awaited<ReturnType<typeof forwarder>>; app: Redis } | undefined
afterAll(() => {
  reach?.app.disconnect()
  reach?.proxy.close()
})

/** A new store with the user of `rule`, which the backend reaches through the forwarder; the store's answer to the rule,
 *  the calls' outcomes, and what the store refused the user from the first connection on. */
async function asUserOf(store: Store, rule: string[]) {
  if (reach === undefined) {
    const proxy = await forwarder()
    const app = new Redis({ port: proxy.port, username: 'telefunc', password: 'pw', lazyConnect: true })
    app.on('error', () => {})
    installRedis(app)
    reach = { proxy, app }
  }
  removeStores()
  const { host, admin } = await startStore(store)
  const set = await setUser(admin, rule)
  await admin.call('ACL', 'LOG', 'RESET')
  // Pointing the forwarder at this store drops the backend's connections to the last one: they connect again here.
  reach.proxy.to(host)
  await reaches(store.name)
  return { rule: set, calls: await calls(), refused: await refused([admin]) }
}

describe.runIf(runs)("the Redis page's ACL rule", () => {
  for (const store of STORES) {
    it(`${store.name}: the store takes the rule, and a user of it makes every call`, async () => {
      expect(await asUserOf(store, documentedRule())).toEqual({
        rule: 'OK',
        calls: EVERY_CALL_RUNS,
        refused: knowsClientInfo(store) ? ['command client|setinfo'] : [],
      })
    }, 120_000)

    it.runIf(knowsClientInfo(store))(
      `${store.name}: with the optional grant, the store refuses the user nothing`,
      async () => {
        expect(await asUserOf(store, [...documentedRule(), CLIENT_INFO_GRANT])).toEqual({
          rule: 'OK',
          calls: EVERY_CALL_RUNS,
          refused: [],
        })
      },
      120_000,
    )
  }
})
