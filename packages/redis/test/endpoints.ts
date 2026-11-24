// Role endpoints, and a spec file's rows on a store each. Each connection the backend opens reaches the store through a
// forwarder of its role's: the command client through the address the app's client names, the subscriber through one
// of its own. A row holds or reads one role's connection alone, which a `CLIENT SETNAME` tag can't do for a held AUTH
// or HELLO, as both come before the name.
export { routeSubscribers, endpoint, standaloneRows, carriers, delivered, type Endpoint, type StandaloneRow }

import { Redis, type RedisOptions } from 'ioredis'
import { vi } from 'vitest'
import { Broadcast } from 'telefunc'
import { installRedis } from '../src/index.js'
import { reaches } from './acl.js'
import { forwarder, sent, type Forwarder } from './forwarder.js'
import { removeStores, startStore, type Store } from './stores.js'

/** How the backend names each subscriber connection it opens (`createSubscriberSocket()`): its start. */
const SUBSCRIBER_NAME = 'telefunc-subscriber-'

/** Where the subscribers the backend opens go, given the options of the client each is duplicated from. */
let route: ((source: RedisOptions) => Partial<RedisOptions>) | undefined

/** Sends each subscriber the backend opens from now on through what `subscriberOf` gives for the client it duplicates:
 *  the backend's command client, or a Cluster node of it. That is its port, or under Sentinel the forwarders its nodes'
 *  addresses map to. A row's `vi.restoreAllMocks()` ends it. */
function routeSubscribers(subscriberOf: (source: RedisOptions) => Partial<RedisOptions>): void {
  route = subscriberOf
  if (vi.isMockFunction(Redis.prototype.duplicate)) return
  const { duplicate } = Redis.prototype
  vi.spyOn(Redis.prototype, 'duplicate').mockImplementation(function (this: Redis, override) {
    const subscriber = override?.connectionName?.startsWith(SUBSCRIBER_NAME) === true
    return duplicate.call(this, subscriber && route !== undefined ? { ...override, ...route(this.options) } : override)
  })
}

/** A server's two role endpoints: the forwarder its command connections take and the one its subscribers take. `to()`
 *  points both at a store and forgets what they recorded and held. */
type Endpoint = { command: Forwarder; subscriber: Forwarder; to: (host: string) => void; close: () => void }

/** A new pair of role endpoints to the store at `host`, or nowhere (`''`). */
async function endpoint(host: string): Promise<Endpoint> {
  const [command, subscriber] = [await forwarder(), await forwarder()]
  const made = {
    command,
    subscriber,
    to: (to: string) => {
      for (const proxy of [command, subscriber]) {
        proxy.to(to)
        proxy.clear()
      }
    },
    close: () => {
      command.close()
      subscriber.close()
    },
  }
  made.to(host)
  return made
}

type StandaloneRow = Endpoint & { admin: Redis; host: string; app: Redis }

/** A spec file's rows on a server each: `reached()` starts a row's own store, with a user `telefunc` of `+@all`, and
 *  points the file's role endpoints at it, forgetting what they recorded and held, once the backend reaches it; `end()`,
 *  the file's `afterAll`, ends the client and the forwarders. The backend, one per process, is installed by the first
 *  row on an app client of the endpoints, with `options`: a row that needs other options has a file of its own. */
function standaloneRows(options: RedisOptions = {}) {
  let made: { ends: Endpoint; app: Redis } | undefined
  return {
    async reached(store: Store): Promise<StandaloneRow> {
      removeStores()
      const { host, admin } = await startStore(store)
      await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
      if (made === undefined) {
        const ends = await endpoint('')
        routeSubscribers(() => ({ port: ends.subscriber.port }))
        const app = new Redis({
          port: ends.command.port,
          username: 'telefunc',
          password: 'pw',
          lazyConnect: true,
          ...options,
        })
        app.on('error', () => {})
        installRedis(app)
        made = { ends, app }
      }
      made.ends.to(host)
      await reaches(store.name)
      return { ...made.ends, admin, host, app: made.app }
    },
    end(): void {
      made?.app.disconnect()
      made?.ends.close()
    },
  }
}

/** Which of `proxies`, by their index, carried a request that holds `text`. */
function carriers(proxies: readonly Forwarder[], text: string): number[] {
  return proxies.flatMap((proxy, i) => (sent(proxy, 'toStore', text) ? [i] : []))
}

/** What a subscription to `channel` received of one publish of 'one', made once it was ready. */
async function delivered(channel: string): Promise<unknown[]> {
  const got = Promise.withResolvers<void>()
  const messages: unknown[] = []
  const subscription = Broadcast.subscribe(channel, (message) => {
    messages.push(message)
    got.resolve()
  })
  await subscription.ready
  await Broadcast.publish(channel, 'one')
  await got.promise
  subscription()
  return messages
}
