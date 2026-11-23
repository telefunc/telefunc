// The stores the store specs (`pnpm run test:redis-stores`) run against, each in Docker, and what they share.
export {
  STORES,
  runs,
  docker,
  startStore,
  hostOf,
  removeStores,
  forwarder,
  reached,
  endReached,
  snapshot,
  changedKeys,
  type Store,
}

import { connect, createServer, type Socket } from 'node:net'
import { setTimeout as sleep } from 'node:timers/promises'
import { isDeepStrictEqual } from 'node:util'
import { Redis } from 'ioredis'
import { inject } from 'vitest'
import { BackendError } from 'telefunc'
import { Room } from '@telefunc/room'
import { installRedis } from '../src/index.js'
import { docker, RUN_LABEL, runs, STORES, type Store } from './docker.js'

/** The stores this process started and hasn't removed, each with the admin client `startStore()` made of it. */
const containers: { name: string; admin: Redis }[] = []

/** The forwarders this process made and hasn't closed: `removeStores()` points them nowhere. */
const forwarders = new Set<{ to: (host: string) => void }>()

/** The store in a container of its own, once it answers; the container's name, its address, and an admin client of it. */
async function startStore(store: Store, ...options: string[]): Promise<{ name: string; host: string; admin: Redis }> {
  const name = `tf-redis-stores-${process.pid}-${containers.length}`
  docker(
    'run',
    '--detach',
    '--name',
    name,
    '--label',
    `${RUN_LABEL}=${storeSpecsRun()}`,
    store.image,
    store.server,
    '--protected-mode',
    'no',
    '--save',
    '',
    '--appendonly',
    'no',
    ...options,
  )
  const host = hostOf(name)
  const admin = new Redis({ host, retryStrategy: () => 50, maxRetriesPerRequest: null })
  admin.on('error', () => {})
  containers.push({ name, admin })
  for (let tries = 0; (await admin.ping().catch(() => null)) !== 'PONG'; tries++) {
    if (tries > 200) throw new Error(`${store.name} never answered`)
    await sleep(25)
  }
  return { name, host, admin }
}

/** The address of the store's container, which a restart can change. */
function hostOf(name: string): string {
  return docker('inspect', '--format', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', name)
}

/** The run's id, which its global setup provides. */
function storeSpecsRun(): string {
  const run = inject('storeSpecsRun')
  if (typeof run !== 'string')
    throw new Error(
      'The store specs run with packages/redis/vitest.stores.config.ts, whose global setup removes their containers.',
    )
  return run
}

/** Removes the stores this process started so far, as a row does before it starts its own. First their admin clients
 *  end and every forwarder points nowhere, as Docker gives a removed container's address to the next container it
 *  starts, another spec file's too: a client that reconnected to it would reach that store. Those a spec file leaves at
 *  its end go with the run's global teardown. */
function removeStores(): void {
  for (const proxy of forwarders) proxy.to('')
  for (const { name, admin } of containers.splice(0)) {
    admin.disconnect()
    docker('rm', '--force', name)
  }
}

/** The one place the app's client connects: a forwarder to the store under test, as the backend installs once per
 *  process. Pointing it at another store drops the connections to the last one, which the client then makes again;
 *  pointed nowhere (`''`), it ends each connection it takes. `connections` counts the connections made through it. */
async function forwarder(): Promise<{
  port: number
  connections: () => number
  to: (host: string) => void
  close: () => void
}> {
  let upstream = ''
  let connections = 0
  const sockets = new Set<Socket>()
  const server = createServer((client) => {
    connections++
    if (upstream === '') {
      client.destroy()
      return
    }
    const store = connect(6379, upstream)
    for (const socket of [client, store]) {
      sockets.add(socket)
      socket.on('error', () => {})
      socket.on('close', () => {
        sockets.delete(socket)
        client.destroy()
        store.destroy()
      })
    }
    client.pipe(store).pipe(client)
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('the forwarder has no port')
  const made = {
    port: address.port,
    connections: () => connections,
    to: (host: string) => {
      upstream = host
      for (const socket of sockets) socket.destroy()
    },
    close: () => {
      forwarders.delete(made)
      for (const socket of sockets) socket.destroy()
      server.close()
    },
  }
  forwarders.add(made)
  return made
}

/** The app's client a spec file's backend is installed on, and the forwarder it reaches each row's store through: made
 *  by the file's first `reached()`. */
let reach: { proxy: Awaited<ReturnType<typeof forwarder>>; app: Redis } | undefined

/** A new store of the row's, which the backend reaches through the forwarder, logging in as `telefunc`. The last row's
 *  store goes first, so a file leaves only its last row's to the run's global teardown. */
async function reached(store: Store): Promise<Redis> {
  if (reach === undefined) {
    const proxy = await forwarder()
    const app = new Redis({ port: proxy.port, username: 'telefunc', password: 'pw', lazyConnect: true })
    app.on('error', () => {})
    installRedis(app)
    reach = { proxy, app }
  }
  removeStores()
  const { host, admin } = await startStore(store)
  await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
  reach.proxy.to(host)
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

/** Ends the client and the forwarder `reached()` made: a spec file's `afterAll`. */
function endReached(): void {
  reach?.app.disconnect()
  reach?.proxy.close()
  reach = undefined
}

/** Every key under Telefunc's prefix and what it holds as the store's bytes, a hash's fields in their bytes' order. */
async function snapshot(admin: Redis): Promise<Map<string, unknown>> {
  const keys = (await admin.keys('tf:*')).sort()
  const held = async (key: string) =>
    (await admin.type(key)) === 'hash' ? hashFields(admin, key) : admin.dumpBuffer(key)
  return new Map(await Promise.all(keys.map(async (key) => [key, await held(key)] as const)))
}

/** The hash's fields and values as raw bytes, which text would decode alike where a byte isn't UTF-8 on its own. */
async function hashFields(admin: Redis, key: string): Promise<[field: Buffer, value: Buffer][]> {
  // `hgetallBuffer` decodes the fields, so HGETALL's reply is read as it came: field, value, field, value.
  const reply = await admin.callBuffer('HGETALL', key)
  if (!Array.isArray(reply)) throw new Error(`HGETALL ${key} answered ${String(reply)}`)
  const parts: unknown[] = reply
  const pairs: [Buffer, Buffer][] = []
  for (let i = 0; i < parts.length; i += 2) {
    const [field, value] = [parts[i], parts[i + 1]]
    if (!Buffer.isBuffer(field) || !Buffer.isBuffer(value))
      throw new Error(`HGETALL ${key} answered a part that isn't bytes`)
    pairs.push([field, value])
  }
  return pairs.sort(([a], [b]) => Buffer.compare(a, b))
}

/** The keys whose value differs between two snapshots, one taken before and one after. */
function changedKeys(before: Map<string, unknown>, after: Map<string, unknown>): string[] {
  const keys = new Set([...before.keys(), ...after.keys()])
  return [...keys].filter((key) => !isDeepStrictEqual(before.get(key), after.get(key))).sort()
}
