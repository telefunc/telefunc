// A store that is loading its data set answers LOADING. Telefunc tells such a call `'not-run'`: here, on the oldest and
// the newest Redis and Valkey Telefunc supports, each in Docker, every public write made while the store loads is
// refused, and once the load is over no key of Telefunc's is in the store. A machine without Docker skips them; CI runs
// them.
import { execFileSync } from 'node:child_process'
import { connect, createServer, type Socket } from 'node:net'
import { setTimeout as sleep } from 'node:timers/promises'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { BackendError, Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'
import { installRedis } from './index.js'

const STORES = [
  { name: 'Redis 7.0.0', image: 'redis:7.0.0', server: 'redis-server' },
  { name: 'Redis 7.4.12', image: 'redis:7.4.12', server: 'redis-server' },
  { name: 'Valkey 7.2.5', image: 'valkey/valkey:7.2.5', server: 'valkey-server' },
  { name: 'Valkey 8.1.1', image: 'valkey/valkey:8.1.1', server: 'valkey-server' },
]
/** Keys of the data set the store loads, and the wait per key while loading it: about 6 s of LOADING. */
const LOADED_KEYS = 200_000
const LOAD_DELAY_US = 30

const docker = (...args: string[]) =>
  execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim()
const hasDocker = (() => {
  try {
    docker('version', '--format', '{{.Server.Version}}')
    return true
  } catch {
    return false
  }
})()
const containers: string[] = []

/** A client of the store at `host` that sends its commands while the store loads, as ioredis's ready check would wait
 *  out the load. */
function clientOf(host: string): Redis {
  const redis = new Redis({ host, enableReadyCheck: false, retryStrategy: () => 50, maxRetriesPerRequest: null })
  redis.on('error', () => {})
  return redis
}

async function loading(admin: Redis): Promise<boolean> {
  return /^loading:1\r?$/m.test(await admin.info('persistence'))
}

/** The store, restarted with a data set it loads slowly; its admin client once the store answers LOADING. */
async function loadingStore(store: (typeof STORES)[number]): Promise<{ host: string; admin: Redis }> {
  docker('pull', '--quiet', store.image)
  const container = docker(
    'run',
    '--detach',
    store.image,
    store.server,
    '--protected-mode',
    'no',
    '--save',
    '',
    '--appendonly',
    'no',
    '--enable-debug-command',
    'yes',
    '--key-load-delay',
    String(LOAD_DELAY_US),
    // It answers clients while it loads only between such chunks of its data set, by default 2 MB.
    '--loading-process-events-interval-bytes',
    '1024',
  )
  containers.push(container)
  const host = () =>
    docker('inspect', '--format', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', container)
  const filler = clientOf(host())
  await filler.call('DEBUG', 'POPULATE', String(LOADED_KEYS), 'loaded')
  await filler.call('SAVE')
  filler.disconnect()
  docker('restart', '--time', '0', container)
  const admin = clientOf(host())
  for (let tries = 0; !(await loading(admin).catch(() => false)); tries++) {
    if (tries > 200) throw new Error(`${store.name} never answered LOADING`)
    await sleep(10)
  }
  return { host: host(), admin }
}

/** The one place the app's client connects: a forwarder to the store under test, as the backend installs once per
 *  process. Pointing it at another store drops the connections to the last one, which the client then makes again. */
async function forwarder(): Promise<{ port: number; to: (host: string) => void; close: () => void }> {
  let upstream = ''
  const sockets = new Set<Socket>()
  const server = createServer((client) => {
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
  return {
    port: address.port,
    to: (host) => {
      upstream = host
      for (const socket of sockets) socket.destroy()
    },
    close: () => {
      for (const socket of sockets) socket.destroy()
      server.close()
    },
  }
}

const runs = hasDocker || process.env.CI !== undefined
if (!runs) console.log('Skipping the Redis and Valkey store tests: Docker is not running here.')

describe.runIf(runs)('a write made while the store loads its data set', () => {
  let proxy: Awaited<ReturnType<typeof forwarder>> | undefined
  let app: Redis | undefined
  afterAll(() => {
    app?.disconnect()
    proxy?.close()
    for (const container of containers) docker('rm', '--force', container)
  })

  for (const store of STORES) {
    it(`${store.name}: is told 'not-run', and the store holds no key of Telefunc's after the load`, async () => {
      if (proxy === undefined) {
        proxy = await forwarder()
        app = new Redis({ port: proxy.port, enableReadyCheck: false, retryStrategy: () => 50, lazyConnect: true })
        app.on('error', () => {})
        installRedis(app)
      }
      const { host, admin } = await loadingStore(store)
      proxy.to(host)
      // A read answered LOADING: the backend's command client reached this store, so the writes below are its own.
      for (let tries = 0; ; tries++) {
        const read = await Room.get('r-none').catch((error: unknown) => error)
        if (read instanceof BackendError && read.code === 'unavailable') break
        if (tries > 200) throw new Error(`the backend never reached ${store.name} while it loads`)
        await sleep(10)
      }
      const started = Date.now()
      const told = (call: () => Promise<unknown>) =>
        call().then(
          () => ({ answered: true, ms: Date.now() - started }),
          (error: unknown) =>
            error instanceof BackendError
              ? { code: error.code, outcome: error.outcome, message: error.message, ms: Date.now() - started }
              : { notABackendError: String(error) },
        )
      const calls = {
        publish: await told(async () => Broadcast.publish('k', 'while loading')),
        create: await told(() => Room.create(`r-${store.image}`)),
      }
      const stillLoading = await loading(admin)
      while (await loading(admin)) await sleep(50)
      const keys = await admin.keys('tf:*')
      admin.disconnect()

      const refused = { code: 'unavailable', outcome: 'not-run' }
      expect({ stillLoading, calls, keys }).toMatchObject({
        stillLoading: true,
        calls: { publish: refused, create: refused },
        keys: [],
      })
    }, 120_000)
  }
})
