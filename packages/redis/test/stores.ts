// The stores the store specs (`pnpm run test:redis-stores`) run against, each in Docker, and what they share.
export { STORES, runs, pullImage, startStore, removeStores, forwarder, type Store }

import { execFileSync } from 'node:child_process'
import { connect, createServer, type Socket } from 'node:net'
import { setTimeout as sleep } from 'node:timers/promises'
import { Redis } from 'ioredis'

type Store = { name: string; image: string; server: string }

/** The oldest and the newest Redis and Valkey Telefunc supports. */
const STORES: readonly Store[] = [
  { name: 'Redis 7.0.0', image: 'redis:7.0.0', server: 'redis-server' },
  { name: 'Redis 7.4.12', image: 'redis:7.4.12', server: 'redis-server' },
  { name: 'Valkey 7.2.5', image: 'valkey/valkey:7.2.5', server: 'valkey-server' },
  { name: 'Valkey 8.1.1', image: 'valkey/valkey:8.1.1', server: 'valkey-server' },
]

const docker = (...args: string[]) =>
  execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim()

/** Without Docker, the store specs skip here and fail on CI. */
const runs = (() => {
  try {
    docker('version', '--format', '{{.Server.Version}}')
    return true
  } catch {
    if (process.env.CI !== undefined) return true
    console.log('Skipping the Redis and Valkey store tests: Docker is not running here.')
    return false
  }
})()

/** The store's image, pulled again once when a pull fails, as a registry does now and then; a second failure ends the
 *  run with a sentence that says so. */
function pullImage(image: string): void {
  for (let attempt = 1; ; attempt++) {
    try {
      docker('pull', '--quiet', image)
      return
    } catch (error) {
      if (attempt === 2) {
        const reason = error instanceof Error ? error.message : String(error)
        throw new Error(`Docker couldn't pull ${image}, tried twice: ${reason}`)
      }
    }
  }
}

const containers: string[] = []

/** The store in a container of its own, once it answers; its address, and an admin client of it. */
async function startStore(store: Store, ...options: string[]): Promise<{ host: string; admin: Redis }> {
  const name = `tf-redis-stores-${process.pid}-${containers.length}`
  pullImage(store.image)
  docker(
    'run',
    '--detach',
    '--name',
    name,
    store.image,
    store.server,
    '--protected-mode',
    'no',
    '--save',
    '',
    ...options,
  )
  containers.push(name)
  const host = docker('inspect', '--format', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', name)
  const admin = new Redis({ host, retryStrategy: () => 50, maxRetriesPerRequest: null })
  admin.on('error', () => {})
  for (let tries = 0; (await admin.ping().catch(() => null)) !== 'PONG'; tries++) {
    if (tries > 200) throw new Error(`${store.name} never answered`)
    await sleep(25)
  }
  return { host, admin }
}

/** Removes every store this process started. */
function removeStores(): void {
  for (const name of containers.splice(0)) docker('rm', '--force', name)
}

/** The one place the app's client connects: a forwarder to the store under test, as the backend installs once per
 *  process. Pointing it at another store drops the connections to the last one, which the client then makes again.
 *  `connections` counts the connections made through it. */
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
    connections: () => connections,
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
