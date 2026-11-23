// The Docker the store specs run their stores in: its command, whether it is there, the stores' images, and the label of
// a run's containers.
export { docker, runs, RUN_LABEL, STORES, pullImages, type Store }

import { execFileSync } from 'node:child_process'

/** Each container a run of the store specs starts carries it, with the run's id: the run's global teardown removes
 *  them by it, and a later run's global setup removes them once that run is gone. */
const RUN_LABEL = 'dev.telefunc.store-specs.run'

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

type Store = { name: string; image: string; server: string }

/** The oldest and the newest Redis and Valkey Telefunc supports. */
const STORES: readonly Store[] = [
  { name: 'Redis 7.0.0', image: 'redis:7.0.0', server: 'redis-server' },
  { name: 'Redis 7.4.12', image: 'redis:7.4.12', server: 'redis-server' },
  { name: 'Valkey 7.2.5', image: 'valkey/valkey:7.2.5', server: 'valkey-server' },
  { name: 'Valkey 8.1.1', image: 'valkey/valkey:8.1.1', server: 'valkey-server' },
]

/** Each store's image, pulled once by the run's global setup, before any row starts a store: a pull can take seconds.
 *  An image is pulled again once when its pull fails, as a registry does now and then; a second failure ends the run
 *  with a sentence that says so. */
function pullImages(): void {
  for (const image of new Set(STORES.map((store) => store.image))) pullImage(image)
}

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
