// The Docker the store specs run their stores in: its command, whether it is there, and the label of a run's containers.
export { docker, runs, RUN_LABEL }

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
