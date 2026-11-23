// The store specs' global setup: an id for the run, which labels every container its spec files start, and the removal
// of those containers once the last file has ended. A run that ends without it, its main process or a worker killed,
// leaves them to the next run's setup, which first removes the containers of every run that is gone.
export { setup }

import { readFileSync } from 'node:fs'
import { hostname } from 'node:os'
import type { TestProject } from 'vitest/node'
import { docker, RUN_LABEL, runs } from './docker.js'

declare module 'vitest' {
  export interface ProvidedContext {
    storeSpecsRun: string
  }
}

/** This boot of this machine, so that a run's id outlives no reboot; `undefined` where the system doesn't say. */
const BOOT = (() => {
  try {
    return readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim()
  } catch {
    return undefined
  }
})()

function setup(project: TestProject): () => void {
  // The machine, its boot and the run's main process: no other run alive has the same.
  const run = `${hostname()}/${BOOT ?? 'unknown'}/${process.pid}`
  project.provide('storeSpecsRun', run)
  if (runs) removeContainers(listed().filter(({ of }) => isGone(of)))
  return () => {
    if (runs) removeContainers(listed().filter(({ of }) => of === run))
  }
}

/** Every container a run of the store specs started, with its run's id. */
function listed(): { id: string; of: string }[] {
  const lines = docker('ps', '--all', '--filter', `label=${RUN_LABEL}`, '--format', `{{.ID}} {{.Label "${RUN_LABEL}"}}`)
  return lines.split('\n').flatMap((line) => {
    const [id, of] = line.split(' ')
    return id && of ? [{ id, of }] : []
  })
}

/** Whether the run `of` names is gone: started on this machine, and on an earlier boot or by a process that has
 *  ended. A run on another machine, or on one that doesn't say its boot, is never taken for gone. */
function isGone(of: string): boolean {
  const [host, boot, pid] = of.split('/')
  if (host !== hostname() || BOOT === undefined) return false
  if (boot !== BOOT) return true
  try {
    process.kill(Number(pid), 0)
    return false
  } catch (error) {
    return error instanceof Error && 'code' in error && error.code === 'ESRCH'
  }
}

function removeContainers(containers: { id: string }[]): void {
  if (containers.length > 0) docker('rm', '--force', ...containers.map(({ id }) => id))
}
