export default setup
export { buildCopy, withBuildLock }
export type { PageBuild }

import { spawn } from 'node:child_process'
import { cp, mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises'
import type { TestProject } from 'vitest/node'
import { hasProp } from '../utils/hasProp.js'

// vitest's `globalSetup` for the spec pages (`page.worker.mjs`): the package as built, once before the specs and again
// before each watch rerun. tsc rewrites `dist/` in place, so a page reading it while another run builds could read a
// file cut short: each run's pages read a copy of `dist/` of their own, which appears whole as one rename.

/** The copy of the package as built that the pages import, or why there is none. */
type PageBuild = { dist: string } | { failed: string }

declare module 'vitest' {
  export interface ProvidedContext {
    pageBuild: PageBuild
  }
}

const packageDir = new URL('../', import.meta.url)
const copiesDir = new URL('node_modules/.page-dist/', packageDir)
const lockDir = new URL('lock/', copiesDir)
let copies = 0

async function setup(project: TestProject): Promise<() => Promise<void>> {
  await removeCopiesOfEndedRuns()
  let current: string | null = null
  const provide = async () => {
    const previous = current
    const build = await buildCopy()
    current = 'dist' in build ? build.dist : null
    project.provide('pageBuild', build)
    if (previous !== null) await rm(new URL('../', previous), { recursive: true, force: true })
  }
  await provide()
  project.onTestsRerun(provide)
  return async () => {
    if (current !== null) await rm(new URL('../', current), { recursive: true, force: true })
  }
}

/** The package's incremental `tsc --build`, then a copy of its `dist/`, named for this process, renamed into place. The
 *  copy keeps its `dist/` folder: the Vite plugin reads its place in the package off its own path. */
async function buildCopy(): Promise<PageBuild> {
  return withBuildLock(async () => {
    const tsc = spawn('pnpm', ['exec', 'tsc', '--build'], { cwd: packageDir })
    let output = ''
    tsc.stdout.on('data', (chunk: Buffer) => (output += chunk))
    tsc.stderr.on('data', (chunk: Buffer) => (output += chunk))
    const status = await new Promise<number | null>((resolve, reject) => {
      tsc.once('error', reject)
      tsc.once('close', resolve)
    })
    if (status !== 0) return { failed: `The package's build, \`tsc --build\`, failed with ${status}:\n${output}` }
    const name = `${process.pid}-${copies++}`
    const partial = new URL(`${name}.partial/`, copiesDir)
    const copy = new URL(`${name}/`, copiesDir)
    await cp(new URL('dist/', packageDir), new URL('dist/', partial), { recursive: true })
    await rename(partial, copy)
    return { dist: new URL('dist/', copy).href }
  })
}

/** Runs `build` while no other process of this checkout builds: a lock directory that appears with its owner's pid in
 *  it, as one rename, and is taken over from an owner that ended. */
async function withBuildLock<T>(build: () => Promise<T>): Promise<T> {
  await mkdir(copiesDir, { recursive: true })
  const mine = new URL(`${process.pid}.lock/`, copiesDir)
  for (;;) {
    await mkdir(mine, { recursive: true })
    await writeFile(new URL('pid', mine), String(process.pid))
    try {
      await rename(mine, lockDir)
      break
    } catch (err) {
      if (!hasProp(err, 'code', 'string') || (err.code !== 'ENOTEMPTY' && err.code !== 'EEXIST')) throw err
      const owner = Number(await readFile(new URL('pid', lockDir), 'utf8').catch(() => ''))
      if (owner > 0 && !isRunning(owner)) await rm(lockDir, { recursive: true, force: true })
      else await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
  try {
    return await build()
  } finally {
    await rm(lockDir, { recursive: true, force: true })
  }
}

/** A run that ended without its teardown leaves its copy: the next run removes the copies of processes that ended. */
async function removeCopiesOfEndedRuns(): Promise<void> {
  const names = await readdir(copiesDir).catch(() => [])
  for (const name of names) {
    const owner = Number(name.split(/[-.]/)[0])
    if (owner > 0 && !isRunning(owner)) await rm(new URL(name, copiesDir), { recursive: true, force: true })
  }
}

function isRunning(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch (err) {
    return hasProp(err, 'code', 'string') && err.code === 'EPERM'
  }
}
