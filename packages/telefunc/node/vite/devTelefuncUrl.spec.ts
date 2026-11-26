// `vite dev` serves Telefunc's requests at the app's `config.telefuncUrl`, and leaves every other URL to the app: the
// plugin as an app installs it, the package as built, on Vite's own server.

import { afterAll, beforeAll, describe, expect, inject, test } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { ViteDevServer } from 'vite'

// Vite 8's bundler, rolldown, installs its native binding on Node 22.12 and later only.
const [major = 0, minor = 0] = process.versions.node.split('.').map(Number)
const nodeRunsVite = major > 22 || (major === 22 && minor >= 12)

describe.skipIf(!nodeRunsVite)('`vite dev`', () => {
  let server: ViteDevServer | undefined
  let root: string | undefined
  let origin: string

  beforeAll(async () => {
    const build = inject('pageBuild')
    if (!('dist' in build)) throw new Error(build.failed)
    const { config }: typeof import('../server/index.js') = await import(
      new URL('node/server/index.js', build.dist).href
    )
    const { telefunc }: typeof import('./index.js') = await import(new URL('node/vite/index.js', build.dist).href)
    // As an app with no server code of its own sets it, in its Vite config.
    config.telefuncUrl = '/api/_telefunc'
    root = await mkdtemp(join(tmpdir(), 'telefunc-vite-dev-'))
    const { createServer } = await import('vite')
    server = await createServer({
      configFile: false,
      root,
      logLevel: 'silent',
      server: { host: '127.0.0.1', port: 0 },
      plugins: [telefunc()],
    })
    await server.listen()
    const address = server.httpServer?.address()
    if (!address || typeof address === 'string') throw new Error('no port')
    origin = `http://127.0.0.1:${address.port}`
  })
  afterAll(async () => {
    await server?.close()
    if (root) await rm(root, { recursive: true, force: true })
  })

  /** What the server answers a POST to `path` whose body names no telefunction. */
  async function answer(path: string): Promise<{ status: number; body: string }> {
    const response = await fetch(origin + path, { method: 'POST', body: 'nope' })
    return { status: response.status, body: await response.text() }
  }

  test("`vite dev` serves Telefunc's requests at a custom `config.telefuncUrl`", async () => {
    const telefuncs = { status: 400, body: 'Malformed Telefunc Request' }
    expect(await answer('/api/_telefunc')).toEqual(telefuncs)
    expect(await answer('/api/_telefunc?client=1')).toEqual(telefuncs)
  })

  test('`vite dev` leaves the default URL to the app once `config.telefuncUrl` names another', async () => {
    expect(await answer('/_telefunc')).toEqual({ status: 404, body: '' })
  })
})
