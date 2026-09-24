export { loadStreamNodeModule, loadStreamNodeModuleOnce, getStreamNodeModule }

import { import_ } from '@brillout/import'
import { getGlobalObject } from './getGlobalObject.js'
// Because of Cloudflare Workers, we cannot statically import the `stream` module, instead we dynamically import it.

async function loadStreamNodeModule() {
  const streamModule = (await import_('node:stream')).default as Awaited<typeof import('node:stream')>
  const { Readable, Writable } = streamModule
  const { pipeline } = streamModule.promises
  return { Readable, Writable, pipeline }
}

type StreamModule = Awaited<ReturnType<typeof loadStreamNodeModule>>

const globalObject = getGlobalObject<{
  module: StreamModule | null
  loadPromise: Promise<void> | null
}>('loadStreamNodeModule.ts', { module: null, loadPromise: null })

async function loadStreamNodeModuleOnce(): Promise<void> {
  if (globalObject.loadPromise) return globalObject.loadPromise
  globalObject.loadPromise = (async () => {
    try {
      globalObject.module = await loadStreamNodeModule()
    } catch {}
  })()
  return globalObject.loadPromise
}

/** Sync access to the cached `node:stream`. `loadStreamNodeModuleOnce()` must
 *  have been awaited at least once before the first call — `runTelefunc`
 *  does this at its entrypoint so all downstream sync access is safe. */
function getStreamNodeModule(): StreamModule {
  if (!globalObject.module) {
    throw new Error('`loadStreamNodeModuleOnce()` must be awaited before `getStreamNodeModule()`.')
  }
  return globalObject.module
}
