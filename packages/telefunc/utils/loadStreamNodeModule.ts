export { loadStreamNodeModule }

import { import_ } from '@brillout/import'

// Because of Cloudflare Workers, we cannot statically import the `stream` module, instead we dynamically import it.
async function loadStreamNodeModule() {
  const streamModule = (await import_('stream')).default as Awaited<typeof import('stream')>
  const utilModule = (await import_('util')).default as Awaited<typeof import('util')>
  const pipeline = utilModule.promisify(streamModule.pipeline)
  const { Readable, Writable } = streamModule
  return { Readable, Writable, pipeline }
}
