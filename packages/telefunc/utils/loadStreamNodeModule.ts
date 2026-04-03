export { loadStreamNodeModule }

import { import_ } from '@brillout/import'
// Because of Cloudflare Workers, we cannot statically import the `stream` module, instead we dynamically import it.

async function loadStreamNodeModule() {
  const streamModule = (await import_('node:stream')).default as Awaited<typeof import('node:stream')>
  const { Readable, Writable } = streamModule
  const { pipeline } = streamModule.promises
  return { Readable, Writable, pipeline }
}
