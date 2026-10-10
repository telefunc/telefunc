import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { build } from 'esbuild'

const here = dirname(fileURLToPath(import.meta.url))

export async function bundleWorker(): Promise<string> {
  const result = await build({
    entryPoints: [resolve(here, 'worker-entry.ts')],
    bundle: true,
    format: 'esm',
    platform: 'neutral',
    conditions: ['workerd', 'worker'],
    target: 'es2022',
    write: false,
    external: ['cloudflare:workers', 'node:async_hooks'],
    logLevel: 'silent',
  })
  const output = result.outputFiles[0]
  if (output === undefined) throw new Error('Cloudflare Room CI bundle produced no output')
  return output.text
}
