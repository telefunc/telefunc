import { existsSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { build, type Plugin } from 'esbuild'

const here = dirname(fileURLToPath(import.meta.url))

const resolveJsToTs: Plugin = {
  name: 'js-to-ts',
  setup(builder) {
    builder.onResolve({ filter: /\.js$/ }, (args) => {
      const tsPath = resolve(args.resolveDir, args.path).replace(/\.js$/, '.ts')
      return existsSync(tsPath) ? { path: tsPath } : null
    })
  },
}

export async function bundleWorker(): Promise<string> {
  const result = await build({
    entryPoints: [resolve(here, 'worker-entry.ts')],
    bundle: true,
    format: 'esm',
    platform: 'neutral',
    conditions: ['workerd', 'worker'],
    target: 'es2022',
    write: false,
    external: ['cloudflare:workers'],
    plugins: [resolveJsToTs],
    logLevel: 'silent',
  })
  const output = result.outputFiles[0]
  if (output === undefined) throw new Error('Cloudflare Room CI bundle produced no output')
  return output.text
}
