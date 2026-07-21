// Bundles the workerd entry (and the production DO it imports) into a single ESM string for Miniflare.
// The suite runs against current source, not a prebuilt dist, so a code change to the DO is reflected
// without a separate build step. `cloudflare:workers` is external (provided by the workerd runtime).
//
// esbuild resolves the repo's `.js` specifiers back to their `.ts` sources via the small plugin below, so
// the same NodeNext-style relative imports the DO uses at compile time bundle here too.

import { existsSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { build, type Plugin } from 'esbuild'

const here = dirname(fileURLToPath(import.meta.url))

const resolveJsToTs: Plugin = {
  name: 'js-to-ts',
  setup(builder) {
    builder.onResolve({ filter: /\.js$/ }, (args) => {
      if (args.kind === 'entry-point' || args.importer === '') return null
      const abs = resolve(args.resolveDir, args.path)
      const tsPath = abs.replace(/\.js$/, '.ts')
      if (existsSync(tsPath)) return { path: tsPath }
      return null
    })
  },
}

let cached: string | null = null

export async function bundleWorker(): Promise<string> {
  if (cached !== null) return cached
  const result = await build({
    entryPoints: [resolve(here, 'worker-entry.ts')],
    bundle: true,
    format: 'esm',
    platform: 'neutral',
    target: 'es2022',
    write: false,
    external: ['cloudflare:workers'],
    plugins: [resolveJsToTs],
    logLevel: 'silent',
  })
  const output = result.outputFiles[0]
  if (output === undefined) throw new Error('bundleWorker: esbuild produced no output')
  cached = output.text
  return cached
}
