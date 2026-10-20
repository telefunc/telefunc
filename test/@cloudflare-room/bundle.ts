import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { build, type Plugin } from 'esbuild'

const here = dirname(fileURLToPath(import.meta.url))
const SECOND = 'second'

// `./second-copy.js` and every module it imports load a second time, as a second install of the package would, sharing
// its dependencies.
const secondCopy: Plugin = {
  name: 'second-copy',
  setup(build) {
    build.onResolve({ filter: /.*/ }, async (args) => {
      if (args.pluginData === SECOND) return undefined
      const root = args.path === './second-copy.js'
      if (!root && args.namespace !== SECOND) return undefined
      const { kind, importer, resolveDir } = args
      const resolved = await build.resolve(args.path, { kind, importer, resolveDir, pluginData: SECOND })
      if (resolved.errors.length > 0) return { errors: resolved.errors }
      if (resolved.external || resolved.path.includes('/node_modules/')) return resolved
      return { path: resolved.path, namespace: SECOND }
    })
    build.onLoad({ filter: /.*/, namespace: SECOND }, async (args) => ({
      contents: await readFile(args.path, 'utf8'),
      loader: args.path.endsWith('.ts') ? 'ts' : 'js',
      resolveDir: dirname(args.path),
    }))
  },
}

export async function bundleWorker(entry = 'worker-entry.ts'): Promise<string> {
  const result = await build({
    entryPoints: [resolve(here, entry)],
    bundle: true,
    format: 'esm',
    platform: 'neutral',
    conditions: ['workerd', 'worker'],
    target: 'es2022',
    write: false,
    external: ['cloudflare:workers', 'node:async_hooks'],
    plugins: [secondCopy],
    logLevel: 'silent',
  })
  const output = result.outputFiles[0]
  if (output === undefined) throw new Error('Cloudflare Room CI bundle produced no output')
  return output.text
}
