export { pluginCloudflareExtensions }

import type { Plugin } from 'vite'
import { realpathSync } from 'node:fs'
import { join, resolve, sep } from 'node:path'
import { createRequire } from 'node:module'
import { iterateExtensions } from '../../shared/discoverExtensions.js'
import { escapeRegex } from '../../../utils/escapeRegex.js'

// telefunc/cloudflare imports `telefunc/__internal/cloudflare-extensions` (serve/cloudflare.ts), an empty module. This
// plugin fills it with an import of each extension's server entry, the imports it prepends to every .telefunc file, so
// that the entries run when the Worker starts, before a Durable Object's first alarm() or call.
const HOST_SPECIFIER = 'telefunc/__internal/cloudflare-extensions'
const HOST_FILE = /[\\/]telefunc[\\/]dist[\\/]serve[\\/]cloudflareExtensions\.js(\?.*)?$/
// The entries are imported from a virtual module, so that they resolve from the app's root, as a .telefunc file's do.
const VIRTUAL_ID = 'virtual:telefunc:cloudflare-extensions'
const RESOLVED_VIRTUAL_ID = '\0' + VIRTUAL_ID

function pluginCloudflareExtensions(): Plugin[] {
  let root: string
  let specifiers: string[] = []
  return [
    {
      name: 'telefunc:pluginCloudflareExtensions',
      config: {
        handler(config) {
          root = resolve(config.root ?? process.cwd())
          specifiers = [...iterateExtensions(root)].flatMap(({ telefunc }) => telefunc.server ?? [])
        },
      },
      configEnvironment: {
        // After @cloudflare/vite-plugin's, which makes its Worker's environment bundle every dependency.
        order: 'post',
        handler(name, config) {
          if (name === 'client' || config.consumer === 'client') return
          // Excluded, a pre-bundled telefunc keeps the import, which then goes through load() as in a build.
          const exclude = [HOST_SPECIFIER]
          if (config.resolve?.noExternal !== true || specifiers.length === 0) return { optimizeDeps: { exclude } }
          // In the Worker's first optimizer run: a dependency found later reloads the Worker, which runs the entries
          // again. A linked package isn't pre-bundled, as Vite does for those it finds, else it'd load twice.
          const include = ['telefunc', ...specifiers].filter((specifier) => isInNodeModules(root, specifier))
          return { optimizeDeps: { exclude, include, entries: ['**/*.telefunc.{js,ts,mjs,mts,jsx,tsx}'] } }
        },
      },
      resolveId: {
        filter: { id: new RegExp(`^${escapeRegex(VIRTUAL_ID)}$`) },
        handler() {
          return RESOLVED_VIRTUAL_ID
        },
      },
      load: {
        filter: { id: [HOST_FILE, new RegExp(`^${escapeRegex(RESOLVED_VIRTUAL_ID)}$`)] },
        handler(id) {
          if (id !== RESOLVED_VIRTUAL_ID) return `import ${JSON.stringify(VIRTUAL_ID)};\n`
          return specifiers.map((specifier) => `import ${JSON.stringify(specifier)};\n`).join('')
        },
      },
    },
  ]
}

function isInNodeModules(root: string, specifier: string): boolean {
  try {
    const resolved = realpathSync(createRequire(join(root, 'package.json')).resolve(specifier))
    return resolved.split(sep).includes('node_modules')
  } catch {
    return false
  }
}
