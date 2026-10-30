export { pluginCloudflareExtensions }

import type { Plugin } from 'vite'
import { realpathSync } from 'node:fs'
import { join, resolve, sep } from 'node:path'
import { createRequire } from 'node:module'
import { iterateExtensions } from '../../shared/discoverExtensions.js'
import { escapeRegex } from '../../../utils/escapeRegex.js'
import { getMagicString } from '../../shared/getMagicString.js'
import { DYNAMIC_IMPORT } from './non-runnable-dev/pluginReplaceConstantsNonRunnableDev.js'

// telefunc/cloudflare imports `virtual:telefunc:cloudflare-extensions` where Vite bundles it (serve/cloudflare.ts). This
// plugin fills it with an import of each extension's server entry, the imports it prepends to every .telefunc file, so
// that the entries run when the Worker starts, before a Durable Object's first alarm() or call.
const IS_BUNDLED = 'globalThis.__TELEFUNC__IS_BUNDLED'
const VIRTUAL_ID = 'virtual:telefunc:cloudflare-extensions'
const RESOLVED_VIRTUAL_ID = '\0' + VIRTUAL_ID

declare global {
  var __TELEFUNC__IS_BUNDLED: undefined | true
}

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
          if (config.resolve?.noExternal !== true || specifiers.length === 0) return
          // In the Worker's first optimizer run: a dependency found later reloads the Worker, which runs the entries
          // again. A linked package isn't pre-bundled, as Vite does for those it finds, else it'd load twice.
          const include = ['telefunc', ...specifiers].filter((specifier) => isInNodeModules(root, specifier))
          return { optimizeDeps: { include, entries: ['**/*.telefunc.{js,ts,mjs,mts,jsx,tsx}'] } }
        },
      },
      transform: {
        // By content: Vite's optimizer bundles telefunc/cloudflare into a chunk of its own name in dev.
        filter: { code: { include: IS_BUNDLED } },
        handler(code, id) {
          const { magicString, getMagicStringResult } = getMagicString(code, id)
          magicString.replaceAll(IS_BUNDLED, JSON.stringify(true))
          magicString.replaceAll(DYNAMIC_IMPORT, 'import')
          return getMagicStringResult()
        },
      },
      resolveId: {
        filter: { id: new RegExp(`^${escapeRegex(VIRTUAL_ID)}$`) },
        handler() {
          return RESOLVED_VIRTUAL_ID
        },
      },
      load: {
        filter: { id: new RegExp(`^${escapeRegex(RESOLVED_VIRTUAL_ID)}$`) },
        handler() {
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
