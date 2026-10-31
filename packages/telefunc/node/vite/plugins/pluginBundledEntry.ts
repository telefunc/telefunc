export { pluginBundledEntry }

import type { Plugin } from 'vite'
import { realpathSync } from 'node:fs'
import { join, resolve, sep } from 'node:path'
import { createRequire } from 'node:module'
import { escapeRegex } from '../../../utils/escapeRegex.js'
import { isDevCheck } from '../../../utils/isDev.js'
import { getMagicString } from '../../shared/getMagicString.js'
import { getExtensionSpecifiers } from '../../shared/discoverExtensions.js'
import { DYNAMIC_IMPORT } from './non-runnable-dev/pluginReplaceConstantsNonRunnableDev.js'
import { getServerProductionEntryCode } from './pluginBuildEntry.js'

// Imported by importBundledEntry() where Vite bundles Telefunc (loadTelefuncFilesUsingVite.ts)
const IS_BUNDLED = 'globalThis.__TELEFUNC__IS_BUNDLED'
const VIRTUAL_FILE_BUNDLED_ENTRY_ID = 'virtual:vite:telefunc:bundled-entry'
const resolvedId = '\0' + VIRTUAL_FILE_BUNDLED_ENTRY_ID

declare global {
  var __TELEFUNC__IS_BUNDLED: undefined | true
}

function pluginBundledEntry(): Plugin[] {
  let root: string
  let isDev: boolean
  let specifiers: string[] = []
  return [
    {
      name: 'telefunc:pluginBundledEntry',
      config: {
        handler(config, configEnv) {
          root = resolve(config.root ?? process.cwd())
          isDev = isDevCheck(configEnv)
          specifiers = getExtensionSpecifiers(root, 'server')
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
        // Runs only where Vite bundles Telefunc; by content, as Vite's optimizer names its own chunks in dev
        filter: { code: { include: IS_BUNDLED } },
        handler(code, id) {
          const { magicString, getMagicStringResult } = getMagicString(code, id)
          magicString.replaceAll(IS_BUNDLED, JSON.stringify(true))
          magicString.replaceAll(DYNAMIC_IMPORT, 'import')
          return getMagicStringResult()
        },
      },
      resolveId: {
        filter: { id: new RegExp(`^${escapeRegex(VIRTUAL_FILE_BUNDLED_ENTRY_ID)}$`) },
        handler() {
          return resolvedId
        },
      },
      load: {
        filter: { id: new RegExp(`^${escapeRegex(resolvedId)}$`) },
        handler() {
          // Each extension's server entry, so that a Cloudflare Worker runs it before a Durable Object's first call
          const imports = specifiers.map((specifier) => `import ${JSON.stringify(specifier)};\n`).join('')
          // Only Telefunc's part of the server entry, in builds: other parts (e.g. Vike's) load the app's server
          return isDev ? imports : imports + getServerProductionEntryCode()
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
