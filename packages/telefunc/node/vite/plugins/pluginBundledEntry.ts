export { pluginBundledEntry }

import type { Plugin } from 'vite'
import { escapeRegex } from '../../../utils/escapeRegex.js'
import { getMagicString } from '../../shared/getMagicString.js'
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
  return [
    {
      name: 'telefunc:pluginBundledEntry',
      apply: 'build',
      transform: {
        // Runs only where Vite bundles Telefunc, which then loads the telefunctions without the auto importer
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
          // Only Telefunc's part of the server entry: other parts (e.g. Vike's) load the app's server
          return getServerProductionEntryCode()
        },
      },
    },
  ]
}
