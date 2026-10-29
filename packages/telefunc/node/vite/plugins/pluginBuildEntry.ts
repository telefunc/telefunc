export { pluginBuildEntry }

import { serverProductionEntryPlugin } from '@brillout/vite-plugin-server-entry/plugin'
import type { Plugin } from 'vite'
import { projectInfo } from '../../../utils/projectInfo.js'
import { VIRTUAL_FILE_ENTRY_ID } from './pluginVirtualFileEntry/VIRTUAL_FILE_ENTRY_ID.js'
import { config } from '../../server/serverConfig.js'
import { assertManifest, type Manifest } from '../../server/runTelefunc/loadTelefuncFilesUsingVite/assertManifest.js'
import { getMagicString } from '../../shared/getMagicString.js'
import { escapeRegex } from '../../../utils/escapeRegex.js'
import { DYNAMIC_IMPORT } from './non-runnable-dev/pluginReplaceConstantsNonRunnableDev.js'

const IS_BUNDLED = 'globalThis.__TELEFUNC__IS_BUNDLED'
const VIRTUAL_FILE_SERVER_PRODUCTION_ENTRY_ID = 'virtual:vite:telefunc:server-production-entry'
const resolvedId = '\0' + VIRTUAL_FILE_SERVER_PRODUCTION_ENTRY_ID

declare global {
  var __TELEFUNC__IS_BUNDLED: undefined | true
}

function pluginBuildEntry(): Plugin[] {
  return [
    ...serverProductionEntryPlugin({
      getServerProductionEntry: () => {
        return getServerProductionEntryCode()
      },
      libraryName: projectInfo.projectName,
    }),
    {
      name: 'telefunc:pluginBuildEntry:bundled',
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
        filter: { id: new RegExp(`^${escapeRegex(VIRTUAL_FILE_SERVER_PRODUCTION_ENTRY_ID)}$`) },
        handler() {
          return resolvedId
        },
      },
      load: {
        filter: { id: new RegExp(`^${escapeRegex(resolvedId)}$`) },
        handler() {
          // Only Telefunc's part: other parts of the server entry (e.g. Vike's) load the app's server
          return getServerProductionEntryCode()
        },
      },
    },
  ]
}

function getServerProductionEntryCode() {
  const telefuncManifest = getTelefuncManifest()

  const importerCode = [
    `import { setTelefuncLoaders } from 'telefunc/__internal/loadBuildEntry';`,
    `import * as telefuncFiles from '${VIRTUAL_FILE_ENTRY_ID}';`,
    'setTelefuncLoaders({',
    `  loadTelefuncFiles: () => telefuncFiles,`,
    `  loadManifest: () => (${JSON.stringify(telefuncManifest, null, 2)})`,
    '});',
    '',
  ].join('\n')
  return importerCode
}

function getTelefuncManifest(): Manifest {
  const manifest = {
    version: projectInfo.projectVersion,
    config,
  }
  assertManifest(manifest)
  return manifest
}
