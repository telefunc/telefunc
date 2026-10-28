export { pluginBuildEntry }

import { serverProductionEntryPlugin } from '@brillout/vite-plugin-server-entry/plugin'
import type { Plugin } from 'vite'
import { projectInfo } from '../../../utils/projectInfo.js'
import { VIRTUAL_FILE_ENTRY_ID } from './pluginVirtualFileEntry/VIRTUAL_FILE_ENTRY_ID.js'
import { config } from '../../server/serverConfig.js'
import { assertManifest, type Manifest } from '../../server/runTelefunc/loadTelefuncFilesUsingVite/assertManifest.js'
import { getMagicString } from '../../shared/getMagicString.js'

function pluginBuildEntry(): Plugin[] {
  return [
    ...serverProductionEntryPlugin({
      getServerProductionEntry: () => {
        return getServerProductionEntryCode()
      },
      libraryName: projectInfo.projectName,
    }),
    {
      name: 'telefunc:pluginBuildEntry:cloudflare',
      apply: 'build',
      transform: {
        // The auto importer can't load the server entry inside a Cloudflare Worker: it imports a file path
        filter: { id: /^\0virtual:cloudflare\/worker-entry$/ },
        handler(code, id) {
          const { magicString, getMagicStringResult } = getMagicString(code, id)
          // Only Telefunc's part: other parts of the server entry (e.g. Vike's) load the main Worker's app
          magicString.prepend(getServerProductionEntryCode())
          return getMagicStringResult()
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
