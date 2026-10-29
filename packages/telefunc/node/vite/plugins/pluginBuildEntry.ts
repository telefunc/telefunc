export { pluginBuildEntry }

import { serverProductionEntryPlugin } from '@brillout/vite-plugin-server-entry/plugin'
import type { Plugin } from 'vite'
import { projectInfo } from '../../../utils/projectInfo.js'
import { VIRTUAL_FILE_ENTRY_ID } from './pluginVirtualFileEntry/VIRTUAL_FILE_ENTRY_ID.js'
import { config } from '../../server/serverConfig.js'
import { assertManifest, type Manifest } from '../../server/runTelefunc/loadTelefuncFilesUsingVite/assertManifest.js'
import { getMagicString } from '../../shared/getMagicString.js'
import { escapeRegex } from '../../../utils/escapeRegex.js'
import { readFileSync } from 'node:fs'

const loaderFileSuffix = '/dist/node/server/runTelefunc/loadTelefuncFilesUsingVite.js'

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
        // Telefunc's loader, not the app's entry: only an environment that uses Telefunc imports the telefunctions
        filter: { id: new RegExp(`${escapeRegex(loaderFileSuffix)}$`) },
        handler(code, id) {
          // Any installed Telefunc's loader, whatever folder it's in (the plugin may come from another Telefunc copy)
          const packageJson = JSON.parse(readFileSync(`${id.slice(0, -loaderFileSuffix.length)}/package.json`, 'utf-8'))
          if (packageJson.name !== 'telefunc') return
          const { magicString, getMagicStringResult } = getMagicString(code, id)
          // Only Telefunc's part: other parts of the server entry (e.g. Vike's) load the app's server
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
