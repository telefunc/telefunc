export { loadTelefuncFilesWithImportBuild }
export { setTelefuncLoaders }

import { applyManifestConfig } from '../../serverConfig.js'
import { getGlobalObject } from '../../../../utils/getGlobalObject.js'
import { assertManifest } from './assertManifest.js'

const globalObject = getGlobalObject<{
  loadTelefuncFiles?: LoadTelefuncFiles
}>('loadBuildEntry.ts', {})

type LoadTelefuncFiles = () => Promise<unknown>
type LoadManifest = () => Record<string, unknown>

function setTelefuncLoaders({
  loadTelefuncFiles,
  loadManifest,
}: {
  loadTelefuncFiles: LoadTelefuncFiles
  loadManifest: LoadManifest
}) {
  globalObject.loadTelefuncFiles = loadTelefuncFiles
  setServerConfig(loadManifest)
}

function setServerConfig(loadManifest: LoadManifest) {
  const manifest = loadManifest()
  assertManifest(manifest)
  applyManifestConfig(manifest.config)
}

async function loadTelefuncFilesWithImportBuild(): Promise<unknown> {
  if (!globalObject.loadTelefuncFiles) {
    return null
  }
  const moduleExports = await globalObject.loadTelefuncFiles()
  return moduleExports
}
