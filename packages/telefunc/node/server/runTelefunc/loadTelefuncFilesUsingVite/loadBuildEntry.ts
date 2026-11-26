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
  // A build that fails the manifest's check must leave no loader: each later load then imports the build again and fails
  setServerConfig(loadManifest)
  globalObject.loadTelefuncFiles = loadTelefuncFiles
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
