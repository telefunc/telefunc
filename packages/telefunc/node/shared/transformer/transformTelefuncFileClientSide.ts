export { transformTelefuncFileClientSide }

import { getExportList } from './getExportList.js'
import { transformTelefuncFileClientSideSync } from './transformTelefuncFileClientSideSync.js'

async function transformTelefuncFileClientSide(
  src: string,
  id: string,
  appRootDir: string,
  extensionImports: string[],
) {
  const exportList = await getExportList(src)
  const exportNames = exportList.map((e) => e.exportName)
  return transformTelefuncFileClientSideSync(id, appRootDir, exportNames, extensionImports)
}
