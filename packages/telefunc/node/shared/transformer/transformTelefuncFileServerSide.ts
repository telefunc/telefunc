export { transformTelefuncFileServerSide }

import { ExportList, getExportList } from './getExportList.js'
import { isNotNullish } from '../../../utils/isNullish.js'
import { assertPosixPath } from '../../../utils/path.js'
import { generateShield } from './generateShield/generateShield.js'
import { getServerConfig } from '../../server/serverConfig.js'
import { getMagicString } from '../../shared/getMagicString.js'

async function transformTelefuncFileServerSide(
  src: string,
  id: string,
  appRootDir: string,
  isDev: boolean,
  extensionImports: string[],
) {
  assertPosixPath(id)
  assertPosixPath(appRootDir)

  // Prepend extension imports to the source so they're visible to:
  //   1. shield generation (ts-morph follows them to load global namespace augmentations)
  //   2. the magicString output (so the runtime extension registers at module load)
  const srcWithImports = [...extensionImports, src].join('\n')

  const exportList = await getExportList(srcWithImports)
  const codeDecoration = decorateTelefunctions(exportList, id.replace(appRootDir, ''), appRootDir)

  let codeShield: string | undefined
  const config = getServerConfig()
  const isShieldEnabled = isDev ? config.shield.dev : config.shield.prod
  if (id.endsWith('.ts') && isShieldEnabled) {
    codeShield = generateShield(srcWithImports, id, appRootDir, exportList)
  }

  const { magicString, getMagicStringResult } = getMagicString(srcWithImports, id)

  // We append everything in order to avoid breaking source map lines for environments that don't support source maps
  magicString.append(['', codeDecoration, codeShield].filter(isNotNullish).join('\n\n'))

  return getMagicStringResult()
}

function decorateTelefunctions(exportList: ExportList, filePath: string, appRootDir: string) {
  assertPosixPath(filePath)

  return [
    'import { __decorateTelefunction } from "telefunc";',
    ...exportList.map(
      ({ exportName, localName }) =>
        // TODO: change to `fn = __decorateTelefunction(fn, ...)` so the returned wrapper sets up
        // context — enables server-side context aware telefunction calls (currently no context for direct calls).
        `__decorateTelefunction(${localName || exportName}, "${exportName}", "${filePath}", "${appRootDir}");`,
    ),
  ].join('\n')
}
