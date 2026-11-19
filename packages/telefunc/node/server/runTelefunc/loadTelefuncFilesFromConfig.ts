export { loadTelefuncFilesFromConfig }

import { assert, assertUsage } from '../../../utils/assert.js'
import { isTelefuncFilePath } from '../../../utils/isTelefuncFilePath.js'
import { assertPosixPath } from '../../../utils/path.js'
import type { TelefuncFiles } from '../types.js'
import { import_ } from '@brillout/import'
import pc from '@brillout/picocolors'
import { getServerConfig } from '../serverConfig.js'

async function loadTelefuncFilesFromConfig(runContext: {
  telefuncFilePath: string
}): Promise<{ telefuncFilesLoaded: TelefuncFiles; telefuncFilesAll: string[] }> {
  const { root, telefuncFiles } = getServerConfig()
  assertUsage(
    root,
    `You need to set ${pc.cyan('config.root')} to be able to use ${pc.cyan(
      'config.telefuncFiles',
    )}, see https://telefunc.com/root`,
  )
  assert(telefuncFiles)
  assertPosixPath(root)
  const telefuncFilesLoaded: TelefuncFiles = {}
  const telefuncFilesAll: string[] = []
  await Promise.all(
    telefuncFiles.map(async (telefuncFilePathAbsolute) => {
      const telefuncFilePath = resolveTelefuncFilePath(telefuncFilePathAbsolute, root)
      telefuncFilesAll.push(telefuncFilePath)
      if (!runContext.telefuncFilePath || !isTelefuncFilePath(runContext.telefuncFilePath)) {
        return
      }
      assert(isTelefuncFilePath(telefuncFilePath))
      if (telefuncFilePath !== runContext.telefuncFilePath) {
        return
      }
      const telefunctions: any = await import_(telefuncFilePathAbsolute)
      telefuncFilesLoaded[telefuncFilePath] = telefunctions
    }),
  )
  return { telefuncFilesLoaded, telefuncFilesAll }
}

function resolveTelefuncFilePath(telefuncFilePathAbsolute: string, appRootDir: string): string {
  assertPosixPath(telefuncFilePathAbsolute)
  assertUsage(
    telefuncFilePathAbsolute.startsWith(appRootDir),
    `The telefunc file ${telefuncFilePathAbsolute} doesn't live inside the root directory ${appRootDir} of your project. Either move the telefunc file inside the root directory, or change ${pc.cyan(
      'config.root',
    )} (https://telefunc.com/root).`,
  )
  let path = telefuncFilePathAbsolute.slice(appRootDir.length)
  if (path.startsWith('/')) path = path.slice(1)
  assert(!path.startsWith('/') && !path.startsWith('.'))
  assertPosixPath(path)
  const telefuncFilePath = '/' + path
  assert(isTelefuncFilePath(telefuncFilePathAbsolute))
  return telefuncFilePath
}
