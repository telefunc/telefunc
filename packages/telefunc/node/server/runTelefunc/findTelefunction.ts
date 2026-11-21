export { findTelefunction }

import { assert, assertUsage, errorPrefix as projectErrorPrefix } from '../../../utils/assert.js'
import type { TelefuncFiles, Telefunction } from '../types.js'
import { assertNamingConvention } from './assertNamingConvention.js'
import { assertTelefunction } from './assertTelefunction.js'
import { getServerConfig } from '../serverConfig.js'
import pc from '@brillout/picocolors'

async function findTelefunction(runContext: {
  telefuncFilePath: string
  telefuncFilesLoaded: TelefuncFiles
  telefuncFilesAll: string[]
  telefunctionName: string
  logMalformedRequests: boolean
  appRootDir: null | string
  serverConfig: {
    disableNamingConvention: boolean
  }
}): Promise<null | Telefunction> {
  assertUsage(
    runContext.telefuncFilesAll.length > 0,
    [
      `Telefunction ${runContext.telefunctionName}() (${runContext.telefuncFilePath}) not found.`,
      "Your app doesn't seem to have any `.telefunc.{js|ts|...}` file, nor any telefunctions registered with `defineTelefunctions()`.",
    ].join(' '),
  )

  const telefuncFile = findTelefuncFile(runContext)
  const telefunction = await (async () => {
    if (!telefuncFile) {
      return null
    }
    if (!(runContext.telefunctionName in telefuncFile.fileExports)) {
      return null
    }
    const telefunction = telefuncFile.fileExports[runContext.telefunctionName]
    assertTelefunction(telefunction, runContext.telefunctionName, telefuncFile.filePath)
    if (!runContext.serverConfig.disableNamingConvention && telefuncFile.filePath !== '') {
      await assertNamingConvention(
        telefunction,
        runContext.telefunctionName,
        telefuncFile.filePath,
        runContext.appRootDir,
      )
    }
    return telefunction
  })()

  if (!telefunction) {
    if (runContext.logMalformedRequests) {
      const errMsg = getNotFoundErrMsg()
      console.error(`${projectErrorPrefix} ${errMsg}`)
    }
    return null
  }

  return telefunction

  function getNotFoundErrMsg() {
    let errMsg = `Telefunction ${runContext.telefunctionName}() (${runContext.telefuncFilePath}) not found: `
    if (!telefuncFile) {
      let extraMsg: string | null = null
      const serverConfig = getServerConfig()
      if (serverConfig.telefuncFiles) {
        assert(serverConfig.telefuncFiles.length > 0)
        assert(serverConfig.root)
        extraMsg = `Did you set ${pc.cyan('config.root')} to the *client-side* root (see https://telefunc.com/root)?`
      }
      errMsg += [
        `the file ${runContext.telefuncFilePath} doesn't exist.`,
        // Hint about config.root
        extraMsg,
        'Found `.telefunc.js` files:',
      ]
        .filter(Boolean)
        .join(' ')
      assert(!runContext.telefuncFilesAll.includes(runContext.telefuncFilePath))
      errMsg += [runContext.telefuncFilePath, ...runContext.telefuncFilesAll]
        .sort()
        .map(
          (filePath) =>
            `\n  ${filePath} ${runContext.telefuncFilesAll.includes(filePath) ? '[✅ Exists]' : "[❌ Doesn't exist]"}`,
        )
        .join('')
    } else {
      assert(!telefuncFile.fileExports[runContext.telefunctionName])
      assert(telefuncFile.filePath === runContext.telefuncFilePath)
      if (telefuncFile.filePath === '') {
        errMsg += `the telefunction "${runContext.telefunctionName}" was not registered with defineTelefunctions(). `
      } else {
        errMsg += `the file ${telefuncFile.filePath} doesn't export a telefunction named "${runContext.telefunctionName}". `
      }
      const telefuncFileExportNames = Object.keys(telefuncFile.fileExports)
      if (telefuncFileExportNames.length === 0) {
        errMsg += `(The file ${telefuncFile.filePath} doesn't export any telefunction.)`
      } else {
        errMsg += 'Found telefunctions:'
        assert(!telefuncFileExportNames.includes(runContext.telefunctionName))
        errMsg += [runContext.telefunctionName, ...telefuncFileExportNames]
          .sort()
          .map(
            (exportName) =>
              `\n  ${telefuncFile.filePath} ${
                telefuncFileExportNames.includes(exportName)
                  ? `exports telefunction ${exportName}() ✅`
                  : `doesn't have an export "${exportName}" ❌`
              }`,
          )
          .join('')
      }
    }
    return errMsg
  }
}

function findTelefuncFile(runContext: {
  telefuncFilesLoaded: TelefuncFiles
  telefuncFilePath: string
  telefuncFilesAll: string[]
}) {
  const found = Object.entries(runContext.telefuncFilesLoaded).filter(([telefuncFilePath]) => {
    assert(runContext.telefuncFilesAll.includes(telefuncFilePath))
    return telefuncFilePath === runContext.telefuncFilePath
  })
  if (found.length === 0) {
    assert(!runContext.telefuncFilesAll.includes(runContext.telefuncFilePath))
    return null
  }
  assert(found.length === 1)
  const [filePath, fileExports] = found[0]!
  const telefuncFileFound = { filePath, fileExports }
  return telefuncFileFound
}
