export { loadTelefuncFilesUsingVite }

import { importServerProductionEntry } from '@brillout/vite-plugin-server-entry/runtime'
import { assert, assertWarning } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { isObject } from '../../../utils/isObject.js'
import { getNodeEnv, isProduction } from '../../../utils/isProduction.js'
import { isTelefuncFilePath } from '../../../utils/isTelefuncFilePath.js'
import { loadTelefuncFilesWithImportBuild } from './loadTelefuncFilesUsingVite/loadBuildEntry.js'
import { getViteDevServer } from '../globalContext.js'
import { VIRTUAL_FILE_ENTRY_ID } from '../../vite/plugins/pluginVirtualFileEntry/VIRTUAL_FILE_ENTRY_ID.js'

async function loadTelefuncFilesUsingVite(runContext: { telefuncFilePath: string }, failOnFailure: boolean) {
  const res = await loadVirtualFile(failOnFailure)
  if (!res) return null
  const { moduleExports, viteProvider } = res
  assert(isObject(moduleExports), { moduleExports, viteProvider })
  assert(hasProp(moduleExports, 'telefuncFilesGlob'), { moduleExports, viteProvider })
  const telefuncFilesGlob = moduleExports.telefuncFilesGlob as GlobFiles
  const { telefuncFilesLoaded, telefuncFilesAll } = await loadViteGlobbedFiles(telefuncFilesGlob, runContext)
  assert(isObjectOfObjects(telefuncFilesLoaded))
  return { telefuncFilesLoaded, viteProvider, telefuncFilesAll }
}

async function loadVirtualFile(failOnFailure: boolean) {
  if (globalThis.__TELEFUNC__IS_NON_RUNNABLE_DEV) {
    // We don't directly use import() because:
    // - Avoid Cloudflare Workers (without @cloudflare/vite-plugin) to try to bundle `import('virtual:id')`.
    // - Using import() seems to lead to a Vite HMR bug:
    //   ```js
    //   assert(false)
    //   // This line breaks the HMR of regular (runnable) apps, even though (as per the assert() above) it's never run. It seems to be a Vite bug: handleHotUpdate() receives an empty `modules` list.
    //   import('virtual:vike:global-entry:server')
    //   ```
    const moduleExports = await __TELEFUNC__DYNAMIC_IMPORT('virtual:vite:telefunc:entry')
    return { moduleExports, viteProvider: 'Vite with `import()`' as const }
  }
  const viteDevServer = getViteDevServer()
  if (viteDevServer) {
    const moduleExports = await viteDevServer.ssrLoadModule(VIRTUAL_FILE_ENTRY_ID, { fixStacktrace: true })
    return { moduleExports, viteProvider: 'Vite with `ssrLoadModule()`' as const }
  } else {
    // `true` only where Vite bundles Telefunc: neither Node nor another bundler sees this import
    if (globalThis.__TELEFUNC__IS_BUNDLED)
      await __TELEFUNC__DYNAMIC_IMPORT('virtual:vite:telefunc:server-production-entry')
    let moduleExports: unknown
    moduleExports = await loadTelefuncFilesWithImportBuild()
    if (moduleExports === null) {
      const tolerateDoesNotExist = !failOnFailure
      const success = await importServerProductionEntry({ tolerateDoesNotExist })
      moduleExports = await loadTelefuncFilesWithImportBuild()
      if (success === false) {
        assert(tolerateDoesNotExist)
        assert(!moduleExports)
        return null
      }
      assert(moduleExports)
    } else {
      assert(moduleExports)
    }
    assertProd()
    return { moduleExports, viteProvider: 'Vite with `@brillout/vite-plugin-server-entry`' as const }
  }
}

function assertProd() {
  if (!isProduction()) {
    const env = getNodeEnv()
    assert(env === undefined || env === 'development' || env === '')
    assertWarning(
      false,
      `This seems to be a production environment yet process.env.NODE_ENV is ${JSON.stringify(
        env,
      )}. Set it to a different value such as "production" or "staging".`,
      { onlyOnce: true },
    )
  }
}

function isObjectOfObjects(obj: unknown): obj is Record<string, Record<string, unknown>> {
  return isObject(obj) && Object.values(obj).every(isObject)
}

type GlobFiles = Record<FilePath, () => Promise<Record<ExportName, ExportValue>>>
type FilePath = string
type ExportName = string
type ExportValue = unknown

// Vite's import.meta.glob() returns promises
async function loadViteGlobbedFiles(telefuncFilesGlob: GlobFiles, runContext: { telefuncFilePath: string }) {
  const telefuncFilesAll = Object.keys(telefuncFilesGlob)
  const telefuncFilesLoaded = Object.fromEntries(
    await Promise.all(
      Object.entries(telefuncFilesGlob)
        .filter(([telefuncFilePath]) => {
          assert(isTelefuncFilePath(telefuncFilePath))
          assert(isTelefuncFilePath(runContext.telefuncFilePath))
          return telefuncFilePath === runContext.telefuncFilePath
        })
        .map(async ([telefuncFilePath, loadModuleExports]) => [telefuncFilePath, await loadModuleExports()]),
    ),
  )
  assert(Object.keys(telefuncFilesLoaded).length <= 1)
  return { telefuncFilesAll, telefuncFilesLoaded }
}
