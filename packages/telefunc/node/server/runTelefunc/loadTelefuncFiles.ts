export { loadTelefuncFiles }

import type { TelefuncFiles } from '../types.js'
import { assertUsage, assert } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { isVikeApp } from '../../../utils/isVikeApp.js'
import { isWebpack } from '../../../utils/isWebpack.js'
import { loadTelefuncFilesUsingVite } from './loadTelefuncFilesUsingVite.js'
import { loadTelefuncFilesUsingRegistration } from './loadTelefuncFilesUsingRegistration.js'
import { loadTelefuncFilesFromConfig } from './loadTelefuncFilesFromConfig.js'
import pc from '@brillout/picocolors'

async function loadTelefuncFiles(runContext: {
  telefuncFilesManuallyProvidedByUser: string[] | null
  telefuncFilePath: string
}): Promise<{ telefuncFilesLoaded: TelefuncFiles; telefuncFilesAll: string[] }> {
  // - defineTelefunctions()
  // - Next.js
  // - Nuxt 2
  // - Vite in production if @brillout/vite-plugin-server-entry fails (see comment below)
  {
    const telefuncFilesLoaded = loadTelefuncFilesUsingRegistration()
    if (telefuncFilesLoaded) {
      const telefuncFilesAll = Object.keys(telefuncFilesLoaded)
      assertUsage(Object.keys(telefuncFilesAll).length > 0, getNothingFoundErr('registration'))
      return { telefuncFilesLoaded, telefuncFilesAll }
    }
  }

  // - The user can manually provide the list of `.telefunc.js` files using `config.telefuncFiles`
  {
    if (runContext.telefuncFilesManuallyProvidedByUser) {
      assert(hasProp(runContext, 'telefuncFilesManuallyProvidedByUser', 'string[]')) // Help TS narrow `runContext`
      const { telefuncFilesLoaded, telefuncFilesAll } = await loadTelefuncFilesFromConfig(runContext)
      assertUsage(Object.keys(telefuncFilesAll).length > 0, getNothingFoundErr('manually provided by user'))
      return { telefuncFilesLoaded, telefuncFilesAll }
    }
  }

  // Vite:
  // - In development, `.telefunc.js` files are provided using Vite's development server
  // - In production, `.telefunc.js` files are provided using @brillout/vite-plugin-server-entry
  //   - It's guaranteed to work when using Vike with vike-server (as `$ node dist/server/index.js` always includes Telefunc's entry)
  //   - It may fail with the following setups as described at https://github.com/brillout/vite-plugin-server-entry#manual-import => we fallback down below with the registration method
  //     - With Vike + fully custom server integration
  //     - SvelteKit
  {
    const res = await loadTelefuncFilesUsingVite(runContext, false)
    if (res) {
      const { telefuncFilesLoaded, viteProvider, telefuncFilesAll } = res
      assertUsage(Object.keys(telefuncFilesAll).length > 0, getNothingFoundErr(viteProvider))
      return { telefuncFilesLoaded, telefuncFilesAll }
    }
  }

  if (isVikeApp()) {
    // Show [manual import error](https://github.com/brillout/vite-plugin-server-entry#manual-import):
    // ```
    // [@brillout/vite-plugin-server-entry][Wrong Usage] The server production entry is missing.
    // (Re-)build your app and try again. If you still get this error, then you need to manually
    // import the server production entry.
    // ```
    //
    const res2 = await loadTelefuncFilesUsingVite(runContext, true)
    assert(res2 === null)
    assert(false) // loadTelefuncFilesUsingVite() should have thrown the assertUsage() error above
  } else {
    // Generic message
    assertUsage(
      false,
      `No telefunction registered. Did you define telefunctions with ${pc.cyan(
        'defineTelefunctions()',
      )} or create ${pc.cyan('.telefunc.{js|ts|...}')} files?`,
    )
  }
}

function getNothingFoundErr(retrievalMethod: string) {
  return `No ${pc.code('.telefunc.{js|ts|...}')} file found. Did you create one? (Retrieval method: ${retrievalMethod}.)`
}
