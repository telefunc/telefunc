// Mechanism used by Vite/Next/Nuxt plugins for automatically loading `.telefunc.js` files.

export { loadTelefuncFilesUsingRegistration }
export { registerTelefunction }
export { clearRegistration }

import { TelefuncFiles, Telefunction } from '../types.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const g = getGlobalObject<{ telefuncFilesLoaded: null | TelefuncFiles }>('loadTelefuncFilesUsingRegistration.ts', {
  telefuncFilesLoaded: null,
})

function loadTelefuncFilesUsingRegistration(): null | TelefuncFiles {
  return g.telefuncFilesLoaded
}

function registerTelefunction(telefunction: Telefunction, exportName: string, telefuncFilePath: string) {
  g.telefuncFilesLoaded = g.telefuncFilesLoaded ?? {}
  g.telefuncFilesLoaded[telefuncFilePath] = {
    ...g.telefuncFilesLoaded[telefuncFilePath],
    [exportName]: telefunction,
  }
}

function clearRegistration() {
  g.telefuncFilesLoaded = null
}
