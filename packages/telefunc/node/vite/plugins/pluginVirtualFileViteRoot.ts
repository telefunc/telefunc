export { pluginVirtualFileViteRoot }

import type { Plugin } from 'vite'
import { assert } from '../../../utils/assert.js'
import { escapeRegex } from '../../../utils/escapeRegex.js'
import { toPosixPath } from '../../../utils/path.js'
import { VIRTUAL_FILE_VITE_ROOT_ID } from './pluginVirtualFileViteRoot/VIRTUAL_FILE_VITE_ROOT_ID.js'

const resolvedId = '\0' + VIRTUAL_FILE_VITE_ROOT_ID

function pluginVirtualFileViteRoot(): Plugin[] {
  let root: string | null = null
  return [
    {
      name: 'telefunc:pluginVirtualFileViteRoot',
      configResolved: {
        handler(config) {
          root = toPosixPath(config.root)
        },
      },
      resolveId: {
        filter: {
          id: new RegExp(`^${escapeRegex(VIRTUAL_FILE_VITE_ROOT_ID)}$`),
        },
        handler(id) {
          assert(id === VIRTUAL_FILE_VITE_ROOT_ID)
          return resolvedId
        },
      },
      load: {
        filter: {
          id: new RegExp(`^${escapeRegex(resolvedId)}$`),
        },
        handler(id) {
          if (id !== resolvedId) return undefined
          assert(root !== null)
          return `export const rootFromVite = ${JSON.stringify(root)};\n`
        },
      },
    },
  ]
}
