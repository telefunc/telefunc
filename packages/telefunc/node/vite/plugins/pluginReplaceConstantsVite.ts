export { pluginReplaceConstantsVite }

import { getMagicString } from '../../shared/getMagicString.js'
import type { Plugin } from 'vite'

const IS_VITE = '__TELEFUNC__IS_VITE'
const DYNAMIC_IMPORT = '__TELEFUNC__DYNAMIC_IMPORT'

declare global {
  var __TELEFUNC__IS_VITE: undefined | true
}

function pluginReplaceConstantsVite(): Plugin[] {
  return [
    {
      name: 'telefunc:pluginReplaceConstantsVite:IS_VITE',
      transform: {
        filter: {
          code: {
            include: IS_VITE,
          },
        },
        handler(code, id) {
          const { magicString, getMagicStringResult } = getMagicString(code, id)
          magicString.replaceAll(IS_VITE, JSON.stringify(true))
          return getMagicStringResult()
        },
      },
    },
    {
      name: 'telefunc:pluginReplaceConstantsVite:DYNAMIC_IMPORT',
      transform: {
        filter: {
          code: {
            include: DYNAMIC_IMPORT,
          },
        },
        handler(code, id) {
          const { magicString, getMagicStringResult } = getMagicString(code, id)
          magicString.replaceAll(DYNAMIC_IMPORT, 'import')
          return getMagicStringResult()
        },
      },
    },
  ]
}
