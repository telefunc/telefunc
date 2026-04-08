export { pluginTransformTelefuncFiles }

import type { Plugin } from 'vite'
import { transformTelefuncFileClientSide } from '../../shared/transformer/transformTelefuncFileClientSide.js'
import { transformTelefuncFileServerSide } from '../../shared/transformer/transformTelefuncFileServerSide.js'
import { getExtensionImports } from '../../shared/discoverExtensions.js'
import { assert } from '../../../utils/assert.js'
import { toPosixPath } from '../../../utils/path.js'

function pluginTransformTelefuncFiles(): Plugin[] {
  let root: string
  let isDev: boolean = false
  let serverExtensionImports: string[]
  let clientExtensionImports: string[]
  return [
    {
      name: 'telefunc:pluginTransformTelefuncFiles',
      enforce: 'pre',
      configResolved: {
        handler(config) {
          root = toPosixPath(config.root)
          assert(root)
          serverExtensionImports = getExtensionImports(root, 'server')
          clientExtensionImports = getExtensionImports(root, 'client')
        },
      },
      configureServer: {
        handler() {
          isDev = true
        },
      },
      transform: {
        filter: {
          id: '**/*.telefunc.*',
        },
        async handler(code, id, options) {
          assert(id.includes('.telefunc'))
          const isClientSide = !options?.ssr
          if (isClientSide) {
            return await transformTelefuncFileClientSide(code, id, root, clientExtensionImports)
          } else {
            return await transformTelefuncFileServerSide(code, id, root, isDev, serverExtensionImports)
          }
        },
      },
    },
  ]
}
