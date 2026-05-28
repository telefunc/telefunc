export { pluginTransformTelefuncFiles }

import type { Plugin } from 'vite'
import { transformTelefuncFileClientSide } from '../../shared/transformer/transformTelefuncFileClientSide.js'
import { transformTelefuncFileServerSide } from '../../shared/transformer/transformTelefuncFileServerSide.js'
import { assert } from '../../../utils/assert.js'
import { toPosixPath } from '../../../utils/path.js'

function pluginTransformTelefuncFiles(): Plugin[] {
  let root: string
  let isDev: boolean = false
  return [
    {
      name: 'telefunc:pluginTransformTelefuncFiles',
      enforce: 'pre', // TODO: also use `order: 'pre'`
      configResolved: {
        handler(config) {
          root = toPosixPath(config.root)
          assert(root)
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
            return await transformTelefuncFileClientSide(code, id, root)
          } else {
            return await transformTelefuncFileServerSide(code, id, root, isDev)
          }
        },
      },
    },
  ]
}
