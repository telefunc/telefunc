export { pluginPreview }

import type { Plugin } from 'vite'
import { apply } from '../shared/apply.js'
import { addTelefuncMiddleware } from '../shared/addTelefuncMiddleware.js'

function pluginPreview(): Plugin[] {
  return [
    {
      name: 'telefunc:pluginPreview',
      apply: apply('preview'),
      // Ensure that SvelteKit's configurePreviewServer() has precedence, see https://github.com/telefunc/telefunc/pull/54
      enforce: 'post',
      configurePreviewServer: {
        handler(server) {
          return () => {
            ;(process.env as Record<string, string>).NODE_ENV = 'production'
            addTelefuncMiddleware(server.middlewares)
          }
        },
      },
    },
  ]
}
