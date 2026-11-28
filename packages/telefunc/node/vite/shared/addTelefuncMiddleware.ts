export { addTelefuncMiddleware }

import { serveNode } from '../../server/telefunc.js'
import type { ViteDevServer } from 'vite'

type ConnectServer = ViteDevServer['middlewares']
function addTelefuncMiddleware(middlewares: ConnectServer) {
  middlewares.use(async (req, res, next) => {
    if (!(await serveNode(req, res))) next()
  })
}
