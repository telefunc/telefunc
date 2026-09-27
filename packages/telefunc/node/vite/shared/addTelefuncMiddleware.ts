export { addTelefuncMiddleware }

import { serveNode } from '../../server/telefunc.js'
import type { ViteDevServer } from 'vite'
import { getRequestPathname } from '../../../utils/getUrlPathname.js'

type ConnectServer = ViteDevServer['middlewares']
function addTelefuncMiddleware(middlewares: ConnectServer) {
  middlewares.use(async (req, res, next) => {
    if (res.headersSent) return next()

    const url = req.originalUrl || req.url
    if (!url) return next()

    if (getRequestPathname(url) !== '/_telefunc') return next()

    const httpResponse = await serveNode(
      {
        readable: req,
        url,
        method: req.method || 'GET',
        headers: req.headers,
      },
      res,
    )
    httpResponse.headers.forEach(([name, value]) => res.setHeader(name, value))
    res.statusCode = httpResponse.statusCode
    res.socket?.setNoDelay(true)
    res.flushHeaders()
    await httpResponse.pipe(res)
  })
}
