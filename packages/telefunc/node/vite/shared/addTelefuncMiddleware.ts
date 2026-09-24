export { addTelefuncMiddleware }

import { serve } from '../../server/index.js'
import type { ViteDevServer } from 'vite'

type ConnectServer = ViteDevServer['middlewares']
function addTelefuncMiddleware(middlewares: ConnectServer) {
  middlewares.use(async (req, res, next) => {
    if (res.headersSent) return next()

    const url = req.originalUrl || req.url
    if (!url) return next()

    if (url !== '/_telefunc') return next()

    const httpResponse = await serve({
      readable: req,
      url,
      method: req.method || 'GET',
      headers: req.headers,
    })
    httpResponse.headers.forEach(([name, value]) => res.setHeader(name, value))
    res.statusCode = httpResponse.statusCode
    res.socket?.setNoDelay(true)
    res.flushHeaders()
    await httpResponse.pipe(res)
  })
}
