export { addTelefuncMiddleware }

import { telefunc } from '../../server/index.js'
import { nodeReadableToWebRequest } from '../../../utils/nodeReadableToWebRequest.js'
import type { ViteDevServer } from 'vite'

type ConnectServer = ViteDevServer['middlewares']
function addTelefuncMiddleware(middlewares: ConnectServer) {
  middlewares.use(async (req, res, next) => {
    if (res.headersSent) return next()

    const url = req.originalUrl || req.url
    if (!url) return next()

    if (url !== '/_telefunc') return next()

    const request = nodeReadableToWebRequest(req, 'http://localhost/_telefunc', req.method!, req.headers)

    const httpResponse = await telefunc({ request })
    httpResponse.headers.forEach(([name, value]) => res.setHeader(name, value))
    res.statusCode = httpResponse.statusCode
    httpResponse.pipe(res)
  })
}
