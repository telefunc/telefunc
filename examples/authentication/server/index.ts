import express from 'express'
import { telefunc } from 'telefunc'
import cookieParser from 'cookie-parser'
import { retrieveUser } from '#app/auth'
import vike from 'vike-node/express'
import { createMiddleware } from '@universal-middleware/express'

startServer()

async function startServer() {
  const app = express()

  app.use(cookieParser())
  app.all('/_telefunc', async (req, res) => {
    const context = { user: retrieveUser(req) }
    const httpResponse = await telefunc({
      url: req.originalUrl,
      method: req.method,
      readable: req,
      headers: req.headers,
      context,
    })
    const { body, statusCode, headers } = httpResponse
    for (const [key, value] of headers) {
      res.setHeader(key, value)
    }
    res.status(statusCode).send(body)
  })

  // Set pageContext.user
  // https://github.com/vikejs/vike-node#custom-pagecontext
  app.use(
    createMiddleware(() => (_request, ctx, runtime) => {
      // @ts-ignore
      const { req } = runtime
      const user = retrieveUser(req)
      return {
        ...ctx,
        user,
      }
    })(),
  )
  app.use(vike())

  const port = process.env.PORT || 3000
  app.listen(port)
  console.log(`Server running at http://localhost:${port}`)
}
