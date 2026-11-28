import type { Server } from 'vike/types'
import express from 'express'
import cookieParser from 'cookie-parser'
import vike, { createMiddleware, toFetchHandler } from '@vikejs/express'
import { serve } from 'telefunc'
import { retrieveUser } from '#app/auth'

const app = express()

app.use(cookieParser())
// Set pageContext.user
app.use(
  createMiddleware(() => (_request, context, runtime) => {
    // @ts-ignore
    const { req } = runtime
    return { ...context, user: retrieveUser(req) }
  })(),
)
vike(app)

app.all('/_telefunc', async (req, res) => {
  const context = { user: retrieveUser(req) }
  const httpResponse = await serve({
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

export default {
  fetch: toFetchHandler(app),
  prod: {
    port: Number(process.env.PORT) || 3000,
  },
} satisfies Server
