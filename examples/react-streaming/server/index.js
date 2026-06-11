const express = require('express')
const { renderPage, createDevMiddleware } = require('vike/server')
const { telefunc } = require('telefunc')

const isProduction = process.env.NODE_ENV === 'production'
const root = `${__dirname}/..`

startServer()

async function startServer() {
  const app = express()

  if (isProduction) {
    app.use(express.static(`${root}/dist/client`))
  } else {
    const { devMiddleware } = await createDevMiddleware({ root })
    app.use(devMiddleware)
  }

  app.all('/_telefunc', async (req, res) => {
    const context = {}
    const httpResponse = await telefunc({
      url: req.originalUrl,
      method: req.method,
      readable: req,
      headers: req.headers,
      context,
    })
    const { body, statusCode, headers } = httpResponse
    res.status(statusCode)
    headers.forEach(([name, value]) => res.setHeader(name, value))
    res.send(body)
  })

  app.get('*', async (req, res, next) => {
    const pageContextInit = {
      urlOriginal: req.originalUrl,
    }
    const pageContext = await renderPage(pageContextInit)
    const { httpResponse } = pageContext
    if (!httpResponse) return next()
    const { statusCode, headers } = httpResponse
    res.status(statusCode)
    headers.forEach(([name, value]) => res.setHeader(name, value))
    httpResponse.pipe(res)
  })

  const port = process.env.PORT || 3000
  app.listen(port)
  console.log(`Server running at http://localhost:${port}`)
}
