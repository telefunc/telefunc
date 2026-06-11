import express from 'express'
import cors from 'cors'
import { telefunc, config } from 'telefunc'
import { createRequire } from 'node:module'

startServer()

config.disableNamingConvention = true
{
  const require = createRequire(import.meta.url)
  config.telefuncFiles = [require.resolve('./hello.telefunc.mjs')]
}

async function startServer() {
  const app = express()
  enableCors(app)
  installTelefunc(app)
  start(app)
}

function start(app) {
  const port = process.env.PORT || 3000
  app.listen(port)
  console.log(`Server running at http://localhost:${port}`)
}

function installTelefunc(app) {
  app.all('/_telefunc', async (req, res) => {
    const httpResponse = await telefunc({
      url: req.originalUrl,
      method: req.method,
      readable: req,
      headers: req.headers,
    })
    for (const [key, value] of httpResponse.headers) {
      res.setHeader(key, value)
    }
    res.status(httpResponse.statusCode).send(httpResponse.body)
  })
}

function enableCors(app) {
  app.use(cors()) // Enable cross-origin requests
}
