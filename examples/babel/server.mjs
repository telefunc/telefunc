import express from 'express'
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
  installTelefunc(app)
  await installFrontend(app)
  start(app)
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

async function installFrontend(app) {
  const root = await getRoot()
  app.use(express.static(`${root}/dist`))
}

function start(app) {
  const port = process.env.PORT || 3000
  app.listen(port)
  console.log(`Server running at http://localhost:${port}`)
}

// https://stackoverflow.com/questions/46745014/alternative-for-dirname-in-node-js-when-using-es6-modules
async function getRoot() {
  const { dirname } = await import('node:path')
  const { fileURLToPath } = await import('node:url')
  const __dirname = dirname(fileURLToPath(import.meta.url))
  const root = __dirname
  return root
}
