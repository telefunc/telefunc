import { afterAll, beforeAll, expect, test } from 'vitest'
import http from 'node:http'
import { addTelefuncMiddleware } from './addTelefuncMiddleware.js'
import { makeHttpRequest } from '../../../client/remoteTelefunctionCall/makeHttpRequest.js'
import { setTelefuncLoaders } from '../../server/runTelefunc/loadTelefuncFilesUsingVite/loadBuildEntry.js'
import { projectInfo } from '../../../utils/projectInfo.js'

async function hello(name: string) {
  return `Hello ${name}`
}
setTelefuncLoaders({
  loadTelefuncFiles: async () => ({ telefuncFilesGlob: { '/hello.telefunc.ts': async () => ({ hello }) } }),
  loadManifest: () => ({ version: projectInfo.projectVersion, config: {} }),
})

type Middleware = (req: http.IncomingMessage, res: http.ServerResponse, next: () => void) => void
let middleware!: Middleware
addTelefuncMiddleware({ use: (fn: Middleware) => void (middleware = fn) } as never)

let server: http.Server
let origin: string
beforeAll(async () => {
  // Vite's own server: the middleware, then a 404
  server = http.createServer((req, res) =>
    middleware(req, res, () => {
      res.statusCode = 404
      res.end()
    }),
  )
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
})
afterAll(() => new Promise((resolve) => server.close(resolve)))

test("a call through Vite's dev and preview server reaches the telefunction", async () => {
  const result = await makeHttpRequest({
    telefuncUrl: `${origin}/_telefunc`,
    httpRequestBody: JSON.stringify({ file: '/hello.telefunc.ts', name: 'hello', args: ['Eva'] }),
    telefunctionName: 'hello',
    telefuncFilePath: '/hello.telefunc.ts',
    headers: null,
    fetch: globalThis.fetch,
    abortController: new AbortController(),
    channel: { transports: ['sse'] },
    requestCloseHandlers: [],
    extensionResponseTypes: [],
  })
  expect(result).toBe('Hello Eva')
})

test('a request whose path is not a URL, such as //, passes the middleware by', async () => {
  const response = await fetch(`${origin}//`)
  expect(response.status).toBe(404)
})
