import { afterAll, beforeAll, expect, onTestFinished, test, vi } from 'vitest'
import { createServer, type IncomingMessage, type Server } from 'node:http'
import { connect } from 'node:net'
import { once } from 'node:events'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'

// Development, where Telefunc also prints a malformed request: set before Telefunc first reads it, which it keeps.
vi.hoisted(() => vi.stubEnv('NODE_ENV', 'development'))
const app = vi.hoisted(() => ({
  file: '/requestBody.telefunc.ts',
  telefunctions: {
    hello: async () => 'hello',
    // Its own outgoing request fails: the app's error, whatever its code.
    callsAnotherServer: async () => {
      throw Object.assign(new Error('the other server reset the connection'), { code: 'ECONNRESET' })
    },
  } as Record<string, () => Promise<unknown>>,
}))
vi.mock('../node/server/runTelefunc/loadTelefuncFiles.js', () => ({
  loadTelefuncFiles: async () => ({
    telefuncFilesLoaded: { [app.file]: app.telefunctions },
    telefuncFilesAll: [app.file],
  }),
}))
config.disableNamingConvention = true

let server: Server
let port: number
/** What each `serve()` settled to, in the order the requests arrived. */
const served: Promise<boolean>[] = []

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => {
    served.push(telefunc.serve({ req, res }))
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  port = address.port
})
afterAll(() => {
  server.close()
  vi.unstubAllEnvs()
})

/** What the server prints as an error while the test runs, and the process's unhandled rejections. */
function recordErrors(): { printed: string[]; unhandled: unknown[] } {
  const printed: string[] = []
  const unhandled: unknown[] = []
  const error = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    printed.push(String(args[0]))
  })
  const record = (err: unknown) => unhandled.push(err)
  process.on('unhandledRejection', record)
  onTestFinished(() => {
    error.mockRestore()
    process.off('unhandledRejection', record)
  })
  return { printed, unhandled }
}

/** A text call to `name`, its answer's status and body. */
async function textCall(name: string): Promise<{ status: number; body: string }> {
  const response = await fetch(`http://127.0.0.1:${port}/_telefunc`, {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify({ file: app.file, name, args: [] }),
  })
  return { status: response.status, body: await response.text() }
}

test('a text call whose client leaves in the middle of its body prints nothing, and serve() resolves', async () => {
  const { printed, unhandled } = recordErrors()
  const socket = connect(port, '127.0.0.1')
  await once(socket, 'connect')
  socket.write(
    `POST /_telefunc HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: txt\r\nContent-Type: text/plain\r\nContent-Length: 1000\r\n\r\n{"file":`,
  )
  const [req] = (await once(server, 'request')) as [IncomingMessage]
  const serving = served.at(-1)
  // Its request ends with Node's `aborted` error, which `once()` would throw.
  const closed = new Promise((resolve) => req.once('close', resolve))
  socket.destroy()
  await closed
  await expect(serving).resolves.toBe(true)
  expect(printed).toEqual([])
  expect(unhandled).toEqual([])
})

test('a telefunction that throws an error with the code of a reset connection is still printed', async () => {
  const { printed } = recordErrors()
  expect((await textCall('callsAnotherServer')).status).toBe(500)
  expect(printed).toEqual([expect.stringContaining('the other server reset the connection')])
})

test('a text call with its whole body is answered', async () => {
  const { printed } = recordErrors()
  const { status, body } = await textCall('hello')
  expect(status).toBe(200)
  expect(body).toContain('hello')
  expect(printed).toEqual([])
})
