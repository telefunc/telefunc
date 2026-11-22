// Which request targets are Telefunc's: the ones `serve({ req, res })` answers are the ones a server that
// `installWebSocket()` took marks for `serve({ request })`, as the client names them on the wire.

import { afterAll, afterEach, beforeAll, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import { connect } from 'node:net'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'

let server: Server
let port: number
type Seen = { marked: boolean; answered: boolean }
/** What the app saw of the request to each target: whether it was marked and `serve({ req, res })` answered it. */
const seeing = new Map<string, (seen: Seen) => void>()

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer(async (req, res) => {
    const marked = req.headers['x-telefunc-server'] !== undefined
    const answered = await telefunc.serve({ req, res })
    seeing.get(req.url ?? '')?.({ marked, answered })
    if (!answered) res.writeHead(404).end()
  })
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  port = address.port
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})
afterEach(() => {
  config.telefuncUrl = '/_telefunc'
})

/** Sends a POST to `target` as it is, which `fetch()` would resolve first, and reads what the server made of it. */
async function send(target: string): Promise<Seen> {
  const seen = new Promise<Seen>((resolve) => seeing.set(target, resolve))
  const socket = connect(port, '127.0.0.1')
  socket.end(`POST ${target} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope`)
  socket.resume()
  await new Promise((resolve) => socket.once('close', resolve))
  seeing.delete(target)
  return seen
}

async function sendAll(targets: string[]): Promise<Record<string, Seen>> {
  const out: Record<string, Seen> = {}
  for (const target of targets) out[target] = await send(target)
  return out
}

const both = { marked: true, answered: true }
const neither = { marked: false, answered: false }

test("a server marks a request to Telefunc's URL for `serve({ request })` where `serve({ req, res })` answers it", async () => {
  expect(
    await sendAll([
      '/_telefunc',
      '/_telefunc?client=1',
      '/_telefunc#top',
      '//example.test/_telefunc',
      'http://example.test/_telefunc',
      '/./_telefunc',
      '/other/../_telefunc',
      '/_telefunc/',
      '/_telefuncs',
      '/%5Ftelefunc',
      '/other',
    ]),
  ).toEqual({
    '/_telefunc': both,
    '/_telefunc?client=1': both,
    '/_telefunc#top': both,
    '//example.test/_telefunc': both,
    'http://example.test/_telefunc': both,
    '/./_telefunc': both,
    '/other/../_telefunc': both,
    '/_telefunc/': neither,
    '/_telefuncs': neither,
    '/%5Ftelefunc': neither,
    '/other': neither,
  })
})

test('a configured `telefuncUrl` with a dot segment is read as `serve({ req, res })` reads it, for every request', async () => {
  config.telefuncUrl = '/api/./_telefunc'
  expect(await sendAll(['/api/./_telefunc', '/api/_telefunc'])).toEqual({
    '/api/./_telefunc': neither,
    '/api/_telefunc': neither,
  })
})
