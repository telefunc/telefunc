import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, request, type Server } from 'node:http'
import { randomBytes } from 'node:crypto'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'

// The pages of app shells with a scheme of their own: Capacitor on iOS, Tauri on macOS and Linux, Cordova with Ionic's
// web view on iOS; and an entry written as an address bar shows it. Their own file: the entries are assigned before any
// page connects.

let server: Server
let host: string

beforeAll(async () => {
  config.channel.allowedOrigins = [
    'capacitor://localhost',
    'tauri://localhost',
    'ionic://localhost',
    'https://pages.example.com/',
  ]
  server = createServer((_req, res) => res.writeHead(404).end())
  new Telefunc().installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  host = `127.0.0.1:${address.port}`
})
afterAll(() => server.close())

/** The status a WebSocket handshake to `/_telefunc` from a page of `origin` is answered with. */
function upgradeStatus(origin: string): Promise<number | undefined> {
  return new Promise((resolve, reject) => {
    const [hostname, port] = host.split(':')
    const req = request({
      host: hostname,
      port,
      path: '/_telefunc',
      headers: {
        Connection: 'Upgrade',
        Upgrade: 'websocket',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': randomBytes(16).toString('base64'),
        Origin: origin,
      },
    })
    req.on('upgrade', (res, socket) => {
      socket.destroy()
      resolve(res.statusCode)
    })
    req.on('response', (res) => {
      res.resume()
      resolve(res.statusCode)
    })
    req.on('error', reject)
    req.end()
  })
}

test.each(['capacitor://localhost', 'tauri://localhost', 'ionic://localhost'])(
  'the WebSocket upgrade of a page of the allowed app shell origin %s opens',
  async (origin) => {
    expect(await upgradeStatus(origin)).toBe(101)
  },
)

test('the WebSocket upgrade of a page of an origin allowed with one trailing slash, as an address bar shows it, opens', async () => {
  expect(await upgradeStatus('https://pages.example.com')).toBe(101)
})

test("the WebSocket upgrade of a page of an app shell origin the app does not allow is refused, and its warning says only to list that origin, as a proxy's host can't be the shell's", async () => {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  try {
    expect(await upgradeStatus('capacitor://another-app')).toBe(403)
    expect(warn).toHaveBeenCalledTimes(1)
    const warning = String(warn.mock.calls[0]?.[0])
    expect(warning).toContain('A WebSocket upgrade from capacitor://another-app was refused')
    expect(warning).toContain('add that origin to `config.channel.allowedOrigins`')
    expect(warning).not.toContain('X-Forwarded-Host')
  } finally {
    warn.mockRestore()
  }
})
