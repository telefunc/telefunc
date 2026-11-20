import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, request, type Server } from 'node:http'
import { randomBytes } from 'node:crypto'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'

let server: Server
let host: string

beforeAll(async () => {
  config.channel.allowedOrigins = ['https://app.example.com', 'https://*.example.org']
  server = createServer((_req, res) => res.writeHead(404).end())
  new Telefunc().installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  host = `127.0.0.1:${address.port}`
})
afterAll(() => server.close())

/** The status a WebSocket handshake to `/_telefunc` with `headers` is answered with. */
function upgradeStatus(headers: Record<string, string>): Promise<number | undefined> {
  return new Promise((resolve, reject) => {
    const [hostname, port] = host.split(':')
    const req = request({
      host: hostname,
      port,
      path: '/_telefunc',
      setHost: false,
      headers: {
        Connection: 'Upgrade',
        Upgrade: 'websocket',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': randomBytes(16).toString('base64'),
        ...headers,
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

const OPENS = 101
const REFUSED = 403

test.each<[string, number, () => Record<string, string>]>([
  ['a client that is no browser, with neither Origin nor Sec-Fetch-Site', OPENS, () => ({ Host: host })],
  ['a page of the same origin', OPENS, () => ({ Host: host, Origin: `http://${host}` })],
  [
    'a page the browser says is of the same origin',
    OPENS,
    () => ({ Host: host, Origin: 'https://public.example.net', 'Sec-Fetch-Site': 'same-origin' }),
  ],
  [
    'a page behind a proxy that forwards the public host',
    OPENS,
    () => ({ Host: host, Origin: 'https://public.example.net', 'X-Forwarded-Host': 'public.example.net, inner' }),
  ],
  ['a page of an allowed origin', OPENS, () => ({ Host: host, Origin: 'https://app.example.com' })],
  ['a page of one subdomain under an allowed wildcard', OPENS, () => ({ Host: host, Origin: 'https://a.example.org' })],
  ['a page of another site', REFUSED, () => ({ Host: host, Origin: 'https://another-site.example' })],
  [
    'a page of another site the browser says is cross-site',
    REFUSED,
    () => ({ Host: host, Origin: 'https://another-site.example', 'Sec-Fetch-Site': 'cross-site' }),
  ],
  [
    'a page of another port of the same host',
    REFUSED,
    () => ({ Host: host, Origin: `http://${host.split(':')[0]}:1` }),
  ],
  [
    'a page of two subdomain labels under an allowed wildcard',
    REFUSED,
    () => ({ Host: host, Origin: 'https://a.b.example.org' }),
  ],
  ["a page of the wildcard's parent itself", REFUSED, () => ({ Host: host, Origin: 'https://example.org' })],
  ['a page whose origin is opaque', REFUSED, () => ({ Host: host, Origin: 'null' })],
  [
    'a page whose origin matches Host but not the forwarded public host',
    REFUSED,
    () => ({ Host: host, Origin: `http://${host}`, 'X-Forwarded-Host': 'public.example.net' }),
  ],
])('the WebSocket upgrade of %s is answered %i', async (_case, status, headers) => {
  expect(await upgradeStatus(headers())).toBe(status)
})

test("a refused upgrade's warning names the origin, says what its page does, and how to let it open, once per origin", async () => {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  try {
    const origin = 'https://warned-site.example'
    expect(await upgradeStatus({ Host: host, Origin: origin })).toBe(REFUSED)
    expect(await upgradeStatus({ Host: host, Origin: origin })).toBe(REFUSED)
    expect(warn).toHaveBeenCalledTimes(1)
    const warning = String(warn.mock.calls[0]?.[0])
    expect(warning).toContain(`A WebSocket upgrade from ${origin} was refused`)
    // What the page does depends on its transports, which the server doesn't know: the warning says both.
    expect(warning).toContain('A page that also takes SSE stays on SSE')
    expect(warning).toContain(
      "one with WebSocket alone can't connect, and its channels end as when the server is unreachable",
    )
    expect(warning).toContain('forward the public host in `X-Forwarded-Host`')
    expect(warning).toContain('add that origin to `config.channel.allowedOrigins`')
  } finally {
    warn.mockRestore()
  }
})

test.each([
  'https://app.example.com/',
  'app.example.com',
  'https://app.*.example.com',
  'https://*example.com',
  'https://app.example.com/path',
])('`allowedOrigins` refuses %s at configuration, saying what an entry looks like', (entry) => {
  expect(() => {
    config.channel.allowedOrigins = [entry]
  }).toThrow(
    "an entry is an origin as a browser sends it, a scheme and a host with no path, such as 'https://app.example.com'",
  )
})
