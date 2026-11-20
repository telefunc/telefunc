import { afterAll, beforeAll, expect, onTestFinished, test, vi } from 'vitest'
import { createServer, request, type Server } from 'node:http'
import { randomBytes } from 'node:crypto'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'

let server: Server
let host: string
/** The array the app assigns, which it may change afterwards. */
const appOrigins = ['https://app.example.com', 'https://*.example.org', 'https://bücher.example']

beforeAll(async () => {
  config.channel.allowedOrigins = appOrigins
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
  [
    'a page of an allowed origin whose entry writes its host as a person does',
    OPENS,
    () => ({ Host: host, Origin: 'https://xn--bcher-kva.example' }),
  ],
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
  [
    'a page of one subdomain under an allowed wildcard, on another scheme',
    REFUSED,
    () => ({ Host: host, Origin: 'http://a.example.org' }),
  ],
  [
    'a page of one subdomain under an allowed wildcard, on another port',
    REFUSED,
    () => ({ Host: host, Origin: 'https://a.example.org:8443' }),
  ],
  ['a page whose origin is opaque', REFUSED, () => ({ Host: host, Origin: 'null' })],
  [
    "a page of an app shell with a scheme of its own, which the app doesn't allow, against a Host of the same name",
    REFUSED,
    () => ({ Host: 'localhost', Origin: 'capacitor://localhost' }),
  ],
  [
    'a page of the same origin behind a proxy that sends an empty X-Forwarded-Host',
    OPENS,
    () => ({ Host: host, Origin: `http://${host}`, 'X-Forwarded-Host': '' }),
  ],
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
  'app.example.com',
  'https://app.*.example.com',
  'https://*example.com',
  'https://app.example.com/path',
  'https://app.example.com/.',
  'https://app.example.com\\',
  'https://user@app.example.com',
  'https://app.example.com?',
  'https://app.example.com#',
  ' https://app.example.com',
  'https://app.exa\tmple.com',
])('`allowedOrigins` refuses %j at configuration, saying what an entry looks like', (entry) => {
  expect(() => {
    config.channel.allowedOrigins = [entry]
  }).toThrow(
    "an entry is an origin as a browser sends it, a scheme and a host with no path, such as 'https://app.example.com'",
  )
})

test.each([
  ['null', 'a page that has no origin sends `null`, and any site can make its page send it'],
  ['file://', 'a page opened from a file has no origin of its own'],
  ['file:///home/app/index.html', 'a page opened from a file has no origin of its own'],
  ['capacitor://*.localhost', "a leading '*.' stands for a subdomain on http and https only"],
])('`allowedOrigins` refuses %j at configuration, saying why', (entry, why) => {
  expect(() => {
    config.channel.allowedOrigins = [entry]
  }).toThrow(why)
})

test('`allowedOrigins` refuses a value that is not an array at configuration', () => {
  // `Reflect.set` assigns what the setting's type doesn't allow, as an app written in JavaScript can.
  expect(() => Reflect.set(config.channel, 'allowedOrigins', 'https://app.example.com')).toThrow(
    '`config.channel.allowedOrigins` should be an array of origins',
  )
  expect(config.channel.allowedOrigins).toEqual(appOrigins)
})

test.each([
  ['an entry that is `undefined`', [undefined]],
  // Its index 0 is a hole.
  ['a hole', Object.assign([], { 1: 'https://app.example.com' })],
])('`allowedOrigins` refuses a list with %s at configuration', (_case, list) => {
  expect(() => Reflect.set(config.channel, 'allowedOrigins', list)).toThrow(
    '`config.channel.allowedOrigins` lists undefined: an entry is an origin',
  )
})

test("a later change of the app's array doesn't change `allowedOrigins`, and reading it back gives the entries as assigned, which can't be changed", async () => {
  appOrigins.push('https://pushed.example')
  onTestFinished(() => {
    appOrigins.pop()
  })
  expect(await upgradeStatus({ Host: host, Origin: 'https://pushed.example' })).toBe(REFUSED)
  const readBack = config.channel.allowedOrigins
  expect(readBack).toEqual(['https://app.example.com', 'https://*.example.org', 'https://bücher.example'])
  expect(Object.isFrozen(readBack)).toBe(true)
})

/** What refused handshakes leave behind, from a process that has logged no refusal yet: the lines printed, the keys
 *  added to the process's once-only warnings, and the names the refusal record keeps. */
async function refuseAll(origins: string[]): Promise<{ lines: string[]; onceKeysAdded: number; namesKept: string[] }> {
  const onceKeys = getGlobalObject<{ alreadyLogged: Set<string> }>('assert.ts', {
    alreadyLogged: new Set(),
  }).alreadyLogged
  const refusals = getGlobalObject('wire-protocol/channel/server/upgradeOrigin.ts', {
    named: new Set<string>(),
    windowStart: 0,
  })
  refusals.named.clear()
  refusals.windowStart = 0
  const onceKeysBefore = onceKeys.size
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  try {
    for (const origin of origins) expect(await upgradeStatus({ Host: host, Origin: origin })).toBe(REFUSED)
    return {
      lines: warn.mock.calls.map((call) => String(call[0])),
      onceKeysAdded: onceKeys.size - onceKeysBefore,
      namesKept: [...refusals.named],
    }
  } finally {
    warn.mockRestore()
  }
}

test("refused handshakes with long Origin headers that aren't origins print one short line, and keep nothing", async () => {
  const origins = Array.from({ length: 30 }, (_, i) => `https://${'a'.repeat(4000)}${i}.example`)
  const { lines, onceKeysAdded, namesKept } = await refuseAll(origins)
  expect(lines).toHaveLength(1)
  expect(lines[0]).toContain('A WebSocket upgrade from a page whose Origin header names no origin was refused')
  expect(lines[0]?.length).toBeLessThan(1000)
  expect(onceKeysAdded).toBe(0)
  expect(namesKept).toEqual(['a page whose Origin header names no origin'])
})

test('a refused page whose origin is opaque is named as such, not by what its Origin header says', async () => {
  // `data:` parses, and its origin is the opaque `null`.
  const { lines, namesKept } = await refuseAll(['data:,x'])
  expect(namesKept).toEqual(['a page whose Origin header names no origin'])
  expect(lines).toEqual([
    expect.stringContaining('A WebSocket upgrade from a page whose Origin header names no origin was refused'),
  ])
})

test('a refused page whose Origin is a `blob:` URL with a long host is named as one with no origin, in one short line', async () => {
  // A `blob:` URL's own host is empty: its origin, the name a refusal prints, is its inner URL's.
  const { lines, namesKept } = await refuseAll([`blob:https://${'a'.repeat(4000)}.example/id`])
  expect(namesKept).toEqual(['a page whose Origin header names no origin'])
  expect(lines).toHaveLength(1)
  expect(lines[0]?.length).toBeLessThan(1000)
})

test('refused pages whose Origin has a scheme longer than any origin has are named as ones with no origin, in one short line, and keep one name', async () => {
  // A scheme of an app shell's own has an origin of its own form (`capacitor://localhost`): its length is bounded too.
  const origins = Array.from({ length: 21 }, (_, i) => `${'s'.repeat(8000)}${i}://x`)
  const { lines, namesKept } = await refuseAll(origins)
  expect(lines).toEqual([
    expect.stringContaining('A WebSocket upgrade from a page whose Origin header names no origin was refused'),
  ])
  expect(lines[0]?.length).toBeLessThan(1000)
  expect(namesKept).toEqual(['a page whose Origin header names no origin'])
})

test.each([
  ['a path', 'https://app.example.com/page'],
  ['upper case', 'HTTPS://APP.EXAMPLE.COM'],
])(
  "an Origin header with %s isn't written as a browser sends an origin: the upgrade is refused, though that origin is allowed, and the warning doesn't say to allow it",
  async (_case, header) => {
    const { lines, namesKept } = await refuseAll([header])
    expect(namesKept).toEqual(['a page whose Origin header names no origin'])
    expect(lines).toEqual([expect.stringContaining("its Origin header isn't an origin as a browser writes one")])
    expect(lines[0]).not.toContain('add that origin')
  },
)

test("after 20 other sites' origins, a refused origin is named again once the next 10 minutes begin", async () => {
  vi.useFakeTimers({ toFake: ['Date'] })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  const strangers = Array.from({ length: 20 }, (_, i) => `https://stranger-${i}.example`)
  await refuseAll(strangers)
  vi.setSystemTime(Date.now() + 10 * 60 * 1000)
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  onTestFinished(() => warn.mockRestore())
  expect(await upgradeStatus({ Host: host, Origin: 'https://own-pages.example' })).toBe(REFUSED)
  expect(warn).toHaveBeenCalledWith(
    expect.stringContaining('A WebSocket upgrade from https://own-pages.example was refused'),
  )
})

test('refused handshakes from many origins name the first 20 origins, then say once that the rest are not logged', async () => {
  const origins = Array.from({ length: 40 }, (_, i) => `https://site-${i}.example`)
  const { lines, onceKeysAdded, namesKept } = await refuseAll([...origins, ...origins])
  expect(lines).toHaveLength(21)
  lines
    .slice(0, 20)
    .forEach((line, i) => expect(line).toContain(`A WebSocket upgrade from https://site-${i}.example was refused`))
  expect(lines[20]).toContain(
    'were refused within 10 minutes: the upgrades of further origins are refused without a warning until those 10 minutes have passed',
  )
  expect(onceKeysAdded).toBe(0)
  // The 21st stands for the closing line.
  expect(namesKept).toEqual(origins.slice(0, 21))
})
