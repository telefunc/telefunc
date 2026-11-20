export { testCounter }
export { testRunClassic }
export { withNetworkConditions }
export { newCDPSession }
export { sendRefusedWarning }
export { upgradeStatus }
export { rawRequestStatus }

import {
  page,
  expect,
  autoRetry,
  run,
  skip,
  test,
  fetchHtml,
  getServerUrl,
  editFile,
  editFileRevert,
} from '@brillout/test-e2e'
import { request as httpRequest } from 'node:http'
import { request as httpsRequest } from 'node:https'
import { randomBytes } from 'node:crypto'
import { connect } from 'node:net'
import { once } from 'node:events'

const sendRefusedWarning = 'A channel refused a `send()`'

/**
 * The status the server answers a WebSocket handshake to its Telefunc URL with, from a page of `origin`, or `'no answer'`
 * where it ends the connection without one.
 */
function upgradeStatus(origin: string): Promise<number | undefined | 'no answer'> {
  const url = new URL('/_telefunc', getServerUrl())
  return new Promise((resolve, reject) => {
    const req = (url.protocol === 'https:' ? httpsRequest : httpRequest)(url, {
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
    req.on('error', (err) => {
      // Node's client names a connection that ended before any answer this way.
      if (err.message === 'socket hang up') resolve('no answer')
      else reject(err)
    })
    req.end()
  })
}

/**
 * The status the server answers a request written byte for byte with, for a request line or a `Host` no HTTP client
 * sends (`GET //_telefunc`, `Host: a b`); `head` holds the request line and the headers, without the blank line.
 */
async function rawRequestStatus(head: string[]): Promise<number> {
  const { hostname, port } = new URL(getServerUrl())
  const socket = connect(Number(port), hostname)
  await once(socket, 'connect')
  socket.write(`${head.join('\r\n')}\r\nConnection: close\r\n\r\n`)
  const first = await new Promise<Buffer>((resolve) => socket.once('data', resolve))
  socket.destroy()
  return Number(first.toString('latin1').split(' ')[1])
}

function newCDPSession() {
  // test-e2e's `page` is a proxy, and a CDP session needs the page itself.
  return page.context().newCDPSession(page.mainFrame().page())
}

/** In bytes per second each way, -1 for none. */
type NetworkConditions = { downloadThroughput: number; uploadThroughput: number }

/** Runs `run` with the page's link throttled as Chrome's DevTools throttle it, WebSockets included; lifted after, however `run` ends. */
async function withNetworkConditions<T>(conditions: NetworkConditions, run: () => Promise<T>): Promise<T> {
  const cdp = await newCDPSession()
  const emulate = ({ downloadThroughput, uploadThroughput }: NetworkConditions) =>
    cdp.send('Network.emulateNetworkConditions', { offline: false, latency: 0, downloadThroughput, uploadThroughput })
  try {
    await cdp.send('Network.enable')
    await emulate(conditions)
    return await run()
  } finally {
    // Detached even where lifting the throttle fails, as on a page that closed meanwhile.
    try {
      await emulate({ downloadThroughput: -1, uploadThroughput: -1 })
    } finally {
      await cdp.detach()
    }
  }
}

async function testCounter(currentValue = 0) {
  await autoRetry(
    async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    },
    { timeout: 5 * 1000 },
  )
  const btn = page.locator('button', { hasText: 'Counter' })
  expect(await btn.textContent()).toBe(`Counter ${currentValue}`)
  await btn.click()
  expect(await btn.textContent()).toBe(`Counter ${currentValue + 1}`)
}

function testRunClassic(
  cmd: 'pnpm run dev' | 'pnpm run preview' | 'pnpm run prod',
  {
    skipAboutPage,
    skipViteEcosystemCi,
    tolerateError,
    additionalTimeout,
    testHmr,
    isVue,
  }: {
    skipAboutPage?: true
    skipViteEcosystemCi?: true
    testHmr?: false | string
    isVue?: true
    tolerateError?: NonNullable<Parameters<typeof run>[1]>['tolerateError']
    additionalTimeout?: number
  } = {},
) {
  const isDev = cmd === 'pnpm run dev'

  if (skipViteEcosystemCi && process.env.VITE_ECOSYSTEM_CI) {
    skip("SKIPPED: skipping this test from Vite's ecosystem CI, see https://github.com/vikejs/vike/pull/2220")
    return
  }

  run(cmd, { tolerateError, additionalTimeout })

  test('page content is rendered to HTML', async () => {
    const html = await fetchHtml('/')
    expect(html).toContain('<h1>Welcome</h1>')
  })

  test('page is rendered to the DOM and interactive', async () => {
    await page.goto(getServerUrl() + '/')
    await page.click('a[href="/"]')
    expect(await page.textContent('h1')).toBe('Welcome')
    await testCounter()
  })

  if (isDev && testHmr !== false) {
    test('HMR', async () => {
      await testCounter(1)
      const org = 'Welcome'
      const mod = 'Wilkommen'
      expect(await page.textContent('h1')).toBe(org)
      editFile(testHmr || `./pages/index/+Page.${isVue ? 'vue' : 'tsx'}`, (s) => s.replace(org, mod))
      await autoRetry(
        async () => {
          expect(await page.textContent('h1')).toBe(mod)
        },
        { timeout: 5000 },
      )
      // Vite's file watcher drops a change of a file within 50 ms of the one before, which a revert as soon as the edit
      // shows can be on a busy runner.
      await new Promise((resolve) => setTimeout(resolve, 500))
      editFileRevert()
      await autoRetry(
        async () => {
          expect(await page.textContent('h1')).toBe(org)
        },
        { timeout: 5000 },
      )
      await testCounter(2)
    })
  }

  if (!skipAboutPage) {
    test('about page', async () => {
      await page.click('a[href="/about"]')
      await autoRetry(async () => {
        expect(await page.textContent('h1')).toBe('About')
      })
      expect(await page.textContent('p')).toBe('Example of using Vike.')
      const html = await fetchHtml('/about')
      expect(html).toContain('<h1>About</h1>')
    })
  }
}
