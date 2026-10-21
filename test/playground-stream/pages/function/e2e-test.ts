export { testFunction }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import * as http from 'node:http'
import * as os from 'node:os'
import { navigate, getResult, forceServerGc, serverRuntime } from '../../e2e-utils'

function testFunction(isDev: boolean, inDocker = false) {
  test('function: simple greeter returns correct string', async () => {
    await navigate(`${getServerUrl()}/function`)

    await page.click('#greeter-run')

    await autoRetry(async () => {
      const result = await getResult<string>('#greeter-result')
      expect(result).toBe('Hello, World! (from server)')
    })
  })

  test('function: stateful adder accumulates server-side state across calls', async () => {
    await page.click('#adder-run')

    await autoRetry(async () => {
      const results = await getResult<number[]>('#adder-results')
      expect(results).to.deep.equal([15, 18, 20])
    })
  })

  test('function: server-side call counter increments across multiple calls', async () => {
    await page.click('#echo-run')

    await autoRetry(async () => {
      const results = await getResult<{ echo: string; callCount: number }[]>('#echo-results')
      expect(results[0]).deep.equal({ echo: 'hello', callCount: 1 })
      expect(results[1]).deep.equal({ echo: 'world', callCount: 2 })
    })
  })

  test('function: pass function as argument — map', async () => {
    await page.click('#map-run')

    await autoRetry(async () => {
      const result = await getResult<number[]>('#map-result')
      expect(result).to.deep.equal([1, 4, 9, 16, 25])
    })
  })

  test('function: pass function as argument — reduce', async () => {
    await page.click('#reduce-run')

    await autoRetry(async () => {
      const result = await getResult<number>('#reduce-result')
      expect(result).toBe(15)
    })
  })

  test('function: shield validates returned function args', async () => {
    await page.click('#shield-return-run')

    await autoRetry(async () => {
      // Valid call should succeed
      const ok = await getResult<number>('#shield-return-ok')
      expect(ok).toBe(7)

      // Invalid call (strings instead of numbers) — the server-side listener returns a
      // marker object (`FN_SHIELD_ERROR_KEY`) as its ack payload and the client-side
      // function reviver throws with the validator's message.
      const error = await getResult<string>('#shield-return-error')
      expect(error).not.toBe('NO_ERROR')
      expect(error).toMatch(/shield validation error/i)
    })
  })

  test('function: shield validates client-callback return value', async () => {
    await page.click('#shield-callback-run')

    await autoRetry(async () => {
      // Valid callback return passes through.
      const ok = await getResult<number>('#shield-callback-ok')
      expect(ok).toBe(42)

      // When the client-provided callback returns a bad type the server's reviver throws a
      // branded ShieldValidationError; the runTelefunc boundary surfaces it as the canonical
      // 422 so the client sees "Shield Validation Error" rather than a generic bug-error.
      const error = await getResult<string>('#shield-callback-error')
      expect(error).not.toMatch(/^NO_ERROR/)
      expect(error).toMatch(/shield validation error/i)
    })
  })

  test('function: file upload with progress callback receives incremental updates', async () => {
    await page.click('#upload-run')

    await autoRetry(async () => {
      const result = await getResult<{ name: string; size: number }>('#upload-result')
      expect(result).to.deep.equal({ name: 'test-upload.txt', size: 1024 * 100 })

      const progress = await getResult<number[]>('#upload-progress')
      // Should have received at least one progress update
      expect(progress.length).greaterThan(0)
      // Last progress update should be 100%
      expect(progress[progress.length - 1]).toBe(100)
      // Progress should be monotonically increasing
      for (let i = 1; i < progress.length; i++) {
        expect(progress[i]!).greaterThanOrEqual(progress[i - 1]!)
      }
    })
  })

  test("function: a search box's call answers at once, though the last keystroke's call, aborted before it left, passed a callback the server never got", async () => {
    await page.click('#search-run')

    await autoRetry(async () => {
      const search = await getResult<{ result: string; ms: number }>('#search-result')
      expect(search.result).toBe('<b>ab</b>')
      // Not held for the server's connectTtl, which waits for the aborted call's callback.
      expect(search.ms).lessThan(3000)
    })
  })

  if (JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!).includes('ws')) {
    test("function: a call's channel.idleTimeout of 0 closes its connection once the callback it passed is let go", async () => {
      await navigate(`${getServerUrl()}/function`)
      const webSocket = page.waitForEvent('websocket', { predicate: (ws) => ws.url().includes('/_telefunc') })
      await page.click('#idle-timeout-run')
      const ws = await webSocket

      await autoRetry(async () => {
        expect(await getResult<number[]>('#idle-timeout-result')).deep.equal([2, 4])
      })
      await forceServerGc()
      await autoRetry(async () => {
        expect(ws.isClosed()).toBe(true)
      })
    })
  }

  // Not on Deno: Deno.serve hands an upgrade request to the app, whose Vike logs a [Bug] rendering `//` (as it does a GET // on Node).
  if (serverRuntime() !== 'deno')
    test("function: the server keeps answering after a WebSocket upgrade whose path isn't a URL", async () => {
      await navigate(`${getServerUrl()}/function`)
      await requestPath('//', {
        Connection: 'Upgrade',
        Upgrade: 'websocket',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
      })
      await page.click('#greeter-run')
      await autoRetry(async () => {
        expect(await getResult<string>('#greeter-result')).toBe('Hello, World! (from server)')
      })
    })

  // Preview only: in development, telefunc logs a GET or HEAD as a malformed request.
  for (const method of isDev ? [] : ['GET', 'HEAD']) {
    test(`function: a ${method} request to /_telefunc gets an answer`, async () => {
      const response = await fetch(`${getServerUrl()}/_telefunc`, { method })
      expect(response.status).toBe(400)
      expect(await response.text()).toBe(method === 'GET' ? 'Malformed Telefunc Request' : '')
    })
  }

  test('function: a callback the server calls without awaiting it, which fails, leaves the server clean', async () => {
    await navigate(`${getServerUrl()}/function`)
    await page.click('#unawaited-run')
    await autoRetry(async () => {
      expect(await getResult<string>('#unawaited-result')).toBe('answered')
    })
  })

  // A browser gives crypto.randomUUID() only to secure origins: HTTPS or localhost.
  const lanAddress = Object.values(os.networkInterfaces())
    .flat()
    .find((address) => address?.family === 'IPv4' && !address.internal)?.address
  // Preview only: Vite's dev server listens on localhost; Docker serves HTTPS.
  // Not on Deno: Deno's node:zlib fails to gzip the page's assets (TypeError: invalid output range), which Chrome asks for on a plain-http page away from localhost (on localhost it asks for br).
  if (!isDev && !inDocker && lanAddress && serverRuntime() !== 'deno') {
    test('function: a page served over plain http from an address other than localhost opens a channel and passes a callback', async () => {
      const url = new URL(getServerUrl())
      url.hostname = lanAddress
      await navigate(`${url.origin}/channel`)
      await page.click('#channel-connect')
      await autoRetry(async () => {
        expect((await getResult<{ welcomeReceived: boolean }>('#channel-state')).welcomeReceived).toBe(true)
      })
      await navigate(`${url.origin}/function`)
      await page.click('#map-run')
      await autoRetry(async () => {
        expect(await getResult<number[]>('#map-result')).to.deep.equal([1, 4, 9, 16, 25])
      })
    })
  }
}

/** Sends a raw request for `path`, and resolves once the server answered or hung up. */
function requestPath(path: string, headers: Record<string, string>) {
  const { hostname, port } = new URL(getServerUrl())
  return new Promise<void>((resolve) => {
    const req = http.request({ hostname, port, path, headers, timeout: 2000 })
    const done = () => {
      req.destroy()
      resolve()
    }
    req.on('response', (res) => {
      res.resume()
      res.on('end', done)
    })
    req.on('upgrade', done)
    req.on('timeout', done)
    req.on('error', done)
    req.on('close', done)
    req.end()
  })
}
