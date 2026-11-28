export { testRun, runPlayground }

import { page, test, expect, run, getServerUrl, autoRetry } from '@brillout/test-e2e'
import { navigate, setServerRuntime } from './e2e-utils'
import { testCounter } from '../utils'
import { testFileUpload } from './pages/file-upload/e2e-test'
import { testFileDownload } from './pages/file-download/e2e-test'
import { testStreaming, testStreamingRequestContext } from './pages/streaming/e2e-test'
import { testAbort } from './pages/abort/e2e-test'
import { testClose } from './pages/close/e2e-test'
import { testChannel } from './pages/channel/e2e-test'
import { testFunction } from './pages/function/e2e-test'
import { testStreamToServer } from './pages/stream-to-server/e2e-test'
import { testLiveQuery } from './pages/live-query/e2e-test'
import { testRxjs } from './pages/rxjs/e2e-test'
import { testPublish } from './pages/publish/e2e-test'
import { testRedisReconnect } from './pages/publish/redis-e2e-test'
import { testRefIdentity } from './pages/ref-identity/e2e-test'
import { testUpgrade } from './pages/channel/upgrade-e2e-test'
import { testWire } from './pages/channel/wire-e2e-test'
import { testSlowLink } from './pages/channel/slow-link-e2e-test'
import { testSessionEnd } from './pages/channel/session-end-e2e-test'
import { parseChannelTransports } from './channelTransports'

function runPlayground(cmd: 'pnpm dev' | 'pnpm preview' | 'pnpm preview:bun' | 'pnpm preview:deno') {
  setServerRuntime(cmd === 'pnpm preview:bun' ? 'bun' : cmd === 'pnpm preview:deno' ? 'deno' : 'node')
  run(cmd, {
    // `pnpm preview` runs srvx (prints `Listening on:`); `pnpm dev` is `vike dev` on vite
    // (prints `Local:` + `http://localhost:3000`). Neither matches test-e2e's default
    // "Server running at" / "Accepting connections at" matcher, so the framework times
    // out waiting for ready unless we accept these explicitly.
    serverIsReadyMessage: (log) =>
      log.includes('Listening on') ||
      log.includes('Server running at') ||
      (log.includes('Local:') && log.includes('http://localhost:3000')),
    tolerateError(log) {
      return (
        log.logText.includes('File arguments are being consumed out of order') ||
        log.logText.includes('multiple streaming values') ||
        log.logText.includes('the server responded with a status of 500') ||
        log.logText.includes('the server responded with a status of 422') ||
        log.logText.includes('[telefunc:channel-error]') ||
        log.logText.includes('Error: server-listener-bug') ||
        log.logText.includes('TypeError: Converting circular structure to JSON') ||
        // Bun's wording of the same error.
        log.logText.includes('TypeError: JSON.stringify cannot serialize cyclic structures') ||
        log.logText.includes('Unexpected generator error') ||
        // rxjs/e2e-test.ts's errors on purpose; never a page that left, which Telefunc doesn't log.
        ((log.logText.includes('[telefunc:rxjs]') || log.logText.includes('Unhandled rxjs error')) &&
          !log.logText.includes('its page left')) ||
        log.logText.includes('Shield Validation Error') ||
        // Transitions between tests close the previous page's channels; the server-side
        // Subject/Observable wire waits for a reconnect within the grace period and logs this
        // if the browser has already navigated away.
        log.logText.includes('Channel timed out: client did not reconnect') ||
        log.logText.includes('The user aborted a request') ||
        log.logText.includes('Telefunc call cancelled') ||
        log.logText.includes('ERR_INTERNET_DISCONNECTED') ||
        log.logText.includes('ERR_ALPN_NEGOTIATION_FAILED') ||
        // A test dropped an SSE stream's connection (wire-sockets.ts).
        log.logText.includes('ERR_INCOMPLETE_CHUNKED_ENCODING') ||
        log.logText.includes('Failed to load resource: the server responded with a status of 403') ||
        // upgrade-e2e-test.ts's upgrade from another site's page.
        log.logText.includes('A WebSocket upgrade from https://another-site.example was refused') ||
        // upgrade-e2e-test.ts's upgrades with forged or odd Origin headers.
        log.logText.includes('A WebSocket upgrade from a page whose Origin header names no origin was refused') ||
        /A WebSocket upgrade from https:\/\/forged-\d+\.example was refused/.test(log.logText) ||
        log.logText.includes('WebSocket upgrades from more than 20 origins were refused') ||
        // A test's page change ends its SSE wire, and the server refuses a POST still in flight for it.
        log.logText.includes('Failed to load resource: the server responded with a status of 400') ||
        (log.logText.includes('WebSocket connection to') && log.logText.includes('failed')) ||
        // function/e2e-test.ts's `GET //` with Upgrade, on Node: Vike 0.4.259 logs a bug of its own for it (vikejs/vike#3357, fixed in Vike 0.4.260).
        ((cmd === 'pnpm dev' || cmd === 'pnpm preview') &&
          log.logSource === 'stderr' &&
          log.logText.includes('[Bug] You stumbled upon a Vike bug') &&
          log.logText.includes('at logHttpResponse')) ||
        // channel/e2e-test.ts's corrupted ACK_RES test triggers an intentional pageerror; Vite dev forwards it to stderr when HMR connects in time.
        (cmd === 'pnpm dev' &&
          log.logSource === 'stderr' &&
          log.logText.includes('[vite][Unhandled error] SyntaxError:') &&
          log.logText.includes('ClientChannel.onPeerAckRes'))
      )
    },
  })
}

function testRun(cmd: 'pnpm dev' | 'pnpm preview' | 'pnpm preview:bun' | 'pnpm preview:deno') {
  runPlayground(cmd)
  const isDev = cmd === 'pnpm dev'

  test('home page', async () => {
    await navigate(`${getServerUrl()}/`)
    await autoRetry(async () => {
      expect(await page.textContent('h1')).toBe('Welcome')
    })
  })

  test('counter', async () => {
    await testCounter()
  })

  testFileUpload(isDev)

  testFileDownload()

  testStreaming(isDev)
  testStreamingRequestContext()

  testAbort(isDev)

  testClose()

  testChannel(isDev)

  testWire(isDev)

  testSlowLink(isDev)

  if (parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).join() === 'ws') testSessionEnd()

  // Runs in every variant: asserts exactly-once commit where the env upgrades (['sse','ws']),
  // and zero commits everywhere else — each non-upgrading variant doubles as the control.
  testUpgrade(isDev)

  testFunction(isDev)

  testStreamToServer(isDev)

  testLiveQuery()

  testRxjs()

  testPublish(isDev)
  if (process.env.REDIS_URL) testRedisReconnect()

  testRefIdentity()
}
