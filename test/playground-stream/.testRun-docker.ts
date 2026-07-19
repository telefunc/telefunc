export { testRunDocker }

import { page, test, expect, run, skip, isCI, getServerUrl, autoRetry } from '@brillout/test-e2e'
import { execSync } from 'node:child_process'
import { navigate } from './e2e-utils'
import { testCounter } from '../utils'
import { testFileUpload } from './pages/file-upload/e2e-test'
import { testFileDownload } from './pages/file-download/e2e-test'
import { testStreaming } from './pages/streaming/e2e-test'
import { testAbort } from './pages/abort/e2e-test'
import { testClose } from './pages/close/e2e-test'
import { testChannel } from './pages/channel/e2e-test'
import { testFunction } from './pages/function/e2e-test'
import { testStreamToServer } from './pages/stream-to-server/e2e-test'
import { testLiveQuery } from './pages/live-query/e2e-test'
import { testRxjs } from './pages/rxjs/e2e-test'
import { testPublish } from './pages/publish/e2e-test'
import { testRefIdentity } from './pages/ref-identity/e2e-test'

// Caddy serves https://localhost:8443 with its own internal CA — skip cert validation.
;(globalThis as { process?: { env: Record<string, string | undefined> } }).process!.env.NODE_TLS_REJECT_UNAUTHORIZED =
  '0'

function testRunDocker() {
  // Skip locally when Docker is unavailable. On CI a missing Docker should fail loudly,
  // not silently shrink coverage.
  if (!isCI() && !isDockerAvailable()) {
    skip('SKIPPED: Docker is not available (`docker info` failed).')
    return
  }

  run('pnpm test:docker', {
    serverUrl: 'https://localhost:8443',
    serverIsReadyMessage: 'serving initial configuration',
    tolerateExitCode: [130],
    tolerateError(log) {
      const t = log.logText
      return (
        t.includes('Container ') ||
        t.includes('Network ') ||
        t.includes('Volume ') ||
        t.includes('Gracefully Stopping') ||
        t.includes('ELIFECYCLE') ||
        t.includes('File arguments are being consumed out of order') ||
        t.includes('multiple streaming values') ||
        t.includes('the server responded with a status of 500') ||
        t.includes('the server responded with a status of 422') ||
        t.includes('[telefunc:channel-error]') ||
        t.includes('Error: server-listener-bug') ||
        t.includes('Unexpected generator error') ||
        t.includes('[telefunc:rxjs]') ||
        t.includes('Unhandled rxjs error') ||
        t.includes('Shield Validation Error') ||
        t.includes('Channel timed out: client did not reconnect') ||
        t.includes('The user aborted a request') ||
        t.includes('Telefunc call cancelled') ||
        t.includes('ERR_INTERNET_DISCONNECTED') ||
        t.includes('ERR_ALPN_NEGOTIATION_FAILED') ||
        // Expected during the docker reconnect test — `restartProxy()` deliberately kills
        // Caddy at the TCP layer; the browser sees these while it retries reconnecting.
        t.includes('ERR_CONNECTION_CLOSED') ||
        t.includes('ERR_CONNECTION_RESET') ||
        t.includes('ERR_CONNECTION_REFUSED') ||
        t.includes('ERR_NETWORK_CHANGED') ||
        t.includes('ERR_SSL_PROTOCOL_ERROR') ||
        // Chromium-in-Docker `NetworkChangeNotifier` aborts in-flight asset fetches on the
        // bridge interface state shift; the dynamic-import wrapper then reports this. The
        // `home page` test's reload loop recovers from it, so tolerate the noise it leaves behind.
        t.includes('Failed to fetch dynamically imported module') ||
        t.includes('Failed to load resource: the server responded with a status of 403') ||
        (t.includes('WebSocket connection to') && t.includes('failed'))
      )
    },
  })

  test('home page', async () => {
    await navigate(`${getServerUrl()}/`, { waitUntil: 'load' })
    await autoRetry(
      async () => {
        expect(await page.textContent('h1')).toBe('Welcome')
      },
      { timeout: 10_000 },
    )
  })

  test('counter', async () => {
    await testCounter()
  })

  testFileUpload()
  testFileDownload()
  testStreaming()
  testAbort()
  testClose()
  testChannel(false, true)
  testFunction()
  testStreamToServer()
  testLiveQuery()
  testRxjs(true)
  testPublish()
  testRefIdentity()
}

// `docker info` fails both when the CLI is missing and when the daemon is not running;
// checking the binary alone would miss the stopped-daemon case.
function isDockerAvailable(): boolean {
  try {
    execSync('docker info', { stdio: 'ignore' })
    return true
  } catch {
    return false
  }
}
