export { testRunDocker }

import { page, test, expect, expectLog, run, getServerUrl, autoRetry, fetchHtml } from '@brillout/test-e2e'
import { testCounter } from '../utils'
import { testFileUpload } from './pages/file-upload/e2e-test'
import { testStreaming } from './pages/streaming/e2e-test'
import { testAbort } from './pages/abort/e2e-test'
import { testClose } from './pages/close/e2e-test'
import { testChannel } from './pages/channel/e2e-test'
import { testFunction } from './pages/function/e2e-test'
import { testStreamToServer } from './pages/stream-to-server/e2e-test'
import { testLiveQuery } from './pages/live-query/e2e-test'
import { testRxjs } from './pages/rxjs/e2e-test'
import { testPublish } from './pages/publish/e2e-test'

// Caddy serves https://localhost:8443 with its own internal CA — skip cert validation.
;(globalThis as { process?: { env: Record<string, string | undefined> } }).process!.env.NODE_TLS_REJECT_UNAUTHORIZED =
  '0'

function testRunDocker() {
  run('pnpm test:docker', {
    serverUrl: 'https://localhost:8443',
    serverIsReadyMessage: 'serving initial configuration',
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
        t.includes('Failed to load resource: the server responded with a status of 403') ||
        (t.includes('WebSocket connection to') && t.includes('failed'))
      )
    },
  })

  test('hello', async () => {
    {
      const html = await fetchHtml('/')
      expect(html).toContain('Loading...')
      expect(html).not.toContain('Eva')
    }
    {
      page.goto(`${getServerUrl()}/`)
      await autoRetry(async () => {
        expect(await page.textContent('body')).toContain('Welcome Eva')
      })
      expect(await page.textContent('body')).not.toContain('Loading')
    }
  })

  test('counter', async () => {
    await testCounter()
  })

  testFileUpload()
  testStreaming()
  testAbort()
  testClose()
  testChannel(false, true)
  testFunction()
  testStreamToServer()
  testLiveQuery()
  testRxjs(true)
  testPublish()

  test('shield() generation', async () => {
    {
      const resp = await makeTelefuncHttpRequest('Jon')
      expect(resp.status).toBe(200)
      const { ret } = await resp.json()
      expect(ret.message).toBe('Welcome Jon')
    }
    {
      const resp = await makeTelefuncHttpRequest(1337)
      expect(resp.status).toBe(422)
      expect(await resp.text()).toBe('Shield Validation Error')
      expectLog('Shield Validation Error', {
        // Docker `2>&1` collapses container stderr into stdout, so don't filter on source.
        filter: (log) => log.logText.includes('onLoad()') && log.logText.includes('Hello.telefunc.ts'),
      })
    }
  })
}

async function makeTelefuncHttpRequest(name: string | number) {
  const resp = await fetch(`${getServerUrl()}/_telefunc`, {
    method: 'POST',
    body: JSON.stringify({
      file: '/pages/index/Hello.telefunc.ts',
      name: 'onLoad',
      args: [{ name }],
    }),
  })
  return resp
}
