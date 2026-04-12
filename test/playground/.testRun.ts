export { testRun }

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

function testRun(cmd: 'npm run dev' | 'npm run preview') {
  run(cmd, {
    tolerateError(log) {
      return (
        log.logText.includes('File arguments are being consumed out of order') ||
        log.logText.includes('multiple streaming values') ||
        log.logText.includes('the server responded with a status of 500') ||
        log.logText.includes('[telefunc:channel-error]') ||
        log.logText.includes('Error: server-listener-bug') ||
        log.logText.includes('Unexpected generator error') ||
        log.logText.includes('[telefunc:rxjs]') ||
        log.logText.includes('Unhandled rxjs error') ||
        log.logText.includes('The user aborted a request') ||
        log.logText.includes('Telefunc call cancelled') ||
        log.logText.includes('ERR_INTERNET_DISCONNECTED') ||
        log.logText.includes('ERR_ALPN_NEGOTIATION_FAILED') ||
        log.logText.includes('Failed to load resource: the server responded with a status of 403') ||
        (log.logText.includes('WebSocket connection to') && log.logText.includes('failed'))
      )
    },
  })

  const isDev = cmd === 'npm run dev'

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

  testChannel(isDev)

  testFunction()

  testStreamToServer()

  testLiveQuery()

  testRxjs()

  if (!isDev) {
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
          filter: (log) =>
            log.logSource === 'stderr' && log.logText.includes('onLoad()') && log.logText.includes('Hello.telefunc.ts'),
        })
      }
    })
  }
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
