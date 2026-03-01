export { testRun }

import { page, test, expect, expectLog, run, getServerUrl, autoRetry, fetchHtml } from '@brillout/test-e2e'
import { testCounter } from '../utils'
import { testFileUpload } from './pages/file-upload/e2e-test'
import { testStreaming } from './pages/streaming/e2e-test'
import { testAbort } from './pages/abort/e2e-test'

function testRun(cmd: 'npm run dev' | 'npm run preview') {
  run(cmd, {
    tolerateError(log) {
      return (
        log.logText.includes('File arguments are being consumed out of order') ||
        log.logText.includes('multiple streaming values') ||
        log.logText.includes('the server responded with a status of 500') ||
        log.logText.includes('Unexpected generator error') ||
        log.logText.includes('The user aborted a request') ||
        log.logText.includes('Telefunc call cancelled')
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
