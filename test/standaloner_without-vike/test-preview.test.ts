import { expect, fetch, getServerUrl, test } from '@brillout/test-e2e'
import { runPreview } from './previewTest'

import { WIRE_VERSION } from '../../packages/telefunc/wire-protocol/constants'

runPreview('pnpm run preview')

test('the first WebSocket opens at the URL from the build', async () => {
  const url = new URL(`/api/_telefunc?wire=${WIRE_VERSION}`, getServerUrl())
  url.protocol = 'ws:'
  const ws = new WebSocket(url)
  try {
    expect(
      await new Promise((resolve) => {
        ws.onopen = () => resolve('open')
        ws.onerror = () => resolve('error')
      }),
    ).toBe('open')
  } finally {
    ws.close()
  }
})

test('the built URL serves the telefunction and leaves an app route alone', async () => {
  const response = await fetch(getServerUrl() + '/api/_telefunc', {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify({ file: '/hello.telefunc.ts', name: 'onHello', args: ['world'] }),
  })
  expect(await response.text()).toBe('{"ret":"Hello world"}')
  expect(await (await fetch(getServerUrl() + '/status')).text()).toBe('app response')
  expect((await fetch(getServerUrl() + '/api/_telefunc/other', { method: 'POST', body: '{}' })).status).toBe(404)
})
