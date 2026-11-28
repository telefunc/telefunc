import { expect, fetch, getServerUrl, test } from '@brillout/test-e2e'
import { runPreview } from './previewTest'

runPreview('pnpm run preview:manual')

test('explicit telefuncFiles win over the existing Vite entry', async () => {
  const response = await fetch(getServerUrl() + '/api/_telefunc', {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify({ file: '/hello.telefunc.ts', name: 'onHello', args: [] }),
  })
  expect(response.status).toBe(200)
  expect(await response.text()).toBe('{"ret":"Manual world"}')
})
