import { expect, fetch, getServerUrl, test } from '@brillout/test-e2e'
import { runPreview } from './previewTest'

runPreview('pnpm run preview')

test('the first HTTP request uses the telefunction URL from the build', async () => {
  const response = await fetch(getServerUrl() + '/api/_telefunc', {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify({ file: '/hello.telefunc.ts', name: 'onHello', args: ['world'] }),
  })
  expect(response.status).toBe(200)
  expect(await response.text()).toBe('{"ret":"Hello world"}')
})
