import { expect, fetch, getServerUrl, run, test } from '@brillout/test-e2e'

run('pnpm run preview')

test('telefunction call', async () => {
  const response = await fetch(getServerUrl() + '/_telefunc', {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify({ file: '/hello.telefunc.ts', name: 'onHello', args: ['world'] }),
  })
  expect(await response.text()).toBe('{"ret":"Hello world"}')
})
