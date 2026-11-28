import { expect, fetch, getServerUrl, run, test } from '@brillout/test-e2e'

run('pnpm run dev', { serverIsReadyMessage: (log) => log.includes('Local:') && log.includes('http://localhost:3000') })

const call = { file: '/hello.telefunc.ts', name: 'onHello', args: ['world'] }

test('`vite dev` serves telefunction calls at a custom `config.telefuncUrl`', async () => {
  for (const path of ['/api/_telefunc', '/api/_telefunc?client=1']) {
    const response = await fetch(getServerUrl() + path, {
      method: 'POST',
      headers: { 'x-telefunc-request': 'txt' },
      body: JSON.stringify(call),
    })
    expect(await response.text()).toBe('{"ret":"Hello world"}')
  }
})

test('`vite dev` leaves `/_telefunc` to the app once `config.telefuncUrl` names another URL', async () => {
  const response = await fetch(getServerUrl() + '/_telefunc', {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify(call),
  })
  expect(response.status).toBe(404)
})
