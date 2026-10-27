import { expect, test } from 'vitest'
import { fileURLToPath } from 'node:url'
import { serve } from './telefunc.js'
import { config } from './serverConfig.js'

config.root = fileURLToPath(new URL('./fixture-app/', import.meta.url))
config.telefuncFiles = [fileURLToPath(new URL('./fixture-app/hello.telefunc.mjs', import.meta.url))]

test('serve({ url, method, body, headers }) accepts `request.url` and `request.headers` as they are', async () => {
  const request = new Request('http://localhost:3000/_telefunc', {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify({ file: '/hello.telefunc.mjs', name: 'onHello', args: [] }),
  })
  const httpResponse = await serve({
    url: request.url,
    method: request.method,
    body: await request.text(),
    headers: request.headers,
  })
  expect(await httpResponse.getBody()).toBe('{"ret":"Hello"}')
  expect(httpResponse.statusCode).toBe(200)
})
