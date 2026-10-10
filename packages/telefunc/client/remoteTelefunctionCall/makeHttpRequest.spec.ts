import { expect, test } from 'vitest'
import { makeHttpRequest } from './makeHttpRequest.js'
import { TELEFUNC_SESSION_HEADER } from '../../wire-protocol/constants.js'

test("a page's concurrent first calls present one session token, so they reach one session", async () => {
  const requests: Array<{ session: string | null; headers: Record<string, string> }> = []
  const fetch = (async (url: string, init: RequestInit) => {
    requests.push({
      session: new URL(url).searchParams.get('session'),
      headers: init.headers as Record<string, string>,
    })
    return new Response('', { status: 500 })
  }) as unknown as typeof globalThis.fetch
  const call = () =>
    makeHttpRequest({
      telefuncUrl: 'http://first-calls.test/_telefunc',
      httpRequestBody: '{}',
      telefunctionName: 'onLoad',
      telefuncFilePath: '/page.telefunc.ts',
      headers: null,
      fetch,
      abortController: new AbortController(),
      channel: { transports: ['sse'] },
      requestCloseHandlers: [],
      extensionResponseTypes: [],
    }).catch(() => {})
  await Promise.all([call(), call()])
  expect(requests[0]!.session).toEqual(expect.any(String))
  expect(requests[1]!.session).toBe(requests[0]!.session)
  // Only the Cloudflare Worker sets the header, on the request it forwards to the session Durable Object.
  for (const { headers } of requests) expect(headers).not.toHaveProperty(TELEFUNC_SESSION_HEADER)
})

test('a page keeps the session token it named, whatever a response names', async () => {
  const sessions: Array<string | null> = []
  const fetch = (async (url: string) => {
    sessions.push(new URL(url).searchParams.get('session'))
    return new Response('', { status: 500, headers: { [TELEFUNC_SESSION_HEADER]: 'named-by-the-server' } })
  }) as unknown as typeof globalThis.fetch
  const call = () =>
    makeHttpRequest({
      telefuncUrl: 'http://kept-token.test/_telefunc',
      httpRequestBody: '{}',
      telefunctionName: 'onLoad',
      telefuncFilePath: '/page.telefunc.ts',
      headers: null,
      fetch,
      abortController: new AbortController(),
      channel: { transports: ['sse'] },
      requestCloseHandlers: [],
      extensionResponseTypes: [],
    }).catch(() => {})
  await call()
  await call()
  expect(sessions[1]).toBe(sessions[0])
})
