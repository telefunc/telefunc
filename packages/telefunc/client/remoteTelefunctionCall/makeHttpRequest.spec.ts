import { afterEach, expect, test, vi } from 'vitest'
import { makeHttpRequest } from './makeHttpRequest.js'
import { TELEFUNC_SESSION_HEADER } from '../../wire-protocol/constants.js'

afterEach(() => {
  vi.restoreAllMocks()
})

function callFrom(telefuncUrl: string) {
  const presented: Array<string | undefined> = []
  const fetch = (async (_url: string, init: RequestInit) => {
    presented.push((init.headers as Record<string, string>)[TELEFUNC_SESSION_HEADER])
    return new Response('', { status: 500 })
  }) as unknown as typeof globalThis.fetch
  const call = () =>
    makeHttpRequest({
      telefuncUrl,
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
  return { presented, call }
}

test("a page's concurrent first calls present one session token, so they reach one session", async () => {
  const { presented, call } = callFrom('http://first-calls.test/_telefunc')
  await Promise.all([call(), call()])
  expect(presented[0]).toEqual(expect.any(String))
  expect(presented[1]).toBe(presented[0])
})

test('a page served over plain http, which has no crypto.randomUUID(), still calls', async () => {
  vi.spyOn(crypto, 'randomUUID').mockImplementation(() => {
    throw new TypeError('crypto.randomUUID is not a function')
  })
  const { presented, call } = callFrom('http://192.168.1.2:3000/_telefunc')
  await call()
  expect(presented[0]).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
})
