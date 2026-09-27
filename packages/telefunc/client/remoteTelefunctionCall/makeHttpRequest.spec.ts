import { expect, test } from 'vitest'
import { makeHttpRequest } from './makeHttpRequest.js'
import { TELEFUNC_SESSION_HEADER } from '../../wire-protocol/constants.js'

test("a page's concurrent first calls present one session token, so they reach one session", async () => {
  const presented: Array<string | undefined> = []
  const fetch = (async (url: string, init: RequestInit) => {
    // Only the Cloudflare adapter sends the header, in its responses: a page's request never carries it.
    expect(init.headers as Record<string, string>).not.toHaveProperty(TELEFUNC_SESSION_HEADER)
    presented.push(new URL(url).searchParams.get('session') ?? undefined)
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
  expect(presented[0]).toEqual(expect.any(String))
  expect(presented[1]).toBe(presented[0])
})
