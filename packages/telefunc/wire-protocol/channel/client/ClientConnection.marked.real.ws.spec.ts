// The twins, on the real Node server, of ClientConnection.marked.spec.ts's rows of a `route` refusal of the open: the
// server serves channels over WebSocket only, so it refuses the page's SSE open `route` (decisions/516-sse-carrier.md,
// the ruling on step 6's check). A proxy that rewrites the refusal's kind stands for one the page does not know.

import { expect, test } from 'vitest'
import { delay, opens, pair, sends, until, useRealServer, world } from './realServer.fixture.js'

useRealServer(['ws'])

test('a `route` refusal of the open, with WebSocket next in the order, opens a WebSocket at once', async () => {
  const page = pair(crypto.randomUUID(), ['sse', 'ws'])
  sends(page.page, 'over the WebSocket')
  await until(() => page.got.length > 0 || page.ended.length > 0)
  expect(page.ended).toEqual([])
  expect(page.got).toEqual(['over the WebSocket'])
  expect(world.sockets).toBe(1)
  expect(opens()).toBe(1)
  expect(world.requests[0]?.contentType).toMatch(/^text\/vnd\.telefunc\.refusal; kind=route; /)
})

test.each([
  ['`route`', (contentType: string) => contentType, /The server doesn't serve SSE channel requests\./],
  ['of a kind the page does not know', (contentType: string) => contentType.replace('kind=route', 'kind=later'), /400/],
  ['of no kind', (contentType: string) => contentType.replace('kind=route; ', ''), /400/],
])(
  'a refusal %s of the open, with no other transport, ends every channel at the first answer, and no open follows',
  async (_kind, rewrite, reason) => {
    world.hooks = [{ kind: 'open', contentType: rewrite }]
    const connectionKey = crypto.randomUUID()
    const pages = [pair(connectionKey), pair(connectionKey)]
    await until(() => pages.every(({ ended }) => ended.length > 0))
    for (const { ended } of pages) expect(ended[0]?.message).toMatch(reason)
    await delay(1_000)
    expect(opens()).toBe(1)
  },
)
