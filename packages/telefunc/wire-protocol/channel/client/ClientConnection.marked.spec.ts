// What the page does with Telefunc's own refusals of its SSE requests, which say so in their media type with a kind:
// a REAL ClientChannel and its connection against a scripted server (scriptedServer.fixture.ts). The verdict is taken
// at the headers; no refusal's body is read. No answer carries an end the server decided: a `frames` answer is a kind
// the page doesn't read.

import { expect, test } from 'vitest'
import { SSE_RECONCILE_DEADLINE_MS } from '../../constants.js'
import { FakeWebSocket, advance, openChannel, serveSse, useScriptedServer, world } from './scriptedServer.fixture.js'

useScriptedServer()

/** `value`, which the test must have by now: `what` says what it is. */
function defined<T>(value: T | null | undefined, what: string): T {
  if (value === null || value === undefined) throw new Error(`Expected ${what}`)
  return value
}

/** Opens `count` channels over `transports`, the first at index 0, and waits till their wire carries them. */
async function open(count: number, transports: ('sse' | 'ws')[] = ['sse']) {
  serveSse()
  const opened = Array.from({ length: count }, () => openChannel(transports))
  await advance(SSE_RECONCILE_DEADLINE_MS + 500)
  return (i: number) => defined(opened[i], `channel ${i}`)
}

const marked = (params: string) => `text/vnd.telefunc.refusal; ${params}`
const typed = (status: number, ...contentType: string[]) => ({ tag: 'typed' as const, status, contentType })

test("Telefunc's own 409 to a batch POST tells the page that the server was updated", async () => {
  const page = (await open(1))(0)
  world.postAnswers = [
    typed(409, marked('kind=reload; reason="The server runs another wire version: reload the page."')),
  ]
  void page.channel.send('hello').catch(() => {})
  await advance(2_000)
  expect(page.ended[0]?.err?.message).toMatch(/server was updated since this page loaded/)
})

test.each([
  ['as the server sends it', [marked('kind=retry; reason="The server no longer has the connection."')]],
  ['with a charset added', [marked('kind=retry; charset=utf-8; reason="The server no longer has the connection."')]],
  [
    'with upper-case names',
    ['TEXT/VND.TELEFUNC.REFUSAL; KIND=retry; Reason="The server no longer has the connection."'],
  ],
  ['with its kind quoted', [marked('kind="retry"; reason="The \\"server\\" no longer has the connection."')]],
  ['after another value, the last one counting', ['text/html', marked('kind=retry; reason="No wire."')]],
])(
  'a `retry` refusal %s, twice running, is a loss: the channel goes on and its data arrives on the wire after',
  async (_how, contentType) => {
    const page = (await open(1))(0)
    world.postAnswers = [typed(400, ...contentType), typed(400, ...contentType)]
    void page.channel.send('again').catch(() => {})
    await advance(10_000)
    expect(page.ended).toEqual([])
    expect(world.server.got.filter(({ ix }) => ix === 0)).toHaveLength(1)
  },
)

test('a refusal whose reason is a quoted string with escapes gives the page the reason unescaped', async () => {
  const page = (await open(1))(0)
  world.postAnswers = [typed(400, marked('kind=route; reason="A \\"quoted\\" word and a back\\\\slash."'))]
  void page.channel.send('refused').catch(() => {})
  await advance(3_000)
  expect(page.ended.map(({ err }) => err?.message)).toEqual([
    'The server refused the page\'s SSE requests: A "quoted" word and a back\\slash.',
  ])
})

test('a marked refusal followed by an unmarked value is read as unmarked: refused twice, the channel ends naming it', async () => {
  const page = (await open(1))(0)
  const contentType = [marked('kind=retry; reason="No wire."'), 'text/html']
  world.postAnswers = [typed(400, ...contentType), typed(400, ...contentType)]
  void page.channel.send('refused').catch(() => {})
  await advance(10_000)
  expect(page.ended[0]?.err?.message).toMatch(/refused with 400 .*, text\/html, again after a reconnect/)
})

test.each([
  ['naming a channel', marked('kind=frames; channel=0; cause=6; reason="A frame the server cannot take."')],
  ['naming no channel', marked('kind=frames; cause=8; reason="The server can\'t read the request\'s frames."')],
])(
  "an answer of the kind `frames` %s, which only a wire's own end carries, ends no channel with its cause: the page reads a kind it does not know, and leaves SSE",
  async (_how, contentType) => {
    const opened = await open(2)
    const pages = [opened(0), opened(1)]
    world.postAnswers = [typed(400, contentType)]
    void pages[0]?.channel.send('refused').catch(() => {})
    await advance(3_000)
    for (const page of pages) {
      expect(page.ended.map(({ err }) => err?.message)).toEqual([
        expect.stringMatching(/in words this page can't read/),
      ])
    }
  },
)

test('a `route` refusal of the open, with WebSocket next in the order, opens a WebSocket at once', async () => {
  serveSse()
  world.openAnswers = [{ status: 400, contentType: [marked('kind=route; reason="The server doesn\'t serve SSE."')] }]
  const page = openChannel(['sse', 'ws'])
  await advance(1_000)
  expect(page.ended).toEqual([])
  expect(FakeWebSocket.made).toHaveLength(1)
  expect(world.opens).toBe(1)
})

test.each([
  ['`route`', marked('kind=route; reason="The server doesn\'t serve SSE."'), /The server doesn't serve SSE\./],
  ['of a kind the page does not know', marked('kind=later; reason="Come back later."'), /400/],
  ['of no kind', marked('reason="Refused."'), /400/],
  ['`frames` naming no cause', marked('kind=frames; reason="Refused."'), /400/],
])(
  'a refusal %s of the open, with no other transport, ends every channel at the first answer, and no open follows',
  async (_kind, contentType, reason) => {
    serveSse()
    world.openAnswers = [{ status: 400, contentType: [contentType] }]
    const pages = [openChannel(['sse']), openChannel(['sse'])]
    await advance(10_000)
    for (const page of pages) expect(page.ended[0]?.err?.message).toMatch(reason)
    expect(world.opens).toBe(1)
  },
)

/** Opens `count` channels over SSE with the streaming upload taken, and waits till their wire carries them. */
async function openWithUpload(count: number) {
  serveSse()
  world.uploads = true
  const opened = Array.from({ length: count }, () => openChannel(['sse']))
  await advance(SSE_RECONCILE_DEADLINE_MS + 500)
  expect(world.uploadPosts).toBe(1)
  return (i: number) => defined(opened[i], `channel ${i}`)
}

test('a `retry` answer to the streaming upload is a loss that keeps it: the next wire uploads again, and the data arrives', async () => {
  const page = (await openWithUpload(1))(0)
  world.uploadAnswers = [{ status: 400, contentType: [marked('kind=retry; reason="No wire."')] }]
  void page.channel.send('on the upload').catch(() => {})
  await advance(5_000)
  expect(page.ended).toEqual([])
  expect(world.uploadPosts).toBe(2)
  expect(world.server.got.filter(({ ix }) => ix === 0)).toHaveLength(1)
})

test.each([
  ['`reload`', marked('kind=reload; reason="Another wire version."'), /server was updated since this page loaded/],
  [
    '`route`',
    marked('kind=route; reason="Not served so."'),
    /The server refused the page's SSE requests: Not served so\./,
  ],
  [
    '`frames`, naming the channel',
    marked('kind=frames; channel=0; cause=6; reason="Unreadable."'),
    /in words this page can't read/,
  ],
  [
    '`frames`, naming no channel',
    marked('kind=frames; cause=8; reason="Unreadable."'),
    /in words this page can't read/,
  ],
])('a %s answer to the streaming upload ends as on the batch path', async (_kind, contentType, told) => {
  const page = (await openWithUpload(1))(0)
  world.uploadAnswers = [{ status: 400, contentType: [contentType] }]
  void page.channel.send('on the upload').catch(() => {})
  await advance(5_000)
  expect(page.ended).toHaveLength(1)
  expect(page.ended[0]?.err?.message).toMatch(told)
})
