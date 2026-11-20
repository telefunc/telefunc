// What the page does with Telefunc's own refusals of its SSE requests, which say so in their media type with a kind
// (decisions/516-sse-carrier.md, step 6 at 13:08Z and 13:14Z): a REAL ClientChannel and its connection against a
// scripted server (scriptedServer.fixture.ts). The verdict is taken at the headers; no refusal's body is read.

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
  const opened = await open(2)
  const [refused, other] = [opened(0), opened(1)]
  world.postAnswers = [typed(400, marked('kind=frames; channel=0; reason="A \\"quoted\\" word and a back\\\\slash."'))]
  void refused.channel.send('refused').catch(() => {})
  await advance(3_000)
  expect(refused.ended.map(({ err }) => err?.message)).toEqual([
    'Channel closed: the server refused what the page sent on it: A "quoted" word and a back\\slash.',
  ])
  expect(other.ended).toEqual([])
})

test('a marked refusal followed by an unmarked value is read as unmarked: refused twice, the channel ends naming it', async () => {
  const page = (await open(1))(0)
  const contentType = [marked('kind=retry; reason="No wire."'), 'text/html']
  world.postAnswers = [typed(400, ...contentType), typed(400, ...contentType)]
  void page.channel.send('refused').catch(() => {})
  await advance(10_000)
  expect(page.ended[0]?.err?.message).toMatch(/refused with 400 .*, text\/html, again after a reconnect/)
})

test('a `frames` refusal naming a channel ends that channel at the first answer with its reason; the other goes on', async () => {
  const opened = await open(2)
  const [refused, other] = [opened(0), opened(1)]
  world.postAnswers = [typed(400, marked('kind=frames; channel=0; reason="A frame the server cannot take."'))]
  void refused.channel.send('unreadable').catch(() => {})
  await advance(3_000)
  expect(refused.ended).toHaveLength(1)
  expect(refused.ended[0]?.err?.message).toMatch(/A frame the server cannot take\./)
  void other.channel.send('goes on')
  await advance(3_000)
  expect(other.ended).toEqual([])
  expect(world.server.got.filter(({ ix }) => ix === 1)).toHaveLength(1)
  expect(world.server.got.filter(({ ix }) => ix === 0)).toEqual([])
})

test("a `frames` refusal naming one of a POST's channels, as of a frame over the limit, ends that one and no other: the other's data arrives", async () => {
  const opened = await open(2)
  const [other, refused] = [opened(0), opened(1)]
  world.postAnswers = [
    typed(400, marked('kind=frames; channel=1; reason="A frame is over the server\'s frame limit."')),
  ]
  void other.channel.send('goes on').catch(() => {})
  void refused.channel.send('x'.repeat(100_000)).catch(() => {})
  await advance(5_000)
  expect(refused.ended).toHaveLength(1)
  expect(refused.ended[0]?.err?.message).toMatch(/over the server's frame limit/)
  expect(other.ended).toEqual([])
  expect(world.server.got.filter(({ ix }) => ix === 0)).toHaveLength(1)
  expect(world.server.got.filter(({ ix }) => ix === 1)).toEqual([])
})

test("a `frames` refusal naming no channel, of a body the server can't frame, is counted against each of the POST's channels", async () => {
  const opened = await open(2)
  const pages = [opened(0), opened(1)]
  const unframed = typed(400, marked('kind=frames; reason="The server can\'t read the request\'s frames."'))
  // Refused twice running, as an unmarked refusal is counted: each channel ends.
  world.postAnswers = [unframed, unframed]
  for (const page of pages) void page.channel.send('unframed').catch(() => {})
  await advance(10_000)
  for (const page of pages) expect(page.ended[0]?.err?.message).toMatch(/can't read the request's frames/)
})

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

test("a `frames` refusal of a frame over the server's limit ends its channel once, and leaves the size of later POSTs where it was", async () => {
  const opened = await open(2)
  const [refused, other] = [opened(0), opened(1)]
  world.postAnswers = [typed(400, marked('kind=frames; reason="A frame is over the server\'s frame limit."'))]
  void refused.channel.send('x'.repeat(100_000)).catch(() => {})
  await advance(3_000)
  expect(refused.ended).toHaveLength(1)
  const refusedPost = defined(
    world.posts.find(({ status }) => status === 400),
    'the refused POST',
  )
  // Many texts at once: their POSTs are as large as the page's own sizing makes them, not halved below the refused one.
  for (let i = 0; i < 400; i++) void other.channel.send('y'.repeat(900)).catch(() => {})
  await advance(5_000)
  const after = world.posts.slice(world.posts.indexOf(refusedPost) + 1).filter(({ data }) => data)
  expect(Math.max(...after.map(({ bytes }) => bytes))).toBeGreaterThan(refusedPost.bytes / 2)
  expect(world.server.got.filter(({ ix }) => ix === 1)).toHaveLength(400)
  expect(refused.ended[0]?.err?.message).toMatch(/over the server's frame limit/)
})

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
  ['`frames`, naming the channel', marked('kind=frames; channel=0; reason="Unreadable."'), /Unreadable\./],
])('a %s answer to the streaming upload ends as on the batch path', async (_kind, contentType, told) => {
  const page = (await openWithUpload(1))(0)
  world.uploadAnswers = [{ status: 400, contentType: [contentType] }]
  void page.channel.send('on the upload').catch(() => {})
  await advance(5_000)
  expect(page.ended).toHaveLength(1)
  expect(page.ended[0]?.err?.message).toMatch(told)
})
