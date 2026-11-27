// The twins, on the real Node server, of ClientConnection.marked.spec.ts's rows on the scripted server: what the page
// does with Telefunc's own refusals of its SSE requests.
// Each refusal is the real server's own, made by what the page sends; a fetch wrapper stands for what is between them:
// a page that breaks the protocol (a body cut or unframed), a proxy that drops the wire version from the URL, and one
// that rewrites the refusal's `Content-Type` into another form a header may take. The rows of a `frames` refusal that
// names a channel have their twins in ../framesRefusal.spec.ts.

import { expect, test } from 'vitest'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'
import { encodeU32 } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import {
  cutIn,
  delay,
  findText,
  open,
  sends,
  textOf,
  until,
  uploads,
  useRealServer,
  withoutWire,
  world,
} from './realServer.fixture.js'

useRealServer(['sse', 'ws'])

test("Telefunc's own 409 to a batch POST tells the page that the server was updated", async () => {
  const [page] = await open(1)
  if (!page) throw new Error('no page')
  world.hooks = [{ kind: 'post', text: 'hello', url: withoutWire }]
  sends(page.page, 'hello')
  await until(() => page.ended.length > 0)
  expect(page.ended[0]?.message).toMatch(/server was updated since this page loaded/)
  expect(world.requests.filter(({ status }) => status === 409)).toHaveLength(1)
})

test.each([
  ['as the server sends it', (contentType: string) => contentType],
  ['with a charset added', (contentType: string) => contentType.replace('kind=retry', 'kind=retry; charset=utf-8')],
  [
    'with upper-case names',
    (contentType: string) =>
      contentType.replace('text/vnd.telefunc.refusal', 'TEXT/VND.TELEFUNC.REFUSAL').replace('kind=', 'KIND='),
  ],
  ['with its kind quoted', (contentType: string) => contentType.replace('kind=retry', 'kind="retry"')],
  ['after another value, the last one counting', (contentType: string) => `text/html, ${contentType}`],
])(
  'a `retry` refusal %s, twice running, is a loss: the channel goes on and its data arrives once on the wire after',
  async (_how, rewrite) => {
    const [page] = await open(1)
    if (!page) throw new Error('no page')
    const retry = { kind: 'post', text: 'again', body: cutIn('again'), contentType: rewrite } as const
    world.hooks = [retry, retry]
    sends(page.page, 'again')
    await until(() => page.got.length > 0)
    await delay(300)
    const refused = world.requests.filter(({ status }) => status === 400)
    expect(refused.map(({ contentType }) => contentType)).toEqual([
      expect.stringMatching(/^text\/vnd\.telefunc\.refusal; kind=retry; /),
      expect.stringMatching(/^text\/vnd\.telefunc\.refusal; kind=retry; /),
    ])
    expect(page.ended).toEqual([])
    expect(page.got).toEqual(['again'])
  },
)

test('a marked refusal followed by an unmarked value is read as unmarked: refused twice, the channel ends naming it', async () => {
  const [page] = await open(1)
  if (!page) throw new Error('no page')
  const refused = {
    kind: 'post',
    text: 'refused',
    body: cutIn('refused'),
    contentType: (contentType: string) => `${contentType}, text/html`,
  } as const
  world.hooks = [refused, refused]
  sends(page.page, 'refused')
  await until(() => page.ended.length > 0)
  expect(page.ended[0]?.message).toMatch(/refused with 400 .*, text\/html, again after a reconnect/)
  expect(page.got).toEqual([])
})

test("a `frames` refusal of a frame over the server's limit ends its channel once, and leaves the size of later POSTs where it was", async () => {
  const [refused, other] = await open(2)
  if (!refused || !other) throw new Error('no pages')
  const big = 'x'.repeat(100_000)
  // The refused POST's size: the page may end the channel from the event stream before its answer comes, and drop it.
  let refusedBytes = 0
  world.hooks = [
    {
      kind: 'post',
      text: big,
      body: (bytes) => {
        refusedBytes = bytes.byteLength
        const found = findText(bytes, big)
        if (found === null) return [bytes]
        // The length says more than the limit; only the header follows.
        const head = encode.close(found.ix, 0, found.seq + 1).subarray(0, 7)
        return [bytes.subarray(0, found.end), encodeU32(WIRE_MAX_RAW_FRAME_BYTES + 1), head]
      },
    },
  ]
  sends(refused.page, big)
  await until(() => refused.ended.length > 0)
  const before = world.requests.length
  // Many texts at once: their POSTs are as large as the page's own sizing makes them, not halved below the refused one.
  for (let i = 0; i < 400; i++) sends(other.page, 'y'.repeat(900))
  await until(() => other.got.length === 400)
  const after = world.requests.slice(before).filter(({ kind, status }) => kind === 'post' && status === 200)
  expect(Math.max(...after.map(({ bytes }) => bytes))).toBeGreaterThan(refusedBytes / 2)
  expect(refused.ended.map(({ message }) => message)).toEqual([expect.stringMatching(/over the server's frame limit/)])
  expect(other.ended).toEqual([])
})

/** The upload's chunks, its body ended after the frame that carries `text`, with the length of a frame that never
 *  comes: the server reads the body to its end in the middle of a frame. */
function uploadCutAfter(text: string) {
  return (body: ReadableStream<Uint8Array>) =>
    body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform: (chunk, controller) => {
          controller.enqueue(chunk)
          if (textOf(new Uint8Array(chunk), text) === null) return
          controller.enqueue(encodeU32(100))
          controller.terminate()
        },
      }),
    )
}

/** One channel over SSE with the streaming upload taken, opened, and its upload started. */
async function openWithUpload() {
  world.uploads = true
  const [page] = await open(1)
  if (!page) throw new Error('no page')
  await until(() => uploads() === 1)
  return page
}

test('a `retry` answer to the streaming upload is a loss that keeps it: the next wire uploads again, and the data arrives once', async () => {
  world.hooks = [{ kind: 'upload', upload: uploadCutAfter('on the upload') }]
  const page = await openWithUpload()
  sends(page.page, 'on the upload')
  await until(() => page.got.length > 0 && uploads() === 2)
  await delay(300)
  expect(world.requests.find(({ kind, status }) => kind === 'upload' && status === 400)?.contentType).toMatch(
    /^text\/vnd\.telefunc\.refusal; kind=retry; /,
  )
  expect(page.ended).toEqual([])
  expect(uploads()).toBe(2)
  expect(page.got).toEqual(['on the upload'])
})
