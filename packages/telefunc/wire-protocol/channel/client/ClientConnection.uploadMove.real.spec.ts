// The move from batch POSTs onto the streaming upload, on the real Node server and the real page: the upload carries
// nothing until the server acknowledged it, and takes the wire's messages only once the POST out was answered, so the
// server reads every message once and in order across the move.

import { expect, test } from 'vitest'
import { TAG } from '../../wire/tags.js'
import { delay, open, rewriting, sends, until, uploads, useRealServer, world } from './realServer.fixture.js'

useRealServer(['sse'])

/** Holds the upload's metadata until `wait` resolves. */
const lateMetadata = (wait: () => Promise<unknown>) => (body: ReadableStream<Uint8Array>) => {
  let first = true
  return body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform: async (chunk, controller) => {
        if (first) await wait()
        first = false
        controller.enqueue(chunk)
      },
    }),
  )
}

test('messages sent before, during and after the move onto the upload, a POST out as the acknowledgement comes, each arrive once and in order', async () => {
  world.uploads = true
  // The acknowledgement comes at about 300 ms; the POST carrying 'm1' reaches the server only at about 600 ms.
  world.hooks = [
    { kind: 'upload', upload: lateMetadata(() => delay(300)) },
    { kind: 'post', text: 'm1', delayMs: 600 },
  ]
  const [page] = await open(1)
  if (!page) throw new Error('no page')
  sends(page.page, 'm1')
  await delay(350)
  for (const message of ['m2', 'm3', 'm4', 'm5']) {
    sends(page.page, message)
    await delay(50)
  }
  await delay(500)
  for (const message of ['m6', 'm7', 'm8', 'm9', 'm10']) {
    sends(page.page, message)
    await delay(20)
  }
  await until(() => page.got.length >= 10)
  await delay(200)
  // 'm1' went in the POST its hook delayed; every other message went on the one upload.
  expect([world.posted, uploads(), page.ended]).toEqual([['m1'], 1, []])
  expect(page.got).toEqual(['m1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7', 'm8', 'm9', 'm10'])
})

test('an SSE upload waits out Retry-After before retrying a refused batch in order', async () => {
  world.uploads = true
  const metadata = Promise.withResolvers<void>()
  let acknowledged = false
  world.hooks = [
    {
      kind: 'open',
      stream: rewriting((frame) => {
        if (frame[0] === TAG.STREAM_REQUEST_OPEN_ACK) acknowledged = true
        return frame
      }).stream,
    },
    { kind: 'upload', upload: lateMetadata(() => metadata.promise) },
    {
      kind: 'post',
      text: 'first',
      refusal: new Response(null, { status: 413, headers: { 'Retry-After': '3' } }),
    },
  ]
  const [page] = await open(1)
  if (!page) throw new Error('no page')
  try {
    sends(page.page, 'first')
    sends(page.page, 'second')
    await until(() => world.requests.some(({ status }) => status === 413))
    sends(page.page, 'third')
    await delay(400)
    expect([world.posted, page.got]).toEqual([['first', 'second'], []])

    metadata.resolve()
    await until(() => acknowledged)
    sends(page.page, 'fourth')
    await delay(400)
    expect(page.got).toEqual([])

    await until(() => page.got.length >= 4)
    expect([world.posted, uploads(), page.ended]).toEqual([['first', 'second'], 1, []])
    expect(page.got).toEqual(['first', 'second', 'third', 'fourth'])
  } finally {
    metadata.resolve()
    page.page.abort()
  }
}, 8_000)
