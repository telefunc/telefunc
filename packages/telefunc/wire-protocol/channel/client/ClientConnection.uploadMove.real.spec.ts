// The move from batch POSTs onto the streaming upload, on the real Node server and the real page: the upload carries
// nothing until the server acknowledged it, and takes the wire's messages only once the POST out was answered, so the
// server reads every message once and in order across the move.

import { expect, test } from 'vitest'
import { delay, open, sends, until, uploads, useRealServer, world } from './realServer.fixture.js'

useRealServer(['sse'])

/** The upload with its first chunk, its metadata, held `ms` on the way: the server acknowledges it that late. */
const lateMetadata = (ms: number) => (body: ReadableStream<Uint8Array>) => {
  let first = true
  return body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform: async (chunk, controller) => {
        if (first) await delay(ms)
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
    { kind: 'upload', upload: lateMetadata(300) },
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
