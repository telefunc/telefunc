// A page's streaming upload over SSE carries nothing until the server acknowledges it: the wire sends in batch POSTs
// meanwhile, and moves onto the upload once the acknowledgement came and no POST is out. A probe that fails to fetch is
// dropped and nothing is remembered; an answer about the request keeps the connection on batch POSTs, read as a batch
// POST's answer is.

import { expect, test, vi } from 'vitest'
import { encode } from '../../wire/frame.js'
import {
  advance,
  openChannel,
  serveSse,
  useScriptedServer,
  world,
  type UploadScript,
} from './scriptedServer.fixture.js'

useScriptedServer()

/** A page over SSE whose uploads meet `scripts` in order, those after them taken. */
function page(...scripts: UploadScript[]) {
  serveSse()
  world.uploads = true
  world.uploadScripts = scripts
  return openChannel(['sse'])
}

/** The latest wire is lost, and the page reconnects. */
async function loseWire(): Promise<void> {
  world.holdOpens = false
  world.streams.at(-1)?.end()
  await advance(5_000)
}

const dataPosts = () => world.posts.filter(({ data }) => data).length
const seqsGot = () => world.server.got.map(({ seq }) => seq)
const seqsUploaded = () => world.uploaded.map(({ seq }) => seq)

test('an upload refused before its acknowledgement keeps the answered refusal', async () => {
  serveSse()
  const serve = globalThis.fetch
  let uploads = 0
  vi.stubGlobal('fetch', (input: RequestInfo | URL, init?: RequestInit) => {
    if (init?.body instanceof ReadableStream) {
      uploads++
      return Promise.resolve(new Response('upload refused', { status: 413 }))
    }
    return serve(input, init)
  })
  openChannel(['sse'])
  await advance(10)
  world.streams[0]?.end()
  await advance(5_000)
  expect([world.opens, uploads]).toEqual([2, 1])
})

test("an arrival PONG and an old PING's answer, then a lost wire: the next wire uploads", async () => {
  serveSse()
  world.uploads = true
  world.uploadScripts = [{ tag: 'fails', afterMs: 2_100, answersAfterMs: 500 }]
  world.controlAnswers = [{ tag: 'slow', ms: 1_000 }]
  openChannel(['sse'])
  await advance(2_550)
  world.streams[0]?.frame(encode.pong())
  await advance(10)
  world.streams[0]?.end()
  await advance(5_000)
  expect([world.opens, world.uploadPosts]).toEqual([2, 2])
})

test('behind a proxy that holds the upload, the first message goes in a batch POST at once', async () => {
  const { channel } = page({ tag: 'held' })
  await advance(200)
  void channel.send('first').catch(() => {})
  await advance(200)
  expect([dataPosts(), seqsGot()]).toEqual([1, [1]])
})

test('a POST out as the acknowledgement comes: what is sent meanwhile goes on the upload after it, each message once and in order', async () => {
  const { channel } = page({ tag: 'taken', ack: 'held' })
  world.postAnswers = [{ tag: 'slow', ms: 1_000 }]
  await advance(200)
  void channel.send('on the POST').catch(() => {})
  await advance(100)
  world.heldAcks[0]?.()
  await advance(10)
  void channel.send('while the POST is out').catch(() => {})
  void channel.send('while the POST is out, too').catch(() => {})
  await advance(1_500)
  void channel.send('after the move').catch(() => {})
  await advance(200)
  expect([dataPosts(), seqsGot(), seqsUploaded()]).toEqual([1, [1, 2, 3, 4], [2, 3, 4]])
})

test('an acknowledgement that comes 5 s late, as behind bytes queued toward the page, still moves the wire', async () => {
  const { channel } = page({ tag: 'taken', ackAfterMs: 5_000 })
  await advance(200)
  void channel.send('before').catch(() => {})
  await advance(5_500)
  void channel.send('after').catch(() => {})
  await advance(200)
  expect([dataPosts(), seqsGot(), seqsUploaded()]).toEqual([1, [1, 2], [2]])
})

test('an acknowledgement on a wire the page left moves nothing: the next wire sends in its own batch POSTs', async () => {
  const { channel } = page({ tag: 'taken', ack: 'held' }, { tag: 'held' })
  world.postAnswers = [{ tag: 'lost' }]
  await advance(200)
  // Its POST gets no answer: the page leaves the wire, whose event stream it still reads until the next wire opens.
  world.holdOpens = true
  void channel.send('lost').catch(() => {})
  await advance(1_000)
  world.heldAcks[0]?.()
  await advance(10)
  world.holdOpens = false
  world.heldOpens.at(-1)?.()
  await advance(1_000)
  void channel.send('next').catch(() => {})
  await advance(200)
  expect([world.opens, seqsUploaded(), seqsGot().at(-1)]).toEqual([2, [], 2])
})

test("a proxy's 502 to the probe, as while it reloads, is the wire's loss and is forgotten: the next wire uploads", async () => {
  const { channel } = page({ tag: 'answered', status: 502 })
  await advance(5_000)
  void channel.send('after').catch(() => {})
  await advance(200)
  expect([world.opens, world.uploadPosts, seqsUploaded()]).toEqual([2, 2, [1]])
})

test("a proxy's 502 to an upload carrying messages is the wire's loss and is forgotten: the next wire uploads", async () => {
  const { channel } = page()
  await advance(200)
  world.uploadAnswers = [{ status: 502, contentType: ['text/html'] }]
  void channel.send('refused').catch(() => {})
  await advance(5_000)
  void channel.send('after').catch(() => {})
  await advance(200)
  expect([world.opens, world.uploadPosts, seqsUploaded()]).toEqual([2, 2, [1, 2]])
})

test("an upload refused for its size (413) once it carried messages: the refusal is the connection's, and the next wire doesn't upload", async () => {
  const { channel } = page()
  await advance(200)
  world.uploadAnswers = [{ status: 413, contentType: ['text/html'] }]
  void channel.send('too large').catch(() => {})
  await advance(5_000)
  void channel.send('after').catch(() => {})
  await advance(200)
  expect([world.opens, world.uploadPosts]).toEqual([2, 1])
})

test('a probe answered without its acknowledgement, as by something that took its body whole: no wire uploads again', async () => {
  page({ tag: 'answered', status: 200 })
  await advance(200)
  await loseWire()
  await loseWire()
  expect([world.opens, world.uploadPosts]).toEqual([3, 1])
})

test('HTTP/1.1, which rejects a body that streams: nothing is remembered, each wire probes again and sends in batch POSTs', async () => {
  const { channel } = page()
  world.uploads = false
  await advance(3_500)
  await loseWire()
  await loseWire()
  void channel.send('after').catch(() => {})
  await advance(200)
  expect([world.opens, world.uploadPosts, dataPosts()]).toEqual([3, 3, 1])
})
