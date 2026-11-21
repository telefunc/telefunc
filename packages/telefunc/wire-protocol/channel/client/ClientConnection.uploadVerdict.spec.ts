// Whether a page whose streaming upload failed sends in batch POSTs after it reconnects. A failure the network caused
// goes with its wire, and the next wire uploads again. A failure of the environment (HTTP/1.1, which rejects a body
// that streams; a proxy that holds a request's body until it ends) is kept for the connection once its wire read a PONG
// that answers a PING sent after the failure, as is an answered refusal of the upload.

import { expect, test } from 'vitest'
import {
  advance,
  openChannel,
  serveSse,
  useScriptedServer,
  world,
  type UploadScript,
} from './scriptedServer.fixture.js'

useScriptedServer()

/** A page over SSE whose first upload meets `script`, the uploads after it taken; `held`: its first open's answer
 *  writes nothing on its stream until the test calls `world.heldOpens[0]`. */
function firstWire(script: UploadScript, { held = false } = {}) {
  serveSse()
  world.uploadScripts = [script]
  world.uploads = true
  world.holdOpens = held
  return openChannel(['sse'])
}

/** The latest wire is lost, and the page reconnects. */
async function loseWire(): Promise<void> {
  world.holdOpens = false
  world.streams.at(-1)?.end()
  await advance(5_000)
}

/** The streaming uploads the page sent, and the data POSTs three sends 500 ms apart take. */
async function afterReconnect(page: ReturnType<typeof openChannel>) {
  const before = world.posts.filter(({ data }) => data).length
  for (let i = 0; i < 3; i++) {
    void page.channel.send(`after ${i}`).catch(() => {})
    await advance(500)
  }
  return { uploads: world.uploadPosts, dataPosts: world.posts.filter(({ data }) => data).length - before }
}

test("an upload that fails, its wire lost before it read anything: the failure was the network's, and the next wire uploads, its sends taking no POST", async () => {
  const page = firstWire({ tag: 'fails', afterMs: 0 }, { held: true })
  await advance(10)
  await loseWire()
  expect(await afterReconnect(page)).toEqual({ uploads: 2, dataPosts: 0 })
})

test('an upload that fails, then its wire reads what the server sent before the failure, then is lost: the next wire uploads', async () => {
  const page = firstWire({ tag: 'fails', afterMs: 0 }, { held: true })
  await advance(10)
  // The open's answer, its RECONCILED, read after the upload failed: bytes in flight before it.
  world.heldOpens[0]?.()
  await advance(10)
  await loseWire()
  expect(await afterReconnect(page)).toEqual({ uploads: 2, dataPosts: 0 })
})

test('an upload that fails, then its wire reads a PONG answering a PING sent before the failure, then is lost: the next wire uploads', async () => {
  // The server reads the upload and answers it 500 ms later; the upload fails at 2.1 s, past the PING the page wrote
  // on it at 2 s and before that PING's PONG. The copy the page sends again by POST is answered only after the wire
  // went: nothing sent after the failure is answered on it.
  const page = firstWire({ tag: 'fails', afterMs: 2_100, answersAfterMs: 500 })
  world.controlAnswers = [{ tag: 'slow', ms: 1_000 }]
  await advance(2_550)
  await loseWire()
  expect(await afterReconnect(page)).toEqual({ uploads: 2, dataPosts: 0 })
})

test("HTTP/1.1, whose fetch rejects a body that streams at once: once the wire read a PONG to a PING sent after, the verdict is the connection's, and no reconnect uploads again", async () => {
  const page = firstWire({ tag: 'fails', afterMs: 0 })
  world.uploads = false
  // Its first PING went on the upload, which the server never read: its copy sent again is answered first, then the
  // PONG to the next PING keeps the verdict, within one of the fixture's 2 s intervals and its hold.
  await advance(3_500)
  await loseWire()
  await loseWire()
  expect(await afterReconnect(page)).toEqual({ uploads: 1, dataPosts: 3 })
})

test("a proxy that holds the upload's body: unacknowledged in 3 s on a wire that goes on, then a PONG to a PING sent after, the verdict is the connection's, and no reconnect uploads again", async () => {
  const page = firstWire({ tag: 'held' })
  world.uploads = false
  // The PINGs written on the held body in its 3 s go again once it is given up; the PONG after theirs keeps the verdict.
  await advance(6_000)
  await loseWire()
  await loseWire()
  expect(await afterReconnect(page)).toEqual({ uploads: 1, dataPosts: 3 })
})

test("an upload refused for its size (413): the refusal is the connection's, and the next wire doesn't upload", async () => {
  const page = firstWire({ tag: 'taken' })
  await advance(1_000)
  world.uploadAnswers = [{ status: 413, contentType: ['text/html'] }]
  void page.channel.send('too large').catch(() => {})
  await advance(5_000)
  expect(await afterReconnect(page)).toEqual({ uploads: 1, dataPosts: 3 })
})
