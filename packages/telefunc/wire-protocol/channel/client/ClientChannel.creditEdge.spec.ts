// A page's sends over SSE in batch POSTs, at the edge of the server's credit: the REAL ClientChannel and connection
// against a scripted server that refuses a streaming upload, so every frame goes in a batch POST. The clock is faked. A
// batch POST that isn't full (64 KiB at first) waits 50 ms for more frames to join it; the frame after which its
// channel is out of credit goes at once, since nothing the channel sends next can join it before the server has it and
// grants more. The server grants 32 KiB, so a POST is not full when its channel runs out.

import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { ClientChannel } from './ClientChannel.js'
import { decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { decodeU32 } from '../../wire/envelope.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { CHANNEL_TRANSPORT, WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'

type Post = { startedAt: number; opensWire: boolean; dataFrames: number[] }

/** A server that answers the page's SSE requests: each batch POST is recorded as it starts, with the byte length of
 *  each data frame in it, and each RECONCILE is answered with a RECONCILED that opens its channels. */
function scriptedServer(creditWindow: number) {
  const posts: Post[] = []
  const encoder = new TextEncoder()
  let push = (_frame: Uint8Array<ArrayBuffer>) => {}
  let end = () => {}
  const sessionId = crypto.randomUUID()
  const reconciled = (open: { ix: number }[]) =>
    encode.reconciled({
      sessionId,
      open: open.map(({ ix }) => ({ ix, lastSeq: 0 })),
      reconnectTimeout: 60_000,
      idleTimeout: 60_000,
      pingInterval: 100_000,
      maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
      creditWindow,
      sseFlushThrottle: 300,
      ssePostIdleFlushDelay: 50,
      transports: ['sse'],
    })
  const fetchImpl = async (_input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const body = init?.body
    // No streaming upload: the page sends in batch POSTs.
    if (!(body instanceof Blob)) throw new TypeError('streaming uploads are refused')
    const startedAt = performance.now()
    const [metadata, ...frames] = lengthPrefixed(new Uint8Array(await body.arrayBuffer()))
    const decoded = frames.map((raw) => ({ frame: decode(raw), byteLength: raw.byteLength }))
    const opens = decoded.flatMap(({ frame }) => (frame.tag === TAG.RECONCILE ? frame.payload.open : []))
    const parsed: unknown = JSON.parse(new TextDecoder().decode(metadata))
    const streamResponse =
      typeof parsed === 'object' && parsed !== null && 'streamResponse' in parsed && parsed.streamResponse === true
    const dataFrames = decoded.filter(({ frame }) => frame.tag === TAG.TEXT || frame.tag === TAG.BINARY)
    posts.push({ startedAt, opensWire: streamResponse, dataFrames: dataFrames.map(({ byteLength }) => byteLength) })
    if (streamResponse) {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          push = (frame) => controller.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`))
          end = () => controller.close()
          controller.enqueue(encoder.encode(': open\n\n'))
        },
      })
      push(reconciled(opens))
      return new Response(stream, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
    }
    if (opens.length > 0) push(reconciled(opens))
    return new Response('', { status: 200 })
  }
  return { posts, fetchImpl, endStream: () => end() }
}

function lengthPrefixed(buf: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer>[] {
  const out: Uint8Array<ArrayBuffer>[] = []
  for (let off = 0; off + 4 <= buf.length; ) {
    const length = decodeU32(buf, off)
    out.push(buf.subarray(off + 4, off + 4 + length))
    off += 4 + length
  }
  return out
}

let server = scriptedServer(0)

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'performance', 'Date'] })
})
afterEach(() => {
  vi.clearAllTimers()
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

/** A page channel over SSE on a connection of its own, opened by a scripted server's RECONCILED that grants
 *  `creditWindow` bytes. */
async function openChannel(creditWindow = 32_768) {
  server = scriptedServer(creditWindow)
  vi.stubGlobal('fetch', server.fetchImpl)
  const channel = new ClientChannel<string, never>({
    channelId: crypto.randomUUID(),
    connection: {
      transports: [CHANNEL_TRANSPORT.SSE],
      telefuncUrl: 'http://test.local/_telefunc',
      connectionKey: crypto.randomUUID(),
    },
  })
  let opened = false
  void channel.opened.then(() => (opened = true))
  for (let i = 0; i < 100 && !opened; i++) await vi.advanceTimersByTimeAsync(10)
  expect(opened).toBe(true)
  // The wire's own POSTs, before this test's sends, go first.
  await vi.advanceTimersByTimeAsync(1_000)
  return channel
}

/** The POSTs with data that started since the `from`th, each with how long after `since` it started and the byte
 *  length of each data frame in it. */
function postsSince(from: number, since: number) {
  return server.posts
    .slice(from)
    .filter(({ dataFrames }) => dataFrames.length > 0)
    .map(({ startedAt, opensWire, dataFrames }) => ({ after: startedAt - since, opensWire, dataFrames }))
}

test('an awaited sender that runs out of credit has the POST of its last frame start at once', async () => {
  const channel = await openChannel()
  const from = server.posts.length
  const since = performance.now()
  // 8 KiB frames, each send awaited: the fourth leaves the channel out of credit, and its send waits for more.
  const chunk = new Uint8Array(8_192)
  let settled = 0
  void (async () => {
    for (let i = 0; i < 5; i++) {
      await channel.sendBinary(chunk)
      settled++
    }
  })()
  await vi.advanceTimersByTimeAsync(10)
  expect(settled).toBe(3)
  const frame = chunk.byteLength + 7
  expect(postsSince(from, since)).toEqual([{ after: 0, opensWire: false, dataFrames: [frame, frame, frame, frame] }])
})

test('a text whose frame fits the credit left goes in a batch POST, as data: its size is the encoded one', async () => {
  const channel = await openChannel(65_536)
  const from = server.posts.length
  const since = performance.now()
  // 32,768 ASCII characters: a 32,777-byte frame, against 65,536 bytes of credit and 100 messages. Three bytes per
  // character, the most UTF-8 takes, would say it doesn't fit.
  void channel.send('x'.repeat(32_768))
  await vi.advanceTimersByTimeAsync(10)
  expect(postsSince(from, since)).toEqual([])
  await vi.advanceTimersByTimeAsync(400)
  expect(postsSince(from, since)).toEqual([{ after: 50, opensWire: false, dataFrames: [32_777] }])
})

test("a page's small sends with credit left share one POST, after the batch timer", async () => {
  const channel = await openChannel()
  const from = server.posts.length
  const since = performance.now()
  for (let i = 0; i < 10; i++) void channel.send(`small ${i}`)
  await vi.advanceTimersByTimeAsync(10)
  expect(postsSince(from, since)).toEqual([])
  await vi.advanceTimersByTimeAsync(400)
  expect(postsSince(from, since)).toMatchObject([{ after: 50, opensWire: false }])
  expect(postsSince(from, since)[0]?.dataFrames.length).toBe(10)
})

test('a lossy send after which the channel is out of credit goes at once', async () => {
  const channel = await openChannel()
  const from = server.posts.length
  const since = performance.now()
  void channel.sendBinary(new Uint8Array(32_768), { lossy: true })
  await vi.advanceTimersByTimeAsync(10)
  expect(postsSince(from, since)).toEqual([{ after: 0, opensWire: false, dataFrames: [32_775] }])
})

test('a lossy replacement that leaves the channel out of credit, made while the gate holds its frame for a reconnect, goes as the gate releases it', async () => {
  const channel = await openChannel()
  server.endStream()
  await vi.advanceTimersByTimeAsync(1)
  const from = server.posts.length
  const since = performance.now()
  void channel.send('small', { lossy: true })
  // It takes the place of the one before, which waits with it in the gate for the reconnect, and counts the bytes it
  // adds. The first wire went before it read a PONG, so its upload's failure went with it: the reopened wire uploads
  // again, the gate releases the frame onto that upload, and as the server refuses it at once the frame, written and
  // unacknowledged, goes again in a batch POST at once.
  void channel.sendBinary(new Uint8Array(32_768), { lossy: true })
  await vi.advanceTimersByTimeAsync(5_000)
  const reopened = server.posts.slice(from).find(({ opensWire }) => opensWire)
  expect(reopened).toBeDefined()
  const reopenedAfter = (reopened?.startedAt ?? 0) - since
  expect(postsSince(from, since)).toEqual([{ after: reopenedAfter, opensWire: false, dataFrames: [32_775] }])
})
