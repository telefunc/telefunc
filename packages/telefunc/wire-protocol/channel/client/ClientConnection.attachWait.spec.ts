// What ends a page's wait for an answer: only the answer, or the wait's deadline. A wire that stays open and says
// something else meanwhile (an SSE comment, a PONG) neither answers nor extends the wait.
//
// A virtual clock runs the real deadlines: each test states when the page gives up, with and without the chatter.

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import {
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_TRANSPORT,
  RECONCILE_FLOOR_BYTES_PER_MS,
  RECONCILE_TIMEOUT_MS,
  SSE_RECONCILE_DEADLINE_MS,
  UPGRADE_HANDOFF_JOIN_TIMEOUT_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../../constants.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { decodeU32 } from '../../wire/envelope.js'
import { decode, encode, type DecodedFrame } from '../../wire/frame.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { TAG } from '../../wire/tags.js'
import { ClientChannel } from './ClientChannel.js'

const encoder = new TextEncoder()

function sseResponse() {
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null
  const stream = new ReadableStream<Uint8Array>({
    start: (c) => {
      controller = c
    },
    cancel: () => {
      controller = null
    },
  })
  const push = (text: string) => controller?.enqueue(encoder.encode(text))
  return {
    response: new Response(stream, { headers: { 'Content-Type': 'text/event-stream' } }),
    comment: () => push(': still here\n\n'),
    frame: (frame: Uint8Array<ArrayBuffer>) => push(`data: ${uint8ArrayToBase64url(frame)}\n\n`),
    end: () => controller?.close(),
  }
}

/** The frames of an SSE POST's body, its metadata first. */
async function postFrames(
  body: Blob,
): Promise<{ metadata: { streamResponse?: boolean }; frames: DecodedFrame[]; sizes: number[] }> {
  const buf = new Uint8Array(await body.arrayBuffer())
  const raw: Uint8Array<ArrayBuffer>[] = []
  for (let off = 0; off + 4 <= buf.length; ) {
    const len = decodeU32(buf.subarray(off, off + 4))
    raw.push(buf.slice(off + 4, off + 4 + len))
    off += 4 + len
  }
  const [head, ...rest] = raw
  return {
    metadata: JSON.parse(new TextDecoder().decode(head)),
    frames: rest.map((frame) => decode(frame)),
    sizes: rest.map((frame) => frame.byteLength),
  }
}

function reconciled(ix: number, upgradeId?: string) {
  return encode.reconciled({
    sessionId: crypto.randomUUID(),
    open: [{ ix, lastSeq: 0 }],
    reconnectTimeout: 60_000,
    idleTimeout: 60_000,
    pingInterval: 5_000,
    maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    creditWindow: 1_000_000,
    sseFlushThrottle: 300,
    ssePostIdleFlushDelay: 50,
    transports: ['sse', 'ws'],
    ...(upgradeId === undefined ? {} : { upgradeId }),
  })
}

class FakeWebSocket {
  static OPEN = 1
  static made: FakeWebSocket[] = []
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((ev: { data: ArrayBuffer }) => void) | null = null
  binaryType = 'blob'
  readyState = 1
  readonly openedAt = Date.now()
  closedAt: number | null = null
  onFrame: (frame: DecodedFrame) => void = () => {}

  constructor(url: string) {
    // A page an earlier test left behind reaches nothing.
    if (!url.includes(new URL(telefuncUrl).pathname)) {
      this.readyState = 3
      queueMicrotask(() => this.onclose?.())
      return
    }
    FakeWebSocket.made.push(this)
    queueMicrotask(() => this.onopen?.())
  }
  send(data: ArrayBuffer | Uint8Array): void {
    this.onFrame(decode(new Uint8Array(data instanceof Uint8Array ? data : new Uint8Array(data))))
  }
  close(): void {
    if (this.readyState === 3) return
    this.readyState = 3
    this.closedAt = Date.now()
    this.onclose?.()
  }
  deliver(frame: Uint8Array<ArrayBuffer>): void {
    if (this.readyState === 1) this.onmessage?.({ data: frame.buffer })
  }
}

type Open = { at: number; wire: ReturnType<typeof sseResponse> }

/** `value`, which the page must have produced by now: `what` says what it is. */
function defined<T>(value: T | null | undefined, what: string): T {
  if (value === null || value === undefined) throw new Error(`Expected ${what}`)
  return value
}

let opens: Open[] = []
/** The size of each RECONCILE the page sent this test's SSE server. */
let reconcileSizes: number[] = []
let channel: ClientChannel<unknown, unknown> | null = null
/** This test's server: a page an earlier test left behind reaches nothing. */
let telefuncUrl = ''

/** `fetch` for this test's server only; any other request fails as on a network error. */
function stubFetch(handle: (init: RequestInit) => Promise<Response>): void {
  vi.stubGlobal('fetch', async (url: string, init: RequestInit) => {
    if (!String(url).startsWith(telefuncUrl)) throw new TypeError('Failed to fetch')
    return await handle(init)
  })
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date', 'performance'] })
  opens = []
  reconcileSizes = []
  telefuncUrl = `http://telefunc.invalid/${crypto.randomUUID()}/_telefunc`
  FakeWebSocket.made = []
  vi.stubGlobal('WebSocket', FakeWebSocket)
})

afterEach(() => {
  channel?.abort()
  channel = null
  for (const { wire } of opens) wire.end()
  vi.unstubAllGlobals()
  vi.useRealTimers()
})

/** A page's channel over `transports`, at virtual time 0. */
function openChannel(transports: ('sse' | 'ws')[]): ClientChannel<unknown, unknown> {
  const opened = new ClientChannel({
    channelId: crypto.randomUUID(),
    connection: { telefuncUrl, transports, connectionKey: crypto.randomUUID() },
  })
  void opened.opened.catch(() => {})
  channel = opened
  return opened
}

/** An SSE server that opens the event stream and never answers its RECONCILE. Any other POST gets a 200. */
function stubSseServer(): void {
  stubFetch(async (init) => {
    if (!(init.body instanceof Blob)) throw new TypeError('Failed to fetch')
    const { metadata, frames, sizes } = await postFrames(init.body)
    for (const [i, frame] of frames.entries()) if (frame.tag === TAG.RECONCILE) reconcileSizes.push(sizes[i] ?? 0)
    if (!metadata.streamResponse) return new Response(null)
    const open: Open = { at: Date.now(), wire: sseResponse() }
    opens.push(open)
    return open.wire.response
  })
}

describe('a wire that never answers the RECONCILE is given up at its deadline, whatever else it says', () => {
  test.each([
    ['trickles a comment each second', true],
    ['says nothing', false],
  ])('an SSE open whose event stream %s', async (_, comments) => {
    stubSseServer()
    const page = openChannel(['sse'])
    await vi.advanceTimersByTimeAsync(SSE_RECONCILE_DEADLINE_MS)
    expect(opens).toHaveLength(1)
    const first = defined(opens[0], 'the open')
    for (let second = 1; second <= 15; second++) {
      if (comments) first.wire.comment()
      await vi.advanceTimersByTimeAsync(1_000)
    }
    // The page leaves the open at the RECONCILE deadline, which over SSE also gives the RECONCILE and its RECONCILED
    // their time at the floor, and opens another after the first reconnect's delay.
    const deadline =
      RECONCILE_TIMEOUT_MS + (2 * defined(reconcileSizes[0], 'the RECONCILE')) / RECONCILE_FLOOR_BYTES_PER_MS
    const next = defined(opens[1], 'the page to open another SSE connection')
    expect(next.at - first.at).toBeGreaterThanOrEqual(deadline)
    expect(next.at - first.at).toBeLessThanOrEqual(
      deadline + CHANNEL_RECONNECT_INITIAL_DELAY_MS + SSE_RECONCILE_DEADLINE_MS,
    )
    expect(page.isClosed).toBe(false)
  })

  test.each([
    ['answers each PING and sends a PONG each second', true],
    ['says nothing', false],
  ])('a WebSocket that opens and %s', async (_, pongs) => {
    vi.stubGlobal('fetch', async () => {
      throw new TypeError('Failed to fetch')
    })
    const page = openChannel(['ws'])
    await vi.advanceTimersByTimeAsync(0)
    expect(FakeWebSocket.made).toHaveLength(1)
    const socket = defined(FakeWebSocket.made[0], 'the WebSocket')
    socket.onFrame = (frame) => {
      if (pongs && frame.tag === TAG.PING) socket.deliver(encode.pong())
    }
    for (let second = 1; second <= 15; second++) {
      if (pongs) socket.deliver(encode.pong())
      await vi.advanceTimersByTimeAsync(1_000)
    }
    // The page leaves the socket at the RECONCILE deadline, which its PONGs don't renew, or, where it answers no PING,
    // at the heartbeat's deadline, its interval the shortest before a first RECONCILED; it opens another after the first
    // reconnect's delay.
    const leftAt = pongs ? RECONCILE_TIMEOUT_MS : pingDeadline(1_000, CHANNEL_TRANSPORT.WS)
    const next = defined(FakeWebSocket.made[1], 'the page to open another WebSocket')
    expect(next.openedAt - socket.openedAt).toBeGreaterThanOrEqual(leftAt)
    expect(next.openedAt - socket.openedAt).toBeLessThanOrEqual(leftAt + CHANNEL_RECONNECT_INITIAL_DELAY_MS)
    expect(page.isClosed).toBe(false)
  })
})

describe('an attached SSE wire whose server stops answering is left at the pong deadline, whatever else it says', () => {
  test.each([
    ['trickles a comment each second', true],
    ['says nothing', false],
  ])('its event stream %s', async (_, comments) => {
    stubFetch(async (init) => {
      if (!(init.body instanceof Blob)) throw new TypeError('Failed to fetch')
      const { metadata, frames } = await postFrames(init.body)
      // A POST is taken, and its PING never answered.
      if (!metadata.streamResponse) return new Response(null)
      const open: Open = { at: Date.now(), wire: sseResponse() }
      opens.push(open)
      const reconcile = frames.find((frame) => frame.tag === TAG.RECONCILE)
      open.wire.frame(reconciled(reconcile?.tag === TAG.RECONCILE ? (reconcile.payload.open[0]?.ix ?? 0) : 0))
      return open.wire.response
    })
    const page = openChannel(['sse'])
    await vi.advanceTimersByTimeAsync(SSE_RECONCILE_DEADLINE_MS)
    const first = defined(opens[0], 'the open')
    for (let second = 1; second <= 20 && opens.length === 1; second++) {
      if (comments) first.wire.comment()
      await vi.advanceTimersByTimeAsync(1_000)
    }
    // The heartbeat starts with the RECONCILED; the page leaves the wire at its pong deadline and opens another.
    const deadline = pingDeadline(5_000, 'sse')
    const next = defined(opens[1], 'the page to open another SSE connection')
    expect(next.at - first.at).toBeGreaterThanOrEqual(deadline)
    expect(next.at - first.at).toBeLessThanOrEqual(
      deadline + CHANNEL_RECONNECT_INITIAL_DELAY_MS + SSE_RECONCILE_DEADLINE_MS,
    )
    expect(page.isClosed).toBe(false)
  })
})

describe("an upgrade's join that never gets the old wire's FIN gives up at its deadline", () => {
  test.each([
    ['delivers a PONG every 500 ms', true],
    ['says nothing', false],
  ])('the old SSE wire %s', async (_, pongs) => {
    const seen: { barrierAt: number | null } = { barrierAt: null }
    stubFetch(async (init) => {
      // No streaming upload: the page's frames go in batch POSTs, which the test reads.
      if (!(init.body instanceof Blob)) throw new TypeError('Failed to fetch')
      const { metadata, frames } = await postFrames(init.body)
      for (const frame of frames) {
        if (frame.tag !== TAG.BARRIER) continue
        seen.barrierAt = Date.now()
        // The server attaches the channel on the new wire at once; only the old wire's FIN never comes.
        const { upgradeId, open } = frame.payload
        setTimeout(() => FakeWebSocket.made[0]?.deliver(reconciled(open[0]?.ix ?? 0, upgradeId)), 0)
      }
      if (!metadata.streamResponse) return new Response(null)
      const open: Open = { at: Date.now(), wire: sseResponse() }
      opens.push(open)
      const reconcile = frames.find((frame) => frame.tag === TAG.RECONCILE)
      const ix = reconcile?.tag === TAG.RECONCILE ? (reconcile.payload.open[0]?.ix ?? 0) : 0
      open.wire.comment()
      open.wire.frame(reconciled(ix))
      return open.wire.response
    })
    openChannel(['sse', 'ws'])
    await vi.advanceTimersByTimeAsync(100)
    expect(opens).toHaveLength(1)
    expect(FakeWebSocket.made, 'the page probes a WebSocket once its SSE wire opened').toHaveLength(1)
    const probe = defined(FakeWebSocket.made[0], 'the probe')
    probe.onFrame = (frame) => {
      if (frame.tag === TAG.PING) probe.deliver(encode.pong())
      if (frame.tag === TAG.PREPARE) probe.deliver(encode.ready({ upgradeId: frame.payload.upgradeId }))
    }
    probe.deliver(encode.pong())
    const old = defined(opens[0], 'the SSE open').wire
    for (let tick = 1; tick <= 20 && probe.closedAt === null; tick++) {
      if (pongs) old.frame(encode.pong())
      await vi.advanceTimersByTimeAsync(500)
    }
    const barrier = defined(seen.barrierAt, 'the page to send its barrier on the old wire')
    const left = defined(probe.closedAt, 'the page to give the join up and leave the new wire')
    // The flip follows the barrier's POST within the same turn of the virtual clock.
    expect(left - barrier).toBeGreaterThanOrEqual(UPGRADE_HANDOFF_JOIN_TIMEOUT_MS)
    expect(left - barrier).toBeLessThan(UPGRADE_HANDOFF_JOIN_TIMEOUT_MS + 500)
  })
})
