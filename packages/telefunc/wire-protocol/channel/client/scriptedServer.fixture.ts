// A scripted Telefunc server for the stalled connection tests. A real ClientChannel reaches it over SSE batch POSTs.

export { advance, holdPage, openChannel, serveSse, useScriptedServer, world }

import { afterEach, beforeEach, vi } from 'vitest'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { decodeU32, parseSseRequestMetadata, type SseRequestMetadata } from '../../wire/envelope.js'
import { decode, encode, isSequencedFrame, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ClientChannel } from './ClientChannel.js'

const PING_INTERVAL_MS = 2_000
const RECONNECT_TIMEOUT_MS = 10_000
const encoder = new TextEncoder()

class Server {
  readonly confirmed = new Map<number, number>()
  /** The channels whose data it takes and never confirms. */
  readonly drops = new Set<number>()
  readonly got: { ix: number; seq: number }[] = []
  settings: { reconnectTimeout?: number } = {}

  answer(frame: DecodedFrame): Uint8Array<ArrayBuffer>[] {
    if (frame.tag === TAG.RECONCILE) {
      const { open } = frame.payload
      for (const { ix } of open) if (!this.confirmed.has(ix)) this.confirmed.set(ix, 0)
      return [this.reconciled(open.map(({ ix }) => ix)), ...this.messageWindows()]
    }
    if (frame.tag === TAG.PING) {
      const windows = [...this.confirmed].map(([ix, lastSeq]) => encode.window(ix, 1_000_000, lastSeq))
      return [...windows, ...this.messageWindows(), encode.pong()]
    }
    if (isSequencedFrame(frame)) {
      this.got.push({ ix: frame.index, seq: frame.seq })
      if (!this.drops.has(frame.index)) this.confirmed.set(frame.index, frame.seq)
    }
    return []
  }

  private messageWindows(): Uint8Array<ArrayBuffer>[] {
    return [...this.confirmed.keys()].map((ix) => encode.msgWindow(ix, 1_000_000))
  }

  private reconciled(ixes: number[]): Uint8Array<ArrayBuffer> {
    return encode.reconciled({
      sessionId: 'session',
      open: ixes.map((ix) => ({ ix, lastSeq: this.confirmed.get(ix) ?? 0 })),
      reconnectTimeout: this.settings.reconnectTimeout ?? RECONNECT_TIMEOUT_MS,
      idleTimeout: 600_000,
      pingInterval: PING_INTERVAL_MS,
      maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
      creditWindow: 1_000_000,
      sseFlushThrottle: 50,
      ssePostIdleFlushDelay: 10,
      transports: ['sse'],
    })
  }
}

/** An SSE event stream the test can hold before the server answers the open, or end to drop the connection. */
function eventStream() {
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null
  let held = false
  const stream = new ReadableStream<Uint8Array>({
    start: (c) => {
      controller = c
    },
    cancel: () => {
      controller = null
    },
  })
  return {
    response: new Response(stream, { headers: { 'Content-Type': 'text/event-stream' } }),
    frame: (frame: Uint8Array<ArrayBuffer>) => {
      if (!held) controller?.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`))
    },
    hold: () => {
      held = true
    },
    answer: () => {
      held = false
    },
    end: () => {
      controller?.close()
      controller = null
    },
  }
}

/** The frames of an SSE POST's body, its metadata first, read as the server reads them. */
async function postFrames(body: Blob): Promise<{ metadata: SseRequestMetadata; frames: DecodedFrame[] }> {
  const buf = new Uint8Array(await body.arrayBuffer())
  const raw: Uint8Array<ArrayBuffer>[] = []
  let off = 0
  while (off < buf.length) {
    if (off + 4 > buf.length) throw new Error('The POST body ends inside a frame length')
    const len = decodeU32(buf.subarray(off, off + 4))
    if (off + 4 + len > buf.length) throw new Error('The POST body ends inside a frame')
    raw.push(buf.slice(off + 4, off + 4 + len))
    off += 4 + len
  }
  const [head, ...rest] = raw
  const metadata = parseSseRequestMetadata(JSON.parse(new TextDecoder().decode(head)))
  return { metadata, frames: rest.map((frame) => decode(frame)) }
}

const world: {
  server: Server
  telefuncUrl: string
  streams: ReturnType<typeof eventStream>[]
  holdOpens: boolean
  heldOpens: (() => void)[]
} = {
  server: new Server(),
  telefuncUrl: '',
  streams: [],
  holdOpens: false,
  heldOpens: [],
}

function serveSse(): void {
  vi.stubGlobal('fetch', async (url: string, init: RequestInit) => {
    // Reject streaming uploads so the client uses batch POSTs.
    if (!String(url).startsWith(world.telefuncUrl) || !(init.body instanceof Blob))
      throw new TypeError('Failed to fetch')
    const { metadata, frames } = await postFrames(init.body)
    const { server } = world
    if (metadata.kind === 'streamResponse') {
      const stream = eventStream()
      world.streams.push(stream)
      const answer = () => {
        stream.answer()
        for (const frame of frames) for (const down of server.answer(frame)) stream.frame(down)
      }
      if (world.holdOpens) {
        stream.hold()
        world.heldOpens.push(answer)
      } else answer()
      return stream.response
    }
    const stream = world.streams.at(-1)
    for (const frame of frames) for (const down of server.answer(frame)) stream?.frame(down)
    return new Response(null)
  })
}

function openChannel(transports: 'sse'[]) {
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    connection: { telefuncUrl: world.telefuncUrl, transports, connectionKey: 'key' },
  })
  void channel.opened.catch(() => {})
  const ended: { at: number; err: Error | null }[] = []
  channel.onClose((err) => void ended.push({ at: performance.now(), err }))
  return { channel, ended }
}

/** Advances the virtual clock in steps of a tenth of a second. */
async function advance(ms: number): Promise<void> {
  for (let left = ms; left > 0; left -= 100) await vi.advanceTimersByTimeAsync(Math.min(100, left))
}

/** Simulates a paused event loop by jumping the clock so each timer that came due runs once. */
async function holdPage(ms: number): Promise<void> {
  const clock: unknown = Reflect.get(globalThis.setTimeout, 'clock')
  if (!canJump(clock)) throw new Error('holdPage needs the fake timers installed')
  clock.jump(ms)
  await vi.advanceTimersByTimeAsync(0)
}

function canJump(clock: unknown): clock is { jump(ms: number): number } {
  return typeof clock === 'object' && clock !== null && typeof Reflect.get(clock, 'jump') === 'function'
}

function useScriptedServer(): void {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date', 'performance'] })
    vi.spyOn(Math, 'random').mockReturnValue(0.9)
    world.server = new Server()
    world.telefuncUrl = `http://telefunc.invalid/${crypto.randomUUID()}/_telefunc`
    world.streams = []
    world.holdOpens = false
    world.heldOpens = []
  })
  afterEach(() => {
    vi.clearAllTimers()
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
    vi.useRealTimers()
  })
}
