// A page that reconnects reads its RECONCILED first, then what the server replays of each channel, then what the app
// sent on the channel after the attach, in seq order: on both of the server's SSE paths, the Web stream a Web adapter
// answers with and the Node `Readable` the Node adapter pipes to its socket. The real SSE handler and ChannelMux run;
// the page is the test, reading the event stream's frames.

import { expect, test, vi } from 'vitest'
import { Readable } from 'node:stream'

import { CHANNEL_TRANSPORT, MAX_CHANNELS_PER_CONNECTION, WIRE_VERSION } from '../../constants.js'
import { loadStreamNodeModuleOnce } from '../../../utils/loadStreamNodeModule.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode, isOneFrame, isSequencedFrame, messageFrames } from '../../wire/frame.js'
import { HEADER, SEQ_OFFSET, readU32 } from '../../wire/header.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux, type ServerTransport } from './ChannelMux.js'
import { SessionRegistry } from './SessionRegistry.js'
import { ServerChannel } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

type ServePath = 'web' | 'node'

/** A page's event stream: what its frames carried so far, read as the page reads them. */
async function openStream(path: ServePath, reconcile: Uint8Array<ArrayBuffer>) {
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const body = encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [reconcile])
  const opened =
    path === 'web'
      ? await handleSseChannelRequest(new Request(url, { method: 'POST', body }))
      : await handleSseChannelRequest(
          new Request(url, { method: 'POST' }),
          Readable.from([new Uint8Array(await body.arrayBuffer())]),
        )
  const stream = opened.body
  if (typeof stream === 'string') throw new Error(`no event stream: ${opened.statusCode} ${stream}`)
  const reader = (stream instanceof ReadableStream ? stream : webStreamOf(stream)).getReader()
  const events = new SseEvents()
  const frames: Uint8Array[] = []
  return {
    /** Reads until a frame `until` accepts has come, and returns every frame so far. */
    async readUntil(until: (frame: Uint8Array) => boolean): Promise<Uint8Array[]> {
      while (!frames.some(until)) {
        const read = await reader.read()
        if (read.done) break
        events.push(read.value, (frame) => frames.push(frame))
      }
      return frames
    },
    /** The page's connection drops: the server's wire closes transiently. */
    drop: () => (stream instanceof ReadableStream ? reader.cancel() : stream.destroy()),
  }
}

/** The Node adapter's body as a stream the page reads. */
function webStreamOf(readable: Readable): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      readable.on('data', (chunk: Uint8Array) => controller.enqueue(chunk))
      readable.on('end', () => controller.close())
    },
  })
}

/** The RECONCILED and each channel frame, as `RECONCILED` or `<seq>:<message>`; a lossy send's head names the seq its
 *  message has. */
function order(frames: Uint8Array[]): string[] {
  return frames.flatMap((frame) => {
    const decoded = decode(frame)
    if (decoded.tag === TAG.RECONCILED) return ['RECONCILED']
    if (!isSequencedFrame(decoded) || decoded.tag === TAG.PUBLISH_TO) return []
    return [`${decoded.seq}:${decoded.tag === TAG.TEXT ? JSON.parse(decoded.text) : `0x${decoded.tag.toString(16)}`}`]
  })
}

/** A frame that carries `message`, which a channel's `send` serializes. */
const carries = (message: string) => (frame: Uint8Array) => {
  const decoded = decode(frame)
  return decoded.tag === TAG.TEXT && decoded.text === JSON.stringify(message)
}

test.each(['web', 'node'] as const)(
  'over SSE on the %s path, a reconnect reads the RECONCILED, then the replay, then what the app sent in the gap before the RECONCILED went out, lossy sends too',
  async (path) => {
    // The Node adapter loads `node:stream` before it serves; the server's push readable reads it from there.
    await loadStreamNodeModuleOnce()
    const mux = getChannelMux()
    const channel = new ServerChannel()
    mux.registerChannel(channel)

    // The page attaches the channel; the app sends two messages, which the page doesn't confirm.
    const first = await openStream(
      path,
      encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
    )
    const [reconciled] = await first.readUntil((frame) => frame[0] === TAG.RECONCILED)
    const answer = reconciled === undefined ? undefined : decode(reconciled)
    if (answer?.tag !== TAG.RECONCILED) throw new Error('no RECONCILED on the first wire')
    void channel.send('a')
    void channel.send('b')
    await first.readUntil(carries('b'))
    await first.drop()

    // The page reconnects. The app sends a third message and two lossy ones after the server attached the channel to
    // the new wire and before the RECONCILED goes out: on SSE, the stream waits for the batch POSTs in flight between
    // the two. A page that keeps up gets every lossy send.
    const sendReconciled = mux.sendReconciled.bind(mux)
    vi.spyOn(mux, 'sendReconciled').mockImplementationOnce((outcome) => {
      void channel.send('c')
      void channel.send('l1', { lossy: true })
      void channel.send('l2', { lossy: true })
      sendReconciled(outcome)
    })
    const second = await openStream(
      path,
      encode.reconcile({ sessionId: answer.payload.sessionId, open: [{ id: channel.id, ix: 0, lastSeq: 0 }] }),
    )
    expect(order(await second.readUntil(carries('l2')))).toEqual(['RECONCILED', '1:a', '2:b', '3:c', '4:l1', '5:l2'])
    await second.drop()
    vi.restoreAllMocks()
  },
)

// What a wire holds while its RECONCILED is owed is bounded by the limit on what waits on its socket or stream, whatever
// keeps the hold on (decisions/516-sse-carrier.md msg44, the review's R1.1).

type FakeWire = { sessionId: string | undefined }

/** A WebSocket the real mux sends on: the frames it was handed, in order, and whether the mux ended it. Its socket keeps
 *  all it is handed, unread, unless `reads`; `knowsBacklog: false` is a runtime that can't tell the socket's backlog
 *  (workerd). */
function openWebSocket({ reads = false, knowsBacklog = true } = {}) {
  const mux = getChannelMux()
  const wire: FakeWire = { sessionId: undefined }
  const frames: Uint8Array[] = []
  const state = { buffered: 0, terminated: false }
  const transport: ServerTransport<FakeWire> = {
    getSessionId: (w) => w.sessionId,
    setSessionId: (w, sessionId) => {
      w.sessionId = sessionId
    },
    getConnId: () => null,
    sendNow: (_, message) => {
      if (!reads) state.buffered += message.byteLength
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) frames.push(frame)
    },
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    bufferedAmount: () => (knowsBacklog ? state.buffered : undefined),
    terminateWire: () => {
      state.terminated = true
    },
  }
  mux.onWireOpen(wire, transport)
  return {
    mux,
    wire,
    frames,
    state,
    /** The page's connection drops: the server's wire closes, transiently unless `permanent`. */
    close: (permanent = true) => mux.onWireClosed(wire, { permanent }),
  }
}

test('over a WebSocket, a reconnect reads the RECONCILED, then the replay, then what the app sent in the gap before the RECONCILED went out, lossy sends too', async () => {
  const mux = getChannelMux()
  const channel = new ServerChannel()
  mux.registerChannel(channel)
  const first = openWebSocket({ reads: true })
  await mux.onWireMessage(
    first.wire,
    encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
  )
  const { sessionId } = first.wire
  if (sessionId === undefined) throw new Error('no session on the first wire')
  void channel.send('a')
  void channel.send('b')
  first.close(false)

  const sendReconciled = mux.sendReconciled.bind(mux)
  vi.spyOn(mux, 'sendReconciled').mockImplementationOnce((outcome) => {
    void channel.send('c')
    void channel.send('l1', { lossy: true })
    void channel.send('l2', { lossy: true })
    sendReconciled(outcome)
  })
  const second = openWebSocket({ reads: true })
  await mux.onWireMessage(second.wire, encode.reconcile({ sessionId, open: [{ id: channel.id, ix: 0, lastSeq: 0 }] }))
  expect(order(second.frames)).toEqual(['RECONCILED', '1:a', '2:b', '3:c', '4:l1', '5:l2'])
  vi.restoreAllMocks()
  second.close()
})

/** A PING listing as many channels as a page may have, none of them open: its PONG answers each, about 28 KB. */
const widePing = () => encode.ping(Array.from({ length: MAX_CHANNELS_PER_CONNECTION }, (_, ix) => ({ ix, lastSeq: 0 })))

/** More wide PINGs than the send backlog's base allows PONGs for. */
const PINGS_PAST_THE_LIMIT = 3_000

test('a WebSocket message of two RECONCILEs ends its wire: a page has one RECONCILE out at a time', async () => {
  const page = openWebSocket()
  const first = encode.reconcile({ open: [] })
  const second = encode.reconcile({ open: [] })
  await page.mux.onWireMessage(
    page.wire,
    encode.message([first, second], first.byteLength + HEADER + second.byteLength),
  )
  for (let i = 0; i < 200 && !page.state.terminated; i++) await page.mux.onWireMessage(page.wire, encode.ping([]))
  expect(page.state.terminated).toBe(true)
  page.close()
})

test("a BARRIER riding behind a PING in one WebSocket message gets its RECONCILED on the WebSocket it commits, which then carries the channel's frames", async () => {
  const old = openWebSocket({ reads: true })
  const channel = new ServerChannel()
  old.mux.registerChannel(channel)
  await old.mux.onWireMessage(
    old.wire,
    encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
  )
  const sessionId = old.wire.sessionId
  if (sessionId === undefined) throw new Error('no session on the old wire')
  const upgraded = openWebSocket({ reads: true })
  await upgraded.mux.onWireMessage(upgraded.wire, encode.prepare({ upgradeId: 'u1', sessionId }))
  const ping = encode.ping([])
  const barrier = encode.barrier({ sessionId, upgradeId: 'u1', open: [{ id: channel.id, ix: 0, lastSeq: 0 }] })
  await old.mux.onWireMessage(old.wire, encode.message([ping, barrier], ping.byteLength + HEADER + barrier.byteLength))
  void channel.send('after the upgrade')
  await vi.waitFor(() => expect(upgraded.frames.some((frame) => frame[0] === TAG.TEXT)).toBe(true))
  expect(order(upgraded.frames)).toEqual(['RECONCILED', '1:after the upgrade'])
  old.close()
  upgraded.close()
})

test('a WebSocket message of a RECONCILE and a BARRIER ends its wire: a page has one of them out at a time', async () => {
  const old = openWebSocket({ reads: true })
  const channel = new ServerChannel()
  old.mux.registerChannel(channel)
  const attach = { id: channel.id, ix: 0, lastSeq: 0 }
  await old.mux.onWireMessage(old.wire, encode.reconcile({ open: [{ ...attach, initial: true }] }))
  const sessionId = old.wire.sessionId
  if (sessionId === undefined) throw new Error('no session on the old wire')
  const upgraded = openWebSocket({ reads: true })
  await upgraded.mux.onWireMessage(upgraded.wire, encode.prepare({ upgradeId: 'u1', sessionId }))
  const reconcile = encode.reconcile({ sessionId, open: [attach] })
  const barrier = encode.barrier({ sessionId, upgradeId: 'u1', open: [attach] })
  await old.mux.onWireMessage(
    old.wire,
    encode.message([reconcile, barrier], reconcile.byteLength + HEADER + barrier.byteLength),
  )
  expect(old.state.terminated).toBe(true)
  old.close()
  upgraded.close()
})

test('a reconcile that throws a bug ends its wire, transiently: nothing stays held on a wire that is up', async () => {
  const page = openWebSocket()
  vi.spyOn(SessionRegistry.prototype, 'setSession').mockImplementationOnce(() => {
    throw new Error('a bug in reconcile')
  })
  vi.spyOn(console, 'error').mockImplementation(() => {})
  await page.mux.onWireMessage(page.wire, encode.reconcile({ open: [] }))
  await Promise.resolve()
  expect(page.state.terminated).toBe(true)
  // The server's bug, not the page's fault: the page's channels keep their reconnect grace.
  expect(page.mux.readPermanentTermination(page.wire)).toBe(false)
  vi.restoreAllMocks()
  page.close()
})

test.each([
  ['tells', true],
  ["can't tell", false],
])(
  "on a WebSocket whose runtime %s its socket's backlog, what a hold kept on by anything else holds ends its wire past the backlog limit",
  async (_runtime, knowsBacklog) => {
    const page = openWebSocket({ knowsBacklog })
    vi.spyOn(page.mux, 'sendReconciled').mockImplementation(() => {})
    // Its PONG is measured before the hold begins.
    await page.mux.onWireMessage(page.wire, encode.ping([]))
    await page.mux.onWireMessage(page.wire, encode.reconcile({ open: [] }))
    let pings = 0
    for (; pings < PINGS_PAST_THE_LIMIT && !page.state.terminated; pings++)
      await page.mux.onWireMessage(page.wire, widePing())
    expect(page.state.terminated).toBe(true)
    expect(page.frames.filter((frame) => frame[0] === TAG.PONG)).toHaveLength(1)
    vi.restoreAllMocks()
    page.close()
  },
)

test('on SSE, PINGs in batch POSTs while a hold is kept on by anything else end the wire past the backlog limit', async () => {
  const mux = getChannelMux()
  vi.spyOn(mux, 'sendReconciled').mockImplementation(() => {})
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const connId = crypto.randomUUID()
  const opened = await handleSseChannelRequest(
    new Request(url, {
      method: 'POST',
      body: encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]),
    }),
  )
  expect(opened.statusCode).toBe(200)
  let status = 200
  for (let post = 0; post < PINGS_PAST_THE_LIMIT / 100 && status === 200; post++) {
    const pings = Array.from({ length: 100 }, widePing)
    const answered = await handleSseChannelRequest(
      new Request(url, { method: 'POST', body: encodeSseRequest({ connId }, pings) }),
    )
    status = answered.statusCode
  }
  expect(status).toBe(400)
  vi.restoreAllMocks()
})

test("over SSE, a reconnect whose replay passes the backlog base is not ended by the limit: the replay is its channel's", async () => {
  const mux = getChannelMux()
  const channel = new ServerChannel()
  mux.registerChannel(channel)
  const first = await openStream(
    'web',
    encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
  )
  const [reconciled] = await first.readUntil((frame) => frame[0] === TAG.RECONCILED)
  const answer = reconciled === undefined ? undefined : decode(reconciled)
  if (answer?.tag !== TAG.RECONCILED) throw new Error('no RECONCILED on the first wire')
  // 100 MiB the page never confirms, past what the backlog allows a wire beside its channels' (64 MiB and its largest
  // frame).
  const names = ['m1', 'm2', 'm3', 'm4', 'm5']
  for (const name of names) void channel.send(name + 'x'.repeat(20 * 2 ** 20))
  /** The frame of the last message, read from its header alone. */
  const last = (frame: Uint8Array) => frame[0] === TAG.TEXT && readU32(frame, SEQ_OFFSET) === names.length
  await first.readUntil(last)
  await first.drop()
  const second = await openStream(
    'web',
    encode.reconcile({ sessionId: answer.payload.sessionId, open: [{ id: channel.id, ix: 0, lastSeq: 0 }] }),
  )
  // Each message's name: what comes before its padding.
  expect(order(await second.readUntil(last)).map((label) => label.split('x')[0])).toEqual([
    'RECONCILED',
    ...names.map((name, i) => `${i + 1}:${name}`),
  ])
  await second.drop()
}, 60_000)
