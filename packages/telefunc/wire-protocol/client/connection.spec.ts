import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_SERVER_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_SERVER_REPLAY_BUFFER_BYTES,
  CHANNEL_TRANSPORT,
  CREDIT_WINDOW_MAX_BYTES,
  MAX_CHANNELS_PER_CONNECTION,
  RECONCILE_TIMEOUT_MS,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../constants.js'
import { ClientConnection } from './connection.js'
import { encodeSseRequest } from '../sse-request.js'
import { encodeLengthPrefixedFrames } from '../frame.js'
import { TAG, decode, encode, type ReconciledPayload } from '../shared-ws.js'

/** Minimal `MuxChannel` — registering one is enough to make the connection open a wire. */
function createChannel(id = crypto.randomUUID()) {
  return {
    id,
    isClosed: false,
    _maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    _onTransportOpen() {},
    _dispatchFrame() {},
    _onTransportClose() {},
    _onTransportBatched() {},
    _reattachState: () => ({}),
    _fitReplays() {},
    _acknowledge() {},
  }
}

/** A `fetch` whose SSE downstream body never emits a byte and never closes: a silently
 *  stalled stream (no error, no FIN), so the RECONCILED frame that clears `reconciling`
 *  never arrives. Counts how many times the downstream (`Accept: text/event-stream`) is
 *  opened, i.e. how many connect attempts ran. */
function createStalledTransport() {
  let sseDownstreamOpens = 0
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const headers = (init.headers ?? {}) as Record<string, string>
    if (headers.Accept === 'text/event-stream') sseDownstreamOpens++
    return new Response(new ReadableStream<Uint8Array>({ start() {} }), { status: 200 })
  }) as unknown as typeof fetch
  return { fetchImpl, getSseDownstreamOpens: () => sseDownstreamOpens }
}

function stalledOptions() {
  return {
    transports: [CHANNEL_TRANSPORT.SSE],
    fetchImpl: createStalledTransport().fetchImpl,
    connectionKey: crypto.randomUUID(),
  }
}

/** A RECONCILED from a server with the default channel config. */
function reconciled(payload: Pick<ReconciledPayload, 'sessionId' | 'open'> & Partial<ReconciledPayload>) {
  return {
    reconnectTimeout: CHANNEL_RECONNECT_TIMEOUT_MS,
    idleTimeout: CHANNEL_IDLE_TIMEOUT_MS,
    pingInterval: CHANNEL_PING_INTERVAL_MS,
    serverReplayBuffer: CHANNEL_SERVER_REPLAY_BUFFER_BYTES,
    serverReplayBufferBinary: CHANNEL_SERVER_REPLAY_BUFFER_BINARY_BYTES,
    clientReplayBuffer: CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
    clientReplayBufferBinary: CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
    sseFlushThrottle: SSE_FLUSH_THROTTLE_MS,
    ssePostIdleFlushDelay: SSE_POST_IDLE_FLUSH_DELAY_MS,
    transports: [CHANNEL_TRANSPORT.SSE],
    ...payload,
  } satisfies ReconciledPayload
}

/** A connection that applied a RECONCILED whose every setting is zero. */
function zeroConfiguredConnection() {
  const options = stalledOptions()
  const connection = ClientConnection.getOrCreate('http://zero.test', createChannel() as never, options) as any
  const ctrl = new Proxy(
    { sessionId: 'zero', open: [], transports: [CHANNEL_TRANSPORT.SSE] },
    { get: (target, key) => Reflect.get(target, key) ?? 0 },
  )
  connection.applyReconciled(ctrl)
  connection.transport.applyReconciledSettings(ctrl)
  return { connection, options }
}

test('a RECONCILED of zeros keeps zero on the client', () => {
  const { connection } = zeroConfiguredConnection()
  expect([
    connection.reconnectTimeoutMs,
    connection.idleTimeoutMs,
    connection.clientReplayBufferBytes,
    connection.clientReplayBufferBinaryBytes,
    connection.transport.flushThrottleMs,
    connection.transport.postIdleFlushDelayMs,
  ]).toEqual(Array(6).fill(0))
  // What a zero replay allows each way: a byte.
  expect(connection.replayWindows).toEqual({ window: 1, peerWindow: 1 })
  connection.dispose()
})

test("a per-call idleTimeout is kept over the server's", () => {
  const options = { ...stalledOptions(), idleTimeout: 0 }
  const connection = ClientConnection.getOrCreate('http://idle.test', createChannel() as never, options) as any
  connection.applyReconciled(reconciled({ sessionId: 'idle', open: [] }), null)
  expect(connection.idleTimeoutMs).toBe(0)
  connection.dispose()
})

test('a zero replay budget still registers a later channel on the connection', () => {
  const { connection, options } = zeroConfiguredConnection()
  expect(ClientConnection.getOrCreate('http://zero.test', createChannel() as never, options)).toBe(connection)
  connection.dispose()
})

test("a reconnect declares a broadcast's subscriptions, not the toggles queued before it", () => {
  const channel = { ...createChannel(), _reattachState: () => ({ broadcast: { text: true, binary: false } }) }
  const connection = ClientConnection.getOrCreate('http://toggle.test', channel as never, stalledOptions()) as any
  // Offline, the listener is swapped: an unsubscribe, then a subscribe. The reconcile entry already says subscribed.
  connection.sendBroadcastUnsubscribe(channel, false)
  connection.sendBroadcastSubscribe(channel, false)
  const { reconcileFrame, movedBufferedFrames } = connection.stageReconcileBatch()
  expect(decode(reconcileFrame.frame)).toMatchObject({
    payload: { open: [{ broadcast: { text: true, binary: false } }] },
  })
  const queued: Array<number | undefined> = [...connection.sendBuffer, ...movedBufferedFrames].map(
    ({ frame }: { frame: Uint8Array }) => frame[0],
  )
  expect(queued.filter((tag) => tag === TAG.BROADCAST_SUB || tag === TAG.BROADCAST_UNSUB)).toEqual([])
  connection.dispose()
})

test("an SSE reconnect sends its own reconcile and leaves a dead POST's messages to the replay, so they can't overtake the ones in flight", () => {
  const connection = ClientConnection.getOrCreate(
    'http://outbox.test',
    createChannel() as never,
    stalledOptions(),
  ) as any
  // A batch POST that failed carried a message, a window update, an older reconcile and an unsubscribe.
  connection.transport.outbox.push(
    { frame: encode.text(0, 'queued', 7), deadline: Infinity },
    { frame: encode.window(0, 65_536, 0), deadline: Infinity },
    { frame: encode.reconcile({ open: [] }), deadline: Infinity },
    { frame: encode.broadcastUnsub(0, false), deadline: Infinity },
  )
  const { initialFrames } = connection.transport.stageInitialBatch()
  const tags = initialFrames.map(({ frame }: { frame: Uint8Array }) => frame[0])
  expect(tags).toEqual([TAG.RECONCILE, TAG.WINDOW])
  connection.dispose()
})

test('a batch POST goes a flush throttle after the one before it started, whenever its first frame came', () => {
  const connection = ClientConnection.getOrCreate(
    'http://throttle.test',
    createChannel() as never,
    stalledOptions(),
  ) as any
  const transport = connection.transport
  const startedAt = 1_000_000
  transport.lastPostStartedAt = startedAt
  // A frame that comes as the last POST's credit returns, 100 ms into the throttle.
  expect(transport.getFrameDeadline('data', startedAt + 100)).toBe(startedAt + SSE_FLUSH_THROTTLE_MS)
  // After a quiet spell, the idle delay.
  const later = startedAt + 10 * SSE_FLUSH_THROTTLE_MS
  expect(transport.getFrameDeadline('data', later)).toBe(later + SSE_POST_IDLE_FLUSH_DELAY_MS)
  connection.dispose()
})

test('what joins the outbox while a batch POST is out goes into the next body as it comes, and that body carries every frame in order', async () => {
  const connection = ClientConnection.getOrCreate('http://fold.test', createChannel() as never, stalledOptions()) as any
  const transport = connection.transport
  const bodies: Blob[] = []
  let answer!: () => void
  transport.post = (body: Blob) => {
    bodies.push(body)
    return new Promise<Response>((resolve) => void (answer = () => resolve(new Response(''))))
  }
  transport.transportAbort = new AbortController()
  transport.outbox = [{ frame: encode.window(0, 65_536, 0), deadline: 0 }]
  const first = transport.flushOutbox()
  const frames = Array.from({ length: 40 }, (_, i) =>
    encode.text(0, JSON.stringify(String(i).repeat(64 * 1024)), i + 1),
  )
  for (const frame of frames) transport.sendFrame({ kind: 'data', frame })
  // A MiB and more joined while the POST was out: it is in the next body before that POST is answered.
  expect(transport.folded?.count).toBeGreaterThanOrEqual(16)
  answer()
  await first
  await vi.waitFor(() => expect(bodies).toHaveLength(2))
  const sent = Buffer.from(await bodies[1]!.arrayBuffer())
  const expected = Buffer.from(
    await encodeSseRequest({ connId: transport.connId }, encodeLengthPrefixedFrames(frames)).arrayBuffer(),
  )
  // Byte for byte, without a deep comparison of 2.6 MB element by element.
  expect(sent.equals(expected)).toBe(true)
  connection.dispose()
})

test('a frame queued with a flush timer pending does not rescan the outbox for the earliest deadline', () => {
  const { connection, transport } = connectionCountingPosts('http://no-rescan.test')
  let scans = 0
  transport.outbox = new Proxy([], {
    get: (target, key, receiver) => {
      if (key === Symbol.iterator) scans++
      return Reflect.get(target, key, receiver)
    },
  })
  transport.lastPostStartedAt = Date.now()
  for (let i = 0; i < 100; i++) transport.sendFrame({ kind: 'data', frame: encode.text(0, '"x"', i + 1) })
  expect(transport.outbox).toHaveLength(100)
  expect(scans).toBe(1)
  connection.dispose()
})

/** A connection whose batch POSTs are counted and stay out until `answer()`. */
function connectionCountingPosts(url: string) {
  const connection = ClientConnection.getOrCreate(url, createChannel() as never, stalledOptions()) as any
  const transport = connection.transport
  const out = { posts: 0, answer: () => {} }
  transport.post = () => {
    out.posts++
    return new Promise<Response>((resolve) => void (out.answer = () => resolve(new Response(''))))
  }
  transport.transportAbort = new AbortController()
  transport.postBytes = CREDIT_WINDOW_MAX_BYTES / 2
  return { connection, transport, out }
}

const sendMiB = (transport: any, mib: number) => {
  for (let i = 0; i < mib; i++)
    transport.sendFrame({ kind: 'data', frame: encode.text(0, '"' + 'x'.repeat(2 ** 20) + '"', i + 1) })
}

test("the SSE transport's buffered amount counts the data frames waiting for a batch POST, not the control frames", () => {
  const { connection, transport } = connectionCountingPosts('http://buffered.test')
  const data = encode.text(0, '"' + 'x'.repeat(1000) + '"', 1)
  transport.outbox = [
    { frame: encode.window(0, 65_536, 0), deadline: 0 },
    { frame: data, deadline: 0 },
  ]
  expect(transport.bufferedAmount()).toBe(data.byteLength)
  connection.dispose()
})

test.each([
  { mib: 31, next: 'waits for its flush throttle' },
  { mib: 33, next: 'goes as the one before it is answered' },
])('a batch POST holding $mib MiB $next', async ({ mib, next }) => {
  const { connection, transport, out } = connectionCountingPosts('http://eager.test')
  transport.outbox = [{ frame: encode.window(0, 65_536, 0), deadline: 0 }]
  const first = transport.flushOutbox()
  sendMiB(transport, mib)
  out.answer()
  await first
  expect(out.posts).toBe(next.startsWith('goes') ? 2 : 1)
  connection.dispose()
})

test.each([
  { mib: 31, posts: 0, next: 'waits for its flush throttle' },
  { mib: 33, posts: 1, next: 'goes at once' },
])('with no POST out, a batch holding $mib MiB $next', async ({ mib, posts }) => {
  const { connection, transport, out } = connectionCountingPosts('http://eager-idle.test')
  transport.lastPostStartedAt = Date.now()
  sendMiB(transport, mib)
  await new Promise((resolve) => setTimeout(resolve, 0))
  expect(out.posts).toBe(posts)
  connection.dispose()
})

test.each([
  { kind: 'flow-control', posts: 0, next: 'waits for its flush throttle' },
  { kind: 'urgent-flow-control', posts: 1, next: 'goes at once' },
])('with no POST out, a $kind frame $next', async ({ kind, posts }) => {
  const { connection, transport, out } = connectionCountingPosts('http://urgent-window.test')
  transport.lastPostStartedAt = Date.now()
  transport.sendFrame({ kind, frame: encode.window(0, 65_536, 0) })
  await new Promise((resolve) => setTimeout(resolve, 0))
  expect(out.posts).toBe(posts)
  connection.dispose()
})

test.each([
  { urgent: true, kind: 'urgent-flow-control' },
  { urgent: false, kind: 'flow-control' },
])('a window update made with urgent $urgent reaches the transport as a $kind frame', ({ urgent, kind }) => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://urgent-kind.test', channel as never, stalledOptions()) as any
  connection.canSendImmediately = () => true
  const sent: string[] = []
  connection.transport.sendFrame = (frame: { kind: string }) => void sent.push(frame.kind)
  connection.sendByteWindowUpdate(channel, 65_536, urgent)
  expect(sent).toEqual([kind])
  connection.dispose()
})

test('an urgent window refresh made while a batch POST is out goes as that POST is answered', async () => {
  const { connection, transport, out } = connectionCountingPosts('http://urgent-window-out.test')
  transport.outbox = [{ frame: encode.text(0, '"x"', 1), deadline: 0 }]
  const first = transport.flushOutbox()
  transport.sendFrame({ kind: 'urgent-flow-control', frame: encode.window(0, 65_536, 0) })
  expect(out.posts).toBe(1)
  out.answer()
  await first
  await new Promise((resolve) => setTimeout(resolve, 50))
  expect(out.posts).toBe(2)
  connection.dispose()
})

test.each([
  { answeredAfter: 100, post: 'answered within that delay' },
  { answeredAfter: CHANNEL_PING_INTERVAL_MS, post: 'still out at that delay' },
])(
  'a PING that comes while a batch POST is out goes its heartbeat delay later, and only then, with the POST $post',
  async ({ answeredAfter }) => {
    vi.useFakeTimers()
    const connection = ClientConnection.getOrCreate(
      'http://ping.test',
      createChannel() as never,
      stalledOptions(),
    ) as any
    try {
      const transport = connection.transport
      const posts: number[] = []
      let answer!: () => void
      transport.post = () => {
        posts.push(Date.now())
        return new Promise<Response>((resolve) => void (answer = () => resolve(new Response(''))))
      }
      transport.transportAbort = new AbortController()
      transport.outbox = [{ frame: encode.window(0, 65_536, 0), deadline: 0 }]
      void transport.flushOutbox()
      const pingAt = Date.now()
      transport.sendFrame({ kind: 'heartbeat', frame: encode.ping() })
      await vi.advanceTimersByTimeAsync(answeredAfter)
      answer()
      await vi.advanceTimersByTimeAsync(CHANNEL_PING_INTERVAL_MS)
      expect(posts.map((at) => at - pingAt)).toEqual([0, CHANNEL_PING_INTERVAL_MS / 2])
    } finally {
      connection.dispose()
      vi.useRealTimers()
    }
  },
)

test('a batch POST that fails after the next wire started puts back only its window refreshes', async () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://late-post.test', channel as never, stalledOptions()) as any
  const transport = connection.transport
  let fail!: () => void
  transport.post = () => new Promise((_resolve, reject) => void (fail = () => reject(new Error('aborted'))))
  transport.transportAbort = new AbortController()
  transport.outbox = [
    { frame: encode.broadcastUnsub(0, false), deadline: 0 },
    { frame: encode.window(0, 65_536, 0), deadline: 0 },
  ]
  const flushing = transport.flushOutbox()
  transport.transportAbort = new AbortController() // the next wire reconciled while that POST hung
  fail()
  await flushing
  expect(transport.outbox.map(({ frame }: { frame: Uint8Array }) => frame[0])).toEqual([TAG.WINDOW])
  connection.dispose()
})

test("a wire's end aborts its batch POST still in flight, which would otherwise hold the next wire's outbox", async () => {
  let endWire: (() => void) | undefined
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    if ((init.headers as Record<string, string>).Accept !== 'text/event-stream')
      return await new Promise<Response>(() => {})
    return new Response(
      new ReadableStream<Uint8Array>({ start: (controller) => void (endWire = () => controller.close()) }),
    )
  }) as unknown as typeof fetch
  const options = { ...stalledOptions(), fetchImpl }
  const connection = ClientConnection.getOrCreate('http://hung-post.test', createChannel() as never, options) as any
  await vi.waitFor(() => expect(endWire).toBeDefined())
  const transport = connection.transport
  // A batch POST hung on a dead TCP connection settles only when its wire aborts it.
  transport.post = (_body: unknown, signal: AbortSignal) =>
    new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason)))
  transport.outbox = [{ frame: encode.window(0, 65_536, 0), deadline: 0 }]
  void transport.flushOutbox()
  endWire!()
  await vi.waitFor(() => expect(transport.flushing).toBe(false))
  connection.dispose()
})

test('the channel cap counts the open channels, not every channel the connection opened', () => {
  const options = stalledOptions()
  const connection = ClientConnection.getOrCreate('http://cap.test', createChannel() as never, options) as any
  connection.nextIndex = MAX_CHANNELS_PER_CONNECTION // what 4,095 channels opened and closed on it leave
  for (let open = 1; open < MAX_CHANNELS_PER_CONNECTION; open++) {
    expect(ClientConnection.getOrCreate('http://cap.test', createChannel() as never, options)).toBe(connection)
  }
  expect(() => ClientConnection.getOrCreate('http://cap.test', createChannel() as never, options)).toThrow(
    'Too many channels',
  )
  connection.dispose()
})

test('a closed channel counts against neither the channel cap nor, when a new one needs its place, what a RECONCILE lists', () => {
  const options = stalledOptions()
  const closing = createChannel()
  const connection = ClientConnection.getOrCreate('http://cap-closed.test', closing as never, options) as any
  for (let open = 1; open < MAX_CHANNELS_PER_CONNECTION; open++)
    ClientConnection.getOrCreate('http://cap-closed.test', createChannel() as never, options)
  connection.sendAbort(closing, 'null') // its abort waits for the wire
  connection.unregister(closing)
  expect(ClientConnection.getOrCreate('http://cap-closed.test', createChannel() as never, options)).toBe(connection)
  expect(connection.channels.size).toBe(MAX_CHANNELS_PER_CONNECTION)
  expect(connection.channels.has(0)).toBe(false)
  connection.dispose()
})

test('a connection out of wire indexes hands a new channel to a fresh connection, which its dispose leaves cached', () => {
  const options = stalledOptions()
  const spent = ClientConnection.getOrCreate('http://rotate.test', createChannel() as never, options) as any
  spent.nextIndex = 0x10000
  const fresh = ClientConnection.getOrCreate('http://rotate.test', createChannel() as never, options) as any
  expect(fresh).not.toBe(spent)
  spent.dispose()
  expect(ClientConnection.getOrCreate('http://rotate.test', createChannel() as never, options)).toBe(fresh)
  fresh.dispose()
})

test('a buffered acked binary send is kept in the binary replay lane, not the text one', () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://binary-ack.test', channel as never, stalledOptions()) as any
  connection.sendBinaryAckReq(channel, new Uint8Array(1_500_000), () => {}) // over the text lane's 1 MiB, within binary's 2
  connection.drainBufferedFrames((ix: number) => ix === 0)
  expect(connection.replayBuffers.get(0).getAfter(0)).toHaveLength(1)
  connection.dispose()
})

test("a frame buffered before the first reconcile is stored under the server's replay budget", () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://replay-drain.test', channel as never, stalledOptions()) as any
  connection.send(channel, 'x'.repeat(2 * 1024 * 1024)) // over the default 1 MiB, waiting for the wire
  connection.buildReconcileFrame()
  connection.applyReconciled(
    reconciled({ sessionId: 'drain', open: [{ ix: 0, lastSeq: 0 }], clientReplayBuffer: 8 * 1024 * 1024 }),
    null,
  )
  expect(connection.replayBuffers.get(0).getAfter(0)).toHaveLength(1)
  connection.dispose()
})

test("a channel whose close timed out while its reconnect's reconcile is in flight sends its close request after that RECONCILED, which ends it on the server", () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate(
    'http://closed-meanwhile.test',
    channel as never,
    stalledOptions(),
  ) as any
  connection.buildReconcileFrame()
  connection.applyReconciled(reconciled({ sessionId: 'meanwhile', open: [{ ix: 0, lastSeq: 0 }] }), null)
  const replay = connection.replayBuffers.get(0)
  replay.push(replay.nextSeq(), encode.close(0, 5_000, 1)) // its close request, lost with the dead wire
  connection.buildReconcileFrame() // the reconnect's, listing the channel
  connection.unregister(channel) // its close timed out
  const { frames } = connection.applyReconciled(
    reconciled({ sessionId: 'meanwhile', open: [{ ix: 0, lastSeq: 0 }] }),
    null,
  )
  const sent = frames.map(({ frame }: { frame: Uint8Array<ArrayBuffer> }) => decode(frame))
  expect(sent).toMatchObject([{ tag: TAG.CLOSE, index: 0, seq: 1 }])
  connection.dispose()
})

test('a channel closed during a reconnect sends what the dead wire lost before what it queued', () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate(
    'http://draining-replay.test',
    channel as never,
    stalledOptions(),
  ) as any
  connection.buildReconcileFrame()
  connection.applyReconciled(reconciled({ sessionId: 'draining', open: [{ ix: 0, lastSeq: 0 }] }), null)
  const replay = connection.replayBuffers.get(0)
  replay.push(replay.nextSeq(), encode.text(0, 'lost with the wire', 1))
  connection.buildReconcileFrame() // the reconnect's: sends wait for its RECONCILED
  connection.send(channel, 'queued')
  connection.sendAbort(channel, 'null')
  connection.unregister(channel)
  const { frames } = connection.applyReconciled(
    reconciled({ sessionId: 'draining', open: [{ ix: 0, lastSeq: 0 }] }),
    null,
  )
  const sent = frames.map(({ frame }: { frame: Uint8Array<ArrayBuffer> }) => decode(frame))
  expect(sent.map((frame: { tag: number; seq?: number }) => [frame.tag, frame.seq])).toEqual([
    [TAG.TEXT, 1],
    [TAG.TEXT, 2],
    [TAG.ABORT, 3],
  ])
  connection.dispose()
})

test('a channel whose abort went out with a reconcile on a wire that then died sends it again after the next RECONCILED, not what its listener answered after', () => {
  const closing = createChannel()
  const options = stalledOptions()
  const connection = ClientConnection.getOrCreate('http://abort-lost.test', closing as never, options) as any
  connection.buildReconcileFrame()
  connection.applyReconciled(reconciled({ sessionId: 'lost', open: [{ ix: 0, lastSeq: 0 }] }), null)
  ClientConnection.getOrCreate('http://abort-lost.test', createChannel() as never, options) // a call's callback
  connection.buildReconcileFrame() // its registration: sends wait for the RECONCILED
  connection.sendAbort(closing, 'null')
  connection.unregister(closing)
  connection.stageReconcileBatch() // the abort leaves with the registration's reconcile
  connection.sendAckRes(closing, 1, '"answer"') // its async listener answers a server send({ ack: true }) after that
  connection.handleTransportLoss(new Error('the wire died'))
  connection.buildReconcileFrame()
  const open = [
    { ix: 0, lastSeq: 0 },
    { ix: 1, lastSeq: 0 },
  ]
  const { frames } = connection.applyReconciled(reconciled({ sessionId: 'lost', open }), null)
  const sent = frames.map(({ frame }: { frame: Uint8Array<ArrayBuffer> }) => decode(frame))
  expect(sent.filter((frame: { index?: number }) => frame.index === 0)).toMatchObject([
    { tag: TAG.ABORT, seq: 1, abortValue: 'null' },
  ])
  connection.dispose()
})

test("a closing channel's listener answer, held while another channel registers on a live wire, reaches the server", () => {
  const closing = createChannel()
  const options = stalledOptions()
  const connection = ClientConnection.getOrCreate('http://answer-held.test', closing as never, options) as any
  connection.buildReconcileFrame()
  connection.applyReconciled(reconciled({ sessionId: 'answer-held', open: [{ ix: 0, lastSeq: 0 }] }), null)
  ClientConnection.getOrCreate('http://answer-held.test', createChannel() as never, options) // the listener opens a channel
  connection.sendAckRes(closing, 1, '"answer"') // then answers; the registration holds it
  connection.unregister(closing) // its close round trip is done
  const { reconcileFrame, movedBufferedFrames } = connection.stageReconcileBatch()
  const reconcile = decode(reconcileFrame.frame) as { payload: { open: { ix: number }[] } }
  const listed = reconcile.payload.open.map((entry) => entry.ix)
  const queued = [...movedBufferedFrames, ...connection.sendBuffer].map(({ frame }: { frame: Uint8Array }) => frame[0])
  expect({ listed: listed.includes(0), answer: queued.includes(TAG.ACK_RES) }).toEqual({ listed: true, answer: true })
  connection.dispose()
})

/** Two open channels; the first one's close request went down with a wire whose loss is then noticed. */
function closeLostWithWire(url: string) {
  const closing = createChannel()
  const options = stalledOptions()
  const connection = ClientConnection.getOrCreate(url, closing as never, options) as any
  ClientConnection.getOrCreate(url, createChannel() as never, options) // another channel on the page
  connection.buildReconcileFrame()
  connection.applyReconciled(
    reconciled({
      sessionId: 'close-lost',
      open: [
        { ix: 0, lastSeq: 0 },
        { ix: 1, lastSeq: 0 },
      ],
    }),
    null,
  )
  const replay = connection.replayBuffers.get(0)
  replay.push(replay.nextSeq(), encode.close(0, 5_000, 1)) // the close request, written to it
  connection.handleTransportLoss(new Error('the wire died'))
  return { connection, closing }
}

test('a channel whose close request went down with the wire stays in the reconnect once its close timed out, which sends the request, then what its listener answered after the loss', () => {
  const { connection, closing } = closeLostWithWire('http://close-lost.test')
  connection.sendAckRes(closing, 1, '"answer"') // its async listener answers a server send({ ack: true })
  connection.unregister(closing) // its close times out
  const reconcile = decode(connection.buildReconcileFrame().frame) as { payload: { open: { ix: number }[] } }
  expect(reconcile.payload.open.map((entry) => entry.ix)).toEqual([0, 1])
  const open = [
    { ix: 0, lastSeq: 0 },
    { ix: 1, lastSeq: 0 },
  ]
  const { frames } = connection.applyReconciled(reconciled({ sessionId: 'close-lost', open }), null)
  const sent = frames.map(({ frame }: { frame: Uint8Array<ArrayBuffer> }) => decode(frame))
  expect(sent.filter((frame: { index?: number }) => frame.index === 0)).toMatchObject([
    { tag: TAG.CLOSE, seq: 1 },
    { tag: TAG.ACK_RES, seq: 2 },
  ])
  connection.dispose()
})

test('such a channel, closed while the reconnect listing it is in flight, sends the request, then the answer, after its RECONCILED, which needs no follow-up', () => {
  const { connection, closing } = closeLostWithWire('http://close-lost-in-flight.test')
  connection.buildReconcileFrame() // the attempt's, built when it starts, listing both
  connection.sendAckRes(closing, 1, '"answer"')
  connection.unregister(closing)
  const open = [
    { ix: 0, lastSeq: 0 },
    { ix: 1, lastSeq: 0 },
  ]
  const { frames } = connection.applyReconciled(reconciled({ sessionId: 'close-lost', open }), null)
  const sent = frames.map(({ frame }: { frame: Uint8Array<ArrayBuffer> }) => decode(frame))
  expect(sent.filter((frame: { tag: number }) => frame.tag === TAG.RECONCILE)).toEqual([])
  expect(sent.filter((frame: { index?: number }) => frame.index === 0)).toMatchObject([
    { tag: TAG.CLOSE, seq: 1 },
    { tag: TAG.ACK_RES, seq: 2 },
  ])
  connection.dispose()
})

test('a channel whose close timed out while the reconnect listing it is in flight sends its reply after the message the dead wire lost before it', () => {
  const closing = createChannel()
  const options = stalledOptions()
  const connection = ClientConnection.getOrCreate('http://gap.test', closing as never, options) as any
  ClientConnection.getOrCreate('http://gap.test', createChannel() as never, options) // another channel on the page
  const open = [
    { ix: 0, lastSeq: 0 },
    { ix: 1, lastSeq: 0 },
  ]
  connection.buildReconcileFrame()
  connection.applyReconciled(reconciled({ sessionId: 'gap', open }), null)
  const replay = connection.replayBuffers.get(0)
  replay.push(replay.nextSeq(), encode.text(0, '"m4"', 1)) // sent into a wire that is dead, not yet noticed
  replay.push(replay.nextSeq(), encode.close(0, 5_000, 2)) // its close request went down with it too
  connection.handleTransportLoss(new Error('the wire died'))
  connection.buildReconcileFrame() // the SSE attempt's, built when it starts, listing both
  connection.sendAckRes(closing, 1, '"answer"') // its async listener answers a server send({ ack: true })
  connection.unregister(closing) // its close times out while the attempt is held
  const { frames } = connection.applyReconciled(reconciled({ sessionId: 'gap', open }), null)
  const sent = frames.map(({ frame }: { frame: Uint8Array<ArrayBuffer> }) => decode(frame))
  expect(sent.filter((frame: { index?: number }) => frame.index === 0)).toMatchObject([
    { tag: TAG.TEXT, seq: 1 },
    { tag: TAG.CLOSE, seq: 2 },
    { tag: TAG.ACK_RES, seq: 3 },
  ])
  connection.dispose()
})

test('a channel whose ATTACH_RESULT overtakes the RECONCILED leaving it out opens with that RECONCILED', () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://attach-early.test', channel as never, stalledOptions()) as any
  connection.buildReconcileFrame()
  // Its call reached the server before the end of the batch POST carrying the RECONCILE, whose RECONCILED goes then.
  connection.dispatchFrame(decode(encode.attachResult(0, 0)))
  const { channelsToOpen } = connection.applyReconciled(reconciled({ sessionId: 'early', open: [] }), null)
  expect(channelsToOpen).toEqual([channel])
  connection.dispose()
})

test("a first connect's retry holds back a newer frame behind the ones its failed attempt sent", () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://first-retry.test', channel as never, stalledOptions()) as any
  const replay = connection.replayBuffers.get(0)
  replay.push(replay.nextSeq(), encode.text(0, 'join', 1)) // sent by the attempt that failed before its RECONCILED
  connection.send(channel, 'second')
  expect(connection.drainBufferedFramesForReconcile(true)).toEqual([])
  connection.dispose()
})

describe('SSE reconcile watchdog', () => {
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  test('reconnects when RECONCILED never arrives on a stalled downstream', async () => {
    vi.useFakeTimers()
    const { fetchImpl, getSseDownstreamOpens } = createStalledTransport()

    ClientConnection.getOrCreate('http://test.local/_telefunc', createChannel() as never, {
      transports: [CHANNEL_TRANSPORT.SSE],
      fetchImpl,
      connectionKey: crypto.randomUUID(),
    })

    // `start()` defers the first openStream by one reconcile window; let it connect.
    await vi.advanceTimersByTimeAsync(100)
    expect(getSseDownstreamOpens()).toBe(1)

    // The downstream is silent, so RECONCILED never lands. Without the watchdog the
    // connection wedges here forever: pings keep flowing but `handlePongTimeout` is
    // suppressed while the wire opens, so the dead wire is never noticed. The watchdog must
    // instead time out the reconcile and reconnect.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    expect(getSseDownstreamOpens()).toBeGreaterThanOrEqual(2)
  })
})
