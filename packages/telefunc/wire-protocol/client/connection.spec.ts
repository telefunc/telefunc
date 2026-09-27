import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  RECONCILE_TIMEOUT_MS,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
} from '../constants.js'
import { ClientConnection } from './connection.js'
import { TAG, decode, encode, type ReconciledPayload } from '../shared-ws.js'

/** Minimal `MuxChannel` — registering one is enough to make the connection open a wire. */
function createChannel(id = crypto.randomUUID()) {
  return {
    id,
    isClosed: false,
    _onTransportOpen() {},
    _dispatchFrame() {},
    _onTransportClose() {},
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
  const connection = ClientConnection.getOrCreate('http://toggle.test', channel as never, {
    transports: [CHANNEL_TRANSPORT.SSE],
    fetchImpl: createStalledTransport().fetchImpl,
    connectionKey: crypto.randomUUID(),
  }) as any
  // Offline, the listener is swapped: an unsubscribe, then a subscribe. The reconcile entry already says subscribed.
  connection.sendBroadcastUnsubscribe(channel, false)
  connection.sendBroadcastSubscribe(channel, false)
  const { movedBufferedFrames } = connection.stageReconcileBatch()
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
    { frame: encode.window(0, 65_536), deadline: Infinity },
    { frame: encode.reconcile({ open: [] }), deadline: Infinity },
    { frame: encode.broadcastUnsub(0, false), deadline: Infinity },
  )
  const { initialFrames } = connection.transport.stageInitialBatch()
  const tags = initialFrames.map(({ frame }: { frame: Uint8Array }) => frame[0])
  expect(tags).toEqual([TAG.RECONCILE, TAG.WINDOW])
  connection.dispose()
})

test('a batch POST that fails after the next wire started puts back only its window refreshes', async () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://late-post.test', channel as never, stalledOptions()) as any
  const transport = connection.transport
  let fail!: () => void
  transport.post = () => new Promise((_resolve, reject) => void (fail = () => reject(new Error('aborted'))))
  transport.transportAbort = new AbortController()
  transport.outbox = [
    { frame: encode.broadcastUnsub(0, false), deadline: 0 },
    { frame: encode.window(0, 65_536), deadline: 0 },
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
  const connection = ClientConnection.getOrCreate('http://hung-post.test', createChannel() as never, {
    transports: [CHANNEL_TRANSPORT.SSE],
    fetchImpl,
    connectionKey: crypto.randomUUID(),
  }) as any
  await vi.waitFor(() => expect(endWire).toBeDefined())
  const transport = connection.transport
  // A batch POST hung on a dead TCP connection settles only when its wire aborts it.
  transport.post = (_body: unknown, signal: AbortSignal) =>
    new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason)))
  transport.outbox = [{ frame: encode.window(0, 65_536), deadline: 0 }]
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
  connection.drainBufferedFrames(new Set([0]))
  expect(connection.replayBuffers.get(0).getAfter(0)).toHaveLength(1)
  connection.dispose()
})

test('a sent frame stays replayable through the pong deadline and the reconnect timeout after it', () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://replay-age.test', channel as never, stalledOptions()) as any
  const pingInterval = 2 * CHANNEL_PING_INTERVAL_MS
  connection.applyReconciled(reconciled({ sessionId: 'age', open: [], pingInterval }), null)
  const replay = connection.replayBuffers.get(0)
  replay.push(replay.nextSeq(), encode.text(0, 'sent as the wire died', 1))
  replay.evict(Date.now() + 2 * pingInterval + CHANNEL_RECONNECT_TIMEOUT_MS)
  expect(replay.getAfter(0)).toHaveLength(1)
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

test('a reconnect re-attaches a channel still closing, so its close request can go out again', () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://closing.test', channel as never, stalledOptions()) as any
  connection.buildReconcileFrame()
  channel.isClosed = true
  const outcome = connection.applyReconciled(reconciled({ sessionId: 'closing', open: [{ ix: 0, lastSeq: 0 }] }), null)
  expect(outcome.channelsToOpen).toEqual([channel])
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
  connection.sendAbort(channel)
  connection.unregister(channel)
  const { frames } = connection.applyReconciled(
    reconciled({ sessionId: 'draining', open: [{ ix: 0, lastSeq: 0 }] }),
    null,
  )
  const sent = frames.map(({ frame }: { frame: Uint8Array<ArrayBuffer> }) => decode(frame))
  expect(sent.map((frame: { tag: number; seq?: number }) => [frame.tag, frame.seq])).toEqual([
    [TAG.TEXT, 1],
    [TAG.TEXT, 2],
    [TAG.CLOSE, undefined],
  ])
  connection.dispose()
})

test('a channel whose abort went out with a reconcile on a wire that then died is left out of the next reconcile, though its listener answered after', () => {
  const closing = createChannel()
  const options = stalledOptions()
  const connection = ClientConnection.getOrCreate('http://draining-lost.test', closing as never, options) as any
  connection.buildReconcileFrame()
  connection.applyReconciled(reconciled({ sessionId: 'lost', open: [{ ix: 0, lastSeq: 0 }] }), null)
  ClientConnection.getOrCreate('http://draining-lost.test', createChannel() as never, options) // a call's callback
  connection.buildReconcileFrame() // its registration: sends wait for the RECONCILED
  connection.sendAbort(closing)
  connection.unregister(closing)
  connection.stageReconcileBatch() // the abort leaves with the registration's reconcile
  connection.sendAckRes(closing, 1, '"answer"') // its async listener answers a server send({ ack: true }) after that
  connection.handleTransportLoss(new Error('the wire died'))
  const reconcile = decode(connection.buildReconcileFrame().frame) as { payload: { open: { ix: number }[] } }
  expect(reconcile.payload.open.map((entry) => entry.ix)).toEqual([1])
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
    // suppressed while reconciling, so the dead wire is never noticed. The watchdog must
    // instead time out the reconcile and reconnect.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    expect(getSseDownstreamOpens()).toBeGreaterThanOrEqual(2)
  })
})
