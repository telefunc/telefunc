import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  RECONCILE_TIMEOUT_MS,
} from '../../constants.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { ChannelClosedError } from '../channelErrors.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { decode, encode, isOneFrame, messageFrames, withTook } from '../../wire/frame.js'
import type { ReconcilePayload } from '../../wire/payloads.js'
import { TAG } from '../../wire/tags.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { ClientConnection } from './ClientConnection.js'
import { WireLifecycle } from './WireLifecycle.js'
import { WsTransport } from './WsTransport.js'
import { ClientChannel } from './ClientChannel.js'
import { fakeMuxChannel } from './fakeMuxChannel.fixture.js'

/** A `fetch` whose SSE downstream body emits only what a test hands it and never closes: a silently
 *  stalled stream (no error, no FIN), so the RECONCILED frame that clears `reconciling`
 *  never arrives. Counts how many times the downstream (`Accept: text/event-stream`) is
 *  opened, i.e. how many connect attempts ran. */
function createStalledTransport() {
  /** Each downstream opened, which delivers what it is handed until the page cancels it. */
  const downstreams: { deliver(chunk: Uint8Array): void }[] = []
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const headers = (init.headers ?? {}) as Record<string, string>
    let cancelled = false
    const body = new ReadableStream<Uint8Array>({
      start: (controller) => {
        if (headers.Accept !== 'text/event-stream') return
        downstreams.push({
          deliver: (chunk) => {
            if (!cancelled) controller.enqueue(chunk)
          },
        })
      },
      cancel: () => {
        cancelled = true
      },
    })
    return new Response(body, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
  }) as unknown as typeof fetch
  return { fetchImpl, downstreams, getSseDownstreamOpens: () => downstreams.length }
}

describe('SSE reconcile watchdog', () => {
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  test('reconnects when RECONCILED never arrives on a stalled downstream', async () => {
    vi.useFakeTimers()
    const { fetchImpl, getSseDownstreamOpens } = createStalledTransport()

    ClientConnection.getOrCreate('http://test.local/_telefunc', fakeMuxChannel(), {
      transports: [CHANNEL_TRANSPORT.SSE],
      fetchImpl,
      connectionKey: crypto.randomUUID(),
    })

    // `start()` defers the first openStream by one reconcile window; let it connect.
    await vi.advanceTimersByTimeAsync(100)
    expect(getSseDownstreamOpens()).toBe(1)

    // The downstream is silent, so RECONCILED never lands. Without the watchdog the
    // connection wedges here forever: pings keep flowing but `onPongTimeout` is
    // suppressed while reconciling, so the dead wire is never noticed. The watchdog must
    // instead time out the reconcile and reconnect.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    expect(getSseDownstreamOpens()).toBeGreaterThanOrEqual(2)
  })

  test('ends the channels at the reconnect timeout when wires open and none reconciles', async () => {
    vi.useFakeTimers()
    const { fetchImpl } = createStalledTransport()
    const ended: unknown[] = []
    const channel = fakeMuxChannel({ _onConnectionLost: (err) => ended.push(err) })

    ClientConnection.getOrCreate('http://test.local/_telefunc', channel, {
      transports: [CHANNEL_TRANSPORT.SSE],
      fetchImpl,
      connectionKey: crypto.randomUUID(),
    })

    // Each wire opens, its RECONCILE deadline drops it, and the page reconnects.
    await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_TIMEOUT_MS)
    expect(ended).toEqual([])
    // The timeout runs from the first loss, one deadline in; the first loss past it, a delay and a deadline on, ends them.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_MAX_DELAY_MS + RECONCILE_TIMEOUT_MS)
    expect(ended).toHaveLength(1)
    expect(ended[0]).toBeInstanceOf(NetworkError)
  })

  test('drops a wire that never reconciles while the wire it replaced still delivers', async () => {
    vi.useFakeTimers()
    const { fetchImpl, downstreams } = createStalledTransport()

    ClientConnection.getOrCreate('http://test.local/_telefunc', fakeMuxChannel(), {
      transports: [CHANNEL_TRANSPORT.SSE],
      fetchImpl,
      connectionKey: crypto.randomUUID(),
    })

    // The first wire's RECONCILE deadline drops it, and the page opens a second.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    expect(downstreams).toHaveLength(2)

    // The first wire's stream still delivers a PONG each second, which says nothing of the second wire.
    const pong = new TextEncoder().encode(`data: ${uint8ArrayToBase64url(encode.pong())}\n\n`)
    const trickle = setInterval(() => downstreams[0]!.deliver(pong), 1000)
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + 2 * CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    clearInterval(trickle)
    expect(downstreams).toHaveLength(3)
  })
})

/** A WebSocket that opens and delivers only what a test hands it: no RECONCILED ever comes. */
class SilentWebSocket {
  static OPEN = 1
  static opened: SilentWebSocket[] = []
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  binaryType = 'blob'
  readyState = 1
  bufferedAmount = 0

  constructor() {
    SilentWebSocket.opened.push(this)
    queueMicrotask(() => this.onopen?.())
  }
  send(): void {}
  close(): void {
    this.readyState = 3
  }
  deliver(frame: Uint8Array<ArrayBuffer>): void {
    this.onmessage?.({ data: frame.slice().buffer })
  }
}

/** A live server's WebSocket, as the page sees it: it answers each PING, saying how many of the page's bytes it took,
 *  and does with each RECONCILE it takes what `onReconcile` says. */
class ServerSocket {
  static OPEN = 1
  static made: ServerSocket[] = []
  static onReconcile: (socket: ServerSocket, reconcile: ReconcilePayload) => void = () => {}
  onopen: (() => void) | null = null
  onclose: ((event: { code: number }) => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  binaryType = 'blob'
  readyState = 1
  bufferedAmount = 0
  /** Whether it answers the page's PINGs. */
  answers = true
  /** The RECONCILEs the page sent on it, the seqs of its TEXT frames, and their texts by channel index. */
  readonly reconciles: ReconcilePayload[] = []
  readonly texts: number[] = []
  readonly textsOf = new Map<number, string[]>()
  readonly textOrder: string[] = []
  /** The bytes of the page's messages, and, while it takes them as a slow link carries them, since when and till when. */
  private sentBytes = 0
  private taking: { from: number; to: number; bytes: number } | null = null

  constructor() {
    ServerSocket.made.push(this)
    queueMicrotask(() => this.onopen?.())
  }
  send(message: Uint8Array<ArrayBuffer>): void {
    this.sentBytes += message.byteLength
    for (const raw of isOneFrame(message) ? [message] : messageFrames(message)) {
      const frame = decode(raw)
      if (frame.tag === TAG.PING && this.answers)
        queueMicrotask(() => this.deliver(withTook(encode.pong([]), this.took())))
      if (frame.tag === TAG.TEXT) {
        this.texts.push(frame.seq)
        this.textsOf.set(frame.index, [...(this.textsOf.get(frame.index) ?? []), frame.text])
        this.textOrder.push(frame.text)
      }
      if (frame.tag !== TAG.RECONCILE) continue
      this.reconciles.push(frame.payload)
      ServerSocket.onReconcile(this, frame.payload)
    }
  }
  /** Takes what the page sent so far over `ms`, as a slow link carries it. */
  takeSlowly(ms: number): void {
    this.taking = { from: performance.now(), to: performance.now() + ms, bytes: this.sentBytes }
  }
  private took(): number {
    const { taking } = this
    if (taking === null || performance.now() >= taking.to) return this.sentBytes
    return Math.floor((taking.bytes * (performance.now() - taking.from)) / (taking.to - taking.from))
  }
  close(): void {
    this.readyState = 3
  }
  /** The wire goes, as when a proxy restarts. */
  drop(): void {
    this.readyState = 3
    this.onclose?.({ code: 1006 })
  }
  deliver(frame: Uint8Array<ArrayBuffer>): void {
    if (this.readyState === 1) this.onmessage?.({ data: frame.slice().buffer })
  }
}

function reconciledFor({ open }: ReconcilePayload) {
  return encode.reconciled({
    sessionId: 'session',
    open: open.map(({ ix }) => ({ ix, lastSeq: 0 })),
    reconnectTimeout: 60_000,
    idleTimeout: 60_000,
    pingInterval: 5_000,
    maxFrameBytes: 64 * 1024 * 1024,
    creditWindow: 1_000_000,
    sseFlushThrottle: 300,
    ssePostIdleFlushDelay: 50,
    transports: ['ws'],
  })
}

/** How long a live server's upload takes before it has the page's RECONCILE whole. */
const UPLOAD_MS = 30_000

describe("a WebSocket RECONCILE's deadline", () => {
  const realWebSocket = globalThis.WebSocket
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
    vi.restoreAllMocks()
    globalThis.WebSocket = realWebSocket
    SilentWebSocket.opened = []
    ServerSocket.made = []
    ServerSocket.onReconcile = () => {}
  })

  /** A page on `ServerSocket`s; `add()` registers one more channel on its connection. Returns the channels' ids too. */
  function page() {
    vi.useFakeTimers()
    globalThis.WebSocket = ServerSocket as unknown as typeof WebSocket
    const options = {
      transports: [CHANNEL_TRANSPORT.WS],
      fetchImpl: createStalledTransport().fetchImpl,
      connectionKey: crypto.randomUUID(),
    }
    const ids: string[] = []
    const opened: string[] = []
    const add = () => {
      const channel = fakeMuxChannel()
      ids.push(channel.id)
      channel._onTransportOpen = () => void opened.push(channel.id)
      ClientConnection.getOrCreate('http://test.local/_telefunc', channel, options)
    }
    add()
    return { add, ids, opened }
  }

  test('drops a wire whose server sends nothing at its heartbeat deadline, while the socket it replaced still delivers', async () => {
    vi.useFakeTimers()
    globalThis.WebSocket = SilentWebSocket as unknown as typeof WebSocket
    ClientConnection.getOrCreate('http://test.local/_telefunc', fakeMuxChannel(), {
      transports: [CHANNEL_TRANSPORT.WS],
      fetchImpl: createStalledTransport().fetchImpl,
      connectionKey: crypto.randomUUID(),
    })
    // Before a first RECONCILED, the heartbeat beats at its shortest interval.
    const deadline = pingDeadline(1_000, CHANNEL_TRANSPORT.WS)
    await vi.advanceTimersByTimeAsync(deadline + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    expect(SilentWebSocket.opened).toHaveLength(2)

    // The first socket still delivers a PONG every 500 ms, which says nothing of the second wire.
    const trickle = setInterval(() => SilentWebSocket.opened[0]!.deliver(encode.pong()), 500)
    await vi.advanceTimersByTimeAsync(deadline + 2 * CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    clearInterval(trickle)
    expect(SilentWebSocket.opened).toHaveLength(3)
  })

  test.each(['first connect', 'resume', 'join'] as const)(
    "on a %s, a server that took the RECONCILE and answers every PING but never sends its RECONCILED has it given up one deadline after the receipt, and the next socket's RECONCILE names every channel",
    async (exchange) => {
      const { add, ids } = page()
      const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
      let receiptAt = Number.POSITIVE_INFINITY
      ServerSocket.onReconcile = (socket, reconcile) => {
        // Before the exchange under test, the server answers at once.
        if (exchange !== 'first connect' && ServerSocket.made.length === 1 && socket.reconciles.length === 1) {
          socket.deliver(encode.reconcileTaken())
          socket.deliver(reconciledFor(reconcile))
          return
        }
        if (receiptAt !== Number.POSITIVE_INFINITY) return
        receiptAt = performance.now() + UPLOAD_MS
        socket.takeSlowly(UPLOAD_MS)
        setTimeout(() => socket.deliver(encode.reconcileTaken()), UPLOAD_MS)
      }
      await vi.advanceTimersByTimeAsync(1_000)
      if (exchange === 'resume') ServerSocket.made[0]!.drop()
      if (exchange === 'join') add()
      await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_INITIAL_DELAY_MS)
      const lostBefore = lost.mock.calls.length
      const socketsBefore = ServerSocket.made.length

      // The upload, then one deadline from the receipt.
      await vi.advanceTimersByTimeAsync(receiptAt + RECONCILE_TIMEOUT_MS - 1 - performance.now())
      expect(lost.mock.calls.length).toBe(lostBefore)
      await vi.advanceTimersByTimeAsync(2)
      expect(lost.mock.calls.length).toBe(lostBefore + 1)

      await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_MAX_DELAY_MS)
      expect(ServerSocket.made.length).toBe(socketsBefore + 1)
      const [next] = ServerSocket.made.at(-1)!.reconciles
      expect(next?.open.map(({ id }) => id).sort()).toEqual([...ids].sort())
      expect(next?.sessionId).toBe(exchange === 'first connect' ? undefined : 'session')
    },
  )

  test.each(['before', 'after'] as const)(
    "a message that arrives at the heartbeat's deadline, run %s it: the wire stays, or it drops and the next carries the page's unacknowledged send once",
    async (order) => {
      vi.useFakeTimers()
      globalThis.WebSocket = ServerSocket as unknown as typeof WebSocket
      ServerSocket.onReconcile = (socket, reconcile) => {
        socket.deliver(encode.reconcileTaken())
        socket.deliver(reconciledFor(reconcile))
      }
      const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
      const page = new ClientChannel<string, string>({
        channelId: crypto.randomUUID(),
        connection: {
          telefuncUrl: 'http://test.local/_telefunc',
          transports: ['ws'],
          connectionKey: crypto.randomUUID(),
        },
      })
      await vi.advanceTimersByTimeAsync(100)
      void page.send('m')
      await vi.advanceTimersByTimeAsync(100)
      const [first] = ServerSocket.made
      first!.answers = false
      // The PONG at T0 sets the deadline at T0 + 10 s; a timer made before it, for the same time, runs first there.
      const atDeadline = () => first!.deliver(encode.pong([]))
      if (order === 'before') setTimeout(atDeadline, pingDeadline(5_000, CHANNEL_TRANSPORT.WS))
      first!.deliver(encode.pong([]))
      if (order === 'after') setTimeout(atDeadline, pingDeadline(5_000, CHANNEL_TRANSPORT.WS))
      await vi.advanceTimersByTimeAsync(pingDeadline(5_000, CHANNEL_TRANSPORT.WS) + 1)
      expect(lost).toHaveBeenCalledTimes(order === 'before' ? 0 : 1)
      await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_MAX_DELAY_MS)
      expect(ServerSocket.made.map(({ texts }) => texts)).toEqual(order === 'before' ? [[1]] : [[1], [1]])
      page.abort()
    },
  )

  test('a RECONCILE whose receipt comes after 30 s of PONGs, then its RECONCILED in pieces 9 s apart, opens its channel on that wire', async () => {
    const { opened, ids } = page()
    const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    ServerSocket.onReconcile = (socket, reconcile) => {
      const answer = reconciledFor(reconcile)
      const third = Math.ceil(answer.byteLength / 3)
      socket.takeSlowly(UPLOAD_MS)
      setTimeout(() => socket.deliver(encode.reconcileTaken()), UPLOAD_MS)
      for (let i = 0; i < 3; i++) {
        const piece = encode.piece(answer.byteLength, answer.subarray(i * third, (i + 1) * third))
        setTimeout(() => socket.deliver(piece), UPLOAD_MS + (i + 1) * 9_000)
      }
    }
    await vi.advanceTimersByTimeAsync(UPLOAD_MS + 3 * 9_000 + 1)
    expect(opened).toEqual(ids)
    expect(lost).not.toHaveBeenCalled()
    expect(ServerSocket.made).toHaveLength(1)
  })
})

describe("a page's channels on a live WebSocket", () => {
  const realWebSocket = globalThis.WebSocket
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
    globalThis.WebSocket = realWebSocket
    ServerSocket.made = []
    ServerSocket.onReconcile = () => {}
  })

  test("while another channel's join waits for its RECONCILED, a channel already open sends and receives on the wire", async () => {
    vi.useFakeTimers()
    globalThis.WebSocket = ServerSocket as unknown as typeof WebSocket
    // The server answers the first RECONCILE at once, and takes the join's without answering it.
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      if (socket.reconciles.length === 1) socket.deliver(reconciledFor(reconcile))
    }
    const connection = {
      telefuncUrl: 'http://test.local/_telefunc',
      transports: ['ws' as const],
      connectionKey: crypto.randomUUID(),
    }
    const open = new ClientChannel<string, string>({ channelId: crypto.randomUUID(), connection })
    const received: string[] = []
    open.listen((message) => void received.push(message))
    await vi.advanceTimersByTimeAsync(100)
    const [socket] = ServerSocket.made
    const ix = socket!.reconciles[0]!.open[0]?.ix
    const joining = new ClientChannel<string, string>({ channelId: crypto.randomUUID(), connection })
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.reconciles).toHaveLength(2)

    void open.send('during')
    socket!.deliver(encode.text(ix!, '"back"', 1))
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.texts).toEqual([1])
    expect(received).toEqual(['back'])
    expect(ServerSocket.made).toHaveLength(1)
    open.abort()
    joining.abort()
  })

  test("a channel the server awaited sends what it queued, then what it sends next, in order, once its ATTACH_RESULT comes during another channel's join", async () => {
    vi.useFakeTimers()
    globalThis.WebSocket = ServerSocket as unknown as typeof WebSocket
    // The server answers the first two RECONCILEs at once, the second awaiting its channel, and takes the third without
    // answering it.
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      if (socket.reconciles.length === 1) socket.deliver(reconciledFor(reconcile))
      if (socket.reconciles.length === 2) socket.deliver(reconciledFor({ ...reconcile, open: [] }))
    }
    const connection = {
      telefuncUrl: 'http://test.local/_telefunc',
      transports: ['ws' as const],
      connectionKey: crypto.randomUUID(),
    }
    const pages = [new ClientChannel<string, string>({ channelId: crypto.randomUUID(), connection })]
    await vi.advanceTimersByTimeAsync(100)
    pages.push(new ClientChannel<string, string>({ channelId: crypto.randomUUID(), connection }))
    await vi.advanceTimersByTimeAsync(100)
    const [socket] = ServerSocket.made
    const awaited = socket!.reconciles[1]!.open[0]?.ix
    void pages[1]!.send('queued')
    pages.push(new ClientChannel<string, string>({ channelId: crypto.randomUUID(), connection }))
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.reconciles).toHaveLength(3)

    socket!.deliver(encode.attachResult(awaited!, 0))
    void pages[1]!.send('next')
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textsOf.get(awaited!)).toEqual(['"queued"', '"next"'])
    expect(ServerSocket.made).toHaveLength(1)
    for (const page of pages) page.abort()
  })

  /** A page's channels on one connection, over `ServerSocket`s: `add()` registers one, `open()` also runs the timers a
   *  while. */
  function pageOf() {
    vi.useFakeTimers()
    globalThis.WebSocket = ServerSocket as unknown as typeof WebSocket
    const connection = {
      telefuncUrl: 'http://test.local/_telefunc',
      transports: ['ws' as const],
      connectionKey: crypto.randomUUID(),
    }
    const pages: ClientChannel<string, string>[] = []
    const add = () => {
      const page = new ClientChannel<string, string>({ channelId: crypto.randomUUID(), connection })
      pages.push(page)
      return page
    }
    const open = async () => {
      const page = add()
      await vi.advanceTimersByTimeAsync(100)
      return page
    }
    return { pages, add, open }
  }

  test("while a channel's join waits, an open channel's sends go at once until the joining channel sends, and then wait behind its send", async () => {
    const { pages, add, open } = pageOf()
    // The server answers every RECONCILE at once but the join's, which it answers when the row says.
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      if (socket.reconciles.length !== 2) socket.deliver(reconciledFor(reconcile))
    }
    const a = await open()
    const c = add()
    await vi.advanceTimersByTimeAsync(100)
    const [socket] = ServerSocket.made
    expect(socket!.reconciles).toHaveLength(2)
    void a.send('a1')
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textOrder).toEqual(['"a1"'])
    void c.send('c1')
    void a.send('a2')
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textOrder).toEqual(['"a1"'])
    socket!.deliver(reconciledFor(socket!.reconciles[1]!))
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textOrder).toEqual(['"a1"', '"c1"', '"a2"'])
    for (const page of pages) page.abort()
  })

  test("a send waits behind a joining channel's send though the sends of a channel the server awaited went ahead of it at an earlier join's RECONCILED", async () => {
    const { pages, add, open } = pageOf()
    // The server answers the first RECONCILE, awaits the channel the second names, and answers the rest when the row
    // says.
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      if (socket.reconciles.length === 1) socket.deliver(reconciledFor(reconcile))
      if (socket.reconciles.length === 2) socket.deliver(reconciledFor({ ...reconcile, open: [] }))
    }
    /** The RECONCILED of the `n`th RECONCILE, attaching what it names but the closed channel, which the server let go. */
    const answer = async (n: number) => {
      const reconcile = socket!.reconciles[n]!
      const closed = n === 4 ? socket!.reconciles[1]!.open[0]!.ix : undefined
      socket!.deliver(reconciledFor({ ...reconcile, open: reconcile.open.filter(({ ix }) => ix !== closed) }))
      await vi.advanceTimersByTimeAsync(100)
    }
    const a = await open()
    const b = await open()
    const [socket] = ServerSocket.made
    void b.send('b1')
    // A join, during which a send passes what the awaited channel queued.
    await open()
    void a.send('a1')
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textOrder).toEqual(['"a1"'])
    // The awaited channel closes, and a registration's follow-up names it; the server has it.
    b.abort()
    add()
    await vi.advanceTimersByTimeAsync(100)
    await answer(2)
    expect(socket!.reconciles).toHaveLength(4)
    await answer(3)
    expect(socket!.textOrder).toEqual(['"a1"', '"b1"'])
    // A later join's channel sends, then an open one.
    const g = add()
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.reconciles).toHaveLength(5)
    void g.send('g1')
    void a.send('a2')
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textOrder).toEqual(['"a1"', '"b1"'])
    await answer(4)
    expect(socket!.textOrder).toEqual(['"a1"', '"b1"', '"g1"', '"a2"'])
    for (const page of pages) page.abort()
  })

  test('the unacknowledged sends of two channels go again on the next wire in the order they were made', async () => {
    const { pages, open } = pageOf()
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      socket.deliver(reconciledFor(reconcile))
    }
    const [a, b] = [await open(), await open()]
    void b!.send('21')
    void a!.send('11')
    await vi.advanceTimersByTimeAsync(100)
    expect(ServerSocket.made[0]!.textOrder).toEqual(['"21"', '"11"'])
    ServerSocket.made[0]!.drop()
    await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_MAX_DELAY_MS)
    expect(ServerSocket.made).toHaveLength(2)
    expect(ServerSocket.made[1]!.textOrder).toEqual(['"21"', '"11"'])
    for (const page of pages) page.abort()
  })

  test("sends two open channels make while a third channel's join waits, and that the next wire replays, reach it in the order they were made", async () => {
    const { pages, open } = pageOf()
    // On the first wire the server answers the first two RECONCILEs, and takes the join's without answering it; the
    // next wire's it answers at once.
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      if (ServerSocket.made.indexOf(socket) > 0 || socket.reconciles.length < 3)
        socket.deliver(reconciledFor(reconcile))
    }
    const [a, b] = [await open(), await open()]
    await open()
    void b!.send('21')
    void a!.send('11')
    // A deadline from the receipt, the page gives the join up, and the next wire's RECONCILED takes the replay.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_MAX_DELAY_MS)
    expect(ServerSocket.made).toHaveLength(2)
    expect(ServerSocket.made[1]!.textOrder).toEqual(['"21"', '"11"'])
    for (const page of pages) page.abort()
  })

  test('on a resumed wire, a send that a channel the server left out makes from its close callback goes behind the replay of what the page sent before', async () => {
    const { pages, open } = pageOf()
    // The next wire's RECONCILED leaves out the channel opened second.
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      const [first] = ServerSocket.made
      const leftOut = first!.reconciles[1]?.open[0]?.ix
      if (socket === first) socket.deliver(reconciledFor(reconcile))
      else socket.deliver(reconciledFor({ ...reconcile, open: reconcile.open.filter(({ ix }) => ix !== leftOut) }))
    }
    const [a, b] = [await open(), await open()]
    b!.onClose(() => void a!.send('a2'))
    void a!.send('a1')
    await vi.advanceTimersByTimeAsync(100)
    ServerSocket.made[0]!.drop()
    await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_MAX_DELAY_MS)
    expect(ServerSocket.made).toHaveLength(2)
    expect(ServerSocket.made[1]!.textOrder).toEqual(['"a1"', '"a2"'])
    for (const page of pages) page.abort()
  })

  test("a channel the next wire's server awaits stays held there, though the wire the page gave up delivers that channel's ATTACH_RESULT before the next wire's RECONCILED", async () => {
    const { pages, open } = pageOf()
    // The first wire's server awaits the channel opened second and takes the third one's join without answering it. The
    // next wire's awaits that channel too, and answers once the first wire delivered its ATTACH_RESULT.
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      const [first] = ServerSocket.made
      const awaited = first!.reconciles[1]?.open[0]?.ix
      if (socket === first) {
        if (socket.reconciles.length === 1) socket.deliver(reconciledFor(reconcile))
        if (socket.reconciles.length === 2) socket.deliver(reconciledFor({ ...reconcile, open: [] }))
        return
      }
      first!.deliver(encode.attachResult(awaited!, 0))
      socket.deliver(reconciledFor({ ...reconcile, open: reconcile.open.filter(({ ix }) => ix !== awaited) }))
    }
    await open()
    const b = await open()
    const awaited = ServerSocket.made[0]!.reconciles[1]!.open[0]?.ix
    void b.send('b1')
    await open()
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_MAX_DELAY_MS)
    expect(ServerSocket.made).toHaveLength(2)
    const next = ServerSocket.made[1]!
    expect(next.textsOf.get(awaited!)).toBeUndefined()
    next.deliver(encode.attachResult(awaited!, 0))
    await vi.advanceTimersByTimeAsync(100)
    expect(next.textsOf.get(awaited!)).toEqual(['"b1"'])
    for (const page of pages) page.abort()
  })

  test("while another channel's join waits, an open channel's send that waited for headroom goes once the wire has room", async () => {
    const { pages, open } = pageOf()
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      if (socket.reconciles.length === 1) socket.deliver(reconciledFor(reconcile))
    }
    const register = vi.spyOn(WireLifecycle.prototype, 'register')
    const a = await open()
    const lifecycle = register.mock.contexts[0] as WireLifecycle
    register.mockRestore()
    await open()
    const [socket] = ServerSocket.made
    const headroom = vi.spyOn(WsTransport.prototype, 'headroom').mockReturnValue(0)
    void a.send('a1')
    await vi.advanceTimersByTimeAsync(100)
    const transport = headroom.mock.contexts[0] as WsTransport
    headroom.mockRestore()
    expect(socket!.textOrder).toEqual([])
    lifecycle.onTransportHeadroom(transport)
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textOrder).toEqual(['"a1"'])
    for (const page of pages) page.abort()
  })

  test("a send queued behind a joining channel's goes once the server aborts that channel, before the join's RECONCILED", async () => {
    const { pages, add, open } = pageOf()
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      if (socket.reconciles.length === 1) socket.deliver(reconciledFor(reconcile))
    }
    const a = await open()
    const c = add()
    void c.send('c1')
    void a.send('a1')
    await vi.advanceTimersByTimeAsync(100)
    const [socket] = ServerSocket.made
    expect(socket!.reconciles).toHaveLength(2)
    expect(socket!.textOrder).toEqual([])
    const joining = socket!.reconciles[1]!.open[0]?.ix
    socket!.deliver(encode.abort(joining!, '"gone"', 1))
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textOrder).toEqual(['"a1"'])
    for (const page of pages) page.abort()
  })

  test("a send queued behind a closed channel's goes once a PONG says the server no longer has that channel, before the RECONCILE that names it is answered", async () => {
    const { pages, add, open } = pageOf()
    // The server answers the first two RECONCILEs at once and the third, a join, when the row says; the follow-up that
    // names the closed channel it doesn't answer.
    ServerSocket.onReconcile = (socket, reconcile) => {
      socket.deliver(encode.reconcileTaken())
      if (socket.reconciles.length <= 2) socket.deliver(reconciledFor(reconcile))
    }
    const a = await open()
    const x = await open()
    add()
    await vi.advanceTimersByTimeAsync(100)
    const [socket] = ServerSocket.made
    expect(socket!.reconciles).toHaveLength(3)
    // Registered behind the join: its send holds the rest till a RECONCILE names it.
    const u = add()
    void u.send('u1')
    void x.send('x1')
    x.abort()
    void a.send('a1')
    await vi.advanceTimersByTimeAsync(100)
    socket!.deliver(reconciledFor(socket!.reconciles[2]!))
    await vi.advanceTimersByTimeAsync(100)
    // The follow-up names the closed channel and the one registered behind the join, which the server then ends.
    const xEntry = socket!.reconciles[1]!.open[0]
    const [xIx, xId] = [xEntry?.ix, xEntry?.id]
    const followUp = socket!.reconciles[3]!.open
    expect(followUp.map(({ id }) => id)).toContain(xId)
    const uIx = followUp.find(({ id }) => id !== xId)!.ix
    socket!.deliver(encode.abort(uIx, '"gone"', 1))
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textOrder).toEqual([])
    // The server no longer has the closed channel.
    socket!.deliver(encode.pong([{ ix: xIx!, lastSeq: null }]))
    await vi.advanceTimersByTimeAsync(100)
    expect(socket!.textOrder).toEqual(['"a1"'])
    for (const page of pages) page.abort()
  })
})

describe('a wire the server refused for a frame it could not read', () => {
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  test("its event stream's REJECTED event ends the channels with the refusal, and the page opens no other wire", async () => {
    vi.useFakeTimers()
    const { fetchImpl, downstreams } = createStalledTransport()
    const ended: unknown[] = []
    ClientConnection.getOrCreate(
      'http://test.local/_telefunc',
      fakeMuxChannel({ _onEnded: (err) => ended.push(err) }),
      {
        transports: [CHANNEL_TRANSPORT.SSE],
        fetchImpl,
        connectionKey: crypto.randomUUID(),
      },
    )
    await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_INITIAL_DELAY_MS)
    expect(downstreams).toHaveLength(1)

    downstreams.at(0)?.deliver(new TextEncoder().encode(`data: ${uint8ArrayToBase64url(encode.rejected())}\n\n`))
    await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_TIMEOUT_MS)

    expect(ended).toHaveLength(1)
    expect(ended[0]).toBeInstanceOf(ChannelClosedError)
    expect(ended[0]).toHaveProperty('message', expect.stringContaining('the server refused a frame it could not read'))
    expect(downstreams).toHaveLength(1)
  })
})

describe("a first connect's deadline", () => {
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  test("ends a first connect at the deadline from its start, with a later attempt's refusal after a first open got no answer", async () => {
    vi.useFakeTimers()
    let opens = 0
    const fetchImpl: typeof fetch = async (_input, init) => {
      if (new Headers(init?.headers).get('Accept') !== 'text/event-stream') return new Response(null, { status: 200 })
      if (++opens === 1) throw new TypeError('fetch failed')
      return new Response('<!doctype html>', { status: 502, headers: { 'Content-Type': 'text/html' } })
    }
    const ended: unknown[] = []
    ClientConnection.getOrCreate(
      'http://test.local/_telefunc',
      fakeMuxChannel({ _onConnectionLost: (err) => ended.push(err) }),
      { transports: [CHANNEL_TRANSPORT.SSE], fetchImpl, connectionKey: crypto.randomUUID() },
    )
    // The defaults' window over SSE: 2.5 pings of 5 s, then 60 s.
    await vi.advanceTimersByTimeAsync(2.5 * 5_000 + CHANNEL_RECONNECT_TIMEOUT_MS + 1)
    expect(opens).toBeGreaterThan(2)
    expect(ended).toHaveLength(1)
    expect(ended[0]).toBeInstanceOf(NetworkError)
    expect(ended[0]).toHaveProperty('message', 'Server rejected SSE connection with 502 text/html')
  })
})
