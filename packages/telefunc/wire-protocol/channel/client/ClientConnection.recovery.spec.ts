import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { CHANNEL_TRANSPORT, RECONCILE_TIMEOUT_MS } from '../../constants.js'
import { encode, isOneFrame, messageFrames, withTook } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ClientConnection } from './ClientConnection.js'
import { WireLifecycle } from './WireLifecycle.js'
import { fakeMuxChannel } from './fakeMuxChannel.fixture.js'

const PING_INTERVAL_MS = 5_000
const RECONNECT_TIMEOUT_MS = 20_000
/** How long the server may hold the page's channels after the page lost its wire: until its ping deadline notices,
 *  then `reconnectTimeout` (wire/pingDeadline.ts `reconnectWindowOf`). */
const RECOVERY_WINDOW_MS = 2 * PING_INTERVAL_MS + RECONNECT_TIMEOUT_MS
/** The same before any RECONCILED said the server's settings: from the defaults, 5 s and 60 s. */
const DEFAULT_RECOVERY_WINDOW_MS = 2 * 5_000 + 60_000
/** The settings the scripted server's RECONCILED says. */
const serverSettings = { pingInterval: PING_INTERVAL_MS, reconnectTimeout: RECONNECT_TIMEOUT_MS }
/** 1 s pings and a 1 s `reconnectTimeout`: the server holds the page's channels 3 s after it lost the wire. */
const SHORT_WINDOW = { pingInterval: 1_000, reconnectTimeout: 1_000 }

/** What a socket the page makes does: reconciles `afterMs` after it opens; opens `afterMs` after it was made, then
 *  reconciles (`despiteClose`: even where the page closed it before, as a runtime may); opens and takes the page's
 *  bytes over `takesMs`, as a slow link carries them, then reconciles; opens and answers PINGs only, as a proxy for a
 *  server it lost; fails at once as a refused connect, `error` then `close` in one task; fails with `error` alone, as
 *  Node 22's after a reset at accept; never opens nor fails, as one whose upgrade gets no answer (`hangs22`: and fires
 *  `error` twice, never `close`, once the page closes it, as Node 22's); or can't be made: its constructor throws. */
type Script =
  | { reconcile: { afterMs: number } }
  | { takes: { takesMs: number } }
  | 'pingsOnly'
  | { opens: { afterMs: number; despiteClose?: boolean } }
  | 'refused'
  | 'errors'
  | 'hangs'
  | 'hangs22'
  | 'throws'

/** A WebSocket that plays the next script: the page's sockets, when it made each, and whether it closed them. Open,
 *  it answers the page's PINGs and takes its RECONCILEs, as a live server does. */
class ScriptedWebSocket {
  static OPEN = 1
  static scripts: Script[] = []
  static made: ScriptedWebSocket[] = []
  onopen: (() => void) | null = null
  onclose: ((event: { code: number }) => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  binaryType = 'blob'
  readyState = 0
  bufferedAmount = 0
  readonly at = performance.now()
  closedByPage = false
  /** The messages the page sent on it. */
  sent = 0
  /** The most of the page's sockets connecting at once, none open yet. */
  static mostConnecting = 0
  private readonly despiteClose: boolean
  private readonly errorsOnClose: boolean
  /** Whether it takes the page's RECONCILE, and when it has taken all the page sent by then, where it takes it slowly. */
  private readonly takesReconcile: boolean = true
  private readonly takenBy: number | null = null
  /** The bytes of the page's messages. */
  private sentBytes = 0

  constructor() {
    if (ScriptedWebSocket.scripts[0] === 'throws') {
      ScriptedWebSocket.scripts.shift()
      throw new SyntaxError('scripted')
    }
    ScriptedWebSocket.made.push(this)
    const connecting = ScriptedWebSocket.made.filter((ws) => ws.readyState === 0 && !ws.closedByPage).length
    ScriptedWebSocket.mostConnecting = Math.max(ScriptedWebSocket.mostConnecting, connecting)
    const script = ScriptedWebSocket.scripts.shift() ?? 'refused'
    this.despiteClose = typeof script === 'object' && 'opens' in script && script.opens.despiteClose === true
    this.errorsOnClose = script === 'hangs22'
    if (script === 'hangs' || script === 'hangs22' || script === 'throws') return
    if (script === 'pingsOnly') {
      this.takesReconcile = false
      queueMicrotask(() => this.open())
      return
    }
    if (typeof script === 'object' && 'takes' in script) {
      this.takesReconcile = false
      this.takenBy = this.at + script.takes.takesMs
      queueMicrotask(() => this.open())
      setTimeout(() => {
        this.deliver(encode.reconcileTaken())
        this.deliver(reconciled())
      }, script.takes.takesMs)
      return
    }
    if (script === 'refused') {
      queueMicrotask(() => {
        this.readyState = 3
        this.onerror?.()
        this.onclose?.({ code: 1006 })
      })
      return
    }
    if (script === 'errors') {
      queueMicrotask(() => this.onerror?.())
      return
    }
    if ('opens' in script) {
      // A page that took no open sends no RECONCILE, so the server answers none.
      setTimeout(() => this.open() && this.deliver(reconciled()), script.opens.afterMs)
      return
    }
    queueMicrotask(() => this.open())
    setTimeout(() => this.deliver(reconciled()), script.reconcile.afterMs)
  }
  /** Whether the page took the open. */
  private open(): boolean {
    if (this.readyState !== 0 || (this.closedByPage && !this.despiteClose) || !this.onopen) return false
    this.readyState = 1
    this.onopen()
    return this.readyState === 1
  }
  send(message: Uint8Array<ArrayBuffer>): void {
    this.sent++
    this.sentBytes += message.byteLength
    for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
      if (frame[0] === TAG.PING) queueMicrotask(() => this.deliver(withTook(encode.pong([]), this.took())))
      if (frame[0] === TAG.RECONCILE && this.takesReconcile) queueMicrotask(() => this.deliver(encode.reconcileTaken()))
    }
  }
  /** How many of the page's bytes it took: all it got, or, taking them slowly, as many as the time since it opened gives. */
  private took(): number {
    if (this.takenBy === null) return this.takesReconcile ? this.sentBytes : 0
    const share = Math.min(1, (performance.now() - this.at) / (this.takenBy - this.at))
    return Math.floor(this.sentBytes * share)
  }
  close(): void {
    this.closedByPage = true
    if (this.readyState === 0 && this.errorsOnClose) {
      this.readyState = 2
      queueMicrotask(() => {
        this.onerror?.()
        this.onerror?.()
      })
      return
    }
    if (this.readyState === 0 && this.despiteClose) return
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

function reconciled() {
  return encode.reconciled({
    sessionId: 'session',
    open: [{ ix: 0, lastSeq: 0 }],
    reconnectTimeout: serverSettings.reconnectTimeout,
    idleTimeout: 60_000,
    pingInterval: serverSettings.pingInterval,
    maxFrameBytes: 64 * 1024 * 1024,
    creditWindow: 1_000_000,
    sseFlushThrottle: 300,
    ssePostIdleFlushDelay: 50,
    transports: ['ws'],
  })
}

/** A page whose first socket reconciles `handshakeMs` after it opens, then sockets as `then` scripts them; its wire
 *  drops once it reconciled. Returns when the wire dropped, and what ended its channel. */
async function lostWire({ handshakeMs = 0, openMs, then }: { handshakeMs?: number; openMs?: number; then: Script[] }) {
  const first: Script = openMs === undefined ? { reconcile: { afterMs: handshakeMs } } : { opens: { afterMs: openMs } }
  ScriptedWebSocket.scripts = [first, ...then]
  const ended: unknown[] = []
  const connectionKey = crypto.randomUUID()
  const options = { transports: [CHANNEL_TRANSPORT.WS], fetchImpl: fetch, connectionKey }
  ClientConnection.getOrCreate(
    'http://test.local/_telefunc',
    fakeMuxChannel({ _onConnectionLost: (e) => ended.push(e) }),
    options,
  )
  await vi.advanceTimersByTimeAsync(Math.max(handshakeMs, openMs ?? 0) + 10)
  const wire = ScriptedWebSocket.made[0]
  if (!wire) throw new Error('the page made no socket')
  wire.drop()
  const lostAt = performance.now()
  /** Registers another channel on the same connection. */
  const register = (channel = fakeMuxChannel()) =>
    ClientConnection.getOrCreate('http://test.local/_telefunc', channel, options)
  return { lostAt, ended, register }
}

/** When each socket made since the loss was made, from the loss. */
const madeSince = (lostAt: number) =>
  ScriptedWebSocket.made.filter((ws) => ws.at >= lostAt).map((ws) => Math.round(ws.at - lostAt))

describe("a page's reconnects", () => {
  const realWebSocket = globalThis.WebSocket
  beforeEach(() => {
    vi.useFakeTimers()
    globalThis.WebSocket = ScriptedWebSocket as unknown as typeof WebSocket
  })
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
    vi.restoreAllMocks()
    globalThis.WebSocket = realWebSocket
    ScriptedWebSocket.made = []
    ScriptedWebSocket.scripts = []
    ScriptedWebSocket.mostConnecting = 0
  })

  test('wait a random part of each delay of the backoff, the first and those at the cap too', async () => {
    const random = vi.spyOn(Math, 'random').mockReturnValue(0.25)
    const { lostAt } = await lostWire({ then: Array<Script>(6).fill('refused') })
    await vi.advanceTimersByTimeAsync(10_000)
    // A quarter of 500, 1,000, 2,000, 4,000, then of the 5,000 cap, each after the last refusal.
    expect(madeSince(lostAt).slice(0, 7)).toEqual([125, 375, 875, 1875, 3125, 4375, 5625])
    expect(random).toHaveBeenCalled()
  })

  test('start again from the first delay once a wire reconciled', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const { lostAt } = await lostWire({ then: ['refused', { reconcile: { afterMs: 0 } }, 'refused'] })
    // Half of 500, then half of 1,000: the third socket reconciles.
    await vi.advanceTimersByTimeAsync(750 + 10)
    const third = ScriptedWebSocket.made[2]
    if (!third) throw new Error('no third socket')
    third.drop()
    const relostAt = performance.now()
    await vi.advanceTimersByTimeAsync(1_000)
    expect(ScriptedWebSocket.made.slice(3).map((ws) => Math.round(ws.at - relostAt))).toEqual([250, 750])
    expect(madeSince(lostAt).slice(0, 2)).toEqual([250, 750])
  })
})

describe("a page's recovery", () => {
  const realWebSocket = globalThis.WebSocket
  beforeEach(() => {
    vi.useFakeTimers()
    globalThis.WebSocket = ScriptedWebSocket as unknown as typeof WebSocket
    vi.spyOn(Math, 'random').mockReturnValue(0.9)
  })
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
    vi.restoreAllMocks()
    globalThis.WebSocket = realWebSocket
    ScriptedWebSocket.made = []
    ScriptedWebSocket.scripts = []
    ScriptedWebSocket.mostConnecting = 0
    Object.assign(serverSettings, { pingInterval: PING_INTERVAL_MS, reconnectTimeout: RECONNECT_TIMEOUT_MS })
  })

  test('ends its channels at the deadline the server holds them to, not before', async () => {
    const { ended } = await lostWire({ then: [] })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 1)
    expect(ended).toEqual([])
    await vi.advanceTimersByTimeAsync(2)
    expect(ended).toHaveLength(1)
    expect(ended[0]).toMatchObject({ message: 'Connection dropped' })
  })

  test('cuts an attempt that never opens at its allowance, 3 s, then 6 s, then 12 s, one socket at a time; the last, after which none could reconcile, runs to the deadline, which ends the channels', async () => {
    const closed = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    const { lostAt, ended } = await lostWire({ then: Array<Script>(8).fill('hangs') })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 1)
    expect(ended).toEqual([])
    // 0.9 of 500, 1,000, 2,000 and 4,000 after each cut: at 450 + 3,000; 4,350 + 6,000; 12,150 + 12,000. One 0.9 of
    // 5,000 after the attempt at 27,750 would start past the deadline, so it isn't cut.
    expect(madeSince(lostAt)).toEqual([450, 4350, 12150, 27750])
    expect(ScriptedWebSocket.mostConnecting).toBe(1)
    // The wire's drop, then each cut.
    expect(closed).toHaveBeenCalledTimes(4)
    expect(ScriptedWebSocket.made.at(-1)?.closedByPage).toBe(false)
    await vi.advanceTimersByTimeAsync(2)
    expect(ended).toHaveLength(1)
    expect(ScriptedWebSocket.made.map((ws) => ws.closedByPage)).toEqual([false, true, true, true, true])
  })

  test('cuts an attempt that never opens 3 s on and backs off from the cut; the next socket is the wire', async () => {
    const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
    const { lostAt, ended } = await lostWire({ then: ['hangs', { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(5_000)
    // 0.9 of 500, then 0.9 of 1,000 after the cut at 3,450.
    expect(madeSince(lostAt)).toEqual([450, 4350])
    const [, hung, next] = ScriptedWebSocket.made
    expect([hung?.closedByPage, hung?.sent]).toEqual([true, 0])
    expect([next?.closedByPage, next?.readyState]).toEqual([false, 1])
    expect(next?.sent).toBeGreaterThan(0)
    // The first wire's open, then the next socket's.
    expect(open).toHaveBeenCalledTimes(2)
    expect(ended).toEqual([])
  })

  test('gives an attempt three times as long as the last open took, where that is past 3 s', async () => {
    const { lostAt, ended } = await lostWire({ openMs: 2_500, then: ['hangs', { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(10_000)
    // Cut 7,500 after 450; the next 0.9 of 1,000 later.
    expect(madeSince(lostAt)).toEqual([450, 8850])
    expect(ended).toEqual([])
  })

  test('after an open of 1.3 s gives the next attempt 3.9 s', async () => {
    const { lostAt } = await lostWire({ openMs: 1_300, then: ['hangs', { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(6_000)
    expect(madeSince(lostAt)).toEqual([450, 5250])
  })

  test('takes a socket that opens slower than three times the last open, inside the 3 s every attempt has', async () => {
    const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
    const { lostAt, ended } = await lostWire({ then: Array<Script>(4).fill({ opens: { afterMs: 1_500 } }) })
    expect(open).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1_949)
    expect(open).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(2)
    expect(open).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(5_000)
    expect(madeSince(lostAt)).toEqual([450])
    expect(ended).toEqual([])
  })

  test('over a link whose open takes 4 s, cuts the first attempt at 3 s and takes the second, given 6 s', async () => {
    const { lostAt, ended } = await lostWire({ then: Array<Script>(4).fill({ opens: { afterMs: 4_000 } }) })
    await vi.advanceTimersByTimeAsync(10_000)
    expect(madeSince(lostAt)).toEqual([450, 4350])
    expect(ScriptedWebSocket.made.slice(1).map((ws) => [ws.readyState, ws.closedByPage])).toEqual([
      [3, true],
      [1, false],
    ])
    expect(ended).toEqual([])
  })

  test('where every open takes 10 s, takes the attempt whose allowance first reaches it, one socket at a time', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.2)
    const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
    const { lostAt, ended } = await lostWire({ then: Array<Script>(4).fill({ opens: { afterMs: 10_000 } }) })
    await vi.advanceTimersByTimeAsync(21_000)
    // 0.2 of 500, cut 3 s on; 0.2 of 1,000, cut 6 s on; 0.2 of 2,000: given 10,150, half of what is left, it opens.
    expect(madeSince(lostAt)).toEqual([100, 3300, 9700])
    expect(ScriptedWebSocket.mostConnecting).toBe(1)
    expect(ScriptedWebSocket.made.slice(1).map((ws) => ws.readyState)).toEqual([3, 3, 1])
    expect(open).toHaveBeenCalledTimes(2)
    expect(ended).toEqual([])
  })

  test('closes a socket it cut that opens all the same, before any frame', async () => {
    const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
    await lostWire({ then: [{ opens: { afterMs: 4_000, despiteClose: true } }, { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(6_000)
    const [, late, next] = ScriptedWebSocket.made
    expect([late?.readyState, late?.sent]).toEqual([3, 0])
    expect(next?.readyState).toBe(1)
    expect(open).toHaveBeenCalledTimes(2)
  })

  test('cuts a first connect that never opens 3 s on, as it measured no open', async () => {
    ScriptedWebSocket.scripts = ['hangs', { reconcile: { afterMs: 0 } }]
    const ended: unknown[] = []
    ClientConnection.getOrCreate(
      'http://test.local/_telefunc',
      fakeMuxChannel({ _onConnectionLost: (e) => ended.push(e) }),
      { transports: [CHANNEL_TRANSPORT.WS], fetchImpl: fetch, connectionKey: crypto.randomUUID() },
    )
    const startedAt = performance.now()
    await vi.advanceTimersByTimeAsync(4_000)
    expect(ScriptedWebSocket.made.map((ws) => Math.round(ws.at - startedAt))).toEqual([0, 3450])
    expect(ScriptedWebSocket.made.map((ws) => ws.closedByPage)).toEqual([true, false])
    expect(ended).toEqual([])
  })

  test('gives up a first connect whose server answers its PINGs and never takes its RECONCILE 10 s after it, and opens the channel on the next socket', async () => {
    ScriptedWebSocket.scripts = ['pingsOnly', { reconcile: { afterMs: 0 } }]
    const ended: unknown[] = []
    const channel = fakeMuxChannel({ _onConnectionLost: (e) => ended.push(e) })
    const opened = vi.spyOn(channel, '_onTransportOpen')
    ClientConnection.getOrCreate('http://test.local/_telefunc', channel, {
      transports: [CHANNEL_TRANSPORT.WS],
      fetchImpl: fetch,
      connectionKey: crypto.randomUUID(),
    })
    const startedAt = performance.now()
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + 5_000)
    const [first, second] = ScriptedWebSocket.made
    expect(ScriptedWebSocket.made).toHaveLength(2)
    expect(first?.closedByPage).toBe(true)
    expect([second?.readyState, second?.closedByPage]).toEqual([1, false])
    expect(Math.round((second?.at ?? 0) - startedAt)).toBeGreaterThanOrEqual(RECONCILE_TIMEOUT_MS)
    expect(opened).toHaveBeenCalledTimes(1)
    expect(ended).toEqual([])
  })

  test('keeps a first connect whose server takes its bytes as a slow link carries them, past 10 s, and then reconciles', async () => {
    ScriptedWebSocket.scripts = [{ takes: { takesMs: 2.5 * RECONCILE_TIMEOUT_MS } }]
    const ended: unknown[] = []
    const channel = fakeMuxChannel({ _onConnectionLost: (e) => ended.push(e) })
    const opened = vi.spyOn(channel, '_onTransportOpen')
    ClientConnection.getOrCreate('http://test.local/_telefunc', channel, {
      transports: [CHANNEL_TRANSPORT.WS],
      fetchImpl: fetch,
      connectionKey: crypto.randomUUID(),
    })
    await vi.advanceTimersByTimeAsync(3 * RECONCILE_TIMEOUT_MS)
    expect(ScriptedWebSocket.made.map((ws) => [ws.readyState, ws.closedByPage])).toEqual([[1, false]])
    expect(opened).toHaveBeenCalledTimes(1)
    expect(ended).toEqual([])
  })

  /** A page's first channel, whose connection makes sockets as `scripts` say; what ended the channel. */
  function firstConnect(scripts: Script[]) {
    ScriptedWebSocket.scripts = scripts
    const ended: unknown[] = []
    ClientConnection.getOrCreate(
      'http://test.local/_telefunc',
      fakeMuxChannel({ _onConnectionLost: (e) => ended.push(e) }),
      { transports: [CHANNEL_TRANSPORT.WS], fetchImpl: fetch, connectionKey: crypto.randomUUID() },
    )
    return ended
  }

  test.each(['hangs', 'refused'] as const)(
    'ends a first connect whose every socket %s at the deadline from its start: the connection was never established',
    async (script) => {
      const ended = firstConnect(Array<Script>(40).fill(script))
      await vi.advanceTimersByTimeAsync(DEFAULT_RECOVERY_WINDOW_MS - 1)
      expect(ended).toEqual([])
      await vi.advanceTimersByTimeAsync(2)
      expect(ended).toHaveLength(1)
      expect(ended[0]).toMatchObject({ name: 'NetworkError', message: 'Connection not established' })
      expect(ScriptedWebSocket.made.every((ws) => ws.closedByPage || ws.readyState === 3)).toBe(true)
    },
  )

  test('keeps a first connect that reconciles late inside its deadline, and the deadline goes with the RECONCILED', async () => {
    // Sockets at 0, 3.45, 10.35, 24.15 and 51.75 s hang, cut 3, 6, 12 and 24 s on, and the last 10.75 s on, where the
    // next still gets the 3 s any attempt is given; the one at 67 s reconciles, inside the deadline's last ping deadline,
    // so its unanswered pings don't end it.
    const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
    const ended = firstConnect([...Array<Script>(5).fill('hangs'), { reconcile: { afterMs: 0 } }])
    await vi.advanceTimersByTimeAsync(DEFAULT_RECOVERY_WINDOW_MS + 1)
    const wire = ScriptedWebSocket.made.at(-1)
    const firstAt = ScriptedWebSocket.made[0]?.at ?? 0
    expect(ScriptedWebSocket.made.map((ws) => Math.round(ws.at - firstAt))).toEqual([
      0, 3450, 10350, 24150, 51750, 67000,
    ])
    expect([wire?.readyState, wire?.closedByPage]).toEqual([1, false])
    expect(open).toHaveBeenCalledTimes(1)
    expect(ended).toEqual([])
  })

  test("reaches a first connect's server that comes back in the last second before the deadline, every attempt before refused", async () => {
    // Sockets at 0, 0.45, 1.35, 3.15 and 6.75 s, then every 4.5 s: the nineteenth, at 69.75 s, reconciles.
    const ended = firstConnect([...Array<Script>(18).fill('refused'), { reconcile: { afterMs: 0 } }])
    await vi.advanceTimersByTimeAsync(DEFAULT_RECOVERY_WINDOW_MS + 1)
    const firstAt = ScriptedWebSocket.made[0]?.at ?? 0
    expect(ScriptedWebSocket.made.map((ws) => Math.round(ws.at - firstAt)).slice(-2)).toEqual([65250, 69750])
    expect(ScriptedWebSocket.made.at(-1)?.readyState).toBe(1)
    expect(ended).toEqual([])
  })

  test("lets a first connect's last attempt run to the deadline where a cut would leave the next less than the 3 s any attempt is given", async () => {
    const ended = firstConnect(Array<Script>(8).fill('hangs'))
    await vi.advanceTimersByTimeAsync(DEFAULT_RECOVERY_WINDOW_MS - 1)
    const firstAt = ScriptedWebSocket.made[0]?.at ?? 0
    expect(ScriptedWebSocket.made.map((ws) => Math.round(ws.at - firstAt))).toEqual([
      0, 3450, 10350, 24150, 51750, 67000,
    ])
    expect(ScriptedWebSocket.made.map((ws) => ws.closedByPage)).toEqual([true, true, true, true, true, false])
    expect(ended).toEqual([])
    await vi.advanceTimersByTimeAsync(2)
    expect(ended).toHaveLength(1)
    expect(ended[0]).toMatchObject({ message: 'Connection not established' })
  })

  /** The page's clock reads `at` from now on while its timers run as they were due, as for a page that slept or
   *  blocked its loop past `at`: the deadline's timer hasn't run yet. */
  const clockPast = (at: number) => vi.spyOn(performance, 'now').mockReturnValue(at)

  test('starts no attempt its backoff runs past the deadline, as after the page slept, and ends the channels', async () => {
    const { lostAt, ended } = await lostWire({ then: [{ reconcile: { afterMs: 0 } }] })
    clockPast(lostAt + RECOVERY_WINDOW_MS)
    // The backoff's 450 ms runs; the deadline's timer, 30 s on by the timers, doesn't.
    await vi.advanceTimersByTimeAsync(460)
    expect(ScriptedWebSocket.made).toHaveLength(1)
    expect(ended).toHaveLength(1)
    expect(ended[0]).toMatchObject({ name: 'NetworkError', message: 'Connection dropped' })
  })

  test("takes a RECONCILED handled past the deadline before the deadline's timer ran: the server still holds the channels", async () => {
    const { lostAt, ended } = await lostWire({ then: [{ reconcile: { afterMs: 100 } }] })
    await vi.advanceTimersByTimeAsync(460)
    expect(ScriptedWebSocket.made).toHaveLength(2)
    const clock = clockPast(lostAt + RECOVERY_WINDOW_MS)
    await vi.advanceTimersByTimeAsync(100)
    const wire = ScriptedWebSocket.made[1]
    expect([wire?.readyState, wire?.closedByPage]).toEqual([1, false])
    clock.mockRestore()
    // The deadline's timer comes due: the RECONCILED ended the recovery.
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 560 + 1)
    expect(ended).toEqual([])
  })

  test('takes nothing from an attempt it cut before the deadline: its socket opening and reconciling late ends nothing twice', async () => {
    // Eight refused attempts, then one at 29.25 s, cut at half the 0.75 s left: it opens 2 s after it was made, after
    // the page closed it, as a runtime may.
    const { lostAt, ended } = await lostWire({
      then: [...Array<Script>(8).fill('refused'), { opens: { afterMs: 2_000, despiteClose: true } }],
    })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS + 3_000)
    const late = ScriptedWebSocket.made.at(-1)
    expect(madeSince(lostAt)).toHaveLength(9)
    expect(madeSince(lostAt).at(-1)).toBe(29_250)
    expect(ended).toHaveLength(1)
    expect([late?.readyState, late?.closedByPage, late?.sent]).toEqual([3, true, 0])
  })

  test('ends a channel registered during the recovery, which the server never attached, with "Connection not established", and the attached one with "Connection dropped"', async () => {
    const { ended, register } = await lostWire({ then: [] })
    const joined: unknown[] = []
    register(fakeMuxChannel({ _onConnectionLost: (e) => joined.push(e) }))
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS + 1)
    expect(ended).toMatchObject([{ name: 'NetworkError', message: 'Connection dropped' }])
    expect(joined).toMatchObject([{ name: 'NetworkError', message: 'Connection not established' }])
  })

  test("starts no attempt that couldn't reconcile before the deadline, as long as the last handshake took", async () => {
    const handshakeMs = 3_000
    const { lostAt, ended } = await lostWire({ handshakeMs, then: [] })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 1)
    expect(ended).toEqual([])
    // 0.9 of 500, 1,000, 2,000, 4,000, then of the 5,000 cap, each after the last refusal: the next, at 29,250,
    // couldn't reconcile 3 s later before the deadline at 30,000.
    expect(madeSince(lostAt)).toEqual([450, 1350, 3150, 6750, 11250, 15750, 20250, 24750])
    await vi.advanceTimersByTimeAsync(2)
    expect(ended).toHaveLength(1)
  })

  test("takes a socket's `error` with no `close` as its failure, as Node 22's after a reset, and backs off from it", async () => {
    const { lostAt, ended } = await lostWire({ then: ['errors', { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(5_000)
    // 0.9 of 500, then 0.9 of the second delay, 1,000, from that socket's failure.
    expect(madeSince(lostAt)).toEqual([450, 1350])
    expect(ScriptedWebSocket.made.at(-1)?.sent).toBeGreaterThan(0)
    expect(ended).toEqual([])
  })

  test("counts a socket's `error` and `close` in one task as one failure", async () => {
    const closed = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    const { lostAt } = await lostWire({ then: ['refused', 'refused', { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(5_000)
    // 0.9 of 500, of 1,000 and of 2,000, each from the last failure.
    expect(madeSince(lostAt)).toEqual([450, 1350, 3150])
    // The wire's drop, then each refused socket once.
    expect(closed).toHaveBeenCalledTimes(3)
  })

  test('takes nothing from the `error`s of a socket it cut, as Node 22 fires once the page closed it', async () => {
    const closed = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    const { lostAt, ended } = await lostWire({ then: Array<Script>(8).fill('hangs22') })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 1)
    // The schedule of attempts whose sockets never answer: the cut sockets' `error`s say nothing of it.
    expect(madeSince(lostAt)).toEqual([450, 4350, 12150, 27750])
    expect(ScriptedWebSocket.mostConnecting).toBe(1)
    // The wire's drop, then each cut.
    expect(closed).toHaveBeenCalledTimes(4)
    expect(ended).toEqual([])
  })

  test('ends an attempt whose socket it could not make, and backs off from it', async () => {
    const { lostAt, ended } = await lostWire({ then: ['throws', { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(5_000)
    // The socket at 450 couldn't be made: the next comes 0.9 of the second delay, 1,000, after it.
    expect(madeSince(lostAt)).toEqual([1350])
    const next = ScriptedWebSocket.made.at(-1)
    expect(next?.readyState).toBe(1)
    expect(next?.sent).toBeGreaterThan(0)
    expect(ended).toEqual([])
  })

  test('a page that leaves while it reconnects lets the socket it was opening go, and opens none after', async () => {
    const page = new EventTarget()
    vi.stubGlobal('addEventListener', page.addEventListener.bind(page))
    vi.stubGlobal('removeEventListener', page.removeEventListener.bind(page))
    try {
      const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
      await lostWire({ then: [{ opens: { afterMs: 2_500, despiteClose: true } }, 'hangs'] })
      await vi.advanceTimersByTimeAsync(1_500)
      expect(ScriptedWebSocket.made).toHaveLength(2)
      page.dispatchEvent(Object.assign(new Event('pagehide'), { persisted: false }))
      // Past its allowance too: the attempt the page let go is not cut, so no other follows it.
      await vi.advanceTimersByTimeAsync(5_000)
      // It would open 2.5 s after it was made, closed or not: it closes again before any frame.
      expect(ScriptedWebSocket.made.slice(1).map((ws) => [ws.closedByPage, ws.sent])).toEqual([[true, 0]])
      expect(ScriptedWebSocket.made).toHaveLength(2)
      expect(open).toHaveBeenCalledTimes(1)
    } finally {
      vi.unstubAllGlobals()
    }
  })

  test('measures a handshake from the start of the attempt that opened, not from the first of a recovery whose attempts were cut', async () => {
    const { lostAt } = await lostWire({ then: ['hangs', 'hangs', { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(12_200)
    expect(madeSince(lostAt)).toEqual([450, 4350, 12150])
    const wire = ScriptedWebSocket.made[3]
    if (!wire) throw new Error('no wire')
    vi.spyOn(Math, 'random').mockReturnValue(0.35)
    wire.drop()
    const relostAt = performance.now()
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 1)
    // 0.35 of 500, 1,000, 2,000, 4,000, then of the 5,000 cap, each after the last refusal. The last two fit the
    // deadline at 30,000 after the opened attempt's own handshake, 0 ms, and wouldn't after the 12 s its recovery took.
    expect(madeSince(relostAt)).toEqual([
      175, 525, 1225, 2625, 4375, 6125, 7875, 9625, 11375, 13125, 14875, 16625, 18375, 20125, 21875, 23625, 25375,
      27125, 28875,
    ])
  })

  test('takes an attempt the server answers 1.7 s late inside a 3 s window: it would be cut only where the next could still reconcile', async () => {
    Object.assign(serverSettings, SHORT_WINDOW)
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const { lostAt, ended } = await lostWire({ handshakeMs: 50, then: [{ opens: { afterMs: 1_700 } }] })
    await vi.advanceTimersByTimeAsync(2_000)
    // Half of 500; its answer at 1,950 comes before 2,450, past which half of 1,000 and a 50 ms handshake miss 3,000.
    expect(madeSince(lostAt)).toEqual([250])
    const wire = ScriptedWebSocket.made.at(-1)
    expect([wire?.readyState, wire?.closedByPage]).toEqual([1, false])
    expect(wire?.sent).toBeGreaterThan(0)
    expect(ended).toEqual([])
  })

  // A second exactly as slow arrives at the deadline's own instant, too late: the deadline's timer, set first, runs first.
  test.each([
    { secondMs: 150, recovers: true },
    { secondMs: 200, recovers: false },
    { secondMs: 250, recovers: false },
  ])(
    'cuts a held first reconnect in a 3 s window where a second as slow as the last open, 200 ms, reconciles at the deadline: a second of $secondMs ms recovers: $recovers',
    async ({ secondMs, recovers }) => {
      Object.assign(serverSettings, SHORT_WINDOW)
      vi.spyOn(Math, 'random').mockReturnValue(0.5)
      const { lostAt, ended } = await lostWire({
        handshakeMs: 200,
        then: ['hangs', { reconcile: { afterMs: secondMs } }],
      })
      await vi.advanceTimersByTimeAsync(3_001)
      // Half of 500; cut 2,050 on, half of 1,000 and 200 ms before 3,000; the second from 2,800.
      expect(madeSince(lostAt)).toEqual([250, 2800])
      const [, held, second] = ScriptedWebSocket.made
      expect(held?.closedByPage).toBe(true)
      if (recovers) {
        expect([second?.readyState, second?.closedByPage]).toEqual([1, false])
        expect(second?.sent).toBeGreaterThan(0)
        expect(ended).toEqual([])
      } else {
        expect(ended).toHaveLength(1)
        expect(ended[0]).toMatchObject({ message: 'Connection dropped' })
      }
    },
  )

  test('starts the next attempt a cut was for where the cut ran 20 ms late', async () => {
    Object.assign(serverSettings, SHORT_WINDOW)
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const { lostAt, ended } = await lostWire({ handshakeMs: 200, then: ['hangs', { reconcile: { afterMs: 150 } }] })
    // The cut is due 2,300 on; from just before it, the page's clock reads 20 ms ahead of its timers.
    await vi.advanceTimersByTimeAsync(2_299)
    const now = performance.now.bind(performance)
    vi.spyOn(performance, 'now').mockImplementation(() => now() + 20)
    await vi.advanceTimersByTimeAsync(702)
    // The second at 2,800 on the timers, 2,820 on the page's clock.
    expect(madeSince(lostAt)).toEqual([250, 2820])
    const second = ScriptedWebSocket.made.at(-1)
    expect([second?.readyState, second?.closedByPage]).toEqual([1, false])
    expect(ended).toEqual([])
  })

  test('gives an attempt after which none could reconcile the rest of a 3 s window, and takes its answer 2.4 s late', async () => {
    Object.assign(serverSettings, SHORT_WINDOW)
    const { lostAt, ended } = await lostWire({ handshakeMs: 1_700, then: [{ opens: { afterMs: 2_400 } }] })
    await vi.advanceTimersByTimeAsync(2_900)
    // 0.9 of 500; 0.9 of 1,000 and a 1.7 s handshake after it fit nowhere before 3,000, so it isn't cut.
    expect(madeSince(lostAt)).toEqual([450])
    const wire = ScriptedWebSocket.made.at(-1)
    expect([wire?.readyState, wire?.closedByPage]).toEqual([1, false])
    expect(ended).toEqual([])
  })

  test('waits after a cut the backoff drawn as the cut attempt started, and takes a second attempt answered 4 s late inside a 10 s window', async () => {
    Object.assign(serverSettings, { pingInterval: 2_000, reconnectTimeout: 6_000 })
    const random = vi.spyOn(Math, 'random')
    // Drawn as the first connect starts, at the loss, then as each attempt starts.
    for (const draw of [0.1, 0.5, 0.3, 0.7]) random.mockReturnValueOnce(draw)
    const { lostAt, ended } = await lostWire({ handshakeMs: 50, then: ['hangs', { opens: { afterMs: 4_000 } }] })
    await vi.advanceTimersByTimeAsync(7_600)
    // 0.5 of 500; cut 3 s on, then 0.3 of 1,000. The second may go 5 s, till 0.7 of 2,000 and 50 ms before 10 s: its
    // answer comes 4 s on.
    expect(madeSince(lostAt)).toEqual([250, 3550])
    expect(random).toHaveBeenCalledTimes(4)
    const wire = ScriptedWebSocket.made.at(-1)
    expect([wire?.readyState, wire?.closedByPage]).toEqual([1, false])
    expect(ended).toEqual([])
  })

  test("a channel the page closed, its server not yet sure of the page's verdict when the wire went, keeps the page retrying until the deadline, and nothing of the connection after", async () => {
    const pageHideListeners = new Set<unknown>()
    vi.stubGlobal('addEventListener', (type: string, listener: unknown) => {
      if (type === 'pagehide') pageHideListeners.add(listener)
    })
    vi.stubGlobal('removeEventListener', (type: string, listener: unknown) => {
      if (type === 'pagehide') pageHideListeners.delete(listener)
    })
    try {
      ScriptedWebSocket.scripts = [{ reconcile: { afterMs: 0 } }]
      const channel = fakeMuxChannel()
      const options = { transports: [CHANNEL_TRANSPORT.WS], fetchImpl: fetch, connectionKey: crypto.randomUUID() }
      const connection = ClientConnection.getOrCreate('http://test.local/_telefunc', channel, options)
      await vi.advanceTimersByTimeAsync(10)
      // As the page's `close()` resolves: its CLOSE_DONE sent, the channel let go.
      connection.sendCloseDone(channel)
      connection.unregister(channel)
      const first = ScriptedWebSocket.made[0]
      if (!first) throw new Error('the page made no socket')
      first.drop()
      const lostAt = performance.now()
      await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS + RECONNECT_TIMEOUT_MS)
      const retried = madeSince(lostAt)
      expect(retried.length).toBeGreaterThan(0)
      expect(Math.max(...retried)).toBeLessThan(RECOVERY_WINDOW_MS)
      expect(vi.getTimerCount()).toBe(0)
      expect(pageHideListeners.size).toBe(0)
      expect(ScriptedWebSocket.made.filter((ws) => ws.readyState !== 3)).toEqual([])
    } finally {
      vi.unstubAllGlobals()
    }
  })

  test('a channel registered while a reconnect waits starts no attempt sooner', async () => {
    const { lostAt, register } = await lostWire({ then: [] })
    await vi.advanceTimersByTimeAsync(100)
    register()
    expect(ScriptedWebSocket.made).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(400)
    expect(madeSince(lostAt)).toEqual([450])
  })
})
