import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { CHANNEL_TRANSPORT } from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { ClientConnection } from './ClientConnection.js'
import { WireLifecycle } from './WireLifecycle.js'
import { fakeMuxChannel } from './fakeMuxChannel.fixture.js'

const PING_INTERVAL_MS = 5_000
const RECONNECT_TIMEOUT_MS = 20_000
/** How long the server may hold the page's channels after the page lost its wire: until its ping deadline notices,
 *  then `reconnectTimeout` (wire/pingDeadline.ts `reconnectWindowOf`). */
const RECOVERY_WINDOW_MS = 2 * PING_INTERVAL_MS + RECONNECT_TIMEOUT_MS

/** What a socket the page makes does: reconciles `afterMs` after it opens; opens `afterMs` after it was made, then
 *  reconciles (`despiteClose`: even where the page closed it before, as a runtime may); closes at once as a refused
 *  connect; or never opens nor closes, as Node 22's after a reset at accept. */
type Script =
  | { reconcile: { afterMs: number } }
  | { opens: { afterMs: number; despiteClose?: boolean } }
  | 'refused'
  | 'hangs'

/** A WebSocket that plays the next script: the page's sockets, when it made each, and whether it closed them. */
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

  constructor() {
    ScriptedWebSocket.made.push(this)
    const connecting = ScriptedWebSocket.made.filter((ws) => ws.readyState === 0 && !ws.closedByPage).length
    ScriptedWebSocket.mostConnecting = Math.max(ScriptedWebSocket.mostConnecting, connecting)
    const script = ScriptedWebSocket.scripts.shift() ?? 'refused'
    this.despiteClose = typeof script === 'object' && 'opens' in script && script.opens.despiteClose === true
    if (script === 'hangs') return
    if (script === 'refused') {
      queueMicrotask(() => this.drop())
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
  send(): void {
    this.sent++
  }
  close(): void {
    this.closedByPage = true
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
    reconnectTimeout: RECONNECT_TIMEOUT_MS,
    idleTimeout: 60_000,
    pingInterval: PING_INTERVAL_MS,
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
  const register = () => ClientConnection.getOrCreate('http://test.local/_telefunc', fakeMuxChannel(), options)
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
  })

  test('ends its channels at the deadline the server holds them to, not before', async () => {
    const { ended } = await lostWire({ then: [] })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 1)
    expect(ended).toEqual([])
    await vi.advanceTimersByTimeAsync(2)
    expect(ended).toHaveLength(1)
    expect(ended[0]).toMatchObject({ message: 'Connection dropped' })
  })

  test('ends an attempt that never opens at that deadline, with every socket it raced, never two connecting at once', async () => {
    const { lostAt, ended } = await lostWire({ then: Array<Script>(8).fill('hangs') })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 1)
    expect(ended).toEqual([])
    // 0.9 of 500, then a socket more 1 s, 2 s, 4 s and 8 s on; the next, 16 s on, would come past the deadline.
    expect(madeSince(lostAt)).toEqual([450, 1450, 3450, 7450, 15450])
    expect(ScriptedWebSocket.mostConnecting).toBe(2)
    await vi.advanceTimersByTimeAsync(2)
    expect(ended).toHaveLength(1)
    expect(ScriptedWebSocket.made.map((ws) => ws.closedByPage)).toEqual([false, true, true, true, true, true])
  })

  test('races a socket beside an attempt that never opens, 1 s on with no backoff, and the one that opens is the wire', async () => {
    const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
    const { lostAt, ended } = await lostWire({ then: ['hangs', { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(5_000)
    expect(madeSince(lostAt)).toEqual([450, 1450])
    const [, hung, raced] = ScriptedWebSocket.made
    expect([hung?.closedByPage, hung?.sent]).toEqual([true, 0])
    expect([raced?.closedByPage, raced?.readyState]).toEqual([false, 1])
    expect(raced?.sent).toBeGreaterThan(0)
    // The first wire's open, then the raced socket's alone.
    expect(open).toHaveBeenCalledTimes(2)
    expect(ended).toEqual([])
  })

  test('races a socket three times as long as the last open after an attempt started, where that is past 1 s', async () => {
    // The first connect races a socket 1 s on, which hangs.
    const { lostAt, ended } = await lostWire({ openMs: 3_000, then: ['hangs', 'hangs', { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(13_000)
    expect(madeSince(lostAt)).toEqual([450, 9450])
    expect(ended).toEqual([])
  })

  test('takes the first socket of a link that got slower than 1 s and three times its last open, when it opens', async () => {
    const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
    const { lostAt, ended } = await lostWire({ then: Array<Script>(4).fill({ opens: { afterMs: 1_500 } }) })
    // The first wire opened; the first socket after the loss opens at 450 + 1,500, as it would without a race.
    expect(open).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1_949)
    expect(open).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(2)
    expect(open).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(5_000)
    expect(madeSince(lostAt)).toEqual([450, 1450])
    const [, first, raced] = ScriptedWebSocket.made
    expect([first?.readyState, first?.closedByPage]).toEqual([1, false])
    expect([raced?.closedByPage, raced?.sent]).toEqual([true, 0])
    expect(open).toHaveBeenCalledTimes(2)
    expect(ended).toEqual([])
  })

  test('over a link whose open outlasts two races, ends on a socket it opened, having made three', async () => {
    const { lostAt, ended } = await lostWire({ then: Array<Script>(4).fill({ opens: { afterMs: 3_500 } }) })
    await vi.advanceTimersByTimeAsync(9_000)
    // The third, 2 s after the second, lets the first go; the second opens 3.5 s after it was made.
    expect(madeSince(lostAt)).toEqual([450, 1450, 3450])
    expect(ScriptedWebSocket.made.slice(1).map((ws) => [ws.readyState, ws.closedByPage])).toEqual([
      [3, true],
      [1, false],
      [3, true],
    ])
    expect(ended).toEqual([])
  })

  test('where every open outlasts its allowance, keeps at most two sockets connecting: at the backoff draw, then 1, 3 and 7 s on', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.2)
    const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
    const { lostAt, ended } = await lostWire({ then: Array<Script>(4).fill({ opens: { afterMs: 10_000 } }) })
    await vi.advanceTimersByTimeAsync(14_000)
    // 0.2 of 500 first; the third socket, made 3 s on, opens 10 s after it was made, before the second's open.
    expect(madeSince(lostAt)).toEqual([100, 1100, 3100, 7100])
    expect(ScriptedWebSocket.mostConnecting).toBe(2)
    expect(ScriptedWebSocket.made.slice(1).map((ws) => ws.readyState)).toEqual([3, 3, 1, 3])
    expect(open).toHaveBeenCalledTimes(2)
    expect(ended).toEqual([])
  })

  test('after an open of 1.3 s waits 3.9 s before it races, so on that link it makes one socket per reconnect', async () => {
    // The first connect races a socket 1 s on, which hangs.
    const { lostAt } = await lostWire({
      openMs: 1_300,
      then: ['hangs', { opens: { afterMs: 1_300 } }, 'hangs', { reconcile: { afterMs: 0 } }],
    })
    await vi.advanceTimersByTimeAsync(2_000)
    expect(madeSince(lostAt)).toEqual([450])
    const reconnected = ScriptedWebSocket.made[2]
    if (!reconnected) throw new Error('no reconnect')
    expect(reconnected.readyState).toBe(1)
    reconnected.drop()
    const relostAt = performance.now()
    await vi.advanceTimersByTimeAsync(5_000)
    // Its socket hangs: the race comes 3 × 1.3 s on.
    expect(madeSince(relostAt)).toEqual([450, 4350])
  })

  test('closes a socket it let go that opens all the same, before any frame', async () => {
    const open = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
    await lostWire({ then: [{ opens: { afterMs: 3_000, despiteClose: true } }, { reconcile: { afterMs: 0 } }] })
    await vi.advanceTimersByTimeAsync(5_000)
    const [, late, raced] = ScriptedWebSocket.made
    expect([late?.readyState, late?.sent]).toEqual([3, 0])
    expect(raced?.readyState).toBe(1)
    expect(open).toHaveBeenCalledTimes(2)
  })

  test('races a first connect that never opens 1 s on, as it measured no open', async () => {
    ScriptedWebSocket.scripts = ['hangs', { reconcile: { afterMs: 0 } }]
    const ended: unknown[] = []
    ClientConnection.getOrCreate(
      'http://test.local/_telefunc',
      fakeMuxChannel({ _onConnectionLost: (e) => ended.push(e) }),
      { transports: [CHANNEL_TRANSPORT.WS], fetchImpl: fetch, connectionKey: crypto.randomUUID() },
    )
    const startedAt = performance.now()
    await vi.advanceTimersByTimeAsync(3_000)
    expect(ScriptedWebSocket.made.map((ws) => Math.round(ws.at - startedAt))).toEqual([0, 1_000])
    expect(ScriptedWebSocket.made.map((ws) => ws.closedByPage)).toEqual([true, false])
    expect(ended).toEqual([])
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

  test('a channel registered while a reconnect waits starts no attempt sooner', async () => {
    const { lostAt, register } = await lostWire({ then: [] })
    await vi.advanceTimersByTimeAsync(100)
    register()
    expect(ScriptedWebSocket.made).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(400)
    expect(madeSince(lostAt)).toEqual([450])
  })
})
