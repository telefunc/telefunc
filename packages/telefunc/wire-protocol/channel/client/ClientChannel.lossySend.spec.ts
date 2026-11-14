// A page's `send(data, { lossy: true })`: the REAL ClientChannel on the REAL ClientConnection and SendGate, over an SSE
// wire that never reconciles, so the gate holds every frame the page sends and the test reads them in order. The clock
// is faked, and the wire measured a 50 ms round trip: a frame held past its `behindAfter`, 650 ms, is behind, and a newer
// lossy send may take its place. Its channel is open, as after its first attach, unless a test says otherwise. The
// reconnect, with what the server then reads, is in ClientConnection.reconnectOrdering.spec.ts.

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { ClientChannel } from './ClientChannel.js'
import { ClientBroadcast } from './ClientBroadcast.js'
import type { ClientChannel as ClientChannelType } from '../channel.js'
import { decode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { CHANNEL_TRANSPORT, CREDIT_MSG_WINDOW_INITIAL, WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'

/** An SSE downstream that never sends a byte: the RECONCILED that would open sends never comes. */
const stalledFetch = (async () => new Response(new ReadableStream({ start() {} }), { status: 200 })) as typeof fetch

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'performance', 'Date'] })
  vi.stubGlobal('fetch', stalledFetch)
})
afterEach(() => {
  vi.clearAllTimers()
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

/** A channel whose wire opened and measured its round trip, and which a RECONCILED opened: what it sends from then on
 *  waits in the gate for the next RECONCILED. */
function newChannel(ack = false) {
  const channel = unopenedChannel(ack)
  // The wire opens one reconcile window after the first channel, carrying what the gate held till then.
  vi.advanceTimersByTime(200)
  // As a BDP probe's answer does: until a round trip is measured, the page is never behind.
  ;(channel as any).connection.transport.uplink.roundTrips.measured(50)
  // As a RECONCILED that lists it does.
  const { channels } = (channel as any).connection
  channels.open(channels.of(channel))
  return channel
}

/** A channel no RECONCILED opened yet, as it is before its first attach. */
function unopenedChannel(ack = false) {
  return new ClientChannel<string, never>({
    channelId: crypto.randomUUID(),
    ack,
    connection: {
      transports: [CHANNEL_TRANSPORT.SSE],
      telefuncUrl: 'http://test.local/_telefunc',
      connectionKey: crypto.randomUUID(),
    },
  })
}

/** The frames the gate holds of the page's sends, in order. */
function heldFrames(channel: ClientChannel<string, never>): Uint8Array[] {
  return (channel as any).connection.gate.settleQueue().map(({ frame }: { frame: Uint8Array }) => frame)
}

/** What the gate holds of the page's sends, in order: a text its value, a binary `[bytes]`, a close `CLOSE`. */
function held(channel: ClientChannel<string, never>): string[] {
  return heldFrames(channel).map((frame) => {
    const decoded = decode(frame as Uint8Array<ArrayBuffer>)
    if (decoded.tag === TAG.TEXT) return JSON.parse(decoded.text) as string
    if (decoded.tag === TAG.BINARY) return `[${[...decoded.data]}]`
    if (decoded.tag === TAG.CLOSE) return 'CLOSE'
    return `tag ${decoded.tag}`
  })
}

/** The bytes the page counted against the server's credit, less those of its frames the gate holds: 0 when it counted
 *  each frame as it is. */
function miscounted(channel: ClientChannel<string, never>): number {
  const sent = heldFrames(channel).reduce((sum, frame) => sum + frame.byteLength, 0)
  return (channel as any).sendCredit.sentBytes - sent
}

function send(channel: ClientChannel<string, never>, sends: string) {
  for (const s of sends.split(' ')) void (s.startsWith('L') ? channel.send(s, { lossy: true }) : channel.send(s))
}

/** Plain sends, never awaited, until the server's message credit is used up. */
function useUpCredit(channel: ClientChannel<string, never>) {
  for (let i = 0; i < CREDIT_MSG_WINDOW_INITIAL; i++) void channel.send('-')
  expect((channel as any).sendCredit.isOutOfCredit()).toBe(true)
}

/** The server's `MSG_WINDOW` or `WINDOW` for the channel. */
function grant(channel: ClientChannel<string, never>, limit: { count: number } | { bytes: number }) {
  const index = (channel as any).connection.channels.of(channel).ix
  channel._dispatchFrame({ tag: 'count' in limit ? TAG.MSG_WINDOW : TAG.WINDOW, index, ...limit } as any)
}

/** Past the wire's `behindAfter`, short of its reconcile timeout. */
const behind = () => vi.advanceTimersByTime(1_000)

describe('a lossy send from the page, while the gate holds it', () => {
  test('before the first attach, only the newest waits, up to a plain send', () => {
    // Before its wire opens, which sends what the gate holds: no round trip is measured yet.
    const channel = unopenedChannel()
    send(channel, 'L1 L2 R1 L3 L4')
    expect(held(channel)).toEqual(['L2', 'R1', 'L4'])
    expect(miscounted(channel)).toBe(0)
  })

  test('on a link that is not behind, each one waits to go', () => {
    const channel = newChannel()
    send(channel, 'L1 L2')
    expect(held(channel)).toEqual(['L1', 'L2'])
  })

  test('once the link is behind, a newer one takes its place, and the page counts the bytes that adds', () => {
    const channel = newChannel()
    send(channel, 'L1')
    behind()
    void channel.send('L2, longer', { lossy: true })
    expect(held(channel)).toEqual(['L2, longer'])
    expect(miscounted(channel)).toBe(0)
  })

  for (const [sends, read] of [
    ['L1 R1 L2', 'L1 R1 L2'],
    ['L1 L2 R1 L3 L4', 'L2 R1 L4'],
  ] as const) {
    test(`${sends}, the link behind before each: a plain send keeps the one before it, ${read} go`, () => {
      const channel = newChannel()
      for (const s of sends.split(' ')) {
        send(channel, s)
        behind()
      }
      expect(held(channel).join(' ')).toBe(read)
      expect(miscounted(channel)).toBe(0)
    })
  }

  test('a replacement larger than the server accepts throws, and leaves the one that waits', () => {
    const channel = newChannel()
    void channel.sendBinary(new Uint8Array([1]), { lossy: true })
    behind()
    expect(() => channel.sendBinary(new Uint8Array(WIRE_MAX_RAW_FRAME_BYTES), { lossy: true })).toThrow(
      'Channel message too large',
    )
    expect(held(channel)).toEqual(['[1]'])
    expect(miscounted(channel)).toBe(0)
  })
})

describe('a lossy send from the page out of credit', () => {
  test('waits as the newest, and goes once the server grants more messages', () => {
    const channel = newChannel()
    useUpCredit(channel)
    send(channel, 'L1 L2')
    expect(held(channel).at(-1)).toBe('-')
    grant(channel, { count: 2 * CREDIT_MSG_WINDOW_INITIAL })
    expect(held(channel).at(-1)).toBe('L2')
    expect(held(channel)).toHaveLength(CREDIT_MSG_WINDOW_INITIAL + 1)
    expect(miscounted(channel)).toBe(0)
  })

  test('waits as the newest, and goes once the server grants more bytes', () => {
    const channel = newChannel()
    for (let i = 0; i < 3; i++) void channel.sendBinary(new Uint8Array(1024 * 1024))
    expect((channel as any).sendCredit.isOutOfCredit()).toBe(true)
    send(channel, 'L1')
    expect(held(channel)).toHaveLength(3)
    grant(channel, { bytes: 16 * 1024 * 1024 })
    expect(held(channel).at(-1)).toBe('L1')
  })

  test('a limit that grants nothing leaves it waiting', () => {
    const channel = newChannel()
    useUpCredit(channel)
    send(channel, 'L1')
    grant(channel, { count: CREDIT_MSG_WINDOW_INITIAL })
    expect(held(channel).at(-1)).toBe('-')
  })

  test('goes ahead of a plain send, and of the close', () => {
    const channel = newChannel()
    useUpCredit(channel)
    send(channel, 'L1 R1 L2')
    void channel.close()
    expect(held(channel).slice(-4)).toEqual(['L1', 'R1', 'L2', 'CLOSE'])
  })

  test('one larger than the server accepts throws at its call, and leaves the one held and the next plain send', () => {
    const channel = newChannel()
    useUpCredit(channel)
    send(channel, 'L1')
    expect(() => channel.sendBinary(new Uint8Array(WIRE_MAX_RAW_FRAME_BYTES), { lossy: true })).toThrow(
      'Channel message too large',
    )
    expect(() => channel.send('R')).not.toThrow()
    expect(held(channel).slice(-2)).toEqual(['L1', 'R'])
  })

  test('is dropped by a newer one sent once a reconnect raised the credit, so it never goes after it', () => {
    const channel = newChannel()
    channel._attach.capWindows(1024)
    void channel.sendBinary(new Uint8Array(1024))
    send(channel, 'L1')
    // A later RECONCILED with a larger window: credit is back without a `WINDOW`.
    channel._attach.capWindows(1024 * 1024)
    send(channel, 'L2 R1')
    expect(held(channel).slice(1)).toEqual(['L2', 'R1'])
  })

  test("the caller's bytes are its own once `sendBinary` returned", () => {
    const channel = newChannel()
    useUpCredit(channel)
    const bytes = new Uint8Array([1, 2])
    void channel.sendBinary(bytes, { lossy: true })
    bytes[0] = 9
    grant(channel, { count: 2 * CREDIT_MSG_WINDOW_INITIAL })
    expect(held(channel).at(-1)).toBe('[1,2]')
  })

  test('is let go when the channel aborts', () => {
    const channel = newChannel()
    useUpCredit(channel)
    send(channel, 'L1')
    channel.abort()
    expect((channel as any).heldLossy).toBe(null)
  })
})

describe('`lossy` on the page', () => {
  test('resolves at once, on a channel made with `ack: true` too; with `ack: true` it is a usage error', async () => {
    const channel = newChannel(true)
    await expect(channel.send('L', { lossy: true })).resolves.toBe(undefined)
    // A text that asks for no acknowledgement.
    expect(held(channel)).toEqual(['L'])
    // @ts-expect-error
    expect(() => channel.send('x', { lossy: true, ack: true })).toThrow("`lossy` can't be combined with `ack: true`")
  })
})

describe('bytes given to `send()` and `publish()` on the page', () => {
  test('are a usage error naming `sendBinary()`, and nothing goes', () => {
    const channel = newChannel()
    const page = channel as unknown as ClientChannelType
    expect(() => page.send(new Uint8Array([1, 2, 255]))).toThrow('Bytes go through `sendBinary()`')
    expect(() => page.send(new ArrayBuffer(4), { lossy: true })).toThrow('turns this `ArrayBuffer` into a plain object')
    void page.send('R')
    expect(held(channel)).toEqual(['R'])
  })

  test('`publish()` is a usage error naming `publishBinary()`', () => {
    const room = new ClientBroadcast({
      channelId: crypto.randomUUID(),
      connection: {
        transports: [CHANNEL_TRANSPORT.SSE],
        telefuncUrl: 'http://test.local/_telefunc',
        connectionKey: crypto.randomUUID(),
      },
    })
    expect(() => room.publish(new Uint8Array(1))).toThrow('Bytes go through `publishBinary()`')
  })
})
