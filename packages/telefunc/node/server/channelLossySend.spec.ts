import { afterEach, describe, expect, expectTypeOf, test, vi } from 'vitest'

import { Channel, ServerChannel } from '../../wire-protocol/channel/server/ServerChannel.js'
import { Broadcast, BroadcastChannel } from '../../wire-protocol/channel/server/ServerBroadcast.js'
import { IndexedPeer, type PeerSender } from '../../wire-protocol/channel/server/IndexedPeer.js'
import { hold, letGo, releaseHolder, type MemoryHolder } from '../../wire-protocol/channel/server/channelMemory.js'
import { countWaitingPublishes, droppingPublishes } from '../../wire-protocol/flowControl/server/waitingPublishes.js'
import { pairPublishes } from '../../wire-protocol/channel/client/pairPublishes.js'
import { decode, type DecodedFrame } from '../../wire-protocol/wire/frame.js'
import { TAG } from '../../wire-protocol/wire/tags.js'
import { getChannelConfig } from './serverConfig.js'

afterEach(() => vi.useRealTimers())

/** What this server's channels hold, on the ledger every channel shares. */
const ledgerTotal = () => (globalThis as any)._telefunc['channelMemory.ts'].total as number

/** The page: what it read of its channel, in order, each lossy one marked `~`, and the bytes it consumed. */
type Page = { lastSeq: number; read: string[]; consumed: number }

/** One wire to the page. While `stalled`, what the server sends waits in the wire's queue; `deliver()` hands the page
 *  what waits; `close()` drops what is sent from then on, as a closed wire does. `behindAfter`: how long a lossy send may
 *  wait, or how that follows from the least confirmation time. */
function wire(
  channel: ServerChannel<never, string>,
  page: Page,
  { behindAfter = Number.POSITIVE_INFINITY }: { behindAfter?: number | ((leastConfirmMs: number) => number) } = {},
) {
  let waiting: Uint8Array[] = []
  let stalled = false
  let closed = false
  const pair = pairPublishes()
  const read = (frame: Uint8Array) => {
    const decoded = pair(decode(frame) as DecodedFrame)
    if (decoded === null || !('seq' in decoded) || decoded.seq <= page.lastSeq) return
    page.lastSeq = decoded.seq
    if (decoded.tag === TAG.TEXT) {
      page.read.push(`${decoded.lossy ? '~' : ''}${JSON.parse(decoded.text)}`)
      page.consumed += decoded.bytes
    }
    if (decoded.tag === TAG.BINARY) {
      page.read.push(`${decoded.lossy ? '~' : ''}[${[...decoded.data]}]`)
      page.consumed += decoded.bytes
    }
    if (decoded.tag === TAG.CLOSE) page.read.push('CLOSE')
  }
  const sender: PeerSender = {
    send(frame) {
      if (closed) return
      if (stalled) waiting.push(frame)
      else read(frame)
    },
    carry() {},
    bufferedAmount: () => 0,
    behindAfter: (leastConfirmMs) => (typeof behindAfter === 'number' ? behindAfter : behindAfter(leastConfirmMs)),
    deliveredBefore: () => 0,
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    queue: {
      waitingPublishes: (ix) => countWaitingPublishes(waiting, ix),
      dropWaitingPublishes(ix, count) {
        const dropping = droppingPublishes(ix, count)
        waiting = waiting.filter((frame) => !dropping(frame))
      },
      dropWaitingFromPublish() {},
    },
  }
  return {
    /** Attaches the page as `ChannelMux.attachChannel` does: what it lacks from the replay, then the channel. */
    attach() {
      channel._onPageAttach(page.lastSeq, undefined)
      const peer = new IndexedPeer({ sender, index: 7, replay: channel._replayBuffer! })
      for (const frame of channel._replayBuffer!.getAfter(page.lastSeq)) sender.send(frame)
      channel._attachPeer(peer)
      return peer
    },
    stall() {
      stalled = true
    },
    deliver() {
      stalled = false
      const frames = waiting
      waiting = []
      for (const frame of frames) read(frame)
    },
    /** The page's `WINDOW`: it has everything through its last seq. */
    confirm() {
      channel._dispatchFrame({ tag: TAG.WINDOW, index: 7, bytes: 1 << 30, lastSeq: page.lastSeq } as any)
    },
    close() {
      closed = true
    },
  }
}

function newPage(): Page {
  return { lastSeq: 0, read: [], consumed: 0 }
}

/** What the channel counted against its page's credit and the page hasn't consumed. */
function owed(channel: ServerChannel<never, string>, page: Page): number {
  return (channel as any).sendCredit.sentBytes - page.consumed
}

/** How many of 200 more lossy sends, each confirmed as it goes, the page reads once it granted 100 messages past those it
 *  read: 100 where the channel counted against the page's credit exactly the messages the page read. */
function spareMessages(channel: ServerChannel<never, string>, page: Page): number {
  const read = page.read.length
  channel._dispatchFrame({ tag: TAG.MSG_WINDOW, index: 7, count: read + 100 } as any)
  for (let i = 0; i < 200; i++) {
    void channel.send(`C${i}`, { lossy: true })
    channel._dispatchFrame({ tag: TAG.WINDOW, index: 7, bytes: 1 << 30, lastSeq: page.lastSeq } as any)
  }
  return page.read.length - read
}

function send(channel: ServerChannel<never, string>, sends: string) {
  for (const s of sends.split(' ')) void (s.startsWith('L') ? channel.send(s, { lossy: true }) : channel.send(s))
}

describe('a lossy send before the telefunction returns', () => {
  for (const [sends, read] of [
    ['L', '~L'],
    ['L1 L2 L3', '~L3'],
    ['L R', '~L R'],
    ['R L', 'R ~L'],
    ['L1 R L2', '~L1 R ~L2'],
    ['L1 L2 R1 L3 L4 R2', '~L2 R1 ~L4 R2'],
  ] as const) {
    test(`${sends}: the page reads ${read}`, () => {
      const channel = new ServerChannel<never, string>()
      send(channel, sends)
      channel._registerChannel()
      const page = newPage()
      wire(channel, page).attach()
      expect(page.read.join(' ')).toBe(read)
      expect(owed(channel, page)).toBe(0)
    })
  }
})

describe('a lossy send while the page is away', () => {
  test('L1, R, L2, L3, close(): the page reads L1, R, L3, then the close', () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    const first = wire(channel, page)
    const peer = first.attach()
    first.close()
    channel._onPeerDisconnect(peer, 60_000)
    send(channel, 'L1 R L2 L3')
    void channel.close()
    wire(channel, page).attach()
    expect(page.read.join(' ')).toBe('~L1 R ~L3 CLOSE')
  })

  test('the newest held for a page that is behind goes into the replay as the wire goes, ahead of what follows', async () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    const first = wire(channel, page, { behindAfter: 0 })
    const peer = first.attach()
    send(channel, 'L1')
    await new Promise((resolve) => setTimeout(resolve, 2))
    // Unconfirmed past `behindAfter`: the page is behind, so the newest waits.
    send(channel, 'L2 L3')
    expect(page.read.join(' ')).toBe('~L1')
    first.close()
    channel._onPeerDisconnect(peer, 60_000)
    send(channel, 'R L4')
    wire(channel, page).attach()
    expect(page.read.join(' ')).toBe('~L1 ~L3 R ~L4')
    expect(spareMessages(channel, page)).toBe(100)
  })
})

describe('a lossy send to an attached page', () => {
  test('a graceful close hands the held newest ahead of the close', async () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    wire(channel, page, { behindAfter: 0 }).attach()
    send(channel, 'L1')
    await new Promise((resolve) => setTimeout(resolve, 2))
    send(channel, 'L2')
    void channel.close()
    expect(page.read.join(' ')).toBe('~L1 ~L2 CLOSE')
  })

  test('a page that keeps up reads every one, in order with the others', () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    wire(channel, page).attach()
    send(channel, 'L1 R1 L2 L3 R2')
    expect(page.read.join(' ')).toBe('~L1 R1 ~L2 ~L3 R2')
  })

  test('L waits on the wire, R goes, L2 comes while the page is behind: L goes and its credit comes back', async () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    const w = wire(channel, page, { behindAfter: 0 })
    w.attach()
    w.stall()
    send(channel, 'L R')
    await new Promise((resolve) => setTimeout(resolve, 2))
    send(channel, 'L2')
    w.deliver()
    // L went, so nothing of the page's was unconfirmed when L2 came, and it went.
    expect(page.read.join(' ')).toBe('R ~L2')
    expect(spareMessages(channel, page)).toBe(100)
  })

  test("a lossy send whose head the wire took isn't replaced, and counts once", async () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    const w = wire(channel, page, { behindAfter: 0 })
    w.attach()
    send(channel, 'L')
    w.stall()
    await new Promise((resolve) => setTimeout(resolve, 2))
    send(channel, 'L2')
    w.deliver()
    w.confirm()
    expect(page.read.join(' ')).toBe('~L ~L2')
    expect(spareMessages(channel, page)).toBe(100)
  })

  test('the newest held while the page has no credit left goes once the page grants more', () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    const w = wire(channel, page)
    w.attach()
    // The page's first message window is 100.
    for (let i = 0; i < 100; i++) void channel.send(`${i}`, { lossy: true })
    send(channel, 'L1 L2')
    expect(page.read.length).toBe(100)
    channel._dispatchFrame({ tag: TAG.MSG_WINDOW, index: 7, count: 200 } as any)
    expect(page.read.at(-1)).toBe('~L2')
    expect(page.read.length).toBe(101)
  })

  test("the caller's bytes are its own once `sendBinary` returned, held or buffered", async () => {
    const channel = new ServerChannel<never, string>()
    const bytes = new Uint8Array([1, 2])
    void channel.sendBinary(bytes, { lossy: true })
    bytes[0] = 9
    channel._registerChannel()
    const page = newPage()
    const w = wire(channel, page, { behindAfter: 0 })
    w.attach()
    expect(page.read).toEqual(['~[1,2]'])
    send(channel, 'L')
    await new Promise((resolve) => setTimeout(resolve, 2))
    const held = new Uint8Array([3, 4])
    void channel.sendBinary(held, { lossy: true })
    held[0] = 9
    w.confirm()
    // The page was behind from `L` on, so `[3,4]` took its place.
    expect(page.read).toEqual(['~[1,2]', '~[3,4]'])
  })

  test("`lossy` with `ack: true` is a usage error, and the types don't allow it", () => {
    const channel = new Channel<never, string>({ ack: true })
    // @ts-expect-error
    expect(() => channel.send('x', { lossy: true, ack: true })).toThrow("`lossy` can't be combined with `ack: true`")
    expectTypeOf(channel.send('x', { lossy: true })).toEqualTypeOf<Promise<void>>()
    expectTypeOf(channel.sendBinary(new Uint8Array(1), { lossy: true, ack: false })).toEqualTypeOf<Promise<void>>()
    const opts = { lossy: true } as const
    expectTypeOf(channel.send('x', opts)).toEqualTypeOf<Promise<void>>()
  })
})

describe('bytes given to `send()` and `publish()`', () => {
  test('are a usage error naming `sendBinary()`, and nothing goes', () => {
    const channel = new Channel()
    expect(() => channel.send(new Uint8Array([1, 2, 255]))).toThrow('Bytes go through `sendBinary()`')
    expect(() => channel.send(Buffer.from('hi'), { ack: true })).toThrow('turns this `Buffer` into a plain object')
    expect(() => channel.send(new DataView(new ArrayBuffer(2)), { lossy: true })).toThrow('this `DataView`')
    expect(() => channel.send(new ArrayBuffer(4), { ack: false })).toThrow('this `ArrayBuffer`')
    void channel.send('R')
    const server = channel as unknown as ServerChannel<never, string>
    server._registerChannel()
    const page = newPage()
    wire(server, page).attach()
    expect(page.read).toEqual(['R'])
  })

  test('are a usage error naming `publishBinary()`, and nothing is published', async () => {
    const key = `room:bytes:${crypto.randomUUID()}`
    const received: unknown[] = []
    const unsubscribe = Broadcast.subscribe(key, (data) => received.push(data))
    const room = new BroadcastChannel({ key })
    expect(() => room.publish(new Float32Array(2))).toThrow('Bytes go through `publishBinary()`')
    expect(() => Broadcast.publish(key, new Uint8Array(1))).toThrow('Bytes go through `publishBinary()`')
    await Broadcast.publish(key, 'R')
    expect(received).toEqual(['R'])
    unsubscribe()
  })
})

describe('the confirmation time a lossy send measures', () => {
  test('a lossy send behind an unconfirmed plain one keeps it, so a wire that dies silently takes 20 values in 2 s, not 60', () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance', 'Date'] })
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    // A WebSocket's: 600 ms past the least confirmation time.
    const w = wire(channel, page, { behindAfter: (leastConfirmMs) => 600 + leastConfirmMs })
    w.attach()
    void channel.send('warm', { lossy: true })
    vi.advanceTimersByTime(50)
    w.confirm()
    expect(channel._replayBuffer!.leastConfirmMs).toBe(50)
    // A 1 MiB snapshot, then a cursor value, which waits for the snapshot's 8.4 s at 1 Mbit/s before it is confirmed.
    w.stall()
    void channel.send('R'.repeat(1024 * 1024))
    void channel.send('cursor', { lossy: true })
    vi.advanceTimersByTime((1024 * 1024 * 8) / 1000 + 50)
    w.deliver()
    w.confirm()
    expect(channel._replayBuffer!.leastConfirmMs).toBe(50)
    w.close()
    const handed = channel._replayBuffer!.seq
    for (let i = 0; i < 60; i++) {
      vi.advanceTimersByTime(1000 / 30)
      void channel.send(`cursor ${i}`, { lossy: true })
    }
    expect(channel._replayBuffer!.seq - handed).toBe(20)
  })
})

describe('a page away for 5 s, with 30 lossy sends and one plain send a second', () => {
  /** One send every 1000 / 30 ms for 8 s: the wire goes at 1 s, and the page is back at 6 s. Returns what the page read,
   *  what it read after it was back, and its `spareMessages()`. */
  function run(died: 'closed' | 'silently') {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance', 'Date'] })
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    // A confirmation waits 300 ms for the wire, past 300 ms of slack: a WebSocket's `behindAfter` at a 0 ms round trip.
    const first = wire(channel, page, { behindAfter: 600 })
    const peer = first.attach()
    let back = 0
    for (let i = 1; i <= 8 * 30; i++) {
      vi.advanceTimersByTime(1000 / 30)
      if (i === 30) {
        first.close()
        if (died === 'closed') channel._onPeerDisconnect(peer, 60_000)
      }
      if (i === 6 * 30) {
        back = page.read.length
        wire(channel, page, { behindAfter: 600 }).attach()
      }
      void channel.send(`L${i}`, { lossy: true })
      if (i % 30 === 0) void channel.send(`R${i / 30}`)
      // The page confirms within 50 ms of what it reads, while it is there.
      if (i < 30 || i >= 6 * 30)
        channel._dispatchFrame({ tag: TAG.WINDOW, index: 7, bytes: 1 << 30, lastSeq: page.lastSeq } as any)
    }
    const after = page.read.slice(back)
    return { read: [...page.read], after, spare: spareMessages(channel, page) }
  }

  for (const died of ['closed', 'silently'] as const) {
    test(`after a wire that ${died === 'closed' ? 'closed' : 'died silently'}: every plain one once and in order, the newest last`, () => {
      const { read, after, spare } = run(died)
      expect(read.filter((value) => value.startsWith('R'))).toEqual(['R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8'])
      expect(new Set(read).size).toBe(read.length)
      expect(read.at(-2)).toBe('~L240')
      // R1 to R5 went while the page was away.
      expect(after.filter((value) => value.startsWith('R')).slice(0, 5)).toEqual(['R1', 'R2', 'R3', 'R4', 'R5'])
      expect(spare).toBe(100)
    })
  }
})

describe('a large lossy send near the memory budget', () => {
  const size = 256 * 1024
  /** Holds the budget down to `room` bytes, until `free()`. */
  function leaveRoom(room: number) {
    const filler: MemoryHolder = { heldBytes: 0 }
    hold(filler, getChannelConfig().maxMemory - ledgerTotal() - room)
    return () => {
      letGo(filler, filler.heldBytes)
      releaseHolder(filler)
    }
  }
  /** The first two characters of what the page read: whether lossy, and which value. */
  const firstChars = (page: Page) => page.read.map((value) => value.slice(0, 2))

  test('before the first attach, a newest as large as the one waiting takes its place where both would not fit, and one too large is refused', async () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const free = leaveRoom(1.5 * size)
    try {
      await expect(channel.send('a'.repeat(size), { lossy: true })).resolves.toBeUndefined()
      await expect(channel.send('b'.repeat(size), { lossy: true })).resolves.toBeUndefined()
      await expect(channel.send('c'.repeat(2 * size), { lossy: true })).rejects.toThrow('Send refused')
      const page = newPage()
      wire(channel, page).attach()
      expect(firstChars(page)).toEqual(['~b'])
    } finally {
      free()
      channel._end()
      channel._release()
    }
  })

  test('attached and behind, a newest as large as the one waiting takes its place where both would not fit beside what the page has yet to confirm', async () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    const w = wire(channel, page, { behindAfter: 0 })
    w.attach()
    const free = leaveRoom(2.5 * size)
    try {
      void channel.send('a'.repeat(size), { lossy: true })
      await new Promise((resolve) => setTimeout(resolve, 2))
      // Unconfirmed past `behindAfter`: the page is behind, so the newest waits.
      void channel.send('b'.repeat(size), { lossy: true })
      await expect(channel.send('c'.repeat(size), { lossy: true })).resolves.toBeUndefined()
      w.confirm()
      expect(firstChars(page)).toEqual(['~a', '~c'])
    } finally {
      free()
      channel._end()
      channel._release()
    }
  })
})

describe("the server's memory after a lossy send", () => {
  test('a held newest lets go at abort()', async () => {
    const channel = new ServerChannel<never, string>()
    channel._registerChannel()
    const page = newPage()
    const w = wire(channel, page, { behindAfter: 0 })
    w.attach()
    send(channel, 'L1')
    await new Promise((resolve) => setTimeout(resolve, 2))
    const sent = channel.heldBytes
    void channel.send('x'.repeat(64 * 1024), { lossy: true })
    expect(channel.heldBytes - sent).toBeGreaterThan(64 * 1024)
    channel.abort()
    // What its replay keeps for the page, the ABORT among it, stays until the page is gone.
    expect(channel.heldBytes - sent).toBeLessThan(1024)
    channel._release()
    expect(channel.heldBytes).toBe(0)
  })

  test('a buffered one lets go at the connect TTL, and at the reconnect timeout', () => {
    vi.useFakeTimers()
    const unattached = new ServerChannel<never, string>()
    void unattached.send('x'.repeat(64 * 1024), { lossy: true })
    unattached._registerChannel()
    const away = new ServerChannel<never, string>()
    away._registerChannel()
    const peer = wire(away, newPage()).attach()
    away._onPeerDisconnect(peer, 1_000)
    void away.send('x'.repeat(64 * 1024), { lossy: true })
    expect(unattached.heldBytes + away.heldBytes).toBeGreaterThan(2 * 64 * 1024)
    vi.advanceTimersByTime(getChannelConfig().connectTtl + 1_000)
    expect(unattached.isClosed && away.isClosed).toBe(true)
    expect(unattached.heldBytes + away.heldBytes).toBe(0)
  })

  test('registration with the memory nearly full keeps the sends it admits, and lets go of the rest', () => {
    const before = ledgerTotal()
    const channel = new ServerChannel<never, string>()
    void channel.send('R')
    const room = 256 * 1024
    void channel.send('x'.repeat(2 * room), { lossy: true })
    const filler: MemoryHolder = { heldBytes: 0 }
    hold(filler, getChannelConfig().maxMemory - before - room)
    channel._registerChannel()
    const page = newPage()
    wire(channel, page).attach()
    expect(page.read).toEqual(['R'])
    letGo(filler, filler.heldBytes)
    releaseHolder(filler)
    channel._end()
    channel._release()
    expect(ledgerTotal()).toBe(before)
  })
})
