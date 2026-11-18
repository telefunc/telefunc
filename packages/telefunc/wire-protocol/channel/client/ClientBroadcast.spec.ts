// A page's lossy publishes: the REAL ClientBroadcast on the REAL ClientConnection and SendGate, over an SSE wire that
// never reconciles, as ClientChannel.lossySend.spec.ts runs a channel's lossy sends: the gate holds every frame, and one
// held past the wire's `behindAfter` is behind, so a newer lossy one of its stream may take its place.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createHook } from 'node:async_hooks'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, type ChannelFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ClientBroadcast } from './ClientBroadcast.js'
import { ClientChannel } from './ClientChannel.js'
import { ClientConnection } from './ClientConnection.js'
import { ReceiveCredit } from '../../flowControl/ReceiveCredit.js'
import type { BroadcastPublishOptions } from '../channel.js'

/** An SSE downstream that never sends a byte: the RECONCILED that would open sends never comes. */
const stalledFetch: typeof fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'performance', 'Date'] })
  vi.stubGlobal('fetch', stalledFetch)
})
afterEach(() => {
  vi.clearAllTimers()
  vi.useRealTimers()
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('ClientBroadcast uplink', () => {
  it('sends a lossy publish after a plain one of its stream that it followed', () => {
    const page = behindUplink()
    void page._publishBinaryUnreported(new Uint8Array([1]), 'cursor', true)
    behind()
    void page._publishBinaryUnreported(new Uint8Array([2]), 'cursor', false)
    void page._publishBinaryUnreported(new Uint8Array([3]), 'cursor', true)

    expect(waiting(page)).toEqual([1, 2, 3])
  })

  it("replaces a stream's waiting lossy publish across a plain one of another stream", () => {
    const page = behindUplink()
    void page._publishBinaryUnreported(new Uint8Array([1]), 'cursor', true)
    behind()
    void page._publishBinaryUnreported(new Uint8Array([2]), 'drawing', false)
    void page._publishBinaryUnreported(new Uint8Array([3]), 'cursor', true)

    expect(waiting(page)).toEqual([3, 2])
  })

  // The server reads from its first byte whether a page that is behind may skip it.
  it('marks a lossy publishBinary(), whose place a newer one takes, and sends one after a plain one it followed', () => {
    const page = behindUplink()
    void page.publishBinary(new Uint8Array([5]), { lossy: true })
    behind()
    void page.publishBinary(new Uint8Array([6]))
    void page.publishBinary(new Uint8Array([7]), { lossy: true })
    void page.publishBinary(new Uint8Array([8]), { lossy: true })

    expect(waitingPublishes(page)).toEqual([[0xfe, 5], [6], [0xfe, 8]])
  })

  // Its key orders both kinds: a lossy publish published after a plain one of the other kind never goes ahead of it.
  it.each([
    [
      [{ binary: 5, lossy: true }, { text: 'plain' }, { binary: 7, lossy: true }],
      [[0xfe, 5], '"plain"', [0xfe, 7]],
    ],
    [
      [{ text: 'old', lossy: true }, { binary: 6 }, { text: 'new', lossy: true }],
      ['\n"old"', [6], '\n"new"'],
    ],
  ])('sends %j in the order they were published', (published, sent) => {
    const page = behindUplink()
    published.forEach((publish, i) => {
      const options = { lossy: publish.lossy === true }
      if ('text' in publish) void page.publish(publish.text, options)
      else void page.publishBinary(new Uint8Array([publish.binary]), options)
      if (i === 0) behind()
    })

    expect(waitingPublishes(page)).toEqual(sent)
  })

  // Options read from a configuration file: a `null` there is not an absent option.
  it("refuses a `lossy` that isn't a boolean, at the call", () => {
    const page = behindUplink()
    const options: BroadcastPublishOptions = JSON.parse('{ "lossy": null }')

    expect(() => page.publish(1, options)).toThrow("`publish()`'s `lossy` should be a boolean")
    expect(() => page.publishBinary(new Uint8Array([1]), options)).toThrow(
      "`publishBinary()`'s `lossy` should be a boolean",
    )
    expect(waitingPublishes(page)).toEqual([])
  })

  // The publish whose place it took holds the close till its answer comes, and both get that answer.
  it('closes once the publish a newer one replaced is answered, and both get its receipt', async () => {
    const page = behindUplink()
    const first = page._publishBinaryUnreported(new Uint8Array([1]), 'cursor', true)
    behind()
    const newer = page._publishBinaryUnreported(new Uint8Array([2]), 'cursor', true)
    expect(waiting(page)).toEqual([2])
    let closed = false
    void page.close().then(() => (closed = true))
    const { ix } = (page as any).connection.channels.of(page)
    page._dispatchFrame(received(encode.closeAck(ix, 1)))
    await vi.advanceTimersByTimeAsync(0)
    expect(closed).toBe(false)

    const [seq] = (page as any).pendingAcks.keys()
    const receipt = { key: 'cursor', seq: 7, timestamp: 7 }
    page._dispatchFrame(received(encode.ackRes(ix, 2, seq, JSON.stringify(receipt))))
    await vi.advanceTimersByTimeAsync(0)
    expect(await first).toEqual(receipt)
    expect(await newer).toEqual(receipt)
    expect(closed).toBe(true)
  })
})

describe('ClientBroadcast delivery', () => {
  it("hands its listeners what was published, and Room's local ones the payload, and confirms a lossy one soon", () => {
    const confirmSoon = vi.spyOn(ReceiveCredit.prototype, 'confirmSoon')
    const page = behindUplink()
    const text: unknown[] = []
    const bytes: number[][] = []
    const payloads: number[][] = []
    page.subscribe((data) => text.push(data))
    page.subscribeBinary((data) => bytes.push([...data]))
    page._subscribeLocal('binary', (data) => payloads.push([...data]))
    const info = { seq: 1, timestamp: 1 }

    page._dispatchFrame(received(encode.publish('\n"lossy"', info)))
    page._dispatchFrame(received(encode.publish('"plain"', info)))
    expect(confirmSoon).toHaveBeenCalledTimes(1)
    page._dispatchFrame(received(encode.publishBinary(new Uint8Array([0xfe, 9]), info)))
    page._dispatchFrame(received(encode.publishBinary(new Uint8Array([8]), info)))

    expect(confirmSoon).toHaveBeenCalledTimes(2)
    expect(text).toEqual(['lossy', 'plain'])
    expect(bytes).toEqual([[9], [8]])
    expect(payloads).toEqual([[0xfe, 9], [8]])
  })

  it('does not call a listener that an earlier one unsubscribed during the same publish', () => {
    const page = behindUplink()
    const text: unknown[] = []
    const bytes: number[][] = []
    page.subscribe(() => unsubscribeText())
    const unsubscribeText = page.subscribe((data) => text.push(data))
    page.subscribeBinary(() => unsubscribeBinary())
    const unsubscribeBinary = page.subscribeBinary((data) => bytes.push([...data]))
    const info = { seq: 1, timestamp: 1 }

    page._dispatchFrame(received(encode.publish('"plain"', info)))
    page._dispatchFrame(received(encode.publishBinary(new Uint8Array([0, 8]), info)))

    expect(text).toEqual([])
    expect(bytes).toEqual([])
  })
})

// One added once its channel ended registers nothing, and its `ready` rejects with what the channel ended with.
describe('a listener added once its channel ended on the page', () => {
  it('of a broadcast whose earlier subscription the server confirmed never takes effect', async () => {
    const page = new ClientBroadcast(channelOptions())
    page.subscribe(() => {})
    page._dispatchFrame(received(encode.wanted(0, 1)))
    page.abort('gone')
    const closedWith = await page.closed.catch((err: unknown) => err)
    await expect(page.subscribe(() => {}).ready).rejects.toBe(closedWith)
    await expect(page.subscribeBinary(() => {}).ready).rejects.toBe(closedWith)
  })

  it('of a plain channel never takes effect', async () => {
    const page = new ClientChannel(channelOptions())
    page.abort('gone')
    const closedWith = await page.closed.catch((err: unknown) => err)
    await expect(page.listen(() => {}).ready).rejects.toBe(closedWith)
    await expect(page.listenBinary(() => {}).ready).rejects.toBe(closedWith)
  })
})

// An app may clean its listeners up in `onClose()`: a listener whose `ready` was pending when the page's broadcast ended
// still gets that end, whatever a close callback does after.
describe("a page's broadcast that ends while a subscriber waits, its `onClose()` removing that subscriber", () => {
  for (const binary of [false, true]) {
    it(`${binary ? 'subscribeBinary()' : 'subscribe()'}: \`ready\` rejects with the end`, async () => {
      const page = new ClientBroadcast(channelOptions())
      const stop = binary ? page.subscribeBinary(() => {}) : page.subscribe(() => {})
      page.onClose(() => stop())
      page.abort('gone')
      const ready = await stop.ready.then(
        () => 'resolved',
        (err: unknown) => err,
      )
      expect(ready).toBe(await page.closed.catch((err: unknown) => err))
    })
  }
})

// A call carries each want the server hasn't confirmed yet, and none of a channel once it confirmed them or ended.
it("a call carries the page's wants the server hasn't confirmed", () => {
  const page = new ClientBroadcast(channelOptions())
  const carried = () =>
    (ClientConnection.unconfirmedWants('http://test.local/_telefunc') ?? []).filter((want) => want.channel === page.id)
  page.subscribe(() => {})
  expect(carried()).toEqual([{ channel: page.id, version: 1, text: true, binary: false }])
  page._dispatchFrame(received(encode.wanted(0, 1)))
  expect(carried()).toEqual([])
  page.subscribeBinary(() => {})
  expect(carried()).toEqual([{ channel: page.id, version: 2, text: true, binary: true }])
  page.abort()
  expect(carried()).toEqual([])
})

// A broadcast the page is closing has no want left to bring: a call meanwhile carries none, as one with nothing pending.
it('a call made while the page closes its broadcast before the server confirmed its want carries no `wants`', () => {
  const telefuncUrl = `http://test.local/${crypto.randomUUID()}/_telefunc`
  const page = new ClientBroadcast(channelOptions(telefuncUrl))
  page.subscribe(() => {})
  page.close({ timeout: 50 }).catch(() => {})
  expect(ClientConnection.unconfirmedWants(telefuncUrl)).toBe(undefined)
  page.abort()
})

// A `ready` nobody reads makes no promise: subscribers that come and go while the server hasn't confirmed leave nothing.
it("1,000 page subscribers added and removed while the server hasn't confirmed make no promise", () => {
  const page = new ClientBroadcast(channelOptions())
  page.subscribe(() => {})
  let made = 0
  const hook = createHook({
    init(_id, type) {
      if (type === 'PROMISE') made++
    },
  }).enable()
  try {
    for (let i = 0; i < 1_000; i++) page.subscribe(() => {})()
  } finally {
    hook.disable()
  }
  expect(made).toBe(0)
  page.abort()
})

function channelOptions(telefuncUrl = 'http://test.local/_telefunc') {
  return {
    channelId: crypto.randomUUID(),
    connection: {
      transports: [CHANNEL_TRANSPORT.SSE],
      telefuncUrl,
      connectionKey: crypto.randomUUID(),
    },
  }
}

/** A page's broadcast whose wire opened and measured its round trip, and which a RECONCILED opened: what it publishes
 *  from then on waits in the gate. */
function behindUplink() {
  const page = new ClientBroadcast({
    channelId: crypto.randomUUID(),
    connection: {
      transports: [CHANNEL_TRANSPORT.SSE],
      telefuncUrl: 'http://test.local/_telefunc',
      connectionKey: crypto.randomUUID(),
    },
  })
  vi.advanceTimersByTime(200)
  ;(page as any).connection.transport.uplink.roundTrips.measured(50)
  const { channels } = (page as any).connection
  channels.open(channels.of(page))
  return page
}

/** Past the wire's `behindAfter`, short of its reconcile timeout. */
const behind = () => vi.advanceTimersByTime(1_000)

/** `frame` as the page's connection hands it to its channel. */
function received(frame: Uint8Array<ArrayBuffer>): ChannelFrame {
  const decoded = decode(frame)
  if (!('index' in decoded)) throw new Error('Not a channel frame')
  return decoded
}

/** Each publish the gate holds, in the order they go: its text, or its bytes. */
function waitingPublishes(page: ClientBroadcast): (string | number[])[] {
  return (page as any).connection.gate
    .settleQueue()
    .map(({ frame }: { frame: Uint8Array<ArrayBuffer> }) => decode(frame))
    .flatMap((decoded: ReturnType<typeof decode>) => {
      if (decoded.tag === TAG.PUBLISH_ACK_REQ) return [decoded.text]
      return decoded.tag === TAG.PUBLISH_BINARY_ACK_REQ ? [[...decoded.data]] : []
    })
}

/** The first byte of each binary publish the gate holds, in the order they go. */
function waiting(page: ClientBroadcast): (number | undefined)[] {
  return waitingPublishes(page).flatMap((publish) => (typeof publish === 'string' ? [] : [publish[0]]))
}
