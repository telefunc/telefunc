// A channel whose data gets no further while the page reconnects, at instants a browser can't stage: a loss and a
// RECONCILED at the same time, and a page whose timers were held. A REAL ClientChannel and its connection against a
// scripted server (scriptedServer.fixture.ts), over SSE batch POSTs.

import { beforeEach, describe, expect, it, test, vi } from 'vitest'
import { defined } from '../../../utils/defined.fixture.js'
import { SSE_RECONCILE_DEADLINE_MS } from '../../constants.js'
import { advance, holdPage, openChannel, serveSse, useScriptedServer, world } from './scriptedServer.fixture.js'

import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, type ChannelFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ClientBroadcast } from './ClientBroadcast.js'
import { END_CAUSE } from '../endCause.js'
import { ClientConnection } from './ClientConnection.js'
import { ReceiveCredit } from '../../flowControl/ReceiveCredit.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import type { BroadcastPublishOptions } from '../channel.js'

useScriptedServer()

/** Opens `count` channels over SSE, the first at index 0, and waits till their wire carries them. */
async function openOverSse(count: number) {
  serveSse()
  const opened = Array.from({ length: count }, () => openChannel(['sse']))
  await advance(SSE_RECONCILE_DEADLINE_MS + 500)
  return (i: number) => defined(opened[i], `channel ${i}`)
}

/** What the server got of channel `ix`. */
const gotOf = (ix: number) => world.server.got.filter((got) => got.ix === ix)

/** The most a reconnect waits, with the fixture's jitter. */
const MOST_BACKOFF_MS = 7_250

/** A channel whose data the server takes and never confirms, its wire lost at 3, 6 and 9 s after the send (dated at
 *  the first), the next open held till the date is older than reconnectTimeout: then the test answers it. */
async function overdueWithHeldOpen() {
  const page = (await openOverSse(1))(0)
  world.server.drops.add(0)
  void page.channel.send('taken, unconfirmed').catch(() => {})
  for (let i = 0; i < 3; i++) {
    await advance(3_000)
    if (i === 2) world.holdOpens = true
    defined(world.streams.at(-1), 'an event stream').end()
  }
  await advance(5_000)
  expect(world.heldOpens).toHaveLength(1)
  // The server now has the data and says so in the RECONCILED it holds.
  world.server.drops.delete(0)
  world.server.confirmed.set(0, defined(gotOf(0)[0], 'the frame the server took').seq)
  return page
}

test('a RECONCILED that confirms an overdue channel, then a loss at the same time: the channel goes on', async () => {
  const page = await overdueWithHeldOpen()
  defined(world.heldOpens.shift(), 'the held open')()
  defined(world.streams.at(-1), 'the held open stream').end()
  world.holdOpens = false
  await advance(5_000)
  expect(page.ended).toEqual([])
})

test('a loss, then the RECONCILED that would confirm an overdue channel, at the same time: the channel ends at the loss', async () => {
  const page = await overdueWithHeldOpen()
  defined(world.streams.at(-1), 'the held open stream').end()
  defined(world.heldOpens.shift(), 'the held open')()
  world.holdOpens = false
  await advance(5_000)
  expect(page.ended).toHaveLength(1)
  expect(defined(page.ended[0], 'the channel ending').err?.message).toMatch(/The last try: Connection dropped/)
})

// A held page: reconnectTimeout at its default, 60 s, so that 90 s is past it.
const DEFAULT_RECONNECT_TIMEOUT_MS = 60_000

test('a page held 90 s whose channel had no loss before the hold makes one more attempt, which gets through', async () => {
  world.server.settings = { reconnectTimeout: DEFAULT_RECONNECT_TIMEOUT_MS }
  const page = (await openOverSse(1))(0)
  world.server.drops.add(0)
  void page.channel.send('taken before the hold, confirmed after').catch(() => {})
  await advance(1_000)
  await holdPage(90_000)
  world.server.drops.delete(0)
  world.server.confirmed.set(0, defined(gotOf(0)[0], 'the frame the server took').seq)
  await advance(30_000)
  expect(page.ended).toEqual([])
})

test('a page held 90 s whose channel was dated before the hold ends at the first loss after it, naming it', async () => {
  world.server.settings = { reconnectTimeout: DEFAULT_RECONNECT_TIMEOUT_MS }
  const page = (await openOverSse(1))(0)
  // The server takes the data and never confirms it; one loss dates the channel, and the page reconnects before the
  // hold, so that what ends it after the hold is the channel's own verdict and not the connection's recovery.
  world.server.drops.add(0)
  void page.channel.send('taken, never confirmed').catch(() => {})
  await advance(1_000)
  defined(world.streams.at(-1), 'an event stream').end()
  await advance(MOST_BACKOFF_MS + 1_000)
  expect(world.streams).toHaveLength(2)
  await holdPage(90_000)
  // The first loss after the hold: the wire's PONG is overdue, as the server's PING deadline dropped it meanwhile.
  const wokeAt = performance.now()
  await advance(5_000)
  const { at, err } = defined(page.ended[0], 'the channel ending')
  expect(at - wokeAt).toBeLessThan(5_000)
  expect(world.streams).toHaveLength(2)
  expect(err?.message).toMatch(
    /the server hasn't confirmed what the page sent on it for over \d+ s of reconnects\. The last try: Connection dropped/,
  )
})

describe('a broadcast whose wire is stalled', () => {
  /** An SSE downstream that never sends a byte: the RECONCILED that would open sends never comes. */
  const stalledFetch: typeof fetch = async () =>
    new Response(new ReadableStream({ start() {} }), { status: 200, headers: { 'Content-Type': 'text/event-stream' } })

  beforeEach(() => vi.stubGlobal('fetch', stalledFetch))

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
      const { ix } = defined(connectionOf(page)['lifecycle'].channels.of(page), "the broadcast's record")
      page._dispatchFrame(received(encode.closeAck(ix, 0, 1)))
      await vi.advanceTimersByTimeAsync(0)
      expect(closed).toBe(false)

      const seq = defined(page['pendingAcks'].keys().next().value, 'the publish that waits for its answer')
      const receipt = { key: 'cursor', seq: 7, timestamp: 7 }
      page._dispatchFrame(received(encode.ackRes(ix, 2, seq, JSON.stringify(receipt))))
      await vi.advanceTimersByTimeAsync(0)
      expect(await first).toEqual(receipt)
      expect(await newer).toEqual(receipt)
      // The server's side of the close is done once it answered.
      page._dispatchFrame(received(encode.closeDone(ix, 0, 3)))
      await vi.advanceTimersByTimeAsync(0)
      expect(closed).toBe(true)
    })
  })

  it("hands its listeners what was published, and Room's local ones the payload, and confirms a lossy one soon", () => {
    const confirmSoon = vi.spyOn(ReceiveCredit.prototype, 'confirmSoon')
    const page = behindUplink()
    const text: unknown[] = []
    const bytes: number[][] = []
    const payloads: number[][] = []
    const confirmationsAtDelivery: number[] = []
    page.subscribe((data) => {
      confirmationsAtDelivery.push(confirmSoon.mock.calls.length)
      text.push(data)
    })
    page.subscribeBinary((data) => {
      confirmationsAtDelivery.push(confirmSoon.mock.calls.length)
      bytes.push([...data])
    })
    page._subscribeLocal('binary', (data) => payloads.push([...data]))
    const info = { seq: 1, timestamp: 1 }

    page._dispatchFrame(received(encode.publish('\n"lossy"', info)))
    page._dispatchFrame(received(encode.publish('"plain"', info)))
    expect(confirmSoon).toHaveBeenCalledTimes(1)
    page._dispatchFrame(received(encode.publishBinary(new Uint8Array([0xfe, 9]), info)))
    page._dispatchFrame(received(encode.publishBinary(new Uint8Array([8]), info)))

    expect(confirmSoon).toHaveBeenCalledTimes(2)
    expect(confirmationsAtDelivery).toEqual([1, 1, 2, 2])
    expect(text).toEqual(['lossy', 'plain'])
    expect(bytes).toEqual([[9], [8]])
    expect(payloads).toEqual([[0xfe, 9], [8]])
  })

  describe("a page's broadcast `close()` resolves with its outcome", () => {
    it('when the server never acknowledges it: the timeout, once it passes', async () => {
      const page = new ClientBroadcast(channelOptions())
      const closing = page.close({ timeout: 10 })
      await vi.advanceTimersByTimeAsync(10)
      await expect(closing).resolves.toMatchObject({ message: 'Channel close timed out' })
    })

    it('when an `onClose()` callback throws, and when the promise one returns rejects, each reported', async () => {
      const reported = vi.spyOn(console, 'error').mockImplementation(() => {})
      const page = new ClientBroadcast(channelOptions())
      page.onClose(() => {
        throw new Error('thrown')
      })
      page.onClose(() => Promise.reject(new Error('rejected')))
      const closing = page.close({ timeout: 10 })
      await vi.advanceTimersByTimeAsync(10)
      await expect(closing).resolves.toMatchObject({ message: 'Channel close timed out' })
      expect(reported.mock.calls.map(([, err]) => String(err))).toEqual(['Error: thrown', 'Error: rejected'])
    })

    it('when the server asked to close it first, or it ended before', async () => {
      const asked = new ClientBroadcast(channelOptions())
      asked._dispatchFrame(received(encode.close(0, 10, 1)))
      const closing = asked.close({ timeout: 10 })
      // The server decides: its deadline passes, and its ERROR ends the page's end.
      setTimeout(() => asked._onEnded(END_CAUSE.closeTimedOut.error()), 10)
      await vi.advanceTimersByTimeAsync(10)
      await expect(closing).resolves.toMatchObject({ message: 'Channel close timed out' })

      const ended = new ClientBroadcast(channelOptions())
      ended.abort('gone')
      await expect(ended.close()).resolves.toBe(await ended.closed)
    })
  })

  // Room's page declares a want's parts at each change: 4,096 track subscriptions in one loop would build and send 4,096
  // wants, each with every track subscribed so far.
  it('parts changed 4,096 times in one task are one want, read once, which a call made after them carries', () => {
    const telefuncUrl = `http://test.local/${crypto.randomUUID()}/_telefunc`
    const page = new ClientBroadcast(channelOptions(telefuncUrl))
    const tracks: string[] = []
    let reads = 0
    for (let i = 0; i < 4_096; i++) {
      tracks.push(`x${i}`)
      page._setWantParts(() => {
        reads++
        return { tracks: [...tracks] }
      })
    }
    expect(ClientConnection.unconfirmedWants(telefuncUrl)).toEqual([
      { channel: page.id, version: 1, text: false, binary: false, parts: { tracks } },
    ])
    expect(reads).toBe(1)
    page.abort()
  })

  // A publish made right after a change of the parts reaches the server after the want that declares it.
  it('parts changed in a task are declared before a publish made after them in that task', () => {
    const page = new ClientBroadcast(channelOptions(`http://test.local/${crypto.randomUUID()}/_telefunc`))
    page._setWantParts(() => ({ tracks: ['x'] }))
    page.publish('after').catch(() => {})
    expect(page._attach.unconfirmedWant).toEqual({ version: 1, text: false, binary: false, parts: { tracks: ['x'] } })
    page.abort()
  })

  function channelOptions(telefuncUrl = 'http://test.local/_telefunc') {
    return {
      channelId: crypto.randomUUID(),
      key: 'k',
      connection: {
        transports: [CHANNEL_TRANSPORT.SSE],
        telefuncUrl,
        connectionKey: crypto.randomUUID(),
      },
    }
  }

  /** A page's broadcast whose wire opened and measured its round trip, and which a RECONCILED opened: what it publishes
   *  from then on waits in the send queue. */
  function behindUplink() {
    const page = new ClientBroadcast({
      channelId: crypto.randomUUID(),
      key: 'k',
      connection: {
        transports: [CHANNEL_TRANSPORT.SSE],
        telefuncUrl: 'http://test.local/_telefunc',
        connectionKey: crypto.randomUUID(),
      },
    })
    vi.advanceTimersByTime(200)
    const connection = connectionOf(page)
    connection['transport'].uplink.roundTrips.measured(50)
    const { channels } = connection['lifecycle']
    channels.open(defined(channels.of(page), "the broadcast's record"))
    return page
  }

  /** The connection the page's broadcast is on. */
  function connectionOf(page: ClientBroadcast): ClientConnection {
    const kept = getGlobalObject<{ connections: Map<string, ClientConnection> }>(
      'channel/client/ClientConnection.ts',
      () => {
        throw new Error('The page keeps no connections yet')
      },
    )
    const connection = [...kept.connections.values()].find((each) => each['lifecycle'].channels.of(page) !== undefined)
    return defined(connection, "the broadcast's connection")
  }

  /** Past the wire's `behindAfter`, short of its reconcile timeout. */
  const behind = () => vi.advanceTimersByTime(1_000)

  /** `frame` as the page's connection hands it to its channel. */
  function received(frame: Uint8Array<ArrayBuffer>): ChannelFrame {
    const decoded = decode(frame)
    if (!('index' in decoded)) throw new Error('Not a channel frame')
    return decoded
  }

  /** Each publish the send queue holds, in the order they go: its text, or its bytes. */
  function waitingPublishes(page: ClientBroadcast): (string | number[])[] {
    return connectionOf(page)
      ['sendQueue']['settleQueue']()
      .map(({ frame }) => decode(frame))
      .flatMap((decoded): (string | number[])[] => {
        if (decoded.tag === TAG.PUBLISH_ACK_REQ) return [decoded.text]
        return decoded.tag === TAG.PUBLISH_BINARY_ACK_REQ ? [[...decoded.data]] : []
      })
  }

  /** The first byte of each binary publish the send queue holds, in the order they go. */
  function waiting(page: ClientBroadcast): (number | undefined)[] {
    return waitingPublishes(page).flatMap((publish) => (typeof publish === 'string' ? [] : [publish[0]]))
  }
})
