// A page's broadcast handing out what the server published: the REAL ClientBroadcast on the REAL ClientConnection, over
// an SSE wire that never sends a byte, so each publish is the frame the test hands the page.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createHook } from 'node:async_hooks'
import { getEventListeners } from 'node:events'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, type ChannelFrame } from '../../wire/frame.js'
import { ClientBroadcast } from './ClientBroadcast.js'
import { ChannelClosedError } from '../channelErrors.js'
import { ClientChannel } from './ClientChannel.js'
import { ClientConnection } from './ClientConnection.js'

/** An SSE downstream that never sends a byte. */
const stalledFetch: typeof fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })

beforeEach(() => {
  vi.stubGlobal('fetch', stalledFetch)
})
afterEach(() => {
  vi.unstubAllGlobals()
})

describe('ClientBroadcast delivery', () => {
  it('does not call a listener that an earlier one unsubscribed during the same publish', () => {
    const page = new ClientBroadcast({
      channelId: crypto.randomUUID(),
      connection: {
        transports: [CHANNEL_TRANSPORT.SSE],
        telefuncUrl: 'http://test.local/_telefunc',
        connectionKey: crypto.randomUUID(),
      },
    })
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

  it('an `onOpen()` callback added to a channel that ended unopened registers nothing on its signal and never runs', async () => {
    const page = new ClientChannel(channelOptions())
    page.abort('gone')
    await page.closed.catch(() => {})
    const signal = new AbortController().signal
    const opened = vi.fn()
    page.onOpen(opened, { signal })
    expect({ listeners: getEventListeners(signal, 'abort').length, opened: opened.mock.calls.length }).toEqual({
      listeners: 0,
      opened: 0,
    })
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

// Its close's outcome is decided after its callbacks start: here the cleanup one of them runs outlasts the close.
describe("a page's broadcast whose close, asked by the server, times out on an `onClose()` callback's cleanup while a subscriber waits", () => {
  for (const binary of [false, true])
    for (const removes of [false, true]) {
      it(`${binary ? 'subscribeBinary()' : 'subscribe()'}: ${removes ? 'removed by the callback, its `ready` rejects at the removal with the close, not the end it times out with' : 'still subscribed, its `ready` rejects with the end the close times out with'}`, async () => {
        const page = new ClientBroadcast(channelOptions())
        const stop = binary ? page.subscribeBinary(() => {}) : page.subscribe(() => {})
        page.onClose(() => {
          if (removes) stop()
          return new Promise<void>(() => {})
        })
        page._dispatchFrame(received(encode.close(0, 10, 1)))
        const ended = await page.closed.catch((err: unknown) => err)
        expect(ended).toMatchObject({ message: 'Channel close timed out' })
        const ready = await stop.ready.then(
          () => 'resolved',
          (err: unknown) => err,
        )
        if (removes) {
          expect(ready).toBeInstanceOf(ChannelClosedError)
          expect(ready).not.toBe(ended)
        } else expect(ready).toBe(ended)
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

/** `frame` as the page's connection hands it to its channel. */
function received(frame: Uint8Array<ArrayBuffer>): ChannelFrame {
  const decoded = decode(frame)
  if (!('index' in decoded)) throw new Error('Not a channel frame')
  return decoded
}

describe("a page's subscriber a close callback removes before the server confirmed it", () => {
  for (const method of ['subscribe', 'subscribeBinary'] as const) {
    it(`${method}(): a callback that awaits its \`ready\` lets the close end gracefully`, async () => {
      const page = new ClientBroadcast(channelOptions())
      const stop = page[method](() => {})
      page.onClose(async () => {
        stop()
        await stop.ready.catch(() => {})
      })
      const closed = page.closed
      page._dispatchFrame(received(encode.close(0, 50, 1)))
      await expect(closed).resolves.toBeUndefined()
    })
  }
})

// When a waiting `ready` settles around its broadcast's close on the page: at the moment its subscription can no longer
// take effect, with what is known of the end then.
describe("a page subscriber's `ready` around its broadcast's close", () => {
  const outcome = (ready: Promise<void> | undefined) =>
    Promise.resolve(ready).then(
      () => 'resolved',
      (err: Error) => err.message,
    )

  for (const kind of ['subscribe', 'subscribeBinary'] as const) {
    it(`${kind}(): one removed before the close resolves`, async () => {
      const page = new ClientBroadcast(channelOptions())
      const subscriber = page[kind](() => {})
      subscriber()
      page.abort('gone')
      expect(await outcome(subscriber.ready)).toBe('resolved')
    })

    it(`${kind}(): the server's confirmation during the callbacks' cleanup resolves one still subscribed, not one they removed`, async () => {
      const page = new ClientBroadcast(channelOptions())
      const removed = page[kind](() => {})
      const subscribed = page[kind](() => {})
      const cleanup = Promise.withResolvers<void>()
      page.onClose(() => {
        removed()
        return cleanup.promise
      })
      const removedOutcome = outcome(removed.ready)
      page._dispatchFrame(received(encode.close(0, 500, 1)))
      await Promise.resolve()
      page._dispatchFrame(received(encode.wanted(0, 2)))
      const subscribedOutcome = await outcome(subscribed.ready)
      cleanup.resolve()
      await page.closed
      expect({ removed: await removedOutcome, subscribed: subscribedOutcome }).toEqual({
        removed: 'Channel is closed',
        subscribed: 'resolved',
      })
    })

    it(`${kind}(): one still subscribed at a forced end rejects with its error`, async () => {
      const page = new ClientBroadcast(channelOptions())
      const subscriber = page[kind](() => {})
      page.abort('gone')
      const ended = await page.closed.catch((err: unknown) => err)
      expect(await subscriber.ready.catch((err: unknown) => err)).toBe(ended)
    })

    it(`${kind}(): one that resolved stays resolved when its callback removes it`, async () => {
      const page = new ClientBroadcast(channelOptions())
      const subscriber = page[kind](() => {})
      page._dispatchFrame(received(encode.wanted(0, 1)))
      await subscriber.ready
      page.onClose(() => subscriber())
      page.abort('gone')
      await page.closed.catch(() => {})
      expect(await outcome(subscriber.ready)).toBe('resolved')
    })
  }
})
