// A page's broadcast handing out what the server published: the REAL ClientBroadcast on the REAL ClientConnection, over
// an SSE wire that never sends a byte, so each publish is the frame the test hands the page.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createHook } from 'node:async_hooks'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, type ChannelFrame } from '../../wire/frame.js'
import { ClientBroadcast } from './ClientBroadcast.js'
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
