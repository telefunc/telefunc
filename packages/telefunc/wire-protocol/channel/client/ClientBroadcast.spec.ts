// A page's broadcast handing out what the server published: the REAL ClientBroadcast on the REAL ClientConnection, over
// an SSE wire that never sends a byte, so each publish is the frame the test hands the page.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, type ChannelFrame } from '../../wire/frame.js'
import { ClientBroadcast } from './ClientBroadcast.js'

/** An SSE downstream that never sends a byte. */
const stalledFetch = (async () => new Response(new ReadableStream({ start() {} }), { status: 200 })) as typeof fetch

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

/** `frame` as the page's connection hands it to its channel. */
function received(frame: Uint8Array<ArrayBuffer>): ChannelFrame {
  const decoded = decode(frame)
  if (!('index' in decoded)) throw new Error('Not a channel frame')
  return decoded
}
