// A page's lossy publishes: the REAL ClientBroadcast on the REAL ClientConnection and SendGate, over an SSE wire that
// never reconciles, as ClientChannel.lossySend.spec.ts runs a channel's lossy sends: the gate holds every frame, and one
// held past the wire's `behindAfter` is behind, so a newer lossy one of its stream may take its place.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ClientBroadcast } from './ClientBroadcast.js'

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
})

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

/** The first byte of each publish the gate holds, in the order they go. */
function waiting(page: ClientBroadcast): (number | undefined)[] {
  return (page as any).connection.gate
    .settleQueue()
    .map(({ frame }: { frame: Uint8Array<ArrayBuffer> }) => decode(frame))
    .flatMap((decoded: ReturnType<typeof decode>) =>
      decoded.tag === TAG.PUBLISH_BINARY_ACK_REQ ? [decoded.data[0]] : [],
    )
}
