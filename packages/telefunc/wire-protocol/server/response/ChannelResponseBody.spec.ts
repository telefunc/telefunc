import { expect, test, vi } from 'vitest'
import '../../../node/server/async_hooks.js'
import { pumpProducerToChannel } from './ChannelResponseBody.js'
import { ChannelMux } from '../mux.js'
import type { ServerChannel } from '../channel.js'
import { IndexedPeer } from '../IndexedPeer.js'
import { ReplayBuffer } from '../../replay-buffer.js'
import { TAG, decode } from '../../shared-ws.js'

function createPeer(frames: Uint8Array[]) {
  return new IndexedPeer(
    { send: (frame) => void frames.push(frame) },
    7,
    new ReplayBuffer(1024 * 1024, 60_000, 2 * 1024 * 1024),
  )
}

test('a returned stream that ends while its page is away is still closing when its page is back within its reconnect window', async () => {
  vi.useFakeTimers()
  try {
    const register = vi.spyOn(ChannelMux.prototype, 'registerChannel')
    const chunks = (async function* () {
      yield new Uint8Array([7]) as Uint8Array<ArrayBuffer>
    })()
    pumpProducerToChannel(() => ({ chunks, cancel: () => {} }), {
      context: {} as never,
      requestContext: { responseAbort: { errorPromise: new Promise(() => {}), abort: () => {} } } as never,
      telefunctionName: 'onCountdown',
      telefuncFilePath: '/countdown.telefunc.ts',
    })
    const channel = register.mock.calls[0]![0] as ServerChannel
    const lost: Uint8Array[] = []
    const gone = createPeer(lost) // the page's side of the wire dies silently: what the server sends is lost
    channel._attachPeer(gone)
    await vi.advanceTimersByTimeAsync(8_000)
    expect(lost.map((frame) => decode(frame).tag)).toEqual([TAG.BINARY, TAG.CLOSE]) // the stream ended meanwhile
    expect(channel._didShutdown).toBe(false)
    channel._onPeerDisconnect(gone, 60_000) // the drop is noticed
    const frames: Uint8Array[] = []
    channel._attachPeer(createPeer(frames)) // the page is back within its reconnect window
    expect(frames.map((frame) => decode(frame).tag)).toEqual([TAG.CLOSE])
  } finally {
    vi.useRealTimers()
    vi.restoreAllMocks()
  }
})
