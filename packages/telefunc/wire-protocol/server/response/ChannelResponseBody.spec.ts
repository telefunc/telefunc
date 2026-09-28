import { expect, test, vi } from 'vitest'
import '../../../node/server/async_hooks.js'
import { pumpProducerToChannel } from './ChannelResponseBody.js'
import { ChannelMux, getChannelMux } from '../mux.js'
import type { ServerChannel } from '../channel.js'
import { IndexedPeer, type PeerSender } from '../IndexedPeer.js'
import { TAG, decode } from '../../shared-ws.js'

/** Records what the channel sends, but for the flow-control limits and totals every attach sends. */
function createSender(frames: Uint8Array[]): PeerSender {
  return {
    send: (frame, onCommit) => {
      onCommit?.()
      if (frame[0] === TAG.WINDOW || frame[0] === TAG.MSG_WINDOW || frame[0] === TAG.SENT) return
      frames.push(frame)
    },
    bufferedAmount: () => 0,
  }
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
    // The page's side of the wire dies silently: what the server sends is lost.
    const gone = new IndexedPeer(createSender(lost), 7, channel._replayBuffer!)
    channel._attachPeer(gone)
    await vi.advanceTimersByTimeAsync(8_000)
    expect(lost.map((frame) => decode(frame).tag)).toEqual([TAG.BINARY, TAG.CLOSE]) // the stream ended meanwhile
    expect(channel._didShutdown).toBe(false)
    channel._onPeerDisconnect(gone, 60_000) // the drop is noticed
    const frames: Uint8Array[] = []
    // The page is back within its reconnect window, with the stream's chunk.
    const attachChannel = getChannelMux()['attachChannel'].bind(getChannelMux())
    attachChannel(channel, { id: channel.id, ix: 7, lastSeq: 1 }, createSender(frames))
    expect(frames.map((frame) => decode(frame).tag)).toEqual([TAG.CLOSE])
  } finally {
    vi.useRealTimers()
    vi.restoreAllMocks()
  }
})
