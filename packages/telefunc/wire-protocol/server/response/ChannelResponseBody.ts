export { buildChannelResponseBody }

import type { StreamingValueServer } from '../../streaming-types.js'
import { generateResponseBody } from './StreamingResponseBody.js'
import type { TelefuncIdentifier } from '../../../shared/constants.js'
import type { ServerChannel } from '../../../node/server/channel.js'
import { hasProp } from '../../../utils/hasProp.js'
import { assert } from '../../../utils/assert.js'
import { restoreContext, Telefunc } from '../../../node/server/getContext.js'
import { RequestContext, restoreRequestContext } from '../../../node/server/requestContext.js'

/** Narrow interface: only what the frame pump needs from a ServerChannel. */
type FrameChannel = Pick<ServerChannel, 'peerConnected' | 'isOpen' | 'listen' | 'onAbort' | 'sendBinary' | 'close'>
/** Pump indexed data frames from streaming producers into a WebSocket channel.
 *
 *  Metadata is NOT sent over the WS — it's already in the HTTP response body.
 *  Only indexed frames + terminator are pumped.
 *
 *  Backpressure: honors `{ p: 1 }` (pause) / `{ p: 0 }` (resume) messages
 *  sent by the client's `ChannelStreamReader` when its buffer hits watermarks.
 *
 *  The pump waits for the WebSocket peer to connect before starting.
 *  If the channel closes before the peer connects, the pump exits cleanly.
 *
 *  Cancellation: when the channel closes (peer disconnect / TTL), we cancel
 *  all producers and terminate the generator — mirroring how the HTTP streaming
 *  path uses abortSignal to cancel on connection drop. Without this, the pump
 *  would hang forever on a blocked generator .next(). */
function buildChannelResponseBody(
  streamingValues: StreamingValueServer[],
  telefuncId: TelefuncIdentifier,
  channel: FrameChannel,
  runContext: {
    requestContext: RequestContext
    providedContext: Telefunc.Context | null
  },
): void {
  let paused = false
  let pauseResolve: (() => void) | null = null

  channel.listen((msg) => {
    assert(hasProp(msg, 'p'))
    paused = msg.p === 1
    if (!paused) {
      pauseResolve?.()
      pauseResolve = null
    }
  })

  const producers = streamingValues.map((sv) => ({ producer: sv.createProducer(), index: sv.index }))
  const gen = generateResponseBody('', producers, telefuncId, { skipMetadata: true })

  let cancelled = false
  const doCancel = () => {
    if (cancelled) return
    cancelled = true
    for (const { producer } of producers) producer.cancel()
    gen.return(undefined)
    // Unblock any paused pump
    pauseResolve?.()
    pauseResolve = null
  }

  // Wire up: when the peer disconnects, cancel the pump.
  channel.onAbort(doCancel)
  ;(async () => {
    try {
      await channel.peerConnected
      restoreContext(runContext.providedContext)
      restoreRequestContext(runContext.requestContext)
      for await (const frame of gen) {
        if (cancelled) break
        if (paused)
          await new Promise<void>((r) => {
            pauseResolve = r
          })
        if (cancelled) break
        channel.sendBinary(frame)
      }
    } catch {
      // ChannelClosedError from peerConnected rejection or sendBinary guard — pump exits cleanly
    } finally {
      doCancel()
      channel.close()
    }
  })()
}
