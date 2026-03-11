export { buildChannelResponseBody }

import type { StreamingValueServer } from '../../streaming-types.js'
import { generateResponseBody } from './StreamingResponseBody.js'
import type { TelefuncIdentifier } from '../../../shared/constants.js'
import { ChannelClosedError, type ServerChannel } from '../channel.js'
import { restoreContext, Telefunc } from '../../../node/server/getContext.js'
import { RequestContext, restoreRequestContext } from '../../../node/server/requestContext.js'

/** Narrow interface: only what the frame pump needs from a ServerChannel. */
type FrameChannel = Pick<ServerChannel, 'onOpen' | '_onPause' | '_onResume' | 'onClose' | 'sendBinary' | 'close'>

/** Pump indexed data frames from streaming producers into a channel.
 *
 *  Metadata is NOT sent over the channel — it's in the HTTP response body.
 *  Only indexed frames + terminator are pumped.
 *
 *  Backpressure: honors `onPause` / `onResume` callbacks fired by the transport
 *  layer (works for both direct WS and shared/multiplexed WS).
 *
 *  The pump waits for the peer to connect before starting.
 *  If the channel closes before the peer connects, the pump exits cleanly. */
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

  channel._onPause(() => {
    paused = true
  })

  channel._onResume(() => {
    paused = false
    pauseResolve?.()
    pauseResolve = null
  })

  const producers = streamingValues.map((sv) => ({ producer: sv.createProducer(), index: sv.index }))
  const gen = generateResponseBody('', producers, telefuncId, {
    skipMetadata: true,
    responseAbort: runContext.requestContext.responseAbort,
  })

  let cancelled = false
  const doCancel = () => {
    if (cancelled) return
    cancelled = true
    for (const { producer } of producers) producer.cancel()
    gen.return(undefined)
    pauseResolve?.()
    pauseResolve = null
  }

  channel.onClose(doCancel)
  ;(async () => {
    try {
      await new Promise<void>((resolve, reject) => {
        channel.onOpen(resolve)
        channel.onClose(() => reject(new ChannelClosedError()))
      })
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
      // ChannelClosedError from onClose rejection or sendBinary — pump exits cleanly
    } finally {
      doCancel()
      channel.close()
    }
  })()
}
