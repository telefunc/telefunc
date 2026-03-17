export { buildChannelResponseBody }

import type { StreamingValueServer } from '../../streaming-types.js'
import { generateResponseBody } from './StreamingResponseBody.js'
import type { TelefuncIdentifier } from '../../../node/server/runTelefunc/serializeTelefunctionResult.js'
import { ChannelClosedError, type ServerChannel } from '../channel.js'
import { restoreContext, Telefunc } from '../../../node/server/getContext.js'
import { RequestContext, restoreRequestContext } from '../../../node/server/requestContext.js'

/** Narrow interface: only what the frame pump needs from a ServerChannel. */
type FrameChannel = Pick<ServerChannel, 'onOpen' | 'onClose' | 'close' | '_sendBinaryAwaitable'>

/** Pump indexed data frames from streaming producers into a channel.
 *
 *  Metadata is NOT sent over the channel — it's in the HTTP response body.
 *  Only indexed frames + terminator are pumped.
 *
 *  The pump waits for the client to connect before starting.
 *  If the channel closes before the client connects, the pump exits cleanly. */
function buildChannelResponseBody(
  streamingValues: StreamingValueServer[],
  telefuncId: TelefuncIdentifier,
  channel: FrameChannel,
  runContext: {
    requestContext: RequestContext
    providedContext: Telefunc.Context | null
  },
): void {
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
        const pending = channel._sendBinaryAwaitable(frame)
        if (pending) await pending
      }
    } catch {
      // ChannelClosedError from onClose rejection or sendBinary — pump exits cleanly
    } finally {
      doCancel()
      channel.close()
    }
  })()
}
