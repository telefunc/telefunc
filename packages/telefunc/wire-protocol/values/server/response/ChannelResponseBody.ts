export { pumpProducerToChannel }
export type { ChannelPumpRunContext }

import type { StreamingProducer } from '../../types.js'
import { concat } from '../../../wire/envelope.js'
import { CHANNEL_PUMP_TAG_ERROR } from '../../../constants.js'
import { ServerChannel } from '../../../channel/server/ServerChannel.js'
import { ChannelClosedError } from '../../../channel/channelErrors.js'
import { reconnectWindow } from '../../../../node/server/serverConfig.js'
import { getChannelMux } from '../../../channel/server/ChannelMux.js'
import { isAbort } from '../../../../node/server/Abort.js'
import { encodeErrorPayload } from './StreamingResponseBody.js'
import { serializeAbortReason } from '../../../channel/channel.js'
import { restoreContext } from '../../../../node/server/context/context.js'
import type { Context } from '../../../../node/server/context/context.js'
import type { RequestContext } from '../../../../node/server/context/requestContext.js'
import { createDeferred } from '../../../../utils/createDeferred.js'
import { sendChunks } from '../../sendChunks.js'

const TAG_ERROR = new Uint8Array([CHANNEL_PUMP_TAG_ERROR])

type ChannelPumpRunContext = {
  context: Context
  requestContext: RequestContext
  telefunctionName: string
  telefuncFilePath: string
}

/**
 * Pump a single producer's chunks through a dedicated ServerChannel.
 *
 * Registers the channel, waits for client connection, then streams
 * chunks with proper context restoration and cleanup.
 *
 * Each binary send is tagged: `[TAG_DATA][payload]` for data (`[TAG_PART]` for a chunk's part before its last), `[TAG_ERROR][errorPayload]`
 * for errors, `[TAG_END]` at the producer's end. Tags are defined in constants.ts and shared with the client's
 * ChannelStreamSource. Errors are encoded with `encodeErrorPayload` — the same
 * payload format as inline streaming.
 *
 * Each wait, for the page to attach, for `producer.chunks.next()` and for the page's credit, races one `stop` promise,
 * built once from:
 *   1. `responseAbort.errorPromise` — rejects when a sibling throws Abort
 *   2. `cancelledPromise` — resolves when the channel closes (client disconnect/abort)
 *
 * `cancelledPromise` is needed because async generator `.return()` is queued behind
 * a pending `.next()` — it can't interrupt an in-progress `await` (e.g. `sleep()`).
 * Without it the pump would stay stuck until the generator's current await resolves.
 */
function pumpProducerToChannel(
  channel: ServerChannel<never, never>,
  producer: StreamingProducer,
  runContext: ChannelPumpRunContext,
  onComplete: () => void,
): void {
  getChannelMux().registerChannel(channel as unknown as ServerChannel<unknown, unknown>)

  const telefuncId = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
  }

  let cancelled = false
  const { promise: cancelledPromise, resolve: resolveCancelled } = createDeferred<null>()
  const doCancel = () => {
    if (cancelled) return
    cancelled = true
    resolveCancelled(null)
    producer.cancel()
  }

  channel.onClose(doCancel)

  async function runChannelPump() {
    const stop = Promise.race([runContext.requestContext.responseAbort.errorPromise, cancelledPromise])
    try {
      if ((await Promise.race([channel.opened, stop])) === null) return
      await restoreContext(runContext.context, () => sendChunks(channel, producer, stop))
    } catch (err) {
      if (err instanceof ChannelClosedError) return
      // A reason JSON can't hold goes to none of the response's values: the error frame reports it.
      if (isAbort(err)) runContext.requestContext.responseAbort.abort(serializeAbortReason(err.abortValue).value)
      if (channel.isClosed) return
      try {
        await Promise.race([channel._sendBinary(concat(TAG_ERROR, encodeErrorPayload(err, telefuncId))), stop])
      } catch {
        // The channel ended, or the response stopped: the pump doesn't wait for its page to take the frame.
      }
    } finally {
      doCancel()
      // A page that is away gets the stream's end once back, as an open channel waits for it.
      channel.close({ timeout: reconnectWindow() })
      onComplete()
    }
  }

  void runChannelPump()
}
