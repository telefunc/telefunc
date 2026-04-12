export { pumpProducerToChannel }
export type { ChannelPumpRunContext }

import type { StreamingProducer } from '../../types.js'
import { concat } from '../../frame.js'
import { CHANNEL_PUMP_TAG_DATA, CHANNEL_PUMP_TAG_ERROR } from '../../constants.js'
import { ChannelClosedError, ServerChannel } from '../channel.js'
import { isAbort } from '../../../node/server/Abort.js'
import { encodeErrorPayload } from './StreamingResponseBody.js'
import { restoreContext } from '../../../node/server/context/context.js'
import type { Context } from '../../../node/server/context/context.js'
import type { RequestContext } from '../../../node/server/context/requestContext.js'

const TAG_DATA = new Uint8Array([CHANNEL_PUMP_TAG_DATA])
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
 * Creates the channel, registers it, waits for client connection, then streams
 * chunks with proper context restoration and cleanup.
 *
 * Each binary send is tagged: `[TAG_DATA][payload]` for data, `[TAG_ERROR][errorPayload]`
 * for errors. Tags are defined in constants.ts and shared with the client's
 * ChannelChunkReader. Errors are encoded with `encodeErrorPayload` — the same
 * payload format as inline streaming.
 *
 * The pump races three promises each iteration:
 *   1. `responseAbort.errorPromise` — resolves when a sibling throws Abort
 *   2. `cancelledPromise` — resolves when the channel closes (client disconnect/abort)
 *   3. `producer.chunks.next()` — resolves when the producer yields a chunk
 *
 * `cancelledPromise` is needed because async generator `.return()` is queued behind
 * a pending `.next()` — it can't interrupt an in-progress `await` (e.g. `sleep()`).
 * Without it the pump would stay stuck until the generator's current await resolves.
 *
 * Returns the channelId for inclusion in serialized metadata.
 */
function pumpProducerToChannel(
  createProducer: () => StreamingProducer,
  runContext: ChannelPumpRunContext,
  onComplete?: () => void,
): string {
  const channel = new ServerChannel<never, never>()
  channel._registerChannel()
  const producer = createProducer()

  const telefuncId = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
  }

  let cancelled = false
  let resolveCancelled!: (v: IteratorResult<Uint8Array<ArrayBuffer>>) => void
  const cancelledPromise = new Promise<IteratorResult<Uint8Array<ArrayBuffer>>>((r) => {
    resolveCancelled = r
  })
  const doCancel = () => {
    if (cancelled) return
    cancelled = true
    resolveCancelled({ done: true, value: undefined as never })
    producer.cancel()
  }

  channel.onClose(doCancel)
  ;(async () => {
    try {
      await new Promise<void>((resolve, reject) => {
        channel.onOpen(resolve)
        channel.onClose(() => reject(new ChannelClosedError()))
      })
      await restoreContext(runContext.context, async () => {
        const { responseAbort } = runContext.requestContext
        while (true) {
          const { done, value } = await Promise.race([
            responseAbort.errorPromise,
            cancelledPromise,
            producer.chunks.next(),
          ])
          if (done || cancelled) break
          const pending = channel._sendBinary(concat(TAG_DATA, value))
          if (pending) await pending
        }
      })
    } catch (err) {
      if (!(err instanceof ChannelClosedError)) {
        if (isAbort(err)) {
          runContext.requestContext.responseAbort.abort(err.abortValue)
        }
        const errorFrame = concat(TAG_ERROR, encodeErrorPayload(err, telefuncId))
        const pending = channel._sendBinary(errorFrame)
        if (pending) await pending
      }
    } finally {
      doCancel()
      channel.close()
      onComplete?.()
    }
  })()

  return channel.id
}
