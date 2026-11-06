export { pumpClientProducerToChannel }

import { CHANNEL_PUMP_TAG_ERROR } from '../../../constants.js'
import { concat, textEncoder } from '../../../wire/envelope.js'
import { ChannelClosedError } from '../../../channel/channelErrors.js'
import { ClientChannel, type ConnectionOptions } from '../../../channel/client/ClientChannel.js'
import type { StreamingProducer } from '../../types.js'
import { randomUuid } from '../../../../utils/randomUuid.js'
import { createDeferred } from '../../../../utils/createDeferred.js'
import { sendChunks } from '../../sendChunks.js'

const TAG_ERROR = new Uint8Array([CHANNEL_PUMP_TAG_ERROR])

/**
 * Pump a single producer's chunks to the server through a dedicated ClientChannel.
 *
 * Creates the channel, starts the pump, and returns the channel so the caller
 * can register it for abort handling.
 *
 * The pump races `cancelledPromise` against `producer.chunks.next()` so that
 * channel close / abort breaks the loop immediately.
 *
 * On abort: the abort error propagates through the race (reject, not resolve),
 * so the catch block sees it. On clean close: resolves with null.
 */
function pumpClientProducerToChannel(createProducer: () => StreamingProducer, connection: ConnectionOptions) {
  const channel = new ClientChannel({ channelId: randomUuid(), connection })

  const producer = createProducer()

  let cancelled = false
  const { promise: cancelledPromise, resolve: resolveCancelled } = createDeferred<null>()
  const doCancel = (err?: Error) => {
    if (cancelled) return
    cancelled = true
    resolveCancelled(null)
    producer.cancel(err)
  }

  channel.onClose(doCancel)

  async function runUploadPump() {
    try {
      await channel.opened
      await sendChunks(channel, producer, cancelledPromise)
    } catch (err) {
      // A channel that ended has nothing to report: `opened` rejects with what ended it, before it opened, and
      // sendBinary with ChannelClosedError, mid-send (e.g. by abort(res)).
      // Abort semantics propagate through doCancel(err) → producer.cancel(err) →
      // reader.cancel(err), not through this catch.
      // Anything else is the source failing: the server's stream errors rather than end as if complete. No detail of the
      // page's error crosses to the server.
      if (!(err instanceof ChannelClosedError) && !channel.isClosed) {
        channel.sendBinary(concat(TAG_ERROR, textEncoder.encode('{}')))
      }
    } finally {
      doCancel()
      // A server that is away gets the upload's end once back, as an open channel waits for it.
      channel.close({ timeout: channel._connection.reconnectWindow() })
    }
  }

  void runUploadPump()

  return {
    metadata: { channelId: channel.id },
    async close() {
      await channel.close()
    },
    abort() {
      channel.abort()
    },
  }
}
