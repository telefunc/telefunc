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
 * Pumps a single producer's chunks to the server through a channel of its own, and returns the channel's id as the
 * metadata the request carries, with `close()` and `abort()` for the request's lifetime.
 *
 * The channel's end, a close or an abort, wakes the pump at once, and its error, if any, goes to `producer.cancel()`.
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
