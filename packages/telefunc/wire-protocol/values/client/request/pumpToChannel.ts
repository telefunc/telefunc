export { pumpClientProducerToChannel }

import {
  CHANNEL_PUMP_TAG_DATA,
  CHANNEL_PUMP_TAG_END,
  CHANNEL_PUMP_TAG_ERROR,
  STREAM_CHUNK_PART_BYTES,
} from '../../../constants.js'
import { concat, textEncoder } from '../../../wire/envelope.js'
import { ChannelClosedError } from '../../../channel/channelErrors.js'
import { ClientChannel, type ConnectionOptions } from '../../../channel/client/ClientChannel.js'
import type { StreamingProducer } from '../../types.js'
import { randomUuid } from '../../../../utils/randomUuid.js'
import { createDeferred } from '../../../../utils/createDeferred.js'
import { createStopRace } from '../../../../utils/createStopRace.js'

const TAG_DATA = new Uint8Array([CHANNEL_PUMP_TAG_DATA])
const TAG_ERROR = new Uint8Array([CHANNEL_PUMP_TAG_ERROR])
const TAG_END = new Uint8Array([CHANNEL_PUMP_TAG_END])

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
 * so the catch block sees it. On clean close: resolves with `{ done: true }`.
 */
function pumpClientProducerToChannel(createProducer: () => StreamingProducer, connection: ConnectionOptions) {
  const channel = new ClientChannel({ channelId: randomUuid(), connection })

  const producer = createProducer()

  let cancelled = false
  const { promise: cancelledPromise, resolve: resolveCancelled } =
    createDeferred<IteratorResult<Uint8Array<ArrayBuffer>>>()
  const doCancel = (err?: Error) => {
    if (cancelled) return
    cancelled = true
    resolveCancelled({ done: true, value: undefined as never })
    producer.cancel(err)
  }

  channel.onClose(doCancel)

  async function runUploadPump() {
    try {
      await channel.opened
      await sendChunks(channel, producer, cancelledPromise, () => cancelled)
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

/** Sends the producer's chunks, each in parts, then its end; stops at the first chunk read once cancelled. */
async function sendChunks(
  channel: ClientChannel,
  producer: StreamingProducer,
  cancelledPromise: Promise<IteratorResult<Uint8Array<ArrayBuffer>>>,
  isCancelled: () => boolean,
): Promise<void> {
  const race = createStopRace(cancelledPromise)
  while (true) {
    const { done, value } = await race(producer.chunks.next())
    if (isCancelled()) return
    if (done) {
      channel.sendBinary(TAG_END)
      return
    }
    for (let at = 0; at < value.byteLength; at += STREAM_CHUNK_PART_BYTES) {
      const pending = channel._sendBinary(concat(TAG_DATA, value.subarray(at, at + STREAM_CHUNK_PART_BYTES)))
      if (pending) await pending
    }
  }
}
