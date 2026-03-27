export { pumpClientProducerToChannel }

import { CHANNEL_PUMP_TAG_DATA } from '../../constants.js'
import { concat } from '../../frame.js'
import { ChannelClosedError } from '../../channel-errors.js'
import { ClientChannel } from '../channel.js'
import type { ChannelTransports } from '../../constants.js'
import type { StreamingProducer } from '../../types.js'

const TAG_DATA = new Uint8Array([CHANNEL_PUMP_TAG_DATA])

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
function pumpClientProducerToChannel(createProducer: () => StreamingProducer, channelTransports: ChannelTransports) {
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    transports: channelTransports,
    defer: true,
  })

  const producer = createProducer()

  let cancelled = false
  let resolveCancelled!: (v: IteratorResult<Uint8Array<ArrayBuffer>>) => void
  const cancelledPromise = new Promise<IteratorResult<Uint8Array<ArrayBuffer>>>((r) => {
    resolveCancelled = r
  })
  const doCancel = (err?: Error) => {
    if (cancelled) return
    cancelled = true
    resolveCancelled({ done: true, value: undefined as never })
    producer.cancel(err)
  }

  channel.onClose(doCancel)

  void (async () => {
    try {
      await new Promise<void>((resolve, reject) => {
        channel.onOpen(resolve)
        channel.onClose(() => reject(new ChannelClosedError()))
      })
      while (true) {
        const { done, value } = await Promise.race([cancelledPromise, producer.chunks.next()])
        if (done || cancelled) break
        const pending = channel._sendBinaryAwaitable(concat(TAG_DATA, value))
        if (pending) await pending
      }
    } catch {
      // ChannelClosedError — either from onOpen rejection (closed before connect)
      // or from _sendBinaryAwaitable (closed mid-send, e.g. by abort(res)).
      // Abort semantics propagate through doCancel(err) → producer.cancel(err) →
      // reader.cancel(err), not through this catch.
    } finally {
      doCancel()
      channel.close()
    }
  })()

  return channel
}
