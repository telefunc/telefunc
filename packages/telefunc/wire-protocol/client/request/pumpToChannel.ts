export { pumpClientProducerToChannel }
export type { PumpChannelOptions }

import { CHANNEL_PUMP_TAG_DATA, CHANNEL_PUMP_TAG_END, CHANNEL_PUMP_TAG_ERROR } from '../../constants.js'
import { concat, textEncoder } from '../../frame.js'
import { ChannelClosedError } from '../../channel-errors.js'
import { ClientChannel } from '../channel.js'
import type { StreamingProducer } from '../../types.js'
import { randomUuid } from '../../../utils/randomUuid.js'

/** What the pump's channel shares with the call's other channels. */
type PumpChannelOptions = Omit<ConstructorParameters<typeof ClientChannel>[0], 'channelId' | 'ack' | 'key'>

const TAG_DATA = new Uint8Array([CHANNEL_PUMP_TAG_DATA])
const TAG_ERROR = new Uint8Array([CHANNEL_PUMP_TAG_ERROR])
const TAG_END = new Uint8Array([CHANNEL_PUMP_TAG_END])
// A larger chunk goes in parts, each a message under every channel's frame limit (32 MiB on Cloudflare).
const CHUNK_PART_BYTES = 8 * 1024 * 1024

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
function pumpClientProducerToChannel(createProducer: () => StreamingProducer, opts: PumpChannelOptions) {
  const channel = new ClientChannel({ channelId: randomUuid(), ...opts })

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
        if (cancelled) break
        if (done) {
          channel.sendBinary(TAG_END)
          break
        }
        for (let at = 0; at < value.byteLength; at += CHUNK_PART_BYTES) {
          const pending = channel._sendBinary(concat(TAG_DATA, value.subarray(at, at + CHUNK_PART_BYTES)))
          if (pending) await pending
        }
      }
    } catch (err) {
      // ChannelClosedError — either from onOpen rejection (closed before connect)
      // or from sendBinary (closed mid-send, e.g. by abort(res)).
      // Abort semantics propagate through doCancel(err) → producer.cancel(err) →
      // reader.cancel(err), not through this catch.
      // Anything else is the source failing: the server's stream errors rather than end as if complete. No detail of the
      // page's error crosses to the server.
      if (!(err instanceof ChannelClosedError) && !channel.isClosed)
        channel._sendBinary(concat(TAG_ERROR, textEncoder.encode('{}')))
    } finally {
      doCancel()
      // A server that is away gets the upload's end once back, as an open channel waits for it.
      channel.close({ timeout: channel._reconnectWindow() })
    }
  })()

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
