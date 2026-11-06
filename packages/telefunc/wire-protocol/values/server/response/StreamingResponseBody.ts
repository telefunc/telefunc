export { buildInlineResponseBody, encodeErrorPayload }

import type { Readable } from 'node:stream'
import type { StreamingProducer, StreamingValueServer } from '../../types.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { encodeU32, encodeIndexedFrame, textEncoder } from '../../../wire/envelope.js'
import { STREAMING_ERROR_FRAME_MARKER, STREAMING_ERROR_TYPE } from '../../../constants.js'
import type { StreamingErrorFrameAbort, StreamingErrorFrameBug } from '../../../constants.js'
import { isAbort } from '../../../../node/server/Abort.js'
import type { AbortError } from '../../../../shared/Abort.js'
import {
  handleTelefunctionBug,
  validateTelefunctionError,
} from '../../../../node/server/runTelefunc/validateTelefunctionError.js'
import type { ResponseAbortSource } from '../../../../node/server/context/requestContext.js'
import type { TelefuncId } from '../../../../node/server/runTelefunc/serializeTelefunctionResult.js'
import { restoreContext } from '../../../../node/server/context/context.js'
import type { Context } from '../../../../node/server/context/context.js'
import { createPushReadable } from '../../../../utils/pushReadable.js'
import { createPushReadableStream } from '../../../../utils/pushReadableStream.js'
import { Fifo } from '../../../../utils/Fifo.js'

const EMPTY = new Uint8Array(0)

type Entry = { producer: StreamingProducer; index: number }
type Settled = { entry: Entry; result: IteratorResult<Uint8Array<ArrayBuffer>> } | { err: unknown }

/**
 * Stream the inline response body: framed metadata + multiplexed streaming values.
 *
 * Wire shape:
 *   `[u32 metadata_len][metadata_bytes] (([u32 frame_len][u8 index][payload])* | error_frame) [u32 terminator]`
 *   - terminator: `0x00000000` on success.
 *   - error frame: `0xFFFFFFFF [u32 payload_len][payload]` instead of terminator.
 *
 * For `'binary-inline'`: each frame is the raw bytes.
 * For `'sse-inline'`: `encodeFrame` wraps each frame as `data: <base64>\n\n`.
 *
 * Push-based, not generator-based: the merge loop is a plain `async` function
 * that calls `sink.push(frame)` per emit, into a backpressured sink — `PushReadable`
 * on Node, `PushReadableStream` on Web. Generators paid V8's async-iterator protocol
 * cost on every `yield`; push avoids it. Backpressure: `sink.push` returns `false`
 * when the consumer's buffer is full; the loop awaits `wakeDrain` (resolved by the
 * sink's `read` / `pull` callback) before the next push.
 *
 * Cleanup paths — one `cancelled` flag means "consumer gone, stop emitting":
 *   - `abortSignal` fires                    → onConsumerGone: cancelled=true + cancelProducers; emits no-op.
 *   - consumer cancels sink                  → onConsumerGone (same).
 *   - producer throws (caught)               → cancelProducers only; emits still flow if wire alive.
 *   - normal completion                      → emit terminator.
 * `finally` always closes the sink and calls `onComplete()`.
 */
function buildInlineResponseBody(runContext: {
  metadataSerialized: string
  streamingValues: StreamingValueServer[]
  telefuncId: TelefuncId
  context: Context
  abortSignal: AbortSignal
  responseAbort: Pick<ResponseAbortSource, 'errorPromise' | 'abort'>
  onComplete: () => void
  encodeFrame: (frame: Uint8Array<ArrayBuffer>) => Uint8Array<ArrayBuffer>
  useNodeStream: boolean
}): Readable | ReadableStream<Uint8Array<ArrayBuffer>> {
  const {
    metadataSerialized,
    streamingValues,
    telefuncId,
    context,
    abortSignal,
    responseAbort,
    onComplete,
    encodeFrame,
    useNodeStream,
  } = runContext

  // Producers are constructed eagerly so the abort listener can cancel them
  // even before the loop starts. Cancelling interrupts any suspended
  // `reader.read()` inside a producer — `iter.return()` alone can't.
  const producers = streamingValues.map((sv) => ({ producer: sv.createProducer(), index: sv.index }))

  let cancelled = false
  // What the merge loop takes next, in the order it settled. A `Promise.race()` per frame would leave a reaction on
  // each promise it's given until that one settles, so the abort's promise and an idle producer's `.next()` would
  // grow the heap with every frame of a long-lived stream.
  const settled = new Fifo<Settled>()
  // Async-generator return() queues behind a pending next(). Waking on cancellation
  // separately lets the merge loop release its lifecycle hold in finally.
  let wakeMerge = () => {}
  let drainResolve: (() => void) | null = null

  const wakeDrain = () => {
    const r = drainResolve
    drainResolve = null
    r?.()
  }
  // `producersStopped` is a private idempotency latch — calling `producer.cancel()`
  // twice on the same producer is at best wasted work, at worst a double-free.
  let producersStopped = false
  const cancelProducers = () => {
    if (producersStopped) return
    producersStopped = true
    for (const { producer } of producers) producer.cancel()
    // Unblock any emit awaiting drain so the IIFE can progress to its catch/finally.
    wakeDrain()
  }
  const onConsumerGone = () => {
    if (cancelled) return
    cancelled = true
    wakeMerge()
    cancelProducers()
  }

  const sink = useNodeStream
    ? createPushReadable(onConsumerGone, wakeDrain)
    : createPushReadableStream<Uint8Array<ArrayBuffer>>(onConsumerGone, wakeDrain)

  const emit = async (frame: Uint8Array<ArrayBuffer>): Promise<void> => {
    if (cancelled) return
    if (!sink.push(frame)) await new Promise<void>((resolve) => (drainResolve = resolve))
  }

  const settle = (item: Settled) => {
    settled.push(item)
    wakeMerge()
  }
  const pullNext = async (entry: Entry) => {
    try {
      settle({ entry, result: await entry.producer.chunks.next() })
    } catch (err) {
      settle({ err })
    }
  }

  void (async () => {
    abortSignal.addEventListener('abort', onConsumerGone, { once: true })
    // A response-wide abort takes its turn after what settled before it, and routes us into the catch below.
    responseAbort.errorPromise.catch((err: unknown) => settle({ err }))
    try {
      // The telefunc context is restored around the whole merge loop, like the
      // channel pump (ChannelResponseBody.ts) does, so `getContext()` works
      // inside producer bodies (generator code runs lazily, on `.next()`).
      await restoreContext(context, async () => {
        // The producers' first `.next()` must run inside the synchronous
        // stretch of the restored context: in sync mode the context global is
        // cleared on the next macrotask, and the header `emit` below can
        // suspend until the consumer's first read (the Web sink applies
        // backpressure from the first push). So start the pulls before
        // emitting the header.
        for (const entry of producers) void pullNext(entry)

        // ── Header: [u32 metadata_len][metadata_bytes] — atomic on the wire,
        // so don't break between the length and its payload.
        const metadataBytes = textEncoder.encode(metadataSerialized)
        await emit(encodeFrame(encodeU32(metadataBytes.length)))
        await emit(encodeFrame(metadataBytes))

        // ── Multiplexed merge loop ──
        // Each producer keeps one in-flight `.next()`, and gets the next one once its frame went out, so a fast
        // producer doesn't starve the others.
        let open = producers.length
        while (open > 0) {
          if (settled.length === 0 && !cancelled) await new Promise<void>((resolve) => (wakeMerge = resolve))
          // Consumer is gone: bail before emitting a "this producer is done" frame
          // — would also be misleading for cancelled producers returning done.
          if (cancelled) return
          const next = settled.shift()!
          if ('err' in next) throw next.err
          const { entry, result } = next
          if (result.done) {
            // Empty-payload frame so the client knows this index is done
            // without waiting for the global terminator.
            await emit(encodeFrame(encodeIndexedFrame(entry.index, EMPTY)))
            open--
          } else {
            await emit(encodeFrame(encodeIndexedFrame(entry.index, result.value as Uint8Array<ArrayBuffer>)))
            void pullNext(entry)
          }
        }
        // `emit` is already a no-op if `cancelled` — but we'd rather not allocate
        // the terminator frame at all when there's no one to receive it.
        if (cancelled) return
        await emit(encodeFrame(encodeU32(0)))
      })
    } catch (err) {
      // Cancel producers immediately so peer producers stop emitting work
      // that would never reach the consumer while we emit the error frames.
      cancelProducers()
      // Propagate aborts to side-channels (request argument channels etc.)
      // regardless of cancellation state — listeners care even when the
      // consumer is already gone.
      if (isAbort(err)) responseAbort.abort(err.abortValue)
      // Error frame: [marker][u32 payload_len][payload].
      const errorBytes = encodeErrorPayload(err, telefuncId)
      await emit(encodeFrame(encodeU32(STREAMING_ERROR_FRAME_MARKER)))
      await emit(encodeFrame(encodeU32(errorBytes.byteLength)))
      await emit(encodeFrame(errorBytes))
    } finally {
      abortSignal.removeEventListener('abort', onConsumerGone)
      sink.close()
      onComplete()
    }
  })()

  return sink.readable
}

/** Serialize a streaming error as its on-wire payload string. Used by both the
 *  inline path (with a u32 marker prefix) and the channel pump path (with a
 *  tag prefix) — only the framing differs. */
function encodeErrorPayload(err: unknown, telefuncId: TelefuncId): Uint8Array<ArrayBuffer> {
  validateTelefunctionError(err, telefuncId)
  let errorPayload: string
  if (isAbort(err)) {
    const abortError: AbortError = err
    try {
      const payload: StreamingErrorFrameAbort = {
        type: STREAMING_ERROR_TYPE.ABORT,
        abortValue: abortError.abortValue,
      }
      errorPayload = stringify(payload)
    } catch (serializationErr) {
      // Abort value not serializable — fall back to bug.
      handleTelefunctionBug(serializationErr)
      const payload: StreamingErrorFrameBug = { type: STREAMING_ERROR_TYPE.BUG }
      errorPayload = stringify(payload)
    }
  } else {
    handleTelefunctionBug(err)
    const payload: StreamingErrorFrameBug = { type: STREAMING_ERROR_TYPE.BUG }
    errorPayload = stringify(payload)
  }
  return textEncoder.encode(errorPayload)
}
