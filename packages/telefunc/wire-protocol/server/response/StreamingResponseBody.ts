export { buildInlineResponseBody, encodeErrorPayload }

import type { Readable } from 'node:stream'
import type { StreamingValueServer } from '../../types.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { encodeU32, encodeIndexedFrame, textEncoder } from '../../frame.js'
import { STREAMING_ERROR_FRAME_MARKER, STREAMING_ERROR_TYPE } from '../../constants.js'
import type { StreamingErrorFrameAbort, StreamingErrorFrameBug } from '../../constants.js'
import { isAbort } from '../../../node/server/Abort.js'
import type { AbortError } from '../../../shared/Abort.js'
import {
  handleTelefunctionBug,
  validateTelefunctionError,
} from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import type { ResponseAbortSource } from '../../../node/server/context/requestContext.js'
import type { TelefuncId } from '../../../node/server/runTelefunc/serializeTelefunctionResult.js'
import { createPushReadable } from '../../push-readable.js'
import { createPushReadableStream } from '../../push-readable-stream.js'

const EMPTY = new Uint8Array(0)

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
    cancelProducers()
  }

  const sink = useNodeStream
    ? createPushReadable(onConsumerGone, wakeDrain)
    : createPushReadableStream<Uint8Array<ArrayBuffer>>(onConsumerGone, wakeDrain)

  const emit = async (frame: Uint8Array<ArrayBuffer>): Promise<void> => {
    if (cancelled) return
    if (!sink.push(frame)) await new Promise<void>((resolve) => (drainResolve = resolve))
  }

  type RaceEntry = {
    index: number
    iter: AsyncIterator<Uint8Array<ArrayBuffer>>
    pending: Promise<{ entry: RaceEntry; result: IteratorResult<Uint8Array<ArrayBuffer>> }>
  }
  const advance = (entry: RaceEntry) => {
    entry.pending = entry.iter.next().then((result) => ({ entry, result }))
  }

  void (async () => {
    abortSignal.addEventListener('abort', onConsumerGone, { once: true })
    try {
      // ── Header: [u32 metadata_len][metadata_bytes] — atomic on the wire,
      // so don't break between the length and its payload.
      const metadataBytes = textEncoder.encode(metadataSerialized)
      await emit(encodeFrame(encodeU32(metadataBytes.length)))
      await emit(encodeFrame(metadataBytes))

      // ── Multiplexed merge loop ──
      // Each producer keeps one in-flight `.next()`; `Promise.race` picks
      // whichever resolves first. Winner is rotated to the end of `active` so
      // a fast producer at index 0 doesn't starve others (race resolves in
      // array order on ties).
      const active: RaceEntry[] = []
      for (const { producer, index } of producers) {
        const entry: RaceEntry = { index, iter: producer.chunks, pending: null! }
        advance(entry)
        active.push(entry)
      }

      while (active.length > 0) {
        // `responseAbort.errorPromise` rejects on a response-wide abort; it
        // wins the race and routes us into the catch below.
        const { entry, result } = await Promise.race([responseAbort.errorPromise, ...active.map((e) => e.pending)])
        // Consumer is gone: bail before emitting a "this producer is done" frame
        // — would also be misleading for cancelled producers returning done.
        if (cancelled) return
        if (result.done) {
          // Empty-payload frame so the client knows this index is done
          // without waiting for the global terminator.
          await emit(encodeFrame(encodeIndexedFrame(entry.index, EMPTY)))
          active.splice(active.indexOf(entry), 1)
        } else {
          await emit(encodeFrame(encodeIndexedFrame(entry.index, result.value as Uint8Array<ArrayBuffer>)))
          advance(entry)
          active.splice(active.indexOf(entry), 1)
          active.push(entry)
        }
      }
      // `emit` is already a no-op if `cancelled` — but we'd rather not allocate
      // the terminator frame at all when there's no one to receive it.
      if (cancelled) return
      await emit(encodeFrame(encodeU32(0)))
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

  return sink
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
