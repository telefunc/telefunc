export { buildInlineResponseBody, encodeErrorPayload }

import type { Readable } from 'node:stream'
import type { StreamingValueServer } from '../../types.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { encodeU32, encodeIndexedFrame, textEncoder } from '../../../wire/envelope.js'
import { STREAMING_ERROR_FRAME_MARKER, STREAMING_ERROR_TYPE } from '../../../constants.js'
import type { StreamingErrorFrameAbort, StreamingErrorFrameBug } from '../../../constants.js'
import { Abort } from '../../../../node/server/Abort.js'
import type { AbortError } from '../../../../shared/Abort.js'
import {
  handleTelefunctionBug,
  validateTelefunctionError,
} from '../../../../node/server/runTelefunc/validateTelefunctionError.js'
import type { ResponseAbortSource } from '../../../../node/server/context/requestContext.js'
import type { TelefuncId } from '../../../../shared/TelefuncId.js'
import { restoreContext } from '../../../../node/server/context/context.js'
import type { Context } from '../../../../node/server/context/context.js'
import { createPushReadable } from '../../../../utils/pushReadable.js'
import { serializeAbortReason, stringifyValue } from '../../../channel/channel.js'
import { createPushReadableStream } from '../../../../utils/pushReadableStream.js'
import { Fifo } from '../../../../utils/Fifo.js'
import { assert } from '../../../../utils/assert.js'

const EMPTY = new Uint8Array(0)

type Settled = { entry: StreamingValueServer; result: IteratorResult<Uint8Array<ArrayBuffer>> } | { err: unknown }

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
  producers: StreamingValueServer[]
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
    producers,
    telefuncId,
    context,
    abortSignal,
    responseAbort,
    onComplete,
    encodeFrame,
    useNodeStream,
  } = runContext

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
  // Both the consumer leaving and a failed merge reach here: each producer is cancelled once.
  let producersStopped = false
  const cancelProducers = () => {
    if (producersStopped) return
    producersStopped = true
    for (const { producer } of producers) producer.cancel()
    // Unblock any emit awaiting drain so `runInlineResponse` can progress to its catch/finally.
    wakeDrain()
  }
  const onConsumerGone = () => {
    if (cancelled) return
    cancelled = true
    wakeMerge()
    cancelProducers()
  }
  // Ends the merge after what settled before it, and stops the producers now: the page may not read the body again.
  const onResponseAborted = (err: unknown) => {
    settle({ err })
    cancelProducers()
  }

  const sink = useNodeStream
    ? createPushReadable(onConsumerGone, wakeDrain)
    : createPushReadableStream<Uint8Array<ArrayBuffer>>(onConsumerGone, wakeDrain)

  // Once the producers stopped, what's left (a frame each that settled, the error) goes without waiting for the page.
  const emit = async (frame: Uint8Array<ArrayBuffer>): Promise<void> => {
    if (cancelled) return
    if (!sink.push(frame) && !producersStopped) await new Promise<void>((resolve) => (drainResolve = resolve))
  }

  const settle = (item: Settled) => {
    settled.push(item)
    wakeMerge()
  }
  const pullNext = async (entry: StreamingValueServer) => {
    try {
      settle({ entry, result: await entry.producer.chunks.next() })
    } catch (err) {
      settle({ err })
    }
  }

  // Each active producer keeps one in-flight next(), and gets the next one once its frame went out.
  async function merge(open: number): Promise<void> {
    while (open > 0) {
      if (settled.length === 0 && !cancelled) await new Promise<void>((resolve) => (wakeMerge = resolve))
      if (cancelled) return
      const next = settled.shift()
      assert(next)
      if ('err' in next) throw next.err
      const { entry, result } = next
      if (result.done) {
        // An empty payload marks the end of this index.
        await emit(encodeFrame(encodeIndexedFrame(entry.index, EMPTY)))
        open--
        continue
      }
      if (result.value.byteLength > 0) {
        await emit(encodeFrame(encodeIndexedFrame(entry.index, result.value)))
      }
      if (!producersStopped) void pullNext(entry)
    }
  }

  function throwIfResponseAborted() {
    if (!producersStopped) return
    const next = settled.shift()
    assert(next && 'err' in next)
    throw next.err
  }

  async function mergeProducers() {
    // The client drains eager values before it can hand the lazy ones to the caller.
    const eager = producers.filter((entry) => entry.eager)
    const first = eager.length === 0 ? producers : eager
    for (const entry of first) void pullNext(entry)

    const metadataBytes = textEncoder.encode(metadataSerialized)
    await emit(encodeFrame(encodeU32(metadataBytes.length)))
    await emit(encodeFrame(metadataBytes))

    await merge(first.length)
    if (cancelled) return
    if (first.length < producers.length) {
      throwIfResponseAborted()
      for (const entry of producers) {
        if (!entry.eager) void pullNext(entry)
      }
      await merge(producers.length - first.length)
    }
    if (cancelled) return
    throwIfResponseAborted()
    await emit(encodeFrame(encodeU32(0)))
  }

  async function runInlineResponse() {
    abortSignal.addEventListener('abort', onConsumerGone, { once: true })
    responseAbort.errorPromise.catch(onResponseAborted)
    try {
      // With AsyncLocalStorage, this one restore covers the producers for the whole loop.
      await restoreContext(context, mergeProducers)
    } catch (err) {
      // Cancel producers immediately so peer producers stop emitting work
      // that would never reach the consumer while we emit the error frames.
      cancelProducers()
      // Propagate aborts to side-channels (request argument channels etc.)
      // regardless of cancellation state — listeners care even when the
      // consumer is already gone. A reason JSON can't hold goes to none of them: the error frame reports it.
      if (err instanceof Abort) responseAbort.abort(serializeAbortReason(err.abortValue).value)
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
  }

  void runInlineResponse()

  return sink.readable
}

/** Serialize a streaming error as its on-wire payload string. Used by both the
 *  inline path (with a u32 marker prefix) and the channel pump path (with a
 *  tag prefix) — only the framing differs. */
function encodeErrorPayload(err: unknown, telefuncId: TelefuncId): Uint8Array<ArrayBuffer> {
  try {
    validateTelefunctionError(err, telefuncId)
  } catch (usageErr) {
    // A non-object or a bare `Abort` thrown: the mistake is what's reported, and the page gets a bug.
    err = usageErr
  }
  let errorPayload: string
  if (err instanceof Abort) {
    const abortError: AbortError = err
    try {
      const payload: StreamingErrorFrameAbort = {
        type: STREAMING_ERROR_TYPE.ABORT,
        abortValue: abortError.abortValue,
      }
      // The rule for every abort reason, which refuses bytes JSON would turn into an object.
      stringifyValue(abortError.abortValue, 'abort')
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
