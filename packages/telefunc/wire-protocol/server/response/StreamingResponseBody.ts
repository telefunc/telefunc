export { buildStreamingResponseBody, buildSSEResponseBody, generateResponseBody }

import { stringify } from '@brillout/json-serializer/stringify'
import { encodeU32, encodeIndexedFrame, textEncoder } from '../../frame.js'
import { STREAMING_ERROR_FRAME_MARKER, STREAMING_ERROR_TYPE } from '../../constants.js'
import type { StreamingErrorFrameAbort, StreamingErrorFrameBug } from '../../constants.js'
import type { StreamingValueServer, StreamingProducer } from '../../streaming-types.js'
import { isAbort } from '../../../node/server/Abort.js'
import type { AbortError } from '../../../shared/Abort.js'
import {
  handleTelefunctionBug,
  validateTelefunctionError,
} from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import type { ResponseAbortSource } from '../../../node/server/requestContext.js'
import type { TelefuncIdentifier } from '../../../node/server/runTelefunc/serializeTelefunctionResult.js'
import { uint8ArrayToBase64url } from '../../base64url.js'

const EMPTY = new Uint8Array(0)

/** Build a ReadableStream that frames metadata + multiplexed streaming values.
 *
 *  Preserves pull-based backpressure: one frame per pull() call.
 *  Cancellation: doCancel() stops all producers and closes the controller. */
function buildStreamingResponseBody(
  metadataSerialized: string,
  streamingValues: StreamingValueServer[],
  telefuncId: TelefuncIdentifier,
  onStreamComplete: () => void,
  abortSignal: AbortSignal,
  responseAbort: ResponseAbortSource,
): ReadableStream<Uint8Array<ArrayBuffer>> {
  return buildResponseBodyStream(
    metadataSerialized,
    streamingValues,
    telefuncId,
    onStreamComplete,
    abortSignal,
    responseAbort,
    (frame) => frame,
  )
}

/** Build a ReadableStream that frames metadata + multiplexed streaming values as SSE events.
 *
 *  Each binary frame is base64url-encoded and wrapped in `data: ...\n\n`.
 *  Same pull-based backpressure as the raw binary variant. */
function buildSSEResponseBody(
  metadataSerialized: string,
  streamingValues: StreamingValueServer[],
  telefuncId: TelefuncIdentifier,
  onStreamComplete: () => void,
  abortSignal: AbortSignal,
  responseAbort: ResponseAbortSource,
): ReadableStream<Uint8Array<ArrayBuffer>> {
  return buildResponseBodyStream(
    metadataSerialized,
    streamingValues,
    telefuncId,
    onStreamComplete,
    abortSignal,
    responseAbort,
    (frame) => textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`),
  )
}

/** Shared core: both `'binary-inline'` and `'sse-inline'` transports use this.
 *  The only difference is the `encodeFrame` closure — identity for raw binary,
 *  base64url SSE wrapping for `text/event-stream`. */
function buildResponseBodyStream(
  metadataSerialized: string,
  streamingValues: StreamingValueServer[],
  telefuncId: TelefuncIdentifier,
  onStreamComplete: () => void,
  abortSignal: AbortSignal,
  responseAbort: Pick<ResponseAbortSource, 'errorPromise'>,
  encodeFrame: (frame: Uint8Array<ArrayBuffer>) => Uint8Array<ArrayBuffer>,
): ReadableStream<Uint8Array<ArrayBuffer>> {
  let cancelled = false
  let streamController: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>> | null = null

  // Create all producers upfront so doCancel can call cancel() on each.
  // This is critical for ReadableStream: gen.return() alone can't interrupt
  // a suspended reader.read(); reader.cancel() resolves it immediately.
  const producers = streamingValues.map((sv) => ({ producer: sv.createProducer(), index: sv.index }))

  const gen = generateResponseBody(metadataSerialized, producers, telefuncId, { responseAbort })

  const doCancel = (controller?: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>> | null) => {
    if (cancelled) return
    cancelled = true
    // cancel() interrupts suspended reads (reader.read() / gen.next()) —
    // iter.return() alone can't do this since it waits for the pending await to settle.
    for (const { producer } of producers) producer.cancel()
    // Terminates the merge loop generator, whose finally block calls
    // iter.return() on active entries → triggers each producer's finally for cleanup.
    gen.return(undefined)
    if (controller)
      try {
        controller.close()
      } catch {}
  }

  abortSignal.addEventListener('abort', () => doCancel(streamController), { once: true })

  return new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(controller) {
      streamController = controller
    },
    async pull(controller) {
      try {
        const { done, value } = await gen.next()
        if (cancelled) return
        if (done) {
          onStreamComplete()
          controller.close()
        } else {
          controller.enqueue(encodeFrame(value))
        }
      } catch (err) {
        if (cancelled) return
        onStreamComplete()
        try {
          controller.error(err)
        } catch {}
      }
    },
    cancel() {
      doCancel()
    },
  })
}

// ===== Frame generation =====

/** Multiplexed frame generator: races all streaming value producers,
 *  yields indexed frames as they become ready.
 *
 *  Wire format per frame: [u32 frame_len][u8 index][payload]
 *  Terminator: [u32 0x00000000]
 *  Error: [u32 0xFFFFFFFF][u32 error_len][error_bytes] */
async function* generateResponseBody(
  metadataSerialized: string,
  producerEntries: Array<{ producer: StreamingProducer; index: number }>,
  telefuncId: TelefuncIdentifier,
  options?: { skipMetadata?: boolean; responseAbort?: Pick<ResponseAbortSource, 'errorPromise'> },
): AsyncGenerator<Uint8Array<ArrayBuffer>> {
  // Metadata header (skipped for WS transport — metadata is in the HTTP body)
  if (!options?.skipMetadata) {
    const metadataBytes = textEncoder.encode(metadataSerialized)
    yield encodeU32(metadataBytes.length)
    yield metadataBytes
  }

  type RaceEntry = {
    index: number
    iter: AsyncIterator<Uint8Array<ArrayBuffer>>
    pending: Promise<{ entry: RaceEntry; result: IteratorResult<Uint8Array<ArrayBuffer>> }>
  }

  const advance = (entry: RaceEntry) => {
    entry.pending = entry.iter.next().then((result) => ({ entry, result }))
  }

  // Each producer gets one pending .next() call — the minimum needed for
  // multiplexing. Without this, Promise.race can't know which producer
  // has data ready.
  const active: RaceEntry[] = []
  for (const { producer, index } of producerEntries) {
    const entry: RaceEntry = { index, iter: producer.chunks, pending: null! }
    advance(entry)
    active.push(entry)
  }

  try {
    // Merge loop: yield one frame per pull().
    // Response abort is checked with priority over producer readiness so a
    // response-wide abort preempts any further unresolved work immediately.
    // After a producer wins the race, it's moved to the end of the array so
    // other already-resolved producers get priority next iteration.
    // Without this, a fast producer at index 0 would always win Promise.race
    // (which picks the first resolved promise in array order), starving others.
    while (active.length > 0) {
      const { entry, result } = await Promise.race(
        options?.responseAbort
          ? [options.responseAbort.errorPromise, ...active.map((e) => e.pending)]
          : active.map((e) => e.pending),
      )

      if (result.done) {
        // Send empty-payload frame to signal this index is done.
        // Without this, the client consumer would hang until the global terminator.
        yield encodeIndexedFrame(entry.index, EMPTY)
        active.splice(active.indexOf(entry), 1)
      } else {
        yield encodeIndexedFrame(entry.index, result.value as Uint8Array<ArrayBuffer>)
        advance(entry)
        // Move to end for fair round-robin scheduling
        active.splice(active.indexOf(entry), 1)
        active.push(entry)
      }
    }

    // Success: zero-length terminator
    yield encodeU32(0)
  } catch (err) {
    // Cancel all producers — iter.return() can't interrupt a suspended await,
    // only producer.cancel() can unblock pending reads.
    for (const { producer } of producerEntries) {
      producer.cancel()
    }
    // Error mid-stream: send error frame instead of terminator
    yield* encodeErrorFrame(err, telefuncId)
  }
}

// ===== Frame encoding helpers =====

/** Encode an error as an error frame: [ERROR_MARKER][u32 payload_len][payload_bytes] */
function encodeErrorFrame(err: unknown, telefuncId: TelefuncIdentifier): Uint8Array<ArrayBuffer>[] {
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
    } catch {
      // Abort value not serializable — fall back to bug
      handleTelefunctionBug(err)
      const payload: StreamingErrorFrameBug = { type: STREAMING_ERROR_TYPE.BUG }
      errorPayload = stringify(payload)
    }
  } else {
    handleTelefunctionBug(err)
    const payload: StreamingErrorFrameBug = { type: STREAMING_ERROR_TYPE.BUG }
    errorPayload = stringify(payload)
  }
  const errorBytes = textEncoder.encode(errorPayload)
  return [encodeU32(STREAMING_ERROR_FRAME_MARKER), encodeU32(errorBytes.byteLength), errorBytes]
}
