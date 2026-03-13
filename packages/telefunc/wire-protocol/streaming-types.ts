export type {
  StreamingTypeContract,
  ClientRevivedValue,
  ClientReviveClose,
  AsyncGeneratorContract,
  ReadableStreamContract,
  PromiseContract,
  ServerStreamingType,
  ClientStreamingType,
  StreamingValueServer,
  StreamingProducer,
}

/** Plugin metadata — empty for now, extensible in the future. */
type StreamingMetadata = Record<string, never>

/** Shared contract tying server and client plugins for one streaming type. */
type StreamingTypeContract<V = unknown, R = unknown, M extends Record<string, unknown> = Record<string, unknown>> = {
  value: V
  result: R
  metadata: M
}

/** Collected during serialization: one entry per detected streaming value. */
type StreamingValueServer = {
  createProducer: () => StreamingProducer
  /** Index assigned during serialization (for multiplexed frames). */
  index: number
}

/** A running producer: the chunk iterable + a cancel function for immediate cleanup.
 *
 *  The cancel function is critical for ReadableStream: gen.return() alone can't
 *  interrupt a suspended reader.read() — reader.cancel() resolves it immediately. */
type StreamingProducer = {
  chunks: AsyncIterator<Uint8Array>
  /** Immediately cancel the underlying resource (e.g., reader.cancel()).
   *  Called by the framework on abort/disconnect. */
  cancel: () => void
}

/** Server-side plugin: how to detect, serialize metadata, and encode chunks for a streaming type. */
type ServerStreamingType<C extends StreamingTypeContract = StreamingTypeContract> = {
  prefix: string
  detect(value: unknown): value is C['value']
  /** Return plugin-specific metadata */
  getMetadata(value: C['value']): C['metadata']
  /** Create a producer for the given value. Returns an async iterable of chunk payloads
   *  and a cancel function for immediate cleanup.
   *
   *  - AsyncGenerator: each payload is a serialized JSON value.
   *  - ReadableStream: each payload is raw bytes.
   *  - Promise: single payload = serialized resolved value. */
  createProducer(value: C['value']): StreamingProducer
}

/** Client-side plugin: how to reconstruct a live value from metadata + chunk reader. */
type ClientReviveClose = (() => void) | undefined

type ClientRevivedValue<T> = {
  value: T
  close: ClientReviveClose
}

type ClientStreamingType<C extends StreamingTypeContract = StreamingTypeContract> = {
  prefix: string
  createValue(
    metadata: C['metadata'],
    readNextChunk: () => Promise<Uint8Array | null>,
    cancel: () => void,
  ): ClientRevivedValue<C['result']>
}

// ===== Concrete contracts =====

type AsyncGeneratorContract = StreamingTypeContract<AsyncGenerator<unknown>, AsyncGenerator<unknown>, StreamingMetadata>
type ReadableStreamContract = StreamingTypeContract<
  ReadableStream<Uint8Array>,
  ReadableStream<Uint8Array>,
  StreamingMetadata
>
type PromiseContract = StreamingTypeContract<Promise<unknown>, Promise<unknown>, StreamingMetadata>
