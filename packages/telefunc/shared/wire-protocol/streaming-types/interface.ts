export type { ServerStreamingType, ClientStreamingType, StreamingValueServer, StreamingProducer }

/** Collected during serialization: one entry per detected streaming value. */
type StreamingValueServer = {
  type: ServerStreamingType
  value: unknown
  /** Index assigned during serialization (for multiplexed tagged frames). */
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
type ServerStreamingType = {
  prefix: string
  detect: (value: unknown) => boolean
  getMetadata: (value: unknown, index: number) => unknown
  /** Create a producer for the given value. Returns an async iterable of chunk payloads
   *  and a cancel function for immediate cleanup.
   *
   *  - AsyncGenerator: each payload is a serialized JSON value.
   *  - ReadableStream: each payload is raw bytes.
   *  - Promise: single payload = serialized resolved value. */
  createProducer: (value: unknown) => StreamingProducer
}

/** Client-side plugin: how to reconstruct a live value from metadata + chunk reader. */
type ClientStreamingType = {
  prefix: string
  createValue: (metadata: unknown, readNextChunk: () => Promise<Uint8Array | null>, cancel: () => void) => unknown
}
