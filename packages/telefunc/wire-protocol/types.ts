export type {
  // ===== Base types =====
  TypeContract,
  ReplacerType,
  StreamingReplacerType,
  ReviverType,
  StreamingProducer,
  // ===== Contexts =====
  ClientReviverContext,
  ServerReviverContext,
  ClientReplacerContext,
  ServerReplacerContext,
  StreamSource,
  StreamReadOptions,
  // ===== Supporting =====
  StreamingMetadata,
  StreamingValueServer,
  // ===== Concrete contracts =====
  AsyncGeneratorContract,
  ReadableStreamContract,
  ReadableStreamRequestContract,
  PromiseContract,
  FileRequestContract,
  BlobRequestContract,
  FileResponseContract,
  BlobResponseContract,
  FileDownloadResponseContract,
  BlobDownloadResponseContract,
  FileMetadata,
  BlobMetadata,
  FileResponseMetadata,
  BlobResponseMetadata,
  FileDownloadMetadata,
  BlobDownloadMetadata,
  ChannelContract,
  BroadcastContract,
  FunctionContract,
  FileDownload,
  BlobDownload,
  DownloadProgress,
}

import type { ServerChannel } from './server/channel.js'
import type { ServerBroadcast } from './server/server-broadcast.js'
import type { ClientChannel, ClientBroadcast } from './client/channel.js'
import type { AbortError } from '../shared/Abort.js'
import type { ShieldValidators } from '../node/server/shield.js'
import type { FileDownload, BlobDownload } from './client/response/DownloadClasses.js'

// ===== Base types =====

/** Contract tying replacer and reviver for one serializable type. */
type TypeContract<V = unknown, R = unknown, M extends Record<string, unknown> = Record<string, unknown>> = {
  value: V
  result: R
  metadata: M
}

/** Replacer: detect a value during serialization and replace it with prefix+metadata on the wire.
 *  `replace` is the primary verb of the Replacer API — it runs once per serialized value and may
 *  perform side effects (register channels, install listeners, wire lifecycle). The returned
 *  `metadata` is what crosses the wire; `close` / `abort` are lifecycle hooks tracked by the
 *  registry and invoked when the carrier message completes or is aborted. */
type ReplacerType<C extends TypeContract = TypeContract, Context = unknown> = {
  prefix: string
  detect(value: unknown): value is C['value']
  replace(
    value: C['value'],
    context: Context,
  ): { metadata: C['metadata']; close: () => Promise<void> | void; abort: (abortError: AbortError) => void }
}

/** Streaming replacer: replacer + producer factory for chunk-based streaming. */
type StreamingReplacerType<C extends TypeContract = TypeContract, Context = unknown> = ReplacerType<C, Context> & {
  createProducer(value: C['value']): StreamingProducer
}

/** Reviver: reconstruct a live value from prefix+metadata during deserialization.
 *  `revive` is the primary verb of the Reviver API — it runs once per deserialized value and may
 *  perform side effects (create channels, start readers, attach validators). Mirrors `replace`
 *  on the Replacer side. */
type ReviverType<C extends TypeContract = TypeContract, Context = unknown> = {
  prefix: string
  revive(
    metadata: C['metadata'],
    context: Context,
  ): { value: C['result']; close: () => Promise<void> | void; abort: (abortError: AbortError) => void }
}

// ===== Producer =====

type StreamingProducer = {
  chunks: AsyncIterator<Uint8Array<ArrayBuffer>>
  cancel: (reason?: unknown) => void
}

type StreamingValueServer = {
  createProducer: () => StreamingProducer
  index: number
}

// ===== Contexts =====

type StreamCancelBehavior = 'error' | 'close'

type StreamReadOptions = {
  expectedSize?: number
  onChunk?: (chunkSize: number) => void
  /** How source-side `cancel()` should surface to the current consumer.
   *  - `'error'` (default): interrupted delivery rejects / errors instead of looking like EOF.
   *  - `'close'`: treat source cancellation as graceful end-of-stream / partial-buffer completion. */
  cancelBehavior?: StreamCancelBehavior
}

type StreamSource = {
  readNextChunk: () => Promise<Uint8Array<ArrayBuffer> | null>
  /** Buffers all chunks into a single Uint8Array. By default throws on cancel
   *  mid-stream or on `expectedSize` mismatch. */
  bytes: (opts?: StreamReadOptions) => Promise<Uint8Array<ArrayBuffer>>
  /** WHATWG `ReadableStream` view over `readNextChunk`. `cancel()` on the stream
   *  propagates upstream. With `opts.onChunk`, fires per chunk; with `opts.expectedSize`,
   *  errors the stream on truncation. Source-side cancellation defaults to an error,
   *  but callers can opt into graceful close with `cancelBehavior: 'close'`. */
  stream: (opts?: StreamReadOptions) => ReadableStream<Uint8Array<ArrayBuffer>>
  cancel: () => void
  abort: (abortError: AbortError) => void
}

/** Context for all client-side response revivers (streaming + placeholder). */
type ClientReviverContext = {
  createChannel<ClientToServer = unknown, ServerToClient = unknown>(opts: {
    channelId: string
    ack?: boolean
  }): ClientChannel<ClientToServer, ServerToClient>
  createBroadcast<T = unknown>(opts: { channelId: string; key: string }): ClientBroadcast<T>
  receiveStream(metadata: StreamingMetadata): StreamSource
  /** Awaited before the call settles — for revivers that need to buffer before the user reads. */
  waitFor(promise: Promise<unknown>): void
}

/** Context for all server-side request revivers (File/Blob + Function + ReadableStream). */
type ServerReviverContext = {
  registerFile(index: number, size: number): void
  consumeFile(index: number, size: number): ReadableStream<Uint8Array<ArrayBuffer>>
  createChannel<ClientToServer = unknown, ServerToClient = unknown>(opts: {
    id: string
    ack?: boolean
  }): ServerChannel<ClientToServer, ServerToClient>
  receiveStream(metadata: { channelId: string }): StreamSource
  /** Shield validators for the value being revived, keyed by the name declared in `[TELEFUNC_SHIELDS]`.
   *  Populated per-value based on the telefunction's argument shield metadata. Revivers pick the names
   *  relevant to their data flow and call them inline at the point where client data enters. */
  validators: ShieldValidators
}

/** Context for all server-side response replacers (streaming + placeholder). */
type ServerReplacerContext = {
  createChannel<ClientToServer = unknown, ServerToClient = unknown>(opts?: {
    ack?: boolean
  }): ServerChannel<ClientToServer, ServerToClient>
  /** Registers a channel with the response lifecycle. Also installs shield validators if the channel has shields. */
  registerChannel(channel: ServerChannel<any, any>): void
  sendStream(createProducer: () => StreamingProducer): {
    metadata: StreamingMetadata
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }
  /** Shield validators for the value being serialized, keyed by the name declared in `[TELEFUNC_SHIELDS]`.
   *  Replacers pick the names relevant to their data flow. Each returns `true` on success or an error
   *  string — call sites decide the action (throw, drop, ...). */
  validators: ShieldValidators
}

/** Context for all client-side request replacers (File/Blob + Function + ReadableStream). */
type ClientReplacerContext = {
  registerFile(body: Blob): number
  createChannel<ClientToServer = unknown, ServerToClient = unknown>(opts?: {
    ack?: boolean
  }): ClientChannel<ClientToServer, ServerToClient>
  sendStream(createProducer: () => StreamingProducer): {
    metadata: { channelId: string }
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }
}

// ===== Metadata =====

type StreamingMetadata = { channelId: string } | { __index: number }

/** Request-direction (client→server): bytes are framed back-to-back in the request body,
 *  so the reader needs an explicit `index` and `size` to know where this file starts and ends. */
type FileMetadata = { index: number; name: string; size: number; type: string; lastModified: number }
type BlobMetadata = { index: number; size: number; type: string }

type FileResponseMetadata = StreamingMetadata & {
  name: string
  type: string
  lastModified: number
  size?: number
}
type BlobResponseMetadata = StreamingMetadata & {
  type: string
  size?: number
}

type FileDownloadMetadata = FileResponseMetadata
type BlobDownloadMetadata = BlobResponseMetadata

// ===== Concrete contracts =====

type AsyncGeneratorContract = TypeContract<AsyncGenerator<unknown>, AsyncGenerator<unknown>, StreamingMetadata>
type ReadableStreamContract = TypeContract<
  ReadableStream<Uint8Array<ArrayBuffer>>,
  ReadableStream<Uint8Array<ArrayBuffer>>,
  StreamingMetadata
>
type ReadableStreamRequestContract = TypeContract<
  ReadableStream<Uint8Array<ArrayBuffer>>,
  ReadableStream<Uint8Array<ArrayBuffer>>,
  { channelId: string }
>
type PromiseContract = TypeContract<Promise<unknown>, Promise<unknown>, StreamingMetadata>
type FileRequestContract = TypeContract<File, File, FileMetadata>
type BlobRequestContract = TypeContract<Blob, Blob, BlobMetadata>
type FileResponseContract = TypeContract<File, Promise<File>, FileResponseMetadata>
type BlobResponseContract = TypeContract<Blob, Promise<Blob>, BlobResponseMetadata>

type FileDownloadResponseContract = TypeContract<FileDownload, FileDownload, FileDownloadMetadata>
type BlobDownloadResponseContract = TypeContract<BlobDownload, BlobDownload, BlobDownloadMetadata>

type DownloadProgress = (loaded: number, total: number | undefined) => void

type ChannelContract = TypeContract<ServerChannel, ClientChannel, { channelId: string; ack?: true }>

type BroadcastContract = TypeContract<ServerBroadcast, ClientBroadcast, { channelId: string; key: string }>

type FunctionContract = TypeContract<
  (...args: readonly unknown[]) => unknown,
  (...args: readonly unknown[]) => Promise<unknown>,
  { channelId: string }
>
