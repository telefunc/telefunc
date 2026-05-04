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
  FileMetadata,
  BlobMetadata,
  ChannelContract,
  BroadcastContract,
  FunctionContract,
}

import type { ServerChannel } from './server/channel.js'
import type { ServerBroadcast } from './server/server-broadcast.js'
import type { ClientChannel, ClientBroadcast } from './client/channel.js'
import type { AbortError } from '../shared/Abort.js'
import type { ShieldValidators } from '../node/server/shield.js'

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

type ChunkReader = {
  readNextChunk: () => Promise<Uint8Array<ArrayBuffer> | null>
  cancel: () => void
  abort: (abortError: AbortError) => void
}

/** Context for all client-side response revivers (streaming + placeholder). */
type ClientReviverContext = {
  createChannel<TOut = unknown, TIn = unknown>(opts: { channelId: string; ack?: boolean }): ClientChannel<TOut, TIn>
  createBroadcast<T = unknown>(opts: { channelId: string; key: string }): ClientBroadcast<T>
  receiveStreamReader(metadata: StreamingMetadata): ChunkReader
  receiveStream(metadata: StreamingMetadata): {
    stream: ReadableStream<Uint8Array<ArrayBuffer>>
    cancel: () => void
    abort: (abortError: AbortError) => void
  }
}

/** Context for all server-side request revivers (File/Blob + Function + ReadableStream). */
type ServerReviverContext = {
  registerFile(index: number, size: number): void
  consumeFile(index: number, size: number): Promise<ReadableStream<Uint8Array>>
  createChannel<TOut = unknown, TIn = unknown>(opts: { id: string; ack?: boolean }): ServerChannel<TOut, TIn>
  receiveStreamReader(metadata: { channelId: string }): ChunkReader
  receiveStream(metadata: { channelId: string }): {
    stream: ReadableStream<Uint8Array<ArrayBuffer>>
    cancel: () => void
    abort: (abortError: AbortError) => void
  }
  /** Shield validators for the value being revived, keyed by the name declared in __DEFINE_TELEFUNC_SHIELDS.
   *  Populated per-value based on the telefunction's argument shield metadata. Revivers pick the names
   *  relevant to their data flow and call them inline at the point where client data enters. */
  validators: ShieldValidators
}

/** Context for all server-side response replacers (streaming + placeholder). */
type ServerReplacerContext = {
  createChannel<TOut = unknown, TIn = unknown>(opts?: { ack?: boolean }): ServerChannel<TOut, TIn>
  /** Registers a channel with the response lifecycle. Also installs shield validators if the channel has shields. */
  registerChannel(channel: ServerChannel<any, any>): void
  sendStream(createProducer: () => StreamingProducer): {
    metadata: StreamingMetadata
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }
  /** Shield validators for the value being serialized, keyed by the name declared in __DEFINE_TELEFUNC_SHIELDS.
   *  Replacers pick the names relevant to their data flow. Each returns `true` on success or an error
   *  string — call sites decide the action (throw, drop, ...). */
  validators: ShieldValidators
}

/** Context for all client-side request replacers (File/Blob + Function + ReadableStream). */
type ClientReplacerContext = {
  registerFile(body: Blob): number
  createChannel<TOut = unknown, TIn = unknown>(opts?: { ack?: boolean }): ClientChannel<TOut, TIn>
  sendStream(createProducer: () => StreamingProducer): {
    metadata: { channelId: string }
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }
}

// ===== Metadata =====

type StreamingMetadata = { channelId: string } | { __index: number }
type FileMetadata = { index: number; name: string; size: number; type: string; lastModified: number }
type BlobMetadata = { index: number; size: number; type: string }

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

type ChannelContract = TypeContract<ServerChannel, ClientChannel, { channelId: string; ack?: true }>

type BroadcastContract = TypeContract<ServerBroadcast, ClientBroadcast, { channelId: string; key: string }>

type FunctionContract = TypeContract<
  (...args: readonly unknown[]) => unknown,
  (...args: readonly unknown[]) => Promise<unknown>,
  { channelId: string }
>
