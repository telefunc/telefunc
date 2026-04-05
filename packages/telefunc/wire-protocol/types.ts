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
  PubSubContract,
  FunctionContract,
}

import type { ServerChannel } from './server/channel.js'
import type { ServerPubSub } from './server/server-pubsub.js'
import type { ClientChannel, ClientPubSub } from './client/channel.js'
import type { AbortError } from '../shared/Abort.js'

// ===== Base types =====

/** Contract tying replacer and reviver for one serializable type. */
type TypeContract<V = unknown, R = unknown, M extends Record<string, unknown> = Record<string, unknown>> = {
  value: V
  result: R
  metadata: M
}

/** Replacer: detect a value during serialization and replace with prefix+metadata. */
type ReplacerType<C extends TypeContract = TypeContract, Context = unknown> = {
  prefix: string
  detect(value: unknown): value is C['value']
  getMetadata(
    value: C['value'],
    context: Context,
  ): { metadata: C['metadata']; close: () => Promise<void> | void; abort: (abortError: AbortError) => void }
}

/** Streaming replacer: replacer + producer factory for chunk-based streaming. */
type StreamingReplacerType<C extends TypeContract = TypeContract, Context = unknown> = ReplacerType<C, Context> & {
  createProducer(value: C['value']): StreamingProducer
}

/** Reviver: reconstruct a live value from prefix+metadata during deserialization. */
type ReviverType<C extends TypeContract = TypeContract, Context = unknown> = {
  prefix: string
  createValue(
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
  createPubSub<T = unknown>(opts: { channelId: string; key: string }): ClientPubSub<T>
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
}

/** Context for all server-side response replacers (streaming + placeholder). */
type ServerReplacerContext = {
  createChannel<TOut = unknown, TIn = unknown>(opts?: { ack?: boolean }): ServerChannel<TOut, TIn>
  registerChannel(channel: ServerChannel<any, any>): void
  sendStream(createProducer: () => StreamingProducer): {
    metadata: StreamingMetadata
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }
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

type PubSubContract = TypeContract<ServerPubSub, ClientPubSub, { channelId: string; key: string }>

type FunctionContract = TypeContract<
  (...args: readonly unknown[]) => unknown,
  (...args: readonly unknown[]) => Promise<unknown>,
  { channelId: string }
>
