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
  FunctionContract,
}

import type { ServerChannel } from './server/channel.js'
import type { ClientChannel } from './client/channel.js'
import type { ChannelTransports } from './constants.js'

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
  getMetadata(value: C['value'], context: Context): C['metadata']
}

/** Streaming replacer: replacer + producer factory for chunk-based streaming. */
type StreamingReplacerType<C extends TypeContract = TypeContract, Context = unknown> = ReplacerType<C, Context> & {
  createProducer(value: C['value']): StreamingProducer
}

/** Reviver: reconstruct a live value from prefix+metadata during deserialization. */
type ReviverType<C extends TypeContract = TypeContract, Context = unknown> = {
  prefix: string
  createValue(metadata: C['metadata'], context: Context): { value: C['result']; close?: () => void }
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

type ChunkReader = { readNextChunk: () => Promise<Uint8Array<ArrayBuffer> | null>; cancel: () => void }

/** Context for all client-side response revivers (streaming + placeholder). */
type ClientReviverContext = {
  channelTransports: ChannelTransports
  sessionToken?: string
  registerChannel(channel: ClientChannel): void
  createInlineChunkReader(index: number): ChunkReader
  createChannelChunkReader(channelId: string): ChunkReader
}

/** Context for all server-side request revivers (File/Blob + Function + ReadableStream). */
type ServerReviverContext = {
  registerFile(index: number, size: number): void
  consumeFile(index: number, size: number): Promise<ReadableStream<Uint8Array>>
  registerChannel(channel: { close(): void }): void
}

type ResponseAbortableChannel = {
  _setResponseAbort(abortResponse: (abortValue?: unknown) => void): void
  abort(abortValue?: unknown): void
}

/** Context for all server-side response replacers (streaming + placeholder). */
type ServerReplacerContext = {
  registerChannel(channel: ResponseAbortableChannel): void
  registerStreamingValue(createProducer: () => StreamingProducer): number
  pumpToChannel(createProducer: () => StreamingProducer): string
  useChannelPump: boolean
}

/** Context for all client-side request replacers (File/Blob + Function + ReadableStream). */
type ClientReplacerContext = {
  channelTransports: ChannelTransports
  registerFile(body: Blob): number
  registerChannel(channel: ClientChannel<any, any>): void
  pumpToChannel(createProducer: () => StreamingProducer): { channelId: string; close: () => void }
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

type ChannelContract = TypeContract<ServerChannel, ClientChannel, { channelId: string; ack?: true; key?: string }>

type FunctionContract = TypeContract<
  (...args: readonly unknown[]) => unknown,
  (...args: readonly unknown[]) => Promise<unknown>,
  { channelId: string }
>
