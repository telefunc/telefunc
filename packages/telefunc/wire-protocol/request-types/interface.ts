export type {
  RequestTypeContract,
  FileRequestContract,
  BlobRequestContract,
  FileMetadata,
  BlobMetadata,
  ClientRequestType,
  ServerRequestType,
  RequestBodyReader,
}

type FileMetadata = { index: number; name: string; size: number; type: string; lastModified: number }
type BlobMetadata = { index: number; size: number; type: string }

/**
 * Abstract reader for the binary request body.
 *
 * StreamReader (server-only) satisfies this structurally, but this type
 * lives in shared/ so it must not import server-only code.
 */
type RequestBodyReader = {
  registerFile(index: number, size: number): void
  consumeFile(index: number, size: number): Promise<ReadableStream<Uint8Array>>
}

/** Shared contract tying client and server plugins for one request type. */
type RequestTypeContract<V = unknown, R = unknown, M extends Record<string, unknown> = Record<string, unknown>> = { value: V; result: R; metadata: M }

type FileRequestContract = RequestTypeContract<File, File, FileMetadata>
type BlobRequestContract = RequestTypeContract<Blob, Blob, BlobMetadata>

/** Client-side plugin: detect a value, extract metadata + blob body for the binary request frame. */
type ClientRequestType<C extends RequestTypeContract = RequestTypeContract> = {
  prefix: string
  detect(value: unknown): value is C['value']
  getMetadata(value: C['value'], index: number): C['metadata']
  getBody(value: C['value']): Blob
}

/** Server-side plugin: reconstruct a live value from metadata + body reader. */
type ServerRequestType<C extends RequestTypeContract = RequestTypeContract> = {
  prefix: string
  createValue(metadata: C['metadata'], reader: RequestBodyReader): C['result']
}
