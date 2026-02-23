export const SERIALIZER_PREFIX_FILE = '!TelefuncFile:'
export const SERIALIZER_PREFIX_BLOB = '!TelefuncBlob:'
export const SERIALIZER_PREFIX_STREAM = '!TelefuncStream:'
export const SERIALIZER_PREFIX_GENERATOR = '!TelefuncGenerator:'

export type FileMetadata = { index: number; name: string; size: number; type: string; lastModified: number }
export type BlobMetadata = { index: number; size: number; type: string }
export type StreamMetadata = Record<string, never>
export type GeneratorMetadata = Record<string, never>
