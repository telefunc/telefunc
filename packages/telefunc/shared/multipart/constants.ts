export const SERIALIZER_PREFIX_FILE = '!TelefuncFile:'
export const SERIALIZER_PREFIX_BLOB = '!TelefuncBlob:'

export type FileMetadata = { index: number; name: string; size: number; type: string; lastModified: number }
export type BlobMetadata = { index: number; size: number; type: string }
