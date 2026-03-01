export { encodeBinaryRequest }

import { encodeU32 } from '../../frame.js'

/** Assemble the binary request body: [u32 metadata length][metadata UTF-8][file0 bytes][file1 bytes]... */
function encodeBinaryRequest(metadataSerialized: string, files: { body: Blob }[]): Blob {
  const metadataBytes = new TextEncoder().encode(metadataSerialized)
  const lengthPrefix = encodeU32(metadataBytes.length)
  return new Blob([lengthPrefix, metadataBytes, ...files.map((f) => f.body)])
}
