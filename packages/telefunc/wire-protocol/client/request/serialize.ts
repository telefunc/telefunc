export { encodeBinaryRequest }

import { encodeRequestEnvelope } from '../../frame.js'

/** Assemble the binary request body: [u32 metadata length][metadata UTF-8][file0 bytes][file1 bytes]... */
function encodeBinaryRequest(metadataSerialized: string, files: { body: Blob }[]): Blob {
  return encodeRequestEnvelope(
    metadataSerialized,
    files.map((file) => file.body),
  )
}
