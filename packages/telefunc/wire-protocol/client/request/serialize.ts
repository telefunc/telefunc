export { encodeBinaryRequest }

/** Assemble the binary request body: [u32 metadata length][metadata UTF-8][file0 bytes][file1 bytes]... */
function encodeBinaryRequest(metadataSerialized: string, files: { body: Blob }[]): Blob {
  const metadataBytes = new TextEncoder().encode(metadataSerialized)
  const lengthPrefix = new Uint8Array(4)
  new DataView(lengthPrefix.buffer).setUint32(0, metadataBytes.length, false)
  return new Blob([lengthPrefix, metadataBytes, ...files.map((f) => f.body)])
}
