export { readChunksToBytes }

// `isCancelled()` distinguishes consumer-cancel from natural EOF — both surface as `null`
// from `readNextChunk()` per WHATWG Streams. Without this, a cancelled stream silently
// produces a partial buffer.
async function readChunksToBytes(
  readNextChunk: () => Promise<Uint8Array<ArrayBuffer> | null>,
  isCancelled: () => boolean,
  expectedSize: number | undefined,
  onChunk?: (chunkSize: number) => void,
): Promise<Uint8Array<ArrayBuffer>> {
  const chunks: Uint8Array<ArrayBuffer>[] = []
  let total = 0
  while (true) {
    const chunk = await readNextChunk()
    if (chunk === null) break
    chunks.push(chunk)
    total += chunk.byteLength
    onChunk?.(chunk.byteLength)
  }
  if (isCancelled()) {
    throw new Error('Stream cancelled before all bytes were received')
  }
  if (expectedSize !== undefined && expectedSize >= 0 && total !== expectedSize) {
    throw new Error(`Stream truncated — received ${total} of ${expectedSize} expected bytes`)
  }
  const out = new Uint8Array(total)
  let offset = 0
  for (const c of chunks) {
    out.set(c, offset)
    offset += c.byteLength
  }
  return out
}
