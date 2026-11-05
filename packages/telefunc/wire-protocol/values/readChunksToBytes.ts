export { readChunksToBytes }

import { getStreamCompletionError } from './getStreamCompletionError.js'
import type { StreamReadOptions } from './types.js'

// `isCancelled()` distinguishes consumer-cancel from natural EOF — both surface as `null`
// from `readNextChunk()` per WHATWG Streams. Without this, a cancelled stream silently
// produces a partial buffer.
async function readChunksToBytes(
  readNextChunk: () => Promise<Uint8Array<ArrayBuffer> | null>,
  isCancelled: () => boolean,
  readOptions?: StreamReadOptions,
): Promise<Uint8Array<ArrayBuffer>> {
  const chunks: Uint8Array<ArrayBuffer>[] = []
  let total = 0
  while (true) {
    const chunk = await readNextChunk()
    if (chunk === null) break
    chunks.push(chunk)
    total += chunk.byteLength
    readOptions?.onChunk?.(chunk.byteLength)
  }
  const completionError = getStreamCompletionError({
    wasCancelled: isCancelled(),
    received: total,
    readOptions,
  })
  if (completionError) {
    throw completionError
  }
  const out = new Uint8Array(total)
  let offset = 0
  for (const c of chunks) {
    out.set(c, offset)
    offset += c.byteLength
  }
  return out
}
