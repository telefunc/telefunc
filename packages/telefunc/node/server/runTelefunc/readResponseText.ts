export { readResponseText }

import type { Readable as StreamReadableNode } from 'node:stream'

/** The rest of a streamed response body, decoded as UTF-8 as it arrives. A Web stream's reader is released however the
 *  read ends, so the stream can be read again after it. */
async function readResponseText(body: StreamReadableNode | ReadableStream<Uint8Array>): Promise<string> {
  const decoder = new TextDecoder()
  let text = ''
  if (!(body instanceof ReadableStream)) {
    for await (const chunk of body) text += decoder.decode(chunk, { stream: true })
    return text + decoder.decode()
  }
  const reader = body.getReader()
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) return text + decoder.decode()
      text += decoder.decode(value, { stream: true })
    }
  } finally {
    reader.releaseLock()
  }
}
