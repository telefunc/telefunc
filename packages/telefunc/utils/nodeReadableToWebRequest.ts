export { nodeReadableToWebRequest }

import type { Readable } from 'node:stream'
import { loadStreamNodeModule } from './loadStreamNodeModule.js'
import { assertIsNotBrowser } from './assertIsNotBrowser.js'
assertIsNotBrowser()

type HeadersInput = Record<string, string | string[] | undefined> | [string, string][]

async function nodeReadableToWebRequest(
  readable: Readable,
  url: string,
  method: string,
  headers: HeadersInput,
): Promise<Request> {
  const { Readable: ReadableClass } = await loadStreamNodeModule()
  const body = ReadableClass.toWeb(readable) as ReadableStream<Uint8Array>

  const headerPairs = normalizeHeaders(headers)

  // Wire the readable's close event to an AbortSignal so that
  // request.signal fires when the client disconnects.
  // `close` fires both after normal completion and on premature disconnect —
  // readableAborted is true when the stream was destroyed before emitting 'end'
  // (i.e. client disconnected).
  const abortController = new AbortController()
  readable.on('close', () => {
    if (readable.readableAborted && !abortController.signal.aborted) abortController.abort()
  })
  return new Request(url, {
    method,
    headers: headerPairs,
    body,
    signal: abortController.signal,
    // @ts-expect-error duplex required for streaming request bodies
    duplex: 'half',
  })
}

function normalizeHeaders(headers: HeadersInput): [string, string][] {
  if (Array.isArray(headers)) return headers
  const pairs: [string, string][] = []
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue
    if (Array.isArray(value)) {
      for (const v of value) pairs.push([key, v])
    } else {
      pairs.push([key, value])
    }
  }
  return pairs
}
