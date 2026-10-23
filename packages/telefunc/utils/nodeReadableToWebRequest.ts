export { nodeReadableToWebRequest, normalizeHeaders }

import type { EventEmitter } from 'node:events'
import type { Readable, Writable } from 'node:stream'
import { loadStreamNodeModule } from './loadStreamNodeModule.js'
import { assertIsNotBrowser } from './assertIsNotBrowser.js'
assertIsNotBrowser()

type HeadersInput = Record<string, string | string[] | undefined> | [string, string][]

async function nodeReadableToWebRequest(
  readable: Readable,
  url: string,
  method: string,
  headers: HeadersInput,
  response?: Writable,
): Promise<{ request: Request; unwatch: () => void }> {
  const { Readable: ReadableClass } = await loadStreamNodeModule()
  // `new Request()` refuses a body for these methods.
  const hasBody = !['GET', 'HEAD'].includes(method.toUpperCase())
  const body = hasBody ? (ReadableClass.toWeb(readable) as ReadableStream<Uint8Array>) : null

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
  const request = new Request(url, {
    method,
    headers: headerPairs,
    body,
    signal: abortController.signal,
    // @ts-expect-error duplex required for streaming request bodies
    duplex: 'half',
  })
  // A disconnect after the body was read shows on the response, else on `req.socket`, which HTTP/1 keep-alive reuses.
  let unwatch = () => {}
  const socket = (readable as { socket?: EventEmitter | null }).socket
  if (response) {
    response.once('close', () => {
      if (!response.writableEnded) abortController.abort()
    })
  } else if (socket) {
    const abort = () => abortController.abort()
    socket.once('close', abort)
    unwatch = () => socket.off('close', abort)
  }
  return { request, unwatch }
}

function normalizeHeaders(headers: HeadersInput): [string, string][] {
  // HTTP/2 puts `:method`, `:path`, `:authority`, `:scheme`, `:status` into `req.headers`.
  // The Web Headers constructor rejects any name starting with `:`, so drop them — the
  // request's method and URL are passed via separate params already.
  if (Array.isArray(headers)) return headers.filter(([k]) => k.charCodeAt(0) !== 58 /* ':' */)
  const pairs: [string, string][] = []
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue
    if (key.charCodeAt(0) === 58 /* ':' */) continue
    if (Array.isArray(value)) {
      for (const v of value) pairs.push([key, v])
    } else {
      pairs.push([key, value])
    }
  }
  return pairs
}
