export { nodeReadableToWebRequest }

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
  // The readable closes as soon as its body is read: a later disconnect only shows on the response, or else on the
  // connection, watched until `unwatch()`. `req.socket`: an HTTP/1 connection, which keep-alive reuses, or an HTTP/2
  // request's stream.
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
  const request = new Request(url, {
    method,
    headers: headerPairs,
    body,
    signal: abortController.signal,
    // @ts-expect-error duplex required for streaming request bodies
    duplex: 'half',
  })
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
