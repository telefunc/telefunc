export { nodeReadableToWebRequest, normalizeHeaders }

import type { EventEmitter } from 'node:events'
import type { Readable, Writable } from 'node:stream'
import { assertIsNotBrowser } from './assertIsNotBrowser.js'
assertIsNotBrowser()

type HeadersInput = Record<string, string | string[] | undefined> | [string, string][] | Headers

function nodeReadableToWebRequest(
  readable: Readable,
  url: string,
  method: string,
  headers: HeadersInput,
  response?: Writable,
): { request: Request; unwatch: () => void; clientGone: AbortSignal } {
  // `new Request()` refuses a body for these methods.
  const hasBody = !['GET', 'HEAD'].includes(method.toUpperCase())
  const body = hasBody ? webBodyOf(readable) : null

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
  // The client left: its request was cut short, or once answered, its socket closed before the body's end.
  const clientGone = AbortSignal.any([abortController.signal, socketClosesBeforeEnd(readable, socket)])
  return { request, unwatch, clientGone }
}

/** Aborts once `socket` closes before `readable`'s end: the client left mid-upload. Once the call is answered, Node no
 *  longer ends the request then, and `serve()` no longer has the request's signal watch the socket (`unwatch`). Watched
 *  until the body's end, as keep-alive reuses the socket. */
function socketClosesBeforeEnd(readable: Readable, socket: EventEmitter | null | undefined): AbortSignal {
  const gone = new AbortController()
  if (!socket || readable.readableEnded) return gone.signal
  const left = () => gone.abort()
  socket.once('close', left)
  readable.once('end', () => socket.off('close', left))
  return gone.signal
}

/** `readable` as a Web body that reads it only once read itself. A binary call's or an SSE upload's body is read off
 *  `readable` by Telefunc's own reader: `Readable.toWeb()` would copy all of it into a queue nothing reads. */
function webBodyOf(readable: Readable): ReadableStream<Uint8Array> {
  // Its generator starts at the first `next()`: until then nothing listens to `readable`.
  const chunks = readable[Symbol.asyncIterator]()
  return new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        const { value, done } = await chunks.next()
        if (done) controller.close()
        else controller.enqueue(value)
      },
      async cancel() {
        await chunks.return?.()
      },
    },
    { highWaterMark: 0 },
  )
}

function normalizeHeaders(headers: HeadersInput): [string, string][] {
  // HTTP/2 puts `:method`, `:path`, `:authority`, `:scheme`, `:status` into `req.headers`.
  // The Web Headers constructor rejects any name starting with `:`, so drop them — the
  // request's method and URL are passed via separate params already.
  if (Array.isArray(headers)) return headers.filter(([k]) => k.charCodeAt(0) !== 58 /* ':' */)
  // A `Headers` object (e.g. `request.headers`) has no own properties for `Object.entries()` to list, so iterate it
  // instead — checking `Symbol.iterator` rather than `instanceof Headers` also covers `Headers` polyfills.
  if (Symbol.iterator in headers) return Array.from(headers)
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
