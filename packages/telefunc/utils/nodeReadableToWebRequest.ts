export { nodeReadableToWebRequest, normalizeHeaders }

import type { EventEmitter } from 'node:events'
import type { Readable, Writable } from 'node:stream'
import { getStreamNodeModule, loadStreamNodeModuleOnce } from './loadStreamNodeModule.js'
import { assertIsNotBrowser } from './assertIsNotBrowser.js'
assertIsNotBrowser()

type HeadersInput = Record<string, string | string[] | undefined> | [string, string][] | Headers

async function nodeReadableToWebRequest(
  readable: Readable,
  url: string,
  method: string,
  headers: Headers,
  nativeBody: boolean,
  response?: Writable,
): Promise<{ request: Request; unwatch: () => void; unwatchClientGone: () => void; clientGone: AbortSignal }> {
  // Even a closed readable can still emit its error on the next tick.
  const unwatchEarlyError = () => {
    readable.off('error', ignoreEarlyReadableError)
    readable.off('close', unwatchEarlyError)
    readable.off('newListener', onNewListener)
  }
  const onNewListener = (event: string | symbol, listener: (...args: unknown[]) => void) => {
    if (event === 'error' && listener !== ignoreEarlyReadableError) unwatchEarlyError()
  }
  readable.on('error', ignoreEarlyReadableError)
  readable.once('close', unwatchEarlyError)
  readable.on('newListener', onNewListener)
  // Wire the readable's close event to an AbortSignal so that
  // request.signal fires when the client disconnects.
  // `close` fires both after normal completion and on premature disconnect —
  // readableAborted is true when the stream was destroyed before emitting 'end'
  // (i.e. client disconnected).
  const abortController = new AbortController()
  const gone = new AbortController()
  // Register before Request so its abort listeners see that the client is gone.
  abortController.signal.addEventListener('abort', () => gone.abort(abortController.signal.reason), { once: true })
  readable.once('close', () => {
    if (readable.readableAborted) abortController.abort(readable.errored ?? undefined)
  })
  let body: ReadableStream<Uint8Array<ArrayBuffer>> | null = null
  if (!nativeBody && !['GET', 'HEAD'].includes(method.toUpperCase())) {
    await loadStreamNodeModuleOnce()
    const { Readable } = getStreamNodeModule()
    body = Readable.toWeb(readable, { strategy: { highWaterMark: 0 } }) as ReadableStream<Uint8Array<ArrayBuffer>>
  }
  const request = new Request(url, {
    method,
    headers,
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
  // Even after unwatch(), a socket that closes before the body's end interrupts its upload.
  let unwatchClientGone = () => {}
  if (socket && !readable.readableEnded) {
    const left = () => gone.abort()
    socket.once('close', left)
    unwatchClientGone = () => {
      socket.off('close', left)
      readable.off('end', unwatchClientGone)
    }
    readable.once('end', unwatchClientGone)
  }
  return { request, unwatch, unwatchClientGone, clientGone: gone.signal }
}

function ignoreEarlyReadableError(): void {}

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
