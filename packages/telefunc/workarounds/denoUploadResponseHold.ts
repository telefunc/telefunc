// Workaround: Deno, unless it runs with `--unstable-no-legacy-abort`, aborts `request.signal` at every response's end,
// under `Deno.serve` and a `node:http` framework that hands `serve({ request })` that signal alike, and that signal is
// then the only sign its client left (a raw Node-readable entry passes its socket's signal, and is never held). Without the hold, a call that answers before its
// upload is over takes its client for gone at the answer: its File reads fail and its unread upload is dropped
// (serve/deno.clientLeft.spec.ts, 12 legacy-mode rows). Remove once Deno's legacy abort is gone.
export { holdUploadResponse, heldResponseBody, unholdOnceOver }
export type { ResponseHold }

import { hasProp } from '../utils/hasProp.js'

/** Keeps the response open until `over`; `clientGone` aborts only for the client's leaving, and `unheld` lets go of the
 *  request's signal once the hold is over. */
type ResponseHold = { over: Promise<void>; clientGone: AbortSignal; unheld: () => void }

/** On Deno, where `clientGone` is the request's own signal: the hold that keeps the response open until the upload is
 *  read (`restRead`) or its client left. Elsewhere, no hold: `clientGone` itself stays valid after the response, as a
 *  Node adapter's socket signal does, under Deno's `node:http` too. */
function holdUploadResponse(request: Request, clientGone: AbortSignal, restRead: Promise<void>): ResponseHold | null {
  if (!hasProp(globalThis, 'Deno', 'object') || clientGone !== request.signal) return null
  const gone = new AbortController()
  const abort = () => gone.abort(clientGone.reason)
  if (clientGone.aborted) abort()
  else clientGone.addEventListener('abort', abort, { once: true })
  return {
    over: Promise.race([restRead, aborted(gone.signal)]),
    clientGone: gone.signal,
    unheld: () => clientGone.removeEventListener('abort', abort),
  }
}

/** `body`, which ends only once the hold is over. */
function heldResponseBody(
  body: ReadableStream<Uint8Array<ArrayBuffer>>,
  hold: ResponseHold,
): ReadableStream<Uint8Array<ArrayBuffer>> {
  return body.pipeThrough(
    new TransformStream<Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>>({ flush: () => hold.over }),
  )
}

async function unholdOnceOver(hold: ResponseHold): Promise<void> {
  await hold.over
  hold.unheld()
}

/** Settles once `signal` aborts. */
function aborted(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  return new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }))
}
