// Workaround: `Deno.serve`, unless Deno runs with `--unstable-no-legacy-abort`, aborts `request.signal` at every
// response's end, and that signal is the only sign its client left. Without the hold, a call that answers before its
// upload is over takes its client for gone at the answer: its File reads fail and its unread upload is dropped
// (serve/deno.clientLeft.spec.ts, 12 legacy-mode rows). Remove once Deno's legacy abort is gone.
export { holdUploadResponse, heldResponseBody, unholdOnceOver }
export type { ResponseHold }

import { hasProp } from '../utils/hasProp.js'

/** Keeps the response open until `over`; `unheld` lets go of the client's signal once it is. */
type ResponseHold = { over: Promise<void>; unheld: () => void }

/** On `Deno.serve`, where `clientGone` is its request's signal: the hold that keeps the response open until the upload
 *  is read (`restRead`) or its client left, and the signal that aborts only for the client's leaving. Elsewhere, no hold
 *  and `clientGone` itself; a Node adapter's socket signal stays valid after the response, under Deno's `node:http` too. */
function holdUploadResponse(
  request: Request,
  clientGone: AbortSignal,
  restRead: Promise<void>,
): { hold: ResponseHold | null; clientGone: AbortSignal } {
  if (!hasProp(globalThis, 'Deno', 'object') || clientGone !== request.signal) return { hold: null, clientGone }
  const gone = new AbortController()
  const abort = () => gone.abort(clientGone.reason)
  if (clientGone.aborted) abort()
  else clientGone.addEventListener('abort', abort, { once: true })
  return {
    hold: {
      over: Promise.race([restRead, aborted(gone.signal)]),
      unheld: () => clientGone.removeEventListener('abort', abort),
    },
    clientGone: gone.signal,
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
