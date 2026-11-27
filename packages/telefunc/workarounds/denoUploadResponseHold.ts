// Workaround: two `Deno.serve` behaviors lose the upload of a call that answers before its upload is over. The hold
// goes to each binary request on Deno whose `clientGone` is its own `request.signal`, a `node:http` framework's
// `Request` too, whose response then ends only once its upload is over, as a browser's answer does anyway; a raw
// `node:http` `serve({ readable })` passes its socket's signal, and is never held.
// - In both signal modes, a read of the body pending as a streamed response ends never settles, nor does any later
//   read: a held streamed response ends only once the upload is over.
// - Unless Deno runs with `--unstable-no-legacy-abort`, `request.signal`, the only sign its client left, also aborts at
//   the response's end: once the upload is over, or a string answer went out, its aborts stop counting, and a failed
//   read of the body says the client left.
// TODO: remove the signal's unhook once https://github.com/denoland/deno/pull/35733 is released and Telefunc drops
// legacy abort mode, and the held body once a fix of https://github.com/denoland/deno/issues/36966 is released.
export { holdUploadResponse, heldResponseBody, unholdOnceOver }
export type { ResponseHold }

import { hasProp } from '../utils/hasProp.js'

/** Keeps the response open until `over`; `clientGone` aborts only for the client's leaving, and `unheld` lets go of the
 *  request's signal once the hold is over. */
type ResponseHold = { over: Promise<void>; clientGone: AbortSignal; unheld: () => void }

/** On Deno, where `clientGone` is the request's own signal: the hold that keeps the response open until the upload's
 *  read-off settles (`restRead`) or its client left. Elsewhere, no hold: `clientGone` itself stays valid after the
 *  response, as a Node adapter's socket signal does, under Deno's `node:http` too. */
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
