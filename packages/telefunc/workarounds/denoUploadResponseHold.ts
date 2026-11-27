// Workaround: two `Deno.serve` behaviors lose the upload of a call that answers before its upload is over. The hold
// goes to each binary request on Deno whose `clientGone` is its own `request.signal`, a `node:http` framework's
// `Request` too, whose response then ends only once its upload is over, as a browser's answer does anyway; a raw
// `node:http` `serve({ readable })` passes its socket's signal, and is never held.
// - In both signal modes, a streamed response that ends before the upload is over can stall the body's pending read
//   and every later one (the issue below): a held streamed response ends only once the upload is over.
// - Unless Deno runs with `--unstable-no-legacy-abort`, `request.signal`, the only sign its client left, also aborts at
//   the response's end: once the upload is over, or a string answer went out, its aborts stop counting, and a failed
//   read of the body says the client left.
// TODO: remove the signal's unhook once https://github.com/denoland/deno/pull/35733 is released and Telefunc drops
// legacy abort mode, and the held body once a fix of https://github.com/denoland/deno/issues/36966 is released.
export { holdUploadResponse }
export type { ResponseHold }

import { hasProp } from '../utils/hasProp.js'

/** The hold of a Deno upload's response, over once the read-off of the body settles or its client left. */
type ResponseHold = {
  /** Aborts only for the client's leaving. */
  clientGone: AbortSignal
  /** `body`, ending only once the upload is over. */
  heldBody: (body: ReadableStream<Uint8Array<ArrayBuffer>>) => ReadableStream<Uint8Array<ArrayBuffer>>
  /** The answer goes out as a string, which ends at once: the request's signal stops counting. */
  answeredAsString: () => void
}

/** On Deno, where `clientGone` is the request's own signal: the hold of its response until the upload's read-off
 *  settles (`restRead`) or its client left. Elsewhere, no hold: `clientGone` itself stays valid after the response, as a
 *  Node adapter's socket signal does, under Deno's `node:http` too. */
function holdUploadResponse(request: Request, clientGone: AbortSignal, restRead: Promise<void>): ResponseHold | null {
  if (!hasProp(globalThis, 'Deno', 'object') || clientGone !== request.signal) return null
  const gone = new AbortController()
  const abort = () => gone.abort(clientGone.reason)
  const unwatch = () => clientGone.removeEventListener('abort', abort)
  if (clientGone.aborted) abort()
  else clientGone.addEventListener('abort', abort, { once: true })
  const over = Promise.race([restRead, aborted(gone.signal)])
  // Started before any held body waits on `over`, so the signal stops counting before the response ends.
  void unwatchOnceOver(over, unwatch)
  return {
    clientGone: gone.signal,
    heldBody: (body) =>
      body.pipeThrough(new TransformStream<Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>>({ flush: () => over })),
    answeredAsString: unwatch,
  }
}

async function unwatchOnceOver(over: Promise<void>, unwatch: () => void): Promise<void> {
  await over
  unwatch()
}

/** Settles once `signal` aborts. */
function aborted(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  return new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }))
}
