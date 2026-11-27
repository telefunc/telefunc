// Workaround: `Bun.serve` aborts `request.signal` once a client leaves, but only until the request is answered. A client
// that leaves after the answer leaves the signal live, and the body's read fails with a `DOMException` named
// `AbortError` ("The connection was closed."). Without reading that error as the client's leaving, a call that answers
// before its upload is over takes such a client's leaving for a read that failed. Remove once Bun aborts
// `request.signal` for a client that leaves after the answer.
export { bunClientLeft }

import type { ClientLeft } from '../node/server/runTelefunc/clientLeft.js'

// TODO: remove once https://github.com/oven-sh/bun/issues/49647 is released.
/** Whether `request`'s failed body read is its client leaving, before its answer or after it. */
function bunClientLeft(request: Request): ClientLeft {
  return (err) => request.signal.aborted || (err instanceof DOMException && err.name === 'AbortError')
}
