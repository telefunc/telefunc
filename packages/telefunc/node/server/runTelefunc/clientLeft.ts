export { noTransportEvidence, socketShowsClientLeft }
export type { ClientLeft }

import type { Readable } from 'node:stream'
import { hasProp } from '../../../utils/hasProp.js'

/** Whether a failed read of a request's body is its client leaving, or null where nothing says. Only the server that
 *  received the request can say: the adapter that was handed it passes its runtime's evidence (`serve/node.ts`,
 *  `serve/bun.ts`, `serve/deno.ts`), and with it `clientGone`, a signal that aborts once its client left and only then
 *  (on Deno, `request.signal` while the response is open: Deno also aborts it at every response's end). */
type ClientLeft = (err: unknown) => boolean | null

/** A `Request` handed over without its transport, as an app or a framework built it: nothing Telefunc can read says who
 *  failed the read (its `signal` is the code's that built it). A text call's failed read is printed; a binary call's
 *  body and an SSE upload end as a truncation, with the error as its `cause`. */
const noTransportEvidence: ClientLeft = () => null

/** Node, through the request's socket: the peer ended its side (the socket's `'end'`), reading the socket failed (a
 *  reset), or the socket was closed with no error, as the server's `closeAllConnections()` does at a shutdown: Node then
 *  ends the request under the read with its own `aborted`, and no app's error is in it. An app's own `req.destroy(err)`
 *  destroys the socket with that same `err`, and isn't taken for it. Server code that destroys the client's socket with
 *  another socket's read error (`req.socket.destroy(upstreamError)`) is taken for a reset: Node keeps only the error the
 *  socket was destroyed with, and both are a read error of a socket. */
function socketShowsClientLeft(readable: Readable): ClientLeft {
  return (err) => {
    const socket = hasProp(readable, 'socket') ? readable.socket : null
    if (!hasProp(socket, 'errored') || !hasProp(socket, 'readableEnded', 'boolean')) return false
    if (socket.errored === err) return false
    if (socket.readableEnded) return true
    if (socket.errored === null) return hasProp(socket, 'destroyed', 'boolean') && socket.destroyed
    return hasProp(socket.errored, 'syscall', 'string') && socket.errored.syscall === 'read'
  }
}
