export { clientLeft }

import type { Readable } from 'node:stream'
import { hasProp } from '../../../utils/hasProp.js'

/** Whether a failed read of a request's body is its client leaving, on what the runtime says of its transport:
 *  - Node, through the request's socket: the peer ended its side (the socket's `'end'`) or reading the socket failed (a
 *    reset). An app's own `req.destroy(err)` destroys the socket with that same `err`, and isn't taken for it.
 *  - Bun: it aborts the request's `signal` when its client disconnects.
 *  - Deno: the read fails with its own `BadResource` or `Http` error, raised by the connection.
 *  Elsewhere the runtime says nothing Telefunc can read (on Node, a `Request`'s `signal` is the code's that built it;
 *  workerd gives an incoming request a `signal` only behind a flag), and the error is printed. */
function clientLeft(err: unknown, request: Request, readable: Readable | undefined): boolean {
  if (readable) return socketShowsPeerLeft(err, readable)
  return runtimeShowsClientLeft(err, request, globalThis)
}

/** `global` is `globalThis`, whose `Bun` and `Deno` aren't typed here. */
function runtimeShowsClientLeft(err: unknown, request: Request, global: object): boolean {
  if (hasProp(global, 'Bun')) return request.signal.aborted
  if (hasProp(global, 'Deno', 'object')) return isDenoConnectionError(err, global.Deno.errors)
  return false
}

function socketShowsPeerLeft(err: unknown, readable: Readable): boolean {
  const socket = hasProp(readable, 'socket') ? readable.socket : null
  if (!hasProp(socket, 'errored') || !hasProp(socket, 'readableEnded', 'boolean')) return false
  if (socket.errored === err) return false
  return socket.readableEnded || (hasProp(socket.errored, 'syscall', 'string') && socket.errored.syscall === 'read')
}

function isDenoConnectionError(err: unknown, errors: unknown): boolean {
  if (!hasProp(errors, 'BadResource', 'function') || !hasProp(errors, 'Http', 'function')) return false
  return err instanceof errors.BadResource || err instanceof errors.Http
}
