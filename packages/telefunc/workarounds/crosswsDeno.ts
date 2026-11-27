// Workarounds of crossws's Deno adapter; remove once Telefunc's own WebSocket adapter replaces crossws.
// - Its peer's `terminate()` calls the `terminate()` Deno's `WebSocket` lacks, which throws, and the socket stays open
//   (https://github.com/h3js/crossws/pull/216, closed unmerged).
// - It reports a close without its code, which tells a page that left from a dropped one: a page that left would be
//   held for its reconnect (https://github.com/h3js/crossws/pull/213, closed unmerged).
export { terminateDenoPeer, denoHooks }

import type { Hooks, Peer } from 'crossws'
import { assert } from '../utils/assert.js'

/** Ends `peer` with a close in place of `terminate()`, which Deno's `WebSocket` lacks: the socket enters `CLOSING`, and
 *  its `close` comes once the handshake ends. */
function terminateDenoPeer(peer: Peer): void {
  peer.close()
}

/** `hooks` for crossws's Deno adapter: a peer's close comes from Deno's own `WebSocket`, with its code and reason, and
 *  crossws's own close, without them, is dropped. */
function denoHooks(hooks: Pick<Hooks, 'upgrade' | 'open' | 'message' | 'close' | 'error'>): Partial<Hooks> {
  return {
    upgrade: hooks.upgrade,
    open(peer) {
      assert(peer.websocket.addEventListener)
      peer.websocket.addEventListener('close', (event) => hooks.close(peer, { code: event.code, reason: event.reason }))
      return hooks.open(peer)
    },
    message: hooks.message,
    close() {},
    error: hooks.error,
  }
}
