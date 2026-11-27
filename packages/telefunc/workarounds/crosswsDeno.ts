// Workarounds of crossws's Deno adapter; remove once Telefunc's own WebSocket adapter replaces crossws.
// - Its peer's `terminate()` calls the `terminate()` Deno's `WebSocket` lacks, which throws, and the socket stays open
//   (https://github.com/h3js/crossws/pull/216, closed unmerged).
// - It reports a close without its code, which tells a page that left from a dropped one: a page that left would be
//   held for its reconnect (https://github.com/h3js/crossws/pull/213, closed unmerged).
export { terminateDenoPeer, onDenoPeerClose }

import type { Peer } from 'crossws'
import { assert } from '../utils/assert.js'

/** Ends `peer` at once: Deno's `WebSocket` has no `terminate()`, so it is closed. */
function terminateDenoPeer(peer: Peer): void {
  peer.close()
}

/** Calls `close` at `peer`'s close, with its code and reason. */
function onDenoPeerClose(peer: Peer, close: (peer: Peer, details: { code: number; reason: string }) => void): void {
  assert(peer.websocket.addEventListener)
  peer.websocket.addEventListener('close', (event) => close(peer, { code: event.code, reason: event.reason }))
}
