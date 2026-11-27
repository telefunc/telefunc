/// <reference types="@cloudflare/workers-types" />

// Workarounds of a Durable Object's WebSocket under crossws 0.4.6's Cloudflare adapter:
// - Before compatibility date 2026-04-07, workerd leaves a client's close unanswered until the Durable Object closes
//   the socket too: without the echo, the page's close handshake never completes. Remove once Telefunc requires a
//   compatibility date of 2026-04-07 or later.
// - crossws has no Durable Object handler for a socket's error: without the translation, the error reaches none of
//   Telefunc's hooks and doesn't end the socket's wire. Remove once Telefunc's own WebSocket adapter replaces crossws.
export { echoClientClose, closeOnDurableError }

import type crossws from 'crossws/adapters/cloudflare'

type DurableAdapter = ReturnType<typeof crossws>

/** Answers a client's close with the same close; `close()` refuses 1005 and 1006, so a close without a code is answered
 *  without one, and an unclean one not at all. */
function echoClientClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): void {
  if (code === 1005) ws.close()
  else if (wasClean) ws.close(code, reason)
}

/** Ends the wire of `ws`, whose socket erred, as crossws ends one at an abnormal close. */
function closeOnDurableError(
  adapter: DurableAdapter,
  obj: Parameters<DurableAdapter['handleDurableClose']>[0],
  ws: WebSocket,
): Promise<void> {
  return adapter.handleDurableClose(obj, ws, 1006, 'error', false)
}
