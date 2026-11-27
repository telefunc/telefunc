// Workaround: Node 22's `WebSocket` (undici 6) fires `error` and no `close` where the connection fails before the
// upgrade (https://github.com/nodejs/undici/issues/3506). Without the `error` handler, a page on Node 22 waits forever on
// a WebSocket that never opened. Remove once Node 22 is no longer supported (`engines`).
export { onUnopenedEnd }

/** Calls `ended` at `ws`'s first `error` or `close`, until it opens. */
function onUnopenedEnd(ws: WebSocket, ended: () => void): void {
  ws.onclose = ws.onerror = ended
}
