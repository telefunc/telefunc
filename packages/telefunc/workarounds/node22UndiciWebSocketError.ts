// Workaround: Node 22's `WebSocket` (undici 6) fires `error` and no `close` where the connection fails before the
// upgrade, and stays `CONNECTING` (https://github.com/nodejs/undici/issues/3506). Without the `error` handler, a page on
// Node 22 hears of the failure only as the attempt's allowance (3 s at least) cuts it, and falls back to SSE that late
// instead of at once. The transport clears both handlers as the attempt fails (`connectFailed`), so it hears one end.
// Remove once Node 22 is no longer supported (`engines`).
export { onUnopenedEnd }

/** Calls `ended` at `ws`'s first `error` or `close`, until it opens. */
function onUnopenedEnd(ws: WebSocket, ended: () => void): void {
  ws.onclose = ws.onerror = ended
}
