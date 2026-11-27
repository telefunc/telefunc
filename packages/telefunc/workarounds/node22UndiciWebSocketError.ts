// Workaround: Node 22's `WebSocket` (undici 6) fires `error` and no `close` where the connection fails before the
// upgrade, and stays `CONNECTING`. Without the `error` handler, a page on
// Node 22 learns of the failed attempt only once its attempt timeout or the recovery deadline fires, which delays the
// retry, or the fallback to SSE where the connection policy selects it. With it, the transport hears of the failure at
// once, and clears both handlers as the attempt fails (`connectFailed`), so it hears one end.
// TODO: remove once Node 22 bundles https://github.com/nodejs/undici/pull/5998 (fixes https://github.com/nodejs/undici/issues/3506) or Telefunc drops Node 22.
export { onUnopenedEnd }

/** Calls `ended` at `ws`'s first `error` or `close`, until it opens. */
function onUnopenedEnd(ws: WebSocket, ended: () => void): void {
  ws.onclose = ws.onerror = ended
}
