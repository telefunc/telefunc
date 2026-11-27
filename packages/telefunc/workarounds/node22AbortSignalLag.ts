// Workaround: on Node 22.0, an `AbortSignal.any()` signal aborts a microtask after its source does: inside the source's
// `abort` listener, the Node adapter's signal for the client's leaving still reads as live. Without the wait, a call's
// end there takes a client that left for one that stayed, and reads off a body nobody sends.
// Remove once Node 22.0 is no longer supported (`engines`).
export { node22AbortSignalLag }

/** Lets a signal that is about to abort, on Node 22.0, do so. */
function node22AbortSignalLag(): Promise<void> {
  return Promise.resolve()
}
