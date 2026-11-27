// Workaround: on Node 22.0, a request's signal aborts a microtask after the body's read fails as its client leaves.
// Without the wait, a call's end takes a client that left for one that stayed, and reads off a body nobody sends.
// Remove once Node 22.0 is no longer supported (`engines`).
export { node22AbortSignalLag }

/** Lets a signal that is about to abort, on Node 22.0, do so. */
function node22AbortSignalLag(): Promise<void> {
  return Promise.resolve()
}
