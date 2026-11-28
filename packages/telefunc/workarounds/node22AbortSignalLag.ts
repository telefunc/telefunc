// Workaround: on Node 22.0, an `AbortSignal.any()` signal aborts only after its source's own `abort` listeners ran
// (still before the source's `abort()` returns): inside such a listener, the Node adapter's signal for the client's
// leaving still reads as live. Waiting a microtask lets the source's synchronous dispatch finish, so the signal reads
// aborted when checked again. Without the wait, a call's end there takes a client that left for one that stayed, and
// reads off a body nobody sends.
// TODO: remove once Node 22.10.0 (https://github.com/nodejs/node/pull/54826) is Telefunc's minimum Node 22 version.
export { node22AbortSignalLag }

/** Lets a signal whose source is dispatching its abort, on Node 22.0, abort too. */
function node22AbortSignalLag(): Promise<void> {
  return Promise.resolve()
}
