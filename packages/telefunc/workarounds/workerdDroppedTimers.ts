// Workaround: workerd drops a request's bare timers once its response ends (unless `waitUntil` keeps the request alive),
// and nothing tells the code that armed them.
// Without the catch-up, a timer armed once, as the session registry's sweep is, never fires in a plain Worker, and what
// it would have freed stays for the isolate's life (wire-protocol/channel/server/SessionRegistry.sweep.spec.ts).
// Remove once workerd runs a request's bare timers after its response ended.
export { catchUpDroppedTimer }

/** Runs `fire` where the timer due at `dueAt` is overdue, whether its host dropped it or the event loop delayed it. */
function catchUpDroppedTimer(dueAt: number | null, fire: () => void): void {
  if (dueAt !== null && dueAt <= performance.now()) fire()
}
