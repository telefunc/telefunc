// Workaround: workerd drops the timers of a request once it answered it, and nothing tells the code that armed them.
// Without the catch-up, a timer armed once, as the session registry's sweep is, never fires in a plain Worker, and what
// it would have freed stays for the isolate's life (wire-protocol/channel/server/SessionRegistry.sweep.spec.ts).
// Remove once workerd runs the timers of a request it answered.
export { catchUpDroppedTimer }

/** Runs `fire` where the timer due at `dueAt` is overdue: its host dropped it. */
function catchUpDroppedTimer(dueAt: number | null, fire: () => void): void {
  if (dueAt !== null && dueAt <= performance.now()) fire()
}
