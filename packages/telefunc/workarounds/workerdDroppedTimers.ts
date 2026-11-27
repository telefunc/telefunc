// Workaround: workerd drops a request's bare timers once its response ends, unless `waitUntil` keeps the request alive,
// and nothing tells the code that armed them. That is Workers' documented request lifetime, not an upstream bug:
// https://developers.cloudflare.com/workers/runtime-apis/context/#waituntil. Without the catch-up, a timer armed once,
// as the session registry's sweep is, never fires in a plain Worker, and what it would have freed stays for the
// isolate's life (wire-protocol/channel/server/SessionRegistry.sweep.spec.ts).
export { catchUpDroppedTimer }

/** Runs `fire` where the timer due at `dueAt` is overdue, whether its host dropped it or the event loop delayed it. */
function catchUpDroppedTimer(dueAt: number | null, fire: () => void): void {
  if (dueAt !== null && dueAt <= performance.now()) fire()
}
