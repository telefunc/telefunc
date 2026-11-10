export { getCallReads, LIVE_READS }
export type { LiveReads }

import { getRawContext, REQUEST_CONTEXT } from 'telefunc'
import type { LiveRead } from './match/liveRead.js'
import type { Router } from './match/router.js'

/** On a telefunction call's context: what libraries (and `watch()`) use to learn when the call's result changed. */
const LIVE_READS = Symbol.for('telefunc.liveReads')

type LiveReads = {
  /** Calls `listener` once, when a committed write changes what the call read (at once if one already did).
   *  Returns `stop`; the reads are kept until then. */
  onChange(listener: () => void): () => void
}

type RequestContext = { onClose(callback: () => void): void }

/** The live reads of one telefunction call. */
class CallReads implements LiveReads {
  private readonly reads = new Map<LiveRead, Router>()
  private readonly listeners = new Set<() => void>()
  private isChanged = false
  private isWatched = false

  constructor(request: RequestContext) {
    // Nobody asked to watch this call: its reads end with its response.
    request.onClose(() => {
      if (!this.isWatched) this.release()
    })
  }

  add(read: LiveRead, router: Router): void {
    if (this.isChanged) router.remove(read)
    else this.reads.set(read, router)
  }

  remove(read: LiveRead): void {
    this.reads.get(read)?.remove(read)
    this.reads.delete(read)
  }

  changed(): void {
    if (this.isChanged) return
    this.isChanged = true
    // The result is stale: the client fetches it again, and that call's reads replace these.
    this.release()
    for (const listener of this.listeners) listener()
  }

  onChange(listener: () => void): () => void {
    this.isWatched = true
    if (this.isChanged) queueMicrotask(listener)
    else this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
      if (this.listeners.size === 0) this.release()
    }
  }

  private release(): void {
    for (const [read, router] of this.reads) router.remove(read)
    this.reads.clear()
  }
}

/** The current telefunction call's live reads; `undefined` outside a call made by a client (SSR included). */
function getCallReads(): CallReads | undefined {
  const context = getRawContext()
  const request = context?.[REQUEST_CONTEXT] as RequestContext | undefined
  if (!context || !request) return undefined
  context[LIVE_READS] ??= new CallReads(request)
  return context[LIVE_READS] as CallReads
}
