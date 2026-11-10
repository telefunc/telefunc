export { readLive, currentQuery }
export type { LiveQuery }

// A query's reads are recorded across its `await`s, and released with its telefunction call's response.
import 'telefunc/async_hooks'
// The `Live` it resolves to can be sent.
import './wire/server.js'
import { AsyncLocalStorage } from 'node:async_hooks'
import { getRawContext, REQUEST_CONTEXT } from 'telefunc'
import { assertUsage } from './assert.js'
import { LiveCell, type Live, type LiveSource } from './live.js'
import type { WatchedRead } from './sharedReads.js'

type RequestContext = { onClose(callback: () => void): void }

/** The `.live()` query running now: the statements it sends record their reads on it. */
const running = new AsyncLocalStorage<LiveQuery>()

/** Runs `query` (the ORM's own execution of a read) and records what it reads: the `Live` it resolves to goes stale
 *  when a committed write changes that. `wrapper` names the function that makes a database's reads recordable. */
async function readLive<T>(query: () => Promise<T>, wrapper: string): Promise<Live<T>> {
  const request = getRawContext()?.[REQUEST_CONTEXT] as RequestContext | undefined
  // Outside a telefunction call made by a client (SSR, a script), nobody can watch the result.
  if (!request) return new LiveCell(await query())
  const live = new LiveQuery(request)
  // Awaited inside the scope: an ORM's query (a Drizzle builder, a Prisma `all()`) runs when awaited.
  const value = await running.run(live, async () => await query())
  assertUsage(live.hasReads(), `.live() sent no query through a database wrapped by ${wrapper}`)
  return new LiveCell(value, [live])
}

function currentQuery(): LiveQuery | undefined {
  return running.getStore()
}

/** The live reads of one `.live()` query: until its `Live` is sent, then while a client holds it. */
class LiveQuery implements LiveSource {
  private readonly reads = new Set<WatchedRead>()
  private readonly listeners = new Set<() => void>()
  private recorded = false
  private isChanged = false
  private isWatched = false

  constructor(request: RequestContext) {
    // The response didn't send the `Live`: nobody will ever watch these reads.
    request.onClose(() => {
      if (!this.isWatched) this.release()
    })
  }

  /** Called by `read.watch()`. */
  add(read: WatchedRead): void {
    this.recorded = true
    if (this.isChanged) read.unwatch(this)
    else this.reads.add(read)
  }

  remove(read: WatchedRead): void {
    read.unwatch(this)
    this.reads.delete(read)
  }

  hasReads(): boolean {
    return this.recorded
  }

  /** A committed write changed what the query read. */
  changed(): void {
    if (this.isChanged) return
    this.isChanged = true
    // The client fetches again, and that query's reads replace these.
    this.release()
    for (const listener of this.listeners) listener()
  }

  subscribe(onStale: () => void): () => void {
    this.isWatched = true
    if (this.isChanged) queueMicrotask(onStale)
    else this.listeners.add(onStale)
    return () => {
      this.listeners.delete(onStale)
      if (this.listeners.size === 0) this.release()
    }
  }

  private release(): void {
    for (const read of this.reads) read.unwatch(this)
    this.reads.clear()
  }
}
