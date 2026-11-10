export { derived, LiveCell, LIVE }
export type { Live, LiveSource }

import { assertUsage } from './assert.js'

/** Marks a server-side `LiveCell`, for the response replacer to find. */
const LIVE = Symbol.for('@telefunc/live/LiveCell')
const TRACKING = Symbol.for('@telefunc/live/tracking')

/**
 * A live value: `.data` is the snapshot the telefunction read. Returned from a telefunction, it tells the client when
 * the snapshot went stale, and the client fetches it again.
 *
 * ```ts
 * export async function onGetTodos(userId: number) {
 *   return db.select().from(todos).where(eq(todos.userId, userId)).live() // Promise<Live<Todo[]>>
 * }
 * ```
 */
type Live<T> = {
  readonly data: T
}

/** What tells a `LiveCell` its value went stale, once someone watches it: the live reads of one `.live()` query.
 *  `subscribe` returns `stop`. */
type LiveSource = { subscribe(onStale: () => void): () => void }

/**
 * Derives a live value from others: `.data` read inside `compute` makes that value a dependency, and the derived
 * value goes stale when any dependency does.
 *
 * ```ts
 * const summary = derived(() => ({ open: todos.data.filter((t) => !t.done).length }))
 * ```
 *
 * `compute` runs once, now: when a dependency goes stale, the client fetches again and the telefunction derives anew.
 */
function derived<R>(compute: () => R): Live<R> {
  return LiveCell.derived(compute)
}

/** The values `.data` reads register on, one frame per `derived()` running. One per process: a value of another copy
 *  of this package (two versions installed, or a bundle besides `node_modules`) registers on the same frames. */
const tracking: Set<LiveCell<unknown>>[] = ((globalThis as any)[TRACKING] ??= [])

/** The server side of a `Live<T>`. Nothing is watched until the response sends it, and the watching stops when the
 *  client lets go of it (`release()`). */
class LiveCell<T> implements Live<T> {
  readonly [LIVE] = true
  /** Until the response sends it: then only whether it went stale is watched, for as long as the client holds it. */
  private value: T | undefined
  /** What says the value went stale: the live reads it was computed from. They hold no rows. */
  private readonly sources: LiveSource[]
  private isSent = false
  private stops: (() => void)[] = []
  private readonly listeners = new Set<() => void>()
  private staleQueued = false

  constructor(value: T, sources: LiveSource[] = []) {
    this.value = value
    this.sources = sources
  }

  static derived<R>(compute: () => R): LiveCell<R> {
    const frame = new Set<LiveCell<unknown>>()
    tracking.push(frame)
    let value: R
    try {
      value = compute()
    } finally {
      tracking.pop()
    }
    // The sources, not the values read: a derived value watched by a client keeps no rows.
    return new LiveCell(value, [...new Set([...frame].flatMap((cell) => cell.sources))])
  }

  get data(): T {
    tracking[tracking.length - 1]?.add(this)
    return this.value as T
  }

  /** The response sends this value: returns it, lets go of it, and starts watching its sources. */
  send(): T {
    assertUsage(!this.isSent, 'A `Live` is sent once: return a new one from each telefunction call.')
    const value = this.value as T
    this.isSent = true
    this.value = undefined
    this.stops = this.sources.map((source) => source.subscribe(() => this.stale()))
    return value
  }

  /** Calls `listener` when the value goes stale; several causes in one tick call it once. Returns `stop`. */
  onStale(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  /** The client let go of it: the watching stops. */
  release(): void {
    for (const stop of this.stops) stop()
    this.stops = []
  }

  private stale(): void {
    if (this.staleQueued) return
    this.staleQueued = true
    queueMicrotask(() => {
      this.staleQueued = false
      if (this.stops.length > 0) for (const listener of [...this.listeners]) listener()
    })
  }
}
