export { Listeners }
export type { Registration }

/** One `add()`: its `listener` is null once it is removed. */
type Entry<T> = { listener: T | null }
type Registration<T> = Readonly<Entry<T>>

/** Listeners in the order they were added, each added and removed in O(1). A dispatch reaches the ones registered when
 *  it began, minus the ones removed since: it walks `list()`, which a change replaces and never mutates, and skips a
 *  registration whose `listener` is null. */
class Listeners<T> {
  private registrations: Set<Entry<T>> | null = null
  private removers: Map<object, () => void> | null = null
  private snapshot: readonly Registration<T>[] | undefined = NO_LISTENERS

  get size(): number {
    return this.registrations?.size ?? 0
  }

  /** Returns the function that removes this registration. */
  add(listener: T, { signal, onRemove }: { signal?: AbortSignal; onRemove?: () => void } = {}): () => void {
    if (signal?.aborted) return () => {}
    const registrations = (this.registrations ??= new Set())
    const registration: Entry<T> = { listener }
    registrations.add(registration)
    this.snapshot = undefined
    const remove = () => {
      if (!registrations.delete(registration)) return
      registration.listener = null
      this.snapshot = undefined
      signal?.removeEventListener('abort', remove)
      this.removers?.delete(registration)
      onRemove?.()
    }
    if (signal) {
      signal.addEventListener('abort', remove, { once: true })
    }
    if (signal || onRemove) (this.removers ??= new Map()).set(registration, remove)
    return remove
  }

  clear(): void {
    if (this.removers) for (const remove of this.removers.values()) remove()
    for (const registration of this.registrations ?? []) registration.listener = null
    this.registrations?.clear()
    this.snapshot = NO_LISTENERS
  }

  list(): readonly Registration<T>[] {
    return (this.snapshot ??= [...this.registrations!.values()])
  }
}

const NO_LISTENERS: readonly never[] = []
