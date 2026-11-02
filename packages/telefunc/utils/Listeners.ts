export { Listeners }

/** Callbacks in the order they were added, each added and removed in O(1). `list()` is replaced on a change, never
 *  mutated, so a dispatch iterates the ones it started with. */
class Listeners<T> {
  private byRegistration: Map<object, T> | null = null
  private readonly abortRemovers = new Map<object, () => void>()
  private snapshot: readonly T[] | undefined = NO_LISTENERS

  get size(): number {
    return this.byRegistration?.size ?? 0
  }

  /** Returns the function that removes this registration. */
  add(listener: T, { signal }: { signal?: AbortSignal } = {}): () => void {
    if (signal?.aborted) return () => {}
    const byRegistration = (this.byRegistration ??= new Map())
    const registration = {}
    byRegistration.set(registration, listener)
    this.snapshot = undefined
    const remove = () => {
      if (byRegistration.delete(registration)) this.snapshot = undefined
      this.abortRemovers.get(registration)?.()
      this.abortRemovers.delete(registration)
    }
    if (signal) {
      signal.addEventListener('abort', remove, { once: true })
      this.abortRemovers.set(registration, () => signal.removeEventListener('abort', remove))
    }
    return remove
  }

  clear(): void {
    for (const remove of this.abortRemovers.values()) remove()
    this.abortRemovers.clear()
    this.byRegistration?.clear()
    this.snapshot = NO_LISTENERS
  }

  list(): readonly T[] {
    return (this.snapshot ??= [...this.byRegistration!.values()])
  }
}

const NO_LISTENERS: readonly never[] = []
