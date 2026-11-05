export { Listeners }

/** Callbacks in the order they were added, each added and removed in O(1). `list()` is replaced on a change, never
 *  mutated, so a dispatch iterates the ones it started with. */
class Listeners<T> {
  private byRegistration: Map<object, T> | null = null
  private removers: Map<object, () => void> | null = null
  private snapshot: readonly T[] | undefined = NO_LISTENERS

  get size(): number {
    return this.byRegistration?.size ?? 0
  }

  /** Returns the registration's removal, which reports whether it was still registered. */
  add(listener: T, { signal, onRemove }: { signal?: AbortSignal; onRemove?: () => void } = {}): () => boolean {
    if (signal?.aborted) return () => false
    const byRegistration = (this.byRegistration ??= new Map())
    const registration = {}
    byRegistration.set(registration, listener)
    this.snapshot = undefined
    const remove = () => {
      if (!byRegistration.delete(registration)) return false
      this.snapshot = undefined
      signal?.removeEventListener('abort', remove)
      this.removers?.delete(registration)
      onRemove?.()
      return true
    }
    if (signal) {
      signal.addEventListener('abort', remove, { once: true })
    }
    if (signal || onRemove) (this.removers ??= new Map()).set(registration, remove)
    return remove
  }

  clear(): void {
    if (this.removers) for (const remove of this.removers.values()) remove()
    this.byRegistration?.clear()
    this.snapshot = NO_LISTENERS
  }

  list(): readonly T[] {
    return (this.snapshot ??= [...this.byRegistration!.values()])
  }
}

const NO_LISTENERS: readonly never[] = []
