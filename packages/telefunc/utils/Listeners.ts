export { Listeners }

/** Callbacks in the order they were added, each added and removed in O(1). `list()` is replaced on a change, never
 *  mutated, so a dispatch iterates the ones it started with. */
class Listeners<T> {
  private byRegistration: Map<object, T> | null = null
  private snapshot: readonly T[] | undefined = NO_LISTENERS

  get size(): number {
    return this.byRegistration?.size ?? 0
  }

  /** Returns the function that removes this registration. */
  add(listener: T): () => void {
    const byRegistration = (this.byRegistration ??= new Map())
    const registration = {}
    byRegistration.set(registration, listener)
    this.snapshot = undefined
    return () => {
      if (byRegistration.delete(registration)) this.snapshot = undefined
    }
  }

  list(): readonly T[] {
    return (this.snapshot ??= [...this.byRegistration!.values()])
  }
}

const NO_LISTENERS: readonly never[] = []
