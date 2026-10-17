export { Listeners }

/** Callbacks in the order they were added, each added and removed in O(1). `list()` is replaced on a change, never
 *  mutated, so a dispatch iterates the ones it started with. */
class Listeners<T> {
  private byRegistration = new Map<object, T>()
  private snapshot: readonly T[] | undefined = []

  get size(): number {
    return this.byRegistration.size
  }

  /** Returns the function that removes this registration. */
  add(listener: T): () => void {
    const registration = {}
    this.byRegistration.set(registration, listener)
    this.snapshot = undefined
    return () => {
      if (this.byRegistration.delete(registration)) this.snapshot = undefined
    }
  }

  list(): readonly T[] {
    return (this.snapshot ??= [...this.byRegistration.values()])
  }
}
