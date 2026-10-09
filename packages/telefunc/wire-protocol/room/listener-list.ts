export { ListenerList }

/** Listeners in registration order. Each registration is removed on its own in constant time, a callback registered twice included. */
class ListenerList<T> {
  private readonly _byRegistration = new Map<object, T>()

  get size(): number {
    return this._byRegistration.size
  }

  /** Returns the registration's removal, which reports whether it was still registered. */
  add(listener: T): () => boolean {
    const registration = {}
    this._byRegistration.set(registration, listener)
    return () => this._byRegistration.delete(registration)
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this._byRegistration.values()
  }
}
